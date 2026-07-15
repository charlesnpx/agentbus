package parkproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestCodecRoundTripAndRejectsBadFrames(t *testing.T) {
	report := IdentityReport{
		ParkInstanceID: "park-instance-1",
		PID:            101,
		PGID:           101,
		StartToken:     "start-101",
		KernelDomainID: model.KernelDomainID{
			HostBootID:        "boot-1",
			PIDNamespaceState: model.PIDNamespaceNotApplicable,
		},
	}

	var good bytes.Buffer
	writer := NewWriter(&good)
	if seq, err := writer.WriteIdentityReport(report); err != nil || seq != 1 {
		t.Fatalf("WriteIdentityReport() seq=%d err=%v", seq, err)
	}
	received, err := NewReader(&good).Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if received.Sequence != 1 {
		t.Fatalf("sequence=%d, want 1", received.Sequence)
	}
	if got, ok := received.Message.(IdentityReport); !ok || got.ParkInstanceID != report.ParkInstanceID || got.PID != report.PID || got.StartToken != report.StartToken {
		t.Fatalf("message=%#v", received.Message)
	}

	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{name: "version mismatch", raw: mustFrame(t, Version+1, 1, report), want: ErrVersionMismatch},
		{name: "malformed json", raw: rawPayload([]byte("{")), want: ErrMalformed},
		{name: "truncated payload", raw: truncatedPayload(t, mustFrame(t, Version, 1, report)), want: ErrTruncated},
		{name: "oversized", raw: oversizedFrame(), want: ErrOversized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(bytes.NewReader(tt.raw)).Read()
			if !errors.Is(err, tt.want) {
				t.Fatalf("Read() error=%v, want %v", err, tt.want)
			}
		})
	}
}

func TestCodecRejectsDuplicateAndOutOfOrderSequences(t *testing.T) {
	report := IdentityReport{
		ParkInstanceID: "park-instance-1",
		PID:            101,
		PGID:           101,
		StartToken:     "start-101",
		KernelDomainID: model.KernelDomainID{
			HostBootID:        "boot-1",
			PIDNamespaceState: model.PIDNamespaceNotApplicable,
		},
	}
	for _, tt := range []struct {
		name string
		seqs []uint64
	}{
		{name: "duplicate", seqs: []uint64{1, 1}},
		{name: "out of order", seqs: []uint64{2}},
		{name: "gap", seqs: []uint64{1, 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var raw bytes.Buffer
			for _, seq := range tt.seqs {
				if err := WriteFrame(&raw, Version, seq, report); err != nil {
					t.Fatal(err)
				}
			}
			reader := NewReader(&raw)
			if tt.seqs[0] == 1 {
				if _, err := reader.Read(); err != nil {
					t.Fatalf("first Read() error = %v", err)
				}
			}
			_, err := reader.Read()
			if !errors.Is(err, ErrSequence) {
				t.Fatalf("Read() error=%v, want %v", err, ErrSequence)
			}
		})
	}
}

func TestReadRawFrameRejectsOversizedDeclaredLengthBeforeBodyRead(t *testing.T) {
	reader := newDeclaredLengthOnlyReader(uint32(MaxFrameSize + 32*1024*1024))
	_, err := readRawFrame(reader, MaxFrameSize)
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("readRawFrame() error=%v, want %v", err, ErrOversized)
	}
	if reader.bodyRead {
		t.Fatal("oversized frame body was read after untrusted length")
	}
}

func TestReleaseBindingChecksPhysicalSecretGroupDigestAndExecDigest(t *testing.T) {
	execSpec := ExecSpec{Path: "/bin/echo", Argv: []string{"/bin/echo", "ok"}, Env: []string{"A=B"}, Dir: "/tmp"}
	execDigest, err := DigestExecSpec(execSpec)
	if err != nil {
		t.Fatal(err)
	}
	groupRef := testGroupRef()
	groupDigest, err := DigestGroupRef(groupRef)
	if err != nil {
		t.Fatal(err)
	}
	binding := testReleaseBinding(execDigest)
	binding.GroupRefDigest = groupDigest
	expectation := binding
	expectation.GroupRefDigest = ""
	release := Release{Binding: binding, ExpectedGroupRef: groupRef, ExecSpec: execSpec}
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: expectation}); err != nil {
		t.Fatalf("valid release rejected: %v", err)
	}

	wrongSecret := expectation
	wrongSecret.ReleaseSecret = "release-secret-other"
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: wrongSecret}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong secret error=%v, want %v", err, ErrBinding)
	}

	wrongDigest := expectation
	wrongDigest.GroupRefDigest = strings.Replace(binding.GroupRefDigest, "a", "b", 1)
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: wrongDigest}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong group digest error=%v, want %v", err, ErrBinding)
	}

	release.Binding.ImmutableExecDigest = "sha256:" + strings.Repeat("0", 64)
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: expectation}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong exec digest error=%v, want %v", err, ErrBinding)
	}
}

func TestReleaseBindingRejectsReplayAcrossParkInstancesAndConsumesOnce(t *testing.T) {
	execSpec := ExecSpec{Path: "/bin/echo", Argv: []string{"/bin/echo", "ok"}, Env: []string{"A=B"}, Dir: "/tmp"}
	execDigest, err := DigestExecSpec(execSpec)
	if err != nil {
		t.Fatal(err)
	}
	groupRef := testGroupRef()
	groupDigest, err := DigestGroupRef(groupRef)
	if err != nil {
		t.Fatal(err)
	}

	bindingA := testReleaseBinding(execDigest)
	bindingA.GroupRefDigest = groupDigest
	expectationA := bindingA
	expectationA.GroupRefDigest = ""
	releaseA := Release{Binding: bindingA, ExpectedGroupRef: groupRef, ExecSpec: execSpec}
	if err := releaseA.ValidateFor(1, ReleaseExpectation{Binding: expectationA}); err != nil {
		t.Fatalf("matching instance release rejected: %v", err)
	}

	expectationB := expectationA
	expectationB.ParkInstanceID = "park-instance-b"
	expectationB.StartToken = "start-101-b"
	if err := releaseA.ValidateFor(1, ReleaseExpectation{Binding: expectationB}); !errors.Is(err, ErrBinding) {
		t.Fatalf("fresh instance replay error=%v, want %v", err, ErrBinding)
	}

	wrongInstance := expectationA
	wrongInstance.ParkInstanceID = "park-instance-other"
	if err := releaseA.ValidateFor(1, ReleaseExpectation{Binding: wrongInstance}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong park instance error=%v, want %v", err, ErrBinding)
	}

	wrongStartToken := expectationA
	wrongStartToken.StartToken = "start-101-other"
	if err := releaseA.ValidateFor(1, ReleaseExpectation{Binding: wrongStartToken}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong start token error=%v, want %v", err, ErrBinding)
	}

	var raw bytes.Buffer
	if err := WriteFrame(&raw, Version, 1, releaseA); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&raw, Version, 1, releaseA); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(&raw)
	received, err := reader.Read()
	if err != nil {
		t.Fatalf("first release read error = %v", err)
	}
	got, ok := received.Message.(Release)
	if !ok {
		t.Fatalf("message=%T, want Release", received.Message)
	}
	if err := got.ValidateFor(received.Sequence, ReleaseExpectation{Binding: expectationA}); err != nil {
		t.Fatalf("first release validation error = %v", err)
	}
	if _, err := reader.Read(); !errors.Is(err, ErrSequence) {
		t.Fatalf("second release read error=%v, want %v", err, ErrSequence)
	}
}

func mustFrame(t *testing.T, version uint16, sequence uint64, message Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, version, sequence, message); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func rawPayload(payload []byte) []byte {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	buf.Write(header[:])
	buf.Write(payload)
	return buf.Bytes()
}

func truncatedPayload(t *testing.T, raw []byte) []byte {
	t.Helper()
	if len(raw) < 6 {
		t.Fatalf("frame too short")
	}
	return raw[:len(raw)-2]
}

func oversizedFrame() []byte {
	var buf bytes.Buffer
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], MaxFrameSize+1)
	buf.Write(header[:])
	buf.WriteString(strings.Repeat("x", 8))
	return buf.Bytes()
}

type declaredLengthOnlyReader struct {
	header   [4]byte
	offset   int
	bodyRead bool
}

func newDeclaredLengthOnlyReader(length uint32) *declaredLengthOnlyReader {
	reader := &declaredLengthOnlyReader{}
	binary.BigEndian.PutUint32(reader.header[:], length)
	return reader
}

func (reader *declaredLengthOnlyReader) Read(p []byte) (int, error) {
	if reader.offset < len(reader.header) {
		n := copy(p, reader.header[reader.offset:])
		reader.offset += n
		return n, nil
	}
	reader.bodyRead = true
	return 0, errors.New("body read attempted")
}

func testReleaseBinding(execDigest string) ReleaseBinding {
	attempt := model.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Epoch: 1}
	boot := model.BootRef{BootID: "boot-1", OwnerID: "owner-1"}
	return ReleaseBinding{
		ProtocolVersion: Version,
		Sequence:        1,
		ParkInstanceID:  "park-instance-a",
		StartToken:      "start-101",
		CustodyID:       "custody-1",
		LaunchKey: model.LaunchKey{
			Attempt: attempt,
			Ordinal: model.LaunchOrdinalOne,
		},
		GroupRefDigest:      "sha256:" + strings.Repeat("a", 64),
		LogicalGrant:        model.LaunchGrant{Attempt: attempt, Ordinal: model.LaunchOrdinalOne, Nonce: "nonce-1", GrantedBy: boot},
		ReleaseSecret:       "release-secret-1",
		ImmutableExecDigest: execDigest,
	}
}

func testGroupRef() model.GroupRef {
	attempt := model.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Epoch: 1}
	return model.GroupRef{
		Version:           1,
		CustodyID:         "custody-1",
		Launch:            model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		HostBootID:        "boot-1",
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              101,
		Leader:            model.ProcessIdentity{PID: 101, HighResStartToken: "start-101"},
		Monitor:           model.ProcessIdentity{PID: 101, HighResStartToken: "start-101"},
	}
}

var _ io.Reader = (*bytes.Reader)(nil)
