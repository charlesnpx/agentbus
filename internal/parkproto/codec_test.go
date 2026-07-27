package parkproto

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

func TestCodecRejectsStrictJSONViolations(t *testing.T) {
	releasePayload := mustReleasePayload(t)
	duplicateReleaseSecretPayload := bytes.Replace(
		releasePayload,
		[]byte(`"releaseSecret":"release-secret-1"`),
		[]byte(`"releaseSecret":"release-secret-1","releaseSecret":"release-secret-2"`),
		1,
	)
	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{
			name: "unknown envelope field",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"ReleaseAck","payload":{"acceptedSequence":1},"extra":true}`,
				Version,
			))),
		},
		{
			name: "unknown payload field",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"ReleaseAck","payload":{"acceptedSequence":1,"extra":true}}`,
				Version,
			))),
		},
		{
			name: "duplicate sequence",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"sequence":1,"type":"ReleaseAck","payload":{"acceptedSequence":1}}`,
				Version,
			))),
		},
		{
			name: "duplicate type",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"ReleaseAck","type":"ReleaseAck","payload":{"acceptedSequence":1}}`,
				Version,
			))),
		},
		{
			name: "duplicate release secret",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"Release","payload":%s}`,
				Version,
				duplicateReleaseSecretPayload,
			))),
		},
		{
			name: "trailing data",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"ReleaseAck","payload":{"acceptedSequence":1}} true`,
				Version,
			))),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(bytes.NewReader(tt.raw)).Read()
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("Read() error=%v, want %v", err, ErrMalformed)
			}
		})
	}
}

func TestCodecRejectsCaseFoldedDuplicateJSONKeys(t *testing.T) {
	goodFrame := mustFrame(t, Version, 1, ReleaseAck{AcceptedSequence: 1})
	t.Run("legitimate single-cased frame decodes", func(t *testing.T) {
		received, err := NewReader(bytes.NewReader(goodFrame)).Read()
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		if received.Sequence != 1 {
			t.Fatalf("sequence=%d, want 1", received.Sequence)
		}
		if got, ok := received.Message.(ReleaseAck); !ok || got.AcceptedSequence != 1 {
			t.Fatalf("message=%#v, want ReleaseAck acceptedSequence=1", received.Message)
		}
	})

	releasePayload := mustReleasePayload(t)
	mixedCaseReleaseSecretPayload := bytes.Replace(
		releasePayload,
		[]byte(`"releaseSecret":"release-secret-1"`),
		[]byte(`"releaseSecret":"release-secret-1","ReleaseSecret":"release-secret-2"`),
		1,
	)
	if bytes.Equal(mixedCaseReleaseSecretPayload, releasePayload) {
		t.Fatal("test release payload did not contain releaseSecret")
	}

	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{
			name: "mixed-case duplicate sequence",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":999,"Sequence":1,"type":"ReleaseAck","payload":{"acceptedSequence":1}}`,
				Version,
			))),
		},
		{
			name: "mixed-case duplicate type",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"IdentityReport","Type":"ReleaseAck","payload":{"acceptedSequence":1}}`,
				Version,
			))),
		},
		{
			name: "nested mixed-case duplicate release secret",
			raw: rawPayload([]byte(fmt.Sprintf(
				`{"version":%d,"sequence":1,"type":"Release","payload":%s}`,
				Version,
				mixedCaseReleaseSecretPayload,
			))),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var stream bytes.Buffer
			stream.Write(tt.raw)
			stream.Write(goodFrame)

			reader := NewReader(&stream)
			received, err := reader.Read()
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("Read() error=%v, want %v", err, ErrMalformed)
			}
			if received.Sequence != 0 || received.Message != nil {
				t.Fatalf("Read() received=%#v, want zero value on rejection", received)
			}
			if reader.lastSeq != 0 {
				t.Fatalf("lastSeq=%d, want 0 after rejection", reader.lastSeq)
			}

			received, err = reader.Read()
			if err != nil {
				t.Fatalf("Read() after rejection error = %v", err)
			}
			if received.Sequence != 1 {
				t.Fatalf("sequence after rejection=%d, want 1", received.Sequence)
			}
			if _, ok := received.Message.(ReleaseAck); !ok {
				t.Fatalf("message after rejection=%T, want ReleaseAck", received.Message)
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

func TestReleaseRejectsRemovedLogicalGrantField(t *testing.T) {
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
	release := Release{Binding: binding, ExpectedGroupRef: groupRef, ExecSpec: execSpec}

	payload := releasePayloadWithLogicalGrant(t, release)
	raw := rawPayload([]byte(fmt.Sprintf(
		`{"version":%d,"sequence":1,"type":"Release","payload":%s}`,
		Version,
		payload,
	)))
	_, err = NewReader(bytes.NewReader(raw)).Read()
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read() error=%v, want %v", err, ErrMalformed)
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
	wrongDigest.GroupRefDigest = "sha256:" + strings.Repeat("0", 64)
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: wrongDigest}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong group digest error=%v, want %v", err, ErrBinding)
	}

	release.Binding.ImmutableExecDigest = "sha256:" + strings.Repeat("0", 64)
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: expectation}); !errors.Is(err, ErrBinding) {
		t.Fatalf("wrong exec digest error=%v, want %v", err, ErrBinding)
	}
}

func TestReleaseGroupRefRetainedDomainRoundTripRequiresCurrentFields(t *testing.T) {
	execSpec := ExecSpec{Path: "/bin/echo", Argv: []string{"/bin/echo", "ok"}, Env: []string{"A=B"}, Dir: "/tmp"}
	execDigest, err := DigestExecSpec(execSpec)
	if err != nil {
		t.Fatal(err)
	}
	groupRef := testGroupRef()
	groupRef.PIDNamespaceID = "pidns-1"
	groupRef.PIDNamespaceState = model.PIDNamespaceKnown
	groupRef.RetainedDomainID = "retained-domain-1"
	groupRef.RetainedDomainState = model.RetainedDomainKnown
	groupRef.RetainedID = "retained-1"
	groupDigest, err := DigestGroupRef(groupRef)
	if err != nil {
		t.Fatal(err)
	}
	binding := testReleaseBinding(execDigest)
	binding.GroupRefDigest = groupDigest
	release := Release{Binding: binding, ExpectedGroupRef: groupRef, ExecSpec: execSpec}

	var roundTrip bytes.Buffer
	if err := WriteFrame(&roundTrip, Version, 1, release); err != nil {
		t.Fatalf("WriteFrame() error = %v", err)
	}
	received, err := NewReader(&roundTrip).Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	decoded, ok := received.Message.(Release)
	if !ok {
		t.Fatalf("decoded message = %T, want Release", received.Message)
	}
	if !decoded.ExpectedGroupRef.Equal(groupRef) {
		t.Fatalf("decoded GroupRef = %#v, want %#v", decoded.ExpectedGroupRef, groupRef)
	}
	if !decoded.ExpectedGroupRef.KernelDomain().ProvablySame(groupRef.KernelDomain()) {
		t.Fatalf("decoded retained domain was not provably same")
	}

	payload, err := json.Marshal(release)
	if err != nil {
		t.Fatalf("Marshal release error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("Unmarshal release fields error = %v", err)
	}
	var groupFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["expectedGroupRef"], &groupFields); err != nil {
		t.Fatalf("Unmarshal group fields error = %v", err)
	}
	if _, ok := groupFields["RetainedDomainState"]; !ok {
		t.Fatal("encoded release omitted required RetainedDomainState")
	}
	delete(groupFields, "RetainedDomainState")
	missingRetainedDomainStateGroup, err := json.Marshal(groupFields)
	if err != nil {
		t.Fatalf("Marshal missing-field group error = %v", err)
	}
	fields["expectedGroupRef"] = missingRetainedDomainStateGroup
	missingFieldPayload, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("Marshal missing-field payload error = %v", err)
	}
	raw := rawPayload([]byte(fmt.Sprintf(
		`{"version":%d,"sequence":1,"type":"Release","payload":%s}`,
		Version,
		missingFieldPayload,
	)))
	if _, err := NewReader(bytes.NewReader(raw)).Read(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("Read() missing RetainedDomainState error=%v, want %v", err, ErrMalformed)
	}
}

func TestReleaseBindingRejectsDifferentlyShapedExpectedGroupRef(t *testing.T) {
	execSpec := ExecSpec{Path: "/bin/echo", Argv: []string{"/bin/echo", "ok"}, Env: []string{"A=B"}, Dir: "/tmp"}
	execDigest, err := DigestExecSpec(execSpec)
	if err != nil {
		t.Fatal(err)
	}
	expectedGroup := testGroupRef()
	expectedDigest, err := DigestGroupRef(expectedGroup)
	if err != nil {
		t.Fatal(err)
	}
	expectedBinding := testReleaseBinding(execDigest)
	expectedBinding.GroupRefDigest = expectedDigest

	differentGroup := expectedGroup
	differentGroup.RetainedDomainID = "retained-domain-different"
	differentGroup.RetainedDomainState = model.RetainedDomainKnown
	differentGroup.RetainedID = "retained-different"
	differentDigest, err := DigestGroupRef(differentGroup)
	if err != nil {
		t.Fatal(err)
	}
	releaseBinding := expectedBinding
	releaseBinding.GroupRefDigest = differentDigest
	release := Release{Binding: releaseBinding, ExpectedGroupRef: differentGroup, ExecSpec: execSpec}

	if err := release.ValidateFor(1, ReleaseExpectation{Binding: expectedBinding}); !errors.Is(err, ErrBinding) {
		t.Fatalf("different expected group binding error=%v, want %v", err, ErrBinding)
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

func TestReleaseBindingRequiresExactExpectedGroupDigest(t *testing.T) {
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
	release := Release{Binding: binding, ExpectedGroupRef: groupRef, ExecSpec: execSpec}

	wildcardExpectation := binding
	wildcardExpectation.GroupRefDigest = ""
	if err := release.ValidateFor(1, ReleaseExpectation{Binding: wildcardExpectation}); !errors.Is(err, ErrBinding) {
		t.Fatalf("empty expected group digest error=%v, want %v", err, ErrBinding)
	}

	mutated := release
	mutated.ExpectedGroupRef.Monitor = model.ProcessIdentity{PID: 202, HighResStartToken: "start-202"}
	mutatedDigest, err := DigestGroupRef(mutated.ExpectedGroupRef)
	if err != nil {
		t.Fatal(err)
	}
	mutated.Binding.GroupRefDigest = mutatedDigest
	if err := mutated.ValidateFor(1, ReleaseExpectation{Binding: binding}); !errors.Is(err, ErrBinding) {
		t.Fatalf("self-consistent wrong group digest error=%v, want %v", err, ErrBinding)
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

func mustReleasePayload(t *testing.T) []byte {
	t.Helper()
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
	raw, err := json.Marshal(Release{Binding: binding, ExpectedGroupRef: groupRef, ExecSpec: execSpec})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func releasePayloadWithLogicalGrant(t *testing.T, release Release) []byte {
	t.Helper()
	raw, err := json.Marshal(release)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	binding, ok := payload["binding"].(map[string]any)
	if !ok {
		t.Fatalf("release payload binding = %T, want object", payload["binding"])
	}
	binding["logicalGrant"] = map[string]any{
		"Attempt": map[string]any{
			"JobID":     "job-1",
			"AttemptID": "attempt-1",
			"Epoch":     1,
		},
		"Ordinal":   1,
		"Nonce":     "nonce-removed-field",
		"GrantedBy": map[string]any{"BootID": "boot-1", "OwnerID": "owner-1"},
	}
	out, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

var _ io.Reader = (*bytes.Reader)(nil)
