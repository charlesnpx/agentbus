package parkproto

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

const (
	Version      uint16 = 1
	MaxFrameSize        = 64 * 1024
)

type MessageType string

const (
	MessageIdentityReport MessageType = "IdentityReport"
	MessageRelease        MessageType = "Release"
	MessageReleaseAck     MessageType = "ReleaseAck"
)

type Message interface {
	messageType() MessageType
}

type ParkInstanceID string

func NewParkInstanceID() (ParkInstanceID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return ParkInstanceID("park-" + base64.RawURLEncoding.EncodeToString(raw[:])), nil
}

func (id ParkInstanceID) String() string {
	return string(id)
}

func (id ParkInstanceID) Validate() error {
	return validateProtocolToken("park instance id", string(id))
}

type IdentityReport struct {
	ParkInstanceID ParkInstanceID       `json:"parkInstanceId"`
	PID            int                  `json:"pid"`
	PGID           int                  `json:"pgid"`
	StartToken     procgroup.StartToken `json:"startToken"`
	KernelDomainID model.KernelDomainID `json:"kernelDomainId"`
}

func (IdentityReport) messageType() MessageType { return MessageIdentityReport }

func IdentityReportFromClaim(claim procgroup.ProcessClaim, parkInstanceID ParkInstanceID) IdentityReport {
	return IdentityReport{
		ParkInstanceID: parkInstanceID,
		PID:            claim.PID,
		PGID:           claim.PGID,
		StartToken:     claim.StartToken,
		KernelDomainID: claim.KernelDomainID,
	}
}

func (report IdentityReport) Validate() error {
	if err := report.ParkInstanceID.Validate(); err != nil {
		return fmt.Errorf("identity report: %w", err)
	}
	if _, err := procgroup.NewProcessClaim(report.PID, report.PGID, report.StartToken, report.KernelDomainID); err != nil {
		return fmt.Errorf("identity report: %w", err)
	}
	return nil
}

type Release struct {
	Binding          ReleaseBinding `json:"binding"`
	ExpectedGroupRef model.GroupRef `json:"expectedGroupRef"`
	ExecSpec         ExecSpec       `json:"execSpec"`
}

func (Release) messageType() MessageType { return MessageRelease }

type ReleaseBinding struct {
	ProtocolVersion     uint16               `json:"protocolVersion"`
	Sequence            uint64               `json:"sequence"`
	ParkInstanceID      ParkInstanceID       `json:"parkInstanceId"`
	StartToken          procgroup.StartToken `json:"startToken"`
	CustodyID           model.CustodyID      `json:"custodyId"`
	LaunchKey           model.LaunchKey      `json:"launchKey"`
	GroupRefDigest      string               `json:"groupRefDigest"`
	LogicalGrant        model.LaunchGrant    `json:"logicalGrant"`
	ReleaseSecret       model.ReleaseSecret  `json:"releaseSecret"`
	ImmutableExecDigest string               `json:"immutableExecDigest"`
}

type ReleaseExpectation struct {
	Binding ReleaseBinding `json:"binding"`
}

func (expectation ReleaseExpectation) Validate() error {
	return expectation.Binding.validateExpectation()
}

func (release Release) ValidateFor(sequence uint64, expectation ReleaseExpectation) error {
	if err := expectation.Validate(); err != nil {
		return fmt.Errorf("%w: invalid expectation: %v", ErrBinding, err)
	}
	if err := release.Binding.validateRelease(); err != nil {
		return fmt.Errorf("%w: invalid release binding: %v", ErrBinding, err)
	}
	if release.Binding.Sequence != sequence {
		return fmt.Errorf("%w: binding sequence %d does not match frame sequence %d", ErrBinding, release.Binding.Sequence, sequence)
	}
	groupDigest, err := DigestGroupRef(release.ExpectedGroupRef)
	if err != nil {
		return fmt.Errorf("%w: digest group ref: %v", ErrBinding, err)
	}
	if groupDigest != release.Binding.GroupRefDigest {
		return fmt.Errorf("%w: group ref digest mismatch", ErrBinding)
	}
	if release.ExpectedGroupRef.CustodyID != release.Binding.CustodyID {
		return fmt.Errorf("%w: group ref custody mismatch", ErrBinding)
	}
	if !release.ExpectedGroupRef.Launch.Equal(release.Binding.LaunchKey) {
		return fmt.Errorf("%w: group ref launch mismatch", ErrBinding)
	}
	if err := release.ExecSpec.Validate(); err != nil {
		return fmt.Errorf("%w: invalid exec spec: %v", ErrBinding, err)
	}
	execDigest, err := DigestExecSpec(release.ExecSpec)
	if err != nil {
		return fmt.Errorf("%w: digest exec spec: %v", ErrBinding, err)
	}
	if execDigest != release.Binding.ImmutableExecDigest {
		return fmt.Errorf("%w: exec digest mismatch", ErrBinding)
	}
	if !release.Binding.equal(expectation.Binding) {
		return fmt.Errorf("%w: release fields do not match expectation", ErrBinding)
	}
	return nil
}

func (binding ReleaseBinding) validateExpectation() error {
	if binding.ProtocolVersion != Version {
		return fmt.Errorf("protocol version = %d, want %d", binding.ProtocolVersion, Version)
	}
	if binding.Sequence == 0 {
		return fmt.Errorf("sequence is required")
	}
	if binding.ParkInstanceID != "" {
		if err := binding.ParkInstanceID.Validate(); err != nil {
			return err
		}
	}
	if binding.StartToken != "" {
		if err := validateProtocolToken("start token", binding.StartToken.String()); err != nil {
			return err
		}
	}
	if err := binding.CustodyID.Validate(); err != nil {
		return err
	}
	if err := binding.LaunchKey.Validate(); err != nil {
		return err
	}
	if err := validateDigest("group ref digest", binding.GroupRefDigest); err != nil {
		return err
	}
	if err := binding.LogicalGrant.Validate(); err != nil {
		return err
	}
	if err := binding.ReleaseSecret.Validate(); err != nil {
		return err
	}
	if err := validateDigest("immutable exec digest", binding.ImmutableExecDigest); err != nil {
		return err
	}
	return nil
}

func (binding ReleaseBinding) validateRelease() error {
	if err := binding.validateExpectation(); err != nil {
		return err
	}
	if err := binding.ParkInstanceID.Validate(); err != nil {
		return err
	}
	if err := validateProtocolToken("start token", binding.StartToken.String()); err != nil {
		return err
	}
	if err := validateDigest("group ref digest", binding.GroupRefDigest); err != nil {
		return err
	}
	return nil
}

func (binding ReleaseBinding) validateStatic() error {
	if err := binding.validateRelease(); err != nil {
		return err
	}
	if err := binding.LogicalGrant.Validate(); err != nil {
		return err
	}
	if err := binding.ReleaseSecret.Validate(); err != nil {
		return err
	}
	if err := validateDigest("immutable exec digest", binding.ImmutableExecDigest); err != nil {
		return err
	}
	return nil
}

func (binding ReleaseBinding) equal(other ReleaseBinding) bool {
	return binding.ProtocolVersion == other.ProtocolVersion &&
		binding.Sequence == other.Sequence &&
		binding.ParkInstanceID == other.ParkInstanceID &&
		binding.StartToken == other.StartToken &&
		binding.CustodyID == other.CustodyID &&
		binding.LaunchKey.Equal(other.LaunchKey) &&
		binding.GroupRefDigest == other.GroupRefDigest &&
		binding.LogicalGrant.Attempt.Equal(other.LogicalGrant.Attempt) &&
		binding.LogicalGrant.Ordinal == other.LogicalGrant.Ordinal &&
		binding.LogicalGrant.Nonce == other.LogicalGrant.Nonce &&
		binding.LogicalGrant.GrantedBy == other.LogicalGrant.GrantedBy &&
		binding.ReleaseSecret == other.ReleaseSecret &&
		binding.ImmutableExecDigest == other.ImmutableExecDigest
}

type ReleaseAck struct {
	AcceptedSequence uint64 `json:"acceptedSequence"`
}

func (ReleaseAck) messageType() MessageType { return MessageReleaseAck }

func (ack ReleaseAck) Validate() error {
	if ack.AcceptedSequence == 0 {
		return fmt.Errorf("release ack accepted sequence is required")
	}
	return nil
}

type ExecSpec struct {
	Path string   `json:"path"`
	Argv []string `json:"argv"`
	Env  []string `json:"env"`
	Dir  string   `json:"dir,omitempty"`
}

func (spec ExecSpec) Validate() error {
	if spec.Path == "" {
		return fmt.Errorf("exec path is required")
	}
	if !filepath.IsAbs(spec.Path) {
		return fmt.Errorf("exec path must be absolute")
	}
	if containsNUL(spec.Path) {
		return fmt.Errorf("exec path must not contain NUL")
	}
	if len(spec.Argv) == 0 {
		return fmt.Errorf("exec argv is required")
	}
	for i, arg := range spec.Argv {
		if arg == "" {
			return fmt.Errorf("exec argv[%d] is required", i)
		}
		if containsNUL(arg) {
			return fmt.Errorf("exec argv[%d] must not contain NUL", i)
		}
	}
	for i, env := range spec.Env {
		if !strings.Contains(env, "=") || strings.HasPrefix(env, "=") {
			return fmt.Errorf("exec env[%d] must be NAME=value", i)
		}
		if containsNUL(env) {
			return fmt.Errorf("exec env[%d] must not contain NUL", i)
		}
	}
	if spec.Dir != "" {
		if !filepath.IsAbs(spec.Dir) {
			return fmt.Errorf("exec dir must be absolute")
		}
		if containsNUL(spec.Dir) {
			return fmt.Errorf("exec dir must not contain NUL")
		}
	}
	return nil
}

func DigestExecSpec(spec ExecSpec) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	return digestJSON(spec)
}

func DigestGroupRef(ref model.GroupRef) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}
	return digestJSON(ref)
}

func digestJSON(value any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return "", err
	}
	sum := sha256.Sum256(bytes.TrimSpace(buf.Bytes()))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateDigest(field, digest string) error {
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+sha256.Size*2 {
		return fmt.Errorf("%s must be sha256 hex digest", field)
	}
	for _, r := range digest[len("sha256:"):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return fmt.Errorf("%s must be lowercase sha256 hex digest", field)
		}
	}
	return nil
}

func validateProtocolToken(field, value string) error {
	const maxTokenBytes = 256
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > maxTokenBytes {
		return fmt.Errorf("%s is too long", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", field)
	}
	for _, r := range value {
		if r <= ' ' || r == 0x7f {
			return fmt.Errorf("%s must not contain whitespace or control characters", field)
		}
	}
	return nil
}

func containsNUL(value string) bool {
	return strings.ContainsRune(value, 0)
}
