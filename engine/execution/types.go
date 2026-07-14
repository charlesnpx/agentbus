package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type Decision string

const (
	DecisionAccepted        Decision = "accepted"
	DecisionAwaitingAck     Decision = "awaiting_ack"
	DecisionCancelRequested Decision = "cancel_requested"
	DecisionTerminal        Decision = "terminal"
)

func AllDecisions() []Decision {
	return []Decision{
		DecisionAccepted,
		DecisionAwaitingAck,
		DecisionCancelRequested,
		DecisionTerminal,
	}
}

type Dispatch string

const (
	DispatchNone               Dispatch = "none"
	DispatchScheduled          Dispatch = "scheduled"
	DispatchSupervisorPrepared Dispatch = "supervisor_prepared"
	DispatchPermitGranted      Dispatch = "permit_granted"
	DispatchActive             Dispatch = "active"
	DispatchReconciling        Dispatch = "reconciling"
	DispatchContained          Dispatch = "contained"
	DispatchResultPublishing   Dispatch = "result_publishing"
	DispatchDone               Dispatch = "done"
)

func AllDispatches() []Dispatch {
	return []Dispatch{
		DispatchNone,
		DispatchScheduled,
		DispatchSupervisorPrepared,
		DispatchPermitGranted,
		DispatchActive,
		DispatchReconciling,
		DispatchContained,
		DispatchResultPublishing,
		DispatchDone,
	}
}

type Outcome string

const (
	OutcomeNone                  Outcome = "none"
	OutcomeCompleted             Outcome = "completed"
	OutcomeCompletedNoncompliant Outcome = "completed_noncompliant"
	OutcomeFailed                Outcome = "failed"
	OutcomeTimedOut              Outcome = "timed_out"
	OutcomeCanceled              Outcome = "canceled"
	OutcomeReaped                Outcome = "reaped"
	OutcomeInterrupted           Outcome = "interrupted"
	OutcomeQuarantined           Outcome = "quarantined"
)

func AllOutcomes() []Outcome {
	return []Outcome{
		OutcomeNone,
		OutcomeCompleted,
		OutcomeCompletedNoncompliant,
		OutcomeFailed,
		OutcomeTimedOut,
		OutcomeCanceled,
		OutcomeReaped,
		OutcomeInterrupted,
		OutcomeQuarantined,
	}
}

type Public string

const (
	PublicQueued                Public = "queued"
	PublicStarting              Public = "starting"
	PublicRunning               Public = "running"
	PublicRetrying              Public = "retrying"
	PublicCompleted             Public = "completed"
	PublicCompletedNoncompliant Public = "completed_noncompliant"
	PublicInterrupted           Public = "interrupted"
	PublicQuarantined           Public = "quarantined"
	PublicFailed                Public = "failed"
	PublicTimedOut              Public = "timed_out"
	PublicCanceled              Public = "canceled"
	PublicReaped                Public = "reaped"
	PublicOrphaned              Public = "orphaned"
)

func AllPublicStates() []Public {
	return []Public{
		PublicQueued,
		PublicStarting,
		PublicRunning,
		PublicRetrying,
		PublicCompleted,
		PublicCompletedNoncompliant,
		PublicInterrupted,
		PublicQuarantined,
		PublicFailed,
		PublicTimedOut,
		PublicCanceled,
		PublicReaped,
		PublicOrphaned,
	}
}

type Mode string

const (
	ModeIdentifiedFenced Mode = "IdentifiedFenced"
	ModeLegacyFenced     Mode = "LegacyFenced"
	ModeLegacyUnfenced   Mode = "LegacyUnfenced"
)

type TerminalProof string

const (
	ProofNone                            TerminalProof = ""
	ProofNeverPermittedAndRetired        TerminalProof = "NeverPermittedAndRetired"
	ProofCleanQuiescentOutcomeAndRetired TerminalProof = "CleanQuiescentOutcomeAndRetired"
	ProofContained                       TerminalProof = "Contained"
)

type PermitState string

const (
	PermitNone      PermitState = "none"
	PermitGranted   PermitState = "granted"
	PermitMaybeSent PermitState = "maybe_sent"
	PermitConsumed  PermitState = "consumed"
	PermitCanceled  PermitState = "canceled"
)

type Fingerprint struct {
	Algorithm string
	Version   int
	Value     string
}

const (
	FingerprintAlgorithmSHA256 = "sha256"
	FingerprintVersionV1       = 1
	FingerprintVersionV2       = 2
	CurrentFingerprintVersion  = FingerprintVersionV2
)

func (f Fingerprint) normalized() Fingerprint {
	if f.Algorithm == "" && f.Version == 0 && f.Value == "" {
		return CurrentFingerprint("empty")
	}
	return f
}

func (f Fingerprint) Equal(other Fingerprint) bool {
	f = f.normalized()
	other = other.normalized()
	return f.Algorithm == other.Algorithm && f.Version == other.Version && f.Value == other.Value
}

func (f Fingerprint) supported() bool {
	f = f.normalized()
	return fingerprintSupported(f.Algorithm, f.Version)
}

func CurrentFingerprint(rawTask string) Fingerprint {
	fp, err := FingerprintTask(FingerprintAlgorithmSHA256, CurrentFingerprintVersion, rawTask)
	if err != nil {
		return Fingerprint{Algorithm: FingerprintAlgorithmSHA256, Version: CurrentFingerprintVersion, Value: "unsupported"}
	}
	return fp
}

func FingerprintTask(algorithm string, version int, rawTask string) (Fingerprint, error) {
	if !fingerprintSupported(algorithm, version) {
		return Fingerprint{}, protocolError(CodeRequestFingerprintUnsupported, "", "fingerprint algorithm/version is unsupported")
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s/v%d\x00%s", algorithm, version, rawTask)))
	return Fingerprint{Algorithm: algorithm, Version: version, Value: hex.EncodeToString(sum[:])}, nil
}

func fingerprintSupported(algorithm string, version int) bool {
	return algorithm == FingerprintAlgorithmSHA256 && (version == FingerprintVersionV1 || version == FingerprintVersionV2)
}

type LaunchSpec struct {
	WorkspaceKey        string
	RequestID           string
	Backend             string
	SessionID           string
	CWD                 string
	DerivedWorkspaceKey string
	Task                string
	RawTask             string
	Fingerprint         Fingerprint
}

type CoordinatorObligation struct {
	JobID              string
	LaunchSpec         LaunchSpec
	Mode               Mode
	Committed          bool
	PreparedSupervisor *GroupRef
	Retired            bool
}

type GroupRef struct {
	PGID               int
	LeaderPID          int
	HighResStartToken  string
	KnownChildRefs     []ChildRef
	PlatformRetainedID string
}

func (g GroupRef) Valid() bool {
	return g.PGID != 0 && g.LeaderPID != 0 && g.HighResStartToken != ""
}

type ChildRef struct {
	PID               int
	HighResStartToken string
}

func (c ChildRef) Valid() bool {
	return c.PID != 0 && c.HighResStartToken != ""
}

type Evidence struct {
	Kind   string
	Detail string
}

func (e Evidence) Present() bool {
	return e.Kind != "" || e.Detail != ""
}

type LaunchQuiescenceEvidence struct {
	ChildExited Evidence
	GroupEmpty  Evidence
}

type ResultRef struct {
	Path   string
	Digest string
	Bytes  int64
}

type ResultArtifact struct {
	Path        string
	Digest      string
	Bytes       int64
	TempWritten bool
	TempSynced  bool
	Closed      bool
	Renamed     bool
	DirSynced   bool
}

type Aggregate struct {
	JobID                   string
	WorkspaceKey            string
	RequestID               string
	Fingerprint             Fingerprint
	Mode                    Mode
	LaunchSpec              LaunchSpec
	BootID                  string
	OwnerID                 string
	AttemptID               string
	Epoch                   int64
	Decision                Decision
	Dispatch                Dispatch
	Outcome                 Outcome
	Acknowledged            bool
	PermitState             PermitState
	PermitNonce             string
	PermitMaybeSent         bool
	ContainmentRequired     bool
	LaunchOrdinal           int
	ActiveOrdinal           int
	LaunchQuiescent         map[int]bool
	LaunchEvidence          map[int]LaunchQuiescenceEvidence
	LaunchNonceHistory      map[int]string
	LiveOrdinals            map[int]int
	Supervisor              GroupRef
	Child                   ChildRef
	PendingChild            ChildRef
	TerminalProof           TerminalProof
	TerminalReason          string
	Retired                 bool
	RetirementStarted       bool
	RetirementControlClosed bool
	RetirementWorkerExited  bool
	RetirementGroupEmpty    bool
	RetirementEvidence      Evidence
	Contained               bool
	ContainmentSignaled     bool
	ContainmentVerified     bool
	Containment             Evidence
	Result                  ResultRef
	SessionID               string
	CreatedStep             int64
	UpdatedStep             int64
	ExecutionSideEffects    int
	LossObserved            bool
	Corrupt                 bool
	QuarantineDiagnostic    string
	StartupReconciled       bool
	StartPhase              string
}

func (a Aggregate) Terminal() bool {
	return a.Decision == DecisionTerminal
}

func (a Aggregate) Public() Public {
	return PublicProjection(a.Decision, a.Dispatch, a.Outcome)
}

func (a *Aggregate) ensureMaps() {
	if a.LaunchQuiescent == nil {
		a.LaunchQuiescent = map[int]bool{}
	}
	if a.LaunchEvidence == nil {
		a.LaunchEvidence = map[int]LaunchQuiescenceEvidence{}
	}
	if a.LaunchNonceHistory == nil {
		a.LaunchNonceHistory = map[int]string{}
	}
	if a.LiveOrdinals == nil {
		a.LiveOrdinals = map[int]int{}
	}
}

func (a Aggregate) copy() Aggregate {
	a.LaunchQuiescent = copyBoolMap(a.LaunchQuiescent)
	a.LaunchEvidence = copyLaunchEvidenceMap(a.LaunchEvidence)
	a.LaunchNonceHistory = copyStringByIntMap(a.LaunchNonceHistory)
	a.LiveOrdinals = copyIntMap(a.LiveOrdinals)
	a.Supervisor.KnownChildRefs = append([]ChildRef(nil), a.Supervisor.KnownChildRefs...)
	return a
}

func copyBoolMap(in map[int]bool) map[int]bool {
	if len(in) == 0 {
		return map[int]bool{}
	}
	out := make(map[int]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyLaunchEvidenceMap(in map[int]LaunchQuiescenceEvidence) map[int]LaunchQuiescenceEvidence {
	if len(in) == 0 {
		return map[int]LaunchQuiescenceEvidence{}
	}
	out := make(map[int]LaunchQuiescenceEvidence, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyIntMap(in map[int]int) map[int]int {
	if len(in) == 0 {
		return map[int]int{}
	}
	out := make(map[int]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringByIntMap(in map[int]string) map[int]string {
	if len(in) == 0 {
		return map[int]string{}
	}
	out := make(map[int]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type Binding struct {
	WorkspaceKey string
	RequestID    string
	JobID        string
	Fingerprint  Fingerprint
}

type Tombstone struct {
	WorkspaceKey string
	RequestID    string
	JobID        string
	Fingerprint  Fingerprint
	ExpiredStep  int64
}

type ErrorCode string

const (
	CodeRequestConflict               ErrorCode = "request_conflict"
	CodeRequestExpired                ErrorCode = "request_expired"
	CodeRequestFingerprintUnsupported ErrorCode = "request_fingerprint_unsupported"
	CodePreconditionFailed            ErrorCode = "precondition_failed"
	CodeUnknownJob                    ErrorCode = "unknown_job"
	CodeRejected                      ErrorCode = "rejected"
	CodeLegacyUnfenced                ErrorCode = "legacy_unfenced"
	CodeCorruptFatal                  ErrorCode = "corrupt_fatal"
)

type ProtocolError struct {
	Code   ErrorCode
	JobID  string
	Reason string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return ""
	}
	if e.JobID != "" && e.Reason != "" {
		return fmt.Sprintf("%s: %s: %s", e.Code, e.JobID, e.Reason)
	}
	if e.JobID != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.JobID)
	}
	if e.Reason != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Reason)
	}
	return string(e.Code)
}

func IsCode(err error, code ErrorCode) bool {
	var pe *ProtocolError
	return errors.As(err, &pe) && pe.Code == code
}

func protocolError(code ErrorCode, jobID, reason string) error {
	return &ProtocolError{Code: code, JobID: jobID, Reason: reason}
}

func bindingKey(workspaceKey, requestID string) string {
	return workspaceKey + "\x00" + requestID
}

func validateWorkspaceRequest(workspaceKey, requestID string, requireRequest bool) error {
	if workspaceKey == "" || strings.ContainsRune(workspaceKey, '\x00') {
		return protocolError(CodeRejected, "", "invalid workspaceKey")
	}
	if requireRequest && requestID == "" {
		return protocolError(CodeRejected, "", "missing requestId")
	}
	if strings.ContainsRune(requestID, '\x00') {
		return protocolError(CodeRejected, "", "invalid requestId")
	}
	return nil
}

func materializeLaunchSpec(req SubmitRequest, fingerprint Fingerprint) (LaunchSpec, error) {
	spec := req.LaunchSpec
	spec.WorkspaceKey = req.WorkspaceKey
	spec.RequestID = req.RequestID
	if spec.SessionID == "" {
		spec.SessionID = req.SessionID
	}
	if spec.RawTask == "" {
		spec.RawTask = spec.Task
	}
	if spec.CWD != "" {
		derived, err := DeriveWorkspaceKey(spec.CWD)
		if err != nil {
			return LaunchSpec{}, err
		}
		spec.DerivedWorkspaceKey = derived
	}
	spec.Fingerprint = fingerprint
	return spec, validateLaunchSpec(spec, req.Mode)
}

func validateLaunchSpec(spec LaunchSpec, mode Mode) error {
	if spec.WorkspaceKey == "" || strings.ContainsRune(spec.WorkspaceKey, '\x00') {
		return protocolError(CodeRejected, "", "invalid launch workspaceKey")
	}
	if spec.RequestID != "" && strings.ContainsRune(spec.RequestID, '\x00') {
		return protocolError(CodeRejected, "", "invalid launch requestId")
	}
	if strings.TrimSpace(spec.Task) == "" {
		return protocolError(CodeRejected, "", "empty launch task")
	}
	if spec.RawTask == "" {
		return protocolError(CodeRejected, "", "empty raw task payload")
	}
	if strings.TrimSpace(spec.Backend) == "" {
		return protocolError(CodeRejected, "", "empty backend")
	}
	if mode != ModeLegacyUnfenced && !backendSupportsFenced(spec.Backend) {
		return protocolError(CodeRejected, "", "backend does not support fenced execution")
	}
	if strings.TrimSpace(spec.SessionID) == "" {
		return protocolError(CodeRejected, "", "missing session data")
	}
	derived, err := DeriveWorkspaceKey(spec.CWD)
	if err != nil {
		return err
	}
	if spec.DerivedWorkspaceKey != "" && spec.DerivedWorkspaceKey != derived {
		return protocolError(CodeRejected, "", "launch spec derived workspaceKey mismatch")
	}
	if derived != spec.WorkspaceKey {
		return protocolError(CodeRejected, "", "supplied workspaceKey does not match canonical cwd")
	}
	if !spec.Fingerprint.supported() {
		return protocolError(CodeRequestFingerprintUnsupported, "", "launch fingerprint is unsupported")
	}
	return nil
}

func requestRawTask(req SubmitRequest) string {
	if req.LaunchSpec.RawTask != "" {
		return req.LaunchSpec.RawTask
	}
	return req.LaunchSpec.Task
}

func DeriveWorkspaceKey(cwd string) (string, error) {
	if strings.TrimSpace(cwd) == "" {
		return "", protocolError(CodeRejected, "", "missing cwd")
	}
	clean := filepath.Clean(cwd)
	if !filepath.IsAbs(clean) {
		return "", protocolError(CodeRejected, "", "cwd is not canonical absolute path")
	}
	base := filepath.Base(clean)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "", protocolError(CodeRejected, "", "cwd cannot derive workspaceKey")
	}
	return base, nil
}

func backendSupportsFenced(backend string) bool {
	switch backend {
	case "codex", "claude":
		return true
	default:
		return false
	}
}
