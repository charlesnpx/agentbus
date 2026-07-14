package execution

import (
	"errors"
	"fmt"
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
	PermitNone     PermitState = "none"
	PermitGranted  PermitState = "granted"
	PermitCanceled PermitState = "canceled"
)

type Fingerprint struct {
	Algorithm string
	Version   int
	Value     string
}

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
	return f.Algorithm == "sha256" && f.Version == 1
}

func CurrentFingerprint(value string) Fingerprint {
	return Fingerprint{Algorithm: "sha256", Version: 1, Value: value}
}

type LaunchSpec struct {
	WorkspaceKey string
	RequestID    string
	Backend      string
	SessionID    string
	Task         string
	Fingerprint  Fingerprint
}

type CoordinatorObligation struct {
	JobID      string
	LaunchSpec LaunchSpec
	Mode       Mode
	Committed  bool
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

type ResultRef struct {
	Path   string
	Digest string
	Bytes  int64
}

type Aggregate struct {
	JobID                string
	WorkspaceKey         string
	RequestID            string
	Fingerprint          Fingerprint
	Mode                 Mode
	LaunchSpec           LaunchSpec
	BootID               string
	OwnerID              string
	AttemptID            string
	Epoch                int64
	Decision             Decision
	Dispatch             Dispatch
	Outcome              Outcome
	Acknowledged         bool
	PermitState          PermitState
	PermitNonce          string
	PermitMaybeSent      bool
	ContainmentRequired  bool
	LaunchOrdinal        int
	ActiveOrdinal        int
	LaunchQuiescent      map[int]bool
	Supervisor           GroupRef
	Child                ChildRef
	TerminalProof        TerminalProof
	TerminalReason       string
	Retired              bool
	Contained            bool
	Containment          Evidence
	Result               ResultRef
	SessionID            string
	CreatedStep          int64
	UpdatedStep          int64
	ExecutionSideEffects int
	LossObserved         bool
	Corrupt              bool
	QuarantineDiagnostic string
	StartupReconciled    bool
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
}

func (a Aggregate) copy() Aggregate {
	a.LaunchQuiescent = copyBoolMap(a.LaunchQuiescent)
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
