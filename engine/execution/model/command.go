package model

import (
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

type PermitNonce = LaunchNonce
type QuiescenceReceipt = QuiescenceCertificate
type ResultReceipt = ResultCertificate

type Command interface {
	isCommand()
}

type Acknowledge struct {
	Ref            AttemptRef
	AcknowledgedBy BootRef
}

type BeginReject struct {
	Ref         AttemptRef
	RequestedBy BootRef
}

type BindGroup struct {
	Ref     AttemptRef
	Ordinal LaunchOrdinal
	Group   GroupRef
}

type CommitGrant struct {
	Ref       AttemptRef
	Ordinal   LaunchOrdinal
	Nonce     PermitNonce
	GrantedBy BootRef
}

type RecordReleaseOutcome struct {
	Ref        AttemptRef
	Ordinal    LaunchOrdinal
	Outcome    LaunchReleaseOutcome
	RecordedBy BootRef
}

type RecordRelease struct {
	Ref         AttemptRef
	Ordinal     LaunchOrdinal
	Child       ChildIdentity
	ReleasedBy  BootRef
	Observation Evidence
}

type RecordQuiescence struct {
	Ref     AttemptRef
	Receipt QuiescenceReceipt
}

type RequestCancel struct {
	JobID       JobID
	RequestedBy BootRef
}

type ObserveOutcome struct {
	Ref      AttemptRef
	Outcome  Outcome
	Contract *engine.ContractStamp
}

type CertifyResult struct {
	Ref     AttemptRef
	Receipt ResultReceipt
}

// RecordFinalAttemptStart records the start of the attempt that is currently
// final. A contract retry replaces this timestamp; no attempt history is kept.
type RecordFinalAttemptStart struct {
	JobID     JobID
	StartedAt time.Time
}

// RecordFailure attaches the first observed terminal-failure explanation to a
// job without changing its outcome or terminal-state proof.
type RecordFailure struct {
	JobID  JobID
	Class  engine.FailureClass
	Reason string
}

// RecordTransportFrameDrops preserves bounded metadata about backend frames
// discarded by the transport reader without retaining backend payload bytes.
type RecordTransportFrameDrops struct {
	JobID JobID
	Drops engine.TransportFrameDrops
}

// RecordCancellation attaches the first observed cancellation explanation to
// a job without changing its outcome or terminal-state proof.
type RecordCancellation struct {
	JobID  JobID
	Origin engine.CancellationOrigin
	Reason string
}

type TerminalIntent struct {
	Outcome   Outcome
	Cause     TerminalCause
	DerivedBy BootRef
	Contract  *engine.ContractStamp
	// PartialResult is a verified transcript excerpt committed with a timed-out
	// or interrupted terminal record. It is never a worker final report.
	PartialResult      *ResultReceipt
	CancellationOrigin engine.CancellationOrigin
	CancellationReason string
	// FinalAttemptEndedAt is when the final contract attempt reached this
	// terminal transition. It is not a whole-job duration or attempt history.
	FinalAttemptEndedAt *time.Time
	// ObservedWorkspaceWriteItemCount is the backend-reported workspace-write
	// count for ObservedWorkspaceWriteItemCountAttemptOrdinal. It is terminal
	// metadata, not a verified filesystem state.
	ObservedWorkspaceWriteItemCount uint64
	// ObservedWorkspaceWriteItemCountAttemptOrdinal identifies the corrective
	// attempt that produced ObservedWorkspaceWriteItemCount. A newer ordinal
	// replaces an older count; repeated observations within one ordinal only
	// increase the count.
	ObservedWorkspaceWriteItemCountAttemptOrdinal LaunchOrdinal
}

type Finalize struct {
	Ref    AttemptRef
	Intent TerminalIntent
}

func (Acknowledge) isCommand()               {}
func (BeginReject) isCommand()               {}
func (BindGroup) isCommand()                 {}
func (CommitGrant) isCommand()               {}
func (RecordReleaseOutcome) isCommand()      {}
func (RecordRelease) isCommand()             {}
func (RecordQuiescence) isCommand()          {}
func (RequestCancel) isCommand()             {}
func (ObserveOutcome) isCommand()            {}
func (CertifyResult) isCommand()             {}
func (RecordFinalAttemptStart) isCommand()   {}
func (RecordFailure) isCommand()             {}
func (RecordTransportFrameDrops) isCommand() {}
func (RecordCancellation) isCommand()        {}
func (Finalize) isCommand()                  {}
