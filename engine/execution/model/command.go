package model

import "github.com/charlesnpx/agentbus/engine"

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

// RecordFailure attaches the first observed terminal-failure explanation to a
// job without changing its outcome or terminal-state proof.
type RecordFailure struct {
	JobID  JobID
	Class  engine.FailureClass
	Reason string
}

type TerminalIntent struct {
	Outcome   Outcome
	Cause     TerminalCause
	DerivedBy BootRef
	Contract  *engine.ContractStamp
}

type Finalize struct {
	Ref    AttemptRef
	Intent TerminalIntent
}

func (Acknowledge) isCommand()          {}
func (BeginReject) isCommand()          {}
func (BindGroup) isCommand()            {}
func (CommitGrant) isCommand()          {}
func (RecordReleaseOutcome) isCommand() {}
func (RecordRelease) isCommand()        {}
func (RecordQuiescence) isCommand()     {}
func (RequestCancel) isCommand()        {}
func (ObserveOutcome) isCommand()       {}
func (CertifyResult) isCommand()        {}
func (RecordFailure) isCommand()        {}
func (Finalize) isCommand()             {}
