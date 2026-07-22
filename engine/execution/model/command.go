package model

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
	Ref     AttemptRef
	Outcome Outcome
}

type CertifyResult struct {
	Ref     AttemptRef
	Receipt ResultReceipt
}

type TerminalIntent struct {
	Outcome   Outcome
	Cause     TerminalCause
	DerivedBy BootRef
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
func (Finalize) isCommand()             {}
