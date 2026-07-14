package model

type PermitNonce = LaunchNonce
type QuiescenceReceipt = QuiescenceCertificate
type RetirementReceipt = RetirementCertificate
type ContainmentReceipt = ContainmentCertificate
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

type BindSupervisor struct {
	Ref        AttemptRef
	Supervisor SupervisorIdentity
}

type AuthorizeLaunch struct {
	Ref       AttemptRef
	Ordinal   LaunchOrdinal
	Nonce     PermitNonce
	GrantedBy BootRef
}

type ObserveLaunchConsumed struct {
	Ref         AttemptRef
	Ordinal     LaunchOrdinal
	Child       ChildIdentity
	ConsumedBy  BootRef
	Observation Evidence
}

type ObserveLaunchQuiescent struct {
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

type CertifyRetirement struct {
	Ref     AttemptRef
	Receipt RetirementReceipt
}

type CertifyContainment struct {
	Ref     AttemptRef
	Receipt ContainmentReceipt
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

func (Acknowledge) isCommand()            {}
func (BeginReject) isCommand()            {}
func (BindSupervisor) isCommand()         {}
func (AuthorizeLaunch) isCommand()        {}
func (ObserveLaunchConsumed) isCommand()  {}
func (ObserveLaunchQuiescent) isCommand() {}
func (RequestCancel) isCommand()          {}
func (ObserveOutcome) isCommand()         {}
func (CertifyRetirement) isCommand()      {}
func (CertifyContainment) isCommand()     {}
func (CertifyResult) isCommand()          {}
func (Finalize) isCommand()               {}
