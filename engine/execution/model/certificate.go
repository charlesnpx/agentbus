package model

type LaunchNonce string

func NewLaunchNonce(value string) (LaunchNonce, error) {
	nonce := LaunchNonce(value)
	if err := nonce.Validate(); err != nil {
		return "", err
	}
	return nonce, nil
}

func (nonce LaunchNonce) String() string {
	return string(nonce)
}

func (nonce LaunchNonce) Validate() error {
	return validateToken("launch_nonce", string(nonce))
}

type Evidence struct {
	Kind   string
	Detail string
}

func NewEvidence(kind, detail string) (Evidence, error) {
	evidence := Evidence{Kind: kind, Detail: detail}
	if err := evidence.Validate(); err != nil {
		return Evidence{}, err
	}
	return evidence, nil
}

func (evidence Evidence) Present() bool {
	return evidence.Kind != "" && evidence.Detail != ""
}

func (evidence Evidence) Validate() error {
	if err := validateToken("evidence.kind", evidence.Kind); err != nil {
		return err
	}
	return validateText("evidence.detail", evidence.Detail, 4096)
}

type ChildIdentity struct {
	PID               int
	HighResStartToken string
}

func NewChildIdentity(pid int, highResStartToken string) (ChildIdentity, error) {
	identity := ChildIdentity{PID: pid, HighResStartToken: highResStartToken}
	if err := identity.Validate(); err != nil {
		return ChildIdentity{}, err
	}
	return identity, nil
}

func (identity ChildIdentity) Validate() error {
	if err := validatePositiveInt("child.pid", identity.PID); err != nil {
		return err
	}
	return validateToken("child.high_res_start_token", identity.HighResStartToken)
}

func (identity ChildIdentity) Equal(other ChildIdentity) bool {
	return identity.PID == other.PID && identity.HighResStartToken == other.HighResStartToken
}

type SupervisorIdentity struct {
	PGID               int
	LeaderPID          int
	HighResStartToken  string
	PlatformRetainedID string
}

func NewSupervisorIdentity(pgid, leaderPID int, highResStartToken, platformRetainedID string) (SupervisorIdentity, error) {
	identity := SupervisorIdentity{
		PGID:               pgid,
		LeaderPID:          leaderPID,
		HighResStartToken:  highResStartToken,
		PlatformRetainedID: platformRetainedID,
	}
	if err := identity.Validate(); err != nil {
		return SupervisorIdentity{}, err
	}
	return identity, nil
}

func (identity SupervisorIdentity) Validate() error {
	if err := validatePositiveInt("supervisor.pgid", identity.PGID); err != nil {
		return err
	}
	if err := validatePositiveInt("supervisor.leader_pid", identity.LeaderPID); err != nil {
		return err
	}
	if err := validateToken("supervisor.high_res_start_token", identity.HighResStartToken); err != nil {
		return err
	}
	return validateOptionalToken("supervisor.platform_retained_id", identity.PlatformRetainedID)
}

func (identity SupervisorIdentity) Equal(other SupervisorIdentity) bool {
	return identity.PGID == other.PGID &&
		identity.LeaderPID == other.LeaderPID &&
		identity.HighResStartToken == other.HighResStartToken &&
		identity.PlatformRetainedID == other.PlatformRetainedID
}

type AcknowledgementFact struct {
	Attempt        AttemptRef
	AcknowledgedBy BootRef
}

func (fact AcknowledgementFact) Validate() error {
	if err := fact.Attempt.Validate(); err != nil {
		return err
	}
	return fact.AcknowledgedBy.Validate()
}

type CancelFact struct {
	JobID       JobID
	RequestedBy BootRef
}

func (fact CancelFact) Validate() error {
	if err := fact.JobID.Validate(); err != nil {
		return err
	}
	return fact.RequestedBy.Validate()
}

type OutcomeFact struct {
	Attempt AttemptRef
	Outcome Outcome
}

func (fact OutcomeFact) Validate() error {
	if err := fact.Attempt.Validate(); err != nil {
		return err
	}
	return fact.Outcome.ValidateTerminal()
}

type LaunchGrant struct {
	Attempt    AttemptRef
	Supervisor SupervisorIdentity
	Ordinal    LaunchOrdinal
	Nonce      LaunchNonce
	GrantedBy  BootRef
}

func (grant LaunchGrant) Validate() error {
	if err := grant.Attempt.Validate(); err != nil {
		return err
	}
	if err := grant.Supervisor.Validate(); err != nil {
		return err
	}
	if err := grant.Ordinal.Validate(); err != nil {
		return err
	}
	if err := grant.Nonce.Validate(); err != nil {
		return err
	}
	return grant.GrantedBy.Validate()
}

type LaunchConsumed struct {
	Attempt     AttemptRef
	Ordinal     LaunchOrdinal
	Nonce       LaunchNonce
	Child       ChildIdentity
	ConsumedBy  BootRef
	Observation Evidence
}

func (consumed LaunchConsumed) Validate() error {
	if err := consumed.Attempt.Validate(); err != nil {
		return err
	}
	if err := consumed.Ordinal.Validate(); err != nil {
		return err
	}
	if err := consumed.Nonce.Validate(); err != nil {
		return err
	}
	if err := consumed.Child.Validate(); err != nil {
		return err
	}
	if err := consumed.ConsumedBy.Validate(); err != nil {
		return err
	}
	return consumed.Observation.Validate()
}

type QuiescenceCertificate struct {
	Attempt     AttemptRef
	Ordinal     LaunchOrdinal
	Child       ChildIdentity
	ChildExited Evidence
	GroupEmpty  Evidence
	CertifiedBy BootRef
}

func (certificate QuiescenceCertificate) Validate() error {
	if err := certificate.Attempt.Validate(); err != nil {
		return err
	}
	if err := certificate.Ordinal.Validate(); err != nil {
		return err
	}
	if err := certificate.Child.Validate(); err != nil {
		return err
	}
	if err := certificate.ChildExited.Validate(); err != nil {
		return err
	}
	if err := certificate.GroupEmpty.Validate(); err != nil {
		return err
	}
	return certificate.CertifiedBy.Validate()
}

type ContainmentCertificate struct {
	Attempt      AttemptRef
	Supervisor   SupervisorIdentity
	Signal       Evidence
	Verification Evidence
	CertifiedBy  BootRef
}

func (certificate ContainmentCertificate) Validate() error {
	if err := certificate.Attempt.Validate(); err != nil {
		return err
	}
	if err := certificate.Supervisor.Validate(); err != nil {
		return err
	}
	if err := certificate.Signal.Validate(); err != nil {
		return err
	}
	if err := certificate.Verification.Validate(); err != nil {
		return err
	}
	return certificate.CertifiedBy.Validate()
}

type RetirementCertificate struct {
	Attempt       AttemptRef
	Supervisor    SupervisorIdentity
	ControlClosed Evidence
	WorkerExited  Evidence
	GroupEmpty    Evidence
	CertifiedBy   BootRef
}

func (certificate RetirementCertificate) Validate() error {
	if err := certificate.Attempt.Validate(); err != nil {
		return err
	}
	if err := certificate.Supervisor.Validate(); err != nil {
		return err
	}
	if err := certificate.ControlClosed.Validate(); err != nil {
		return err
	}
	if err := certificate.WorkerExited.Validate(); err != nil {
		return err
	}
	if err := certificate.GroupEmpty.Validate(); err != nil {
		return err
	}
	return certificate.CertifiedBy.Validate()
}

type ResultRef struct {
	Path   string
	Digest string
	Bytes  int64
}

func (result ResultRef) Validate() error {
	if err := validateText("result.path", result.Path, 4096); err != nil {
		return err
	}
	if err := validateToken("result.digest", result.Digest); err != nil {
		return err
	}
	return validateNonNegativeInt64("result.bytes", result.Bytes)
}

type ResultCertificate struct {
	JobID       JobID
	Result      ResultRef
	DirSynced   Evidence
	CertifiedBy BootRef
}

func (certificate ResultCertificate) Validate() error {
	if err := certificate.JobID.Validate(); err != nil {
		return err
	}
	if err := certificate.Result.Validate(); err != nil {
		return err
	}
	if err := certificate.DirSynced.Validate(); err != nil {
		return err
	}
	return certificate.CertifiedBy.Validate()
}

type TerminalCertificate struct {
	JobID               JobID
	Attempt             AttemptRef
	Outcome             Outcome
	Proof               TerminalProof
	Cause               TerminalCause
	DerivedFromRevision uint64
	DerivedBy           BootRef
	Result              *ResultRef
}

func (certificate TerminalCertificate) Validate() error {
	if err := certificate.JobID.Validate(); err != nil {
		return err
	}
	if err := certificate.Attempt.Validate(); err != nil {
		return err
	}
	if err := validateJobField("terminal.attempt.job_id", certificate.Attempt.JobID, certificate.JobID); err != nil {
		return err
	}
	if err := certificate.Outcome.ValidateTerminal(); err != nil {
		return err
	}
	if err := certificate.Proof.Validate(); err != nil {
		return err
	}
	if err := certificate.Cause.Validate(); err != nil {
		return err
	}
	if err := validatePositiveUint64("terminal.derived_from_revision", certificate.DerivedFromRevision); err != nil {
		return err
	}
	if err := certificate.DerivedBy.Validate(); err != nil {
		return err
	}
	if certificate.Result != nil {
		if err := certificate.Result.Validate(); err != nil {
			return err
		}
	}
	if completionOutcome(certificate.Outcome) && certificate.Result == nil {
		return invalid("terminal.result", "is required for completed outcomes")
	}
	return nil
}
