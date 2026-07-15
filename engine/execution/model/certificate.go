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

type ProcessIdentity struct {
	PID               int
	HighResStartToken string
}

func NewProcessIdentity(pid int, highResStartToken string) (ProcessIdentity, error) {
	identity := ProcessIdentity{PID: pid, HighResStartToken: highResStartToken}
	if err := identity.Validate(); err != nil {
		return ProcessIdentity{}, err
	}
	return identity, nil
}

func (identity ProcessIdentity) Validate() error {
	if err := validatePositiveInt("process.pid", identity.PID); err != nil {
		return err
	}
	return validateToken("process.high_res_start_token", identity.HighResStartToken)
}

func (identity ProcessIdentity) Equal(other ProcessIdentity) bool {
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

type CustodyID string

func NewCustodyID(value string) (CustodyID, error) {
	id := CustodyID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id CustodyID) String() string {
	return string(id)
}

func (id CustodyID) Validate() error {
	return validateToken("custody_id", string(id))
}

type LaunchKey struct {
	Attempt AttemptRef
	Ordinal LaunchOrdinal
}

func (key LaunchKey) Validate() error {
	if err := key.Attempt.Validate(); err != nil {
		return err
	}
	return key.Ordinal.Validate()
}

func (key LaunchKey) Equal(other LaunchKey) bool {
	return key.Attempt.Equal(other.Attempt) && key.Ordinal == other.Ordinal
}

type GroupRef struct {
	Version    uint16
	CustodyID  CustodyID
	Launch     LaunchKey
	HostBootID string
	// PIDNamespaceID is empty on platforms where the kernel exposes no PID namespace identity.
	PIDNamespaceID string `json:"PIDNamespaceID,omitempty"`
	PGID           int
	Leader         ProcessIdentity
	Monitor        ProcessIdentity
	RetainedID     string
}

func (ref GroupRef) Validate() error {
	if err := validatePositiveUint16("group.version", ref.Version); err != nil {
		return err
	}
	if err := ref.CustodyID.Validate(); err != nil {
		return err
	}
	if err := ref.Launch.Validate(); err != nil {
		return err
	}
	if err := validateToken("group.host_boot_id", ref.HostBootID); err != nil {
		return err
	}
	if err := validateOptionalToken("group.pid_namespace_id", ref.PIDNamespaceID); err != nil {
		return err
	}
	if err := validatePositiveInt("group.pgid", ref.PGID); err != nil {
		return err
	}
	if err := ref.Leader.Validate(); err != nil {
		return err
	}
	if ref.Leader.PID != ref.PGID {
		return invalid("group.leader.pid", "must match group pgid")
	}
	if err := ref.Monitor.Validate(); err != nil {
		return err
	}
	return validateOptionalToken("group.retained_id", ref.RetainedID)
}

func (ref GroupRef) Equal(other GroupRef) bool {
	return ref.Version == other.Version &&
		ref.CustodyID == other.CustodyID &&
		ref.Launch.Equal(other.Launch) &&
		ref.HostBootID == other.HostBootID &&
		ref.PIDNamespaceID == other.PIDNamespaceID &&
		ref.PGID == other.PGID &&
		ref.Leader.Equal(other.Leader) &&
		ref.Monitor.Equal(other.Monitor) &&
		ref.RetainedID == other.RetainedID
}

func (ref GroupRef) KernelDomain() KernelDomainID {
	return KernelDomainID{HostBootID: ref.HostBootID, PIDNamespaceID: ref.PIDNamespaceID}
}

func (ref GroupRef) SamePhysicalIdentity(other GroupRef) bool {
	return ref.KernelDomain().Equal(other.KernelDomain()) &&
		(ref.PGID == other.PGID || ref.Leader.Equal(other.Leader))
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
	Attempt   AttemptRef
	Ordinal   LaunchOrdinal
	Nonce     LaunchNonce
	GrantedBy BootRef
}

func (grant LaunchGrant) Validate() error {
	if err := grant.Attempt.Validate(); err != nil {
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

type LaunchReleaseFact struct {
	Attempt     AttemptRef
	Ordinal     LaunchOrdinal
	Nonce       LaunchNonce
	Child       ChildIdentity
	ReleasedBy  BootRef
	Observation Evidence
}

func (fact LaunchReleaseFact) Validate() error {
	if err := fact.Attempt.Validate(); err != nil {
		return err
	}
	if err := fact.Ordinal.Validate(); err != nil {
		return err
	}
	if err := fact.Nonce.Validate(); err != nil {
		return err
	}
	if err := fact.Child.Validate(); err != nil {
		return err
	}
	if err := fact.ReleasedBy.Validate(); err != nil {
		return err
	}
	return fact.Observation.Validate()
}

type LaunchConsumed = LaunchReleaseFact

type QuiescenceMethod string

const (
	QuiescenceAlreadyAbsent QuiescenceMethod = "already_absent"
	QuiescenceNaturalExit   QuiescenceMethod = "natural_exit"
	QuiescenceTermKill      QuiescenceMethod = "term_kill"
	QuiescenceHostReboot    QuiescenceMethod = "host_reboot"
)

func (method QuiescenceMethod) Valid() bool {
	switch method {
	case QuiescenceAlreadyAbsent, QuiescenceNaturalExit, QuiescenceTermKill, QuiescenceHostReboot:
		return true
	default:
		return false
	}
}

func (method QuiescenceMethod) Validate() error {
	if !method.Valid() {
		return invalid("quiescence.method", "is unknown")
	}
	return nil
}

type QuiescenceCertificate struct {
	Attempt     AttemptRef
	Ordinal     LaunchOrdinal
	Group       GroupRef
	Method      QuiescenceMethod
	CertifiedBy BootRef
}

func (certificate QuiescenceCertificate) Validate() error {
	if err := certificate.Attempt.Validate(); err != nil {
		return err
	}
	if err := certificate.Ordinal.Validate(); err != nil {
		return err
	}
	if err := certificate.Group.Validate(); err != nil {
		return err
	}
	if err := certificate.Method.Validate(); err != nil {
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
