package containment

import (
	"context"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// Observer reads the target group and process identities without mutating the
// system. Cannot-tell observations must be returned as model unknown states or
// errors; they must never be reported as absent.
type Observer interface {
	ObserveGroup(ctx context.Context, target model.GroupRef) (model.ContainmentObservation, error)
}

// RetainedGroupObject acquires a held capability for the durable group-lifetime
// object identified by GroupRef.RetainedID. The held capability, not a later
// reconstruction from PID/PGID samples, is the provenance for membership proof,
// absence proof, and teardown.
type RetainedGroupObject interface {
	AcquireRetainedGroup(ctx context.Context, target model.GroupRef, acquiredAt time.Time) (RetainedGroupCapability, error)
}

// RetainedGroupCapability is a held retained-object capability. Backends report
// facts about the acquired object only; the containment package records the
// acquisition window and mints sealed retained-object evidence internally.
type RetainedGroupCapability interface {
	Identity() RetainedGroupIdentity
	Membership(ctx context.Context) (RetainedGroupMembership, error)
	StillHeld(ctx context.Context) (bool, error)
	SignalTerm(ctx context.Context) (SignalResult, error)
	Kill(ctx context.Context) (SignalResult, error)
	Release() error
}

// RetainedGroupCleanup is implemented by retained capabilities that can remove
// their held object after absence has been proven. Cleanup status is separate
// from absence proof, so callers must surface cleanup errors instead of treating
// them as successful teardown.
type RetainedGroupCleanup interface {
	Remove(ctx context.Context) error
}

type RetainedGroupIdentity struct {
	RetainedID     string
	KernelDomainID model.KernelDomainID
}

func (identity RetainedGroupIdentity) validate() error {
	if err := validateRetainedEvidenceID(identity.RetainedID); err != nil {
		return err
	}
	return identity.KernelDomainID.Validate()
}

func (identity RetainedGroupIdentity) matches(target model.GroupRef) bool {
	if err := target.Validate(); err != nil {
		return false
	}
	if err := identity.validate(); err != nil {
		return false
	}
	return identity.RetainedID == target.RetainedID && identity.KernelDomainID.ProvablySame(target.KernelDomain())
}

type RetainedGroupMembership uint8

const (
	RetainedMembershipUnknown RetainedGroupMembership = iota + 1
	RetainedMembershipPresent
	RetainedMembershipEmpty
)

func (membership RetainedGroupMembership) validate() error {
	switch membership {
	case RetainedMembershipUnknown, RetainedMembershipPresent, RetainedMembershipEmpty:
		return nil
	default:
		return fmt.Errorf("retained group membership is unknown")
	}
}

// RetainedGroupEvidence is sealed retained-object evidence. It is built by a
// held retained capability, and the engine validates retained identity and
// interval coverage before passing a proof state to the model.
type RetainedGroupEvidence struct {
	identity   RetainedGroupIdentity
	begin      time.Time
	end        time.Time
	membership RetainedGroupMembership
}

func newRetainedGroupEvidence(identity RetainedGroupIdentity, begin, end time.Time, membership RetainedGroupMembership) (RetainedGroupEvidence, error) {
	evidence := RetainedGroupEvidence{
		identity:   identity,
		begin:      begin,
		end:        end,
		membership: membership,
	}
	if err := evidence.validate(); err != nil {
		return RetainedGroupEvidence{}, err
	}
	return evidence, nil
}

func (evidence RetainedGroupEvidence) ProofFor(target model.GroupRef, begin, end time.Time) model.RetainedObjectProof {
	if err := target.Validate(); err != nil {
		return model.RetainedObjectProofNone
	}
	if target.RetainedID == "" {
		return model.RetainedObjectProofNone
	}
	if begin.IsZero() || end.IsZero() || end.Before(begin) {
		return model.RetainedObjectProofNone
	}
	if err := evidence.validate(); err != nil {
		return model.RetainedObjectProofNone
	}
	if !evidence.identity.matches(target) || evidence.begin.After(begin) || evidence.end.Before(end) {
		return model.RetainedObjectProofNone
	}
	switch evidence.membership {
	case RetainedMembershipPresent:
		return model.RetainedObjectProofMembersPresent
	case RetainedMembershipEmpty:
		return model.RetainedObjectProofEmpty
	case RetainedMembershipUnknown:
		return model.RetainedObjectProofUnknown
	default:
		return model.RetainedObjectProofNone
	}
}

func (evidence RetainedGroupEvidence) validate() error {
	if err := evidence.identity.validate(); err != nil {
		return err
	}
	if evidence.begin.IsZero() {
		return fmt.Errorf("retained group evidence begin must be set")
	}
	if evidence.end.IsZero() {
		return fmt.Errorf("retained group evidence end must be set")
	}
	if evidence.end.Before(evidence.begin) {
		return fmt.Errorf("retained group evidence end precedes begin")
	}
	return evidence.membership.validate()
}

func validateRetainedEvidenceID(value string) error {
	const maxTokenBytes = 256
	if value == "" {
		return fmt.Errorf("retained group evidence retained id is required")
	}
	if len(value) > maxTokenBytes {
		return fmt.Errorf("retained group evidence retained id is too long")
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("retained group evidence retained id must be valid UTF-8")
	}
	for _, r := range value {
		if r <= ' ' || r == 0x7f {
			return fmt.Errorf("retained group evidence retained id must not contain whitespace or control characters")
		}
	}
	return nil
}

// ContinuityWitness can bind a live matching-leader observation to a capability
// that later attests the exact group never became absent. A nil witness means
// the engine has no continuity capability and must fail closed on leader-missing
// escalation.
type ContinuityWitness interface {
	BeginGroupContinuity(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, observedAt time.Time) GroupContinuity
}

// GroupContinuity confirms uninterrupted liveness of the exact target group
// since the matching-leader observation that created it.
type GroupContinuity interface {
	ConfirmContinuouslyLive(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, begin, end time.Time) GroupContinuityEvidence
}

// GroupContinuityEvidence is sealed continuity evidence: callers can only build
// it through the constructor, and the engine still validates the target and
// interval before trusting it.
type GroupContinuityEvidence struct {
	group model.GroupRef
	begin time.Time
	end   time.Time
}

func NewGroupContinuityEvidence(group model.GroupRef, begin, end time.Time) (GroupContinuityEvidence, error) {
	evidence := GroupContinuityEvidence{group: group, begin: begin, end: end}
	if err := evidence.validate(); err != nil {
		return GroupContinuityEvidence{}, err
	}
	return evidence, nil
}

func (evidence GroupContinuityEvidence) Covers(target model.GroupRef, begin, end time.Time) bool {
	if err := target.Validate(); err != nil {
		return false
	}
	if begin.IsZero() || end.IsZero() || end.Before(begin) {
		return false
	}
	if err := evidence.validate(); err != nil {
		return false
	}
	return evidence.group.Equal(target) &&
		!evidence.begin.After(begin) &&
		!evidence.end.Before(end)
}

func (evidence GroupContinuityEvidence) validate() error {
	if err := evidence.group.Validate(); err != nil {
		return err
	}
	if evidence.begin.IsZero() {
		return fmt.Errorf("continuity evidence begin must be set")
	}
	if evidence.end.IsZero() {
		return fmt.Errorf("continuity evidence end must be set")
	}
	if evidence.end.Before(evidence.begin) {
		return fmt.Errorf("continuity evidence end precedes begin")
	}
	return nil
}

// Signaler sends signals only to the exact target process group and can probe
// group existence. Ambiguous syscall results must be reported as unprovable.
type Signaler interface {
	SignalGroup(ctx context.Context, target model.GroupRef, signal Signal) (SignalResult, error)
	ProbeGroup(ctx context.Context, target model.GroupRef) (ProbeResult, error)
}

// Clock makes grace periods and polling deadlines deterministic in tests.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type Signal uint8

const (
	SignalTerminate Signal = iota + 1
	SignalKill
)

func (signal Signal) String() string {
	switch signal {
	case SignalTerminate:
		return "TERM"
	case SignalKill:
		return "KILL"
	default:
		return "unknown"
	}
}

func (signal Signal) validate() error {
	switch signal {
	case SignalTerminate, SignalKill:
		return nil
	default:
		return fmt.Errorf("containment signal is unknown")
	}
}

type SignalResult uint8

const (
	SignalDelivered SignalResult = iota + 1
	SignalTargetAbsent
	SignalUnprovable
)

type ProbeResult uint8

const (
	ProbeLive ProbeResult = iota + 1
	ProbeAbsent
	ProbeUnprovable
)

type Params struct {
	GracePeriod                time.Duration
	PollInterval               time.Duration
	PollTimeout                time.Duration
	TrustedMonitorWait         time.Duration
	TrustedMonitorPollInterval time.Duration
	CoherenceRereadLimit       int
	CoherenceRereadInterval    time.Duration
}

const (
	defaultCoherenceRereadLimit    = 2
	defaultCoherenceRereadInterval = 20 * time.Millisecond
)

func (params Params) Validate() error {
	_, err := params.normalized()
	return err
}

func (params Params) normalized() (Params, error) {
	if params.GracePeriod < 0 {
		return Params{}, fmt.Errorf("grace period must be non-negative")
	}
	if params.PollTimeout < 0 {
		return Params{}, fmt.Errorf("poll timeout must be non-negative")
	}
	if params.PollInterval < 0 {
		return Params{}, fmt.Errorf("poll interval must be non-negative")
	}
	if params.TrustedMonitorWait < 0 {
		return Params{}, fmt.Errorf("trusted monitor wait must be non-negative")
	}
	if params.TrustedMonitorPollInterval < 0 {
		return Params{}, fmt.Errorf("trusted monitor poll interval must be non-negative")
	}
	if params.CoherenceRereadLimit < 0 {
		return Params{}, fmt.Errorf("coherence reread limit must be non-negative")
	}
	if params.CoherenceRereadInterval < 0 {
		return Params{}, fmt.Errorf("coherence reread interval must be non-negative")
	}
	if params.PollInterval == 0 {
		params.PollInterval = params.PollTimeout
	}
	if params.TrustedMonitorPollInterval == 0 {
		params.TrustedMonitorPollInterval = params.TrustedMonitorWait
	}
	if params.CoherenceRereadLimit == 0 {
		params.CoherenceRereadLimit = defaultCoherenceRereadLimit
	}
	if params.CoherenceRereadInterval == 0 {
		params.CoherenceRereadInterval = defaultCoherenceRereadInterval
	}
	return params, nil
}

type OutcomeKind uint8

const (
	OutcomeAbsent OutcomeKind = iota + 1
	OutcomeUnprovable
)

type UnprovableReason string

const (
	ReasonNone                      UnprovableReason = ""
	ReasonInvalidInput              UnprovableReason = "invalid_input"
	ReasonContextDone               UnprovableReason = "context_done"
	ReasonObservationFailed         UnprovableReason = "observation_failed"
	ReasonAuthorizationFailed       UnprovableReason = "authorization_failed"
	ReasonAuthorizationUnprovable   UnprovableReason = "authorization_unprovable"
	ReasonUnauthorizedWaitExpired   UnprovableReason = "unauthorized_wait_expired"
	ReasonSignalUnprovable          UnprovableReason = "signal_unprovable"
	ReasonProbeUnprovable           UnprovableReason = "probe_unprovable"
	ReasonProbeContradictedObserver UnprovableReason = "probe_contradicted_observer"
	ReasonAbsenceDeadlineExceeded   UnprovableReason = "absence_deadline_exceeded"
	ReasonUnexpectedDecision        UnprovableReason = "unexpected_decision"
)

// Outcome is deliberately not an attestation or quiescence certificate.
type Outcome struct {
	Kind       OutcomeKind
	Reason     UnprovableReason
	Decision   model.ContainmentDecision
	Err        error
	CleanupErr error
}

func AbsentOutcome(decision model.ContainmentDecision) Outcome {
	return Outcome{Kind: OutcomeAbsent, Decision: decision}
}

func UnprovableOutcome(reason UnprovableReason, decision model.ContainmentDecision, err error) Outcome {
	return Outcome{Kind: OutcomeUnprovable, Reason: reason, Decision: decision, Err: err}
}

func (outcome Outcome) Absent() bool {
	return outcome.Kind == OutcomeAbsent
}

func (outcome Outcome) Unprovable() bool {
	return outcome.Kind == OutcomeUnprovable
}
