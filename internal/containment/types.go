package containment

import (
	"context"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// Observer reads the target group and process identities without mutating the
// system. Cannot-tell observations must be returned as model unknown states or
// errors; they must never be reported as absent.
type Observer interface {
	ObserveGroup(ctx context.Context, target model.GroupRef) (model.ContainmentObservation, error)
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

// Authorizer consumes the P0C containment authorization decision.
type Authorizer interface {
	AuthorizeContainment(input model.ContainmentAuthorization) (model.ContainmentDecision, error)
}

// ModelAuthorizer delegates to model.DecideContainmentAuthorization.
type ModelAuthorizer struct{}

func (ModelAuthorizer) AuthorizeContainment(input model.ContainmentAuthorization) (model.ContainmentDecision, error) {
	return model.DecideContainmentAuthorization(input)
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
	InitialSession             model.ContainmentSession
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
	if params.PollInterval == 0 {
		params.PollInterval = params.PollTimeout
	}
	if params.TrustedMonitorPollInterval == 0 {
		params.TrustedMonitorPollInterval = params.TrustedMonitorWait
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
	Kind     OutcomeKind
	Reason   UnprovableReason
	Decision model.ContainmentDecision
	Err      error
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
