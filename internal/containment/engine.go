package containment

import (
	"context"
	"errors"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type Engine struct {
	Observer   Observer
	Signaler   Signaler
	Clock      Clock
	Continuity ContinuityWitness
}

type containmentState struct {
	session                  model.ContainmentSession
	continuity               GroupContinuity
	matchingLeaderObservedAt time.Time
}

func (engine Engine) Contain(ctx context.Context, target model.GroupRef, params Params) Outcome {
	if err := target.Validate(); err != nil {
		return UnprovableOutcome(ReasonInvalidInput, "", err)
	}
	params, err := params.normalized()
	if err != nil {
		return UnprovableOutcome(ReasonInvalidInput, "", err)
	}
	if engine.Observer == nil || engine.Signaler == nil {
		return UnprovableOutcome(ReasonInvalidInput, "", errors.New("observer and signaler are required"))
	}
	if engine.Clock == nil {
		engine.Clock = RealClock{}
	}

	observation, observedAt, outcome := engine.observe(ctx, target)
	if outcome.Kind != 0 {
		return outcome
	}
	state := engine.advanceSession(ctx, target, containmentState{}, observation, observedAt)
	decision, outcome := authorize(target, observation, state.session, false)
	if outcome.Kind != 0 {
		return outcome
	}

	switch decision {
	case model.AlreadyAbsent:
		return AbsentOutcome(decision)
	case model.SignalDirectly:
		return engine.signalAuthorized(ctx, target, params, state, decision)
	case model.WaitBoundedForTrustedMonitor:
		return engine.waitForTrustedMonitor(ctx, target, params, state, observation)
	case model.Unprovable:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, decision, nil)
	default:
		return UnprovableOutcome(ReasonUnexpectedDecision, decision, nil)
	}
}

func (engine Engine) signalAuthorized(ctx context.Context, target model.GroupRef, params Params, state containmentState, decision model.ContainmentDecision) Outcome {
	if outcome := engine.signal(ctx, target, SignalTerminate, decision); outcome.Kind != 0 {
		return outcome
	}
	if err := engine.Clock.Sleep(ctx, params.GracePeriod); err != nil {
		return UnprovableOutcome(ReasonContextDone, decision, err)
	}

	observation, observedAt, outcome := engine.observe(ctx, target)
	if outcome.Kind != 0 {
		return outcome
	}
	state = engine.advanceSession(ctx, target, state, observation, observedAt)
	decision, outcome = authorize(target, observation, state.session, false)
	if outcome.Kind != 0 {
		return outcome
	}
	switch decision {
	case model.AlreadyAbsent:
		return AbsentOutcome(decision)
	case model.SignalDirectly:
		if outcome := engine.signal(ctx, target, SignalKill, decision); outcome.Kind != 0 {
			return outcome
		}
		return engine.pollUntilAbsent(ctx, target, params, state, decision)
	case model.WaitBoundedForTrustedMonitor:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, decision, nil)
	case model.Unprovable:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, decision, nil)
	default:
		return UnprovableOutcome(ReasonUnexpectedDecision, decision, nil)
	}
}

func (engine Engine) waitForTrustedMonitor(ctx context.Context, target model.GroupRef, params Params, state containmentState, observation model.ContainmentObservation) Outcome {
	deadline := engine.Clock.Now().Add(params.TrustedMonitorWait)
	for {
		expired := !engine.Clock.Now().Before(deadline)
		decision, outcome := authorize(target, observation, state.session, expired)
		if outcome.Kind != 0 {
			return outcome
		}
		switch decision {
		case model.AlreadyAbsent:
			return AbsentOutcome(decision)
		case model.SignalDirectly:
			return engine.signalAuthorized(ctx, target, params, state, decision)
		case model.Unprovable:
			if expired {
				return UnprovableOutcome(ReasonUnauthorizedWaitExpired, decision, nil)
			}
			return UnprovableOutcome(ReasonAuthorizationUnprovable, decision, nil)
		case model.WaitBoundedForTrustedMonitor:
			if expired {
				return UnprovableOutcome(ReasonUnauthorizedWaitExpired, decision, nil)
			}
		default:
			return UnprovableOutcome(ReasonUnexpectedDecision, decision, nil)
		}

		if err := engine.sleepUntil(ctx, params.TrustedMonitorPollInterval, deadline); err != nil {
			return UnprovableOutcome(ReasonContextDone, model.WaitBoundedForTrustedMonitor, err)
		}
		var observedAt time.Time
		observation, observedAt, outcome = engine.observe(ctx, target)
		if outcome.Kind != 0 {
			return outcome
		}
		state = engine.advanceSession(ctx, target, state, observation, observedAt)
	}
}

func (engine Engine) pollUntilAbsent(ctx context.Context, target model.GroupRef, params Params, state containmentState, decision model.ContainmentDecision) Outcome {
	deadline := engine.Clock.Now().Add(params.PollTimeout)
	for {
		observation, observedAt, outcome := engine.observe(ctx, target)
		if outcome.Kind != 0 {
			return outcome
		}
		state = engine.advanceSession(ctx, target, state, observation, observedAt)
		currentDecision, outcome := authorize(target, observation, state.session, false)
		if outcome.Kind != 0 {
			return outcome
		}
		switch currentDecision {
		case model.AlreadyAbsent:
			return AbsentOutcome(currentDecision)
		case model.SignalDirectly:
		case model.WaitBoundedForTrustedMonitor, model.Unprovable:
			return UnprovableOutcome(ReasonAuthorizationUnprovable, currentDecision, nil)
		default:
			return UnprovableOutcome(ReasonUnexpectedDecision, currentDecision, nil)
		}
		probe, err := engine.Signaler.ProbeGroup(ctx, target)
		if err != nil {
			return UnprovableOutcome(ReasonProbeUnprovable, decision, err)
		}
		switch probe {
		case ProbeLive:
		case ProbeAbsent:
			return UnprovableOutcome(ReasonProbeContradictedObserver, decision, nil)
		case ProbeUnprovable:
			return UnprovableOutcome(ReasonProbeUnprovable, decision, nil)
		default:
			return UnprovableOutcome(ReasonProbeUnprovable, decision, nil)
		}
		if !engine.Clock.Now().Before(deadline) {
			return UnprovableOutcome(ReasonAbsenceDeadlineExceeded, decision, nil)
		}
		if err := engine.sleepUntil(ctx, params.PollInterval, deadline); err != nil {
			return UnprovableOutcome(ReasonContextDone, decision, err)
		}
	}
}

func (engine Engine) signal(ctx context.Context, target model.GroupRef, signal Signal, decision model.ContainmentDecision) Outcome {
	if err := signal.validate(); err != nil {
		return UnprovableOutcome(ReasonInvalidInput, decision, err)
	}
	result, err := engine.Signaler.SignalGroup(ctx, target, signal)
	if err != nil {
		return UnprovableOutcome(ReasonSignalUnprovable, decision, err)
	}
	switch result {
	case SignalDelivered:
		return Outcome{}
	case SignalTargetAbsent:
		return AbsentOutcome(model.AlreadyAbsent)
	case SignalUnprovable:
		return UnprovableOutcome(ReasonSignalUnprovable, decision, nil)
	default:
		return UnprovableOutcome(ReasonSignalUnprovable, decision, nil)
	}
}

func (engine Engine) observe(ctx context.Context, target model.GroupRef) (model.ContainmentObservation, time.Time, Outcome) {
	observation, err := engine.Observer.ObserveGroup(ctx, target)
	if err != nil {
		return model.ContainmentObservation{}, time.Time{}, UnprovableOutcome(ReasonObservationFailed, "", err)
	}
	if err := observation.Validate(); err != nil {
		return model.ContainmentObservation{}, time.Time{}, UnprovableOutcome(ReasonObservationFailed, "", err)
	}
	return observation, engine.Clock.Now(), Outcome{}
}

func (engine Engine) sleepUntil(ctx context.Context, interval time.Duration, deadline time.Time) error {
	now := engine.Clock.Now()
	if !now.Before(deadline) {
		return nil
	}
	if interval <= 0 || now.Add(interval).After(deadline) {
		interval = deadline.Sub(now)
	}
	return engine.Clock.Sleep(ctx, interval)
}

func authorize(target model.GroupRef, observation model.ContainmentObservation, session model.ContainmentSession, deadlineExpired bool) (model.ContainmentDecision, Outcome) {
	decision, err := model.DecideContainmentAuthorization(model.ContainmentAuthorization{
		Group:           target,
		Observation:     observation,
		Session:         session,
		DeadlineExpired: deadlineExpired,
	})
	if err != nil {
		return "", UnprovableOutcome(ReasonAuthorizationFailed, "", err)
	}
	return decision, Outcome{}
}

func (engine Engine) advanceSession(ctx context.Context, target model.GroupRef, state containmentState, observation model.ContainmentObservation, observedAt time.Time) containmentState {
	if observation.Group != model.GroupLive || !target.KernelDomain().ProvablySame(observation.KernelDomainID) {
		return containmentState{}
	}
	switch observation.Leader {
	case model.ProcessIdentityMatching:
		next := containmentState{
			session:                  model.ContainmentSession{BeganFromMatchingLeader: true},
			matchingLeaderObservedAt: observedAt,
		}
		if engine.Continuity != nil {
			next.continuity = engine.Continuity.BeginGroupContinuity(ctx, target, observation, observedAt)
		}
		return next
	case model.ProcessIdentityMissing:
		if state.session.BeganFromMatchingLeader && state.continuity != nil {
			evidence := state.continuity.ConfirmContinuouslyLive(ctx, target, observation, state.matchingLeaderObservedAt, observedAt)
			if !evidence.Covers(target, state.matchingLeaderObservedAt, observedAt) {
				return containmentState{}
			}
			state.session.ContinuouslyObservedLive = true
			return state
		}
	}
	return containmentState{}
}
