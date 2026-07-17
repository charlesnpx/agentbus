package containment

import (
	"context"
	"errors"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type Engine struct {
	Observer       Observer
	Signaler       Signaler
	Clock          Clock
	Continuity     ContinuityWitness
	RetainedObject RetainedGroupObject
}

type containmentState struct {
	session                  model.ContainmentSession
	continuity               GroupContinuity
	retainedObject           RetainedGroupCapability
	retainedAcquisitionErr   error
	operationBegin           time.Time
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

	state, outcome := engine.acquireRetainedObject(ctx, target)
	if outcome.Kind != 0 {
		return outcome
	}
	defer state.releaseRetainedObject()
	observation, observedAt, outcome := engine.observe(ctx, target)
	if outcome.Kind != 0 {
		return outcome
	}
	state = engine.advanceSession(ctx, target, state, observation, observedAt)
	authorization, outcome := engine.authorize(ctx, target, observation, state, false, observedAt)
	if outcome.Kind != 0 {
		return outcome
	}

	switch authorization.Decision {
	case model.AlreadyAbsent:
		return AbsentOutcome(authorization.Decision)
	case model.SignalDirectly:
		return engine.signalAuthorized(ctx, target, params, state, authorization)
	case model.WaitBoundedForTrustedMonitor:
		return engine.waitForTrustedMonitor(ctx, target, params, state, observation)
	case model.Unprovable:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, authorization.Decision, nil)
	default:
		return UnprovableOutcome(ReasonUnexpectedDecision, authorization.Decision, nil)
	}
}

func (engine Engine) acquireRetainedObject(ctx context.Context, target model.GroupRef) (containmentState, Outcome) {
	if !retainedObjectRequired(target) {
		return containmentState{}, Outcome{}
	}
	acquiredAt := engine.Clock.Now()
	if target.RetainedID == "" {
		err := errors.New("required retained object id is missing")
		return containmentState{retainedAcquisitionErr: err, operationBegin: acquiredAt}, UnprovableOutcome(ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	if engine.RetainedObject == nil {
		err := errors.New("required retained object acquisition provider is missing")
		return containmentState{retainedAcquisitionErr: err, operationBegin: acquiredAt}, UnprovableOutcome(ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	retainedObject, err := engine.RetainedObject.AcquireRetainedGroup(ctx, target, acquiredAt)
	if err != nil {
		return containmentState{retainedAcquisitionErr: err, operationBegin: acquiredAt}, UnprovableOutcome(ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	if retainedObject == nil {
		err := errors.New("required retained object acquisition returned nil capability")
		return containmentState{retainedAcquisitionErr: err, operationBegin: acquiredAt}, UnprovableOutcome(ReasonAuthorizationUnprovable, model.Unprovable, err)
	}
	return containmentState{retainedObject: retainedObject, operationBegin: acquiredAt}, Outcome{}
}

func retainedObjectRequired(target model.GroupRef) bool {
	return target.RetainedDomainID != "" || target.RetainedDomainState == model.RetainedDomainKnown
}

func (engine Engine) signalAuthorized(ctx context.Context, target model.GroupRef, params Params, state containmentState, authorization model.ContainmentAuthorizationResult) Outcome {
	if outcome := engine.signal(ctx, target, state, authorization, SignalTerminate); outcome.Kind != 0 {
		return outcome
	}
	if err := engine.Clock.Sleep(ctx, params.GracePeriod); err != nil {
		return UnprovableOutcome(ReasonContextDone, authorization.Decision, err)
	}

	observation, observedAt, outcome := engine.observe(ctx, target)
	if outcome.Kind != 0 {
		return outcome
	}
	state = engine.advanceSession(ctx, target, state, observation, observedAt)
	authorization, outcome = engine.authorize(ctx, target, observation, state, false, observedAt)
	if outcome.Kind != 0 {
		return outcome
	}
	switch authorization.Decision {
	case model.AlreadyAbsent:
		return AbsentOutcome(authorization.Decision)
	case model.SignalDirectly:
		if outcome := engine.signal(ctx, target, state, authorization, SignalKill); outcome.Kind != 0 {
			return outcome
		}
		return engine.pollUntilAbsent(ctx, target, params, state, authorization)
	case model.WaitBoundedForTrustedMonitor:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, authorization.Decision, nil)
	case model.Unprovable:
		return UnprovableOutcome(ReasonAuthorizationUnprovable, authorization.Decision, nil)
	default:
		return UnprovableOutcome(ReasonUnexpectedDecision, authorization.Decision, nil)
	}
}

func (engine Engine) waitForTrustedMonitor(ctx context.Context, target model.GroupRef, params Params, state containmentState, observation model.ContainmentObservation) Outcome {
	deadline := engine.Clock.Now().Add(params.TrustedMonitorWait)
	for {
		expired := !engine.Clock.Now().Before(deadline)
		authorization, outcome := engine.authorize(ctx, target, observation, state, expired, engine.Clock.Now())
		if outcome.Kind != 0 {
			return outcome
		}
		switch authorization.Decision {
		case model.AlreadyAbsent:
			return AbsentOutcome(authorization.Decision)
		case model.SignalDirectly:
			return engine.signalAuthorized(ctx, target, params, state, authorization)
		case model.Unprovable:
			if expired {
				return UnprovableOutcome(ReasonUnauthorizedWaitExpired, authorization.Decision, nil)
			}
			return UnprovableOutcome(ReasonAuthorizationUnprovable, authorization.Decision, nil)
		case model.WaitBoundedForTrustedMonitor:
			if expired {
				return UnprovableOutcome(ReasonUnauthorizedWaitExpired, authorization.Decision, nil)
			}
		default:
			return UnprovableOutcome(ReasonUnexpectedDecision, authorization.Decision, nil)
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

func (engine Engine) pollUntilAbsent(ctx context.Context, target model.GroupRef, params Params, state containmentState, authorization model.ContainmentAuthorizationResult) Outcome {
	deadline := engine.Clock.Now().Add(params.PollTimeout)
	for {
		observation, observedAt, outcome := engine.observe(ctx, target)
		if outcome.Kind != 0 {
			return outcome
		}
		state = engine.advanceSession(ctx, target, state, observation, observedAt)
		currentAuthorization, outcome := engine.authorize(ctx, target, observation, state, false, observedAt)
		if outcome.Kind != 0 {
			return outcome
		}
		switch currentAuthorization.Decision {
		case model.AlreadyAbsent:
			return AbsentOutcome(currentAuthorization.Decision)
		case model.SignalDirectly:
		case model.WaitBoundedForTrustedMonitor, model.Unprovable:
			return UnprovableOutcome(ReasonAuthorizationUnprovable, currentAuthorization.Decision, nil)
		default:
			return UnprovableOutcome(ReasonUnexpectedDecision, currentAuthorization.Decision, nil)
		}
		probe, err := engine.probe(ctx, target, state, currentAuthorization)
		if err != nil {
			return UnprovableOutcome(ReasonProbeUnprovable, authorization.Decision, err)
		}
		switch probe {
		case ProbeLive:
		case ProbeAbsent:
			if currentAuthorization.Basis == model.ContainmentBasisRetainedObject {
				return AbsentOutcome(model.AlreadyAbsent)
			}
			return UnprovableOutcome(ReasonProbeContradictedObserver, authorization.Decision, nil)
		case ProbeUnprovable:
			return UnprovableOutcome(ReasonProbeUnprovable, authorization.Decision, nil)
		default:
			return UnprovableOutcome(ReasonProbeUnprovable, authorization.Decision, nil)
		}
		if !engine.Clock.Now().Before(deadline) {
			return UnprovableOutcome(ReasonAbsenceDeadlineExceeded, authorization.Decision, nil)
		}
		if err := engine.sleepUntil(ctx, params.PollInterval, deadline); err != nil {
			return UnprovableOutcome(ReasonContextDone, authorization.Decision, err)
		}
	}
}

func (engine Engine) signal(ctx context.Context, target model.GroupRef, state containmentState, authorization model.ContainmentAuthorizationResult, signal Signal) Outcome {
	if err := signal.validate(); err != nil {
		return UnprovableOutcome(ReasonInvalidInput, authorization.Decision, err)
	}
	result, err := engine.signalGroup(ctx, target, state, authorization, signal)
	if err != nil {
		return UnprovableOutcome(ReasonSignalUnprovable, authorization.Decision, err)
	}
	switch result {
	case SignalDelivered:
		return Outcome{}
	case SignalTargetAbsent:
		if authorization.Basis == model.ContainmentBasisRetainedObject {
			proof, valid := engine.retainedObjectProof(ctx, target, state, engine.Clock.Now())
			if !valid || proof != model.RetainedObjectProofEmpty {
				return UnprovableOutcome(ReasonSignalUnprovable, authorization.Decision, nil)
			}
		}
		return AbsentOutcome(model.AlreadyAbsent)
	case SignalUnprovable:
		return UnprovableOutcome(ReasonSignalUnprovable, authorization.Decision, nil)
	default:
		return UnprovableOutcome(ReasonSignalUnprovable, authorization.Decision, nil)
	}
}

func (engine Engine) signalGroup(ctx context.Context, target model.GroupRef, state containmentState, authorization model.ContainmentAuthorizationResult, signal Signal) (SignalResult, error) {
	switch authorization.Basis {
	case model.ContainmentBasisLeader:
		return engine.Signaler.SignalGroup(ctx, target, signal)
	case model.ContainmentBasisRetainedObject:
		if state.retainedObject == nil {
			return SignalUnprovable, errors.New("retained object capability is missing")
		}
		switch signal {
		case SignalTerminate:
			return state.retainedObject.SignalTerm(ctx)
		case SignalKill:
			return state.retainedObject.Kill(ctx)
		default:
			return SignalUnprovable, errors.New("retained object signal is unknown")
		}
	default:
		return SignalUnprovable, errors.New("signal authority basis is missing")
	}
}

func (engine Engine) probe(ctx context.Context, target model.GroupRef, state containmentState, authorization model.ContainmentAuthorizationResult) (ProbeResult, error) {
	switch authorization.Basis {
	case model.ContainmentBasisLeader:
		return engine.Signaler.ProbeGroup(ctx, target)
	case model.ContainmentBasisRetainedObject:
		if state.retainedObject == nil {
			return ProbeUnprovable, errors.New("retained object capability is missing")
		}
		proof, valid := engine.retainedObjectProof(ctx, target, state, engine.Clock.Now())
		if !valid {
			return ProbeUnprovable, nil
		}
		switch proof {
		case model.RetainedObjectProofMembersPresent:
			return ProbeLive, nil
		case model.RetainedObjectProofEmpty:
			return ProbeAbsent, nil
		default:
			return ProbeUnprovable, nil
		}
	default:
		return ProbeUnprovable, errors.New("probe authority basis is missing")
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

func (engine Engine) authorize(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, state containmentState, deadlineExpired bool, observedAt time.Time) (model.ContainmentAuthorizationResult, Outcome) {
	retainedProof, retainedValid := engine.retainedObjectProof(ctx, target, state, observedAt)
	if !retainedValid {
		return model.ContainmentAuthorizationResult{}, UnprovableOutcome(ReasonAuthorizationUnprovable, model.Unprovable, nil)
	}
	authorization, err := model.DecideContainmentAuthorizationWithBasis(model.ContainmentAuthorization{
		Group:           target,
		Observation:     observation,
		Session:         state.session,
		RetainedObject:  retainedProof,
		DeadlineExpired: deadlineExpired,
	})
	if err != nil {
		return model.ContainmentAuthorizationResult{}, UnprovableOutcome(ReasonAuthorizationFailed, "", err)
	}
	return authorization, Outcome{}
}

func (engine Engine) retainedObjectProof(ctx context.Context, target model.GroupRef, state containmentState, observedAt time.Time) (model.RetainedObjectProof, bool) {
	if !retainedObjectRequired(target) {
		return model.RetainedObjectProofNone, true
	}
	if state.retainedAcquisitionErr != nil || state.retainedObject == nil || target.RetainedID == "" || state.operationBegin.IsZero() || observedAt.IsZero() || observedAt.Before(state.operationBegin) {
		return model.RetainedObjectProofUnknown, false
	}
	identity := state.retainedObject.Identity()
	if !identity.matches(target) {
		return model.RetainedObjectProofUnknown, false
	}
	membership, err := state.retainedObject.Membership(ctx)
	if err != nil || membership == RetainedMembershipUnknown {
		return model.RetainedObjectProofUnknown, false
	}
	stillHeld, err := state.retainedObject.StillHeld(ctx)
	if err != nil || !stillHeld {
		return model.RetainedObjectProofUnknown, false
	}
	evidence, err := newRetainedGroupEvidence(identity, state.operationBegin, observedAt, membership)
	if err != nil {
		return model.RetainedObjectProofUnknown, false
	}
	return evidence.ProofFor(target, state.operationBegin, observedAt), true
}

func (engine Engine) advanceSession(ctx context.Context, target model.GroupRef, state containmentState, observation model.ContainmentObservation, observedAt time.Time) containmentState {
	if state.operationBegin.IsZero() {
		state.operationBegin = observedAt
	}
	if observation.Group != model.GroupLive || !target.KernelDomain().ProvablySame(observation.KernelDomainID) {
		return state.retentionState()
	}
	switch observation.Leader {
	case model.ProcessIdentityMatching:
		next := containmentState{
			session:                  model.ContainmentSession{BeganFromMatchingLeader: true},
			retainedObject:           state.retainedObject,
			retainedAcquisitionErr:   state.retainedAcquisitionErr,
			operationBegin:           state.operationBegin,
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
				return state.retentionState()
			}
			state.session.ContinuouslyObservedLive = true
			return state
		}
	}
	return state.retentionState()
}

func (state containmentState) retentionState() containmentState {
	return containmentState{
		retainedObject:         state.retainedObject,
		retainedAcquisitionErr: state.retainedAcquisitionErr,
		operationBegin:         state.operationBegin,
	}
}

func (state containmentState) releaseRetainedObject() {
	if state.retainedObject == nil {
		return
	}
	_ = state.retainedObject.Release()
}
