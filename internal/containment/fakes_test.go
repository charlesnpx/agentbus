package containment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type fakeObserver struct {
	observations []model.ContainmentObservation
	onObserve    func()
	calls        int
}

func (observer *fakeObserver) ObserveGroup(_ context.Context, _ model.GroupRef) (model.ContainmentObservation, error) {
	if len(observer.observations) == 0 {
		return model.ContainmentObservation{}, errors.New("no scripted observations")
	}
	if observer.onObserve != nil {
		observer.onObserve()
	}
	index := observer.calls
	if index >= len(observer.observations) {
		index = len(observer.observations) - 1
	}
	observer.calls++
	return observer.observations[index], nil
}

type fakeContinuityWitness struct {
	confirmed      bool
	evidenceTarget *model.GroupRef
	beginOffset    time.Duration
	endOffset      time.Duration
	starts         int
	confirms       int
}

func (witness *fakeContinuityWitness) BeginGroupContinuity(_ context.Context, _ model.GroupRef, _ model.ContainmentObservation, _ time.Time) GroupContinuity {
	witness.starts++
	return witness
}

func (witness *fakeContinuityWitness) ConfirmContinuouslyLive(_ context.Context, target model.GroupRef, _ model.ContainmentObservation, begin, end time.Time) GroupContinuityEvidence {
	witness.confirms++
	if !witness.confirmed {
		return GroupContinuityEvidence{}
	}
	evidenceTarget := target
	if witness.evidenceTarget != nil {
		evidenceTarget = *witness.evidenceTarget
	}
	evidence, err := NewGroupContinuityEvidence(evidenceTarget, begin.Add(witness.beginOffset), end.Add(witness.endOffset))
	if err != nil {
		return GroupContinuityEvidence{}
	}
	return evidence
}

type fakeRetainedObject struct {
	membership      RetainedGroupMembership
	memberships     []RetainedGroupMembership
	retainedID      string
	kernelDomainID  model.KernelDomainID
	stillHelds      []bool
	acquireErr      error
	membershipErr   error
	stillHeldErr    error
	script          []signalScript
	acquired        bool
	acquiredAt      time.Time
	acquireCalls    int
	membershipCalls int
	stillHeldCalls  int
	signalCalls     []signalCall
	releaseCalls    int
}

func (object *fakeRetainedObject) AcquireRetainedGroup(_ context.Context, target model.GroupRef, acquiredAt time.Time) (RetainedGroupCapability, error) {
	if object == nil {
		return nil, errors.New("retained object capability is missing")
	}
	object.acquireCalls++
	if object.acquireErr != nil {
		return nil, object.acquireErr
	}
	object.acquired = true
	object.acquiredAt = acquiredAt
	if object.retainedID == "" {
		object.retainedID = target.RetainedID
	}
	if object.kernelDomainID == (model.KernelDomainID{}) {
		object.kernelDomainID = target.KernelDomain()
	}
	return object, nil
}

func (object *fakeRetainedObject) Identity() RetainedGroupIdentity {
	return RetainedGroupIdentity{
		RetainedID:     object.retainedID,
		KernelDomainID: object.kernelDomainID,
	}
}

func (object *fakeRetainedObject) Membership(_ context.Context) (RetainedGroupMembership, error) {
	if object == nil || !object.acquired {
		return RetainedMembershipUnknown, errors.New("retained object capability is missing")
	}
	index := object.membershipCalls
	object.membershipCalls++
	if object.membershipErr != nil {
		return RetainedMembershipUnknown, object.membershipErr
	}
	membership := object.membership
	if index < len(object.memberships) {
		membership = object.memberships[index]
	} else if len(object.memberships) > 0 {
		membership = object.memberships[len(object.memberships)-1]
	}
	return membership, nil
}

func (object *fakeRetainedObject) StillHeld(_ context.Context) (bool, error) {
	if object == nil || !object.acquired {
		return false, errors.New("retained object capability is missing")
	}
	index := object.stillHeldCalls
	object.stillHeldCalls++
	if object.stillHeldErr != nil {
		return false, object.stillHeldErr
	}
	if index < len(object.stillHelds) {
		return object.stillHelds[index], nil
	}
	if len(object.stillHelds) > 0 {
		return object.stillHelds[len(object.stillHelds)-1], nil
	}
	return true, nil
}

func (object *fakeRetainedObject) SignalTerm(ctx context.Context) (SignalResult, error) {
	return object.signal(ctx, SignalTerminate)
}

func (object *fakeRetainedObject) Kill(ctx context.Context) (SignalResult, error) {
	return object.signal(ctx, SignalKill)
}

func (object *fakeRetainedObject) signal(ctx context.Context, signal Signal) (SignalResult, error) {
	if err := ctx.Err(); err != nil {
		return SignalUnprovable, err
	}
	object.signalCalls = append(object.signalCalls, signalCall{signal: signal})
	if len(object.script) == 0 {
		return SignalDelivered, nil
	}
	next := object.script[0]
	object.script = object.script[1:]
	if next.signal != 0 && next.signal != signal {
		return SignalUnprovable, fmt.Errorf("retained signal = %s, want %s", signal, next.signal)
	}
	return next.result, next.err
}

func (object *fakeRetainedObject) Release() error {
	object.releaseCalls++
	return nil
}

type signalScript struct {
	signal Signal
	result SignalResult
	err    error
}

type signalCall struct {
	signal Signal
	pgid   int
}

type fakeSignaler struct {
	script      []signalScript
	calls       []signalCall
	probes      []ProbeResult
	probeErrors []error
	probeCalls  int
}

func (signaler *fakeSignaler) SignalGroup(_ context.Context, target model.GroupRef, signal Signal) (SignalResult, error) {
	signaler.calls = append(signaler.calls, signalCall{signal: signal, pgid: target.PGID})
	if len(signaler.script) == 0 {
		return SignalDelivered, nil
	}
	next := signaler.script[0]
	signaler.script = signaler.script[1:]
	if next.signal != 0 && next.signal != signal {
		return SignalUnprovable, fmt.Errorf("signal = %s, want %s", signal, next.signal)
	}
	return next.result, next.err
}

func (signaler *fakeSignaler) ProbeGroup(_ context.Context, _ model.GroupRef) (ProbeResult, error) {
	index := signaler.probeCalls
	signaler.probeCalls++
	if index < len(signaler.probeErrors) && signaler.probeErrors[index] != nil {
		return ProbeUnprovable, signaler.probeErrors[index]
	}
	if index < len(signaler.probes) {
		return signaler.probes[index], nil
	}
	return ProbeLive, nil
}

type fakeClock struct {
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)}
}

func (clock *fakeClock) Now() time.Time {
	return clock.now
}

func (clock *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration < 0 {
		return fmt.Errorf("negative sleep %s", duration)
	}
	clock.sleeps = append(clock.sleeps, duration)
	clock.now = clock.now.Add(duration)
	return ctx.Err()
}
