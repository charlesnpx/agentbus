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
	calls        int
}

func (observer *fakeObserver) ObserveGroup(_ context.Context, _ model.GroupRef) (model.ContainmentObservation, error) {
	if len(observer.observations) == 0 {
		return model.ContainmentObservation{}, errors.New("no scripted observations")
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
	membership  RetainedGroupMembership
	memberships []RetainedGroupMembership
	retainedID  string
	begin       time.Time
	end         time.Time
	err         error
	calls       int
}

func (object *fakeRetainedObject) ProveRetainedGroup(_ context.Context, target model.GroupRef, begin, end time.Time) (RetainedGroupEvidence, error) {
	if object == nil {
		return RetainedGroupEvidence{}, errors.New("retained object proof is missing")
	}
	index := object.calls
	object.calls++
	if object.err != nil {
		return RetainedGroupEvidence{}, object.err
	}
	membership := object.membership
	if index < len(object.memberships) {
		membership = object.memberships[index]
	} else if len(object.memberships) > 0 {
		membership = object.memberships[len(object.memberships)-1]
	}
	retainedID := object.retainedID
	if retainedID == "" {
		retainedID = target.RetainedID
	}
	evidenceBegin := object.begin
	if evidenceBegin.IsZero() {
		evidenceBegin = begin
	}
	evidenceEnd := object.end
	if evidenceEnd.IsZero() {
		evidenceEnd = end
	}
	evidence, err := NewRetainedGroupEvidence(retainedID, evidenceBegin, evidenceEnd, membership)
	if err != nil {
		return RetainedGroupEvidence{}, err
	}
	return evidence, nil
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
