package containment

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestContainmentAlreadyAbsentDoesNotSignal(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
}

func TestContainmentTermSufficesWithinGrace(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{{signal: SignalTerminate, result: SignalDelivered}}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler, SignalTerminate)
}

func TestContainmentTermIgnoredThenKillPollsAbsent(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
		{signal: SignalKill, result: SignalDelivered},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler, SignalTerminate, SignalKill)
}

func TestContainmentNeverAbsentWithinBoundIsUnprovable(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{
		script: []signalScript{
			{signal: SignalTerminate, result: SignalDelivered},
			{signal: SignalKill, result: SignalDelivered},
		},
		probes: []ProbeResult{ProbeLive, ProbeLive, ProbeLive},
	}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAbsenceDeadlineExceeded)
	assertSignals(t, signaler, SignalTerminate, SignalKill)
}

func TestContainmentSignalAmbiguousIsUnprovable(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalUnprovable, err: errors.New("permission denied")},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonSignalUnprovable)
	assertSignals(t, signaler, SignalTerminate)
}

func TestContainmentUnknownOrContradictoryObservationIsUnprovable(t *testing.T) {
	target := testGroupRef(t)
	tests := []struct {
		name  string
		group model.GroupExistenceObservation
	}{
		{name: "unknown", group: model.GroupExistenceUnknown},
		{name: "contradictory", group: model.GroupExistenceContradictory},
		{name: "permission_denied", group: model.GroupExistencePermissionDenied},
		{name: "unsupported", group: model.GroupExistenceUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &fakeObserver{observations: []model.ContainmentObservation{
				testObservation(target, tt.group, model.ProcessIdentityUnknown),
			}}
			signaler := &fakeSignaler{}

			outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

			assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
			assertSignals(t, signaler)
		})
	}
}

func TestContainmentColdLeaderMissingWaitsWithoutSignalling(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		trustedMonitorObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		trustedMonitorObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		trustedMonitorObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonUnauthorizedWaitExpired)
	assertSignals(t, signaler)
}

func TestContainmentColdLeaderMissingFirstObservationIsUnprovableWithoutSignal(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler)
}

func TestContainmentReusedLeaderAfterTermRevokesAuthorityWithoutKill(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler, SignalTerminate)
}

func TestContainmentEngineMintedContinuousLiveGroupEscalatesAfterLeaderExit(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
		{signal: SignalKill, result: SignalDelivered},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler, SignalTerminate, SignalKill)
}

func testEngine(observer *fakeObserver, signaler *fakeSignaler) Engine {
	return Engine{Observer: observer, Signaler: signaler, Clock: newFakeClock()}
}

func testParams() Params {
	return Params{
		GracePeriod:                time.Second,
		PollInterval:               time.Second,
		PollTimeout:                2 * time.Second,
		TrustedMonitorWait:         2 * time.Second,
		TrustedMonitorPollInterval: time.Second,
	}
}

func testObservation(target model.GroupRef, group model.GroupExistenceObservation, leader model.ProcessIdentityObservation) model.ContainmentObservation {
	return model.ContainmentObservation{
		KernelDomainID: target.KernelDomain(),
		Group:          group,
		Leader:         leader,
	}
}

func trustedMonitorObservation(target model.GroupRef, group model.GroupExistenceObservation, leader model.ProcessIdentityObservation) model.ContainmentObservation {
	observation := testObservation(target, group, leader)
	observation.Monitor = model.ContainmentMonitorObservation{
		Observed:          true,
		KernelDomainID:    target.KernelDomain(),
		Alive:             true,
		Identity:          model.ProcessIdentityMatching,
		BoundToExactGroup: true,
	}
	return observation
}

func testGroupRef(t *testing.T) model.GroupRef {
	t.Helper()
	ref := model.GroupRef{
		Version:   1,
		CustodyID: "custody-1",
		Launch: model.LaunchKey{
			Attempt: model.AttemptRef{JobID: "job-1", AttemptID: "attempt-1", Epoch: 1},
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:        "host-boot-1",
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              1001,
		Leader:            model.ProcessIdentity{PID: 1001, HighResStartToken: "leader-start-1001"},
		Monitor:           model.ProcessIdentity{PID: 2001, HighResStartToken: "monitor-start-2001"},
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("test GroupRef invalid: %v", err)
	}
	return ref
}

func assertAbsent(t *testing.T, outcome Outcome) {
	t.Helper()
	if outcome.Kind != OutcomeAbsent {
		t.Fatalf("outcome = %#v, want absent", outcome)
	}
}

func assertUnprovable(t *testing.T, outcome Outcome, reason UnprovableReason) {
	t.Helper()
	if outcome.Kind != OutcomeUnprovable {
		t.Fatalf("outcome = %#v, want unprovable", outcome)
	}
	if outcome.Reason != reason {
		t.Fatalf("unprovable reason = %s, want %s; outcome=%#v", outcome.Reason, reason, outcome)
	}
}

func assertSignals(t *testing.T, signaler *fakeSignaler, signals ...Signal) {
	t.Helper()
	got := make([]Signal, len(signaler.calls))
	for i, call := range signaler.calls {
		got[i] = call.signal
	}
	if !slices.Equal(got, signals) {
		t.Fatalf("signals = %v, want %v", got, signals)
	}
}
