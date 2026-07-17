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

func TestContainmentMatchingLeaderAfterGraceAuthorizesKillWithoutWitness(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
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
	witness := &fakeContinuityWitness{confirmed: true}

	outcome := testEngineWithContinuity(observer, signaler, witness).Contain(context.Background(), target, testParams())

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

func TestContainmentLeaderMissingAfterGraceWithoutContinuityIsUnprovable(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler, SignalTerminate)
}

func TestContainmentContinuityWitnessAuthorizesKillAfterLeaderExit(t *testing.T) {
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
	witness := &fakeContinuityWitness{confirmed: true}

	outcome := testEngineWithContinuity(observer, signaler, witness).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler, SignalTerminate, SignalKill)
	if witness.confirms == 0 {
		t.Fatalf("continuity witness was not consulted")
	}
}

func TestContainmentRejectsWrongGroupContinuityEvidence(t *testing.T) {
	target := testGroupRef(t)
	evidenceTarget := wrongPhysicalGroupRef(target)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}
	witness := &fakeContinuityWitness{confirmed: true, evidenceTarget: &evidenceTarget}

	outcome := testEngineWithContinuity(observer, signaler, witness).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler, SignalTerminate)
	if witness.confirms == 0 {
		t.Fatalf("continuity witness was not consulted")
	}
}

func TestContainmentRejectsPartialContinuityInterval(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}
	witness := &fakeContinuityWitness{confirmed: true, endOffset: -time.Nanosecond}

	outcome := testEngineWithContinuity(observer, signaler, witness).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler, SignalTerminate)
	if witness.confirms == 0 {
		t.Fatalf("continuity witness was not consulted")
	}
}

func TestContainmentRetainedObjectProofAuthorizesKillWithMissingLeader(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
		{signal: SignalKill, result: SignalDelivered},
	}}
	retained := &fakeRetainedObject{memberships: []RetainedGroupMembership{
		RetainedMembershipPresent,
		RetainedMembershipPresent,
		RetainedMembershipEmpty,
	}}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
	assertRetainedSignals(t, retained, SignalTerminate, SignalKill)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained capability membership was not consulted")
	}
}

func TestContainmentRetainedSignalAbsentRequiresEmptyProof(t *testing.T) {
	target := testRetainedGroupRef(t)
	tests := []struct {
		name        string
		memberships []RetainedGroupMembership
		wantAbsent  bool
	}{
		{
			name:        "recursive_empty",
			memberships: []RetainedGroupMembership{RetainedMembershipPresent, RetainedMembershipEmpty},
			wantAbsent:  true,
		},
		{
			name:        "still_populated",
			memberships: []RetainedGroupMembership{RetainedMembershipPresent, RetainedMembershipPresent},
			wantAbsent:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &fakeObserver{observations: []model.ContainmentObservation{
				testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
			}}
			signaler := &fakeSignaler{}
			retained := &fakeRetainedObject{
				memberships: tt.memberships,
				script: []signalScript{
					{signal: SignalTerminate, result: SignalTargetAbsent},
				},
			}

			outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

			if tt.wantAbsent {
				assertAbsent(t, outcome)
			} else {
				assertUnprovable(t, outcome, ReasonSignalUnprovable)
			}
			assertSignals(t, signaler)
			assertRetainedSignals(t, retained, SignalTerminate)
		})
	}
}

func TestContainmentRetainedObjectCapabilityMustMatchObjectAndCoverOperation(t *testing.T) {
	target := testRetainedGroupRef(t)
	differentDomain := target.KernelDomain()
	differentDomain.HostBootID = "different-host-boot"
	tests := []struct {
		name     string
		retained *fakeRetainedObject
	}{
		{
			name:     "wrong_object",
			retained: &fakeRetainedObject{membership: RetainedMembershipPresent, retainedID: "different-retained-object"},
		},
		{
			name: "wrong_domain",
			retained: &fakeRetainedObject{
				membership:     RetainedMembershipPresent,
				kernelDomainID: differentDomain,
			},
		},
		{
			name: "stale_object",
			retained: &fakeRetainedObject{
				membership: RetainedMembershipPresent,
				stillHelds: []bool{false},
			},
		},
		{
			name:     "missing_proof",
			retained: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observer := &fakeObserver{observations: []model.ContainmentObservation{
				testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
			}}
			signaler := &fakeSignaler{}

			outcome := testEngineWithRetained(observer, signaler, tt.retained).Contain(context.Background(), target, testParams())

			assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
			assertSignals(t, signaler)
		})
	}
}

func TestContainmentRetainedObjectEmptyProofProvesAbsent(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{membership: RetainedMembershipEmpty}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained capability membership was not consulted")
	}
}

func TestContainmentRetainedObjectMembersPresentDifferentKernelDomainIsUnprovable(t *testing.T) {
	target := testRetainedGroupRef(t)
	observation := testObservation(target, model.GroupLive, model.ProcessIdentityMissing)
	observation.KernelDomainID.RetainedDomainID = "retained-domain-different"
	observer := &fakeObserver{observations: []model.ContainmentObservation{observation}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{membership: RetainedMembershipPresent}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler)
	assertRetainedSignals(t, retained)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained capability membership was not consulted")
	}
}

func TestContainmentRetainedObjectEmptyProofDifferentKernelDomainProvesAbsent(t *testing.T) {
	target := testRetainedGroupRef(t)
	observation := testObservation(target, model.GroupLive, model.ProcessIdentityMissing)
	observation.KernelDomainID.RetainedDomainID = "retained-domain-different"
	observer := &fakeObserver{observations: []model.ContainmentObservation{observation}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{membership: RetainedMembershipEmpty}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
	assertRetainedSignals(t, retained)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained capability membership was not consulted")
	}
}

func TestContainmentRetainedObjectUnknownProofIsUnprovable(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{membership: RetainedMembershipUnknown}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler)
}

func TestContainmentRetainedObjectUnknownStopsLeaderAuthorization(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
	}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{membership: RetainedMembershipUnknown}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained capability membership was not consulted")
	}
}

func TestContainmentRetainedAuthorityUsesRetainedTeardownForReusedPGID(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{
		memberships: []RetainedGroupMembership{
			RetainedMembershipPresent,
			RetainedMembershipPresent,
			RetainedMembershipEmpty,
		},
	}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
	assertRetainedSignals(t, retained, SignalTerminate, SignalKill)
	if retained.membershipCalls == 0 {
		t.Fatalf("retained membership was not consulted")
	}
}

func TestContainmentRetainedProbeUsesRetainedCapabilityMembership(t *testing.T) {
	target := testRetainedGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
		testObservation(target, model.GroupLive, model.ProcessIdentityReused),
	}}
	signaler := &fakeSignaler{}
	retained := &fakeRetainedObject{
		memberships: []RetainedGroupMembership{
			RetainedMembershipPresent,
			RetainedMembershipPresent,
			RetainedMembershipPresent,
			RetainedMembershipEmpty,
		},
	}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler)
	assertRetainedSignals(t, retained, SignalTerminate, SignalKill)
	if retained.membershipCalls != 4 {
		t.Fatalf("retained membership calls = %d, want 4", retained.membershipCalls)
	}
	if signaler.probeCalls != 0 {
		t.Fatalf("numeric PGID probes = %d, want 0", signaler.probeCalls)
	}
}

func TestContainmentAcquiresRetainedCapabilityBeforeFirstObservation(t *testing.T) {
	target := testRetainedGroupRef(t)
	retained := &fakeRetainedObject{membership: RetainedMembershipEmpty}
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	observer.onObserve = func() {
		if !retained.acquired {
			t.Fatalf("retained capability was not acquired before first observation")
		}
	}
	signaler := &fakeSignaler{}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	if retained.acquireCalls != 1 {
		t.Fatalf("retained acquire calls = %d, want 1", retained.acquireCalls)
	}
	if retained.acquiredAt.IsZero() {
		t.Fatalf("retained acquiredAt was not recorded")
	}
	if retained.releaseCalls != 1 {
		t.Fatalf("retained release calls = %d, want 1", retained.releaseCalls)
	}
}

func TestContainmentRetainedDomainNotApplicableDoesNotRequireRetainedAcquisition(t *testing.T) {
	target := testGroupRef(t)
	target.RetainedID = "legacy-retained-1001"
	if err := target.Validate(); err != nil {
		t.Fatalf("legacy retained GroupRef invalid: %v", err)
	}
	retained := &fakeRetainedObject{acquireErr: errors.New("should not acquire")}
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupAbsent, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertAbsent(t, outcome)
	assertSignals(t, signaler, SignalTerminate)
	if retained.acquireCalls != 0 {
		t.Fatalf("retained acquire calls = %d, want 0", retained.acquireCalls)
	}
}

func TestContainmentRequiredRetainedAcquisitionErrorStopsLeaderPGIDSignal(t *testing.T) {
	target := testRetainedGroupRef(t)
	acquireErr := errors.New("retained reacquisition failed")
	retained := &fakeRetainedObject{acquireErr: acquireErr}
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}

	outcome := testEngineWithRetained(observer, signaler, retained).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	if !errors.Is(outcome.Err, acquireErr) {
		t.Fatalf("outcome error = %v, want acquisition error", outcome.Err)
	}
	assertSignals(t, signaler)
	if retained.acquireCalls != 1 {
		t.Fatalf("retained acquire calls = %d, want 1", retained.acquireCalls)
	}
	if retained.releaseCalls != 0 {
		t.Fatalf("retained release calls = %d, want 0", retained.releaseCalls)
	}
}

func TestContainmentReusedPGIDLeaderMissingWithoutContinuityDoesNotKill(t *testing.T) {
	target := testGroupRef(t)
	observer := &fakeObserver{observations: []model.ContainmentObservation{
		testObservation(target, model.GroupLive, model.ProcessIdentityMatching),
		testObservation(target, model.GroupLive, model.ProcessIdentityMissing),
	}}
	signaler := &fakeSignaler{script: []signalScript{
		{signal: SignalTerminate, result: SignalDelivered},
	}}

	outcome := testEngine(observer, signaler).Contain(context.Background(), target, testParams())

	assertUnprovable(t, outcome, ReasonAuthorizationUnprovable)
	assertSignals(t, signaler, SignalTerminate)
}

func testEngine(observer *fakeObserver, signaler *fakeSignaler) Engine {
	return testEngineWithContinuity(observer, signaler, nil)
}

func testEngineWithContinuity(observer *fakeObserver, signaler *fakeSignaler, witness ContinuityWitness) Engine {
	return Engine{Observer: observer, Signaler: signaler, Clock: newFakeClock(), Continuity: witness}
}

func testEngineWithRetained(observer *fakeObserver, signaler *fakeSignaler, retained RetainedGroupObject) Engine {
	return Engine{Observer: observer, Signaler: signaler, Clock: newFakeClock(), RetainedObject: retained}
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

func testRetainedGroupRef(t *testing.T) model.GroupRef {
	t.Helper()
	ref := testGroupRef(t)
	ref.RetainedDomainID = "retained-domain-1"
	ref.RetainedDomainState = model.RetainedDomainKnown
	ref.RetainedID = "retained-1001"
	if err := ref.Validate(); err != nil {
		t.Fatalf("test retained GroupRef invalid: %v", err)
	}
	return ref
}

func wrongPhysicalGroupRef(ref model.GroupRef) model.GroupRef {
	ref.PGID++
	ref.Leader = model.ProcessIdentity{PID: ref.PGID, HighResStartToken: "leader-start-wrong"}
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

func assertRetainedSignals(t *testing.T, retained *fakeRetainedObject, signals ...Signal) {
	t.Helper()
	got := make([]Signal, len(retained.signalCalls))
	for i, call := range retained.signalCalls {
		got[i] = call.signal
	}
	if !slices.Equal(got, signals) {
		t.Fatalf("retained signals = %v, want %v", got, signals)
	}
}
