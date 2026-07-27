package model

import "testing"

func TestContainmentCallerSuppliedContinuityAuthorizesKillAfterLeaderExitWithLiveGrandchild(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		Session: ContainmentSession{
			BeganFromMatchingLeader:  true,
			ContinuouslyObservedLive: true,
		},
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != SignalDirectly {
		t.Fatalf("decision = %s, want %s", decision, SignalDirectly)
	}
}

func TestContainmentRetainedObjectProofAuthorizesMissingLeader(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: ref.KernelDomain(),
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofMembersPresent,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != SignalDirectly {
		t.Fatalf("decision = %s, want %s", decision, SignalDirectly)
	}
}

func TestContainmentRetainedMembersPresentContradictsDifferentKernelDomain(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	differentDomain := ref.KernelDomain()
	differentDomain.RetainedDomainID = "retained-domain-different"
	result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: differentDomain,
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofMembersPresent,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
	}
	if result.Decision != Unprovable || result.Basis != ContainmentBasisNone {
		t.Fatalf("authorization = %#v, want unprovable/no basis", result)
	}
}

func TestContainmentRequiredRetainedMissingProofIsUnprovable(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	differentDomain := ref.KernelDomain()
	differentDomain.RetainedDomainID = "retained-domain-different"
	tests := []struct {
		name        string
		observation ContainmentObservation
	}{
		{
			name: "different_domain",
			observation: ContainmentObservation{
				KernelDomainID: differentDomain,
				Group:          GroupLive,
				Leader:         ProcessIdentityMissing,
			},
		},
		{
			name: "observed_absent",
			observation: ContainmentObservation{
				KernelDomainID: ref.KernelDomain(),
				Group:          GroupAbsent,
				Leader:         ProcessIdentityMissing,
			},
		},
		{
			name: "matching_live_leader",
			observation: ContainmentObservation{
				KernelDomainID: ref.KernelDomain(),
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
				Group:       ref,
				Observation: tt.observation,
			})
			if err != nil {
				t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
			}
			if result.Decision != Unprovable || result.Basis != ContainmentBasisNone {
				t.Fatalf("authorization = %#v, want unprovable/no basis", result)
			}
		})
	}
}

func TestContainmentAuthorizationCarriesBasis(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	leaderResult, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupLive,
			Leader:         ProcessIdentityMatching,
		},
	})
	if err != nil {
		t.Fatalf("leader authorization error = %v", err)
	}
	if leaderResult.Decision != SignalDirectly || leaderResult.Basis != ContainmentBasisLeader {
		t.Fatalf("leader authorization = %#v, want signal_directly/leader", leaderResult)
	}

	retainedRef := retainedReducerGroup(LaunchOrdinalOne)
	retainedResult, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: retainedRef,
		Observation: ContainmentObservation{
			KernelDomainID: retainedRef.KernelDomain(),
			Group:          GroupLive,
			Leader:         ProcessIdentityReused,
		},
		RetainedObject: RetainedObjectProofMembersPresent,
	})
	if err != nil {
		t.Fatalf("retained authorization error = %v", err)
	}
	if retainedResult.Decision != SignalDirectly || retainedResult.Basis != ContainmentBasisRetainedObject {
		t.Fatalf("retained authorization = %#v, want signal_directly/retained_object", retainedResult)
	}
}

func TestContainmentRetainedObjectEmptyProofProvesAbsent(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: ref.KernelDomain(),
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofEmpty,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != AlreadyAbsent {
		t.Fatalf("decision = %s, want %s", decision, AlreadyAbsent)
	}
}

func TestContainmentRetainedObjectEmptyProofProvesAbsentWithDifferentKernelDomain(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	differentDomain := ref.KernelDomain()
	differentDomain.RetainedDomainID = "retained-domain-different"
	result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: differentDomain,
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofEmpty,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
	}
	if result.Decision != AlreadyAbsent || result.Basis != ContainmentBasisRetainedObject {
		t.Fatalf("authorization = %#v, want already_absent/retained_object", result)
	}
}

func TestContainmentObservedUnknownMonitorIdentityIsIncoherent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	monitor := ContainmentMonitorObservation{
		Observed:       true,
		KernelDomainID: ref.KernelDomain(),
		Alive:          true,
		Identity:       ProcessIdentityUnknown,
	}
	if monitor.coherent() {
		t.Fatal("observed monitor with unknown identity is coherent, want incoherent")
	}
}

func TestContainmentRetainedEmptyObservationCoherentIgnoresUnknownMonitorButRequiresMissingLeader(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	observation := ContainmentObservation{
		KernelDomainID: ref.KernelDomain(),
		Group:          GroupLive,
		Leader:         ProcessIdentityMissing,
		Monitor: ContainmentMonitorObservation{
			Observed:       true,
			KernelDomainID: ref.KernelDomain(),
			Alive:          true,
			Identity:       ProcessIdentityUnknown,
		},
	}
	if !retainedEmptyObservationCoherent(observation) {
		t.Fatal("retained empty observation with unknown monitor and missing leader is incoherent, want coherent")
	}

	observation.Leader = ProcessIdentityMatching
	if retainedEmptyObservationCoherent(observation) {
		t.Fatal("retained empty observation with matching live leader is coherent, want incoherent")
	}
}

func TestContainmentRetainedObjectUnknownProofDoesNotProveAbsentOrSignal(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofUnknown,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != Unprovable {
		t.Fatalf("decision = %s, want %s", decision, Unprovable)
	}
}

func TestContainmentNonRetainedObjectProofIsInconsistent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	proofs := []RetainedObjectProof{
		RetainedObjectProofEmpty,
		RetainedObjectProofMembersPresent,
		RetainedObjectProofUnknown,
	}

	for _, proof := range proofs {
		t.Run(string(proof), func(t *testing.T) {
			decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
				Group: ref,
				Observation: ContainmentObservation{
					KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
					Group:          GroupAbsent,
					Leader:         ProcessIdentityMissing,
				},
				RetainedObject: proof,
			})
			if err != nil {
				t.Fatalf("DecideContainmentAuthorization error = %v", err)
			}
			if decision != Unprovable {
				t.Fatalf("decision = %s, want %s", decision, Unprovable)
			}
		})
	}
}

func TestContainmentSessionDoesNotAuthorizeReusedLeader(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupLive,
			Leader:         ProcessIdentityReused,
		},
		Session: ContainmentSession{
			BeganFromMatchingLeader:  true,
			ContinuouslyObservedLive: true,
		},
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != Unprovable {
		t.Fatalf("decision = %s, want %s", decision, Unprovable)
	}
}

func TestColdLeaderMissingContainmentWaitsThenBecomesUnprovable(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	observation := ContainmentObservation{
		KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
		Group:          GroupLive,
		Leader:         ProcessIdentityMissing,
		Monitor: ContainmentMonitorObservation{
			Observed:          true,
			KernelDomainID:    noPIDNamespaceDomain(ref.HostBootID),
			Alive:             true,
			Identity:          ProcessIdentityMatching,
			BoundToExactGroup: true,
		},
	}
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group:       ref,
		Observation: observation,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization wait error = %v", err)
	}
	if decision != WaitBoundedForTrustedMonitor {
		t.Fatalf("cold decision = %s, want %s", decision, WaitBoundedForTrustedMonitor)
	}

	decision, err = DecideContainmentAuthorization(ContainmentAuthorization{
		Group:           ref,
		Observation:     observation,
		DeadlineExpired: true,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization deadline error = %v", err)
	}
	if decision != Unprovable {
		t.Fatalf("deadline decision = %s, want %s", decision, Unprovable)
	}
}

func TestContainmentAlreadyAbsentAfterIndependentObservation(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupAbsent,
			Leader:         ProcessIdentityMissing,
		},
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != AlreadyAbsent {
		t.Fatalf("decision = %s, want %s", decision, AlreadyAbsent)
	}
}

func TestContainmentNonRetainedObservedUnknownMonitorDoesNotProveAbsent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupAbsent,
			Leader:         ProcessIdentityMissing,
			Monitor: ContainmentMonitorObservation{
				Observed:       true,
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Alive:          true,
				Identity:       ProcessIdentityUnknown,
			},
		},
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
	}
	if result.Decision != Unprovable || result.Basis != ContainmentBasisNone {
		t.Fatalf("authorization = %#v, want unprovable/no basis", result)
	}
}

func TestContainmentNonRetainedDifferentKernelDomainRemainsAlreadyAbsent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	differentDomain := noPIDNamespaceDomain("different-host-boot")
	result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: differentDomain,
			Group:          GroupLive,
			Leader:         ProcessIdentityMissing,
		},
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
	}
	if result.Decision != AlreadyAbsent || result.Basis != ContainmentBasisNone {
		t.Fatalf("authorization = %#v, want already_absent/no basis", result)
	}
}

func TestContainmentAuthorizationTargetStateDecisionTable(t *testing.T) {
	retainedStates := []struct {
		name  string
		state RetainedDomainState
	}{
		{name: "known", state: RetainedDomainKnown},
		{name: "not_applicable", state: RetainedDomainNotApplicable},
		{name: "unknown", state: RetainedDomainUnknown},
	}
	proofs := []struct {
		name  string
		proof RetainedObjectProof
	}{
		{name: "none", proof: RetainedObjectProofNone},
		{name: "empty", proof: RetainedObjectProofEmpty},
		{name: "members_present", proof: RetainedObjectProofMembersPresent},
		{name: "unknown", proof: RetainedObjectProofUnknown},
	}
	relations := []struct {
		name     string
		relation kernelDomainRelation
	}{
		{name: "same", relation: kernelDomainSame},
		{name: "different", relation: kernelDomainDifferent},
		{name: "unprovable", relation: kernelDomainUnprovable},
	}
	groups := []GroupExistenceObservation{
		GroupLive,
		GroupAbsent,
		GroupExistenceUnknown,
		GroupExistenceContradictory,
		GroupExistencePermissionDenied,
		GroupExistenceUnsupported,
	}
	leaders := []ProcessIdentityObservation{
		ProcessIdentityMatching,
		ProcessIdentityMissing,
		ProcessIdentityReused,
		ProcessIdentityUnknown,
	}

	checked := 0
	pruned := 0
	for _, retainedState := range retainedStates {
		for _, proof := range proofs {
			for _, relation := range relations {
				for _, group := range groups {
					for _, leader := range leaders {
						t.Run(retainedState.name+"/"+proof.name+"/"+relation.name+"/"+string(group)+"/"+string(leader), func(t *testing.T) {
							target := decisionTableGroup(t, retainedState.state)
							observationDomain, ok := decisionTableObservationDomain(target, relation.relation)
							if !ok {
								// A target with an unknown retained domain cannot be
								// proven in the same retained domain by construction.
								// This is the only pruned combination; expanding the
								// group axis from four to six states increases it from
								// 64 to 96 base cells.
								pruned++
								return
							}
							actualRelation, err := compareKernelDomain(target.KernelDomain(), observationDomain)
							if err != nil {
								t.Fatalf("compareKernelDomain error = %v", err)
							}
							if actualRelation != relation.relation {
								t.Fatalf("constructed relation = %v, want %v", actualRelation, relation.relation)
							}

							for _, monitor := range decisionTableMonitorCases(target, retainedState.state, proof.proof, relation.relation, group, leader) {
								t.Run(monitor.name, func(t *testing.T) {
									got, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
										Group: target,
										Observation: ContainmentObservation{
											KernelDomainID: observationDomain,
											Group:          group,
											Leader:         leader,
											Monitor:        monitor.observation,
										},
										RetainedObject: proof.proof,
									})
									if err != nil {
										t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
									}
									want := decisionTableAuthorization(retainedState.state, proof.proof, relation.relation, group, leader, monitor.observation)
									if got != want {
										t.Fatalf("authorization = %#v, want %#v", got, want)
									}
									checked++
								})
							}
						})
					}
				}
			}
		}
	}
	// 768 base cells plus monitor-axis coverage for retained empty
	// incoherence/unknown monitors and the non-retained false-absence guard.
	if checked != 781 {
		t.Fatalf("checked decision-table cells = %d, want 781", checked)
	}
	if pruned != 96 {
		t.Fatalf("pruned decision-table cells = %d, want 96", pruned)
	}
}

func TestContainmentRequiredRetainedEmptyProofRejectsOpaqueGroupObservation(t *testing.T) {
	ref := retainedReducerGroup(LaunchOrdinalOne)
	groups := []GroupExistenceObservation{
		GroupExistencePermissionDenied,
		GroupExistenceUnsupported,
	}

	for _, group := range groups {
		t.Run(string(group), func(t *testing.T) {
			result, err := DecideContainmentAuthorizationWithBasis(ContainmentAuthorization{
				Group: ref,
				Observation: ContainmentObservation{
					KernelDomainID: ref.KernelDomain(),
					Group:          group,
					Leader:         ProcessIdentityMissing,
				},
				RetainedObject: RetainedObjectProofEmpty,
			})
			if err != nil {
				t.Fatalf("DecideContainmentAuthorizationWithBasis error = %v", err)
			}
			if result.Decision != Unprovable || result.Basis != ContainmentBasisNone {
				t.Fatalf("authorization = %#v, want unprovable/no basis", result)
			}
		})
	}
}

func TestContainmentGroupLeaderCoherenceExhaustive(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	groups := []GroupExistenceObservation{
		GroupLive,
		GroupAbsent,
		GroupExistenceUnknown,
		GroupExistenceContradictory,
	}
	leaders := []ProcessIdentityObservation{
		ProcessIdentityMatching,
		ProcessIdentityMissing,
		ProcessIdentityReused,
		ProcessIdentityUnknown,
	}

	for _, group := range groups {
		for _, leader := range leaders {
			t.Run(string(group)+"/"+string(leader), func(t *testing.T) {
				want := Unprovable
				if group == GroupAbsent && leader == ProcessIdentityMissing {
					want = AlreadyAbsent
				}
				if group == GroupLive && leader == ProcessIdentityMatching {
					want = SignalDirectly
				}

				decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
					Group: ref,
					Observation: ContainmentObservation{
						KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
						Group:          group,
						Leader:         leader,
					},
				})
				if err != nil {
					t.Fatalf("DecideContainmentAuthorization error = %v", err)
				}
				if decision != want {
					t.Fatalf("decision = %s, want %s", decision, want)
				}
			})
		}
	}
}

func retainedReducerGroup(ordinal LaunchOrdinal) GroupRef {
	ref := reducerGroup(ordinal)
	ref.RetainedID = "retained-" + ordinal.String()
	ref.RetainedDomainID = "retained-domain-" + ordinal.String()
	ref.RetainedDomainState = RetainedDomainKnown
	return ref
}

func decisionTableGroup(t *testing.T, retainedState RetainedDomainState) GroupRef {
	t.Helper()
	ref := reducerGroup(LaunchOrdinalOne)
	switch retainedState {
	case RetainedDomainKnown:
		ref.RetainedID = "retained-table"
		ref.RetainedDomainID = "retained-domain-table"
	case RetainedDomainNotApplicable, RetainedDomainUnknown:
		ref.RetainedID = ""
		ref.RetainedDomainID = ""
	default:
		t.Fatalf("unsupported retained domain state %v", retainedState)
	}
	ref.RetainedDomainState = retainedState
	if err := ref.Validate(); err != nil {
		t.Fatalf("decision-table GroupRef invalid: %v", err)
	}
	return ref
}

func decisionTableObservationDomain(target GroupRef, relation kernelDomainRelation) (KernelDomainID, bool) {
	domain := target.KernelDomain()
	switch target.RetainedDomainState {
	case RetainedDomainKnown:
		switch relation {
		case kernelDomainSame:
			return domain, true
		case kernelDomainDifferent:
			domain.RetainedDomainID = "retained-domain-table-different"
			return domain, true
		case kernelDomainUnprovable:
			domain.RetainedDomainID = ""
			domain.RetainedDomainState = RetainedDomainUnknown
			return domain, true
		}
	case RetainedDomainNotApplicable:
		switch relation {
		case kernelDomainSame:
			return domain, true
		case kernelDomainDifferent:
			domain.HostBootID = "host-boot-table-different"
			return domain, true
		case kernelDomainUnprovable:
			domain.PIDNamespaceState = PIDNamespaceUnknown
			return domain, true
		}
	case RetainedDomainUnknown:
		switch relation {
		case kernelDomainSame:
			return KernelDomainID{}, false
		case kernelDomainDifferent:
			domain.HostBootID = "host-boot-table-different"
			return domain, true
		case kernelDomainUnprovable:
			return domain, true
		}
	}
	return KernelDomainID{}, false
}

type decisionTableMonitorCase struct {
	name        string
	observation ContainmentMonitorObservation
}

func decisionTableMonitorCases(target GroupRef, retainedState RetainedDomainState, proof RetainedObjectProof, relation kernelDomainRelation, group GroupExistenceObservation, leader ProcessIdentityObservation) []decisionTableMonitorCase {
	cases := []decisionTableMonitorCase{{name: "coherent_monitor"}}
	if retainedState == RetainedDomainKnown &&
		proof == RetainedObjectProofEmpty &&
		(group == GroupLive || group == GroupAbsent) &&
		leader == ProcessIdentityMissing {
		cases = append(cases, decisionTableMonitorCase{
			name: "incoherent_monitor",
			observation: ContainmentMonitorObservation{
				Observed:          true,
				KernelDomainID:    target.KernelDomain(),
				Identity:          ProcessIdentityMatching,
				BoundToExactGroup: true,
			},
		})
		cases = append(cases, decisionTableMonitorCase{
			name:        "observed_unknown_monitor",
			observation: decisionTableObservedUnknownMonitor(target),
		})
	}
	if retainedState == RetainedDomainNotApplicable &&
		proof == RetainedObjectProofNone &&
		relation == kernelDomainSame &&
		group == GroupAbsent &&
		leader == ProcessIdentityMissing {
		cases = append(cases, decisionTableMonitorCase{
			name:        "observed_unknown_monitor",
			observation: decisionTableObservedUnknownMonitor(target),
		})
	}
	return cases
}

func decisionTableObservedUnknownMonitor(target GroupRef) ContainmentMonitorObservation {
	return ContainmentMonitorObservation{
		Observed:       true,
		KernelDomainID: target.KernelDomain(),
		Alive:          true,
		Identity:       ProcessIdentityUnknown,
	}
}

func decisionTableAuthorization(retainedState RetainedDomainState, proof RetainedObjectProof, relation kernelDomainRelation, group GroupExistenceObservation, leader ProcessIdentityObservation, monitor ContainmentMonitorObservation) ContainmentAuthorizationResult {
	switch retainedState {
	case RetainedDomainUnknown:
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
	case RetainedDomainKnown:
		switch proof {
		case RetainedObjectProofMembersPresent:
			if relation == kernelDomainDifferent {
				return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
			}
			return containmentAuthorizationResult(SignalDirectly, ContainmentBasisRetainedObject)
		case RetainedObjectProofEmpty:
			if (group == GroupLive || group == GroupAbsent) && leader == ProcessIdentityMissing {
				return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisRetainedObject)
			}
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
		default:
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
		}
	case RetainedDomainNotApplicable:
		if proof != RetainedObjectProofNone {
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
		}
		switch relation {
		case kernelDomainDifferent:
			return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisNone)
		case kernelDomainUnprovable:
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
		}
		if !monitor.coherent() {
			return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
		}
		if group == GroupAbsent && leader == ProcessIdentityMissing {
			return containmentAuthorizationResult(AlreadyAbsent, ContainmentBasisNone)
		}
		if group == GroupLive && leader == ProcessIdentityMatching {
			return containmentAuthorizationResult(SignalDirectly, ContainmentBasisLeader)
		}
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
	default:
		return containmentAuthorizationResult(Unprovable, ContainmentBasisNone)
	}
}

func TestContainmentIncoherentAbsentLiveLeaderIsUnprovable(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	tests := []struct {
		name   string
		leader ProcessIdentityObservation
	}{
		{name: "matching", leader: ProcessIdentityMatching},
		{name: "reused", leader: ProcessIdentityReused},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
				Group: ref,
				Observation: ContainmentObservation{
					KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
					Group:          GroupAbsent,
					Leader:         tt.leader,
				},
			})
			if err != nil {
				t.Fatalf("DecideContainmentAuthorization error = %v", err)
			}
			if decision != Unprovable {
				t.Fatalf("decision = %s, want %s", decision, Unprovable)
			}
		})
	}
}

func TestContainmentMonitorTrustRequiresExactVerifiedBinding(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	tests := []struct {
		name    string
		monitor ContainmentMonitorObservation
	}{
		{
			name: "wrong_kernel_domain",
			monitor: ContainmentMonitorObservation{
				Observed:          true,
				KernelDomainID:    noPIDNamespaceDomain("other-host-boot"),
				Alive:             true,
				Identity:          ProcessIdentityMatching,
				BoundToExactGroup: true,
			},
		},
		{
			name: "not_alive",
			monitor: ContainmentMonitorObservation{
				Observed:          true,
				KernelDomainID:    noPIDNamespaceDomain(ref.HostBootID),
				Identity:          ProcessIdentityMatching,
				BoundToExactGroup: true,
			},
		},
		{
			name: "identity_not_verified",
			monitor: ContainmentMonitorObservation{
				Observed:          true,
				KernelDomainID:    noPIDNamespaceDomain(ref.HostBootID),
				Alive:             true,
				Identity:          ProcessIdentityUnknown,
				BoundToExactGroup: true,
			},
		},
		{
			name: "not_bound_to_exact_group",
			monitor: ContainmentMonitorObservation{
				Observed:       true,
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Alive:          true,
				Identity:       ProcessIdentityMatching,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
				Group: ref,
				Observation: ContainmentObservation{
					KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
					Group:          GroupLive,
					Leader:         ProcessIdentityMissing,
					Monitor:        tt.monitor,
				},
			})
			if err != nil {
				t.Fatalf("DecideContainmentAuthorization error = %v", err)
			}
			if decision != Unprovable {
				t.Fatalf("decision = %s, want %s", decision, Unprovable)
			}
		})
	}
}
