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
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
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

func TestContainmentRetainedObjectEmptyProofProvesAbsent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
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

func TestContainmentRetainedObjectMembersContradictIndependentAbsent(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	decision, err := DecideContainmentAuthorization(ContainmentAuthorization{
		Group: ref,
		Observation: ContainmentObservation{
			KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
			Group:          GroupAbsent,
			Leader:         ProcessIdentityMissing,
		},
		RetainedObject: RetainedObjectProofMembersPresent,
	})
	if err != nil {
		t.Fatalf("DecideContainmentAuthorization error = %v", err)
	}
	if decision != Unprovable {
		t.Fatalf("decision = %s, want %s", decision, Unprovable)
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
