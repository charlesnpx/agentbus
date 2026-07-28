package model

import "testing"

func TestDecideGroupRecoveryMatrix(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	boots := []struct {
		name string
		id   string
	}{
		{name: "same_boot", id: ref.HostBootID},
		{name: "different_boot", id: "host-boot-after-restart"},
	}
	groups := []GroupExistenceObservation{
		GroupLive,
		GroupAbsent,
		GroupExistenceUnknown,
		GroupExistenceContradictory,
		GroupExistencePermissionDenied,
		GroupExistenceUnsupported,
	}
	identities := []ProcessIdentityObservation{
		ProcessIdentityMatching,
		ProcessIdentityMissing,
		ProcessIdentityReused,
		ProcessIdentityUnknown,
	}

	count := 0
	for _, boot := range boots {
		for _, group := range groups {
			for _, leader := range identities {
				observation := GroupRecoveryObservation{
					KernelDomainID: noPIDNamespaceDomain(boot.id),
					Group:          group,
					Leader:         leader,
				}
				want := expectedGroupRecovery(ref, observation)
				count++
				t.Run(boot.name+"/"+string(group)+"/leader_"+string(leader), func(t *testing.T) {
					got, err := DecideGroupRecovery(ref, observation)
					if err != nil {
						t.Fatalf("DecideGroupRecovery error = %v", err)
					}
					if got != want {
						t.Fatalf("decision = %s, want %s", got, want)
					}
				})
			}
		}
	}
	if count != 48 {
		t.Fatalf("covered %d matrix cases, want 48", count)
	}
}

func TestDecideGroupRecoveryMonitorStateIsNotCorrectnessInput(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	priorMonitorStates := []ProcessIdentityObservation{
		ProcessIdentityMissing,
		ProcessIdentityReused,
		ProcessIdentityMatching,
	}

	for _, priorMonitor := range priorMonitorStates {
		t.Run("prior_monitor_"+string(priorMonitor), func(t *testing.T) {
			observation := GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			}
			got, err := DecideGroupRecovery(ref, observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery error = %v", err)
			}
			if got != GroupRecoverySignal {
				t.Fatalf("decision = %s, want %s", got, GroupRecoverySignal)
			}
		})
	}
}

func TestDecideGroupRecoveryFailClosedOnSameBoot(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	tests := []struct {
		name        string
		observation GroupRecoveryObservation
	}{
		{
			name: "unknown_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupExistenceUnknown,
				Leader:         ProcessIdentityMatching,
			},
		},
		{
			name: "contradictory_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupExistenceContradictory,
				Leader:         ProcessIdentityMatching,
			},
		},
		{
			name: "permission_denied_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupExistencePermissionDenied,
				Leader:         ProcessIdentityMatching,
			},
		},
		{
			name: "unsupported_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupExistenceUnsupported,
				Leader:         ProcessIdentityMatching,
			},
		},
		{
			name: "leader_missing_on_live_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupLive,
				Leader:         ProcessIdentityMissing,
			},
		},
		{
			name: "leader_reused_on_live_group",
			observation: GroupRecoveryObservation{
				KernelDomainID: noPIDNamespaceDomain(ref.HostBootID),
				Group:          GroupLive,
				Leader:         ProcessIdentityReused,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := DecideGroupRecovery(ref, test.observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery error = %v", err)
			}
			if got != GroupRecoveryUnprovable {
				t.Fatalf("decision = %s, want %s", got, GroupRecoveryUnprovable)
			}
		})
	}
}

func TestDecideGroupRecoveryLegacyHostBootOnlyObservationIsUnprovable(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	observation := GroupRecoveryObservation{
		HostBootID: ref.HostBootID,
		Group:      GroupLive,
		Leader:     ProcessIdentityMatching,
	}
	domain, err := observation.kernelDomain()
	if err != nil {
		t.Fatalf("kernelDomain() error = %v", err)
	}
	if domain.PIDNamespaceState != PIDNamespaceUnknown {
		t.Fatalf("kernelDomain PIDNamespaceState = %v, want %v", domain.PIDNamespaceState, PIDNamespaceUnknown)
	}
	got, err := DecideGroupRecovery(ref, observation)
	if err != nil {
		t.Fatalf("DecideGroupRecovery error = %v", err)
	}
	if got != GroupRecoveryUnprovable {
		t.Fatalf("decision = %s, want %s", got, GroupRecoveryUnprovable)
	}
}

func TestDecideGroupRecoveryExplicitUnknownObservationRemainsUnprovable(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	observation := GroupRecoveryObservation{
		KernelDomainID: KernelDomainID{
			HostBootID:        ref.HostBootID,
			PIDNamespaceState: PIDNamespaceUnknown,
		},
		Group:  GroupLive,
		Leader: ProcessIdentityMatching,
	}
	got, err := DecideGroupRecovery(ref, observation)
	if err != nil {
		t.Fatalf("DecideGroupRecovery error = %v", err)
	}
	if got != GroupRecoveryUnprovable {
		t.Fatalf("decision = %s, want %s", got, GroupRecoveryUnprovable)
	}
}

func expectedGroupRecovery(ref GroupRef, observation GroupRecoveryObservation) GroupRecoveryDecision {
	observationDomain, err := observation.kernelDomain()
	if err != nil {
		return GroupRecoveryUnprovable
	}
	relation, err := compareKernelDomain(ref.KernelDomain(), observationDomain)
	if err != nil {
		return GroupRecoveryUnprovable
	}
	if relation == kernelDomainDifferent {
		return GroupRecoveryQuiescent
	}
	if relation == kernelDomainUnprovable {
		return GroupRecoveryUnprovable
	}
	if observation.Group == GroupAbsent {
		return GroupRecoveryQuiescent
	}
	if observation.Group == GroupLive && observation.Leader == ProcessIdentityMatching {
		return GroupRecoverySignal
	}
	return GroupRecoveryUnprovable
}

func noPIDNamespaceDomain(hostBootID string) KernelDomainID {
	return KernelDomainID{HostBootID: hostBootID, PIDNamespaceState: PIDNamespaceNotApplicable}
}
