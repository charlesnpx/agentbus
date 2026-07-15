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
	groups := []GroupExistenceObservation{GroupLive, GroupAbsent}
	identities := []ProcessIdentityObservation{ProcessIdentityMatching, ProcessIdentityMissing, ProcessIdentityReused}
	descendants := []DescendantObservation{DescendantsPresent, DescendantsAbsent, DescendantsUnknown}

	count := 0
	for _, boot := range boots {
		for _, group := range groups {
			for _, leader := range identities {
				for _, monitor := range identities {
					for _, descendant := range descendants {
						observation := GroupRecoveryObservation{
							HostBootID:  boot.id,
							Group:       group,
							Leader:      leader,
							Monitor:     monitor,
							Descendants: descendant,
						}
						want := expectedGroupRecovery(ref, observation)
						count++
						t.Run(boot.name+"/"+string(group)+"/leader_"+string(leader)+"/monitor_"+string(monitor)+"/desc_"+string(descendant), func(t *testing.T) {
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
		}
	}
	if count != 108 {
		t.Fatalf("covered %d matrix cases, want 108", count)
	}
}

func TestDecideGroupRecoveryUnknownObservationsFailClosedOnSameBoot(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	tests := []GroupRecoveryObservation{
		{
			HostBootID:  ref.HostBootID,
			Group:       GroupExistenceUnknown,
			Leader:      ProcessIdentityMissing,
			Monitor:     ProcessIdentityMissing,
			Descendants: DescendantsAbsent,
		},
		{
			HostBootID:  ref.HostBootID,
			Group:       GroupAbsent,
			Leader:      ProcessIdentityUnknown,
			Monitor:     ProcessIdentityMissing,
			Descendants: DescendantsAbsent,
		},
		{
			HostBootID:  ref.HostBootID,
			Group:       GroupAbsent,
			Leader:      ProcessIdentityMissing,
			Monitor:     ProcessIdentityUnknown,
			Descendants: DescendantsAbsent,
		},
	}

	for _, observation := range tests {
		t.Run(string(observation.Group)+"/leader_"+string(observation.Leader)+"/monitor_"+string(observation.Monitor), func(t *testing.T) {
			got, err := DecideGroupRecovery(ref, observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery error = %v", err)
			}
			if got != GroupRecoveryUnprovable {
				t.Fatalf("decision = %s, want %s", got, GroupRecoveryUnprovable)
			}
		})
	}
}

func expectedGroupRecovery(ref GroupRef, observation GroupRecoveryObservation) GroupRecoveryDecision {
	if observation.HostBootID != ref.HostBootID {
		return GroupRecoveryQuiescent
	}
	if observation.Descendants != DescendantsAbsent &&
		(recoveryTestIdentityGone(observation.Leader) || recoveryTestIdentityGone(observation.Monitor)) {
		return GroupRecoveryUnprovable
	}
	if observation.Group == GroupAbsent &&
		recoveryTestIdentityGone(observation.Leader) &&
		recoveryTestIdentityGone(observation.Monitor) &&
		observation.Descendants == DescendantsAbsent {
		return GroupRecoveryQuiescent
	}
	if observation.Group == GroupLive && observation.Leader == ProcessIdentityMatching {
		return GroupRecoverySignal
	}
	return GroupRecoveryUnprovable
}

func recoveryTestIdentityGone(observation ProcessIdentityObservation) bool {
	switch observation {
	case ProcessIdentityMissing, ProcessIdentityReused:
		return true
	default:
		return false
	}
}
