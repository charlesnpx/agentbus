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
					HostBootID: boot.id,
					Group:      group,
					Leader:     leader,
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
				HostBootID: ref.HostBootID,
				Group:      GroupLive,
				Leader:     ProcessIdentityMatching,
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
				HostBootID: ref.HostBootID,
				Group:      GroupExistenceUnknown,
				Leader:     ProcessIdentityMatching,
			},
		},
		{
			name: "contradictory_group",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupExistenceContradictory,
				Leader:     ProcessIdentityMatching,
			},
		},
		{
			name: "permission_denied_group",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupExistencePermissionDenied,
				Leader:     ProcessIdentityMatching,
			},
		},
		{
			name: "unsupported_group",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupExistenceUnsupported,
				Leader:     ProcessIdentityMatching,
			},
		},
		{
			name: "leader_missing_on_live_group",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupLive,
				Leader:     ProcessIdentityMissing,
			},
		},
		{
			name: "leader_reused_on_live_group",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupLive,
				Leader:     ProcessIdentityReused,
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

func expectedGroupRecovery(ref GroupRef, observation GroupRecoveryObservation) GroupRecoveryDecision {
	if observation.HostBootID != ref.HostBootID {
		return GroupRecoveryQuiescent
	}
	if observation.Group == GroupAbsent {
		return GroupRecoveryQuiescent
	}
	if observation.Group == GroupLive && observation.Leader == ProcessIdentityMatching {
		return GroupRecoverySignal
	}
	return GroupRecoveryUnprovable
}
