package model

import "testing"

func TestDecideGroupRecoveryMatrix(t *testing.T) {
	ref := reducerGroup(LaunchOrdinalOne)
	tests := []struct {
		name        string
		observation GroupRecoveryObservation
		want        GroupRecoveryDecision
	}{
		{
			name: "different boot is absent",
			observation: GroupRecoveryObservation{
				HostBootID:  "host-boot-after-restart",
				Group:       GroupExistenceUnknown,
				Leader:      ProcessIdentityUnknown,
				Monitor:     ProcessIdentityUnknown,
				Descendants: DescendantsUnknown,
			},
			want: GroupRecoveryQuiescent,
		},
		{
			name: "same boot matching leader and live group signals",
			observation: GroupRecoveryObservation{
				HostBootID:  ref.HostBootID,
				Group:       GroupLive,
				Leader:      ProcessIdentityMatching,
				Monitor:     ProcessIdentityMatching,
				Descendants: DescendantsPresent,
			},
			want: GroupRecoverySignal,
		},
		{
			name: "same boot absent group is quiescent",
			observation: GroupRecoveryObservation{
				HostBootID:  ref.HostBootID,
				Group:       GroupAbsent,
				Leader:      ProcessIdentityMissing,
				Monitor:     ProcessIdentityMissing,
				Descendants: DescendantsAbsent,
			},
			want: GroupRecoveryQuiescent,
		},
		{
			name: "same boot reused leader while group exists is unprovable",
			observation: GroupRecoveryObservation{
				HostBootID:  ref.HostBootID,
				Group:       GroupLive,
				Leader:      ProcessIdentityReused,
				Monitor:     ProcessIdentityMatching,
				Descendants: DescendantsPresent,
			},
			want: GroupRecoveryUnprovable,
		},
		{
			name: "same boot missing leader while group exists is unprovable",
			observation: GroupRecoveryObservation{
				HostBootID:  ref.HostBootID,
				Group:       GroupLive,
				Leader:      ProcessIdentityMissing,
				Monitor:     ProcessIdentityMatching,
				Descendants: DescendantsPresent,
			},
			want: GroupRecoveryUnprovable,
		},
		{
			name: "leader and monitor gone with unknown descendants is unprovable",
			observation: GroupRecoveryObservation{
				HostBootID:  ref.HostBootID,
				Group:       GroupExistenceUnknown,
				Leader:      ProcessIdentityMissing,
				Monitor:     ProcessIdentityMissing,
				Descendants: DescendantsUnknown,
			},
			want: GroupRecoveryUnprovable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecideGroupRecovery(ref, tt.observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("decision = %s, want %s", got, tt.want)
			}
		})
	}
}
