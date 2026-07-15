package model

import "testing"

func TestKernelDomainIDValidateAndEqual(t *testing.T) {
	same := KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1"}
	if err := same.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1"}) {
		t.Fatalf("Equal() = false for matching domain")
	}
	if same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-2"}) {
		t.Fatalf("Equal() = true for different pid namespace")
	}
	if !same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1"}) {
		t.Fatalf("Equal() changed after mismatch check")
	}
	darwinStyle := KernelDomainID{HostBootID: "host-boot-1"}
	if err := darwinStyle.Validate(); err != nil {
		t.Fatalf("Validate() with empty pid namespace error = %v", err)
	}
}

func TestDecideGroupRecoveryUsesKernelDomain(t *testing.T) {
	ref := kernelDomainTestGroupRef()

	tests := []struct {
		name        string
		observation GroupRecoveryObservation
		want        GroupRecoveryDecision
	}{
		{
			name: "same full domain signals matching live leader",
			observation: GroupRecoveryObservation{
				KernelDomainID: KernelDomainID{HostBootID: ref.HostBootID, PIDNamespaceID: ref.PIDNamespaceID},
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			},
			want: GroupRecoverySignal,
		},
		{
			name: "same host boot but different pid namespace is host reboot equivalent",
			observation: GroupRecoveryObservation{
				KernelDomainID: KernelDomainID{HostBootID: ref.HostBootID, PIDNamespaceID: "pidns-2"},
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			},
			want: GroupRecoveryQuiescent,
		},
		{
			name: "legacy host boot observation reduces to boot id when durable pid namespace is empty",
			observation: GroupRecoveryObservation{
				HostBootID: ref.HostBootID,
				Group:      GroupLive,
				Leader:     ProcessIdentityMatching,
			},
			want: GroupRecoverySignal,
		},
	}

	legacyRef := ref
	legacyRef.PIDNamespaceID = ""
	tests[2].observation.HostBootID = legacyRef.HostBootID

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRef := ref
			if tt.name == "legacy host boot observation reduces to boot id when durable pid namespace is empty" {
				testRef = legacyRef
			}
			got, err := DecideGroupRecovery(testRef, tt.observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("DecideGroupRecovery() = %s, want %s", got, tt.want)
			}
		})
	}
}

func kernelDomainTestGroupRef() GroupRef {
	leader := ProcessIdentity{PID: 4321, HighResStartToken: "leader-start"}
	return GroupRef{
		Version:        1,
		CustodyID:      CustodyID("custody-1"),
		Launch:         LaunchKey{Attempt: AttemptRef{JobID: JobID("job-1"), AttemptID: AttemptID("attempt-1"), Epoch: 1}, Ordinal: LaunchOrdinal(1)},
		HostBootID:     "host-boot-1",
		PIDNamespaceID: "pidns-1",
		PGID:           leader.PID,
		Leader:         leader,
		Monitor:        ProcessIdentity{PID: 4322, HighResStartToken: "monitor-start"},
	}
}
