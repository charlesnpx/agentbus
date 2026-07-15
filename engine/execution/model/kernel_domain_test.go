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
	unknown := KernelDomainID{HostBootID: "host-boot-1"}
	if err := unknown.Validate(); err != nil {
		t.Fatalf("Validate() with unknown pid namespace error = %v", err)
	}
	noNamespace := KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceState: PIDNamespaceNotApplicable}
	if err := noNamespace.Validate(); err != nil {
		t.Fatalf("Validate() with not-applicable pid namespace error = %v", err)
	}
	if !noNamespace.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceState: PIDNamespaceNotApplicable}) {
		t.Fatalf("Equal() = false for matching no-namespace platform domain")
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
			name: "same host boot with unknown pid namespace is unprovable",
			observation: GroupRecoveryObservation{
				KernelDomainID: KernelDomainID{HostBootID: ref.HostBootID},
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			},
			want: GroupRecoveryUnprovable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecideGroupRecovery(ref, tt.observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("DecideGroupRecovery() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestDecideGroupRecoveryPIDNamespaceKnowledge(t *testing.T) {
	knownRef := kernelDomainTestGroupRef()
	unknownRef := knownRef
	unknownRef.PIDNamespaceID = ""
	unknownRef.PIDNamespaceState = PIDNamespaceUnknown
	noNamespaceRef := unknownRef
	noNamespaceRef.PIDNamespaceState = PIDNamespaceNotApplicable

	tests := []struct {
		name        string
		ref         GroupRef
		observation GroupRecoveryObservation
		want        GroupRecoveryDecision
	}{
		{
			name: "nonempty reference and empty observation is unprovable",
			ref:  knownRef,
			observation: liveMatchingObservation(KernelDomainID{
				HostBootID: knownRef.HostBootID,
			}),
			want: GroupRecoveryUnprovable,
		},
		{
			name: "empty reference and nonempty observation is unprovable",
			ref:  unknownRef,
			observation: liveMatchingObservation(KernelDomainID{
				HostBootID:     knownRef.HostBootID,
				PIDNamespaceID: knownRef.PIDNamespaceID,
			}),
			want: GroupRecoveryUnprovable,
		},
		{
			name: "both unknown is unprovable",
			ref:  unknownRef,
			observation: liveMatchingObservation(KernelDomainID{
				HostBootID: knownRef.HostBootID,
			}),
			want: GroupRecoveryUnprovable,
		},
		{
			name: "both not applicable reduces to boot identity",
			ref:  noNamespaceRef,
			observation: liveMatchingObservation(KernelDomainID{
				HostBootID:        knownRef.HostBootID,
				PIDNamespaceState: PIDNamespaceNotApplicable,
			}),
			want: GroupRecoverySignal,
		},
		{
			name: "known differing namespaces are quiescent",
			ref:  knownRef,
			observation: liveMatchingObservation(KernelDomainID{
				HostBootID:     knownRef.HostBootID,
				PIDNamespaceID: "pidns-2",
			}),
			want: GroupRecoveryQuiescent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecideGroupRecovery(tt.ref, tt.observation)
			if err != nil {
				t.Fatalf("DecideGroupRecovery() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("DecideGroupRecovery() = %s, want %s", got, tt.want)
			}
		})
	}
}

func liveMatchingObservation(domain KernelDomainID) GroupRecoveryObservation {
	return GroupRecoveryObservation{
		KernelDomainID: domain,
		Group:          GroupLive,
		Leader:         ProcessIdentityMatching,
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
