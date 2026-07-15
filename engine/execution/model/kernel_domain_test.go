package model

import (
	"encoding/json"
	"testing"
)

func TestKernelDomainIDValidateAndEqual(t *testing.T) {
	same := KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1", PIDNamespaceState: PIDNamespaceKnown}
	if err := same.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if !same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1", PIDNamespaceState: PIDNamespaceKnown}) {
		t.Fatalf("Equal() = false for matching domain")
	}
	if same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-2", PIDNamespaceState: PIDNamespaceKnown}) {
		t.Fatalf("Equal() = true for different pid namespace")
	}
	if !same.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1", PIDNamespaceState: PIDNamespaceKnown}) {
		t.Fatalf("Equal() changed after mismatch check")
	}
	unknown := KernelDomainID{HostBootID: "host-boot-1"}
	if err := unknown.Validate(); err != nil {
		t.Fatalf("Validate() with unknown pid namespace error = %v", err)
	}
	if !unknown.Equal(KernelDomainID{HostBootID: "host-boot-1"}) {
		t.Fatalf("Equal() = false for matching unknown namespace domain")
	}
	if unknown.ProvablySame(KernelDomainID{HostBootID: "host-boot-1"}) {
		t.Fatalf("ProvablySame() = true for matching unknown namespace domain")
	}
	noNamespace := KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceState: PIDNamespaceNotApplicable}
	if err := noNamespace.Validate(); err != nil {
		t.Fatalf("Validate() with not-applicable pid namespace error = %v", err)
	}
	if !noNamespace.Equal(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceState: PIDNamespaceNotApplicable}) {
		t.Fatalf("Equal() = false for matching no-namespace platform domain")
	}
	if !noNamespace.ProvablySame(KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceState: PIDNamespaceNotApplicable}) {
		t.Fatalf("ProvablySame() = false for matching no-namespace platform domain")
	}
	if withNamespace, err := NewKernelDomainID("host-boot-1", "pidns-1"); err != nil {
		t.Fatalf("NewKernelDomainID() error = %v", err)
	} else if withNamespace.PIDNamespaceState != PIDNamespaceKnown {
		t.Fatalf("NewKernelDomainID() state = %v, want %v", withNamespace.PIDNamespaceState, PIDNamespaceKnown)
	}
	if _, err := NewKernelDomainID("host-boot-1", ""); err == nil {
		t.Fatalf("NewKernelDomainID() accepted empty namespace")
	}
	if withoutNamespace, err := NewKernelDomainIDWithoutPIDNamespace("host-boot-1"); err != nil {
		t.Fatalf("NewKernelDomainIDWithoutPIDNamespace() error = %v", err)
	} else if withoutNamespace.PIDNamespaceState != PIDNamespaceNotApplicable {
		t.Fatalf("NewKernelDomainIDWithoutPIDNamespace() state = %v, want %v", withoutNamespace.PIDNamespaceState, PIDNamespaceNotApplicable)
	}
	if err := (KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1", PIDNamespaceState: PIDNamespaceUnknown}).Validate(); err == nil {
		t.Fatal("Validate() accepted non-empty pid namespace with unknown state")
	}
	if err := (KernelDomainID{HostBootID: "host-boot-1", PIDNamespaceID: "pidns-1", PIDNamespaceState: PIDNamespaceNotApplicable}).Validate(); err == nil {
		t.Fatal("Validate() accepted non-empty pid namespace with not-applicable state")
	}
}

func TestGroupRefEqualUsesCanonicalKernelDomain(t *testing.T) {
	left := kernelDomainTestGroupRef()
	right := left
	if !left.Equal(right) {
		t.Fatal("GroupRef.Equal() = false for identical known kernel domain")
	}
	if !left.SamePhysicalIdentity(right) {
		t.Fatal("SamePhysicalIdentity() = false for identical known kernel domain")
	}
	if !left.KernelDomain().Equal(right.KernelDomain()) {
		t.Fatal("KernelDomain().Equal() = false for identical known kernel domain")
	}

	legacyLeft := left
	legacyLeft.PIDNamespaceID = ""
	legacyLeft.PIDNamespaceState = PIDNamespaceUnknown
	legacyRight := legacyLeft
	if !legacyLeft.Equal(legacyRight) {
		t.Fatal("GroupRef.Equal() = false for identical unknown kernel domain")
	}
	if legacyLeft.SamePhysicalIdentity(legacyRight) {
		t.Fatal("SamePhysicalIdentity() = true for identical unknown kernel domain")
	}
	if !legacyLeft.KernelDomain().Equal(legacyRight.KernelDomain()) {
		t.Fatal("KernelDomain().Equal() = false for identical unknown kernel domain")
	}
	if legacyLeft.KernelDomain().ProvablySame(legacyRight.KernelDomain()) {
		t.Fatal("KernelDomain().ProvablySame() = true for identical unknown kernel domain")
	}
}

func TestGroupRefDecodeLegacyHostBootOnlyUsesUnknownPIDNamespace(t *testing.T) {
	original := kernelDomainTestGroupRef()
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("Unmarshal fields error = %v", err)
	}
	delete(fields, "PIDNamespaceID")
	delete(fields, "PIDNamespaceState")
	legacy, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("Marshal legacy fields error = %v", err)
	}

	var decoded GroupRef
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatalf("Unmarshal legacy GroupRef error = %v", err)
	}
	if decoded.PIDNamespaceState != PIDNamespaceUnknown {
		t.Fatalf("decoded PIDNamespaceState = %v, want %v", decoded.PIDNamespaceState, PIDNamespaceUnknown)
	}
	if decoded.PIDNamespaceID != "" {
		t.Fatalf("decoded PIDNamespaceID = %q, want empty", decoded.PIDNamespaceID)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatalf("decoded legacy GroupRef Validate() error = %v", err)
	}
	record := validSafetyRecord()
	decoded.Launch.Attempt = record.Attempt.Ref
	record.Attempt.Launches.First.Group = &decoded
	record.Attempt.Launches.First.Quiescence = &QuiescenceCertificate{
		Attempt:     record.Attempt.Ref,
		Ordinal:     LaunchOrdinalOne,
		Group:       decoded,
		Method:      QuiescenceAlreadyAbsent,
		CertifiedBy: record.AdmittedBy,
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("legacy unknown GroupRef failed safety internal-consistency validation: %v", err)
	}
	decision, err := DecideGroupRecovery(decoded, liveMatchingObservation(KernelDomainID{
		HostBootID:        decoded.HostBootID,
		PIDNamespaceState: PIDNamespaceNotApplicable,
	}))
	if err != nil {
		t.Fatalf("DecideGroupRecovery() legacy GroupRef error = %v", err)
	}
	if decision != GroupRecoveryUnprovable {
		t.Fatalf("legacy recovery decision = %s, want %s", decision, GroupRecoveryUnprovable)
	}
}

func TestLegacyHostBootOnlyObservationIsUnknownAndUnprovable(t *testing.T) {
	ref := kernelDomainTestGroupRef()
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
		t.Fatalf("legacy observation PIDNamespaceState = %v, want %v", domain.PIDNamespaceState, PIDNamespaceUnknown)
	}
	got, err := DecideGroupRecovery(ref, observation)
	if err != nil {
		t.Fatalf("DecideGroupRecovery() error = %v", err)
	}
	if got != GroupRecoveryUnprovable {
		t.Fatalf("DecideGroupRecovery() = %s, want %s", got, GroupRecoveryUnprovable)
	}
}

func TestExplicitNoPIDNamespaceDomainUsesBootEquality(t *testing.T) {
	left, err := NewKernelDomainIDWithoutPIDNamespace("host-boot-1")
	if err != nil {
		t.Fatalf("NewKernelDomainIDWithoutPIDNamespace() left error = %v", err)
	}
	right, err := NewKernelDomainIDWithoutPIDNamespace("host-boot-1")
	if err != nil {
		t.Fatalf("NewKernelDomainIDWithoutPIDNamespace() right error = %v", err)
	}
	if !left.Equal(right) {
		t.Fatalf("Equal() = false for explicit no-PID-namespace boot equality")
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
				KernelDomainID: KernelDomainID{HostBootID: ref.HostBootID, PIDNamespaceID: ref.PIDNamespaceID, PIDNamespaceState: PIDNamespaceKnown},
				Group:          GroupLive,
				Leader:         ProcessIdentityMatching,
			},
			want: GroupRecoverySignal,
		},
		{
			name: "same host boot but different pid namespace is host reboot equivalent",
			observation: GroupRecoveryObservation{
				KernelDomainID: KernelDomainID{HostBootID: ref.HostBootID, PIDNamespaceID: "pidns-2", PIDNamespaceState: PIDNamespaceKnown},
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
				HostBootID:        knownRef.HostBootID,
				PIDNamespaceID:    knownRef.PIDNamespaceID,
				PIDNamespaceState: PIDNamespaceKnown,
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
				HostBootID:        knownRef.HostBootID,
				PIDNamespaceID:    "pidns-2",
				PIDNamespaceState: PIDNamespaceKnown,
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
		Version:           1,
		CustodyID:         CustodyID("custody-1"),
		Launch:            LaunchKey{Attempt: AttemptRef{JobID: JobID("job-1"), AttemptID: AttemptID("attempt-1"), Epoch: 1}, Ordinal: LaunchOrdinal(1)},
		HostBootID:        "host-boot-1",
		PIDNamespaceID:    "pidns-1",
		PIDNamespaceState: PIDNamespaceKnown,
		PGID:              leader.PID,
		Leader:            leader,
		Monitor:           ProcessIdentity{PID: 4322, HighResStartToken: "monitor-start"},
	}
}
