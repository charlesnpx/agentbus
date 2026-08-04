package model

import (
	"encoding/json"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
)

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

func TestRecoveryTerminalFromOutcomeFactCarriesContractStamp(t *testing.T) {
	compliantStamp := &engine.ContractStamp{
		Status:         engine.ContractCompliant,
		Missing:        []string{},
		ContractSHA256: "sha256:compliant",
		Attempts:       1,
	}
	compliant := recoveryFinalizeRecordedCompletion(t, OutcomeCompleted, compliantStamp)
	if !contractStampEqual(compliant.Terminal.Contract, compliantStamp) {
		t.Fatalf("compliant terminal contract = %#v, want %#v", compliant.Terminal.Contract, compliantStamp)
	}

	noncompliantStamp := &engine.ContractStamp{
		Status:         engine.ContractNoncompliant,
		Missing:        []string{"Findings"},
		Reason:         "response missed structural requirements",
		ContractSHA256: "sha256:noncompliant",
		Attempts:       1,
	}
	noncompliant := recoveryFinalizeRecordedCompletion(t, OutcomeCompletedNoncompliant, noncompliantStamp)
	if !contractStampEqual(noncompliant.Terminal.Contract, noncompliantStamp) {
		t.Fatalf("noncompliant terminal contract = %#v, want %#v", noncompliant.Terminal.Contract, noncompliantStamp)
	}
}

func TestRecoveryTerminalFromLegacyOutcomeFactHasNilContract(t *testing.T) {
	record := reducerQuiescentRecord(t)
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted})
	record = reducerMustApply(t, record, reducerResultCommand(t, record))
	record = decodeLegacyOutcomeFactWithoutContract(t, record)

	intent, err := RecoveryTerminalIntent(record, RecoveryStartupLoss, true)
	if err != nil {
		t.Fatalf("RecoveryTerminalIntent legacy outcome error = %v", err)
	}
	if intent.Contract != nil {
		t.Fatalf("legacy recovery intent contract = %#v, want nil", intent.Contract)
	}

	finalized := recoveryFinalize(t, record)
	if finalized.Terminal.Contract != nil {
		t.Fatalf("legacy terminal contract = %#v, want nil", finalized.Terminal.Contract)
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

func recoveryFinalizeRecordedCompletion(t *testing.T, outcome Outcome, stamp *engine.ContractStamp) SafetyRecord {
	t.Helper()
	record := reducerQuiescentRecord(t)
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: outcome, Contract: stamp})
	record = reducerMustApply(t, record, reducerResultCommand(t, record))
	if !contractStampEqual(record.Outcome.Contract, stamp) {
		t.Fatalf("outcome contract = %#v, want %#v", record.Outcome.Contract, stamp)
	}
	return recoveryFinalize(t, record)
}

func recoveryFinalize(t *testing.T, record SafetyRecord) SafetyRecord {
	t.Helper()
	plan, err := PlanRecovery(record, RecoveryStartupLoss)
	if err != nil {
		t.Fatalf("PlanRecovery error = %v", err)
	}
	if plan.Next.Kind != RecoveryFinalizeCertified || plan.Next.Finalize == nil {
		t.Fatalf("recovery action = %#v, want certified finalize", plan.Next)
	}
	if !contractStampEqual(plan.Next.Finalize.Intent.Contract, record.Outcome.Contract) {
		t.Fatalf("recovery intent contract = %#v, want %#v", plan.Next.Finalize.Intent.Contract, record.Outcome.Contract)
	}
	finalized, err := apply(record, *plan.Next.Finalize)
	if err != nil {
		t.Fatalf("recovery Finalize error = %v", err)
	}
	if finalized.Record.Terminal == nil {
		t.Fatal("recovery terminal missing")
	}
	return finalized.Record
}

func decodeLegacyOutcomeFactWithoutContract(t *testing.T, record SafetyRecord) SafetyRecord {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal safety record: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("unmarshal safety record object: %v", err)
	}
	var outcome map[string]json.RawMessage
	if err := json.Unmarshal(fields["Outcome"], &outcome); err != nil {
		t.Fatalf("unmarshal outcome object: %v", err)
	}
	delete(outcome, "Contract")
	fields["Outcome"], err = json.Marshal(outcome)
	if err != nil {
		t.Fatalf("marshal legacy outcome object: %v", err)
	}
	raw, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal legacy safety record: %v", err)
	}
	var decoded SafetyRecord
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode legacy safety record: %v", err)
	}
	if decoded.Outcome == nil || decoded.Outcome.Contract != nil {
		t.Fatalf("decoded legacy outcome = %#v, want nil contract", decoded.Outcome)
	}
	if err := ValidateSafetyRecord(decoded); err != nil {
		t.Fatalf("legacy safety record failed validation: %v", err)
	}
	return decoded
}
