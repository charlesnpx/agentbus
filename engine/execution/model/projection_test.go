package model

import "testing"

func TestProjectionEnumsMatchProtocolStrings(t *testing.T) {
	assertStrings(t, "Decision", decisionsToStrings(AllDecisions()), []string{"accepted", "awaiting_ack", "cancel_requested", "terminal"})
	assertStrings(t, "Dispatch", dispatchesToStrings(AllDispatches()), []string{"none", "scheduled", "supervisor_prepared", "permit_granted", "active", "reconciling", "contained", "result_publishing", "done"})
	assertStrings(t, "Outcome", outcomesToStrings(AllOutcomes()), []string{"none", "completed", "completed_noncompliant", "failed", "timed_out", "canceled", "reaped", "interrupted", "quarantined"})
	assertStrings(t, "PublicState", publicsToStrings(AllPublicStates()), []string{"queued", "starting", "running", "retrying", "completed", "completed_noncompliant", "interrupted", "quarantined", "failed", "timed_out", "canceled", "reaped", "orphaned"})
	assertStrings(t, "TerminalCause", causesToStrings(AllTerminalCauses()), []string{"completed_normally", "canceled_before_authorization", "canceled_after_authorization", "daemon_restarted_before_authorization", "daemon_restarted_after_authorization", "supervisor_lost_before_authorization", "supervisor_lost_after_authorization", "corrupt_projection", "response_undeliverable", "release_outcome_unknown", "release_definitely_not_sent"})
}

func TestPublicProjectionIsDefinedForReachableTuples(t *testing.T) {
	public := map[PublicState]bool{}
	for _, state := range AllPublicStates() {
		public[state] = true
	}
	for _, decision := range AllDecisions() {
		for _, dispatch := range AllDispatches() {
			for _, outcome := range AllOutcomes() {
				got := PublicProjection(decision, dispatch, outcome)
				if got == 0 {
					if ReachableInternal(decision, dispatch, outcome) {
						t.Fatalf("reachable projection(%s,%s,%s) is empty", decision, dispatch, outcome)
					}
					continue
				}
				if !public[got] {
					t.Fatalf("projection(%s,%s,%s) = %q, not a public state", decision, dispatch, outcome, got)
				}
				if terminalPublicState(got) && (decision != DecisionTerminal || dispatch != DispatchDone || !terminalOutcome(outcome)) {
					t.Fatalf("projection(%s,%s,%s) = %q, terminal public before terminal tuple", decision, dispatch, outcome, got)
				}
			}
		}
	}
}

func TestTerminalOutcomesProjectToTerminalPublicStates(t *testing.T) {
	tests := map[Outcome]PublicState{
		OutcomeCompleted:             PublicCompleted,
		OutcomeCompletedNoncompliant: PublicCompletedNoncompliant,
		OutcomeFailed:                PublicFailed,
		OutcomeTimedOut:              PublicTimedOut,
		OutcomeCanceled:              PublicCanceled,
		OutcomeReaped:                PublicReaped,
		OutcomeInterrupted:           PublicInterrupted,
		OutcomeQuarantined:           PublicQuarantined,
	}
	for outcome, want := range tests {
		got := PublicProjection(DecisionTerminal, DispatchDone, outcome)
		if got != want {
			t.Fatalf("terminal %s projected to %s, want %s", outcome, got, want)
		}
	}
}

func TestProjectDerivesReadModelFromSafetyRecordOnly(t *testing.T) {
	record := validSafetyRecord()
	projection, err := Project(record, ProjectionMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("project scheduled record: %v", err)
	}
	if projection.Decision != DecisionAccepted || projection.Dispatch != DispatchPermitGranted || projection.Outcome != OutcomeNone || projection.Public != PublicStarting {
		t.Fatalf("projection = decision:%s dispatch:%s outcome:%s public:%s", projection.Decision, projection.Dispatch, projection.Outcome, projection.Public)
	}

	release := LaunchReleaseFact{
		Attempt:     record.Attempt.Ref,
		Ordinal:     LaunchOrdinalOne,
		Nonce:       "nonce-1",
		Child:       mustChild(t),
		ReleasedBy:  record.AdmittedBy,
		Observation: mustEvidence(t, "started", "child observed"),
	}
	record.Attempt.Launches.First.Released = &release
	projection, err = Project(record, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("project active record: %v", err)
	}
	if projection.Dispatch != DispatchActive || projection.Public != PublicRunning {
		t.Fatalf("active projection = dispatch:%s public:%s", projection.Dispatch, projection.Public)
	}

	result := ResultRef{Path: "results/job-0001.txt", Digest: "sha256:abc123", Bytes: 3}
	record.Outcome = &OutcomeFact{Attempt: record.Attempt.Ref, Outcome: OutcomeCompleted}
	record.Attempt.Launches.First.Quiescence = &QuiescenceCertificate{
		Attempt:     record.Attempt.Ref,
		Ordinal:     LaunchOrdinalOne,
		Group:       *record.Attempt.Launches.First.Group,
		Method:      QuiescenceNaturalExit,
		CertifiedBy: record.AdmittedBy,
	}
	record.Result = &ResultCertificate{
		JobID:       record.JobID,
		Result:      result,
		DirSynced:   mustEvidence(t, "dir_synced", "result directory fsynced"),
		CertifiedBy: record.AdmittedBy,
	}
	record.Terminal = &TerminalCertificate{
		JobID:               record.JobID,
		Attempt:             record.Attempt.Ref,
		Outcome:             OutcomeCompleted,
		Proof:               ProofCleanQuiescentOutcomeAndRetired,
		Cause:               CauseCompletedNormally,
		DerivedFromRevision: record.Revision,
		DerivedBy:           record.AdmittedBy,
		Result:              &result,
	}
	projection, err = Project(record, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("project terminal record: %v", err)
	}
	if projection.Decision != DecisionTerminal || projection.Dispatch != DispatchDone || projection.Outcome != OutcomeCompleted || projection.Public != PublicCompleted || projection.TerminalCause != CauseCompletedNormally {
		t.Fatalf("terminal projection = decision:%s dispatch:%s outcome:%s public:%s cause:%s", projection.Decision, projection.Dispatch, projection.Outcome, projection.Public, projection.TerminalCause)
	}
}

func TestReachableInternalRejectsCompletedPermitGranted(t *testing.T) {
	if ReachableInternal(DecisionAccepted, DispatchPermitGranted, OutcomeCompleted) {
		t.Fatal("ReachableInternal accepted completed outcome with permit-granted dispatch")
	}
}

func assertStrings(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}

func decisionsToStrings(in []Decision) []string {
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = value.String()
	}
	return out
}

func dispatchesToStrings(in []Dispatch) []string {
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = value.String()
	}
	return out
}

func outcomesToStrings(in []Outcome) []string {
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = value.String()
	}
	return out
}

func publicsToStrings(in []PublicState) []string {
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = value.String()
	}
	return out
}

func causesToStrings(in []TerminalCause) []string {
	out := make([]string, len(in))
	for i, value := range in {
		out[i] = value.String()
	}
	return out
}
