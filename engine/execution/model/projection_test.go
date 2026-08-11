package model

import (
	"testing"
	"time"
)

func TestProjectionEnumsMatchProtocolStrings(t *testing.T) {
	assertStrings(t, "Decision", decisionsToStrings(AllDecisions()), []string{"accepted", "awaiting_ack", "cancel_requested", "terminal"})
	assertStrings(t, "Dispatch", dispatchesToStrings(AllDispatches()), []string{"none", "scheduled", "supervisor_prepared", "permit_granted", "active", "reconciling", "contained", "result_publishing", "done"})
	assertStrings(t, "Outcome", outcomesToStrings(AllOutcomes()), []string{"none", "completed", "completed_noncompliant", "failed", "timed_out", "canceled", "reaped", "interrupted", "quarantined", "orphaned"})
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
		OutcomeOrphaned:              PublicOrphaned,
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
	if projection.FinalAttemptStartedAt != nil || projection.FinalAttemptEndedAt != nil {
		t.Fatalf("legacy projection timing = start:%v end:%v, want absent", projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt)
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
	if projection.FinalAttemptStartedAt != nil || projection.FinalAttemptEndedAt != nil {
		t.Fatalf("active projection timing = start:%v end:%v, want absent", projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt)
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
	startedAt := time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(3 * time.Second)
	record.FinalAttemptStartedAt = &startedAt
	record.FinalAttemptEndedAt = &endedAt
	projection, err = Project(record, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("project terminal record: %v", err)
	}
	if projection.Decision != DecisionTerminal || projection.Dispatch != DispatchDone || projection.Outcome != OutcomeCompleted || projection.Public != PublicCompleted || projection.TerminalCause != CauseCompletedNormally {
		t.Fatalf("terminal projection = decision:%s dispatch:%s outcome:%s public:%s cause:%s", projection.Decision, projection.Dispatch, projection.Outcome, projection.Public, projection.TerminalCause)
	}
	if projection.FinalAttemptStartedAt == nil || !projection.FinalAttemptStartedAt.Equal(startedAt) || projection.FinalAttemptEndedAt == nil || !projection.FinalAttemptEndedAt.Equal(endedAt) {
		t.Fatalf("terminal projection timing = start:%v end:%v, want %s/%s", projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt, startedAt, endedAt)
	}
}

func TestProjectSuppressesPartialTerminalFinalAttemptTiming(t *testing.T) {
	startedAt := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)
	record := reducerCanceledRetiredRecord(t)
	record = reducerMustApply(t, record, RecordFinalAttemptStart{JobID: reducerJobID(), StartedAt: startedAt})
	record = reducerMustApply(t, record, Finalize{
		Ref:    reducerRef(),
		Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization},
	})
	if record.FinalAttemptStartedAt == nil || !record.FinalAttemptStartedAt.Equal(startedAt) || record.FinalAttemptEndedAt != nil {
		t.Fatalf("durable partial timing = start:%v end:%v, want %s/<nil>", record.FinalAttemptStartedAt, record.FinalAttemptEndedAt, startedAt)
	}

	projection, err := Project(record, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("Project partial terminal timing: %v", err)
	}
	if projection.FinalAttemptStartedAt != nil || projection.FinalAttemptEndedAt != nil {
		t.Fatalf("partial terminal projection timing = start:%v end:%v, want absent", projection.FinalAttemptStartedAt, projection.FinalAttemptEndedAt)
	}
}

func TestProjectKeepsCompletedOutcomeWhenCleanupUnresolved(t *testing.T) {
	record := reducerConsumedRecord(t)
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted})
	record = reducerMustApply(t, record, reducerResultCommand(t, record))
	finalized := reducerMustApply(t, record, Finalize{
		Ref:    reducerRef(),
		Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseDaemonRestartedAfterAuthorization},
	})
	if finalized.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if finalized.Terminal.Proof != ProofUnresolvedAbsence {
		t.Fatalf("terminal proof = %s, want %s", finalized.Terminal.Proof, ProofUnresolvedAbsence)
	}
	if finalized.Terminal.Result == nil {
		t.Fatal("completed unresolved terminal lost result")
	}
	if got := DeriveCleanupDisposition(finalized); got != CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, CleanupDispositionUnresolved)
	}

	projection, err := Project(finalized, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("Project completed unresolved terminal: %v", err)
	}
	if projection.Outcome != OutcomeCompleted || projection.Public != PublicCompleted {
		t.Fatalf("projection outcome/public = %s/%s, want completed/completed", projection.Outcome, projection.Public)
	}
	if projection.Public == PublicOrphaned {
		t.Fatal("completed unresolved cleanup projected as orphaned")
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
