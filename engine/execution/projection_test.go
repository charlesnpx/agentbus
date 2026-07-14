package execution

import "testing"

func TestEnumsMatchSpec(t *testing.T) {
	assertStrings(t, "Decision", decisionsToStrings(AllDecisions()), []string{"accepted", "awaiting_ack", "cancel_requested", "terminal"})
	assertStrings(t, "Dispatch", dispatchesToStrings(AllDispatches()), []string{"none", "scheduled", "supervisor_prepared", "permit_granted", "active", "reconciling", "contained", "result_publishing", "done"})
	assertStrings(t, "Outcome", outcomesToStrings(AllOutcomes()), []string{"none", "completed", "completed_noncompliant", "failed", "timed_out", "canceled", "reaped", "interrupted", "quarantined"})
	assertStrings(t, "Public", publicsToStrings(AllPublicStates()), []string{"queued", "starting", "running", "retrying", "completed", "completed_noncompliant", "interrupted", "quarantined", "failed", "timed_out", "canceled", "reaped", "orphaned"})
}

func TestPublicProjectionIsTotal(t *testing.T) {
	public := map[Public]bool{}
	for _, state := range AllPublicStates() {
		public[state] = true
	}
	for _, decision := range AllDecisions() {
		for _, dispatch := range AllDispatches() {
			for _, outcome := range AllOutcomes() {
				got := PublicProjection(decision, dispatch, outcome)
				if !public[got] {
					t.Fatalf("projection(%s,%s,%s) = %q, not a public state", decision, dispatch, outcome, got)
				}
				if ReachableInternal(decision, dispatch, outcome) && got == "" {
					t.Fatalf("reachable projection(%s,%s,%s) is empty", decision, dispatch, outcome)
				}
			}
		}
	}
}

func TestTerminalOutcomesProjectToTerminalPublicStates(t *testing.T) {
	tests := map[Outcome]Public{
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
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func dispatchesToStrings(in []Dispatch) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func outcomesToStrings(in []Outcome) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func publicsToStrings(in []Public) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}
