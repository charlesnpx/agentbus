package execution

import "testing"

func TestIdentifiedLifecycleCompletes(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-life", "owner")
	res, err := c.Submit(modelRequest("ws-life", "req-life", "fp-life"), nil)
	if err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	if err := c.GrantPermit(res.JobID, 1, "nonce-1", nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	if err := c.Complete(res.JobID, OutcomeCompleted); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, ok := c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("job missing")
	}
	if !job.Terminal() || job.Public() != PublicCompleted || job.TerminalProof != ProofCleanQuiescentOutcomeAndRetired {
		t.Fatalf("job = %+v, want completed with clean proof", job)
	}
	if c.HasOwnedWork() {
		t.Fatalf("owned work remains after terminal commit")
	}
}

func TestCancelBeforePermitDoesNotStart(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-cancel", "owner")
	res, err := c.Submit(modelRequest("ws-cancel", "req-cancel", "fp-cancel"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(res.JobID); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ := c.Store.GetJob(res.JobID)
	if job.ExecutionSideEffects != 0 {
		t.Fatalf("execution side effects = %d, want 0", job.ExecutionSideEffects)
	}
	if job.Outcome != OutcomeCanceled || job.TerminalProof != ProofNeverPermittedAndRetired {
		t.Fatalf("job = %+v, want canceled never-permitted proof", job)
	}
}

func TestCancelAfterPermitContainsBeforeTerminal(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-cancel2", "owner")
	res, err := c.Submit(modelRequest("ws-cancel2", "req-cancel2", "fp-cancel2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.GrantPermit(res.JobID, 1, "nonce-1", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Cancel(res.JobID); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ := c.Store.GetJob(res.JobID)
	if job.Outcome != OutcomeCanceled || job.TerminalProof != ProofContained || !job.Contained {
		t.Fatalf("job = %+v, want canceled contained proof", job)
	}
}

func TestCorrectiveLaunchOrdinalRequiresQuiescence(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-ordinal", "owner")
	res := submitPreparedPermitted(t, c, "ws-ordinal", "req-ordinal", "fp-ordinal")
	if err := c.GrantPermit(res.JobID, 2, "nonce-2", nil); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("ordinal 2 before quiescence err = %v, want precondition failure", err)
	}
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	job, _ := c.Store.GetJob(res.JobID)
	if _, err := c.Store.RecordLaunchQuiescent(res.JobID, job.AttemptID, job.Epoch, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.GrantPermit(res.JobID, 2, "nonce-2", nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ = c.Store.GetJob(res.JobID)
	if job.LaunchOrdinal != 2 || !job.LaunchQuiescent[1] {
		t.Fatalf("job = %+v, want ordinal 2 after ordinal 1 quiescent", job)
	}
}

func TestStartupReconciliationTerminalizesPriorBootWork(t *testing.T) {
	store := NewMemoryAdmissionStore()
	old := NewCoordinator(store, "boot-old", "owner")
	beforeLaunch, err := old.Submit(modelRequest("ws-startup", "req-before", "fp-before"), nil)
	if err != nil {
		t.Fatal(err)
	}
	permitMaybe := submitPreparedPermitted(t, old, "ws-startup", "req-permit", "fp-permit")

	newBoot := NewCoordinator(store, "boot-new", "owner")
	if err := newBoot.StartupReconcile(); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, newBoot)
	job, _ := store.GetJob(beforeLaunch.JobID)
	if job.Outcome != OutcomeFailed || job.TerminalReason != "daemon_restarted_before_launch" || job.TerminalProof != ProofNeverPermittedAndRetired {
		t.Fatalf("before-launch job = %+v", job)
	}
	job, _ = store.GetJob(permitMaybe.JobID)
	if job.Outcome != OutcomeReaped || job.TerminalReason != "daemon_restarted" || job.TerminalProof != ProofContained {
		t.Fatalf("permit-maybe job = %+v", job)
	}
}

func TestResultCleanupExclusion(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-cleanup", "owner")
	res := submitPreparedPermitted(t, c, "ws-cleanup", "req-cleanup", "fp-cleanup")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	job, _ := c.Store.GetJob(res.JobID)
	if _, err := c.Store.RecordOutcome(res.JobID, job.AttemptID, job.Epoch, OutcomeCompleted); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.BeginResultPublication(res.JobID, "results/"+res.JobID+".txt", "sha256:"+res.JobID, 4); err != nil {
		t.Fatal(err)
	}
	job, _ = c.Store.GetJob(res.JobID)
	if ResultCleanupEligible(job, c.Obligations(), "tmp-"+res.JobID) {
		t.Fatalf("publishing job was eligible for cleanup")
	}
}

func submitPreparedPermitted(t *testing.T, c *Coordinator, workspaceKey, requestID, fp string) ResolveResult {
	t.Helper()
	res, err := c.Submit(modelRequest(workspaceKey, requestID, fp), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.GrantPermit(res.JobID, 1, "nonce-1", nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	return res
}

func checkCoordinator(t *testing.T, c *Coordinator) {
	t.Helper()
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
}
