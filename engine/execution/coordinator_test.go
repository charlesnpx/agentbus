package execution

import (
	"strings"
	"testing"
)

func TestIdentifiedLifecycleCompletes(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-life", "owner")
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

func TestPostCommitPreRunnableInterruptionFailStops(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-prerunnable", "owner")
	injector := &FailureInjector{Target: FailPostCommitPreRunnable}

	res, err := c.Submit(modelRequest("ws-prerunnable", "req-prerunnable", "fp-prerunnable"), injector)
	if err == nil {
		t.Fatal("Submit returned nil error for post-commit pre-runnable interruption")
	}
	if !injector.Hit {
		t.Fatal("post-commit pre-runnable failpoint was not hit")
	}
	if !c.FailStopping {
		t.Fatal("coordinator did not enter fail-stop after committed obligation failed to become runnable")
	}
	if c.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("lifecycle state = %q, want fail-stopped", c.LifecycleState)
	}

	job, ok := c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("accepted job missing after post-commit pre-runnable interruption")
	}
	if job.Terminal() || job.Decision != DecisionAccepted {
		t.Fatalf("job = %+v, want accepted nonterminal job", job)
	}
	obligation, ok := c.Obligations()[res.JobID]
	if !ok {
		t.Fatal("obligation missing after durable acceptance")
	}
	if obligation.state() != ObligationCommitted || obligation.runnable() {
		t.Fatalf("obligation state = %q, want committed and not runnable", obligation.state())
	}
	checkCoordinator(t, c)

	failStoppedCalls := []struct {
		name string
		run  func() error
	}{
		{name: "Submit", run: func() error {
			_, err := c.Submit(modelRequest("ws-prerunnable", "req-after-stop", "fp-after-stop"), nil)
			return err
		}},
		{name: "SubmitLegacyFenced", run: func() error {
			_, err := c.SubmitLegacyFenced(modelRequest("ws-prerunnable", "req-legacy-fenced", "fp-legacy-fenced"), nil)
			return err
		}},
		{name: "SubmitLegacyUnfenced", run: func() error {
			_, err := c.SubmitLegacyUnfenced(modelRequest("ws-prerunnable", "", "fp-legacy-unfenced"), true, nil)
			return err
		}},
		{name: "Acknowledge", run: func() error { return c.Acknowledge(res.JobID) }},
		{name: "RejectUnacknowledged", run: func() error { return c.RejectUnacknowledged(res.JobID) }},
		{name: "PrepareSupervisor", run: func() error { return c.PrepareSupervisor(res.JobID, nil) }},
		{name: "GrantPermit", run: func() error { return c.GrantPermit(res.JobID, 1, "nonce-after-stop", nil) }},
		{name: "Start", run: func() error { return c.Start(res.JobID, nil) }},
		{name: "Complete", run: func() error { return c.Complete(res.JobID, OutcomeFailed) }},
		{name: "Cancel", run: func() error { return c.Cancel(res.JobID) }},
		{name: "LiveSupervisorLoss", run: func() error { return c.LiveSupervisorLoss(res.JobID) }},
		{name: "StartupReconcile", run: func() error { return c.StartupReconcile() }},
		{name: "Expire", run: func() error {
			_, err := c.Expire("ws-prerunnable", "req-prerunnable", nil)
			return err
		}},
		{name: "MarkCorrupt", run: func() error { return c.MarkCorrupt(res.JobID, false, true, "after fail-stop", nil) }},
	}
	for _, call := range failStoppedCalls {
		if err := call.run(); err == nil || !strings.Contains(err.Error(), "fail-stopped") {
			t.Fatalf("%s err = %v, want fail-stopped rejection", call.name, err)
		}
	}

	c.FailStopping = false
	if err := c.Check(); err == nil {
		t.Fatal("CheckInvariants accepted nonterminal job without runnable obligation or fail-stop")
	}

	newBoot := NewCoordinator(c.Store, "boot-prerunnable-new", "owner")
	if err := newBoot.StartupReconcile(); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, newBoot)
	job, ok = c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("accepted job missing after startup reconciliation")
	}
	if job.Outcome != OutcomeFailed || job.TerminalProof != ProofNeverPermittedAndRetired || job.TerminalReason != "daemon_restarted_before_launch" {
		t.Fatalf("job = %+v, want startup-reconciled failed never-permitted terminal", job)
	}
	if _, err := newBoot.Submit(modelRequest("ws-prerunnable-new", "req-prerunnable-new", "fp-prerunnable-new"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestCancelBeforePermitDoesNotStart(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-cancel", "owner")
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
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-cancel2", "owner")
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

func TestMarkCorruptUsesIndependentPermitEvidence(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-corrupt-permit", "owner")
	res, err := c.Submit(modelRequest("ws-corrupt", "req-corrupt-permit", "fp-corrupt"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	job, _ := c.Store.GetJob(res.JobID)
	if executionUncertain(&job) {
		t.Fatalf("test setup decoded permit evidence from aggregate: %+v", job)
	}

	if err := c.MarkCorrupt(res.JobID, true, true, "independent permit evidence", nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ = c.Store.GetJob(res.JobID)
	if !job.Terminal() || job.Outcome != OutcomeQuarantined || job.TerminalProof != ProofContained || !job.Contained {
		t.Fatalf("job = %+v, want quarantined contained terminal from independent permit evidence", job)
	}
	if job.TerminalProof == ProofNeverPermittedAndRetired {
		t.Fatalf("job = %+v, must not use never-permitted proof when independent permit evidence is present", job)
	}
}

func TestMarkCorruptDoesNotContainWithoutVerification(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-corrupt-verify", "owner")
	res, err := c.Submit(modelRequest("ws-corrupt", "req-corrupt-verify", "fp-corrupt"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	injector := &FailureInjector{Target: FailContainmentVerifyBefore}
	if err := c.MarkCorrupt(res.JobID, true, true, "verification interrupted", injector); err == nil {
		t.Fatal("expected containment verification interruption")
	}
	if !injector.Hit {
		t.Fatal("containment verification failpoint was not hit")
	}
	job, _ := c.Store.GetJob(res.JobID)
	if job.Contained || job.ContainmentVerified || job.Terminal() || job.Outcome == OutcomeQuarantined {
		t.Fatalf("job = %+v, want no contained/quarantined state without verification", job)
	}
}

func TestMarkCorruptUntrustedIdentityFailStops(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-corrupt-fatal", "owner")
	res, err := c.Submit(modelRequest("ws-corrupt", "req-corrupt-fatal", "fp-corrupt"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.MarkCorrupt(res.JobID, true, false, "untrusted identity", nil); !IsCode(err, CodeCorruptFatal) {
		t.Fatalf("err = %v, want corrupt fatal", err)
	}
	if !c.FailStopping || c.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("coordinator did not fail-stop after untrusted corrupt identity: %+v", c)
	}
}

func TestCorrectiveLaunchOrdinalRequiresQuiescence(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-ordinal", "owner")
	res := submitPreparedPermitted(t, c, "ws-ordinal", "req-ordinal", "fp-ordinal")
	if err := c.GrantPermit(res.JobID, 2, "nonce-2", nil); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("ordinal 2 before quiescence err = %v, want precondition failure", err)
	}
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	quiesceCurrentLaunch(t, c, res.JobID)
	if err := c.GrantPermit(res.JobID, 2, "nonce-2", nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ := c.Store.GetJob(res.JobID)
	if job.LaunchOrdinal != 2 || !job.LaunchQuiescent[1] {
		t.Fatalf("job = %+v, want ordinal 2 after ordinal 1 quiescent", job)
	}
}

func TestParkedExecBackendIdentityMatchesSupervisorLeader(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-parked-exec", "owner")
	res := submitPreparedPermitted(t, c, "ws-parked-exec", "req-parked-exec", "fp-parked-exec")
	job, ok := c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("job missing")
	}
	supervisor := job.Supervisor
	if !supervisor.Valid() {
		t.Fatal("prepared supervisor missing")
	}

	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, ok = c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("job missing after start")
	}
	want := ChildRef{PID: supervisor.LeaderPID, HighResStartToken: supervisor.HighResStartToken}
	if job.Child != want {
		t.Fatalf("backend identity child=%+v, want supervisor leader %+v", job.Child, want)
	}

	broken := c.Store.jobs[res.JobID]
	broken.Child.PID++
	c.Store.jobs[res.JobID] = broken
	if err := c.Check(); err == nil {
		t.Fatal("CheckInvariants accepted backend child PID mismatch")
	}
}

func TestInvariantRejectsRegrantOrdinalOne(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-regrant-1", "owner")
	res := submitPreparedPermitted(t, c, "ws-regrant-1", "req-regrant-1", "fp-regrant-1")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	quiesceCurrentLaunch(t, c, res.JobID)

	job := c.Store.jobs[res.JobID]
	job.PermitState = PermitGranted
	job.PermitNonce = "nonce-regrant-1"
	job.LaunchOrdinal = 1
	job.ActiveOrdinal = 1
	job.LiveOrdinals[1] = 1
	job.Dispatch = DispatchPermitGranted

	if err := c.Check(); err == nil {
		t.Fatal("CheckInvariants returned nil for re-granted ordinal 1")
	}
}

func TestInvariantRejectsRegrantOrdinalTwo(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-regrant-2", "owner")
	res := submitPreparedPermitted(t, c, "ws-regrant-2", "req-regrant-2", "fp-regrant-2")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	quiesceCurrentLaunch(t, c, res.JobID)
	if err := c.GrantPermit(res.JobID, 2, "nonce-2", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	quiesceCurrentLaunch(t, c, res.JobID)

	job := c.Store.jobs[res.JobID]
	job.PermitState = PermitGranted
	job.PermitNonce = "nonce-regrant-2"
	job.LaunchOrdinal = 2
	job.ActiveOrdinal = 2
	job.LiveOrdinals[2] = 1
	job.Dispatch = DispatchPermitGranted

	if err := c.Check(); err == nil {
		t.Fatal("CheckInvariants returned nil for re-granted ordinal 2")
	}
}

func TestInvariantRejectsLaunchNonceReuse(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-nonce-reuse", "owner")
	res := submitPreparedPermitted(t, c, "ws-nonce-reuse", "req-nonce-reuse", "fp-nonce-reuse")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	quiesceCurrentLaunch(t, c, res.JobID)

	job := c.Store.jobs[res.JobID]
	job.LaunchNonceHistory[2] = job.LaunchNonceHistory[1]
	job.LaunchQuiescent[2] = true
	job.LaunchEvidence[2] = LaunchQuiescenceEvidence{
		ChildExited: Evidence{Kind: "child_exit", Detail: "test"},
		GroupEmpty:  Evidence{Kind: "group_empty", Detail: "test"},
	}

	if err := c.Check(); err == nil {
		t.Fatal("CheckInvariants returned nil for reused launch nonce")
	}
}

func TestStartupReconciliationTerminalizesPriorBootWork(t *testing.T) {
	store := NewMemoryAdmissionStore()
	old := newReadyCoordinator(t, store, "boot-old", "owner")
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

func TestStartupReconciliationGatesReplayUntilReady(t *testing.T) {
	store := NewMemoryAdmissionStore()
	old := newReadyCoordinator(t, store, "boot-replay-old", "owner")
	req := modelRequest("ws-startup-gate", "req-replay", "fp-replay")
	res, err := old.Submit(req, nil)
	if err != nil {
		t.Fatal(err)
	}

	newBoot := NewCoordinator(store, "boot-replay-new", "owner")
	replay, err := newBoot.Submit(req, nil)
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("replay before StartupReconcile = (%+v,%v), want not-ready rejection", replay, err)
	}
	if replay.JobID != "" || replay.Job.JobID != "" {
		t.Fatalf("replay before StartupReconcile returned job %+v", replay)
	}
	job, _ := store.GetJob(res.JobID)
	if job.Terminal() {
		t.Fatalf("old-boot job was terminal before StartupReconcile: %+v", job)
	}

	if err := newBoot.StartupReconcile(); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, newBoot)
	job, _ = store.GetJob(res.JobID)
	if job.Outcome != OutcomeFailed || job.TerminalProof != ProofNeverPermittedAndRetired || job.TerminalReason != "daemon_restarted_before_launch" {
		t.Fatalf("old-boot job = %+v, want reconciled failed terminal", job)
	}
	replay, err = newBoot.Submit(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replay.JobID != res.JobID || !replay.Job.Terminal() || replay.Job.Outcome != OutcomeFailed {
		t.Fatalf("replay after StartupReconcile = %+v, want terminal old-boot job %s", replay, res.JobID)
	}
}

func TestResultCleanupExclusion(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-cleanup", "owner")
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

func newReadyCoordinator(t *testing.T, store *MemoryAdmissionStore, bootID, ownerID string) *Coordinator {
	t.Helper()
	c := NewCoordinator(store, bootID, ownerID)
	if err := c.StartupReconcile(); err != nil {
		t.Fatal(err)
	}
	return c
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

func quiesceCurrentLaunch(t *testing.T, c *Coordinator, jobID string) {
	t.Helper()
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		t.Fatal("job missing")
	}
	if _, err := c.Store.RecordLaunchExitEvidence(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal, Evidence{Kind: "child_exit", Detail: "test"}, Evidence{Kind: "group_empty", Detail: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Store.RecordLaunchQuiescent(jobID, job.AttemptID, job.Epoch, job.LaunchOrdinal); err != nil {
		t.Fatal(err)
	}
}

func checkCoordinator(t *testing.T, c *Coordinator) {
	t.Helper()
	if err := c.Check(); err != nil {
		t.Fatal(err)
	}
}
