package execution

import (
	"fmt"
	"testing"
)

func TestRegressionSeedsAreUnreachableOrTerminalized(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "torn reservation", run: seedTornReservation},
		{name: "gc deletes replaced", run: seedGCDeletesReplaced},
		{name: "backend before record", run: seedBackendBeforeRecord},
		{name: "validation bypass", run: seedValidationBypass},
		{name: "lock across start", run: seedLockAcrossStart},
		{name: "replay cancel activate", run: seedReplayCancelActivate},
		{name: "activation update fail leaves runless queued", run: seedActivationUpdateFail},
		{name: "start fail queued to starting fail leaves runless queued", run: seedStartFailQueuedStartingFail},
		{name: "start fail update fail request-bound runless job", run: seedStartFailUpdateFailRequestBound},
		{name: "acceptance without after unreconstructable queued job", run: seedAcceptanceWithoutAfter},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, tt.run)
	}
}

func seedTornReservation(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	injector := &FailureInjector{Target: FailBeforeCommit}
	if _, err := c.Submit(modelRequest("ws-seed", "req-torn", "fp"), injector); err == nil {
		t.Fatal("expected injected failure")
	}
	checkCoordinator(t, c)
	if len(c.Store.jobs) != 0 || len(c.Store.bindings) != 0 {
		t.Fatalf("durable state after torn reservation = jobs %d bindings %d", len(c.Store.jobs), len(c.Store.bindings))
	}
	res, err := c.Submit(modelRequest("ws-seed", "req-torn", "fp"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != ResolveAcceptedNew {
		t.Fatalf("status = %s, want accepted_new", res.Status)
	}
}

func seedGCDeletesReplaced(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-seed", "req-gc", "fp")
	res, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	terminalizeDirectStoreForExpiry(t, store, res.JobID)
	if _, err := store.Expire(req.WorkspaceKey, req.RequestID); err != nil {
		t.Fatal(err)
	}
	result, err := store.ResolveOrAccept(req)
	if !IsCode(err, CodeRequestExpired) || result.JobID != res.JobID {
		t.Fatalf("replay after gc = (%v,%v), want expired original %s", result, err, res.JobID)
	}
}

func terminalizeDirectStoreForExpiry(t *testing.T, store *MemoryAdmissionStore, jobID string) {
	t.Helper()
	job, ok := store.GetJob(jobID)
	if !ok {
		t.Fatal("job missing")
	}
	group := GroupRef{PGID: 10, LeaderPID: 10, HighResStartToken: "direct-expiry"}
	if _, err := store.RecordSupervisor(jobID, job.AttemptID, job.Epoch, group); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementStarted(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "control_closed", Detail: "direct expiry"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementWorkerExited(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "worker_exit", Detail: "direct expiry"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementGroupEmpty(jobID, job.AttemptID, job.Epoch, Evidence{Kind: "group_empty", Detail: "direct expiry"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordOutcome(jobID, job.AttemptID, job.Epoch, OutcomeCanceled); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishTerminal(jobID, job.AttemptID, job.Epoch, OutcomeCanceled, ProofNeverPermittedAndRetired, "canceled_before_permit"); err != nil {
		t.Fatal(err)
	}
}

func seedBackendBeforeRecord(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-backend-before-record", "fp")
	injector := &FailureInjector{Target: FailExecDeathAfterExecBeforeStart}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected injected death")
	}
	checkCoordinator(t, c)
	job, _ := c.Store.GetJob(res.JobID)
	if job.Outcome != OutcomeReaped || job.TerminalProof != ProofContained {
		t.Fatalf("job = %+v, want contained reaped terminal", job)
	}
}

func seedValidationBypass(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-seed", "req-validation", "fp")
	if _, err := store.ResolveOrAccept(req); err != nil {
		t.Fatal(err)
	}
	replay := req
	replay.WorkspaceKey = "ws-seed"
	replay.LaunchSpec.RawTask = "different"
	replay.LaunchSpec.Task = ""
	if _, err := store.ResolveOrAccept(replay); !IsCode(err, CodeRequestConflict) {
		t.Fatalf("err = %v, want conflict before validation/new acceptance", err)
	}
	if len(store.jobs) != 1 {
		t.Fatalf("jobs = %d, want original only", len(store.jobs))
	}
}

func seedLockAcrossStart(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-lock-start", "fp")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if c.Store.sideEffectInCAS {
		t.Fatalf("modeled side effect happened during a CAS mutation")
	}
}

func seedReplayCancelActivate(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	req := modelRequest("ws-seed", "req-replay-cancel", "fp")
	trace := []string{"accept"}
	res, err := c.Submit(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	trace = append(trace, regressionSnapshot(t, c, res.JobID, "accepted"))
	if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, regressionSnapshot(t, c, res.JobID, "prepared"))
	if err := c.Cancel(res.JobID); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, regressionSnapshot(t, c, res.JobID, "canceled"))
	replay, err := c.Submit(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replay.JobID != res.JobID {
		t.Fatalf("replay jobID = %s, want %s", replay.JobID, res.JobID)
	}
	if err := c.GrantPermit(res.JobID, 1, "late-activation", nil); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("late activation after cancellation err = %v trace=%v, want precondition failure", err, trace)
	}
	job, _ := c.Store.GetJob(res.JobID)
	if job.ExecutionSideEffects != 0 || job.Outcome != OutcomeCanceled {
		t.Fatalf("job = %+v trace=%v, want canceled without activation", job, trace)
	}

	c2 := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res2 := submitPreparedPermitted(t, c2, "ws-seed", "req-cancel-after-permit", "fp")
	if err := c2.Cancel(res2.JobID); err != nil {
		t.Fatal(err)
	}
	job, _ = c2.Store.GetJob(res2.JobID)
	if job.Outcome != OutcomeCanceled || job.TerminalProof != ProofContained {
		t.Fatalf("permit/cancel interleaving job = %+v, want contained canceled", job)
	}
}

func seedActivationUpdateFail(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-activation-fail", "fp")
	injector := &FailureInjector{Target: FailRecordStartedBeforeCAS}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected activation CAS failure after backend started")
	}
	assertTerminalizedOrFailStopped(t, c, res.JobID)
}

func seedStartFailQueuedStartingFail(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-start-fail", "fp")
	injector := &FailureInjector{Target: FailExecForkBeforeCAS}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected queued to starting store-update failure")
	}
	assertTerminalizedOrFailStopped(t, c, res.JobID)
}

func seedStartFailUpdateFailRequestBound(t *testing.T) {
	store := NewMemoryAdmissionStore()
	old := newReadyCoordinator(t, store, "boot-old", "owner")
	res := submitPreparedPermitted(t, old, "ws-seed", "req-start-update", "fp")
	newBoot := NewCoordinator(store, "boot-new", "owner")
	observeInitializedAnchor(t, store)
	injector := &FailureInjector{Target: FailTerminalBeforeCAS}
	if err := newBoot.StartupReconcileWithInjector(injector); err == nil {
		t.Fatal("expected startup terminalization failure after request-bound start failure")
	}
	if !injector.Hit {
		t.Fatal("startup reconciliation did not hit terminal failpoint")
	}
	job, _ := store.GetJob(res.JobID)
	if job.Terminal() {
		t.Fatalf("job = %+v, terminalized despite injected terminal commit failure", job)
	}
	assertTerminalizedOrFailStopped(t, newBoot, res.JobID)
}

func seedAcceptanceWithoutAfter(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	req := modelRequest("ws-seed", "req-no-after", "fp")
	injector := &FailureInjector{Target: FailAfterCommit}
	res, err := c.Submit(req, injector)
	if err == nil {
		t.Fatal("expected injected failure")
	}
	checkCoordinator(t, c)
	if _, err := c.Submit(req, nil); err == nil {
		t.Fatal("same coordinator accepted replay after fail-stop")
	}
	newBoot := NewCoordinator(c.Store, "boot-seed-new", "owner")
	observeInitializedAnchor(t, c.Store)
	if err := newBoot.StartupReconcile(); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, newBoot)
	replay, err := newBoot.Submit(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replay.JobID != res.JobID {
		t.Fatalf("replay jobID = %s, want %s", replay.JobID, res.JobID)
	}
}

func regressionSnapshot(t *testing.T, c *Coordinator, jobID, label string) string {
	t.Helper()
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		return label + ":missing"
	}
	return fmt.Sprintf("%s:%s/%s/%s permit=%s live=%d outcome=%s", label, job.Decision, job.Dispatch, job.Public(), job.PermitState, liveOrdinalCount(job.LiveOrdinals), job.Outcome)
}

func assertTerminalizedOrFailStopped(t *testing.T, c *Coordinator, jobID string) {
	t.Helper()
	checkCoordinator(t, c)
	job, ok := c.Store.GetJob(jobID)
	if !ok {
		t.Fatal("job missing")
	}
	if job.Terminal() {
		if job.TerminalProof != ProofContained {
			t.Fatalf("job = %+v, want contained terminal", job)
		}
		return
	}
	if !c.FailStopping || c.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("job = %+v coordinator failStopping=%v lifecycle=%s, want terminalized or fail-stopped", job, c.FailStopping, c.LifecycleState)
	}
}
