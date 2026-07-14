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
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
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
	if _, err := store.Expire(req.WorkspaceKey, req.RequestID); err != nil {
		t.Fatal(err)
	}
	result, err := store.ResolveOrAccept(req)
	if !IsCode(err, CodeRequestExpired) || result.JobID != res.JobID {
		t.Fatalf("replay after gc = (%v,%v), want expired original %s", result, err, res.JobID)
	}
}

func seedBackendBeforeRecord(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
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
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-lock-start", "fp")
	if err := c.Start(res.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if c.Store.sideEffectInCAS {
		t.Fatalf("modeled side effect happened during a CAS mutation")
	}
}

func seedReplayCancelActivate(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
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

	c2 := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
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
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-activation-fail", "fp")
	injector := &FailureInjector{Target: FailRecordStartedBeforeCAS}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected activation CAS failure after backend started")
	}
	job, _ := c.Store.GetJob(res.JobID)
	if job.StartPhase != "backend_started" || !job.PendingChild.Valid() {
		t.Fatalf("activation failure state = %+v, want backend-started pending child before RecordStarted", job)
	}
	if err := c.LiveSupervisorLoss(res.JobID); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ = c.Store.GetJob(res.JobID)
	if job.Outcome != OutcomeReaped || job.TerminalProof != ProofContained {
		t.Fatalf("job = %+v, want contained terminal after activation CAS failure", job)
	}
}

func seedStartFailQueuedStartingFail(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-start-fail", "fp")
	injector := &FailureInjector{Target: FailExecForkBeforeCAS}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected queued to starting store-update failure")
	}
	job, _ := c.Store.GetJob(res.JobID)
	if job.StartPhase != "" || job.PermitState != PermitMaybeSent {
		t.Fatalf("queued->starting failure state = %+v, want permit maybe-sent before fork record", job)
	}
	if err := c.LiveSupervisorLoss(res.JobID); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, c)
	job, _ = c.Store.GetJob(res.JobID)
	if !job.Terminal() || job.TerminalProof != ProofContained {
		t.Fatalf("job = %+v, want contained terminal", job)
	}
}

func seedStartFailUpdateFailRequestBound(t *testing.T) {
	store := NewMemoryAdmissionStore()
	old := NewCoordinator(store, "boot-old", "owner")
	res := submitPreparedPermitted(t, old, "ws-seed", "req-start-update", "fp")
	injector := &FailureInjector{Target: FailTerminalBeforeCAS}
	if err := old.LiveSupervisorLossWithInjector(res.JobID, injector); err == nil {
		t.Fatal("expected terminalization failure after Start failure")
	}
	job, _ := store.GetJob(res.JobID)
	if job.Terminal() {
		t.Fatalf("job = %+v, terminalized despite injected terminal commit failure", job)
	}
	newBoot := NewCoordinator(store, "boot-new", "owner")
	if err := old.LiveSupervisorLoss(res.JobID); err != nil {
		t.Fatal(err)
	}
	checkCoordinator(t, newBoot)
	job, _ = store.GetJob(res.JobID)
	if job.Outcome != OutcomeReaped || job.TerminalProof != ProofContained {
		t.Fatalf("job = %+v, want startup contained reaped terminal", job)
	}
}

func seedAcceptanceWithoutAfter(t *testing.T) {
	c := NewCoordinator(NewMemoryAdmissionStore(), "boot-seed", "owner")
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
