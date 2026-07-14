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
		{name: "activation update fail then terminal fail retains obligation", run: seedActivationThenTerminalFail},
		{name: "start fail queued to starting fail leaves runless queued", run: seedStartFailQueuedStartingFail},
		{name: "start fail update fail request-bound runless job", run: seedStartFailUpdateFailRequestBound},
		{name: "acceptance without after unreconstructable queued job", run: seedAcceptanceWithoutAfter},
		{name: "fingerprint covers task spec", run: seedFingerprintCoversTaskSpec},
		{name: "zero fingerprint fails closed", run: seedZeroFingerprintFailsClosed},
		{name: "terminal projection waits for decision and result", run: seedTerminalProjectionWaitsForDecisionAndResult},
		{name: "supplied job id collision rejected", run: seedSuppliedJobIDCollisionRejected},
		{name: "reject final cas double failure keeps ownership", run: seedRejectFinalCASDoubleFailure},
		{name: "anchor recover only publishes anchor", run: seedAnchorRecoverOnlyPublishesAnchor},
		{name: "anchor first init crash after db rename", run: seedAnchorFirstInitCrashAfterDBRename},
		{name: "supervisor loss without identity never fabricates containment", run: seedSupervisorLossWithoutIdentity},
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

func seedActivationThenTerminalFail(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res := submitPreparedPermitted(t, c, "ws-seed", "req-activation-terminal-fail", "fp")
	injector := &FailureInjector{Script: []Failpoint{FailRecordStartedBeforeCAS, FailTerminalBeforeCAS}}
	if err := c.Start(res.JobID, injector); err == nil {
		t.Fatal("expected activation failure followed by terminalization failure")
	}
	if injector.ScriptIndex != len(injector.Script) {
		t.Fatalf("script index = %d, want %d hits=%v", injector.ScriptIndex, len(injector.Script), injector.Hits)
	}
	job, ok := c.Store.GetJob(res.JobID)
	if !ok {
		t.Fatal("job missing")
	}
	if job.Terminal() {
		if !validTerminalProof(&job) || !validTerminalOutcomeProofReason(&job) {
			t.Fatalf("terminal job lacks valid proof: %+v", job)
		}
		return
	}
	if !c.FailStopping || c.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("job = %+v coordinator failStopping=%v lifecycle=%s, want fail-stop", job, c.FailStopping, c.LifecycleState)
	}
	if _, ok := c.obligations[res.JobID]; !ok {
		t.Fatalf("obligation for %s was not retained after failed terminalization", res.JobID)
	}
	checkCoordinator(t, c)
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

func seedFingerprintCoversTaskSpec(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-seed", "req-fingerprint", "fp")
	if _, err := store.ResolveOrAccept(req); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []struct {
		name string
		fn   func(*SubmitRequest)
	}{
		{name: "backend", fn: func(req *SubmitRequest) { req.LaunchSpec.Backend = "claude" }},
		{name: "cwd", fn: func(req *SubmitRequest) { req.LaunchSpec.CWD = "/tmp/other-workspace" }},
		{name: "session", fn: func(req *SubmitRequest) { req.LaunchSpec.SessionID = "other-session" }},
		{name: "prompt", fn: func(req *SubmitRequest) { req.LaunchSpec.RawTask = "different raw prompt" }},
	} {
		replay := req
		mutate.fn(&replay)
		if _, err := store.ResolveOrAccept(replay); !IsCode(err, CodeRequestConflict) {
			t.Fatalf("%s replay err = %v, want request_conflict", mutate.name, err)
		}
	}
}

func seedZeroFingerprintFailsClosed(t *testing.T) {
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-seed", "req-zero-fingerprint", "fp")
	if _, err := store.ResolveOrAccept(req); err != nil {
		t.Fatal(err)
	}
	key := bindingKey(req.WorkspaceKey, req.RequestID)
	binding := store.bindings[key]
	binding.Fingerprint = Fingerprint{}
	store.bindings[key] = binding
	if _, err := store.ResolveOrAccept(req); !IsCode(err, CodeRequestFingerprintUnsupported) {
		t.Fatalf("err = %v, want request_fingerprint_unsupported", err)
	}
}

func seedTerminalProjectionWaitsForDecisionAndResult(t *testing.T) {
	assertProjectionSemantics(t)
	store := NewMemoryAdmissionStore()
	req := modelRequest("ws-seed", "req-terminal-projection", "fp")
	res, err := store.ResolveOrAccept(req)
	if err != nil {
		t.Fatal(err)
	}
	job := res.Job
	group := GroupRef{PGID: 41, LeaderPID: 41, HighResStartToken: "projection"}
	if _, err := store.RecordSupervisor(job.JobID, job.AttemptID, job.Epoch, group); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GrantPermit(job.JobID, job.AttemptID, job.Epoch, 1, "projection-nonce"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordPermitMaybeSent(job.JobID, job.AttemptID, job.Epoch, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordExecForked(job.JobID, job.AttemptID, job.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordExeced(job.JobID, job.AttemptID, job.Epoch); err != nil {
		t.Fatal(err)
	}
	child := ChildRef{PID: group.LeaderPID, HighResStartToken: group.HighResStartToken}
	if _, err := store.RecordBackendStarted(job.JobID, job.AttemptID, job.Epoch, child); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordStarted(job.JobID, job.AttemptID, job.Epoch, 1, child); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordOutcome(job.JobID, job.AttemptID, job.Epoch, OutcomeCompleted); err != nil {
		t.Fatal(err)
	}
	observed, _ := store.GetJob(job.JobID)
	if observed.Decision == DecisionTerminal || terminalPublicState(observed.Public()) {
		t.Fatalf("public state = %s decision=%s, want nonterminal after outcome only", observed.Public(), observed.Decision)
	}
	if _, err := store.RecordLaunchExitEvidence(job.JobID, job.AttemptID, job.Epoch, 1, Evidence{Kind: "child_exit", Detail: "projection"}, Evidence{Kind: "group_empty", Detail: "projection"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordLaunchQuiescent(job.JobID, job.AttemptID, job.Epoch, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementStarted(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "control_closed", Detail: "projection"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementWorkerExited(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "worker_exit", Detail: "projection"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRetirementGroupEmpty(job.JobID, job.AttemptID, job.Epoch, Evidence{Kind: "group_empty", Detail: "projection"}); err != nil {
		t.Fatal(err)
	}
	result := ResultRef{Path: "results/projection.txt", Digest: CurrentFingerprint("projection-result").Value, Bytes: 17}
	if _, err := store.BeginResultPublication(job.JobID, result.Path, result.Digest, result.Bytes); err != nil {
		t.Fatal(err)
	}
	observed, _ = store.GetJob(job.JobID)
	if terminalPublicState(observed.Public()) {
		t.Fatalf("public state = %s, want nonterminal while result is not durable", observed.Public())
	}
	if _, err := store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeCompleted, ProofCleanQuiescentOutcomeAndRetired, string(OutcomeCompleted)); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("publish without durable result err = %v, want precondition failure", err)
	}
	if err := store.RecordResultTempWritten(result.Path, result.Digest, result.Bytes); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordResultTempSynced(result.Path); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordResultClosed(result.Path); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordResultRenamed(result.Path); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordResultDirSynced(result.Path); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishTerminal(job.JobID, job.AttemptID, job.Epoch, OutcomeCompleted, ProofCleanQuiescentOutcomeAndRetired, string(OutcomeCompleted)); err != nil {
		t.Fatal(err)
	}
	observed, _ = store.GetJob(job.JobID)
	if observed.Decision != DecisionTerminal || observed.Public() != PublicCompleted {
		t.Fatalf("public state = %s decision=%s, want completed terminal", observed.Public(), observed.Decision)
	}
}

func seedSuppliedJobIDCollisionRejected(t *testing.T) {
	store := NewMemoryAdmissionStore()
	first := modelRequest("ws-seed", "req-jobid-first", "fp")
	res, err := store.ResolveOrAccept(first)
	if err != nil {
		t.Fatal(err)
	}
	second := modelRequest("ws-seed", "req-jobid-second", "fp")
	second.JobID = res.JobID
	if _, err := store.ResolveOrAccept(second); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("store collision err = %v, want precondition failure", err)
	}
	suppliedDirect := modelRequest("ws-seed", "req-jobid-direct", "fp")
	suppliedDirect.JobID = "caller-supplied-direct"
	if _, err := store.ResolveOrAccept(suppliedDirect); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("store supplied jobID err = %v, want precondition failure", err)
	}
	job, _ := store.GetJob(res.JobID)
	if job.RequestID != first.RequestID || job.WorkspaceKey != first.WorkspaceKey {
		t.Fatalf("job identity overwritten: %+v", job)
	}
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	supplied := modelRequest("ws-seed", "req-supplied-jobid", "fp")
	supplied.JobID = "caller-supplied"
	if _, err := c.Submit(supplied, nil); !IsCode(err, CodePreconditionFailed) {
		t.Fatalf("coordinator supplied jobID err = %v, want precondition failure", err)
	}
}

func seedRejectFinalCASDoubleFailure(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res, err := c.SubmitLegacyFenced(modelRequest("ws-seed", "req-reject-final", "fp"), nil)
	if err != nil {
		t.Fatal(err)
	}
	injector := &FailureInjector{Script: []Failpoint{FailRejectFinalBeforeCAS, FailRejectFinalBeforeCAS}}
	if err := c.RejectUnacknowledgedWithInjector(res.JobID, injector); err == nil {
		t.Fatal("expected both final reject CAS attempts to fail")
	}
	if injector.ScriptIndex != len(injector.Script) {
		t.Fatalf("script index = %d, want %d hits=%v", injector.ScriptIndex, len(injector.Script), injector.Hits)
	}
	if !c.FailStopping || c.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("coordinator failStopping=%v lifecycle=%s, want fail-stopped", c.FailStopping, c.LifecycleState)
	}
	if _, ok := c.obligations[res.JobID]; !ok {
		t.Fatalf("ownership obligation for %s was not retained", res.JobID)
	}
	checkCoordinator(t, c)
}

func seedAnchorRecoverOnlyPublishesAnchor(t *testing.T) {
	store := NewMemoryAdmissionStore()
	decision := store.ObserveStartupAnchor(AnchorInput{
		DBPresent:           true,
		DBValid:             true,
		AnchorPresent:       false,
		EverInitialized:     true,
		DBUUID:              "db-recover",
		DBSchemaMajor:       1,
		DBGeneration:        7,
		HighWaterGeneration: 0,
	})
	if decision.Action != StartupRecoverAnchor {
		t.Fatalf("decision = %+v, want recover_anchor", decision)
	}
	if err := store.completeStartupAnchorDisposition(nil); err != nil {
		t.Fatal(err)
	}
	if store.anchorInitState.TempDBCreated || store.anchorInitState.Renamed || !store.anchorInitState.AnchorPublished || !store.anchorInitState.AnchorDirFsynced {
		t.Fatalf("anchor init state = %+v, want anchor-only publish", store.anchorInitState)
	}
	if store.anchorState.DBUUID != "db-recover" || store.anchorState.HighWaterGeneration != 7 {
		t.Fatalf("anchor state = %+v, want recovered db anchor", store.anchorState)
	}

	advance := NewMemoryAdmissionStore()
	decision = advance.ObserveStartupAnchor(AnchorInput{
		DBPresent:           true,
		DBValid:             true,
		AnchorPresent:       true,
		AnchorValid:         true,
		EverInitialized:     true,
		DBUUID:              "db-advance",
		AnchorDBUUID:        "db-advance",
		DBSchemaMajor:       1,
		AnchorSchemaMajor:   1,
		DBGeneration:        9,
		HighWaterGeneration: 3,
	})
	if decision.Action != StartupAdvanceAnchor {
		t.Fatalf("decision = %+v, want advance_anchor", decision)
	}
	if err := advance.completeStartupAnchorDisposition(nil); err != nil {
		t.Fatal(err)
	}
	if advance.anchorInitState.TempDBCreated || advance.anchorInitState.Renamed || !advance.anchorInitState.AnchorPublished {
		t.Fatalf("advance init state = %+v, want anchor-only publish", advance.anchorInitState)
	}
	if advance.anchorState.HighWaterGeneration != 9 {
		t.Fatalf("advance anchor high water = %d, want 9", advance.anchorState.HighWaterGeneration)
	}
}

func seedAnchorFirstInitCrashAfterDBRename(t *testing.T) {
	store := NewMemoryAdmissionStore()
	c := NewCoordinator(store, "boot-seed", "owner")
	injector := &FailureInjector{Target: FailAnchorPublishBefore}
	if err := c.StartupReconcileWithInjector(injector); err == nil {
		t.Fatal("expected crash before first anchor publish")
	}
	if !store.anchorInitState.Renamed || !store.anchorInitState.DirFsynced || store.anchorInitState.AnchorPublished {
		t.Fatalf("anchor init state = %+v, want db renamed before anchor publish", store.anchorInitState)
	}
	if store.startupAnchorCompleted {
		t.Fatal("startup anchor completed despite publish crash")
	}
}

func seedSupervisorLossWithoutIdentity(t *testing.T) {
	c := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res, err := c.Submit(modelRequest("ws-seed", "req-loss-no-identity", "fp"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.LiveSupervisorLoss(res.JobID); err != nil {
		t.Fatal(err)
	}
	job, _ := c.Store.GetJob(res.JobID)
	if job.TerminalProof != ProofNeverPermittedAndRetired || job.TerminalProof == ProofContained {
		t.Fatalf("job = %+v, want never-permitted proof without containment", job)
	}

	corrupt := newReadyCoordinator(t, NewMemoryAdmissionStore(), "boot-seed", "owner")
	res, err = corrupt.Submit(modelRequest("ws-seed", "req-loss-bad-identity", "fp"), nil)
	if err != nil {
		t.Fatal(err)
	}
	corrupt.Store.mutableAttemptAuthority(res.JobID).GrantNonceHistory = map[int]string{1: "lost-nonce"}
	if err := corrupt.LiveSupervisorLoss(res.JobID); !IsCode(err, CodeCorruptFatal) {
		t.Fatalf("err = %v, want corrupt fatal", err)
	}
	if !corrupt.FailStopping || corrupt.LifecycleState != CoordinatorLifecycleFailStopped {
		t.Fatalf("coordinator failStopping=%v lifecycle=%s, want fail-stopped", corrupt.FailStopping, corrupt.LifecycleState)
	}
}

func assertProjectionSemantics(t *testing.T) {
	t.Helper()
	for _, decision := range AllDecisions() {
		for _, dispatch := range AllDispatches() {
			for _, outcome := range AllOutcomes() {
				public := PublicProjection(decision, dispatch, outcome)
				if !ReachableInternal(decision, dispatch, outcome) {
					continue
				}
				if public == "" {
					t.Fatalf("reachable tuple %s/%s/%s has empty public projection", decision, dispatch, outcome)
				}
				if terminalPublicState(public) != (decision == DecisionTerminal) {
					t.Fatalf("tuple %s/%s/%s projects %s, terminal public mismatch", decision, dispatch, outcome, public)
				}
				if decision != DecisionTerminal && terminalOutcome(outcome) && terminalPublicState(public) {
					t.Fatalf("tuple %s/%s/%s projected terminal public state %s before terminal decision", decision, dispatch, outcome, public)
				}
			}
		}
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
