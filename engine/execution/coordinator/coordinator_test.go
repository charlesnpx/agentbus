package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestAuthorityLifecycleCompletes(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "lifecycle")
	accepted := h.submit(t, ctx, "lifecycle")

	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.Start(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.Complete(ctx, accepted.Record.JobID, model.OutcomeCompleted, []byte("result"), nil); err != nil {
		t.Fatal(err)
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Proof != model.ProofCleanQuiescentOutcomeAndRetired {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofCleanQuiescentOutcomeAndRetired)
	}
	if snapshot.Projection.Public != model.PublicCompleted {
		t.Fatalf("public = %s, want %s", snapshot.Projection.Public, model.PublicCompleted)
	}
	if snapshot.Record.Result == nil || h.results.published != 1 || h.results.verified != 1 {
		t.Fatalf("result publication = record:%#v published:%d verified:%d", snapshot.Record.Result, h.results.published, h.results.verified)
	}
	owned, err := h.coordinator.HasOwnedWork(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if owned {
		t.Fatal("owned work remained after terminal commit")
	}
	if err := h.coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCancelBeforePermitRetiresWithoutContainment(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-before")
	accepted := h.submit(t, ctx, "cancel-before")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	if err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeCanceled {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeCanceled)
	}
	if snapshot.Record.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}
	if snapshot.Record.Terminal.Cause != model.CauseCanceledBeforeAuthorization {
		t.Fatalf("cause = %s, want %s", snapshot.Record.Terminal.Cause, model.CauseCanceledBeforeAuthorization)
	}
	if h.supervisor.contained != 0 {
		t.Fatalf("containment calls = %d, want 0", h.supervisor.contained)
	}
	if h.supervisor.permits != 0 {
		t.Fatalf("permit sends = %d, want 0", h.supervisor.permits)
	}
}

func TestCancelAfterPermitContainsBeforeTerminal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-after")
	accepted := h.submitPreparedPermitted(t, ctx, "cancel-after")

	if err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeCanceled {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeCanceled)
	}
	if snapshot.Record.Terminal.Proof != model.ProofContained {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofContained)
	}
	if snapshot.Record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
		t.Fatalf("cause = %s, want %s", snapshot.Record.Terminal.Cause, model.CauseCanceledAfterAuthorization)
	}
	if h.supervisor.contained != 1 {
		t.Fatalf("containment calls = %d, want 1", h.supervisor.contained)
	}
}

func TestLiveSupervisorLossContainsAndReaps(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "loss")
	accepted := h.submitPreparedPermitted(t, ctx, "loss")
	if err := h.coordinator.Start(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	if err := h.coordinator.LiveSupervisorLoss(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeReaped {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeReaped)
	}
	if snapshot.Record.Terminal.Proof != model.ProofContained {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofContained)
	}
	if snapshot.Record.Terminal.Cause != model.CauseSupervisorLostAfterAuthorization {
		t.Fatalf("cause = %s, want %s", snapshot.Record.Terminal.Cause, model.CauseSupervisorLostAfterAuthorization)
	}
}

func TestShutdownBlocksUntilOwnedWorkDrained(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "shutdown")
	accepted := h.submit(t, ctx, "shutdown")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	timeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	if err := h.coordinator.Shutdown(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v, want deadline exceeded", err)
	}
	if err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCorrectiveLaunchRequiresQuiescence(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "corrective")
	accepted := h.submitPreparedPermitted(t, ctx, "corrective")
	if err := h.coordinator.Start(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 2, model.PermitNonce("nonce-2"), nil); err == nil {
		t.Fatal("ordinal 2 grant succeeded before ordinal 1 quiescence")
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	record := snapshot.Record
	if err := h.coordinator.certifyQuiescence(ctx, &record, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 2, model.PermitNonce("nonce-2"), nil); err != nil {
		t.Fatal(err)
	}
	snapshot = h.snapshot(t, ctx, accepted.Record.JobID)
	if _, ok := snapshot.Record.Attempt.Grants.Get(model.LaunchOrdinalTwo); !ok {
		t.Fatal("ordinal 2 grant missing after quiescence")
	}
}

func TestPostGrantFailureRecoveryContainsAndRetires(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "post-grant")
	accepted := h.submit(t, ctx, "post-grant")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	h.supervisor.failSend = true

	err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), nil)
	if err == nil {
		t.Fatal("GrantPermit returned nil for failed permit send")
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing after recovery")
	}
	if snapshot.Record.Terminal.Proof != model.ProofContained {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofContained)
	}
	if h.supervisor.contained != 1 || h.supervisor.retired != 1 {
		t.Fatalf("recovery effects contain=%d retire=%d, want 1/1", h.supervisor.contained, h.supervisor.retired)
	}
}

func TestDoubleFaultDuringRecoveryFailStops(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "double-fault")
	accepted := h.submit(t, ctx, "double-fault")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	h.supervisor.failSend = true
	h.supervisor.failContain = true

	err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), nil)
	if err == nil {
		t.Fatal("GrantPermit returned nil for double fault")
	}
	if !h.authority.failStopped {
		t.Fatal("authority was not fail-stopped after recovery double fault")
	}
}

type harness struct {
	authority   *readyAuthority
	coordinator *Coordinator
	supervisor  *testSupervisor
	results     *testResults
}

func newHarness(t *testing.T, name string) *harness {
	t.Helper()
	repo := memory.NewRepository()
	bootstrapper, err := authority.NewBootstrapper(repo)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(context.Background(), boot)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	auth := &readyAuthority{ready: ready}
	supervisor := &testSupervisor{}
	results := &testResults{receipts: map[model.ResultRef]model.ResultReceipt{}}
	coordinator, err := New(auth, supervisor, results, model.OwnerID("coordinator-"+name))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		authority:   auth,
		coordinator: coordinator,
		supervisor:  supervisor,
		results:     results,
	}
}

func (h *harness) submit(t *testing.T, ctx context.Context, name string) AdmissionResult {
	t.Helper()
	accepted, err := h.coordinator.Submit(ctx, admissionRequest(t, name))
	if err != nil {
		t.Fatal(err)
	}
	return accepted
}

func (h *harness) submitPreparedPermitted(t *testing.T, ctx context.Context, name string) AdmissionResult {
	t.Helper()
	accepted := h.submit(t, ctx, name)
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), nil); err != nil {
		t.Fatal(err)
	}
	return accepted
}

func (h *harness) snapshot(t *testing.T, ctx context.Context, jobID model.JobID) JobSnapshot {
	t.Helper()
	snapshot, err := h.coordinator.Snapshot(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func admissionRequest(t *testing.T, name string) AdmissionRequest {
	t.Helper()
	key, err := model.NewRequestKey("workspace-"+name, "request-"+name)
	if err != nil {
		t.Fatal(err)
	}
	return AdmissionRequest{
		RequestKey:   key,
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("task-" + name)),
		Mode:         model.ModeIdentifiedFenced,
	}
}

type readyAuthority struct {
	ready       *authority.Ready
	failStopped bool
	failReason  error
}

func (a *readyAuthority) Accept(ctx context.Context, request AdmissionRequest) (AdmissionResult, error) {
	accepted, err := a.ready.Accept(ctx, authority.AcceptRequest{
		RequestKey:   request.RequestKey,
		TaskIdentity: request.TaskIdentity,
		Mode:         request.Mode,
		SessionID:    request.SessionID,
	})
	if err != nil {
		return AdmissionResult{}, err
	}
	return AdmissionResult{Record: accepted.Record, Projection: accepted.Projection, Replayed: accepted.Replayed}, nil
}

func (a *readyAuthority) Apply(ctx context.Context, jobID model.JobID, command model.Command) (StepResult, error) {
	applied, err := a.ready.Apply(ctx, jobID, command)
	if err != nil {
		return StepResult{}, err
	}
	return StepResult{Record: applied.Record, Projection: applied.Projection, Changed: applied.Changed}, nil
}

func (a *readyAuthority) Snapshot(ctx context.Context, jobID model.JobID) (JobSnapshot, error) {
	image, err := a.ready.LoadJob(ctx, jobID)
	if err != nil {
		return JobSnapshot{}, err
	}
	if image.Safety.State != repository.RecordValid {
		return JobSnapshot{}, fmt.Errorf("safety state = %s", image.Safety.State)
	}
	if image.Projection.State != repository.RecordValid {
		return JobSnapshot{}, fmt.Errorf("projection state = %s", image.Projection.State)
	}
	return JobSnapshot{Record: image.Safety.Value, Projection: image.Projection.Value}, nil
}

func (a *readyAuthority) RecoveryPlan(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger) (model.RecoveryPlan, error) {
	snapshot, err := a.Snapshot(ctx, jobID)
	if err != nil {
		return model.RecoveryPlan{}, err
	}
	if trigger == model.RecoveryCancelAfterGrant && !hasLaunchEvidence(snapshot.Record) {
		return cancelBeforeAuthorizationPlan(snapshot.Record), nil
	}
	return model.PlanRecovery(snapshot.Record, trigger)
}

func (a *readyAuthority) ClaimPending(ctx context.Context, ref model.AttemptRef, owner model.OwnerID) error {
	return a.ready.ClaimPending(ctx, ref, owner)
}

func (a *readyAuthority) HasOwnedWork(ctx context.Context) (bool, error) {
	snapshot, err := a.ready.RuntimeSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0, nil
}

func (a *readyAuthority) FailStop(ctx context.Context, err error) error {
	a.failStopped = true
	a.failReason = err
	if err == nil {
		return a.ready.FailStop(ctx, "")
	}
	return a.ready.FailStop(ctx, err.Error())
}

func hasLaunchEvidence(record model.SafetyRecord) bool {
	return record.Attempt.Grants.Count() != 0 || record.Attempt.Consumed.Count() != 0 || record.Attempt.Quiescence.Count() != 0
}

func cancelBeforeAuthorizationPlan(record model.SafetyRecord) model.RecoveryPlan {
	plan := model.RecoveryPlan{BasedOnRevision: record.Revision}
	if record.Terminal != nil {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryFinalizeCertified}
		return plan
	}
	intent := model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	}
	if _, err := model.DeriveTerminalCertificate(record, intent); err == nil {
		finalize := model.Finalize{Ref: record.Attempt.Ref, Intent: intent}
		plan.Next = model.RecoveryAction{Kind: model.RecoveryFinalizeCertified, Finalize: &finalize}
		return plan
	}
	if record.Attempt.Supervisor != nil && record.Attempt.Retirement == nil {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryRetireThenFinalize}
		return plan
	}
	plan.Next = model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}
	return plan
}

type testSupervisor struct {
	next        int
	permits     int
	contained   int
	retired     int
	failSend    bool
	failContain bool
}

func (s *testSupervisor) Prepare(ctx context.Context, plan LaunchPlan) (PreparedSupervisor, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSupervisor{}, err
	}
	s.next++
	return PreparedSupervisor{
		Ref: plan.Ref,
		Identity: model.SupervisorIdentity{
			PGID:               1000 + s.next,
			LeaderPID:          2000 + s.next,
			HighResStartToken:  fmt.Sprintf("supervisor-token-%d", s.next),
			PlatformRetainedID: fmt.Sprintf("retained-%d", s.next),
		},
	}, nil
}

func (s *testSupervisor) SendPermit(ctx context.Context, prepared PreparedSupervisor, grant model.LaunchGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.failSend {
		return errors.New("permit send failed")
	}
	s.permits++
	return nil
}

func (s *testSupervisor) ObserveLaunch(ctx context.Context, prepared PreparedSupervisor, grant model.LaunchGrant) (LaunchObservation, error) {
	if err := ctx.Err(); err != nil {
		return LaunchObservation{}, err
	}
	return LaunchObservation{
		Ordinal: grant.Ordinal,
		Child: model.ChildIdentity{
			PID:               prepared.Identity.LeaderPID,
			HighResStartToken: prepared.Identity.HighResStartToken,
		},
		Evidence: evidence("launch-consumed"),
	}, nil
}

func (s *testSupervisor) VerifyQuiescence(ctx context.Context, prepared PreparedSupervisor, consumed model.LaunchConsumed) (model.QuiescenceReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.QuiescenceReceipt{}, err
	}
	return model.QuiescenceReceipt{
		Attempt:     prepared.Ref,
		Ordinal:     consumed.Ordinal,
		Child:       consumed.Child,
		ChildExited: evidence("child-exit"),
		GroupEmpty:  evidence("group-empty"),
	}, nil
}

func (s *testSupervisor) Contain(ctx context.Context, prepared PreparedSupervisor) (model.ContainmentReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ContainmentReceipt{}, err
	}
	if s.failContain {
		return model.ContainmentReceipt{}, errors.New("containment failed")
	}
	s.contained++
	return model.ContainmentReceipt{
		Attempt:      prepared.Ref,
		Supervisor:   prepared.Identity,
		Signal:       evidence("contain-signal"),
		Verification: evidence("contain-verified"),
	}, nil
}

func (s *testSupervisor) Retire(ctx context.Context, prepared PreparedSupervisor) (model.RetirementReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.RetirementReceipt{}, err
	}
	s.retired++
	return model.RetirementReceipt{
		Attempt:       prepared.Ref,
		Supervisor:    prepared.Identity,
		ControlClosed: evidence("control-closed"),
		WorkerExited:  evidence("worker-exited"),
		GroupEmpty:    evidence("retire-group-empty"),
	}, nil
}

type testResults struct {
	published int
	verified  int
	receipts  map[model.ResultRef]model.ResultReceipt
}

func (r *testResults) Publish(ctx context.Context, jobID model.JobID, payload []byte) (model.ResultReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	r.published++
	ref := model.ResultRef{
		Path:   "results/" + jobID.String() + ".txt",
		Digest: fmt.Sprintf("sha256:%x", len(payload)),
		Bytes:  int64(len(payload)),
	}
	receipt := model.ResultReceipt{
		JobID:       jobID,
		Result:      ref,
		DirSynced:   evidence("result-dir-synced"),
		CertifiedBy: model.BootRef{},
	}
	r.receipts[ref] = receipt
	return receipt, nil
}

func (r *testResults) Verify(ctx context.Context, ref model.ResultRef) (model.ResultReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	receipt, ok := r.receipts[ref]
	if !ok {
		return model.ResultReceipt{}, fmt.Errorf("unknown result ref %s", ref.Path)
	}
	r.verified++
	return receipt, nil
}

func evidence(kind string) model.Evidence {
	return model.Evidence{Kind: kind, Detail: kind + "-evidence"}
}
