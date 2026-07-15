package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
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
	launch, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalTwo)
	if !ok || launch.Grant == nil {
		t.Fatal("ordinal 2 grant missing after quiescence")
	}
}

func TestCorrectiveLaunchUsesIndependentOrdinalCustody(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "corrective-independent")
	accepted := h.submitPreparedPermitted(t, ctx, "corrective-independent")
	if err := h.coordinator.Start(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	record := snapshot.Record
	if err := h.coordinator.certifyQuiescence(ctx, &record, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 2, model.PermitNonce("nonce-2"), nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.Start(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.Complete(ctx, accepted.Record.JobID, model.OutcomeCompleted, []byte("result"), nil); err != nil {
		t.Fatal(err)
	}

	snapshot = h.snapshot(t, ctx, accepted.Record.JobID)
	first, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok {
		t.Fatal("ordinal 1 launch missing")
	}
	second, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalTwo)
	if !ok {
		t.Fatal("ordinal 2 launch missing")
	}
	if first.Group == nil || second.Group == nil {
		t.Fatalf("launch groups = first:%#v second:%#v, want both groups", first.Group, second.Group)
	}
	if first.Group.CustodyID == second.Group.CustodyID {
		t.Fatalf("custody id reused across ordinals: %s", first.Group.CustodyID)
	}
	if first.Group.SamePhysicalIdentity(*second.Group) {
		t.Fatalf("physical identity reused across ordinals: first:%#v second:%#v", first.Group, second.Group)
	}
	if first.Grant == nil || second.Grant == nil || first.Grant.Ordinal != model.LaunchOrdinalOne || second.Grant.Ordinal != model.LaunchOrdinalTwo {
		t.Fatalf("launch grants = first:%#v second:%#v, want per-ordinal grants", first.Grant, second.Grant)
	}
	if first.Released == nil || second.Released == nil || first.Released.Ordinal != model.LaunchOrdinalOne || second.Released.Ordinal != model.LaunchOrdinalTwo {
		t.Fatalf("launch releases = first:%#v second:%#v, want per-ordinal releases", first.Released, second.Released)
	}
	if first.Quiescence == nil || second.Quiescence == nil {
		t.Fatalf("launch quiescence = first:%#v second:%#v, want both ordinals quiescent", first.Quiescence, second.Quiescence)
	}
	if !first.Quiescence.Group.Equal(*first.Group) || !second.Quiescence.Group.Equal(*second.Group) {
		t.Fatalf("quiescence groups = first:%#v second:%#v, want matching per-ordinal groups", first.Quiescence.Group, second.Quiescence.Group)
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
	if h.supervisor.contained != 1 || h.supervisor.retired != 0 {
		t.Fatalf("recovery effects contain=%d retire=%d, want 1/0", h.supervisor.contained, h.supervisor.retired)
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

func TestGrantBeforeCommitFailpointRetiresBoundGroupWithoutPermit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "grant-before-commit")
	accepted := h.submit(t, ctx, "grant-before-commit")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}

	injector := &FailureInjector{Target: FailGrantBeforeCommit}
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), injector); err == nil {
		t.Fatal("GrantPermit returned nil for grant before-commit failpoint")
	}
	if !injector.Hit {
		t.Fatal("grant before-commit failpoint was not hit")
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if hasCommittedGrant(snapshot.Record) {
		t.Fatalf("record after before-commit failure = %#v, want no grant", snapshot.Record)
	}
	launch, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launch.Quiescence == nil {
		t.Fatalf("launch after before-commit failure = %#v, want retired quiescence", launch)
	}
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing after before-commit recovery")
	}
	if snapshot.Record.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}
	if h.supervisor.permits != 0 {
		t.Fatalf("permit sends = %d, want 0", h.supervisor.permits)
	}
	if h.supervisor.retired != 1 {
		t.Fatalf("retire calls = %d, want 1", h.supervisor.retired)
	}
}

func TestPostGrantFailpointsRecoverWithTerminalProof(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name        string
		point       Failpoint
		wantPermits int
	}{
		{name: "after grant commit", point: FailGrantAfterCommit},
		{name: "before permit send", point: FailPermitSendBefore},
		{name: "after permit send", point: FailPermitSendAfter, wantPermits: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "post-grant-"+strings.ReplaceAll(tt.name, " ", "-"))
			accepted := h.submit(t, ctx, "post-grant-"+strings.ReplaceAll(tt.name, " ", "-"))
			if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
				t.Fatal(err)
			}
			injector := &FailureInjector{Target: tt.point}

			if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), injector); err == nil {
				t.Fatalf("GrantPermit returned nil for %s", tt.point)
			}
			if !injector.Hit {
				t.Fatalf("failpoint %s was not hit", tt.point)
			}
			snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
			if snapshot.Record.Terminal == nil {
				t.Fatal("terminal certificate missing after post-grant recovery")
			}
			if snapshot.Record.Terminal.Proof != model.ProofContained {
				t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofContained)
			}
			if snapshot.Record.Terminal.Outcome != model.OutcomeReaped {
				t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeReaped)
			}
			if h.supervisor.permits != tt.wantPermits {
				t.Fatalf("permit sends = %d, want %d", h.supervisor.permits, tt.wantPermits)
			}
		})
	}
}

func TestRegressionSeedPR28DoubleFaultNeverIssuesSecondGrant(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "pr28-double-fault")
	accepted := h.submit(t, ctx, "pr28-double-fault")
	if err := h.coordinator.PrepareSupervisor(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	h.supervisor.failSend = true
	h.supervisor.failContain = true

	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 1, model.PermitNonce("nonce-1"), nil); err == nil {
		t.Fatal("GrantPermit returned nil for PR #28 double fault")
	}
	if !h.authority.failStopped {
		t.Fatal("authority was not fail-stopped after PR #28 double fault")
	}

	h.supervisor.failSend = false
	h.supervisor.failContain = false
	if err := h.coordinator.GrantPermit(ctx, accepted.Record.JobID, 2, model.PermitNonce("nonce-2"), nil); err == nil {
		t.Fatal("second grant succeeded after PR #28 double fault")
	}
	if h.supervisor.permits != 0 {
		t.Fatalf("permit sends = %d, want no successful sends", h.supervisor.permits)
	}
	if err := h.repo.View(ctx, func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("safety state = %s, want valid", image.Safety.State)
		}
		launch, ok := image.Safety.Value.Attempt.Launches.Get(model.LaunchOrdinalTwo)
		if ok && launch.Grant != nil {
			t.Fatal("ordinal 2 grant was recorded after PR #28 double fault")
		}
		if image.Safety.Value.Terminal != nil && image.Safety.Value.Terminal.Proof != model.ProofContained {
			t.Fatalf("terminal proof = %s, want contained proof if terminalized", image.Safety.Value.Terminal.Proof)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorProductionIsStorageAdapterIndependent(t *testing.T) {
	for _, source := range coordinatorProductionSources(t) {
		if strings.Contains(source.text, "engine/execution/repository") ||
			strings.Contains(source.text, "engine/execution/storage") ||
			strings.Contains(source.text, "repository.") ||
			strings.Contains(source.text, "memory.") {
			t.Fatalf("%s names repository/storage concrete details", source.path)
		}
	}
}

func TestNoListenerFactoryCallExistsBeforeReadyCapability(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	disallowed := []string{"NewListener(", "ListenerFactory", "listenerFactory", ".Listen("}
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, needle := range disallowed {
			if strings.Contains(string(data), needle) {
				t.Fatalf("%s contains listener factory call %q before a Ready-owned integration exists", path, needle)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type harness struct {
	authority   *readyAuthority
	coordinator *Coordinator
	supervisor  *testSupervisor
	results     *testResults
	repo        *memory.Repository
}

func newHarness(t *testing.T, name string) *harness {
	t.Helper()
	repo := memory.NewRepository()
	issuer, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithQuiescenceVerifier(verifier))
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
	supervisor := &testSupervisor{issuer: issuer, boot: boot}
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
		repo:        repo,
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

func (a *readyAuthority) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef) (StepResult, error) {
	applied, err := a.ready.BindGroup(ctx, jobID, ref, ordinal, group)
	return stepResult(applied, err)
}

func (a *readyAuthority) CommitGrant(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, nonce model.PermitNonce) (StepResult, error) {
	applied, err := a.ready.CommitGrant(ctx, jobID, ref, ordinal, nonce)
	return stepResult(applied, err)
}

func (a *readyAuthority) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (StepResult, error) {
	applied, err := a.ready.RecordRelease(ctx, jobID, ref, ordinal, child, evidence)
	return stepResult(applied, err)
}

func (a *readyAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (StepResult, error) {
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return stepResult(applied, err)
}

func (a *readyAuthority) RequestCancel(ctx context.Context, jobID model.JobID) (StepResult, error) {
	applied, err := a.ready.RequestCancel(ctx, jobID)
	return stepResult(applied, err)
}

func (a *readyAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome) (StepResult, error) {
	applied, err := a.ready.RecordOutcome(ctx, jobID, ref, outcome)
	return stepResult(applied, err)
}

func (a *readyAuthority) RecordResult(ctx context.Context, jobID model.JobID, ref model.AttemptRef, receipt model.ResultReceipt) (StepResult, error) {
	applied, err := a.ready.RecordResult(ctx, jobID, ref, receipt)
	return stepResult(applied, err)
}

func (a *readyAuthority) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent) (StepResult, error) {
	applied, err := a.ready.Finalize(ctx, jobID, ref, intent)
	return stepResult(applied, err)
}

func stepResult(applied authority.ApplyResult, err error) (StepResult, error) {
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
	if trigger == model.RecoveryCancelAfterGrant && !hasAuthorizationEvidence(snapshot.Record) {
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

func hasAuthorizationEvidence(record model.SafetyRecord) bool {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && (launch.Grant != nil || launch.Released != nil) {
			return true
		}
	}
	return false
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
	if hasPreparedUnquiescedGroup(record) {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryRetireThenFinalize}
		return plan
	}
	plan.Next = model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}
	return plan
}

func hasPreparedUnquiescedGroup(record model.SafetyRecord) bool {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && launch.Group != nil && launch.Quiescence == nil {
			return true
		}
	}
	return false
}

func hasCommittedGrant(record model.SafetyRecord) bool {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && launch.Grant != nil {
			return true
		}
	}
	return false
}

type testSupervisor struct {
	next        int
	permits     int
	contained   int
	retired     int
	failSend    bool
	failContain bool
	issuer      custodian.AttestationIssuer
	boot        model.BootRef
}

func (s *testSupervisor) Prepare(ctx context.Context, plan LaunchPlan) (PreparedSupervisor, error) {
	if err := ctx.Err(); err != nil {
		return PreparedSupervisor{}, err
	}
	s.next++
	group := model.GroupRef{
		Version:   1,
		CustodyID: model.CustodyID(fmt.Sprintf("custody-%s-%s", plan.Ref.JobID, plan.Ordinal)),
		Launch: model.LaunchKey{
			Attempt: plan.Ref,
			Ordinal: plan.Ordinal,
		},
		HostBootID: "host-boot-" + plan.Ref.JobID.String(),
		PGID:       1000 + s.next,
		Leader: model.ProcessIdentity{
			PID:               2000 + s.next,
			HighResStartToken: fmt.Sprintf("leader-token-%d", s.next),
		},
		Monitor: model.ProcessIdentity{
			PID:               3000 + s.next,
			HighResStartToken: fmt.Sprintf("monitor-token-%d", s.next),
		},
		RetainedID: fmt.Sprintf("retained-%d", s.next),
	}
	return PreparedSupervisor{
		Ref:     plan.Ref,
		Ordinal: plan.Ordinal,
		Group:   group,
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
			PID:               prepared.Group.Leader.PID,
			HighResStartToken: prepared.Group.Leader.HighResStartToken,
		},
		Evidence: evidence("launch-consumed"),
	}, nil
}

func (s *testSupervisor) VerifyQuiescence(ctx context.Context, prepared PreparedSupervisor, released model.LaunchReleaseFact) (custodian.VerifiedQuiescence, error) {
	if err := ctx.Err(); err != nil {
		return custodian.VerifiedQuiescence{}, err
	}
	return s.attest(prepared, model.QuiescenceNaturalExit)
}

func (s *testSupervisor) Contain(ctx context.Context, prepared PreparedSupervisor) (custodian.VerifiedQuiescence, error) {
	if err := ctx.Err(); err != nil {
		return custodian.VerifiedQuiescence{}, err
	}
	if s.failContain {
		return custodian.VerifiedQuiescence{}, errors.New("containment failed")
	}
	s.contained++
	return s.attest(prepared, model.QuiescenceTermKill)
}

func (s *testSupervisor) Retire(ctx context.Context, prepared PreparedSupervisor) (custodian.VerifiedQuiescence, error) {
	if err := ctx.Err(); err != nil {
		return custodian.VerifiedQuiescence{}, err
	}
	s.retired++
	return s.attest(prepared, model.QuiescenceAlreadyAbsent)
}

func (s *testSupervisor) attest(prepared PreparedSupervisor, method model.QuiescenceMethod) (custodian.VerifiedQuiescence, error) {
	return s.issuer.AttestQuiescence(model.QuiescenceCertificate{
		Attempt:     prepared.Ref,
		Ordinal:     prepared.Ordinal,
		Group:       prepared.Group,
		Method:      method,
		CertifiedBy: s.boot,
	})
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

type productionSource struct {
	path string
	text string
}

func coordinatorProductionSources(t *testing.T) []productionSource {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sources []productionSource
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, productionSource{path: path, text: string(data)})
	}
	return sources
}
