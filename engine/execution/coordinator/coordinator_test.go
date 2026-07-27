package coordinator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
	"github.com/charlesnpx/agentbus/internal/containment"
)

func TestAuthorityLifecycleCompletesFromLiveLaunchFacts(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "lifecycle")
	accepted := h.submit(t, ctx, "lifecycle")
	group := h.bindGrantReleaseQuiescence(t, ctx, accepted, model.LaunchOrdinalOne, model.QuiescenceNaturalExit)

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
	first, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence == nil || !first.Quiescence.Group.Equal(group) {
		t.Fatalf("launch quiescence = %+v, want preserved live quiescence for group %+v", first, group)
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

func TestCompleteWithoutQuiescenceTerminalizesUnresolved(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "complete-no-certify")
	accepted := h.submit(t, ctx, "complete-no-certify")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)

	if err := h.coordinator.Complete(ctx, accepted.Record.JobID, model.OutcomeCompleted, []byte("result"), nil); err != nil {
		t.Fatal(err)
	}
	if h.containment.contained != 0 || h.containment.retired != 0 {
		t.Fatalf("launch containment calls = contain:%d retire:%d, want 0/0", h.containment.contained, h.containment.retired)
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeCompleted)
	}
	if snapshot.Record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofUnresolvedAbsence)
	}
	if got := model.DeriveCleanupDisposition(snapshot.Record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
	first, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence != nil {
		t.Fatalf("launch quiescence = %+v, want no coordinator-certified quiescence", first)
	}
}

func TestCancelBeforePermitRetiresWithoutContainment(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-before")
	accepted := h.submit(t, ctx, "cancel-before")
	h.bindGroup(t, ctx, accepted, model.LaunchOrdinalOne)

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
	if h.containment.contained != 0 || h.containment.retired != 1 {
		t.Fatalf("launch containment contain=%d retire=%d, want 0/1", h.containment.contained, h.containment.retired)
	}
}

func TestCancelAfterPermitContainsBeforeTerminal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-after")
	accepted := h.submit(t, ctx, "cancel-after")
	h.bindGrant(t, ctx, accepted, model.LaunchOrdinalOne)

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
	if h.containment.contained != 1 || h.containment.retired != 0 {
		t.Fatalf("launch containment contain=%d retire=%d, want 1/0", h.containment.contained, h.containment.retired)
	}
}

func TestCancelAfterPermitRecordsQuiescenceBeforeCleanupWarning(t *testing.T) {
	var logs bytes.Buffer
	oldLogWriter := log.Writer()
	oldLogFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldLogWriter)
		log.SetFlags(oldLogFlags)
	}()

	ctx := context.Background()
	h := newHarness(t, "cancel-cleanup-order")
	events := &coordinatorEventLog{}
	h.authority.events = events
	h.containment.events = events
	cleanupErr := errors.New("cleanup failed after valid containment proof")
	h.containment.cleanupErr = cleanupErr
	accepted := h.submit(t, ctx, "cancel-cleanup-order")
	h.bindGrant(t, ctx, accepted, model.LaunchOrdinalOne)

	err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil)
	if err != nil {
		t.Fatalf("Cancel error = %v, want nil cleanup warning", err)
	}
	if h.authority.failStopped {
		t.Fatalf("authority fail-stopped after post-proof cleanup warning: %v", h.authority.failReason)
	}
	wantEvents := "contain,record_quiescence"
	if got := strings.Join(events.snapshot(), ","); got != wantEvents {
		t.Fatalf("events = %s, want %s", got, wantEvents)
	}
	var record model.SafetyRecord
	if err := h.repo.View(ctx, func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("safety state = %s, want valid", image.Safety.State)
		}
		record = image.Safety.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence == nil {
		t.Fatalf("launch quiescence = %+v, want recorded despite cleanup warning", first)
	}
	if record.Terminal == nil || record.Terminal.Proof != model.ProofContained {
		t.Fatalf("terminal = %+v, want contained proof", record.Terminal)
	}
	if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionVerifiedAbsent {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionVerifiedAbsent)
	}
	gotLogs := logs.String()
	if !strings.Contains(gotLogs, "cleanup warning") ||
		!strings.Contains(gotLogs, cleanupErr.Error()) ||
		!strings.Contains(gotLogs, accepted.Record.JobID.String()) {
		t.Fatalf("cleanup warning log = %q, want job id and cleanup error", gotLogs)
	}
}

func TestCancelAfterPermitTypedUnresolvedTerminalizesCanceled(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-unresolved-terminal")
	accepted := h.submit(t, ctx, "cancel-unresolved-terminal")
	h.bindGrant(t, ctx, accepted, model.LaunchOrdinalOne)
	h.containment.unresolvedOrdinals = map[model.LaunchOrdinal]bool{model.LaunchOrdinalOne: true}

	if err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil); err != nil {
		t.Fatal(err)
	}
	if h.authority.failStopped {
		t.Fatalf("authority fail-stopped after typed unresolved cleanup: %v", h.authority.failReason)
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeCanceled {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeCanceled)
	}
	if snapshot.Record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofUnresolvedAbsence)
	}
	if snapshot.Record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
		t.Fatalf("cause = %s, want %s", snapshot.Record.Terminal.Cause, model.CauseCanceledAfterAuthorization)
	}
	if got := model.DeriveCleanupDisposition(snapshot.Record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
}

func TestCancelAfterPermitFailsClosedWhenContainmentUnprovable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-unprovable")
	accepted := h.submit(t, ctx, "cancel-unprovable")
	h.bindGrant(t, ctx, accepted, model.LaunchOrdinalOne)
	h.containment.failContain = true

	err := h.coordinator.Cancel(ctx, accepted.Record.JobID, nil)
	if err == nil {
		t.Fatal("Cancel returned nil for unprovable containment")
	}
	if !h.authority.failStopped {
		t.Fatal("authority was not fail-stopped after unprovable cancel containment")
	}
	if h.containment.contained != 1 || h.containment.retired != 0 {
		t.Fatalf("launch containment contain=%d retire=%d, want 1/0 failed containment attempt", h.containment.contained, h.containment.retired)
	}
	if err := h.repo.View(ctx, func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("safety state = %s, want valid", image.Safety.State)
		}
		record := image.Safety.Value
		if record.Cancel == nil {
			t.Fatal("cancel intent was not durably recorded")
		}
		if record.Terminal != nil {
			t.Fatalf("terminal = %+v, want none after unprovable containment", record.Terminal)
		}
		first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
		if !ok || first.Quiescence != nil {
			t.Fatalf("launch proof = %+v, want no quiescence after failed containment", first)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverLiveLossContainsAndReaps(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "loss")
	accepted := h.submit(t, ctx, "loss")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)

	if err := h.coordinator.Recover(ctx, accepted.Record.JobID, model.RecoveryLiveLoss, nil); err != nil {
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

func TestRecoverLiveLossTypedUnresolvedOrphans(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "live-loss-unresolved")
	accepted := h.submit(t, ctx, "live-loss-unresolved")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)
	h.containment.unresolvedOrdinals = map[model.LaunchOrdinal]bool{model.LaunchOrdinalOne: true}

	if err := h.coordinator.Recover(ctx, accepted.Record.JobID, model.RecoveryLiveLoss, nil); err != nil {
		t.Fatal(err)
	}
	if h.authority.failStopped {
		t.Fatalf("authority fail-stopped after live unresolved cleanup: %v", h.authority.failReason)
	}

	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeOrphaned {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeOrphaned)
	}
	if snapshot.Record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofUnresolvedAbsence)
	}
	if snapshot.Record.Terminal.Cause != model.CauseSupervisorLostAfterAuthorization {
		t.Fatalf("cause = %s, want %s", snapshot.Record.Terminal.Cause, model.CauseSupervisorLostAfterAuthorization)
	}
	if got := model.DeriveCleanupDisposition(snapshot.Record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
}

func TestRecoverRecordedCompletedTypedUnresolvedKeepsResult(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "completed-unresolved")
	accepted := h.submit(t, ctx, "completed-unresolved")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)
	if _, err := h.authority.RecordOutcome(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.OutcomeCompleted); err != nil {
		t.Fatal(err)
	}
	if err := h.coordinator.publishResult(ctx, accepted.Record.JobID, []byte("completed-result"), nil); err != nil {
		t.Fatal(err)
	}
	h.containment.unresolvedOrdinals = map[model.LaunchOrdinal]bool{model.LaunchOrdinalOne: true}

	if err := h.coordinator.Recover(ctx, accepted.Record.JobID, model.RecoveryLiveLoss, nil); err != nil {
		t.Fatal(err)
	}
	if h.authority.failStopped {
		t.Fatalf("authority fail-stopped after completed unresolved cleanup: %v", h.authority.failReason)
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if snapshot.Record.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("outcome = %s, want %s", snapshot.Record.Terminal.Outcome, model.OutcomeCompleted)
	}
	if snapshot.Record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("proof = %s, want %s", snapshot.Record.Terminal.Proof, model.ProofUnresolvedAbsence)
	}
	if snapshot.Record.Terminal.Result == nil || snapshot.Record.Result == nil {
		t.Fatalf("result was not preserved: terminal=%+v record=%+v", snapshot.Record.Terminal.Result, snapshot.Record.Result)
	}
	if got := model.DeriveCleanupDisposition(snapshot.Record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
	if h.containment.contained != 1 {
		t.Fatalf("containments = %d, want no relaunch and one containment", h.containment.contained)
	}
}

func TestCompleteContradictoryOpenOutcomeConflictIsNotReconciledByLaterTerminal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "open-outcome-conflict")
	accepted := h.submit(t, ctx, "open-outcome-conflict")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)
	if _, err := h.authority.RecordOutcome(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.OutcomeFailed); err != nil {
		t.Fatal(err)
	}
	auth := &terminalizingConflictAuthority{readyAuthority: h.authority}
	coord, err := New(auth, h.containment, h.results, model.OwnerID("coordinator-open-outcome-conflict"))
	if err != nil {
		t.Fatal(err)
	}

	err = coord.Complete(ctx, accepted.Record.JobID, model.OutcomeCompleted, []byte("result"), nil)
	if err == nil {
		t.Fatal("Complete returned nil for contradictory open outcome")
	}
	if !errors.Is(err, model.ErrConflictingDuplicate) {
		t.Fatalf("Complete error = %v, want ErrConflictingDuplicate", err)
	}
	if errors.Is(err, ErrAlreadyFinalized) {
		t.Fatalf("Complete error = %v, want unreconciled open-record conflict", err)
	}
	if !auth.finalizedAfterConflict {
		t.Fatal("test authority did not finalize after the open-record conflict")
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if snapshot.Record.Terminal == nil || snapshot.Record.Terminal.Outcome != model.OutcomeFailed {
		t.Fatalf("terminal = %+v, want later failed terminal snapshot", snapshot.Record.Terminal)
	}
}

func TestCancelDuringPendingCompletedResultAwaitsSettlementWithoutFailStop(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "cancel-pending-completion")
	accepted := h.submit(t, ctx, "cancel-pending-completion")
	h.bindGrantRelease(t, ctx, accepted, model.LaunchOrdinalOne)
	auth := &awaitingRecoveryAuthority{
		readyAuthority: h.authority,
		awaited:        make(chan struct{}, 1),
	}
	coord, err := New(auth, h.containment, h.results, model.OwnerID("coordinator-cancel-pending-completion"))
	if err != nil {
		t.Fatal(err)
	}
	outcomeRecorded := make(chan struct{}, 1)
	allowPublish := make(chan struct{})
	h.results.beforePublish = func(ctx context.Context, _ model.JobID, _ []byte) error {
		select {
		case outcomeRecorded <- struct{}{}:
		default:
		}
		select {
		case <-allowPublish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	completeDone := make(chan error, 1)
	go func() {
		completeDone <- coord.Complete(ctx, accepted.Record.JobID, model.OutcomeCompleted, []byte("completed-result"), nil)
	}()
	publishReleased := false
	defer func() {
		if !publishReleased {
			close(allowPublish)
		}
	}()

	select {
	case <-outcomeRecorded:
	case <-time.After(5 * time.Second):
		t.Fatal("completion did not record outcome before publishing result")
	}
	mid := h.snapshot(t, ctx, accepted.Record.JobID)
	if mid.Record.Outcome == nil || mid.Record.Outcome.Outcome != model.OutcomeCompleted || mid.Record.Result != nil || mid.Record.Terminal != nil {
		t.Fatalf("mid-race record = %+v, want completed outcome without result or terminal", mid.Record)
	}

	cancelDone := make(chan error, 1)
	go func() {
		cancelDone <- coord.Cancel(ctx, accepted.Record.JobID, nil)
	}()
	select {
	case <-auth.awaited:
	case <-time.After(5 * time.Second):
		t.Fatal("cancel recovery did not await the pending completion result")
	}
	close(allowPublish)
	publishReleased = true

	completeErr := receiveCoordinatorErr(t, completeDone, "Complete")
	if completeErr != nil && !errors.Is(completeErr, ErrAlreadyFinalized) {
		t.Fatalf("Complete error = %v, want nil or ErrAlreadyFinalized contention", completeErr)
	}
	if cancelErr := receiveCoordinatorErr(t, cancelDone, "Cancel"); cancelErr != nil {
		t.Fatalf("Cancel error = %v", cancelErr)
	}
	if h.authority.failStopped {
		t.Fatalf("authority fail-stopped during pending completion recovery: %v", h.authority.failReason)
	}
	snapshot := h.snapshot(t, ctx, accepted.Record.JobID)
	if err := model.ValidateSafetyRecord(snapshot.Record); err != nil {
		t.Fatalf("settled safety record is invalid: %v", err)
	}
	if snapshot.Record.Terminal == nil ||
		snapshot.Record.Terminal.Outcome != model.OutcomeCompleted ||
		snapshot.Record.Terminal.Result == nil ||
		snapshot.Record.Result == nil {
		t.Fatalf("terminal = %+v result = %+v, want completed terminal with result", snapshot.Record.Terminal, snapshot.Record.Result)
	}
}

func TestShutdownBlocksUntilOwnedWorkDrained(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "shutdown")
	accepted := h.submit(t, ctx, "shutdown")

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

func TestContainmentFailpointFailStops(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "contain-failpoint")
	accepted := h.submit(t, ctx, "contain-failpoint")
	h.bindGrant(t, ctx, accepted, model.LaunchOrdinalOne)
	injector := &FailureInjector{Target: FailContainSignal}

	err := h.coordinator.Cancel(ctx, accepted.Record.JobID, injector)
	if err == nil {
		t.Fatal("Cancel returned nil for containment failpoint")
	}
	if !injector.Hit {
		t.Fatal("containment failpoint was not hit")
	}
	if !h.authority.failStopped {
		t.Fatal("authority was not fail-stopped after containment failpoint")
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
	containment *testLaunchContainment
	results     *testResults
	repo        *memory.Repository
	nextGroup   int
}

type coordinatorEventLog struct {
	events []string
}

func (log *coordinatorEventLog) add(event string) {
	log.events = append(log.events, event)
}

func (log *coordinatorEventLog) snapshot() []string {
	return append([]string(nil), log.events...)
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
	containment := &testLaunchContainment{issuer: issuer}
	results := &testResults{receipts: map[model.ResultRef]model.ResultReceipt{}}
	coordinator, err := New(auth, containment, results, model.OwnerID("coordinator-"+name))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		authority:   auth,
		coordinator: coordinator,
		containment: containment,
		results:     results,
		repo:        repo,
	}
}

func (h *harness) submit(t *testing.T, ctx context.Context, name string) authority.AcceptResult {
	t.Helper()
	accepted, err := h.authority.ready.AcceptAndClaim(ctx, admissionRequest(t, name), model.OwnerID("coordinator-"+name))
	if err != nil {
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

func (h *harness) bindGroup(t *testing.T, ctx context.Context, accepted authority.AcceptResult, ordinal model.LaunchOrdinal) model.GroupRef {
	t.Helper()
	h.nextGroup++
	group := testGroup(accepted.Record.Attempt.Ref, ordinal, h.nextGroup)
	if _, err := h.authority.ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, ordinal, group); err != nil {
		t.Fatal(err)
	}
	return group
}

func (h *harness) bindGrant(t *testing.T, ctx context.Context, accepted authority.AcceptResult, ordinal model.LaunchOrdinal) model.GroupRef {
	t.Helper()
	group := h.bindGroup(t, ctx, accepted, ordinal)
	if _, err := h.authority.ready.CommitGrant(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, ordinal, model.PermitNonce(fmt.Sprintf("nonce-%s", ordinal))); err != nil {
		t.Fatal(err)
	}
	return group
}

func (h *harness) bindGrantRelease(t *testing.T, ctx context.Context, accepted authority.AcceptResult, ordinal model.LaunchOrdinal) model.GroupRef {
	t.Helper()
	group := h.bindGrant(t, ctx, accepted, ordinal)
	child := model.ChildIdentity{
		PID:               group.Leader.PID,
		HighResStartToken: group.Leader.HighResStartToken,
	}
	if _, err := h.authority.ready.RecordRelease(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, ordinal, child, evidence("launch-released")); err != nil {
		t.Fatal(err)
	}
	return group
}

func (h *harness) bindGrantReleaseQuiescence(t *testing.T, ctx context.Context, accepted authority.AcceptResult, ordinal model.LaunchOrdinal, method model.QuiescenceMethod) model.GroupRef {
	t.Helper()
	group := h.bindGrantRelease(t, ctx, accepted, ordinal)
	h.recordQuiescence(t, ctx, accepted.Record.JobID, ordinal, group, method)
	return group
}

func (h *harness) recordQuiescence(t *testing.T, ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, group model.GroupRef, method model.QuiescenceMethod) {
	t.Helper()
	verified, err := h.containment.issuer.AttestQuiescence(custodian.PhysicalQuiescence{
		Group:  group,
		Method: method,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.authority.ready.RecordQuiescence(ctx, jobID, ordinal, verified); err != nil {
		t.Fatal(err)
	}
}

func receiveCoordinatorErr(t *testing.T, done <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", name)
	}
	return nil
}

func admissionRequest(t *testing.T, name string) authority.AcceptRequest {
	t.Helper()
	key, err := model.NewRequestKey("workspace-"+name, "request-"+name)
	if err != nil {
		t.Fatal(err)
	}
	return authority.AcceptRequest{
		RequestKey:   key,
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("task-" + name)),
		Mode:         model.ModeIdentifiedFenced,
	}
}

func testGroup(ref model.AttemptRef, ordinal model.LaunchOrdinal, seq int) model.GroupRef {
	pgid := 1000 + seq
	return model.GroupRef{
		Version:   1,
		CustodyID: model.CustodyID(fmt.Sprintf("custody-%s-%s", ref.JobID, ordinal)),
		Launch: model.LaunchKey{
			Attempt: ref,
			Ordinal: ordinal,
		},
		HostBootID:          "host-boot-" + ref.JobID.String(),
		PIDNamespaceState:   model.PIDNamespaceNotApplicable,
		RetainedDomainID:    fmt.Sprintf("retained-domain-%d", seq),
		RetainedDomainState: model.RetainedDomainKnown,
		PGID:                pgid,
		Leader: model.ProcessIdentity{
			PID:               pgid,
			HighResStartToken: fmt.Sprintf("leader-token-%d", seq),
		},
		Monitor: model.ProcessIdentity{
			PID:               3000 + seq,
			HighResStartToken: fmt.Sprintf("monitor-token-%d", seq),
		},
		RetainedID: fmt.Sprintf("retained-%d", seq),
	}
}

type readyAuthority struct {
	ready       *authority.Ready
	failStopped bool
	failReason  error
	events      *coordinatorEventLog
}

type terminalizingConflictAuthority struct {
	*readyAuthority
	finalizedAfterConflict bool
}

func (a *terminalizingConflictAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome) (StepResult, error) {
	applied, err := a.readyAuthority.RecordOutcome(ctx, jobID, ref, outcome)
	if err == nil || !errors.Is(err, model.ErrConflictingDuplicate) {
		return applied, err
	}
	snapshot, snapshotErr := a.readyAuthority.Snapshot(ctx, jobID)
	if snapshotErr != nil {
		return applied, errors.Join(err, snapshotErr)
	}
	_, finalizeErr := a.readyAuthority.Finalize(ctx, jobID, snapshot.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeFailed,
		Cause:   model.CauseCompletedNormally,
	})
	if finalizeErr != nil {
		return applied, errors.Join(err, finalizeErr)
	}
	a.finalizedAfterConflict = true
	return applied, err
}

type awaitingRecoveryAuthority struct {
	*readyAuthority
	awaited chan struct{}
}

func (a *awaitingRecoveryAuthority) RecoveryPlan(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger) (model.RecoveryPlan, error) {
	plan, err := a.readyAuthority.RecoveryPlan(ctx, jobID, trigger)
	if err == nil && plan.Next.Kind == model.RecoveryAwaitResultCertificate {
		select {
		case a.awaited <- struct{}{}:
		default:
		}
	}
	return plan, err
}

func (a *readyAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (StepResult, error) {
	if a.events != nil {
		a.events.add("record_quiescence")
	}
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

func (a *readyAuthority) HasOwnedWork(ctx context.Context) (bool, error) {
	snapshot, err := a.ready.RuntimeSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0, nil
}

func (a *readyAuthority) FailStop(ctx context.Context, err error) error {
	if a.events != nil {
		a.events.add("fail_stop")
	}
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

type testLaunchContainment struct {
	contained          int
	retired            int
	failContain        bool
	cleanupErr         error
	unresolvedOrdinals map[model.LaunchOrdinal]bool
	issuer             custodian.AttestationIssuer
	events             *coordinatorEventLog
}

func (c *testLaunchContainment) ContainAndVerify(ctx context.Context, launchCtx launch.LaunchContext, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if c.events != nil {
		c.events.add("contain")
	}
	if err := ctx.Err(); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	if err := launchCtx.Validate(); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	if err := group.Validate(); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	if !group.Launch.Attempt.Equal(launchCtx.Attempt) || group.Launch.Ordinal != launchCtx.Ordinal {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, errors.New("launch containment group mismatch")
	}
	method := model.QuiescenceTermKill
	switch cause {
	case custodian.QuiescenceCauseRecovery:
		c.retired++
		method = model.QuiescenceAlreadyAbsent
	default:
		c.contained++
	}
	if c.failContain {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, errors.New("containment failed")
	}
	if c.unresolvedOrdinals[launchCtx.Ordinal] {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, &custodian.CleanupUnresolvedError{
			Reason:   containment.ReasonProbeUnprovable,
			Decision: model.Unprovable,
		}
	}
	verified, err := c.issuer.AttestQuiescence(custodian.PhysicalQuiescence{
		Group:  group,
		Method: method,
	})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: c.cleanupErr}, nil
}

type testResults struct {
	published     int
	verified      int
	receipts      map[model.ResultRef]model.ResultReceipt
	beforePublish func(context.Context, model.JobID, []byte) error
}

func (r *testResults) Publish(ctx context.Context, jobID model.JobID, payload []byte) (model.ResultReceipt, error) {
	if err := ctx.Err(); err != nil {
		return model.ResultReceipt{}, err
	}
	if r.beforePublish != nil {
		if err := r.beforePublish(ctx, jobID, payload); err != nil {
			return model.ResultReceipt{}, err
		}
	}
	r.published++
	ref := model.ResultRef{
		Path:   "results/" + jobID.String() + ".txt",
		Digest: fmt.Sprintf("sha256:%x", len(payload)),
		Bytes:  int64(len(payload)),
	}
	receipt := model.ResultReceipt{
		JobID:     jobID,
		Result:    ref,
		DirSynced: evidence("result-dir-synced"),
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
