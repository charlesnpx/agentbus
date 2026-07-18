package authority

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

type readyAdmission interface {
	Accept(context.Context, AcceptRequest) (AcceptResult, error)
	BindGroup(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.GroupRef, ...ApplyOption) (ApplyResult, error)
}

var testAttestationIssuers sync.Map

func TestReadyTypestateExposesAdmissionOnlyAfterSeal(t *testing.T) {
	var _ readyAdmission = (*Ready)(nil)
	if _, ok := any(&Bootstrapper{}).(readyAdmission); ok {
		t.Fatal("Bootstrapper exposes admission methods")
	}
	if _, ok := any(&RecoverySession{}).(readyAdmission); ok {
		t.Fatal("RecoverySession exposes admission methods")
	}

	session := newRecoverySession(t, memory.NewRepository(), "typestate")
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ready.Boot().BootID != "boot-typestate" {
		t.Fatalf("ready boot = %s, want boot-typestate", ready.Boot().BootID)
	}
}

func TestAcceptWritesBindingSafetyProjectionAndReplays(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	ready := newReady(t, repo, "accept")
	request := acceptRequest(t, "accept")

	accepted, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Replayed {
		t.Fatal("first acceptance reported replay")
	}
	assertAcceptedImage(t, repo, accepted)

	replayed, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed {
		t.Fatal("second acceptance did not report replay")
	}
	if replayed.Record.JobID != accepted.Record.JobID {
		t.Fatalf("replay job = %s, want %s", replayed.Record.JobID, accepted.Record.JobID)
	}
	if replayed.Commit.Generation != accepted.Commit.Generation {
		t.Fatalf("replay generation = %d, want unchanged %d", replayed.Commit.Generation, accepted.Commit.Generation)
	}

	snapshot, err := ready.RuntimeSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Pending) != 1 || !snapshot.Pending[0].Equal(accepted.Record.Attempt.Ref) {
		t.Fatalf("pending snapshot = %#v, want accepted attempt", snapshot.Pending)
	}
}

func TestAcceptAdvanceFailureReturnsAcceptedResultAndDurableFailStop(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "accept-advance-fail")
	advanceErr := errors.New("advance fsync failed")
	anchorStore.FailNextForTest(AnchorAdvance, advanceErr)

	accepted, err := ready.Accept(ctx, acceptRequest(t, "accept-advance-fail"))
	if !errors.Is(err, advanceErr) {
		t.Fatalf("Accept error = %v, want injected advance failure", err)
	}
	if accepted.Record.JobID == "" || accepted.Commit.Generation == 0 {
		t.Fatalf("accepted result = %+v, want populated result with commit", accepted)
	}
	if accepted.Record.Cancel != nil {
		t.Fatalf("accepted record has cancel after advance failure: %+v", accepted.Record.Cancel)
	}
	if accepted.Record.Terminal != nil {
		t.Fatalf("accepted record terminal after advance failure: %+v", accepted.Record.Terminal)
	}
	assertAcceptedImage(t, repo, accepted)
	assertAnchorFailStopped(t, anchorStore)
	if _, err := ready.Accept(ctx, acceptRequest(t, "accept-after-advance-fail")); !errors.Is(err, ErrFailStopped) {
		t.Fatalf("accept after fail-stop error = %v, want ErrFailStopped", err)
	}
}

func TestAcceptAdvanceFailureReportsFailStopRecordFailure(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "accept-fail-stop-record")
	advanceErr := errors.New("advance fsync failed")
	failStopErr := errors.New("fail-stop fsync failed")
	anchorStore.FailNextForTest(AnchorAdvance, advanceErr)
	anchorStore.FailNextForTest(AnchorFailStop, failStopErr)

	accepted, err := ready.Accept(ctx, acceptRequest(t, "accept-fail-stop-record"))
	if !errors.Is(err, ErrFailStopRecord) || !errors.Is(err, advanceErr) || !errors.Is(err, failStopErr) {
		t.Fatalf("Accept error = %v, want ErrFailStopRecord wrapping advance and fail-stop failures", err)
	}
	if accepted.Record.JobID == "" || accepted.Commit.Generation == 0 {
		t.Fatalf("accepted result = %+v, want populated result with commit", accepted)
	}
	assertAcceptedImage(t, repo, accepted)
	if _, err := ready.Accept(ctx, acceptRequest(t, "accept-after-fail-stop-record")); !errors.Is(err, ErrFailStopped) {
		t.Fatalf("accept after in-memory fail-stop error = %v, want ErrFailStopped", err)
	}
}

func TestAcceptAndClaimConflictReturnsAcceptedResultAndDurableFailStop(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "accept-claim-fail")
	futureJobID := model.JobID("job-00000000000000000001")
	ready.core.runtime.owned[futureJobID] = OwnedAttempt{
		Ref:   model.AttemptRef{JobID: futureJobID, AttemptID: "attempt-conflicting-owner", Epoch: 1},
		Owner: model.OwnerID("owner-conflicting"),
	}

	accepted, err := ready.AcceptAndClaim(ctx, acceptRequest(t, "accept-claim-fail"), model.OwnerID("owner-claim"))
	if !errors.Is(err, ErrRuntimeConflict) {
		t.Fatalf("AcceptAndClaim error = %v, want ErrRuntimeConflict", err)
	}
	if accepted.Record.JobID == "" || accepted.Commit.Generation == 0 {
		t.Fatalf("accepted result = %+v, want populated result with commit", accepted)
	}
	if accepted.Record.Cancel != nil {
		t.Fatalf("accepted record has cancel after claim failure: %+v", accepted.Record.Cancel)
	}
	assertAcceptedImage(t, repo, accepted)
	assertAnchorFailStopped(t, anchorStore)
}

func TestAcceptPreCommitFailureDoesNotFailStopOrPersistAcceptedJob(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "accept-pre-commit-fail")
	request := acceptRequest(t, "accept-pre-commit-fail")
	repo.InjectTombstoneForTest(repository.Tombstone{
		RequestKey:        request.RequestKey,
		JobID:             model.JobID("job-expired"),
		TaskIdentity:      request.TaskIdentity,
		ExpiredGeneration: 1,
	})

	accepted, err := ready.Accept(ctx, request)
	if !errors.Is(err, ErrRequestExpired) {
		t.Fatalf("Accept error = %v, want ErrRequestExpired", err)
	}
	if accepted.Record.JobID != "" || accepted.Commit.Generation != 0 {
		t.Fatalf("accepted result = %+v, want empty pre-commit result", accepted)
	}
	assertNoAcceptedJobs(t, repo)
	snapshot := anchorSnapshot(t, anchorStore)
	if snapshot.Phase != "ready" {
		t.Fatalf("anchor phase = %q, want ready", snapshot.Phase)
	}
}

func TestAcceptRaceRecheckRejectsConflictingReplay(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "replay-conflict")
	request := acceptRequest(t, "replay-conflict")
	if _, err := ready.Accept(ctx, request); err != nil {
		t.Fatal(err)
	}

	request.TaskIdentity = model.NewSHA256TaskIdentity([]byte("different-task"))
	_, err := ready.Accept(ctx, request)
	if !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrReplayConflict", err)
	}
}

func TestReadyRejectsStaleAndNotReadyAtTransactionBoundary(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "stale")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "stale"))
	if err != nil {
		t.Fatal(err)
	}
	stale := *ready

	group := groupRef(accepted.Record.Attempt.Ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.RequestCancel(ctx, accepted.Record.JobID); !errors.Is(err, ErrStaleCapability) {
		t.Fatalf("stale apply error = %v, want ErrStaleCapability", err)
	}

	session := newRecoverySession(t, memory.NewRepository(), "not-ready")
	notReady := &Ready{core: session.core, token: readyCapability{boot: session.token.boot, token: "not-ready", generation: session.token.generation}}
	_, err = notReady.Accept(ctx, acceptRequest(t, "not-ready"))
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("not-ready accept error = %v, want ErrNotReady", err)
	}
}

func TestApplyReducerProjectionAndTerminalReleaseAfterCommit(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "terminal")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "terminal"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	owner := model.OwnerID("owner-terminal")
	if err := ready.ClaimPending(ctx, ref, owner); err != nil {
		t.Fatal(err)
	}

	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, err := ready.RequestCancel(ctx, accepted.Record.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)); err != nil {
		t.Fatal(err)
	}
	if _, err := ready.Finalize(ctx, accepted.Record.JobID, ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	}); err != nil {
		t.Fatal(err)
	}

	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.State != repository.RecordValid || image.Safety.Value.Terminal == nil {
		t.Fatalf("terminal safety image = %#v", image.Safety)
	}
	if image.Safety.Value.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("terminal proof = %s, want %s", image.Safety.Value.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}

	snapshot, err := ready.RuntimeSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0 {
		t.Fatalf("runtime snapshot after terminal = %#v, want empty", snapshot)
	}
}

func TestRecoverySessionPlansBlockReadinessForPriorBootWork(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	oldReady := newReady(t, repo, "old")
	if _, err := oldReady.Accept(ctx, acceptRequest(t, "old")); err != nil {
		t.Fatal(err)
	}

	session := newRecoverySession(t, repo, "new")
	plans, err := session.Plans(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("recovery plans = %d, want 1", len(plans))
	}
	if _, err := session.SealReady(ctx); !errors.Is(err, ErrRecoveryNeeded) {
		t.Fatalf("SealReady error = %v, want ErrRecoveryNeeded", err)
	}
	if _, err := oldReady.Accept(ctx, acceptRequest(t, "old-after-new-boot")); !errors.Is(err, ErrStaleCapability) {
		t.Fatalf("old ready after new boot error = %v, want ErrStaleCapability", err)
	}
}

func TestFailStopRejectsLaterAuthorityOperations(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "fail-stop")
	if err := ready.FailStop(ctx, "test stop"); err != nil {
		t.Fatal(err)
	}
	_, err := ready.Accept(ctx, acceptRequest(t, "fail-stop"))
	if !errors.Is(err, ErrFailStopped) {
		t.Fatalf("accept after fail-stop error = %v, want ErrFailStopped", err)
	}
}

func TestReadyFailStopWithDeadCallerContextRecordsDurably(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "expired",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				<-ctx.Done()
				return ctx, cancel
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.NewRepository()
			anchorStore := NewAnchorStore()
			ready := newReadyWithAnchorStore(t, repo, anchorStore, "fail-stop-dead-"+tt.name)
			ctx, cancel := tt.ctx()
			defer cancel()
			reason := "dead caller " + tt.name

			if err := ready.FailStop(ctx, reason); err != nil {
				t.Fatalf("Ready.FailStop with %s context error = %v", tt.name, err)
			}
			snapshot := anchorSnapshot(t, anchorStore)
			if snapshot.Phase != "fail_stopped" || snapshot.Reason != reason {
				t.Fatalf("anchor snapshot = %+v, want durable fail-stop reason %q", snapshot, reason)
			}
			if _, err := ready.Accept(context.Background(), acceptRequest(t, "fail-stop-after-"+tt.name)); !errors.Is(err, ErrFailStopped) {
				t.Fatalf("Accept after %s fail-stop error = %v, want ErrFailStopped", tt.name, err)
			}
		})
	}
}

func newReady(t *testing.T, repo repository.Repository, name string) *Ready {
	t.Helper()
	session := newRecoverySession(t, repo, name)
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func newRecoverySession(t *testing.T, repo repository.Repository, name string) *RecoverySession {
	t.Helper()
	issuer, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := NewBootstrapper(repo, WithQuiescenceVerifier(verifier))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatal(err)
	}
	testAttestationIssuers.Store(boot, issuer)
	session, err := bootstrapper.Begin(context.Background(), boot)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func acceptRequest(t *testing.T, name string) AcceptRequest {
	t.Helper()
	key, err := model.NewRequestKey("workspace-"+name, "request-"+name)
	if err != nil {
		t.Fatal(err)
	}
	return AcceptRequest{
		RequestKey:   key,
		TaskIdentity: model.NewSHA256TaskIdentity([]byte("task-" + name)),
	}
}

func assertAcceptedImage(t *testing.T, repo repository.Repository, accepted AcceptResult) {
	t.Helper()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		request := tx.LookupRequest(accepted.Binding.RequestKey)
		if request.Binding.State != repository.RecordValid {
			t.Fatalf("binding state = %s, want valid", request.Binding.State)
		}
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("safety state = %s, want valid", image.Safety.State)
		}
		if image.Projection.State != repository.RecordValid {
			t.Fatalf("projection state = %s, want valid", image.Projection.State)
		}
		if image.Safety.Value.Revision != accepted.Projection.Revision {
			t.Fatalf("safety/projection revisions = %d/%d", image.Safety.Value.Revision, accepted.Projection.Revision)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertNoAcceptedJobs(t *testing.T, repo repository.Repository) {
	t.Helper()
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			if image.Safety.State == repository.RecordValid {
				t.Fatalf("unexpected accepted safety record after pre-commit failure: %+v", image.Safety.Value)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertAnchorFailStopped(t *testing.T, anchorStore *AnchorStore) {
	t.Helper()
	snapshot := anchorSnapshot(t, anchorStore)
	if snapshot.Phase != "fail_stopped" {
		t.Fatalf("anchor phase = %q, want fail_stopped", snapshot.Phase)
	}
}

func anchorSnapshot(t *testing.T, anchorStore *AnchorStore) AnchorSnapshot {
	t.Helper()
	var snapshot AnchorSnapshot
	if err := json.Unmarshal(anchorStore.SnapshotBytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func groupRef(ref model.AttemptRef, ordinal model.LaunchOrdinal) model.GroupRef {
	pgid := 20 + int(ordinal)
	return model.GroupRef{
		Version:   1,
		CustodyID: model.CustodyID("custody-" + ref.JobID.String() + "-" + ordinal.String()),
		Launch: model.LaunchKey{
			Attempt: ref,
			Ordinal: ordinal,
		},
		HostBootID:        "host-boot-" + ref.JobID.String(),
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader: model.ProcessIdentity{
			PID:               pgid,
			HighResStartToken: "leader-start-" + ordinal.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               30 + int(ordinal),
			HighResStartToken: "monitor-start-" + ordinal.String(),
		},
		RetainedID: "retained-" + ref.JobID.String() + "-" + ordinal.String(),
	}
}

func verifiedQuiescence(t *testing.T, boot model.BootRef, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef, method model.QuiescenceMethod) custodian.VerifiedQuiescence {
	t.Helper()
	value, ok := testAttestationIssuers.Load(boot)
	if !ok {
		t.Fatalf("missing test attestation issuer for boot %s", boot.BootID)
	}
	issuer := value.(custodian.AttestationIssuer)
	verified, err := issuer.AttestQuiescence(custodian.PhysicalQuiescence{
		Group:  group,
		Method: method,
	})
	if err != nil {
		t.Fatal(err)
	}
	return verified
}

func evidence(kind string) model.Evidence {
	return model.Evidence{Kind: kind, Detail: kind + "-evidence"}
}
