package authority

import (
	"context"
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
