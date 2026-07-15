package authority

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestReplayConflictLeavesRepositoryAllocationAndAnchorUnchanged(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "accept-conflict")
	request := acceptRequest(t, "accept-conflict")

	accepted, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	beforeRepo := repo.SnapshotBytes()
	beforeAnchor := anchorStore.SnapshotBytes()
	beforeGeneration := ready.Generation()

	conflict := request
	conflict.TaskIdentity = model.NewSHA256TaskIdentity([]byte("different task"))
	if _, err := ready.Accept(ctx, conflict); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("conflicting replay error = %v, want ErrReplayConflict", err)
	}
	assertBytesEqual(t, "repository snapshot", beforeRepo, repo.SnapshotBytes())
	assertBytesEqual(t, "anchor snapshot", beforeAnchor, anchorStore.SnapshotBytes())
	if ready.Generation() != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", ready.Generation(), beforeGeneration)
	}

	next, err := ready.Accept(ctx, acceptRequest(t, "accept-conflict-next"))
	if err != nil {
		t.Fatal(err)
	}
	if next.Record.JobID != "job-00000000000000000002" {
		t.Fatalf("next job id = %s, want job-00000000000000000002 after rolled-back allocation", next.Record.JobID)
	}
	if accepted.Record.JobID != "job-00000000000000000001" {
		t.Fatalf("first job id = %s, want job-00000000000000000001", accepted.Record.JobID)
	}
}

func TestRejectedFinalizationLeavesSafetyAndProjectionUnchanged(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	ready := newReady(t, repo, "finalize-rollback")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "finalize-rollback"))
	if err != nil {
		t.Fatal(err)
	}
	group := groupRef(accepted.Record.Attempt.Ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}

	beforeRepo := repo.SnapshotBytes()
	beforeImage, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	beforeGeneration := ready.Generation()

	_, err = ready.Finalize(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	})
	if !errors.Is(err, model.ErrCommandPrecondition) {
		t.Fatalf("rejected finalize error = %v, want ErrCommandPrecondition", err)
	}
	assertBytesEqual(t, "repository snapshot", beforeRepo, repo.SnapshotBytes())
	if ready.Generation() != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", ready.Generation(), beforeGeneration)
	}

	afterImage, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterImage.Safety, beforeImage.Safety) {
		t.Fatalf("safety changed after rejected finalization\nbefore: %#v\nafter:  %#v", beforeImage.Safety, afterImage.Safety)
	}
	if !reflect.DeepEqual(afterImage.Projection, beforeImage.Projection) {
		t.Fatalf("projection changed after rejected finalization\nbefore: %#v\nafter:  %#v", beforeImage.Projection, afterImage.Projection)
	}
}

func TestDuplicateReceiptsAreNoopWithoutRevisionOrGenerationAdvance(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "duplicate-receipts")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "duplicate-receipts"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)

	bound, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group)
	if err != nil {
		t.Fatal(err)
	}
	duplicateBind, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group)
	if err != nil {
		t.Fatal(err)
	}
	assertNoopApply(t, "duplicate supervisor binding", bound, duplicateBind)

	verified := verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
	retired, err := ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verified)
	if err != nil {
		t.Fatal(err)
	}
	duplicateRetire, err := ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verified)
	if err != nil {
		t.Fatal(err)
	}
	assertNoopApply(t, "duplicate retirement receipt", retired, duplicateRetire)
}

func TestRecordQuiescenceResultMutationDoesNotRewriteStoredProof(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	ready := newReady(t, repo, "quiescence-alias")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "quiescence-alias"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	applied, err := ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent))
	if err != nil {
		t.Fatal(err)
	}
	before := repo.SnapshotBytes()

	mutateLaunchPhysicalFact(t, &applied.Record, model.LaunchOrdinalOne)

	assertBytesEqual(t, "repository snapshot after returned record mutation", before, repo.SnapshotBytes())
	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	launch, ok := image.Safety.Value.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launch.Group == nil || launch.Quiescence == nil {
		t.Fatalf("stored launch after returned record mutation = %#v", launch)
	}
	if !launch.Group.Equal(group) {
		t.Fatalf("stored group = %#v, want %#v", *launch.Group, group)
	}
	if !launch.Quiescence.Group.Equal(group) {
		t.Fatalf("stored quiescence group = %#v, want %#v", launch.Quiescence.Group, group)
	}
}

func TestConflictingDuplicateReceiptsFailWithoutMutation(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	ready := newReady(t, repo, "conflicting-receipts")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "conflicting-receipts"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	before := repo.SnapshotBytes()
	beforeGeneration := ready.Generation()

	conflicting := group
	conflicting.CustodyID = "custody-conflicting"
	conflicting.PGID++
	conflicting.Leader.PID++
	conflicting.Leader.HighResStartToken = "different-group"
	_, err = ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, conflicting)
	if !errors.Is(err, model.ErrConflictingDuplicate) {
		t.Fatalf("conflicting duplicate error = %v, want ErrConflictingDuplicate", err)
	}
	assertBytesEqual(t, "repository snapshot", before, repo.SnapshotBytes())
	if ready.Generation() != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", ready.Generation(), beforeGeneration)
	}
}

func TestReplayTombstoneOrderingUsesAuthorityRecords(t *testing.T) {
	ctx := context.Background()
	ready := newReady(t, memory.NewRepository(), "replay-tombstone")
	request := acceptRequest(t, "replay-tombstone")
	accepted, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.Record.JobID != accepted.Record.JobID {
		t.Fatalf("replay = %#v, want original job %s", replayed, accepted.Record.JobID)
	}

	conflict := request
	conflict.TaskIdentity = model.NewSHA256TaskIdentity([]byte("different replay task"))
	if _, err := ready.Accept(ctx, conflict); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("live conflicting replay error = %v, want ErrReplayConflict", err)
	}

	terminalizeReady(t, ctx, ready, accepted.Record.Attempt.Ref, accepted.Record.JobID)
	tombstone, err := ready.Expire(ctx, request.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.JobID != accepted.Record.JobID {
		t.Fatalf("tombstone job = %s, want %s", tombstone.JobID, accepted.Record.JobID)
	}
	lookup, err := ready.LookupReplay(ctx, request.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.State != ReplayExpired || lookup.Tombstone.JobID != accepted.Record.JobID {
		t.Fatalf("lookup = %#v, want expired original %s", lookup, accepted.Record.JobID)
	}
	if _, err := ready.Accept(ctx, request); !errors.Is(err, ErrRequestExpired) {
		t.Fatalf("expired replay error = %v, want ErrRequestExpired", err)
	}
}

func TestMissingSafetyIsRecordMissingAndFatalBeforeReady(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, "missing-safety-old")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "missing-safety"))
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectMissingSafetyForTest(accepted.Record.JobID)

	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		image := tx.LoadJob(accepted.Record.JobID)
		if image.Safety.State != repository.RecordMissing {
			t.Fatalf("safety state = %s, want RecordMissing", image.Safety.State)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	restartRepo, restartAnchor := restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
	_, err = beginRecoveryWithAnchorStore(t, restartRepo, restartAnchor, "missing-safety-new")
	if !errors.Is(err, repository.ErrInvalidRecord) {
		t.Fatalf("Begin error = %v, want ErrInvalidRecord before Ready", err)
	}
}

func assertNoopApply(t *testing.T, label string, first, duplicate ApplyResult) {
	t.Helper()
	if duplicate.Changed {
		t.Fatalf("%s changed record", label)
	}
	if duplicate.Record.Revision != first.Record.Revision {
		t.Fatalf("%s revision = %d, want unchanged %d", label, duplicate.Record.Revision, first.Record.Revision)
	}
	if duplicate.Commit.Generation != first.Commit.Generation {
		t.Fatalf("%s generation = %d, want unchanged %d", label, duplicate.Commit.Generation, first.Commit.Generation)
	}
	if !reflect.DeepEqual(duplicate.Record, first.Record) {
		t.Fatalf("%s record changed\nbefore: %#v\nafter:  %#v", label, first.Record, duplicate.Record)
	}
	if !reflect.DeepEqual(duplicate.Projection, first.Projection) {
		t.Fatalf("%s projection changed\nbefore: %#v\nafter:  %#v", label, first.Projection, duplicate.Projection)
	}
}

func assertBytesEqual(t *testing.T, label string, before, after []byte) {
	t.Helper()
	if !bytes.Equal(before, after) {
		t.Fatalf("%s changed\nbefore: %s\nafter:  %s", label, before, after)
	}
}

func mutateLaunchPhysicalFact(t *testing.T, record *model.SafetyRecord, ordinal model.LaunchOrdinal) {
	t.Helper()
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil || launch.Quiescence == nil {
		t.Fatalf("launch %s is missing group or quiescence", ordinal)
	}
	launch.Group.CustodyID = model.CustodyID("custody-forged-" + record.JobID.String())
	launch.Group.PGID += 1000
	launch.Group.Leader.PID += 1000
	launch.Group.Leader.HighResStartToken = "forged-leader"
	launch.Group.Monitor.PID += 1000
	launch.Group.Monitor.HighResStartToken = "forged-monitor"
	launch.Group.RetainedID = "forged-retained-" + record.JobID.String()
	launch.Quiescence.Group = *launch.Group
}
