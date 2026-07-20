package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestRecoveryTokenRejectsStaleRevisionWrongBootAndReplay(t *testing.T) {
	ctx := context.Background()
	session, item := recoveryTokenWorkItem(t, "token-reject")

	stale := item.Token
	stale.BasedOnRevision++
	if _, err := session.AdvanceRecovery(ctx, stale); !errors.Is(err, ErrStaleCapability) {
		t.Fatalf("stale revision error = %v, want ErrStaleCapability", err)
	}

	wrongBoot := item.Token
	boot, err := model.NewBootRef("boot-token-reject-other", "owner-token-reject-other")
	if err != nil {
		t.Fatal(err)
	}
	wrongBoot.RecoveryBoot = boot
	if _, err := session.AdvanceRecovery(ctx, wrongBoot); !errors.Is(err, ErrStaleCapability) {
		t.Fatalf("wrong boot error = %v, want ErrStaleCapability", err)
	}

	if _, err := session.AdvanceRecovery(ctx, item.Token); err != nil {
		t.Fatalf("AdvanceRecovery valid token: %v", err)
	}
	if _, err := session.AdvanceRecovery(ctx, item.Token); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("replayed token error = %v, want ErrReplayConflict", err)
	}
}

func TestRecoveryTokenRecordsQuiescenceAndRederivesNextAction(t *testing.T) {
	ctx := context.Background()
	session, item := recoveryTokenWorkItem(t, "token-record")
	if len(item.Launches) != 1 {
		t.Fatalf("work launches = %d, want 1", len(item.Launches))
	}
	launch := item.Launches[0]
	verified := verifiedQuiescence(t, session.token.boot, launch.Group.Launch.Attempt, launch.Ordinal, launch.Group, model.QuiescenceAlreadyAbsent)

	if err := session.RecordQuiescence(ctx, item.Token, launch.Ordinal, verified); err != nil {
		t.Fatalf("RecordQuiescence token: %v", err)
	}
	if err := session.RecordQuiescence(ctx, item.Token, launch.Ordinal, verified); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("replayed quiescence token error = %v, want ErrReplayConflict", err)
	}

	next, err := session.AdvanceRecovery(ctx, item.Token)
	if err != nil {
		t.Fatalf("AdvanceRecovery after quiescence: %v", err)
	}
	if len(next.Launches) != 0 {
		t.Fatalf("finalize work launches = %d, want 0", len(next.Launches))
	}
	if next.Token.BasedOnRevision <= item.Token.BasedOnRevision {
		t.Fatalf("next token revision = %d, want > %d", next.Token.BasedOnRevision, item.Token.BasedOnRevision)
	}
	if _, err := session.AdvanceRecovery(ctx, item.Token); !errors.Is(err, ErrReplayConflict) {
		t.Fatalf("replayed advance token error = %v, want ErrReplayConflict", err)
	}
	if err := session.FinalizePlanned(ctx, next.Token); err != nil {
		t.Fatalf("FinalizePlanned: %v", err)
	}
	items, err := session.WorkItems(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("work items after finalize = %d, want 0", len(items))
	}
	if _, err := session.SealReady(ctx); err != nil {
		t.Fatalf("SealReady after tokenized recovery: %v", err)
	}
}

func recoveryTokenWorkItem(t *testing.T, name string) (*RecoverySession, RecoveryWorkItem) {
	t.Helper()
	ctx := context.Background()
	repo := memory.NewRepository()
	oldReady := newReady(t, repo, name+"-old")
	accepted, err := oldReady.Accept(ctx, acceptRequest(t, name+"-old"))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := oldReady.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}

	session := newRecoverySession(t, repo, name+"-new")
	items, err := session.WorkItems(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("work items = %d, want 1", len(items))
	}
	if items[0].JobID != accepted.Record.JobID {
		t.Fatalf("work item job = %s, want %s", items[0].JobID, accepted.Record.JobID)
	}
	if items[0].BasedOnRevision != accepted.Record.Revision+1 {
		t.Fatalf("work item revision = %d, want %d", items[0].BasedOnRevision, accepted.Record.Revision+1)
	}
	if items[0].Trigger != model.RecoveryStartupLoss {
		t.Fatalf("work item trigger = %v, want startup loss", items[0].Trigger)
	}
	if items[0].WorkspaceLayoutKey != accepted.Record.WorkspaceLayoutKey {
		t.Fatalf("work item layout key = %q, want %q", items[0].WorkspaceLayoutKey, accepted.Record.WorkspaceLayoutKey)
	}
	if err := items[0].Token.Validate(); err != nil {
		t.Fatalf("work item token invalid: %v", err)
	}
	if err := items[0].Validate(); err != nil {
		t.Fatalf("work item invalid: %v", err)
	}
	return session, items[0]
}
