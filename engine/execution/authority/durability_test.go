package authority

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestClassifyDurableMutationOutcomeAndGrantSafety(t *testing.T) {
	tests := []struct {
		name           string
		dbCommit       DBCommitOutcome
		anchorAdvanced bool
		want           DurabilityOutcome
		wantAction     GrantDurabilityAction
	}{
		{
			name:           "db definitely not committed",
			dbCommit:       DBDefinitelyNotCommitted,
			anchorAdvanced: false,
			want:           DefinitelyNotCommitted,
			wantAction:     Proceed,
		},
		{
			name:           "db committed but anchor not advanced",
			dbCommit:       DBCommitted,
			anchorAdvanced: false,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
		{
			name:           "db committed and anchor advanced",
			dbCommit:       DBCommitted,
			anchorAdvanced: true,
			want:           CommittedAndAnchored,
			wantAction:     Proceed,
		},
		{
			name:           "db commit unknown before anchor",
			dbCommit:       DBCommitUnknown,
			anchorAdvanced: false,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
		{
			name:           "db commit unknown after anchor",
			dbCommit:       DBCommitUnknown,
			anchorAdvanced: true,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
		{
			name:           "not committed but anchor advanced is contradictory",
			dbCommit:       DBDefinitelyNotCommitted,
			anchorAdvanced: true,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
		{
			name:           "zero db commit outcome fails closed",
			dbCommit:       DBCommitOutcome(0),
			anchorAdvanced: false,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
		{
			name:           "invalid db commit outcome fails closed",
			dbCommit:       DBCommitOutcome(99),
			anchorAdvanced: true,
			want:           CommitOutcomeUnknown,
			wantAction:     ContainFailStop,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyDurableMutationOutcome(tt.dbCommit, tt.anchorAdvanced)
			if got != tt.want {
				t.Fatalf("ClassifyDurableMutationOutcome() = %v, want %v", got, tt.want)
			}
			if gotAction := SafeActionForGrantDurability(got); gotAction != tt.wantAction {
				t.Fatalf("SafeActionForGrantDurability() = %v, want %v", gotAction, tt.wantAction)
			}
		})
	}
}

func TestSafeActionForGrantDurabilityFailsClosedOnZeroAndInvalid(t *testing.T) {
	tests := []struct {
		name    string
		outcome DurabilityOutcome
	}{
		{name: "zero", outcome: DurabilityOutcome(0)},
		{name: "unknown", outcome: CommitOutcomeUnknown},
		{name: "invalid", outcome: DurabilityOutcome(99)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeActionForGrantDurability(tt.outcome); got != ContainFailStop {
				t.Fatalf("SafeActionForGrantDurability(%v) = %v, want %v", tt.outcome, got, ContainFailStop)
			}
		})
	}
}

func TestReadyApplySurfacesCommitOutcomeUnknownOnAnchorFailure(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		fail AnchorOperation
		kind string
	}{
		{name: "commit-grant-anchor-advance", fail: AnchorAdvance, kind: "commit_grant"},
		{name: "commit-grant-anchor-complete", fail: AnchorComplete, kind: "commit_grant"},
		{name: "record-quiescence-anchor-advance", fail: AnchorAdvance, kind: "record_quiescence"},
		{name: "record-quiescence-anchor-complete", fail: AnchorComplete, kind: "record_quiescence"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := memory.NewRepository()
			anchorStore := NewAnchorStore()
			ready := newReadyWithAnchorStore(t, repo, anchorStore, tt.name)
			accepted, err := ready.Accept(ctx, acceptRequest(t, tt.name))
			if err != nil {
				t.Fatal(err)
			}
			ref := accepted.Record.Attempt.Ref
			group := groupRef(ref, model.LaunchOrdinalOne)
			if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
				t.Fatal(err)
			}

			anchorStore.FailNextForTest(tt.fail, nil)
			var applied ApplyResult
			switch tt.kind {
			case "commit_grant":
				applied, err = ready.CommitGrant(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, model.PermitNonce("nonce-1"))
			case "record_quiescence":
				verified := verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
				applied, err = ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verified)
			default:
				t.Fatalf("unknown mutation kind %q", tt.kind)
			}
			if !errors.Is(err, ErrAnchorInvariant) {
				t.Fatalf("mutation error = %v, want ErrAnchorInvariant", err)
			}
			if applied.Durability != CommitOutcomeUnknown {
				t.Fatalf("durability = %v, want %v", applied.Durability, CommitOutcomeUnknown)
			}
			if SafeActionForGrantDurability(applied.Durability) != ContainFailStop {
				t.Fatalf("SafeActionForGrantDurability() = %v, want %v", SafeActionForGrantDurability(applied.Durability), ContainFailStop)
			}
			if !applied.Changed {
				t.Fatal("applied.Changed = false, want committed DB mutation")
			}
			if applied.Commit.Generation == 0 {
				t.Fatal("commit generation = 0, want committed DB generation")
			}
		})
	}
}

func TestClassifyDurabilityAcrossRepositoryAnchorBoundary(t *testing.T) {
	tests := []struct {
		name               string
		fail               AnchorOperation
		wantErr            bool
		wantDBCommit       DBCommitOutcome
		wantAnchorAdvanced bool
		wantOutcome        DurabilityOutcome
		wantAction         GrantDurabilityAction
	}{
		{
			name:               "success commits db and advances anchor",
			wantDBCommit:       DBCommitted,
			wantAnchorAdvanced: true,
			wantOutcome:        CommittedAndAnchored,
			wantAction:         Proceed,
		},
		{
			name:               "post db commit pre anchor advance failure is unknown",
			fail:               AnchorAdvance,
			wantErr:            true,
			wantDBCommit:       DBCommitted,
			wantAnchorAdvanced: false,
			wantOutcome:        CommitOutcomeUnknown,
			wantAction:         ContainFailStop,
		},
		{
			name:               "anchor begin failure happens before db commit",
			fail:               AnchorBegin,
			wantErr:            true,
			wantDBCommit:       DBDefinitelyNotCommitted,
			wantAnchorAdvanced: false,
			wantOutcome:        DefinitelyNotCommitted,
			wantAction:         Proceed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runRepositoryAnchorBoundary(t, tt.fail)
			if (got.err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", got.err, tt.wantErr)
			}
			if got.dbCommit != tt.wantDBCommit {
				t.Fatalf("db commit = %v, want %v", got.dbCommit, tt.wantDBCommit)
			}
			if got.anchorAdvanced != tt.wantAnchorAdvanced {
				t.Fatalf("anchor advanced = %v, want %v", got.anchorAdvanced, tt.wantAnchorAdvanced)
			}
			outcome := ClassifyDurableMutationOutcome(got.dbCommit, got.anchorAdvanced)
			if outcome != tt.wantOutcome {
				t.Fatalf("ClassifyDurableMutationOutcome() = %v, want %v", outcome, tt.wantOutcome)
			}
			if action := SafeActionForGrantDurability(outcome); action != tt.wantAction {
				t.Fatalf("SafeActionForGrantDurability() = %v, want %v", action, tt.wantAction)
			}
		})
	}
}

type repositoryAnchorBoundaryResult struct {
	dbCommit       DBCommitOutcome
	anchorAdvanced bool
	err            error
}

func runRepositoryAnchorBoundary(t *testing.T, fail AnchorOperation) repositoryAnchorBoundaryResult {
	t.Helper()
	ctx := context.Background()
	boot := model.BootRef{BootID: model.BootID("boot-boundary"), OwnerID: model.OwnerID("owner-boundary")}
	repo := memory.NewRepository()
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		t.Fatal(err)
	}
	store := NewAnchorStore()
	anchor := store.Adapter(dbUUID, schemaMajor)

	if fail == AnchorBegin {
		store.FailNextForTest(AnchorBegin, nil)
	}
	if _, err := anchor.Begin(ctx, boot, 0); err != nil {
		if fail != AnchorBegin {
			t.Fatalf("Begin() error = %v", err)
		}
		if generation := repositoryGeneration(t, repo); generation != 0 {
			t.Fatalf("repository generation = %d after failed Begin, want 0", generation)
		}
		return repositoryAnchorBoundaryResult{dbCommit: DBDefinitelyNotCommitted, err: err}
	}

	commit, err := repo.Update(ctx, func(tx repository.WriteTx) error {
		_, err := tx.AllocateJobID()
		return err
	})
	if err != nil {
		t.Fatalf("repository Update() error = %v", err)
	}
	if commit.Generation == 0 {
		t.Fatalf("repository Update() generation = 0, want real mutation")
	}

	if fail == AnchorAdvance {
		store.FailNextForTest(AnchorAdvance, nil)
	}
	if err := anchor.Advance(ctx, boot, commit.Generation); err != nil {
		if fail != AnchorAdvance {
			t.Fatalf("Advance() error = %v", err)
		}
		return repositoryAnchorBoundaryResult{
			dbCommit:       DBCommitted,
			anchorAdvanced: anchorAdvancedToGeneration(t, store, commit.Generation),
			err:            err,
		}
	}
	return repositoryAnchorBoundaryResult{
		dbCommit:       DBCommitted,
		anchorAdvanced: anchorAdvancedToGeneration(t, store, commit.Generation),
	}
}

func initializedDurabilityAnchor(t *testing.T, ctx context.Context, boot model.BootRef) (*AnchorStore, Anchor) {
	t.Helper()
	store := NewAnchorStore()
	anchor := store.Adapter("db-1", 1)
	if _, err := anchor.Begin(ctx, boot, 0); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	return store, anchor
}

func repositoryGeneration(t *testing.T, repo repository.Repository) uint64 {
	t.Helper()
	var generation uint64
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		meta := tx.Meta()
		if meta.State != repository.RecordValid {
			t.Fatalf("meta state = %s, want valid", meta.State)
		}
		generation = meta.Value.Generation
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return generation
}

func anchorAdvancedToGeneration(t *testing.T, store *AnchorStore, generation uint64) bool {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.Generation == generation
}
