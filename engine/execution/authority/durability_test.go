package authority

import (
	"context"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestClassifyDurableMutationOutcomeAndGrantSafety(t *testing.T) {
	ctx := context.Background()
	boot := model.BootRef{BootID: model.BootID("boot-1"), OwnerID: model.OwnerID("owner-1")}

	tests := []struct {
		name       string
		run        func(t *testing.T) (dbCommitted bool, anchorAdvanced bool)
		want       DurabilityOutcome
		wantAction GrantDurabilityAction
	}{
		{
			name: "db commit failed before commit",
			run: func(t *testing.T) (bool, bool) {
				return false, false
			},
			want:       DefinitelyNotCommitted,
			wantAction: Proceed,
		},
		{
			name: "db committed then stopped before anchor begin",
			run: func(t *testing.T) (bool, bool) {
				return true, false
			},
			want:       CommitOutcomeUnknown,
			wantAction: ContainFailStop,
		},
		{
			name: "db committed then anchor begin failed",
			run: func(t *testing.T) (bool, bool) {
				store := NewAnchorStore()
				anchor := store.Adapter("db-1", 1)
				store.FailNextForTest(AnchorBegin, nil)
				if _, err := anchor.Begin(ctx, boot, 0); err == nil {
					t.Fatalf("Begin() error = nil, want injected failure")
				}
				return true, false
			},
			want:       CommitOutcomeUnknown,
			wantAction: ContainFailStop,
		},
		{
			name: "db committed then anchor advance failed before state write",
			run: func(t *testing.T) (bool, bool) {
				store, anchor := initializedDurabilityAnchor(t, ctx, boot)
				store.FailNextForTest(AnchorAdvance, nil)
				if err := anchor.Advance(ctx, boot, 1); err == nil {
					t.Fatalf("Advance() error = nil, want injected failure")
				}
				return true, anchorAdvancedToGeneration(t, store, 1)
			},
			want:       CommitOutcomeUnknown,
			wantAction: ContainFailStop,
		},
		{
			name: "db committed then anchor complete failed after state write",
			run: func(t *testing.T) (bool, bool) {
				store, anchor := initializedDurabilityAnchor(t, ctx, boot)
				store.FailNextForTest(AnchorComplete, nil)
				if err := anchor.Advance(ctx, boot, 1); err == nil {
					t.Fatalf("Advance() error = nil, want injected post-complete failure")
				}
				return true, anchorAdvancedToGeneration(t, store, 1)
			},
			want:       CommittedAndAnchored,
			wantAction: Proceed,
		},
		{
			name: "db committed then anchor advanced",
			run: func(t *testing.T) (bool, bool) {
				store, anchor := initializedDurabilityAnchor(t, ctx, boot)
				if err := anchor.Advance(ctx, boot, 1); err != nil {
					t.Fatalf("Advance() error = %v", err)
				}
				return true, anchorAdvancedToGeneration(t, store, 1)
			},
			want:       CommittedAndAnchored,
			wantAction: Proceed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbCommitted, anchorAdvanced := tt.run(t)
			got := ClassifyDurableMutationOutcome(dbCommitted, anchorAdvanced)
			if got != tt.want {
				t.Fatalf("ClassifyDurableMutationOutcome() = %v, want %v", got, tt.want)
			}
			if gotAction := SafeActionForGrantDurability(got); gotAction != tt.wantAction {
				t.Fatalf("SafeActionForGrantDurability() = %v, want %v", gotAction, tt.wantAction)
			}
		})
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

func anchorAdvancedToGeneration(t *testing.T, store *AnchorStore, generation uint64) bool {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.Generation == generation
}
