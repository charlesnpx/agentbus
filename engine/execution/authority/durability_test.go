package authority

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
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

func TestReadyDurabilityOutcomesAcrossMutationFailpoints(t *testing.T) {
	ctx := context.Background()
	mutations := []durabilityMutationKind{
		durabilityMutationBindGroup,
		durabilityMutationCommitGrant,
		durabilityMutationRecordRelease,
		durabilityMutationFinalize,
		durabilityMutationRecordQuiescence,
	}
	failpoints := []struct {
		point       durabilityFaultPoint
		wantErr     bool
		wantOutcome DurabilityOutcome
		wantAction  GrantDurabilityAction
	}{
		{
			point:       durabilityFaultPreDBCommit,
			wantErr:     true,
			wantOutcome: DefinitelyNotCommitted,
			wantAction:  Proceed,
		},
		{
			point:       durabilityFaultPostDBCommit,
			wantErr:     true,
			wantOutcome: CommitOutcomeUnknown,
			wantAction:  ContainFailStop,
		},
		{
			point:       durabilityFaultAnchorBegin,
			wantErr:     true,
			wantOutcome: DefinitelyNotCommitted,
			wantAction:  Proceed,
		},
		{
			point:       durabilityFaultAnchorAdvance,
			wantErr:     true,
			wantOutcome: CommitOutcomeUnknown,
			wantAction:  ContainFailStop,
		},
		{
			point:       durabilityFaultAnchorComplete,
			wantErr:     true,
			wantOutcome: CommitOutcomeUnknown,
			wantAction:  ContainFailStop,
		},
		{
			point:       durabilityFaultBeforeReturn,
			wantOutcome: CommittedAndAnchored,
			wantAction:  Proceed,
		},
	}

	for _, mutation := range mutations {
		for _, tt := range failpoints {
			t.Run(string(mutation)+"/"+string(tt.point), func(t *testing.T) {
				ready, repo, anchorStore := newBboltDurabilityReady(t, string(mutation)+"-"+string(tt.point))
				run := prepareDurabilityMutation(t, ctx, ready, mutation, string(tt.point))
				beforeGeneration := repositoryGeneration(t, repo)
				injectDurabilityFault(t, ready, repo, anchorStore, tt.point)

				applied, err := run()
				if (err != nil) != tt.wantErr {
					t.Fatalf("mutation error = %v, wantErr %v", err, tt.wantErr)
				}
				if applied.Durability != tt.wantOutcome {
					t.Fatalf("durability = %v, want %v", applied.Durability, tt.wantOutcome)
				}
				if action := SafeActionForGrantDurability(applied.Durability); action != tt.wantAction {
					t.Fatalf("SafeActionForGrantDurability() = %v, want %v", action, tt.wantAction)
				}
				afterGeneration := repositoryGeneration(t, repo)
				switch tt.point {
				case durabilityFaultPostDBCommit, durabilityFaultAnchorAdvance, durabilityFaultAnchorComplete, durabilityFaultBeforeReturn:
					if afterGeneration <= beforeGeneration {
						t.Fatalf("repository generation = %d, want > %d", afterGeneration, beforeGeneration)
					}
				case durabilityFaultPreDBCommit, durabilityFaultAnchorBegin:
					if afterGeneration != beforeGeneration {
						t.Fatalf("repository generation = %d, want unchanged %d", afterGeneration, beforeGeneration)
					}
				}
				if tt.point == durabilityFaultPostDBCommit && !errors.Is(err, repository.ErrAmbiguousCommit) {
					t.Fatalf("post-DB-commit error = %v, want ErrAmbiguousCommit", err)
				}
			})
		}
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

type durabilityMutationKind string

const (
	durabilityMutationBindGroup        durabilityMutationKind = "bind_group"
	durabilityMutationCommitGrant      durabilityMutationKind = "commit_grant"
	durabilityMutationRecordRelease    durabilityMutationKind = "record_release"
	durabilityMutationFinalize         durabilityMutationKind = "finalize"
	durabilityMutationRecordQuiescence durabilityMutationKind = "record_quiescence"
)

type durabilityFaultPoint string

const (
	durabilityFaultPreDBCommit    durabilityFaultPoint = "pre-db-commit"
	durabilityFaultPostDBCommit   durabilityFaultPoint = "post-db-commit"
	durabilityFaultAnchorBegin    durabilityFaultPoint = "anchor-begin"
	durabilityFaultAnchorAdvance  durabilityFaultPoint = "anchor-advance"
	durabilityFaultAnchorComplete durabilityFaultPoint = "anchor-complete"
	durabilityFaultBeforeReturn   durabilityFaultPoint = "before-return"
)

var errInjectedDurabilityFault = errors.New("injected durability fault")

type durabilityFaultingRepository struct {
	inner repository.Repository
	fault durabilityFaultPoint
}

func (r *durabilityFaultingRepository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	return r.inner.View(ctx, fn)
}

func (r *durabilityFaultingRepository) Update(ctx context.Context, fn func(repository.WriteTx) error) (repository.Commit, error) {
	switch r.fault {
	case durabilityFaultPreDBCommit:
		return repository.Commit{}, fmt.Errorf("%w: pre-db-commit", errInjectedDurabilityFault)
	case durabilityFaultPostDBCommit:
		commit, err := r.inner.Update(ctx, fn)
		if err != nil {
			return commit, err
		}
		return commit, fmt.Errorf("%w: injected bbolt commit-phase failure", repository.ErrAmbiguousCommit)
	default:
		return r.inner.Update(ctx, fn)
	}
}

func (r *durabilityFaultingRepository) AnchorIdentity() (string, uint16, error) {
	identified, ok := r.inner.(interface {
		AnchorIdentity() (string, uint16, error)
	})
	if !ok {
		return "", 0, fmt.Errorf("inner repository does not expose anchor identity")
	}
	return identified.AnchorIdentity()
}

type durabilityFaultingAnchor struct {
	Anchor
	failVerifyReady bool
}

func (a *durabilityFaultingAnchor) VerifyReady(boot model.BootRef, token string, generation uint64) error {
	if a.failVerifyReady {
		return fmt.Errorf("%w: injected anchor-begin failure", ErrAnchorInvariant)
	}
	if verifier, ok := a.Anchor.(anchorVerifier); ok {
		return verifier.VerifyReady(boot, token, generation)
	}
	return nil
}

func (a *durabilityFaultingAnchor) VerifyRecovery(boot model.BootRef, token string, generation uint64) error {
	if verifier, ok := a.Anchor.(anchorVerifier); ok {
		return verifier.VerifyRecovery(boot, token, generation)
	}
	return nil
}

func newBboltDurabilityReady(t *testing.T, name string) (*Ready, *durabilityFaultingRepository, *AnchorStore) {
	t.Helper()
	inner, err := bboltrepo.NewRepository(filepath.Join(t.TempDir(), "admission.bbolt"))
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	t.Cleanup(func() {
		if err := inner.Close(); err != nil {
			t.Fatalf("close bbolt repository: %v", err)
		}
	})
	repo := &durabilityFaultingRepository{inner: inner}
	anchorStore := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, repo, anchorStore, name)
	return ready, repo, anchorStore
}

func prepareDurabilityMutation(t *testing.T, ctx context.Context, ready *Ready, mutation durabilityMutationKind, suffix string) func() (ApplyResult, error) {
	t.Helper()
	accepted, err := ready.Accept(ctx, acceptRequest(t, "durability-"+string(mutation)+"-"+suffix))
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := groupRef(ref, model.LaunchOrdinalOne)
	switch mutation {
	case durabilityMutationBindGroup:
		return func() (ApplyResult, error) {
			return ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group)
		}
	case durabilityMutationCommitGrant:
		bindDurabilityGroup(t, ctx, ready, accepted, group)
		return func() (ApplyResult, error) {
			return ready.CommitGrant(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, model.PermitNonce("nonce-"+suffix))
		}
	case durabilityMutationRecordRelease:
		bindDurabilityGroup(t, ctx, ready, accepted, group)
		commitDurabilityGrant(t, ctx, ready, accepted, suffix)
		return func() (ApplyResult, error) {
			return ready.RecordRelease(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, model.ChildIdentity{
				PID:               group.Leader.PID,
				HighResStartToken: group.Leader.HighResStartToken,
			}, evidence("launch-"+suffix))
		}
	case durabilityMutationFinalize:
		bindDurabilityGroup(t, ctx, ready, accepted, group)
		if _, err := ready.RequestCancel(ctx, accepted.Record.JobID); err != nil {
			t.Fatal(err)
		}
		recordDurabilityQuiescence(t, ctx, ready, accepted, group)
		return func() (ApplyResult, error) {
			return ready.Finalize(ctx, accepted.Record.JobID, ref, model.TerminalIntent{
				Outcome: model.OutcomeCanceled,
				Cause:   model.CauseCanceledBeforeAuthorization,
			})
		}
	case durabilityMutationRecordQuiescence:
		bindDurabilityGroup(t, ctx, ready, accepted, group)
		verified := verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
		return func() (ApplyResult, error) {
			return ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verified)
		}
	default:
		t.Fatalf("unknown durability mutation %q", mutation)
		return nil
	}
}

func bindDurabilityGroup(t *testing.T, ctx context.Context, ready *Ready, accepted AcceptResult, group model.GroupRef) {
	t.Helper()
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
}

func commitDurabilityGrant(t *testing.T, ctx context.Context, ready *Ready, accepted AcceptResult, suffix string) {
	t.Helper()
	if _, err := ready.CommitGrant(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, model.PermitNonce("setup-nonce-"+suffix)); err != nil {
		t.Fatal(err)
	}
}

func recordDurabilityQuiescence(t *testing.T, ctx context.Context, ready *Ready, accepted AcceptResult, group model.GroupRef) {
	t.Helper()
	verified := verifiedQuiescence(t, ready.Boot(), accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
	if _, err := ready.RecordQuiescence(ctx, accepted.Record.JobID, model.LaunchOrdinalOne, verified); err != nil {
		t.Fatal(err)
	}
}

func injectDurabilityFault(t *testing.T, ready *Ready, repo *durabilityFaultingRepository, anchorStore *AnchorStore, point durabilityFaultPoint) {
	t.Helper()
	switch point {
	case durabilityFaultPreDBCommit, durabilityFaultPostDBCommit:
		repo.fault = point
	case durabilityFaultAnchorBegin:
		ready.core.anchor = &durabilityFaultingAnchor{Anchor: ready.core.anchor, failVerifyReady: true}
	case durabilityFaultAnchorAdvance:
		anchorStore.FailNextForTest(AnchorAdvance, nil)
	case durabilityFaultAnchorComplete:
		anchorStore.FailNextForTest(AnchorComplete, nil)
	case durabilityFaultBeforeReturn:
	default:
		t.Fatalf("unknown durability fault point %q", point)
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
