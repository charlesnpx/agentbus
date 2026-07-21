package authority

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
)

func TestAdmissionRootActivationIsOneWayAndAdminDoesNotClearNonemptyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "activation-one-way")
	metadata, _, err := session.ActivateRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !metadata.Activated || metadata.ContractVersion != CurrentAdmissionContractVersion || metadata.ActivatedAtGen == 0 {
		t.Fatalf("activation metadata = %+v", metadata)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ready.Accept(ctx, acceptRequest(t, "activation-one-way")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := InspectAdmissionRoot(ctx, root); err != nil {
		t.Fatalf("inspect activated nonempty root: %v", err)
	}
	if _, err := ResetEmptyAdmissionRoot(ctx, root); !errors.Is(err, ErrRootNotEmpty) {
		t.Fatalf("reset-empty-root error = %v, want ErrRootNotEmpty", err)
	}
	after, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if after.ActivationMetadata != metadata {
		t.Fatalf("activation metadata after admin verbs = %+v, want %+v", after.ActivationMetadata, metadata)
	}
}

func TestResetEmptyRootRefusesEveryNonzeroCategory(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name         string
		populate     func(t *testing.T, ready *Ready)
		wantFragment string
	}{
		{
			name: "job",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				if _, err := ready.Accept(ctx, acceptRequest(t, "reset-job")); err != nil {
					t.Fatal(err)
				}
			},
			wantFragment: "jobs=1",
		},
		{
			name: "binding",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				if _, err := ready.Accept(ctx, acceptRequest(t, "reset-binding")); err != nil {
					t.Fatal(err)
				}
			},
			wantFragment: "bindings=1",
		},
		{
			name: "tombstone",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				accepted, err := ready.Accept(ctx, acceptRequest(t, "reset-tombstone"))
				if err != nil {
					t.Fatal(err)
				}
				terminalizeReady(t, ctx, ready, accepted.Record.Attempt.Ref, accepted.Record.JobID)
				if _, err := ready.Expire(ctx, accepted.Binding.RequestKey); err != nil {
					t.Fatal(err)
				}
			},
			wantFragment: "tombstones=1",
		},
		{
			name: "launch record",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				accepted, err := ready.Accept(ctx, acceptRequest(t, "reset-launch"))
				if err != nil {
					t.Fatal(err)
				}
				group := groupRef(accepted.Record.Attempt.Ref, model.LaunchOrdinalOne)
				if _, err := ready.BindGroup(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.LaunchOrdinalOne, group); err != nil {
					t.Fatal(err)
				}
			},
			wantFragment: "launch_records=1",
		},
		{
			name: "recovery obligation",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				if _, err := ready.Accept(ctx, acceptRequest(t, "reset-recovery")); err != nil {
					t.Fatal(err)
				}
			},
			wantFragment: "recovery_obligations=1",
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			repo, session := beginBboltAdmissionRoot(t, root, "reset-"+strings.ReplaceAll(tt.name, " ", "-"))
			if _, _, err := session.ActivateRoot(ctx); err != nil {
				t.Fatal(err)
			}
			ready, err := session.SealReady(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tt.populate(t, ready)
			if err := repo.Close(); err != nil {
				t.Fatal(err)
			}
			_, err = ResetEmptyAdmissionRoot(ctx, root)
			if !errors.Is(err, ErrRootNotEmpty) {
				t.Fatalf("reset-empty-root error = %v, want ErrRootNotEmpty", err)
			}
			if !strings.Contains(err.Error(), tt.wantFragment) {
				t.Fatalf("reset-empty-root error = %q, want %q", err.Error(), tt.wantFragment)
			}
		})
	}
}

func TestAdmissionInspectAndSeal(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal")
	metadata, _, err := session.ActivateRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := acceptRequest(t, "seal-replay-reset")
	accepted, err := ready.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	terminalizeReady(t, ctx, ready, accepted.Record.Attempt.Ref, accepted.Record.JobID)
	if _, err := ready.Expire(ctx, accepted.Binding.RequestKey); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.ActivationMetadata != metadata || inspection.DomainUUID == "" || inspection.Counts.Tombstones != 1 || inspection.Sealed {
		t.Fatalf("inspection = %+v", inspection)
	}
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{}); !errors.Is(err, ErrSealConfirmationRequired) {
		t.Fatalf("seal without flags error = %v, want ErrSealConfirmationRequired", err)
	}
	sealed, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true})
	if err != nil {
		t.Fatal(err)
	}
	if !sealed.Sealed || sealed.ActivationMetadata != metadata {
		t.Fatalf("sealed inspection = %+v", sealed)
	}
	if sealedSession, sealedRepo, err := beginBboltAdmissionRootAllowError(ctx, root, "sealed-serve"); !errors.Is(err, ErrRootSealed) {
		if sealedRepo != nil {
			_ = sealedRepo.Close()
		}
		if sealedSession != nil {
			t.Fatalf("sealed root session = %+v, want nil", sealedSession)
		}
		t.Fatalf("sealed root begin error = %v, want ErrRootSealed", err)
	}

	// Rotation intentionally resets cross-root replay history: the same request
	// key is accepted as new work in the new authority domain.
	newRoot := t.TempDir()
	newRepo, newSession := beginBboltAdmissionRoot(t, newRoot, "new-domain")
	newReady, err := newSession.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replayedInNewDomain, err := newReady.Accept(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if replayedInNewDomain.Replayed {
		t.Fatal("old-domain request key replayed in new domain; want accepted as new work")
	}
	_ = newRepo.Close()
}

func TestSealRefusesNonterminalObligations(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-nonterminal")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ready.Accept(ctx, acceptRequest(t, "seal-nonterminal")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true}); !errors.Is(err, ErrRootHasRecoveryObligations) {
		t.Fatalf("seal nonterminal error = %v, want ErrRootHasRecoveryObligations", err)
	}
}

func beginBboltAdmissionRoot(t *testing.T, root, name string) (*bboltrepo.Repository, *RecoverySession) {
	t.Helper()
	session, repo, err := beginBboltAdmissionRootAllowError(context.Background(), root, name)
	if err != nil {
		t.Fatal(err)
	}
	return repo, session
}

func beginBboltAdmissionRootAllowError(ctx context.Context, root, name string) (*RecoverySession, *bboltrepo.Repository, error) {
	repo, err := bboltrepo.NewRepository(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		return nil, nil, err
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		_ = repo.Close()
		return nil, nil, err
	}
	issuer, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := NewBootstrapper(repo, WithAnchor(NewFileAnchor(filepath.Join(root, AdmissionAnchorFile), dbUUID, schemaMajor)), WithQuiescenceVerifier(verifier))
	if err != nil {
		_ = repo.Close()
		return nil, nil, err
	}
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		_ = repo.Close()
		return nil, nil, err
	}
	testAttestationIssuers.Store(boot, issuer)
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		_ = repo.Close()
		return nil, repo, err
	}
	return session, repo, nil
}
