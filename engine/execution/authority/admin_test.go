package authority

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestResetEmptyRootReinitializesWholeDomainAndAnchor(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initial, err := ResetEmptyAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	repo, session := beginBboltAdmissionRoot(t, root, "reset-activated-empty")
	activated, _, err := session.ActivateRoot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !activated.Activated {
		t.Fatal("test root was not activated before reset")
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	reset, err := ResetEmptyAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if reset.DomainUUID == "" || reset.DomainUUID == initial.DomainUUID {
		t.Fatalf("reset domain UUID = %q, initial %q; want fresh UUID", reset.DomainUUID, initial.DomainUUID)
	}
	if reset.ActivationMetadata.Activated || reset.ActivationMetadata.ActivatedAtGen != 0 {
		t.Fatalf("reset activation metadata = %+v, want unactivated fresh root", reset.ActivationMetadata)
	}
	anchor, err := LoadFileAnchorSnapshot(filepath.Join(root, AdmissionAnchorFile))
	if err != nil {
		t.Fatal(err)
	}
	if !anchor.Initialized || anchor.DBUUID != reset.DomainUUID || anchor.Generation != reset.Generation {
		t.Fatalf("reset anchor = %+v, inspection = %+v", anchor, reset)
	}
}

func TestResetEmptyRootRefusesBusyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := bboltrepo.NewRepository(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	oldTimeout := adminOpenTimeout
	adminOpenTimeout = 20 * time.Millisecond
	defer func() { adminOpenTimeout = oldTimeout }()

	if _, err := ResetEmptyAdmissionRoot(ctx, root); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("reset busy root error = %v, want ErrRootBusy", err)
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
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	sealedReport, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	sealed := sealedReport.OldInspection
	if !sealed.Sealed || sealed.ActivationMetadata != metadata {
		t.Fatalf("sealed inspection = %+v", sealed)
	}
	if !sealedReport.OldRootSealed || sealedReport.OldRoot != root || sealedReport.NewRoot != newRoot || sealedReport.NewDomainUUID == "" {
		t.Fatalf("sealed report = %+v", sealedReport)
	}
	if sealedReport.NewInspection.DomainUUID != sealedReport.NewDomainUUID || sealedReport.NewDomainUUID == inspection.DomainUUID {
		t.Fatalf("new domain identity = report:%+v old:%s", sealedReport, inspection.DomainUUID)
	}
	if sealedReport.NewInspection.ActivationMetadata.Activated || sealedReport.NewInspection.ActivationMetadata.ContractVersion != metadata.ContractVersion {
		t.Fatalf("new root activation metadata = %+v, want unactivated contract version %d", sealedReport.NewInspection.ActivationMetadata, metadata.ContractVersion)
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

func TestSealRefusesNonemptyDestination(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-destination")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(newRoot, "existing"), []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot}); !errors.Is(err, ErrNewStateRootNotEmpty) {
		t.Fatalf("seal nonempty destination error = %v, want ErrNewStateRootNotEmpty", err)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after destination refusal")
	}
}

func TestSealCrashAfterNewInitLeavesOldServiceableAndRetryRequiresDelete(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-crash-after-new-init")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	crash := errors.New("crash after new-root init")
	setSealAdmissionRootAfterNewInitForTest(t, func() error { return crash })

	report, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if !errors.Is(err, crash) {
		t.Fatalf("seal crash error = %v, want injected crash", err)
	}
	if report.OldRootSealed || report.NewDomainUUID == "" {
		t.Fatalf("crash report = %+v, want unsealed old and initialized new", report)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed before injected crash")
	}
	serviceSession, serviceRepo, err := beginBboltAdmissionRootAllowError(ctx, root, "seal-crash-old-serviceable")
	if err != nil {
		t.Fatalf("old root begin after injected crash: %v", err)
	}
	if _, err := serviceSession.SealReady(ctx); err != nil {
		t.Fatalf("old root ready after injected crash: %v", err)
	}
	if err := serviceRepo.Close(); err != nil {
		t.Fatal(err)
	}

	setSealAdmissionRootAfterNewInitForTest(t, nil)
	_, retryErr := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	var pristineErr PristineNewStateRootError
	if !errors.As(retryErr, &pristineErr) || !strings.Contains(retryErr.Error(), newRoot) {
		t.Fatalf("retry error = %v, want PristineNewStateRootError naming %s", retryErr, newRoot)
	}
	if err := os.RemoveAll(newRoot); err != nil {
		t.Fatal(err)
	}
	retryReport, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("retry after deleting pristine destination: %v", err)
	}
	if !retryReport.OldRootSealed || retryReport.NewDomainUUID == "" {
		t.Fatalf("retry report = %+v, want sealed old and fresh new", retryReport)
	}
}

func TestSealNewInitFailureCleansPartialDestinationAndLeavesOldServiceable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-new-init-fail")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	initErr := errors.New("new-root init failed")
	setInitializeAdmissionRootAfterOpenForTest(t, func() error { return initErr })

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if !errors.Is(err, initErr) {
		t.Fatalf("seal init error = %v, want injected init error", err)
	}
	if _, statErr := os.Stat(newRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("partial destination stat error = %v, want not exist", statErr)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after new-root init failure")
	}
	serviceSession, serviceRepo, err := beginBboltAdmissionRootAllowError(ctx, root, "seal-new-init-fail-serviceable")
	if err != nil {
		t.Fatalf("old root begin after new-root init failure: %v", err)
	}
	if _, err := serviceSession.SealReady(ctx); err != nil {
		t.Fatalf("old root ready after new-root init failure: %v", err)
	}
	if err := serviceRepo.Close(); err != nil {
		t.Fatal(err)
	}

	setInitializeAdmissionRootAfterOpenForTest(t, nil)
	retryReport, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("retry after cleaned init failure: %v", err)
	}
	if !retryReport.OldRootSealed || retryReport.NewDomainUUID == "" {
		t.Fatalf("retry report = %+v, want sealed old and fresh new", retryReport)
	}
}

func TestSealAlreadySealedRootReusesExistingFreshDestination(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-idempotent")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	first, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	second, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("sealed idempotent replay error = %v", err)
	}
	if !second.OldRootSealed || second.NewDomainUUID != first.NewDomainUUID {
		t.Fatalf("idempotent replay = %+v, want same new domain %s", second, first.NewDomainUUID)
	}
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
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: filepath.Join(t.TempDir(), "new-domain")}); !errors.Is(err, ErrRootHasRecoveryObligations) {
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

func setInitializeAdmissionRootAfterOpenForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := initializeAdmissionRootAfterOpenForTest
	initializeAdmissionRootAfterOpenForTest = hook
	t.Cleanup(func() {
		initializeAdmissionRootAfterOpenForTest = previous
	})
}

func setSealAdmissionRootAfterNewInitForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := sealAdmissionRootAfterNewInitForTest
	sealAdmissionRootAfterNewInitForTest = hook
	t.Cleanup(func() {
		sealAdmissionRootAfterNewInitForTest = previous
	})
}
