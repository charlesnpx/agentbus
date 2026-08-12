package authority

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	bolt "go.etcd.io/bbolt"
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
		{
			name: "terminal orphaned unresolved",
			populate: func(t *testing.T, ready *Ready) {
				t.Helper()
				accepted, err := ready.Accept(ctx, acceptRequest(t, "reset-terminal-orphaned"))
				if err != nil {
					t.Fatal(err)
				}
				terminalizeOrphanedUnresolvedForTest(t, ctx, ready, accepted)
			},
			wantFragment: "jobs=1",
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

func TestCandidateV1RootCanResetEmptyOrSealToV2(t *testing.T) {
	ctx := context.Background()

	resetRoot := t.TempDir()
	initial, err := ResetEmptyAdmissionRoot(ctx, resetRoot)
	if err != nil {
		t.Fatal(err)
	}
	forgeBboltAdmissionContractVersionForTest(t, resetRoot, 1)
	forged, err := InspectAdmissionRoot(ctx, resetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if forged.ActivationMetadata.Activated || forged.ActivationMetadata.ContractVersion != 1 {
		t.Fatalf("forged candidate metadata = %+v, want unactivated v1", forged.ActivationMetadata)
	}
	reset, err := ResetEmptyAdmissionRoot(ctx, resetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if reset.DomainUUID == "" || reset.DomainUUID == initial.DomainUUID {
		t.Fatalf("reset domain UUID = %q, initial %q; want fresh UUID", reset.DomainUUID, initial.DomainUUID)
	}
	if reset.ActivationMetadata.Activated || reset.ActivationMetadata.ContractVersion != CurrentAdmissionContractVersion {
		t.Fatalf("reset candidate metadata = %+v, want fresh v%d candidate", reset.ActivationMetadata, CurrentAdmissionContractVersion)
	}

	sealRoot := t.TempDir()
	if _, err := ResetEmptyAdmissionRoot(ctx, sealRoot); err != nil {
		t.Fatal(err)
	}
	forgeBboltAdmissionContractVersionForTest(t, sealRoot, 1)
	successorRoot := filepath.Join(t.TempDir(), "successor")
	sealed, err := SealAdmissionRoot(ctx, sealRoot, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: successorRoot})
	if err != nil {
		t.Fatal(err)
	}
	if !sealed.OldRootSealed || sealed.OldInspection.ActivationMetadata.ContractVersion != 1 {
		t.Fatalf("sealed candidate v1 report = %+v, want sealed old v1 root", sealed)
	}
	if sealed.NewInspection.ActivationMetadata.Activated || sealed.NewInspection.ActivationMetadata.ContractVersion != CurrentAdmissionContractVersion {
		t.Fatalf("seal successor metadata = %+v, want fresh v%d candidate", sealed.NewInspection.ActivationMetadata, CurrentAdmissionContractVersion)
	}
}

func TestResetEmptyRootRepairsMissingAnchorOnlyAfterEmptyProof(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initial, err := ResetEmptyAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	anchorPath := filepath.Join(root, AdmissionAnchorFile)
	if err := os.Remove(anchorPath); err != nil {
		t.Fatal(err)
	}

	reset, err := ResetEmptyAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if reset.DomainUUID == "" || reset.DomainUUID == initial.DomainUUID {
		t.Fatalf("reset domain UUID = %q, initial %q; want fresh UUID", reset.DomainUUID, initial.DomainUUID)
	}
	anchor, err := LoadFileAnchorSnapshot(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	if !anchor.Initialized || anchor.DBUUID != reset.DomainUUID || anchor.Generation != reset.Generation {
		t.Fatalf("reset anchor = %+v, inspection = %+v", anchor, reset)
	}
}

func TestResetEmptyRootRefusesDanglingAnchorSymlinkWithoutRepository(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	anchorPath := filepath.Join(root, AdmissionAnchorFile)
	if err := os.Symlink(filepath.Join(root, "missing-anchor-target"), anchorPath); err != nil {
		t.Fatal(err)
	}

	_, err := ResetEmptyAdmissionRoot(ctx, root)
	if !errors.Is(err, ErrAnchorInvariant) {
		t.Fatalf("ResetEmptyAdmissionRoot error = %v, want ErrAnchorInvariant", err)
	}
	if _, statErr := os.Lstat(filepath.Join(root, AdmissionRepositoryFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("repository lstat = %v, want missing", statErr)
	}
	info, statErr := os.Lstat(anchorPath)
	if statErr != nil {
		t.Fatalf("anchor lstat = %v, want dangling symlink present", statErr)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("anchor mode = %v, want symlink", info.Mode())
	}
}

func TestResetEmptyRootRefusesBusyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := bboltrepo.Create(filepath.Join(root, AdmissionRepositoryFile))
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

func TestInspectAdmissionRootReportsBusyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := bboltrepo.Create(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	oldTimeout := adminOpenTimeout
	adminOpenTimeout = 20 * time.Millisecond
	defer func() { adminOpenTimeout = oldTimeout }()

	_, err = InspectAdmissionRoot(ctx, root)
	if !errors.Is(err, ErrRootBusy) {
		t.Fatalf("inspect busy root error = %v, want ErrRootBusy", err)
	}
	if !errors.Is(err, bolt.ErrTimeout) {
		t.Fatalf("inspect busy root error = %v, want bolt.ErrTimeout", err)
	}
	for _, want := range []string{"admission database is held", "agentbus status", "agentbus result", "stop the daemon", "offline authority inspection"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("inspect busy root error = %q, want guidance containing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "the daemon is running") {
		t.Fatalf("inspect busy root error = %q, must not identify an uncorroborated lock holder as the daemon", err)
	}
}

func TestInspectAdmissionRootReportsLivePIDEvidenceForBusyRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, err := bboltrepo.Create(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	if err := os.WriteFile(filepath.Join(root, admissionDaemonPIDFile), []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	oldTimeout := adminOpenTimeout
	adminOpenTimeout = 20 * time.Millisecond
	defer func() { adminOpenTimeout = oldTimeout }()

	_, err = InspectAdmissionRoot(ctx, root)
	if !errors.Is(err, ErrRootBusy) || !errors.Is(err, bolt.ErrTimeout) {
		t.Fatalf("inspect busy live-pid root error = %v, want ErrRootBusy and bolt.ErrTimeout", err)
	}
	for _, want := range []string{"agentbus.pid names a live process", "pid " + strconv.Itoa(os.Getpid()), "agentbus status", "agentbus result", "stop the daemon"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("inspect busy live-pid root error = %q, want guidance containing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "the daemon is running") {
		t.Fatalf("inspect busy live-pid root error = %q, must not identify an unverified lock holder as the daemon", err)
	}
}

func TestInspectAdmissionRootKeepsMissingRootAndAnchorDiagnostics(t *testing.T) {
	ctx := context.Background()

	t.Run("missing root", func(t *testing.T) {
		_, err := InspectAdmissionRoot(ctx, filepath.Join(t.TempDir(), "missing"))
		if !errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrRootBusy) {
			t.Fatalf("inspect missing root error = %v, want not-exist without ErrRootBusy", err)
		}
	})

	t.Run("absent anchor", func(t *testing.T) {
		root := t.TempDir()
		repo, _ := beginBboltAdmissionRoot(t, root, "inspect-absent-anchor")
		if err := repo.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(root, AdmissionAnchorFile)); err != nil {
			t.Fatal(err)
		}
		_, err := InspectAdmissionRoot(ctx, root)
		if !errors.Is(err, ErrAnchorInvariant) || errors.Is(err, ErrRootBusy) || !strings.Contains(err.Error(), "anchor is missing") {
			t.Fatalf("inspect absent anchor error = %v, want anchor invariant without ErrRootBusy", err)
		}
	})

	t.Run("malformed anchor", func(t *testing.T) {
		root := t.TempDir()
		repo, _ := beginBboltAdmissionRoot(t, root, "inspect-malformed-anchor")
		if err := repo.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, AdmissionAnchorFile), []byte("not json\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := InspectAdmissionRoot(ctx, root)
		if !errors.Is(err, ErrAnchorInvariant) || errors.Is(err, ErrRootBusy) || !strings.Contains(err.Error(), "anchor is corrupt") {
			t.Fatalf("inspect malformed anchor error = %v, want anchor invariant without ErrRootBusy", err)
		}
	})
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

func TestSealFreshDestinationRequiresAbsentDirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-empty-destination")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := t.TempDir()
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot}); !errors.Is(err, ErrNewStateRootNotEmpty) {
		t.Fatalf("seal existing empty destination error = %v, want ErrNewStateRootNotEmpty", err)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after existing empty destination refusal")
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

func TestSealDestinationInjectionCleansOnlyOwnedPaths(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-destination-injection")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	foreignPath := filepath.Join(newRoot, "foreign")
	setInitializeAdmissionRootAfterOpenForTest(t, func() error {
		return os.WriteFile(foreignPath, []byte("concurrent"), 0o600)
	})

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if !errors.Is(err, ErrNewStateRootNotEmpty) {
		t.Fatalf("seal injected destination error = %v, want ErrNewStateRootNotEmpty", err)
	}
	if raw, readErr := os.ReadFile(foreignPath); readErr != nil || string(raw) != "concurrent" {
		t.Fatalf("foreign file after cleanup = %q err=%v, want untouched", string(raw), readErr)
	}
	for _, name := range []string{AdmissionRepositoryFile, AdmissionAnchorFile} {
		path := filepath.Join(newRoot, name)
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("owned path %s stat error = %v, want removed", path, statErr)
		}
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after injected destination refusal")
	}
}

func TestSealRepositoryPathCollisionIsForeignAndSurvivesCleanup(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-repo-collision")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	repoPath := filepath.Join(newRoot, AdmissionRepositoryFile)
	setInitializeAdmissionRootBeforeRepositoryCreateForTest(t, func() error {
		return os.WriteFile(repoPath, []byte("foreign repo"), 0o600)
	})

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if !errors.Is(err, ErrNewStateRootNotEmpty) || !strings.Contains(err.Error(), "foreign destination path") || !strings.Contains(err.Error(), AdmissionRepositoryFile) {
		t.Fatalf("seal repository collision error = %v, want typed foreign repository path", err)
	}
	if raw, readErr := os.ReadFile(repoPath); readErr != nil || string(raw) != "foreign repo" {
		t.Fatalf("foreign repository file after cleanup = %q err=%v, want untouched", string(raw), readErr)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after repository collision")
	}
}

func TestSealAnchorPathCollisionIsForeignAndSurvivesCleanup(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-anchor-collision")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	anchorPath := filepath.Join(newRoot, AdmissionAnchorFile)
	setInitializeAdmissionRootBeforeAnchorCreateForTest(t, func() error {
		return os.WriteFile(anchorPath, []byte("foreign anchor"), 0o600)
	})

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if !errors.Is(err, ErrNewStateRootNotEmpty) || !strings.Contains(err.Error(), "foreign destination path") || !strings.Contains(err.Error(), AdmissionAnchorFile) {
		t.Fatalf("seal anchor collision error = %v, want typed foreign anchor path", err)
	}
	if raw, readErr := os.ReadFile(anchorPath); readErr != nil || string(raw) != "foreign anchor" {
		t.Fatalf("foreign anchor file after cleanup = %q err=%v, want untouched", string(raw), readErr)
	}
	if _, statErr := os.Stat(filepath.Join(newRoot, AdmissionRepositoryFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("owned repository path after anchor collision stat error = %v, want removed", statErr)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after anchor collision")
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

func TestSealSealedReplayRequiresStartableSuccessorAnchor(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-startable-successor")
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

	copyOnlyRoot := filepath.Join(t.TempDir(), "copy-only-domain")
	if err := os.Mkdir(copyOnlyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	copyOnlyRepoPath := filepath.Join(copyOnlyRoot, AdmissionRepositoryFile)
	if err := copyFile(copyOnlyRepoPath, filepath.Join(newRoot, AdmissionRepositoryFile)); err != nil {
		t.Fatal(err)
	}
	beforeCopyBytes, err := os.ReadFile(copyOnlyRepoPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: copyOnlyRoot})
	var mismatch SealedSuccessorMismatchError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrSealedSuccessorMismatch) || mismatch.ExpectedDomainUUID != first.NewDomainUUID {
		t.Fatalf("copy-only sealed replay error = %#v %v, want typed successor mismatch", mismatch, err)
	}
	if !strings.Contains(err.Error(), "anchor") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("copy-only sealed replay error = %v, want missing anchor message", err)
	}
	if _, statErr := os.Stat(filepath.Join(copyOnlyRoot, AdmissionAnchorFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("copy-only replay anchor stat error = %v, want not created", statErr)
	}
	afterCopyBytes, err := os.ReadFile(copyOnlyRepoPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterCopyBytes, beforeCopyBytes) {
		t.Fatal("copy-only replay mutated destination repository")
	}

	if err := os.Remove(filepath.Join(newRoot, AdmissionAnchorFile)); err != nil {
		t.Fatal(err)
	}
	_, err = SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	mismatch = SealedSuccessorMismatchError{}
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrSealedSuccessorMismatch) || mismatch.ExpectedDomainUUID != first.NewDomainUUID {
		t.Fatalf("deleted-anchor sealed replay error = %#v %v, want typed successor mismatch", mismatch, err)
	}
	if !strings.Contains(err.Error(), "anchor") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("deleted-anchor sealed replay error = %v, want missing anchor message", err)
	}
	if _, statErr := os.Stat(filepath.Join(newRoot, AdmissionAnchorFile)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("deleted-anchor replay anchor stat error = %v, want not repaired", statErr)
	}
}

func TestSealSealedReplayRequiresPersistedSuccessorIdentity(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-successor-identity")
	metadata, _, err := session.ActivateRoot(ctx)
	if err != nil {
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
	if first.OldInspection.SuccessorDomainUUID != first.NewDomainUUID || first.OldInspection.SuccessorStateRoot != newRoot {
		t.Fatalf("sealed old inspection successor = %+v, report UUID %s root %s", first.OldInspection, first.NewDomainUUID, newRoot)
	}

	replay, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("sealed replay to true successor error = %v", err)
	}
	if replay.NewDomainUUID != first.NewDomainUUID {
		t.Fatalf("sealed replay UUID = %s, want %s", replay.NewDomainUUID, first.NewDomainUUID)
	}

	absentRoot := filepath.Join(t.TempDir(), "absent-domain")
	_, err = SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: absentRoot})
	var mismatch SealedSuccessorMismatchError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrSealedSuccessorMismatch) || mismatch.ExpectedDomainUUID != first.NewDomainUUID || !strings.Contains(err.Error(), first.NewDomainUUID) {
		t.Fatalf("sealed replay absent destination error = %#v %v, want successor mismatch naming %s", mismatch, err, first.NewDomainUUID)
	}
	if _, statErr := os.Stat(absentRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("absent replay destination stat error = %v, want not created", statErr)
	}

	lookalikeRoot := filepath.Join(t.TempDir(), "lookalike-domain")
	lookalike, err := initializeAdmissionRoot(ctx, lookalikeRoot, AdmissionRootMetadata{ContractVersion: metadata.ContractVersion})
	if err != nil {
		t.Fatal(err)
	}
	if lookalike.DomainUUID == first.NewDomainUUID {
		t.Fatalf("lookalike UUID unexpectedly matched successor UUID %s", first.NewDomainUUID)
	}
	_, err = SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: lookalikeRoot})
	mismatch = SealedSuccessorMismatchError{}
	if !errors.As(err, &mismatch) || mismatch.ExpectedDomainUUID != first.NewDomainUUID || mismatch.ActualDomainUUID != lookalike.DomainUUID {
		t.Fatalf("sealed replay lookalike error = %#v %v, want expected %s actual %s", mismatch, err, first.NewDomainUUID, lookalike.DomainUUID)
	}

	oldInspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatalf("sealed old root inspect after refused replays: %v", err)
	}
	if !oldInspection.Sealed || oldInspection.SuccessorDomainUUID != first.NewDomainUUID {
		t.Fatalf("sealed old root inspection = %+v, want successor %s", oldInspection, first.NewDomainUUID)
	}
}

func TestSealSealedReplayRejectsFailStoppedSuccessorUntilCleared(t *testing.T) {
	ctx := context.Background()
	root, newRoot, first := sealedSuccessorForTest(t, ctx, "seal-replay-fail-stopped")
	persistAdmissionFailStopForTest(t, ctx, newRoot, "persisted successor fail-stop")

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	requireSealedSuccessorMismatch(t, err, first.NewDomainUUID, "fail_stopped")
	if !errors.Is(err, ErrFailStopped) {
		t.Fatalf("fail-stopped successor replay error = %v, want ErrFailStopped", err)
	}

	clear, err := ClearAdmissionFailStop(ctx, newRoot, ClearFailStopOptions{AcknowledgeUnsafeDiagnosis: true})
	if err != nil {
		t.Fatalf("clear fail-stop successor: %v", err)
	}
	if !clear.Cleared || clear.ClearedReason != "persisted successor fail-stop" {
		t.Fatalf("clear fail-stop report = %+v, want cleared persisted reason", clear)
	}
	replay, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("sealed replay after clear-fail-stop error = %v", err)
	}
	if replay.NewDomainUUID != first.NewDomainUUID {
		t.Fatalf("sealed replay after clear UUID = %s, want %s", replay.NewDomainUUID, first.NewDomainUUID)
	}
}

func TestSealSealedReplayRejectsSuccessorSealedOnward(t *testing.T) {
	ctx := context.Background()
	root, newRoot, first := sealedSuccessorForTest(t, ctx, "seal-replay-sealed-onward")
	thirdRoot := filepath.Join(t.TempDir(), "third-domain")
	if _, err := SealAdmissionRoot(ctx, newRoot, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: thirdRoot}); err != nil {
		t.Fatalf("seal successor onward: %v", err)
	}

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	requireSealedSuccessorMismatch(t, err, first.NewDomainUUID, "sealed")
	if !errors.Is(err, ErrRootSealed) {
		t.Fatalf("sealed-onward successor replay error = %v, want ErrRootSealed", err)
	}
}

func TestSealSealedReplayRejectsMatrixInvalidSuccessor(t *testing.T) {
	ctx := context.Background()
	root, newRoot, first := sealedSuccessorForTest(t, ctx, "seal-replay-matrix-invalid")
	jobID := addPendingAdmissionJobForTest(t, ctx, newRoot, "seal-replay-matrix-invalid-successor")
	corruptAdmissionSafetyForTest(t, newRoot, jobID, "safety checksum")

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	requireSealedSuccessorMismatch(t, err, first.NewDomainUUID, "repository integrity")
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("matrix-invalid successor replay error = %v, want ErrCorruptRecord", err)
	}
}

func TestSealSealedReplayAcceptsStartableSuccessorWithPendingRecovery(t *testing.T) {
	ctx := context.Background()
	root, newRoot, first := sealedSuccessorForTest(t, ctx, "seal-replay-pending-recovery")
	addPendingAdmissionJobForTest(t, ctx, newRoot, "seal-replay-pending-recovery-successor")

	replay, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatalf("sealed replay to successor with pending recovery error = %v", err)
	}
	if replay.NewDomainUUID != first.NewDomainUUID {
		t.Fatalf("pending successor replay UUID = %s, want %s", replay.NewDomainUUID, first.NewDomainUUID)
	}
	if replay.NewInspection.Counts.RecoveryObligations == 0 {
		t.Fatalf("pending successor replay inspection = %+v, want recovery obligation retained", replay.NewInspection)
	}
}

func TestBeginStartabilityProbeMatchesBeginOutcomes(t *testing.T) {
	ctx := context.Background()
	_, genuineRoot, _ := sealedSuccessorForTest(t, ctx, "probe-equivalence-genuine")

	_, pendingRoot, _ := sealedSuccessorForTest(t, ctx, "probe-equivalence-pending")
	addPendingAdmissionJobForTest(t, ctx, pendingRoot, "probe-equivalence-pending-successor")

	_, failStoppedRoot, _ := sealedSuccessorForTest(t, ctx, "probe-equivalence-fail-stopped")
	persistAdmissionFailStopForTest(t, ctx, failStoppedRoot, "equivalence fail-stop")

	_, sealedRoot, _ := sealedSuccessorForTest(t, ctx, "probe-equivalence-sealed")
	if _, err := SealAdmissionRoot(ctx, sealedRoot, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: filepath.Join(t.TempDir(), "third-domain")}); err != nil {
		t.Fatalf("seal equivalence successor onward: %v", err)
	}

	_, matrixRoot, _ := sealedSuccessorForTest(t, ctx, "probe-equivalence-matrix")
	matrixJobID := addPendingAdmissionJobForTest(t, ctx, matrixRoot, "probe-equivalence-matrix-successor")
	corruptAdmissionSafetyForTest(t, matrixRoot, matrixJobID, "safety checksum")

	tests := []struct {
		name    string
		root    string
		want    error
		barrier string
	}{
		{name: "genuine successor", root: genuineRoot},
		{name: "pending recovery obligations", root: pendingRoot},
		{name: "persisted fail_stopped", root: failStoppedRoot, want: ErrFailStopped, barrier: "fail_stopped"},
		{name: "sealed onward", root: sealedRoot, want: ErrRootSealed, barrier: "sealed"},
		{name: "matrix invalid", root: matrixRoot, want: repository.ErrCorruptRecord, barrier: "matrix-invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probeRoot := cloneAdmissionRootForTest(t, tt.root, "probe")
			beginRoot := cloneAdmissionRootForTest(t, tt.root, "begin")

			_, probeErr := probeAdmissionStartabilityForTest(t, ctx, probeRoot)
			beginErr := beginAdmissionRootForTest(t, ctx, beginRoot, strings.ReplaceAll(tt.name, " ", "-"))
			if (probeErr == nil) != (beginErr == nil) {
				t.Fatalf("probe error = %v, Begin error = %v; want matching accept/reject", probeErr, beginErr)
			}
			if tt.want == nil {
				if probeErr != nil || beginErr != nil {
					t.Fatalf("probe error = %v, Begin error = %v; want both accepted", probeErr, beginErr)
				}
				return
			}
			if !errors.Is(probeErr, tt.want) || !errors.Is(beginErr, tt.want) {
				t.Fatalf("probe error = %v, Begin error = %v; want both wrapping %v", probeErr, beginErr, tt.want)
			}
			if !strings.Contains(probeErr.Error(), tt.barrier) || !strings.Contains(beginErr.Error(), tt.barrier) {
				t.Fatalf("probe error = %v, Begin error = %v; want barrier %q", probeErr, beginErr, tt.barrier)
			}
		})
	}
}

func TestSealMissingNewStateRootParentIsTypedContractError(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-missing-parent")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "missing-parent", "new-domain")

	_, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	var parentErr NewStateRootParentMissingError
	if !errors.As(err, &parentErr) || !errors.Is(err, ErrNewStateRootParentMissing) || parentErr.Parent != filepath.Dir(newRoot) {
		t.Fatalf("seal missing parent error = %#v %v, want typed parent contract error", parentErr, err)
	}
	if !strings.Contains(err.Error(), "parent directory of --new-state-root must already exist") {
		t.Fatalf("seal missing parent error = %v, want parent contract message", err)
	}
	if _, statErr := os.Stat(filepath.Dir(newRoot)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("missing parent stat error = %v, want not created", statErr)
	}
	inspection, err := InspectAdmissionRoot(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Sealed {
		t.Fatal("old root was sealed after missing parent refusal")
	}
}

func TestSealSuccessorDomainUUIDIsImmutableViaPutMeta(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-successor-immutable")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	report, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatal(err)
	}

	writable, err := bboltrepo.OpenExisting(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer writable.Close()
	_, err = writable.Update(ctx, func(tx repository.WriteTx) error {
		metaRecord := tx.Meta()
		if metaRecord.State != repository.RecordValid {
			t.Fatalf("meta state = %s, want valid", metaRecord.State)
		}
		meta := metaRecord.Value
		meta.SuccessorDomainUUID = "mutated-successor-domain"
		return tx.PutMeta(meta)
	})
	if !errors.Is(err, repository.ErrInvalidRecord) {
		t.Fatalf("successor UUID mutation error = %v, want ErrInvalidRecord", err)
	}
	inspection, err := InspectAdmissionRepository(ctx, writable)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.SuccessorDomainUUID != report.NewDomainUUID {
		t.Fatalf("successor UUID after failed mutation = %s, want %s", inspection.SuccessorDomainUUID, report.NewDomainUUID)
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

func TestSealAllowsTerminalOrphanedUnresolvedHistory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, "seal-terminal-orphaned")
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(ctx, acceptRequest(t, "seal-terminal-orphaned"))
	if err != nil {
		t.Fatal(err)
	}
	record := terminalizeOrphanedUnresolvedForTest(t, ctx, ready, accepted)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeOrphaned {
		t.Fatalf("terminal = %+v, want orphaned", record.Terminal)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := SealAdmissionRoot(ctx, root, SealOptions{
		StartNewAuthorityDomain:       true,
		AcknowledgeReplayHistoryReset: true,
		NewStateRoot:                  filepath.Join(t.TempDir(), "new-domain"),
	})
	if err != nil {
		t.Fatalf("seal terminal orphaned root: %v", err)
	}
	if !report.OldRootSealed || !report.OldInspection.Sealed {
		t.Fatalf("seal report = %+v, want old root sealed", report)
	}
}

func TestSealAndInspectSurfaceCorruptSafetyStats(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobID := addPendingAdmissionJobForTest(t, ctx, root, "seal-corrupt-rootstats")
	corruptAdmissionSafetyForTest(t, root, jobID, "safety checksum")

	if _, err := InspectAdmissionRoot(ctx, root); !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("InspectAdmissionRoot error = %v, want ErrCorruptRecord", err)
	}
	if _, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: filepath.Join(t.TempDir(), "new-domain")}); !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("SealAdmissionRoot error = %v, want ErrCorruptRecord", err)
	}
}

func TestSealAndInspectSurfaceCorruptBindingIndexStats(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobID := addPendingAdmissionJobForTest(t, ctx, root, "seal-corrupt-binding-index")
	corruptAdmissionBindingIndexForTest(t, root, jobID, mustRequestKey(t, "workspace-seal-corrupt-binding-index-other", "request-seal-corrupt-binding-index-other"))

	_, err := InspectAdmissionRoot(ctx, root)
	requireCorruptRecordKind(t, err, "binding_index")
	_, err = SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: filepath.Join(t.TempDir(), "new-domain")})
	requireCorruptRecordKind(t, err, "binding_index")
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
	repo, err := openOrCreateBboltAdmissionRepositoryForTest(filepath.Join(root, AdmissionRepositoryFile))
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

func setInitializeAdmissionRootBeforeRepositoryCreateForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := initializeAdmissionRootBeforeRepositoryCreateForTest
	initializeAdmissionRootBeforeRepositoryCreateForTest = hook
	t.Cleanup(func() {
		initializeAdmissionRootBeforeRepositoryCreateForTest = previous
	})
}

func setInitializeAdmissionRootBeforeAnchorCreateForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := initializeAdmissionRootBeforeAnchorCreateForTest
	initializeAdmissionRootBeforeAnchorCreateForTest = hook
	t.Cleanup(func() {
		initializeAdmissionRootBeforeAnchorCreateForTest = previous
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

func sealedSuccessorForTest(t *testing.T, ctx context.Context, name string) (string, string, SealReport) {
	t.Helper()
	root := t.TempDir()
	repo, session := beginBboltAdmissionRoot(t, root, name)
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	newRoot := filepath.Join(t.TempDir(), "new-domain")
	report, err := SealAdmissionRoot(ctx, root, SealOptions{StartNewAuthorityDomain: true, AcknowledgeReplayHistoryReset: true, NewStateRoot: newRoot})
	if err != nil {
		t.Fatal(err)
	}
	return root, newRoot, report
}

func persistAdmissionFailStopForTest(t *testing.T, ctx context.Context, root, reason string) {
	t.Helper()
	repo, err := openReadOnlyAdmissionRepository(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if closeErr := repo.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	_, anchorPath, err := admissionRootPaths(root)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := adminBootRef("test-fail-stop")
	if err != nil {
		t.Fatal(err)
	}
	if err := NewFileAnchor(anchorPath, dbUUID, schemaMajor).FailStop(ctx, boot, reason); err != nil {
		t.Fatal(err)
	}
}

func addPendingAdmissionJobForTest(t *testing.T, ctx context.Context, root, name string) model.JobID {
	t.Helper()
	repo, session := beginBboltAdmissionRoot(t, root, name)
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(ctx, acceptRequest(t, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	return accepted.Record.JobID
}

func corruptAdmissionSafetyForTest(t *testing.T, root string, jobID model.JobID, diagnostic string) {
	t.Helper()
	repo, err := bboltrepo.OpenExisting(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptSafetyForTest(jobID, diagnostic)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
}

func corruptAdmissionBindingIndexForTest(t *testing.T, root string, jobID model.JobID, key model.RequestKey) {
	t.Helper()
	repo, err := bboltrepo.OpenExisting(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptBindingIndexValueForTest(jobID, key)
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
}

func mustRequestKey(t *testing.T, workspace, request string) model.RequestKey {
	t.Helper()
	key, err := model.NewRequestKey(workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func requireCorruptRecordKind(t *testing.T, err error, kind string) {
	t.Helper()
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("error = %v, want ErrCorruptRecord", err)
	}
	var corrupt repository.CorruptRecordKindError
	if !errors.As(err, &corrupt) || corrupt.Kind != kind {
		t.Fatalf("error = %T %v, want corrupt kind %s", err, err, kind)
	}
}

func requireSealedSuccessorMismatch(t *testing.T, err error, expectedDomainUUID, barrier string) SealedSuccessorMismatchError {
	t.Helper()
	var mismatch SealedSuccessorMismatchError
	if !errors.As(err, &mismatch) || !errors.Is(err, ErrSealedSuccessorMismatch) || mismatch.ExpectedDomainUUID != expectedDomainUUID {
		t.Fatalf("sealed replay error = %#v %v, want typed successor mismatch for %s", mismatch, err, expectedDomainUUID)
	}
	if !strings.Contains(err.Error(), barrier) {
		t.Fatalf("sealed replay error = %v, want barrier %q", err, barrier)
	}
	return mismatch
}

func cloneAdmissionRootForTest(t *testing.T, srcRoot, name string) string {
	t.Helper()
	dstRoot := filepath.Join(t.TempDir(), name)
	if err := os.Mkdir(dstRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{AdmissionRepositoryFile, AdmissionAnchorFile} {
		if err := copyFile(filepath.Join(dstRoot, file), filepath.Join(srcRoot, file)); err != nil {
			t.Fatal(err)
		}
	}
	return dstRoot
}

func probeAdmissionStartabilityForTest(t *testing.T, ctx context.Context, root string) (beginStartabilityProbe, error) {
	t.Helper()
	repoPath, anchorPath, err := admissionRootPaths(root)
	if err != nil {
		t.Fatal(err)
	}
	beforeRepoBytes, err := os.ReadFile(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	beforeAnchorBytes, err := os.ReadFile(anchorPath)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := bboltrepo.OpenExistingReadOnly(repoPath)
	if err != nil {
		return beginStartabilityProbe{}, err
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		if closeErr := repo.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		return beginStartabilityProbe{}, err
	}
	probe, err := probeBeginStartability(ctx, repo, &fileAnchor{
		path:               anchorPath,
		dbUUID:             dbUUID,
		schemaMajor:        schemaMajor,
		requireInitialized: true,
	})
	if closeErr := repo.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	afterRepoBytes, readErr := os.ReadFile(repoPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	afterAnchorBytes, readErr := os.ReadFile(anchorPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(afterRepoBytes, beforeRepoBytes) {
		t.Fatal("startability probe mutated destination repository")
	}
	if !bytes.Equal(afterAnchorBytes, beforeAnchorBytes) {
		t.Fatal("startability probe mutated destination anchor")
	}
	return probe, err
}

func beginAdmissionRootForTest(t *testing.T, ctx context.Context, root, name string) error {
	t.Helper()
	repo, err := openOrCreateBboltAdmissionRepositoryForTest(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		return err
	}
	defer repo.Close()
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return err
	}
	_, anchorPath, err := admissionRootPaths(root)
	if err != nil {
		return err
	}
	boot, err := model.NewBootRef("boot-probe-"+name, "owner-probe-"+name)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapper, err := NewBootstrapper(repo, WithAnchor(NewFileAnchor(anchorPath, dbUUID, schemaMajor)))
	if err != nil {
		return err
	}
	_, err = bootstrapper.Begin(ctx, boot)
	return err
}

func openOrCreateBboltAdmissionRepositoryForTest(path string) (*bboltrepo.Repository, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return bboltrepo.Create(path)
	} else if err != nil {
		return nil, err
	}
	return bboltrepo.OpenExisting(path)
}

type bboltEnvelopeForTest struct {
	Kind          string          `json:"kind"`
	SchemaVersion uint16          `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	Payload       json.RawMessage `json:"payload"`
	Checksum      string          `json:"checksum"`
}

func forgeBboltAdmissionContractVersionForTest(t *testing.T, root string, version uint16) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(root, AdmissionRepositoryFile), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("meta"))
		if bucket == nil {
			return errors.New("meta bucket missing")
		}
		raw := bucket.Get([]byte("authority"))
		if raw == nil {
			return errors.New("authority meta missing")
		}
		var envelope bboltEnvelopeForTest
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
		var meta repository.AuthorityMeta
		if err := json.Unmarshal(envelope.Payload, &meta); err != nil {
			return err
		}
		meta.AdmissionRoot.ContractVersion = version
		payload, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		envelope.Payload = append(json.RawMessage(nil), payload...)
		envelope.Checksum = checksumBboltEnvelopeForTest(envelope.Kind, envelope.SchemaVersion, envelope.Revision, []byte("authority"), envelope.Payload)
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("authority"), encoded)
	}); err != nil {
		t.Fatal(err)
	}
}

func checksumBboltEnvelopeForTest(kind string, schemaVersion uint16, revision uint64, key, payload []byte) string {
	hash := sha256.New()
	checksumBboltFieldForTest(hash, kind)
	checksumBboltFieldForTest(hash, strconv.FormatUint(uint64(schemaVersion), 10))
	checksumBboltFieldForTest(hash, strconv.FormatUint(revision, 10))
	checksumBboltFieldForTest(hash, string(key))
	checksumBboltFieldForTest(hash, string(payload))
	return hex.EncodeToString(hash.Sum(nil))
}

func checksumBboltFieldForTest(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}

func copyFile(dst, src string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, raw, 0o600)
}
