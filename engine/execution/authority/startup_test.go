package authority

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestRecoverySessionCompletesPriorBootFromSnapshots(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	oldReady := newReadyWithAnchorStore(t, repo, anchorStore, "old-recovery")
	accepted, err := oldReady.Accept(ctx, acceptRequest(t, "snapshot-recovery"))
	if err != nil {
		t.Fatal(err)
	}

	restartRepo, restartAnchor := restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
	session := newRecoverySessionWithAnchorStore(t, restartRepo, restartAnchor, "new-recovery")
	plans, err := session.Plans(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if _, err := session.SealReady(ctx); !errors.Is(err, ErrRecoveryNeeded) {
		t.Fatalf("SealReady before receipts error = %v, want ErrRecoveryNeeded", err)
	}

	terminalizeRecovery(t, ctx, session, accepted.Record.Attempt.Ref)
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.State != repository.RecordValid || image.Safety.Value.Terminal == nil {
		t.Fatalf("recovered safety = %#v, want terminal", image.Safety)
	}
	if image.Safety.Value.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("terminal proof = %s, want %s", image.Safety.Value.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}
	runtime, err := ready.RuntimeSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.Pending) != 0 || len(runtime.Owned) != 0 {
		t.Fatalf("runtime = %#v, want empty after prior-boot terminal recovery", runtime)
	}
}

func TestStartupCorruptionMatrixFromSnapshots(t *testing.T) {
	ctx := context.Background()
	baseRepo := memory.NewRepository()
	baseAnchor := NewAnchorStore()
	ready := newReadyWithAnchorStore(t, baseRepo, baseAnchor, "matrix-base")
	accepted, err := ready.Accept(ctx, acceptRequest(t, "matrix-base"))
	if err != nil {
		t.Fatal(err)
	}
	terminalizeReady(t, ctx, ready, accepted.Record.Attempt.Ref, accepted.Record.JobID)
	baseRepoSnapshot := baseRepo.SnapshotBytes()
	baseAnchorSnapshot := baseAnchor.SnapshotBytes()

	t.Run("valid binding safety projection seals ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		session := newRecoverySessionWithAnchorStore(t, repo, anchorStore, "matrix-valid")
		if _, err := session.SealReady(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("valid safety with missing projection is rebuilt", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectMissingProjectionForTest(accepted.Record.JobID)
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		session := newRecoverySessionWithAnchorStore(t, repo, anchorStore, "matrix-missing-projection")
		ready, err := session.SealReady(ctx)
		if err != nil {
			t.Fatal(err)
		}
		image, err := ready.LoadJob(ctx, accepted.Record.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if image.Projection.State != repository.RecordValid {
			t.Fatalf("projection state = %s, want valid", image.Projection.State)
		}
	})

	t.Run("valid safety with corrupt projection is quarantined and rebuilt", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectCorruptProjectionForTest(accepted.Record.JobID, "projection checksum")
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		session := newRecoverySessionWithAnchorStore(t, repo, anchorStore, "matrix-corrupt-projection")
		ready, err := session.SealReady(ctx)
		if err != nil {
			t.Fatal(err)
		}
		image, err := ready.LoadJob(ctx, accepted.Record.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if image.Projection.State != repository.RecordValid {
			t.Fatalf("projection state = %s, want valid", image.Projection.State)
		}
		if image.Quarantine.State != repository.RecordValid {
			t.Fatalf("quarantine state = %s, want valid", image.Quarantine.State)
		}
	})

	t.Run("valid binding with missing safety is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectMissingSafetyForTest(accepted.Record.JobID)
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "matrix-missing-safety")
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("Begin error = %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("valid binding with corrupt safety is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectCorruptSafetyForTest(accepted.Record.JobID, "safety checksum")
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "matrix-corrupt-safety")
		if !errors.Is(err, repository.ErrCorruptRecord) {
			t.Fatalf("Begin error = %v, want ErrCorruptRecord", err)
		}
	})

	t.Run("missing binding with valid safety is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectMissingBindingForTest(accepted.Binding.RequestKey)
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "matrix-missing-binding")
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("Begin error = %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("corrupt binding with valid safety is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectCorruptBindingForTest(accepted.Binding.RequestKey, "binding checksum")
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "matrix-corrupt-binding")
		if !errors.Is(err, repository.ErrCorruptRecord) {
			t.Fatalf("Begin error = %v, want ErrCorruptRecord", err)
		}
	})

	t.Run("tombstone with live safety is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectTombstoneForTest(repository.Tombstone{
			RequestKey:        accepted.Binding.RequestKey,
			JobID:             accepted.Record.JobID,
			TaskIdentity:      accepted.Record.TaskIdentity,
			ExpiredGeneration: 99,
		})
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "matrix-tombstone-live")
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("Begin error = %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("whole db invalid is fatal before ready", func(t *testing.T) {
		repo, anchorStore := restoreAuthoritySnapshot(t, baseRepoSnapshot, baseAnchorSnapshot)
		repo.InjectCorruptMetaForTest("meta checksum")
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := newBootstrapperWithAnchorStore(repo, anchorStore)
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("NewBootstrapper error = %v, want ErrInvalidRecord", err)
		}
	})
}

func TestProjectionCorruptionNeverSuppliesGrantCertainty(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := NewAnchorStore()
	oldReady := newReadyWithAnchorStore(t, repo, anchorStore, "projection-certainty-old")
	accepted, err := oldReady.Accept(ctx, acceptRequest(t, "projection-certainty"))
	if err != nil {
		t.Fatal(err)
	}
	misleading := accepted.Projection
	misleading.Dispatch = model.DispatchPermitGranted
	misleading.Public = model.PublicStarting
	repo.InjectProjectionForTest(misleading)

	restartRepo, restartAnchor := restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
	session := newRecoverySessionWithAnchorStore(t, restartRepo, restartAnchor, "projection-certainty-new")
	plans, err := session.Plans(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if plans[0].Next.Kind != model.RecoveryFinalizeCertified {
		t.Fatalf("plan kind = %v, want RecoveryFinalizeCertified from safety without launch evidence", plans[0].Next.Kind)
	}

	terminalizeRecovery(t, ctx, session, accepted.Record.Attempt.Ref)
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	image, err := ready.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.Value.Terminal == nil {
		t.Fatal("terminal certificate missing")
	}
	if image.Safety.Value.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("terminal proof = %s, want %s", image.Safety.Value.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}
	if image.Quarantine.State != repository.RecordValid {
		t.Fatalf("quarantine state = %s, want valid for mismatched projection", image.Quarantine.State)
	}
}

func TestAnchorStoreStartupBarrierFromSnapshots(t *testing.T) {
	ctx := context.Background()

	t.Run("initial publication failure blocks recovery session", func(t *testing.T) {
		repo := memory.NewRepository()
		anchorStore := NewAnchorStore()
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		anchorStore.FailNextForTest(AnchorBegin, errors.New("begin fsync failed"))
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "anchor-begin-fail")
		if err == nil {
			t.Fatal("Begin succeeded, want anchor failure")
		}
	})

	t.Run("seal ready failure happens before ready", func(t *testing.T) {
		repo := memory.NewRepository()
		anchorStore := NewAnchorStore()
		session := newRecoverySessionWithAnchorStore(t, repo, anchorStore, "anchor-seal")
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		session = newRecoverySessionWithAnchorStore(t, repo, anchorStore, "anchor-seal-restart")
		anchorStore.FailNextForTest(AnchorSealReady, errors.New("seal rename failed"))
		ready, err := session.SealReady(ctx)
		if err == nil || ready != nil {
			t.Fatalf("SealReady = (%#v, %v), want failure before Ready", ready, err)
		}
	})

	t.Run("db ahead with persisted fail-stop refuses restart", func(t *testing.T) {
		repo := memory.NewRepository()
		anchorStore := NewAnchorStore()
		ready := newReadyWithAnchorStore(t, repo, anchorStore, "anchor-advance-old")
		anchorStore.FailNextForTest(AnchorAdvance, errors.New("advance fsync failed"))
		_, err := ready.Accept(ctx, acceptRequest(t, "anchor-advance"))
		if err == nil {
			t.Fatal("Accept succeeded, want anchor advance failure")
		}

		restartRepo, restartAnchor := restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		if _, err := beginRecoveryWithAnchorStore(t, restartRepo, restartAnchor, "anchor-advance-new"); !errors.Is(err, ErrFailStopped) || !strings.Contains(err.Error(), "anchor advance") {
			t.Fatalf("restart Begin error = %v, want retained fail-stop reason", err)
		}
	})

	t.Run("anchor ahead of db is fatal before ready", func(t *testing.T) {
		repo := memory.NewRepository()
		dbUUID, schemaMajor := anchorIdentity(t, repo)
		anchorStore := NewAnchorStore()
		anchorStore.ForceStateForTest(AnchorSnapshot{
			Initialized: true,
			DBUUID:      dbUUID,
			SchemaMajor: schemaMajor,
			Generation:  1,
			Phase:       "ready",
		})
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "anchor-ahead")
		if !errors.Is(err, ErrAnchorInvariant) {
			t.Fatalf("Begin error = %v, want ErrAnchorInvariant", err)
		}
	})

	t.Run("uuid mismatch is fatal before ready", func(t *testing.T) {
		repo := memory.NewRepository()
		_, schemaMajor := anchorIdentity(t, repo)
		anchorStore := NewAnchorStore()
		anchorStore.ForceStateForTest(AnchorSnapshot{
			Initialized: true,
			DBUUID:      "different-db",
			SchemaMajor: schemaMajor,
			Generation:  0,
			Phase:       "ready",
		})
		_, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, "anchor-uuid")
		if !errors.Is(err, ErrAnchorInvariant) {
			t.Fatalf("Begin error = %v, want ErrAnchorInvariant", err)
		}
	})

	t.Run("missing db with initialized anchor is fatal before ready", func(t *testing.T) {
		repo := memory.NewRepository()
		dbUUID, schemaMajor := anchorIdentity(t, repo)
		anchorStore := NewAnchorStore()
		anchorStore.ForceStateForTest(AnchorSnapshot{
			Initialized: true,
			DBUUID:      dbUUID,
			SchemaMajor: schemaMajor,
			Generation:  0,
			Phase:       "ready",
		})
		repo.InjectMissingMetaForTest()
		repo, anchorStore = restoreAuthoritySnapshot(t, repo.SnapshotBytes(), anchorStore.SnapshotBytes())
		_, err := newBootstrapperWithAnchorStore(repo, anchorStore)
		if !errors.Is(err, repository.ErrInvalidRecord) {
			t.Fatalf("NewBootstrapper error = %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("missing anchor with initialized db is fatal before ready", func(t *testing.T) {
		repo := memory.NewRepository()
		anchorStore := NewAnchorStore()
		ready := newReadyWithAnchorStore(t, repo, anchorStore, "missing-anchor-old")
		accepted, err := ready.Accept(ctx, acceptRequest(t, "missing-anchor"))
		if err != nil {
			t.Fatal(err)
		}
		terminalizeReady(t, ctx, ready, accepted.Record.Attempt.Ref, accepted.Record.JobID)
		restartRepo := restoreRepository(t, repo.SnapshotBytes())
		_, err = beginRecoveryWithAnchorStore(t, restartRepo, NewAnchorStore(), "missing-anchor-new")
		if !errors.Is(err, ErrAnchorInvariant) {
			t.Fatalf("Begin error = %v, want ErrAnchorInvariant", err)
		}
	})
}

func newBootstrapperWithAnchorStore(repo repository.Repository, anchorStore *AnchorStore) (*Bootstrapper, error) {
	_, verifier := custodian.NewAttestationChannel()
	return NewBootstrapper(repo, WithAnchorStore(anchorStore), WithQuiescenceVerifier(verifier))
}

func beginRecoveryWithAnchorStore(t *testing.T, repo repository.Repository, anchorStore *AnchorStore, name string) (*RecoverySession, error) {
	t.Helper()
	boot, err := model.NewBootRef("boot-"+name, "owner-"+name)
	if err != nil {
		t.Fatal(err)
	}
	issuer, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := NewBootstrapper(repo, WithAnchorStore(anchorStore), WithQuiescenceVerifier(verifier))
	if err != nil {
		return nil, err
	}
	testAttestationIssuers.Store(boot, issuer)
	return bootstrapper.Begin(context.Background(), boot)
}

func newRecoverySessionWithAnchorStore(t *testing.T, repo repository.Repository, anchorStore *AnchorStore, name string) *RecoverySession {
	t.Helper()
	session, err := beginRecoveryWithAnchorStore(t, repo, anchorStore, name)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func newReadyWithAnchorStore(t *testing.T, repo repository.Repository, anchorStore *AnchorStore, name string) *Ready {
	t.Helper()
	session := newRecoverySessionWithAnchorStore(t, repo, anchorStore, name)
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return ready
}

func restoreAuthoritySnapshot(t *testing.T, repoSnapshot []byte, anchorSnapshot []byte) (*memory.Repository, *AnchorStore) {
	t.Helper()
	return restoreRepository(t, repoSnapshot), restoreAnchorStore(t, anchorSnapshot)
}

func restoreRepository(t *testing.T, snapshot []byte) *memory.Repository {
	t.Helper()
	repo, err := memory.NewRepositoryFromSnapshotBytes(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func restoreAnchorStore(t *testing.T, snapshot []byte) *AnchorStore {
	t.Helper()
	anchorStore, err := NewAnchorStoreFromSnapshotBytes(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return anchorStore
}

func terminalizeReady(t *testing.T, ctx context.Context, ready *Ready, ref model.AttemptRef, jobID model.JobID) {
	t.Helper()
	group := groupRef(ref, model.LaunchOrdinalOne)
	if _, err := ready.BindGroup(ctx, jobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatalf("Ready.BindGroup: %v", err)
	}
	verified := verifiedQuiescence(t, ready.Boot(), ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
	if _, err := ready.RecordQuiescence(ctx, jobID, model.LaunchOrdinalOne, verified); err != nil {
		t.Fatalf("Ready.RecordQuiescence: %v", err)
	}
	if _, err := ready.Finalize(ctx, jobID, ref, model.TerminalIntent{
		Outcome: model.OutcomeFailed,
		Cause:   model.CauseDaemonRestartedBeforeAuthorization,
	}); err != nil {
		t.Fatalf("Ready.Finalize: %v", err)
	}
}

func terminalizeRecovery(t *testing.T, ctx context.Context, session *RecoverySession, ref model.AttemptRef) {
	t.Helper()
	group := groupRef(ref, model.LaunchOrdinalOne)
	if err := session.applyReceipt(ctx, model.BindGroup{Ref: ref, Ordinal: model.LaunchOrdinalOne, Group: group}); err != nil {
		t.Fatalf("RecoverySession.BindGroup: %v", err)
	}
	verified := verifiedQuiescence(t, session.token.boot, ref, model.LaunchOrdinalOne, group, model.QuiescenceAlreadyAbsent)
	if err := session.RecordQuiescence(ctx, ref.JobID, model.LaunchOrdinalOne, verified); err != nil {
		t.Fatalf("RecoverySession.RecordQuiescence: %v", err)
	}
	if err := session.Finalize(ctx, ref, model.TerminalIntent{
		Outcome: model.OutcomeFailed,
		Cause:   model.CauseDaemonRestartedBeforeAuthorization,
	}); err != nil {
		t.Fatalf("RecoverySession.Finalize: %v", err)
	}
}

func firstValidSafetyRecord(t *testing.T, repo repository.Repository) model.SafetyRecord {
	t.Helper()
	var record model.SafetyRecord
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			if image.Safety.State == repository.RecordValid {
				record = image.Safety.Value
				return nil
			}
		}
		return repository.ErrInvalidRecord
	}); err != nil {
		t.Fatal(err)
	}
	return record
}

func anchorIdentity(t *testing.T, repo *memory.Repository) (string, uint16) {
	t.Helper()
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return dbUUID, schemaMajor
}
