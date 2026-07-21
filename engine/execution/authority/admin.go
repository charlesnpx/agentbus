package authority

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
)

const (
	AdmissionRepositoryFile = "admission.bbolt"
	AdmissionAnchorFile     = "admission-anchor.json"
)

var (
	ErrRootNotEmpty               = errors.New("authority root is not empty")
	ErrSealConfirmationRequired   = errors.New("authority seal confirmation required")
	ErrRootHasRecoveryObligations = errors.New("authority root has recovery obligations")
)

type RootInspection struct {
	ActivationMetadata AdmissionRootMetadata         `json:"activationMetadata"`
	ContractVersion    uint16                        `json:"contractVersion"`
	Generation         uint64                        `json:"generation"`
	Counts             repository.AuthorityRootStats `json:"counts"`
	DomainUUID         string                        `json:"domainUUID"`
	Sealed             bool                          `json:"sealed"`
}

type RootNotEmptyError struct {
	Counts repository.AuthorityRootStats
}

func (e RootNotEmptyError) Error() string {
	return fmt.Sprintf("%s: %s", ErrRootNotEmpty, strings.Join(nonzeroRootStats(e.Counts), ", "))
}

func (e RootNotEmptyError) Is(target error) bool {
	return target == ErrRootNotEmpty
}

type SealOptions struct {
	StartNewAuthorityDomain       bool
	AcknowledgeReplayHistoryReset bool
}

func InspectAdmissionRoot(ctx context.Context, stateRoot string) (RootInspection, error) {
	repo, err := openReadOnlyAdmissionRepository(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	defer repo.Close()
	return InspectAdmissionRepository(ctx, repo)
}

func InspectAdmissionRepository(ctx context.Context, repo repository.Repository) (RootInspection, error) {
	if repo == nil {
		return RootInspection{}, errors.New("authority repository is required")
	}
	domainUUID, _, err := repositoryAnchorIdentity(repo)
	if err != nil {
		return RootInspection{}, err
	}
	var inspection RootInspection
	inspection.DomainUUID = domainUUID
	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		metaRecord := tx.Meta()
		if metaRecord.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, metaRecord.State)
		}
		meta := metaRecord.Value
		if err := meta.Validate(); err != nil {
			return err
		}
		stats, err := tx.RootStats()
		if err != nil {
			return err
		}
		inspection.ActivationMetadata = meta.AdmissionRoot
		inspection.ContractVersion = meta.AdmissionRoot.ContractVersion
		inspection.Generation = meta.Generation
		inspection.Counts = stats
		inspection.Sealed = meta.Sealed
		return nil
	}); err != nil {
		return RootInspection{}, err
	}
	return inspection, nil
}

func ResetEmptyAdmissionRoot(ctx context.Context, stateRoot string) (RootInspection, error) {
	if err := ctx.Err(); err != nil {
		return RootInspection{}, err
	}
	repoPath, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return RootInspection{}, err
	}
	if _, err := os.Stat(repoPath); err == nil {
		inspection, err := InspectAdmissionRoot(ctx, stateRoot)
		if err != nil {
			return RootInspection{}, err
		}
		if !inspection.Counts.Empty() {
			return inspection, RootNotEmptyError{Counts: inspection.Counts}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return RootInspection{}, err
	}
	if err := os.Remove(repoPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return RootInspection{}, err
	}
	if err := os.Remove(anchorPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return RootInspection{}, err
	}
	repo, err := bboltrepo.NewRepository(repoPath)
	if err != nil {
		return RootInspection{}, err
	}
	defer repo.Close()
	return InspectAdmissionRepository(ctx, repo)
}

func SealAdmissionRoot(ctx context.Context, stateRoot string, options SealOptions) (RootInspection, error) {
	if !options.StartNewAuthorityDomain || !options.AcknowledgeReplayHistoryReset {
		return RootInspection{}, ErrSealConfirmationRequired
	}
	repo, err := openWritableAdmissionRepository(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	defer repo.Close()

	before, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return RootInspection{}, err
	}
	if before.Counts.RecoveryObligations != 0 {
		return before, fmt.Errorf("%w: %d", ErrRootHasRecoveryObligations, before.Counts.RecoveryObligations)
	}
	if before.Sealed {
		return before, nil
	}
	_, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	_, schemaMajor, err := repositoryAnchorIdentity(repo)
	if err != nil {
		return RootInspection{}, err
	}
	boot, err := adminBootRef("seal")
	if err != nil {
		return RootInspection{}, err
	}
	anchor := NewFileAnchor(anchorPath, before.DomainUUID, schemaMajor)
	if _, err := anchor.Begin(ctx, boot, before.Generation); err != nil {
		return RootInspection{}, err
	}
	commit, err := repo.Update(ctx, func(tx repository.WriteTx) error {
		metaRecord := tx.Meta()
		if metaRecord.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, metaRecord.State)
		}
		meta := metaRecord.Value
		if err := meta.Validate(); err != nil {
			return err
		}
		if meta.Sealed {
			return nil
		}
		meta.Sealed = true
		return tx.PutMeta(meta)
	})
	if err != nil {
		return RootInspection{}, err
	}
	if err := anchor.Advance(ctx, boot, commit.Generation); err != nil {
		return RootInspection{}, err
	}
	return InspectAdmissionRepository(ctx, repo)
}

func openReadOnlyAdmissionRepository(stateRoot string) (*bboltrepo.Repository, error) {
	repoPath, _, err := admissionRootPaths(stateRoot)
	if err != nil {
		return nil, err
	}
	return bboltrepo.OpenReadOnly(repoPath)
}

func openWritableAdmissionRepository(stateRoot string) (*bboltrepo.Repository, error) {
	repoPath, _, err := admissionRootPaths(stateRoot)
	if err != nil {
		return nil, err
	}
	return bboltrepo.NewRepository(repoPath)
}

func admissionRootPaths(stateRoot string) (string, string, error) {
	if strings.TrimSpace(stateRoot) == "" {
		return "", "", errors.New("state root is required")
	}
	return filepath.Join(stateRoot, AdmissionRepositoryFile), filepath.Join(stateRoot, AdmissionAnchorFile), nil
}

func adminBootRef(operation string) (model.BootRef, error) {
	return model.NewBootRef("boot-admission-admin-"+operation, "owner-admission-admin-"+operation)
}

func nonzeroRootStats(stats repository.AuthorityRootStats) []string {
	var parts []string
	if stats.Jobs != 0 {
		parts = append(parts, fmt.Sprintf("jobs=%d", stats.Jobs))
	}
	if stats.Bindings != 0 {
		parts = append(parts, fmt.Sprintf("bindings=%d", stats.Bindings))
	}
	if stats.Tombstones != 0 {
		parts = append(parts, fmt.Sprintf("tombstones=%d", stats.Tombstones))
	}
	if stats.LaunchRecords != 0 {
		parts = append(parts, fmt.Sprintf("launch_records=%d", stats.LaunchRecords))
	}
	if stats.RecoveryObligations != 0 {
		parts = append(parts, fmt.Sprintf("recovery_obligations=%d", stats.RecoveryObligations))
	}
	if len(parts) == 0 {
		parts = append(parts, "empty")
	}
	return parts
}
