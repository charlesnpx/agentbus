package authority

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	bolt "go.etcd.io/bbolt"
)

const (
	AdmissionRepositoryFile = "admission.bbolt"
	AdmissionAnchorFile     = "admission-anchor.json"
)

var adminOpenTimeout = 10 * time.Second

var (
	ErrRootNotEmpty                      = errors.New("authority root is not empty")
	ErrRootBusy                          = errors.New("authority root busy")
	ErrSealConfirmationRequired          = errors.New("authority seal confirmation required")
	ErrClearFailStopConfirmationRequired = errors.New("authority clear fail-stop confirmation required")
	ErrRootHasRecoveryObligations        = errors.New("authority root has recovery obligations")
	ErrNewStateRootNotEmpty              = errors.New("authority new state root is not empty")
)

type RootInspection struct {
	ActivationMetadata AdmissionRootMetadata         `json:"activationMetadata"`
	ContractVersion    uint16                        `json:"contractVersion"`
	Generation         uint64                        `json:"generation"`
	Counts             repository.AuthorityRootStats `json:"counts"`
	DomainUUID         string                        `json:"domainUUID"`
	Sealed             bool                          `json:"sealed"`
	AnchorPhase        string                        `json:"anchorPhase,omitempty"`
	FailStopped        bool                          `json:"failStopped"`
	FailStopReason     string                        `json:"failStopReason,omitempty"`
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
	NewStateRoot                  string
}

type SealReport struct {
	OldRootSealed bool           `json:"oldRootSealed"`
	OldRoot       string         `json:"oldRoot"`
	OldInspection RootInspection `json:"oldInspection"`
	NewRoot       string         `json:"newRoot"`
	NewDomainUUID string         `json:"newDomainUUID"`
	NewInspection RootInspection `json:"newInspection"`
}

type ClearFailStopOptions struct {
	AcknowledgeUnsafeDiagnosis bool
}

type ClearFailStopReport struct {
	Cleared           bool           `json:"cleared"`
	ClearedPhase      string         `json:"clearedPhase,omitempty"`
	ClearedReason     string         `json:"clearedReason,omitempty"`
	ClearedBoot       model.BootRef  `json:"clearedBoot,omitempty"`
	ClearedGeneration uint64         `json:"clearedGeneration"`
	Inspection        RootInspection `json:"inspection"`
}

func InspectAdmissionRoot(ctx context.Context, stateRoot string) (RootInspection, error) {
	repo, err := openReadOnlyAdmissionRepository(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	defer repo.Close()
	inspection, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return RootInspection{}, err
	}
	_, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	_, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return RootInspection{}, err
	}
	return attachAnchorInspection(inspection, anchorPath, schemaMajor)
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
		oldRepo, err := bboltrepo.Open(repoPath, &bolt.Options{Timeout: adminOpenTimeout})
		if err != nil {
			if errors.Is(err, bolt.ErrTimeout) {
				return RootInspection{}, fmt.Errorf("%w: %w", ErrRootBusy, err)
			}
			return RootInspection{}, err
		}
		defer oldRepo.Close()
		inspection, err := InspectAdmissionRepository(ctx, oldRepo)
		if err != nil {
			return RootInspection{}, err
		}
		if !inspection.Counts.Empty() {
			return inspection, RootNotEmptyError{Counts: inspection.Counts}
		}
		// Hold the old bbolt flock across check+unlink so a live Serve cannot
		// slip in between emptiness verification and reset. After unlink, a
		// racing opener can only fail on the missing path or block on the new
		// database we create below; it can never acquire the unlinked inode.
		if err := os.Remove(repoPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return RootInspection{}, err
		}
		if err := os.Remove(anchorPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return RootInspection{}, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.Remove(anchorPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return RootInspection{}, err
		}
	} else {
		return RootInspection{}, err
	}
	return initializeAdmissionRoot(ctx, stateRoot, AdmissionRootMetadata{})
}

func ClearAdmissionFailStop(ctx context.Context, stateRoot string, options ClearFailStopOptions) (ClearFailStopReport, error) {
	if !options.AcknowledgeUnsafeDiagnosis {
		return ClearFailStopReport{}, ErrClearFailStopConfirmationRequired
	}
	repo, err := openWritableAdmissionRepository(stateRoot)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	defer repo.Close()
	inspection, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	_, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	_, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return ClearFailStopReport{}, err
	}
	snapshot, err := loadFileAnchorSnapshot(anchorPath)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	if snapshot.Initialized {
		if snapshot.DBUUID != inspection.DomainUUID || snapshot.SchemaMajor != schemaMajor {
			return ClearFailStopReport{}, fmt.Errorf("%w: anchor identity does not match repository", ErrAnchorInvariant)
		}
	}
	report := ClearFailStopReport{
		Cleared:           snapshot.Phase == "fail_stopped",
		ClearedPhase:      snapshot.Phase,
		ClearedReason:     snapshot.Reason,
		ClearedBoot:       snapshot.Boot,
		ClearedGeneration: snapshot.Generation,
	}
	if report.Cleared {
		snapshot.Phase = ""
		snapshot.Boot = model.BootRef{}
		snapshot.Token = ""
		snapshot.Reason = ""
		if err := saveFileAnchorSnapshot(anchorPath, snapshot); err != nil {
			return ClearFailStopReport{}, err
		}
	}
	report.Inspection, err = InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	report.Inspection, err = attachAnchorInspection(report.Inspection, anchorPath, schemaMajor)
	if err != nil {
		return ClearFailStopReport{}, err
	}
	return report, nil
}

func SealAdmissionRoot(ctx context.Context, stateRoot string, options SealOptions) (SealReport, error) {
	if !options.StartNewAuthorityDomain || !options.AcknowledgeReplayHistoryReset || strings.TrimSpace(options.NewStateRoot) == "" {
		return SealReport{}, ErrSealConfirmationRequired
	}
	newStateRoot := filepath.Clean(options.NewStateRoot)
	if err := requireEmptyStateRoot(newStateRoot); err != nil {
		return SealReport{}, err
	}
	repo, err := openWritableAdmissionRepository(stateRoot)
	if err != nil {
		return SealReport{}, err
	}
	defer repo.Close()

	before, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return SealReport{}, err
	}
	if before.Counts.RecoveryObligations != 0 {
		return SealReport{OldRoot: stateRoot, OldInspection: before}, fmt.Errorf("%w: %d", ErrRootHasRecoveryObligations, before.Counts.RecoveryObligations)
	}
	if before.Sealed {
		newInspection, err := initializeAdmissionRoot(ctx, newStateRoot, AdmissionRootMetadata{ContractVersion: before.ActivationMetadata.ContractVersion})
		if err != nil {
			return SealReport{}, err
		}
		return SealReport{
			OldRootSealed: true,
			OldRoot:       stateRoot,
			OldInspection: before,
			NewRoot:       newStateRoot,
			NewDomainUUID: newInspection.DomainUUID,
			NewInspection: newInspection,
		}, nil
	}
	_, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return SealReport{}, err
	}
	_, schemaMajor, err := repositoryAnchorIdentity(repo)
	if err != nil {
		return SealReport{}, err
	}
	boot, err := adminBootRef("seal")
	if err != nil {
		return SealReport{}, err
	}
	anchor := NewFileAnchor(anchorPath, before.DomainUUID, schemaMajor)
	if _, err := anchor.Begin(ctx, boot, before.Generation); err != nil {
		return SealReport{}, err
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
		return SealReport{}, err
	}
	if err := anchor.Advance(ctx, boot, commit.Generation); err != nil {
		return SealReport{}, err
	}
	oldInspection, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return SealReport{}, err
	}
	oldInspection, err = attachAnchorInspection(oldInspection, anchorPath, schemaMajor)
	if err != nil {
		return SealReport{}, err
	}
	newInspection, err := initializeAdmissionRoot(ctx, newStateRoot, AdmissionRootMetadata{ContractVersion: before.ActivationMetadata.ContractVersion})
	if err != nil {
		return SealReport{}, err
	}
	return SealReport{
		OldRootSealed: oldInspection.Sealed,
		OldRoot:       stateRoot,
		OldInspection: oldInspection,
		NewRoot:       newStateRoot,
		NewDomainUUID: newInspection.DomainUUID,
		NewInspection: newInspection,
	}, nil
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

func initializeAdmissionRoot(ctx context.Context, stateRoot string, metadata AdmissionRootMetadata) (RootInspection, error) {
	repoPath, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return RootInspection{}, err
	}
	repo, err := bboltrepo.NewRepository(repoPath)
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return RootInspection{}, fmt.Errorf("%w: %w", ErrRootBusy, err)
		}
		return RootInspection{}, err
	}
	defer repo.Close()
	if metadata != (AdmissionRootMetadata{}) {
		if _, err := repo.Update(ctx, func(tx repository.WriteTx) error {
			metaRecord := tx.Meta()
			if metaRecord.State != repository.RecordValid {
				return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, metaRecord.State)
			}
			meta := metaRecord.Value
			meta.AdmissionRoot = metadata
			return tx.PutMeta(meta)
		}); err != nil {
			return RootInspection{}, err
		}
	}
	inspection, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return RootInspection{}, err
	}
	_, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return RootInspection{}, err
	}
	if err := saveFileAnchorSnapshot(anchorPath, AnchorSnapshot{
		Initialized: true,
		DBUUID:      inspection.DomainUUID,
		SchemaMajor: schemaMajor,
		Generation:  inspection.Generation,
	}); err != nil {
		return RootInspection{}, err
	}
	return attachAnchorInspection(inspection, anchorPath, schemaMajor)
}

func requireEmptyStateRoot(stateRoot string) error {
	if strings.TrimSpace(stateRoot) == "" {
		return errors.New("new state root is required")
	}
	info, err := os.Stat(stateRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrNewStateRootNotEmpty, stateRoot)
	}
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("%w: %s", ErrNewStateRootNotEmpty, stateRoot)
	}
	return nil
}

func attachAnchorInspection(inspection RootInspection, anchorPath string, schemaMajor uint16) (RootInspection, error) {
	snapshot, err := loadFileAnchorSnapshot(anchorPath)
	if os.IsNotExist(err) {
		return inspection, nil
	}
	if err != nil {
		return RootInspection{}, err
	}
	if !snapshot.Initialized {
		return inspection, nil
	}
	if snapshot.DBUUID != inspection.DomainUUID || snapshot.SchemaMajor != schemaMajor {
		return RootInspection{}, fmt.Errorf("%w: anchor identity does not match repository", ErrAnchorInvariant)
	}
	inspection.AnchorPhase = snapshot.Phase
	if snapshot.Phase == "fail_stopped" {
		inspection.FailStopped = true
		inspection.FailStopReason = snapshot.Reason
	}
	return inspection, nil
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
