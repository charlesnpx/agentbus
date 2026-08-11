package authority

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
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
	initializeAdmissionRootBeforeRepositoryCreateForTest func() error
	initializeAdmissionRootAfterOpenForTest              func() error
	initializeAdmissionRootBeforeAnchorCreateForTest     func() error
	sealAdmissionRootAfterNewInitForTest                 func() error
)

var (
	ErrRootNotEmpty                      = errors.New("authority root is not empty")
	ErrRootBusy                          = errors.New("authority root busy")
	ErrSealConfirmationRequired          = errors.New("authority seal confirmation required")
	ErrClearFailStopConfirmationRequired = errors.New("authority clear fail-stop confirmation required")
	ErrRootHasRecoveryObligations        = errors.New("authority root has recovery obligations")
	ErrNewStateRootNotEmpty              = errors.New("authority new state root is not empty")
	ErrNewStateRootParentMissing         = errors.New("authority new state root parent missing")
	ErrNewStateRootPristineRetry         = errors.New("authority new state root pristine destination requires deletion before retry")
	ErrNewStateRootPartialCleanup        = errors.New("authority new state root partial cleanup failed")
	ErrSealedSuccessorMismatch           = errors.New("authority sealed successor mismatch")
)

type RootInspection struct {
	ActivationMetadata  AdmissionRootMetadata         `json:"activationMetadata"`
	ContractVersion     uint16                        `json:"contractVersion"`
	Generation          uint64                        `json:"generation"`
	Counts              repository.AuthorityRootStats `json:"counts"`
	DomainUUID          string                        `json:"domainUUID"`
	Sealed              bool                          `json:"sealed"`
	SuccessorDomainUUID string                        `json:"successorDomainUUID,omitempty"`
	SuccessorStateRoot  string                        `json:"successorStateRoot,omitempty"`
	AnchorPhase         string                        `json:"anchorPhase,omitempty"`
	FailStopped         bool                          `json:"failStopped"`
	FailStopReason      string                        `json:"failStopReason,omitempty"`
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

type PristineNewStateRootError struct {
	StateRoot  string
	Inspection RootInspection
}

func (e PristineNewStateRootError) Error() string {
	return fmt.Sprintf("%s: %s (delete this pristine destination and retry)", ErrNewStateRootPristineRetry, e.StateRoot)
}

func (e PristineNewStateRootError) Is(target error) bool {
	return target == ErrNewStateRootPristineRetry
}

type PartialNewStateRootCleanupError struct {
	StateRoot string
	Leftover  []string
	Cause     error
}

func (e PartialNewStateRootCleanupError) Error() string {
	message := fmt.Sprintf("%s: %s", ErrNewStateRootPartialCleanup, e.StateRoot)
	if len(e.Leftover) != 0 {
		message += ": leftover paths: " + strings.Join(e.Leftover, ", ")
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e PartialNewStateRootCleanupError) Is(target error) bool {
	return target == ErrNewStateRootPartialCleanup
}

func (e PartialNewStateRootCleanupError) Unwrap() error {
	return e.Cause
}

type NewStateRootParentMissingError struct {
	StateRoot string
	Parent    string
	Cause     error
}

func (e NewStateRootParentMissingError) Error() string {
	message := fmt.Sprintf("%s: parent directory of --new-state-root must already exist: %s", ErrNewStateRootParentMissing, e.Parent)
	if e.StateRoot != "" {
		message += fmt.Sprintf(" (new state root %s)", e.StateRoot)
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e NewStateRootParentMissingError) Is(target error) bool {
	return target == ErrNewStateRootParentMissing
}

func (e NewStateRootParentMissingError) Unwrap() error {
	return e.Cause
}

type SealedSuccessorMismatchError struct {
	StateRoot          string
	ExpectedDomainUUID string
	ActualDomainUUID   string
	Cause              error
}

func (e SealedSuccessorMismatchError) Error() string {
	message := fmt.Sprintf("%s: %s: expected successor domain UUID %q", ErrSealedSuccessorMismatch, e.StateRoot, e.ExpectedDomainUUID)
	if e.ActualDomainUUID != "" {
		message += fmt.Sprintf(", got %q", e.ActualDomainUUID)
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e SealedSuccessorMismatchError) Is(target error) bool {
	return target == ErrSealedSuccessorMismatch
}

func (e SealedSuccessorMismatchError) Unwrap() error {
	return e.Cause
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
	repo, err := openReadOnlyAdmissionRepository(ctx, stateRoot)
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
	return attachRequiredAnchorInspection(inspection, anchorPath, schemaMajor)
}

func InspectAdmissionRepository(ctx context.Context, repo repository.Repository) (RootInspection, error) {
	if repo == nil {
		return RootInspection{}, errors.New("authority repository is required")
	}
	if err := auditAdmissionRepository(ctx, repo); err != nil {
		return RootInspection{}, err
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
		inspection.SuccessorDomainUUID = meta.SuccessorDomainUUID
		inspection.SuccessorStateRoot = meta.SuccessorStateRoot
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
		oldRepo, err := bboltrepo.OpenExisting(repoPath, &bolt.Options{Timeout: adminOpenTimeout})
		if err != nil {
			if errors.Is(err, bolt.ErrTimeout) {
				return RootInspection{}, fmt.Errorf("%w: %w", ErrRootBusy, err)
			}
			return RootInspection{}, err
		}
		defer oldRepo.Close()
		dbUUID, schemaMajor, err := oldRepo.AnchorIdentity()
		if err != nil {
			return RootInspection{}, err
		}
		if err := requireOptionalResetAnchorMatch(anchorPath, dbUUID, schemaMajor); err != nil {
			return RootInspection{}, err
		}
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
		if _, anchorErr := os.Lstat(anchorPath); anchorErr == nil {
			return RootInspection{}, fmt.Errorf("%w: anchor exists without admission repository: %s", ErrAnchorInvariant, anchorPath)
		} else if !errors.Is(anchorErr, os.ErrNotExist) {
			return RootInspection{}, anchorErr
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
	repo, err := openWritableAdmissionRepository(ctx, stateRoot)
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
	snapshot, err := loadRequiredFileAnchorSnapshot(anchorPath, inspection.DomainUUID, schemaMajor)
	if err != nil {
		return ClearFailStopReport{}, err
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
	report.Inspection, err = attachRequiredAnchorInspection(report.Inspection, anchorPath, schemaMajor)
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
	repo, err := openWritableAdmissionRepository(ctx, stateRoot)
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
		newInspection, err := inspectSealedSuccessorDestination(ctx, newStateRoot, before.SuccessorDomainUUID)
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
	// Retry policy after a crash between durable new-root initialization and
	// old-root seal: the old root is still unsealed and serviceable, so a
	// pristine fresh destination is not reused implicitly. Delete the named
	// pristine destination and retry; already-sealed old-root replays accept
	// that same valid destination as an idempotent success.
	reservation, err := reserveSealDestination(newStateRoot)
	if err != nil {
		if errors.Is(err, ErrNewStateRootNotEmpty) {
			if inspection, inspectErr := inspectPristineFreshSealDestination(ctx, newStateRoot, CurrentAdmissionContractVersion); inspectErr == nil {
				return SealReport{OldRoot: stateRoot, OldInspection: before}, PristineNewStateRootError{StateRoot: newStateRoot, Inspection: inspection}
			}
		}
		return SealReport{OldRoot: stateRoot, OldInspection: before}, err
	}
	newInspection, err := initializeReservedAdmissionRoot(ctx, reservation, AdmissionRootMetadata{ContractVersion: CurrentAdmissionContractVersion})
	if err != nil {
		if cleanupErr := cleanupPartialAdmissionRoot(reservation); cleanupErr != nil {
			return SealReport{}, errors.Join(err, cleanupErr)
		}
		return SealReport{}, err
	}
	if sealAdmissionRootAfterNewInitForTest != nil {
		if err := sealAdmissionRootAfterNewInitForTest(); err != nil {
			return SealReport{
				OldRoot:       stateRoot,
				OldInspection: before,
				NewRoot:       newStateRoot,
				NewDomainUUID: newInspection.DomainUUID,
				NewInspection: newInspection,
			}, err
		}
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
	anchor := NewFileAnchor(anchorPath, before.DomainUUID, schemaMajor, WithFileAnchorRequireInitialized())
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
			if meta.SuccessorDomainUUID != newInspection.DomainUUID {
				return SealedSuccessorMismatchError{
					StateRoot:          newStateRoot,
					ExpectedDomainUUID: meta.SuccessorDomainUUID,
					ActualDomainUUID:   newInspection.DomainUUID,
				}
			}
			return nil
		}
		meta.Sealed = true
		meta.SuccessorDomainUUID = newInspection.DomainUUID
		meta.SuccessorStateRoot = newStateRoot
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
	oldInspection, err = attachRequiredAnchorInspection(oldInspection, anchorPath, schemaMajor)
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

func openReadOnlyAdmissionRepository(ctx context.Context, stateRoot string) (*bboltrepo.Repository, error) {
	repoPath, _, err := admissionRootPaths(stateRoot)
	if err != nil {
		return nil, err
	}
	repo, err := bboltrepo.OpenExistingReadOnly(repoPath, &bolt.Options{Timeout: adminOpenTimeout})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("%w: another process (normally the running daemon) holds the admission store at %s; stop the daemon before retrying this offline authority operation: %w", ErrRootBusy, repoPath, err)
		}
		return nil, err
	}
	if err := auditAdmissionRepository(ctx, repo); err != nil {
		_ = repo.Close()
		return nil, err
	}
	return repo, nil
}

func openWritableAdmissionRepository(ctx context.Context, stateRoot string) (*bboltrepo.Repository, error) {
	repoPath, _, err := admissionRootPaths(stateRoot)
	if err != nil {
		return nil, err
	}
	repo, err := bboltrepo.OpenExisting(repoPath, &bolt.Options{Timeout: adminOpenTimeout})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("%w: %w", ErrRootBusy, err)
		}
		return nil, err
	}
	if err := auditAdmissionRepository(ctx, repo); err != nil {
		_ = repo.Close()
		return nil, err
	}
	return repo, nil
}

func admissionRootPaths(stateRoot string) (string, string, error) {
	if strings.TrimSpace(stateRoot) == "" {
		return "", "", errors.New("state root is required")
	}
	return filepath.Join(stateRoot, AdmissionRepositoryFile), filepath.Join(stateRoot, AdmissionAnchorFile), nil
}

func initializeAdmissionRoot(ctx context.Context, stateRoot string, metadata AdmissionRootMetadata) (RootInspection, error) {
	return initializeAdmissionRootWithReservation(ctx, stateRoot, metadata, nil)
}

func inspectPristineFreshSealDestination(ctx context.Context, stateRoot string, contractVersion uint16) (RootInspection, error) {
	inspection, err := InspectAdmissionRoot(ctx, stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	if !inspection.Counts.Empty() {
		return RootInspection{}, RootNotEmptyError{Counts: inspection.Counts}
	}
	if inspection.Sealed {
		return RootInspection{}, fmt.Errorf("%w: destination is sealed", ErrNewStateRootNotEmpty)
	}
	if inspection.ActivationMetadata.Activated || inspection.ActivationMetadata.ActivatedAtGen != 0 {
		return RootInspection{}, fmt.Errorf("%w: destination is activated", ErrNewStateRootNotEmpty)
	}
	if inspection.ActivationMetadata.ContractVersion != contractVersion {
		return RootInspection{}, fmt.Errorf("%w: destination contract version %d does not match %d", ErrNewStateRootNotEmpty, inspection.ActivationMetadata.ContractVersion, contractVersion)
	}
	if inspection.AnchorPhase != "" || inspection.FailStopped {
		return RootInspection{}, fmt.Errorf("%w: destination anchor phase %q is not pristine", ErrNewStateRootNotEmpty, inspection.AnchorPhase)
	}
	if err := requireOnlyAdmissionRootFiles(stateRoot); err != nil {
		return RootInspection{}, err
	}
	return inspection, nil
}

func inspectSealedSuccessorDestination(ctx context.Context, stateRoot, expectedDomainUUID string) (RootInspection, error) {
	if strings.TrimSpace(expectedDomainUUID) == "" {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			Cause:              fmt.Errorf("sealed root has no persisted successor domain UUID"),
		}
	}
	repo, err := openReadOnlyAdmissionRepository(ctx, stateRoot)
	if err != nil {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			Cause:              err,
		}
	}
	defer repo.Close()
	inspection, err := InspectAdmissionRepository(ctx, repo)
	if err != nil {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			Cause:              err,
		}
	}
	if inspection.DomainUUID != expectedDomainUUID {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			ActualDomainUUID:   inspection.DomainUUID,
		}
	}
	_, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			Cause:              err,
		}
	}
	_, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			Cause:              err,
		}
	}
	anchor := &fileAnchor{
		path:               anchorPath,
		dbUUID:             inspection.DomainUUID,
		schemaMajor:        schemaMajor,
		requireInitialized: true,
	}
	probe, err := probeBeginStartability(ctx, repo, anchor)
	if err != nil {
		return RootInspection{}, SealedSuccessorMismatchError{
			StateRoot:          stateRoot,
			ExpectedDomainUUID: expectedDomainUUID,
			ActualDomainUUID:   inspection.DomainUUID,
			Cause:              err,
		}
	}
	inspection.AnchorPhase = probe.Anchor.Phase
	if probe.Anchor.Phase == "fail_stopped" {
		inspection.FailStopped = true
		inspection.FailStopReason = probe.Anchor.Reason
	}
	return inspection, nil
}

func requireOnlyAdmissionRootFiles(stateRoot string) error {
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		switch name {
		case AdmissionRepositoryFile, AdmissionAnchorFile:
			seen[name] = true
		default:
			return fmt.Errorf("%w: unexpected destination entry %s", ErrNewStateRootNotEmpty, filepath.Join(stateRoot, name))
		}
	}
	if !seen[AdmissionRepositoryFile] || !seen[AdmissionAnchorFile] {
		return fmt.Errorf("%w: destination is missing pristine admission files", ErrNewStateRootNotEmpty)
	}
	return nil
}

type admissionRootReservation struct {
	stateRoot string
	owned     map[string]struct{}
}

func reserveSealDestination(stateRoot string) (*admissionRootReservation, error) {
	if strings.TrimSpace(stateRoot) == "" {
		return nil, errors.New("new state root is required")
	}
	if err := os.Mkdir(stateRoot, 0o700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s already exists; fresh rotation destination must not exist", ErrNewStateRootNotEmpty, stateRoot)
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, NewStateRootParentMissingError{
				StateRoot: stateRoot,
				Parent:    filepath.Dir(stateRoot),
				Cause:     err,
			}
		}
		return nil, err
	}
	reservation := &admissionRootReservation{stateRoot: stateRoot, owned: map[string]struct{}{}}
	reservation.addOwned(stateRoot)
	return reservation, nil
}

func (r *admissionRootReservation) addOwned(path string) {
	if r == nil {
		return
	}
	r.owned[path] = struct{}{}
}

func (r *admissionRootReservation) requireOnlyOwnedEntries() error {
	if r == nil {
		return nil
	}
	entries, err := os.ReadDir(r.stateRoot)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		path := filepath.Join(r.stateRoot, entry.Name())
		if _, ok := r.owned[path]; !ok {
			return fmt.Errorf("%w: unexpected destination entry %s", ErrNewStateRootNotEmpty, path)
		}
		seen[path] = struct{}{}
	}
	for path := range r.owned {
		if path == r.stateRoot {
			continue
		}
		if _, ok := seen[path]; !ok {
			return fmt.Errorf("%w: destination is missing owned admission file %s", ErrNewStateRootNotEmpty, path)
		}
	}
	return nil
}

func initializeReservedAdmissionRoot(ctx context.Context, reservation *admissionRootReservation, metadata AdmissionRootMetadata) (RootInspection, error) {
	if reservation == nil {
		return RootInspection{}, errors.New("admission root reservation is required")
	}
	return initializeAdmissionRootWithReservation(ctx, reservation.stateRoot, metadata, reservation)
}

func initializeAdmissionRootWithReservation(ctx context.Context, stateRoot string, metadata AdmissionRootMetadata, reservation *admissionRootReservation) (RootInspection, error) {
	if metadata == (AdmissionRootMetadata{}) {
		metadata.ContractVersion = CurrentAdmissionContractVersion
	}
	repoPath, anchorPath, err := admissionRootPaths(stateRoot)
	if err != nil {
		return RootInspection{}, err
	}
	if reservation != nil {
		info, err := os.Stat(stateRoot)
		if err != nil {
			return RootInspection{}, err
		}
		if !info.IsDir() {
			return RootInspection{}, fmt.Errorf("%w: reserved destination is not a directory: %s", ErrNewStateRootNotEmpty, stateRoot)
		}
	} else if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return RootInspection{}, err
	}
	if reservation != nil {
		if initializeAdmissionRootBeforeRepositoryCreateForTest != nil {
			if err := initializeAdmissionRootBeforeRepositoryCreateForTest(); err != nil {
				return RootInspection{}, err
			}
		}
	}
	repo, err := bboltrepo.Create(repoPath, &bolt.Options{Timeout: adminOpenTimeout})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return RootInspection{}, fmt.Errorf("%w: %w", ErrRootBusy, err)
		}
		var exists bboltrepo.RepositoryAlreadyExistsError
		if reservation != nil && errors.As(err, &exists) {
			return RootInspection{}, foreignDestinationPathError(repoPath)
		}
		return RootInspection{}, err
	}
	if reservation != nil {
		reservation.addOwned(repoPath)
	}
	defer repo.Close()
	if err := auditAdmissionRepository(ctx, repo); err != nil {
		return RootInspection{}, err
	}
	if initializeAdmissionRootAfterOpenForTest != nil {
		if err := initializeAdmissionRootAfterOpenForTest(); err != nil {
			return RootInspection{}, err
		}
	}
	var currentMetadata AdmissionRootMetadata
	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		metaRecord := tx.Meta()
		if metaRecord.State != repository.RecordValid {
			return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, metaRecord.State)
		}
		currentMetadata = metaRecord.Value.AdmissionRoot
		return nil
	}); err != nil {
		return RootInspection{}, err
	}
	if currentMetadata != metadata {
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
	if reservation != nil {
		if err := reservation.requireOnlyOwnedEntries(); err != nil {
			return RootInspection{}, err
		}
		if initializeAdmissionRootBeforeAnchorCreateForTest != nil {
			if err := initializeAdmissionRootBeforeAnchorCreateForTest(); err != nil {
				return RootInspection{}, err
			}
		}
	}
	snapshot := AnchorSnapshot{
		Initialized: true,
		DBUUID:      inspection.DomainUUID,
		SchemaMajor: schemaMajor,
		Generation:  inspection.Generation,
	}
	if reservation != nil {
		if err := saveReservedFileAnchorSnapshot(reservation, anchorPath, snapshot); err != nil {
			return RootInspection{}, err
		}
	} else if err := saveFileAnchorSnapshot(anchorPath, snapshot); err != nil {
		return RootInspection{}, err
	}
	if reservation != nil {
		if err := reservation.requireOnlyOwnedEntries(); err != nil {
			return RootInspection{}, err
		}
	}
	return attachRequiredAnchorInspection(inspection, anchorPath, schemaMajor)
}

func saveReservedFileAnchorSnapshot(reservation *admissionRootReservation, anchorPath string, snapshot AnchorSnapshot) error {
	if reservation == nil {
		return errors.New("admission root reservation is required")
	}
	created, err := saveNewFileAnchorSnapshotExclusive(anchorPath, snapshot)
	if created {
		reservation.addOwned(anchorPath)
	}
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return foreignDestinationPathError(anchorPath)
		}
		return err
	}
	return nil
}

func foreignDestinationPathError(path string) error {
	return fmt.Errorf("%w: foreign destination path already exists: %s", ErrNewStateRootNotEmpty, path)
}

func cleanupPartialAdmissionRoot(reservation *admissionRootReservation) error {
	if reservation == nil {
		return nil
	}
	candidates := make([]string, 0, len(reservation.owned))
	for path := range reservation.owned {
		if path != reservation.stateRoot {
			candidates = append(candidates, path)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(candidates)))
	var cause error
	seen := make(map[string]struct{}, len(candidates))
	for _, path := range candidates {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cause = errors.Join(cause, fmt.Errorf("%s: %w", path, err))
		}
	}
	if err := os.Remove(reservation.stateRoot); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ENOTEMPTY) {
		cause = errors.Join(cause, fmt.Errorf("%s: %w", reservation.stateRoot, err))
	}
	leftovers := ownedStateRootLeftovers(reservation)
	if cause != nil || len(leftovers) != 0 {
		return PartialNewStateRootCleanupError{StateRoot: reservation.stateRoot, Leftover: leftovers, Cause: cause}
	}
	return nil
}

func ownedStateRootLeftovers(reservation *admissionRootReservation) []string {
	var leftovers []string
	for path := range reservation.owned {
		if path == reservation.stateRoot {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			leftovers = append(leftovers, path)
		} else if !errors.Is(err, os.ErrNotExist) {
			leftovers = append(leftovers, path)
		}
	}
	sort.Strings(leftovers)
	return leftovers
}

func attachRequiredAnchorInspection(inspection RootInspection, anchorPath string, schemaMajor uint16) (RootInspection, error) {
	snapshot, err := loadRequiredFileAnchorSnapshot(anchorPath, inspection.DomainUUID, schemaMajor)
	if err != nil {
		return RootInspection{}, err
	}
	return attachAnchorSnapshot(inspection, snapshot), nil
}

func loadRequiredFileAnchorSnapshot(anchorPath, dbUUID string, schemaMajor uint16) (AnchorSnapshot, error) {
	snapshot, err := loadFileAnchorSnapshot(anchorPath)
	if err != nil {
		return AnchorSnapshot{}, err
	}
	if !snapshot.Initialized {
		return AnchorSnapshot{}, fmt.Errorf("%w: anchor is missing: %s", ErrAnchorInvariant, anchorPath)
	}
	if snapshot.DBUUID != dbUUID || snapshot.SchemaMajor != schemaMajor {
		return AnchorSnapshot{}, fmt.Errorf("%w: anchor identity does not match repository", ErrAnchorInvariant)
	}
	return snapshot, nil
}

func requireOptionalResetAnchorMatch(anchorPath, dbUUID string, schemaMajor uint16) error {
	snapshot, err := loadFileAnchorSnapshot(anchorPath)
	if err != nil {
		return err
	}
	if !snapshot.Initialized {
		return nil
	}
	if snapshot.DBUUID != dbUUID || snapshot.SchemaMajor != schemaMajor {
		return fmt.Errorf("%w: anchor identity does not match repository", ErrAnchorInvariant)
	}
	return nil
}

func attachAnchorSnapshot(inspection RootInspection, snapshot AnchorSnapshot) RootInspection {
	inspection.AnchorPhase = snapshot.Phase
	if snapshot.Phase == "fail_stopped" {
		inspection.FailStopped = true
		inspection.FailStopReason = snapshot.Reason
	}
	return inspection
}

func auditAdmissionRepository(ctx context.Context, repo repository.Repository) error {
	auditor, ok := repo.(repository.Auditor)
	if !ok {
		return nil
	}
	return auditor.AuditIntegrity(ctx)
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
