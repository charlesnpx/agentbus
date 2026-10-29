//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	defaultSuccessfulPruneAge = 7 * 24 * time.Hour
	defaultFailedPruneAge     = 30 * 24 * time.Hour
	autoPruneMaxJobsPerSweep  = 64
	maxPruneArtifactEntries   = 10_000
)

func (s *Server) handleArtifactsPrune(raw json.RawMessage) requestOutcome {
	var params protocol.ArtifactsPruneParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	successfulAge, err := parsePruneAge(params.SuccessfulOlderThan, defaultSuccessfulPruneAge)
	if err != nil {
		return invalidParams(fmt.Errorf("successfulOlderThan: %w", err))
	}
	failedAge, err := parsePruneAge(params.FailedOlderThan, defaultFailedPruneAge)
	if err != nil {
		return invalidParams(fmt.Errorf("failedOlderThan: %w", err))
	}
	dryRun := true
	if params.DryRun != nil {
		dryRun = *params.DryRun
	}
	store, err := s.ensureJobStore()
	if err != nil {
		return jobStoreUnavailable("open job store", err)
	}
	result, err := s.pruneArtifacts(store, dryRun, successfulAge, failedAge)
	if err != nil {
		return jobStoreUnavailable("prune artifacts", err)
	}
	return requestOutcome{result: result}
}

func parsePruneAge(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("must be a duration such as 168h")
	}
	age, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("must be a duration such as 168h: %w", err)
	}
	if age <= 0 {
		return 0, errors.New("must be greater than zero")
	}
	return age, nil
}

func (s *Server) automaticPruneLoop(ctx context.Context) {
	if s == nil || s.pruneInterval <= 0 {
		return
	}
	ticker := time.NewTicker(s.pruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			store, err := s.ensureJobStore()
			if err != nil {
				logPruneError(err)
				continue
			}
			if _, err := s.automaticPruneSweep(store); err != nil {
				logPruneError(err)
			}
		}
	}
}

func logPruneError(err error) {
	if err != nil {
		log.Printf("agentbus daemon: artifact retention sweep: %v", err)
	}
}

func (s *Server) pruneArtifacts(store *jobstore.Store, dryRun bool, successfulAge, failedAge time.Duration) (protocol.ArtifactsPruneResult, error) {
	if s == nil || store == nil {
		return protocol.ArtifactsPruneResult{}, errors.New("prune service or job store is unavailable")
	}
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()

	root, err := filepath.Abs(s.stateRoot)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, fmt.Errorf("resolve state root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, fmt.Errorf("evaluate state root: %w", err)
	}
	records, err := store.List()
	if err != nil {
		return protocol.ArtifactsPruneResult{}, err
	}
	return s.pruneArtifactRecords(store, root, records, dryRun, successfulAge, failedAge)
}

func (s *Server) automaticPruneSweep(store *jobstore.Store) (protocol.ArtifactsPruneResult, error) {
	if s == nil || store == nil {
		return protocol.ArtifactsPruneResult{}, errors.New("prune service or job store is unavailable")
	}
	s.pruneMu.Lock()
	defer s.pruneMu.Unlock()
	root, err := filepath.Abs(s.stateRoot)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, fmt.Errorf("resolve state root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, fmt.Errorf("evaluate state root: %w", err)
	}
	records, next, more, err := store.ListPage(autoPruneMaxJobsPerSweep, s.pruneApplyCursor)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, err
	}
	result, err := s.pruneArtifactRecords(store, root, records, false, defaultSuccessfulPruneAge, defaultFailedPruneAge)
	if err != nil {
		return protocol.ArtifactsPruneResult{}, err
	}
	if more {
		s.pruneApplyCursor = next
	} else {
		s.pruneApplyCursor = ""
	}
	return result, nil
}

func (s *Server) pruneArtifactRecords(store *jobstore.Store, root string, records []jobstore.Record, dryRun bool, successfulAge, failedAge time.Duration) (protocol.ArtifactsPruneResult, error) {
	result := emptyPruneResult(dryRun, successfulAge, failedAge)
	logsFD, logsErr := openExistingManagedDirectory(root, "artifacts", "logs")
	if logsErr == nil {
		defer unix.Close(logsFD)
	} else if !errors.Is(logsErr, unix.ENOENT) {
		logsFD = -1
	}
	now := time.Now().UTC()
	for _, record := range records {
		if reason := s.pruneSkipReason(record, now, successfulAge, failedAge); reason != "" {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Reason: reason})
			continue
		}
		if logsFD >= 0 {
			s.pruneJobLogs(store, root, logsFD, record, dryRun, &result)
		} else if logsErr != nil && !errors.Is(logsErr, unix.ENOENT) {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{
				JobID:  record.JobID,
				Path:   filepath.Join(root, "artifacts", "logs"),
				Reason: "open owned log directory: " + logsErr.Error(),
			})
		}
		if record.Backend == "codex" && record.Write {
			s.pruneJobCodexArtifacts(root, record, dryRun, &result)
		}
	}
	return result, nil
}

func emptyPruneResult(dryRun bool, successfulAge, failedAge time.Duration) protocol.ArtifactsPruneResult {
	return protocol.ArtifactsPruneResult{
		DryRun:              dryRun,
		SuccessfulOlderThan: successfulAge.String(),
		FailedOlderThan:     failedAge.String(),
		WouldRemove:         make([]protocol.PruneItem, 0),
		Removed:             make([]protocol.PruneItem, 0),
		Skipped:             make([]protocol.PruneSkip, 0),
	}
}

func (s *Server) pruneSkipReason(record jobstore.Record, now time.Time, successfulAge, failedAge time.Duration) string {
	if s.activeExecution(record.JobID) != nil {
		return "job still has an active execution"
	}
	if !record.State.IsTerminal() {
		return "job is active or queued"
	}
	if record.Cleanup != protocol.CleanupClean {
		return "cleanup is uncertain"
	}
	if record.RetainSession {
		return "session is retained"
	}
	if record.State != protocol.PublicStateCompleted && record.BackendSessionID != "" {
		return "backend session is resumable"
	}
	if record.FinishedAt == nil {
		return "terminal job has no finish time"
	}
	threshold := failedAge
	if record.State == protocol.PublicStateCompleted {
		threshold = successfulAge
	}
	if now.Before(record.FinishedAt.Add(threshold)) {
		return fmt.Sprintf("job is younger than the %s retention threshold", threshold)
	}
	return ""
}

func (s *Server) pruneJobLogs(store *jobstore.Store, root string, logsFD int, record jobstore.Record, dryRun bool, result *protocol.ArtifactsPruneResult) {
	base, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Join(root, "artifacts", "logs")}, record.JobID)
	if err != nil {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Reason: "invalid job log identity: " + err.Error()})
		return
	}
	expectedLog := filepath.Join(root, "artifacts", "logs", record.JobID+".log")
	recordedDirectory, directoryErr := filepath.EvalSymlinks(filepath.Dir(record.Artifacts.Log))
	if directoryErr != nil || filepath.Base(filepath.Clean(record.Artifacts.Log)) != record.JobID+".log" || filepath.Clean(recordedDirectory) != filepath.Dir(expectedLog) {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{
			JobID:  record.JobID,
			Path:   record.Artifacts.Log,
			Reason: "recorded log path is outside the Agentbus artifact layout",
		})
		return
	}
	itemPath, err := itemSidecarPath(base.Stdout)
	if err != nil {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Reason: "resolve transcript sidecar: " + err.Error()})
		return
	}
	paths := []struct {
		kind string
		path string
		name string
	}{
		{kind: "transcript", path: itemPath, name: filepath.Base(itemPath)},
		{kind: "log", path: base.Stdout, name: filepath.Base(base.Stdout)},
		{kind: "log", path: base.Stderr, name: filepath.Base(base.Stderr)},
	}
	type presentSidecar struct {
		kind string
		path string
		name string
	}
	presentSidecars := make([]presentSidecar, 0, len(paths))
	for _, candidate := range paths {
		size, filePresent, statErr := managedRegularFileSize(logsFD, candidate.name)
		if errors.Is(statErr, unix.ENOENT) || (!filePresent && statErr == nil) {
			continue
		}
		if statErr != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: candidate.path, Reason: statErr.Error()})
			continue
		}
		item := protocol.PruneItem{JobID: record.JobID, Kind: candidate.kind, Path: candidate.path, Bytes: size}
		if dryRun {
			result.WouldRemove = append(result.WouldRemove, item)
			result.WouldRemoveBytes += size
			continue
		}
		presentSidecars = append(presentSidecars, presentSidecar{kind: candidate.kind, path: candidate.path, name: candidate.name})
	}
	if dryRun || len(presentSidecars) == 0 {
		return
	}
	if !record.LogsPruned {
		if _, err := store.MarkLogsPruned(record.JobID); err != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Reason: "record pruned logs: " + err.Error()})
			return
		}
	}
	for _, sidecar := range presentSidecars {
		removedBytes, removeErr := removeManagedRegularFile(logsFD, sidecar.name)
		if errors.Is(removeErr, unix.ENOENT) {
			continue
		}
		if removeErr != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: sidecar.path, Reason: removeErr.Error()})
			continue
		}
		item := protocol.PruneItem{JobID: record.JobID, Kind: sidecar.kind, Path: sidecar.path, Bytes: removedBytes}
		result.Removed = append(result.Removed, item)
		result.RemovedBytes += removedBytes
	}
}

func (s *Server) pruneJobCodexArtifacts(root string, record jobstore.Record, dryRun bool, result *protocol.ArtifactsPruneResult) {
	if strings.TrimSpace(record.CWD) == "" {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Reason: "recorded workspace path is unavailable"})
		return
	}
	workspaceKey := engine.WorkspaceKey(record.CWD)
	layout := engine.WorkspaceLayout{Root: root, Key: workspaceKey}
	parentFD, err := openExistingManagedCodexHomeParent(layout)
	if errors.Is(err, unix.ENOENT) {
		return
	}
	if err != nil {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: filepath.Join(root, "workspaces", workspaceKey, "codex"), Reason: "open managed Codex home root: " + err.Error()})
		return
	}
	defer unix.Close(parentFD)
	homeFD, err := openManagedDirectoryAt(parentFD, record.JobID)
	if errors.Is(err, unix.ENOENT) {
		return
	}
	if err != nil {
		result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: filepath.Join(root, "workspaces", workspaceKey, "codex", record.JobID), Reason: "managed Codex home is not an owned directory: " + err.Error()})
		return
	}
	defer unix.Close(homeFD)
	for _, name := range []string{"cache", "tmp", "src"} {
		path := filepath.Join(root, "workspaces", workspaceKey, "codex", record.JobID, name)
		var stat unix.Stat_t
		if err := unix.Fstatat(homeFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
			continue
		} else if err != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: path, Reason: err.Error()})
			continue
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: path, Reason: "managed artifact path is not a real directory"})
			continue
		}
		directoryFD, err := openManagedDirectoryAt(homeFD, name)
		if err != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: path, Reason: "open managed cache directory: " + err.Error()})
			continue
		}
		remaining := maxPruneArtifactEntries
		size, measureErr := measureManagedCodexHomeContents(directoryFD, &remaining)
		closeErr := unix.Close(directoryFD)
		if measureErr != nil || closeErr != nil {
			reason := errors.Join(measureErr, closeErr)
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: path, Reason: "measure managed cache directory: " + reason.Error()})
			continue
		}
		item := protocol.PruneItem{JobID: record.JobID, Kind: name, Path: path, Bytes: size}
		if dryRun {
			result.WouldRemove = append(result.WouldRemove, item)
			result.WouldRemoveBytes += size
			continue
		}
		if err := removeManagedCodexHomeChild(homeFD, name); err != nil {
			result.Skipped = append(result.Skipped, protocol.PruneSkip{JobID: record.JobID, Path: path, Reason: err.Error()})
			continue
		}
		result.Removed = append(result.Removed, item)
		result.RemovedBytes += size
	}
}
