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
	maxPruneDetails           = 128
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
	if store, err := s.ensureJobStore(); err != nil {
		logPruneError(err)
	} else if _, err := s.automaticPrunePass(ctx, store); err != nil && ctx.Err() == nil {
		logPruneError(err)
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
			if _, err := s.automaticPrunePass(ctx, store); err != nil && ctx.Err() == nil {
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
	return s.automaticPrunePass(context.Background(), store)
}

func (s *Server) automaticPrunePass(ctx context.Context, store *jobstore.Store) (protocol.ArtifactsPruneResult, error) {
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
	result := emptyPruneResult(false, defaultSuccessfulPruneAge, defaultFailedPruneAge)
	after := ""
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		records, next, more, err := store.ListPage(autoPruneMaxJobsPerSweep, after)
		if err != nil {
			return result, err
		}
		page, err := s.pruneArtifactRecords(store, root, records, false, defaultSuccessfulPruneAge, defaultFailedPruneAge)
		if err != nil {
			return result, err
		}
		mergePruneDetails(&result, page)
		result.RemovedBytes += page.RemovedBytes
		if !more {
			return result, nil
		}
		after = next
	}
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
			addPruneSkip(&result, protocol.PruneSkip{JobID: record.JobID, Reason: reason})
			continue
		}
		if logsFD >= 0 {
			s.pruneJobLogs(store, root, logsFD, record, dryRun, &result)
		} else if errors.Is(logsErr, unix.ENOENT) && !dryRun && !record.LogsPruned {
			if _, err := store.MarkLogsPruned(record.JobID); err != nil {
				addPruneSkip(&result, protocol.PruneSkip{JobID: record.JobID, Reason: "record pruned logs: " + err.Error()})
			}
		} else if logsErr != nil && !errors.Is(logsErr, unix.ENOENT) {
			addPruneSkip(&result, protocol.PruneSkip{
				JobID:  record.JobID,
				Path:   filepath.Join(root, "artifacts", "logs"),
				Reason: "open owned log directory: " + logsErr.Error(),
			})
		}
	}
	return result, nil
}

// The pass visits every record, but its response must fit in a bounded RPC
// frame. Counts and byte totals still describe the complete pass.
func addPruneItem(result *protocol.ArtifactsPruneResult, item protocol.PruneItem, dryRun bool) {
	item.Path = boundProviderValue(item.Path, 1024)
	if dryRun {
		result.WouldRemoveCount++
		if len(result.WouldRemove) < maxPruneDetails {
			result.WouldRemove = append(result.WouldRemove, item)
		} else {
			result.DetailsTruncated = true
		}
		return
	}
	result.RemovedCount++
	if len(result.Removed) < maxPruneDetails {
		result.Removed = append(result.Removed, item)
	} else {
		result.DetailsTruncated = true
	}
}

func addPruneSkip(result *protocol.ArtifactsPruneResult, skip protocol.PruneSkip) {
	result.SkippedCount++
	if len(result.Skipped) >= maxPruneDetails {
		result.DetailsTruncated = true
		return
	}
	skip.Path = boundProviderValue(skip.Path, 1024)
	skip.Reason = boundProviderValue(skip.Reason, 512)
	result.Skipped = append(result.Skipped, skip)
}

func mergePruneDetails(result *protocol.ArtifactsPruneResult, page protocol.ArtifactsPruneResult) {
	for _, item := range page.Removed {
		if len(result.Removed) < maxPruneDetails {
			result.Removed = append(result.Removed, item)
		}
	}
	for _, skip := range page.Skipped {
		if len(result.Skipped) < maxPruneDetails {
			result.Skipped = append(result.Skipped, skip)
		}
	}
	result.RemovedCount += page.RemovedCount
	result.SkippedCount += page.SkippedCount
	result.DetailsTruncated = result.DetailsTruncated || page.DetailsTruncated || result.RemovedCount > len(result.Removed) || result.SkippedCount > len(result.Skipped)
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
		addPruneSkip(result, protocol.PruneSkip{JobID: record.JobID, Reason: "invalid job log identity: " + err.Error()})
		return
	}
	itemPath, err := itemSidecarPath(base.Stdout)
	if err != nil {
		addPruneSkip(result, protocol.PruneSkip{JobID: record.JobID, Reason: "resolve transcript sidecar: " + err.Error()})
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
			addPruneSkip(result, protocol.PruneSkip{JobID: record.JobID, Path: candidate.path, Reason: statErr.Error()})
			continue
		}
		item := protocol.PruneItem{JobID: record.JobID, Kind: candidate.kind, Path: candidate.path, Bytes: size}
		if dryRun {
			addPruneItem(result, item, true)
			result.WouldRemoveBytes += size
			continue
		}
		presentSidecars = append(presentSidecars, presentSidecar{kind: candidate.kind, path: candidate.path, name: candidate.name})
	}
	if dryRun {
		return
	}
	if !record.LogsPruned {
		if _, err := store.MarkLogsPruned(record.JobID); err != nil {
			addPruneSkip(result, protocol.PruneSkip{JobID: record.JobID, Reason: "record pruned logs: " + err.Error()})
			return
		}
	}
	for _, sidecar := range presentSidecars {
		removedBytes, removeErr := removeManagedRegularFile(logsFD, sidecar.name)
		if errors.Is(removeErr, unix.ENOENT) {
			continue
		}
		if removeErr != nil {
			addPruneSkip(result, protocol.PruneSkip{JobID: record.JobID, Path: sidecar.path, Reason: removeErr.Error()})
			continue
		}
		item := protocol.PruneItem{JobID: record.JobID, Kind: sidecar.kind, Path: sidecar.path, Bytes: removedBytes}
		addPruneItem(result, item, false)
		result.RemovedBytes += removedBytes
	}
}
