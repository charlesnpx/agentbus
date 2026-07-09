package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Clock provides deterministic time in tests.
type Clock interface {
	Now() time.Time
}

// ClockFunc adapts a function to Clock.
type ClockFunc func() time.Time

// Now returns f().
func (f ClockFunc) Now() time.Time { return f() }

// RetentionConfig controls reaper garbage collection.
type RetentionConfig struct {
	TerminalJobTTL time.Duration
	ResultTTL      time.Duration
	StaleJobAfter  time.Duration
}

// DefaultRetention returns protocol default retention settings.
func DefaultRetention() RetentionConfig {
	return RetentionConfig{
		TerminalJobTTL: 14 * 24 * time.Hour,
		ResultTTL:      14 * 24 * time.Hour,
		StaleJobAfter:  30 * time.Minute,
	}
}

// StoreConfig configures a workspace job store.
type StoreConfig struct {
	Root      string
	CWD       string
	Clock     Clock
	Processes ProcessTable
	Retention RetentionConfig
}

// Store persists job records for one workspace namespace.
type Store struct {
	layout    WorkspaceLayout
	clock     Clock
	processes ProcessTable
	retention RetentionConfig
}

// NewStore creates a state store and ensures protocol directories exist.
func NewStore(cfg StoreConfig) (*Store, error) {
	root := cfg.Root
	var err error
	if root == "" {
		root, err = ResolveStateRoot()
		if err != nil {
			return nil, err
		}
	}
	cwd := cfg.CWD
	if cwd == "" {
		cwd, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	layout, err := LayoutForWorkspace(root, cwd)
	if err != nil {
		return nil, err
	}
	if err := ensureLayout(layout); err != nil {
		return nil, err
	}
	clock := cfg.Clock
	if clock == nil {
		clock = ClockFunc(time.Now)
	}
	processes := cfg.Processes
	if processes == nil {
		processes = NativeProcessTable{}
	}
	retention := cfg.Retention
	defaults := DefaultRetention()
	if retention.TerminalJobTTL == 0 {
		retention.TerminalJobTTL = defaults.TerminalJobTTL
	}
	if retention.ResultTTL == 0 {
		retention.ResultTTL = defaults.ResultTTL
	}
	if retention.StaleJobAfter == 0 {
		retention.StaleJobAfter = defaults.StaleJobAfter
	}
	return &Store{layout: layout, clock: clock, processes: processes, retention: retention}, nil
}

// Layout returns the workspace layout used by the store.
func (s *Store) Layout() WorkspaceLayout { return s.layout }

// Save writes a job record atomically.
func (s *Store) Save(record *JobRecord) error {
	if record == nil {
		return errors.New("nil job record")
	}
	if err := validateJobID(record.JobID); err != nil {
		return err
	}
	return s.withJobLock(record.JobID, func() error {
		path, err := s.jobPath(record.JobID)
		if err != nil {
			return err
		}
		now := s.clock.Now().UTC()
		if record.CreatedAt.IsZero() {
			record.CreatedAt = now
		}
		if record.UpdatedAt.IsZero() {
			record.UpdatedAt = now
		}
		record.StatePath = path
		b, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return err
		}
		b = append(b, '\n')
		return atomicWriteFile(record.StatePath, b, 0o600)
	})
}

// Load reads a job record and computes status-only lease fields.
func (s *Store) Load(jobID string) (*JobRecord, error) {
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	if err := s.Reap(); err != nil {
		return nil, err
	}
	path, err := s.jobPath(jobID)
	if err != nil {
		return nil, err
	}
	return s.loadPath(path)
}

// List runs the reaper and loads all non-corrupt job records.
func (s *Store) List() ([]JobRecord, error) {
	if err := s.Reap(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.layout.Jobs)
	if err != nil {
		return nil, err
	}
	var out []JobRecord
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.layout.Jobs, entry.Name())
		record, err := s.loadPath(path)
		if err != nil {
			if qerr := s.quarantine(path, err); qerr != nil {
				return nil, qerr
			}
			continue
		}
		out = append(out, *record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out, nil
}

// Reap scans records, quarantines corrupt files, finalizes orphaned work, and runs GC.
func (s *Store) Reap() error {
	now := s.clock.Now().UTC()
	entries, err := os.ReadDir(s.layout.Jobs)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.layout.Jobs, entry.Name())
		jobID := strings.TrimSuffix(entry.Name(), ".json")
		if err := validateJobID(jobID); err != nil {
			if qerr := s.quarantine(path, err); qerr != nil {
				return qerr
			}
			continue
		}
		if err := s.withJobLock(jobID, func() error {
			record, original, err := s.loadPathWithBytes(path)
			if err != nil {
				if qerr := s.quarantine(path, err); qerr != nil {
					return qerr
				}
				return nil
			}
			changed, err := s.reapRecord(record, now)
			if err != nil {
				return err
			}
			if changed {
				if err := s.saveIfUnchanged(record, path, original); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return s.gc(now)
}

func (s *Store) reapRecord(record *JobRecord, now time.Time) (bool, error) {
	switch record.State {
	case StateOrphaned:
		return true, record.Transition(StateReaped, now)
	case StateQueued, StateStarting:
		if !record.UpdatedAt.IsZero() && now.Sub(record.UpdatedAt) >= s.retention.StaleJobAfter {
			if err := record.Transition(StateOrphaned, now); err != nil {
				return false, err
			}
			return true, record.Transition(StateReaped, now)
		}
	case StateRunning, StateRetrying:
		if !record.Lease.ExpiresAt.IsZero() && !now.Before(record.Lease.ExpiresAt) {
			return true, record.Transition(StateOrphaned, now)
		}
		if s.processGoneOrReused(record.Worker) {
			return true, record.Transition(StateOrphaned, now)
		}
		if s.processGoneOrReused(record.Supervisor) {
			return true, record.Transition(StateOrphaned, now)
		}
		if s.processMissing(record.BackendChildPID) {
			return true, record.Transition(StateOrphaned, now)
		}
	}
	return false, nil
}

func (s *Store) processGoneOrReused(ref ProcessRef) bool {
	if ref.PID <= 0 {
		return false
	}
	info, alive, err := s.processes.Lookup(ref.PID)
	if err != nil || !alive {
		return true
	}
	return ref.StartTime != "" && info.StartTime != "" && ref.StartTime != info.StartTime
}

func (s *Store) processMissing(pid int) bool {
	if pid <= 0 {
		return false
	}
	_, alive, err := s.processes.Lookup(pid)
	return err != nil || !alive
}

func (s *Store) quarantine(path string, cause error) error {
	base := filepath.Base(path)
	stamp := s.clock.Now().UTC().Format("20060102T150405.000000000Z")
	target := filepath.Join(s.layout.Quarantine, stamp+"-"+base)
	if err := os.Rename(path, target); err != nil {
		return err
	}
	diag := []byte(fmt.Sprintf("recordPath: %s\nfailure: %v\n", path, cause))
	if err := atomicWriteFile(target+".diagnostic.txt", diag, 0o600); err != nil {
		return err
	}
	return fsyncDir(s.layout.Quarantine)
}

func (s *Store) gc(now time.Time) error {
	entries, err := os.ReadDir(s.layout.Jobs)
	if err != nil {
		return err
	}
	protectedResults := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.layout.Jobs, entry.Name())
		jobID := strings.TrimSuffix(entry.Name(), ".json")
		if err := validateJobID(jobID); err != nil {
			continue
		}
		record, err := s.loadPath(path)
		if err != nil {
			continue
		}
		if record.Result != nil && record.Result.ResultPath != "" {
			protectedResults[filepath.Clean(record.Result.ResultPath)] = struct{}{}
		}
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.layout.Jobs, entry.Name())
		jobID := strings.TrimSuffix(entry.Name(), ".json")
		if err := validateJobID(jobID); err != nil {
			continue
		}
		if err := s.withJobLock(jobID, func() error {
			record, err := s.loadPath(path)
			if err != nil {
				return nil
			}
			if !IsTerminal(record.State) || now.Sub(record.UpdatedAt) < s.retention.TerminalJobTTL {
				return nil
			}
			if record.Result != nil && record.Result.ResultPath != "" {
				resultPath := filepath.Clean(record.Result.ResultPath)
				delete(protectedResults, resultPath)
				if pathWithinDir(s.layout.Results, resultPath) {
					_ = removeIfExists(resultPath)
				}
			}
			_ = removeContainedIfExists(s.layout.Logs, record.LogPaths.Stdout)
			_ = removeContainedIfExists(s.layout.Logs, record.LogPaths.Stderr)
			if inputPath, err := safePathForID(s.layout.Inputs, record.JobID, ".json"); err == nil {
				_ = removeIfExists(inputPath)
			}
			_ = removeIfExists(record.StatePath)
			return nil
		}); err != nil {
			return err
		}
	}
	for _, dir := range []string{s.layout.Results} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			path := filepath.Join(dir, entry.Name())
			if _, ok := protectedResults[filepath.Clean(path)]; ok {
				continue
			}
			if now.Sub(info.ModTime()) >= s.retention.ResultTTL {
				_ = os.Remove(path)
			}
		}
	}
	return nil
}

func (s *Store) loadPath(path string) (*JobRecord, error) {
	record, _, err := s.loadPathWithBytes(path)
	return record, err
}

func (s *Store) loadPathWithBytes(path string) (*JobRecord, []byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var record JobRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, nil, err
	}
	if record.JobID == "" || record.State == "" {
		return nil, nil, errors.New("invalid job record: missing jobId or state")
	}
	if err := validateJobID(record.JobID); err != nil {
		return nil, nil, err
	}
	expected := strings.TrimSuffix(filepath.Base(path), ".json")
	if record.JobID != expected {
		return nil, nil, fmt.Errorf("invalid job record: jobId %q does not match path %q", record.JobID, filepath.Base(path))
	}
	record.StatePath = path
	status := record.StatusRecord(s.clock.Now().UTC())
	return &status, b, nil
}

func (s *Store) saveIfUnchanged(record *JobRecord, path string, original []byte) error {
	current, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(current, original) {
		return nil
	}
	record.StatePath = path
	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return atomicWriteFile(path, b, 0o600)
}

func (s *Store) withJobLock(jobID string, fn func() error) error {
	lockPath, err := safePathForID(s.layout.Jobs, jobID, ".lock")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

func (s *Store) jobPath(jobID string) (string, error) {
	return safePathForID(s.layout.Jobs, jobID, ".json")
}

func (s *Store) resultPath(jobID string) (string, error) {
	return safePathForID(s.layout.Results, jobID, ".txt")
}

func safePathForID(dir, id, ext string) (string, error) {
	if err := validateJobID(id); err != nil {
		return "", err
	}
	path := filepath.Join(dir, id+ext)
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return "", err
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("job id %q escapes state namespace", id)
	}
	return path, nil
}

func validateJobID(jobID string) error {
	if !strings.HasPrefix(jobID, "job_") || len(jobID) <= len("job_") || len(jobID) > 128 {
		return fmt.Errorf("invalid job id %q", jobID)
	}
	for _, r := range jobID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			continue
		}
		return fmt.Errorf("invalid job id %q", jobID)
	}
	return nil
}

func pathWithinDir(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && !filepath.IsAbs(rel)
}

func removeIfExists(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func removeContainedIfExists(dir, path string) error {
	if path == "" {
		return nil
	}
	clean := filepath.Clean(path)
	if !pathWithinDir(dir, clean) {
		return nil
	}
	return removeIfExists(clean)
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	atomicWriteFileCrashHook("after-temp-sync", tmpName)
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	atomicWriteFileCrashHook("after-rename", path)
	if err := os.Chmod(path, perm); err != nil {
		return err
	}
	return fsyncDir(dir)
}

var atomicWriteFileCrashHook = func(string, string) {}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
