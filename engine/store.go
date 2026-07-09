package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	if record.JobID == "" {
		return errors.New("job id is required")
	}
	now := s.clock.Now().UTC()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	record.StatePath = s.jobPath(record.JobID)
	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return atomicWriteFile(record.StatePath, b, 0o600)
}

// Load reads a job record and computes status-only lease fields.
func (s *Store) Load(jobID string) (*JobRecord, error) {
	path := s.jobPath(jobID)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var record JobRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, err
	}
	if record.JobID == "" || record.State == "" {
		return nil, errors.New("invalid job record: missing jobId or state")
	}
	status := record.StatusRecord(s.clock.Now().UTC())
	return &status, nil
}

// List loads all non-corrupt job records. Corrupt records are left for Reap.
func (s *Store) List() ([]JobRecord, error) {
	entries, err := os.ReadDir(s.layout.Jobs)
	if err != nil {
		return nil, err
	}
	var out []JobRecord
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		record, err := s.Load(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil {
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
		record, err := s.loadPath(path)
		if err != nil {
			if qerr := s.quarantine(path, err); qerr != nil {
				return qerr
			}
			continue
		}
		changed, err := s.reapRecord(record, now)
		if err != nil {
			return err
		}
		if changed {
			if err := s.Save(record); err != nil {
				return err
			}
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
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		record, err := s.loadPath(filepath.Join(s.layout.Jobs, entry.Name()))
		if err != nil || !IsTerminal(record.State) || now.Sub(record.UpdatedAt) < s.retention.TerminalJobTTL {
			continue
		}
		_ = removeIfExists(record.LogPaths.Stdout)
		_ = removeIfExists(record.LogPaths.Stderr)
		_ = removeIfExists(filepath.Join(s.layout.Inputs, record.JobID+".json"))
		_ = removeIfExists(record.StatePath)
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
			if now.Sub(info.ModTime()) >= s.retention.ResultTTL {
				_ = os.Remove(filepath.Join(dir, entry.Name()))
			}
		}
	}
	return nil
}

func (s *Store) loadPath(path string) (*JobRecord, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var record JobRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, err
	}
	if record.JobID == "" || record.State == "" {
		return nil, errors.New("invalid job record: missing jobId or state")
	}
	record.StatePath = path
	status := record.StatusRecord(s.clock.Now().UTC())
	return &status, nil
}

func (s *Store) jobPath(jobID string) string {
	return filepath.Join(s.layout.Jobs, jobID+".json")
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
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if err := os.Chmod(path, perm); err != nil {
		return err
	}
	return fsyncDir(dir)
}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
