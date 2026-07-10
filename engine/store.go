package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

// Waiter provides deterministic waits in tests.
type Waiter interface {
	Wait(time.Duration)
}

// WaiterFunc adapts a function to Waiter.
type WaiterFunc func(time.Duration)

// Wait calls f().
func (f WaiterFunc) Wait(d time.Duration) { f(d) }

// DefaultCancelGrace is the protocol default grace period before SIGKILL.
const DefaultCancelGrace = 10 * time.Second

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
	Root          string
	CWD           string
	Clock         Clock
	Processes     ProcessTable
	ProcessGroups ProcessGroupSignaler
	CancelGrace   time.Duration
	CancelWaiter  Waiter
	Retention     RetentionConfig
}

type workspaceManifest struct {
	Version int    `json:"version"`
	CWD     string `json:"cwd"`
	Key     string `json:"key"`
}

// Store persists job records for one workspace namespace.
type Store struct {
	layout        WorkspaceLayout
	clock         Clock
	processes     ProcessTable
	processGroups ProcessGroupSignaler
	cancelGrace   time.Duration
	cancelWaiter  Waiter
	retention     RetentionConfig
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
	store, err := newStoreWithLayout(cfg, layout)
	if err != nil {
		return nil, err
	}
	if err := writeWorkspaceManifest(layout); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenWorkspaceStores opens every persisted workspace namespace under cfg.Root.
func OpenWorkspaceStores(cfg StoreConfig) ([]*Store, error) {
	root := cfg.Root
	var err error
	if root == "" {
		root, err = ResolveStateRoot()
		if err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "workspaces"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	stores := make([]*Store, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		key := entry.Name()
		if err := validateWorkspaceKey(key); err != nil {
			continue
		}
		workspace := readWorkspaceManifestCWD(filepath.Join(root, "workspaces", key), key)
		layout, err := layoutForWorkspaceKey(root, key, workspace)
		if err != nil {
			return nil, err
		}
		store, err := newStoreWithLayout(cfg, layout)
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	sort.Slice(stores, func(i, j int) bool { return stores[i].layout.Key < stores[j].layout.Key })
	return stores, nil
}

func newStoreWithLayout(cfg StoreConfig, layout WorkspaceLayout) (*Store, error) {
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
	processGroups := cfg.ProcessGroups
	if processGroups == nil {
		processGroups = NativeProcessGroupSignaler{}
	}
	cancelGrace := cfg.CancelGrace
	if cancelGrace < 0 {
		return nil, errors.New("cancel grace cannot be negative")
	}
	if cancelGrace == 0 {
		cancelGrace = DefaultCancelGrace
	}
	cancelWaiter := cfg.CancelWaiter
	if cancelWaiter == nil {
		cancelWaiter = WaiterFunc(time.Sleep)
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
	return &Store{
		layout:        layout,
		clock:         clock,
		processes:     processes,
		processGroups: processGroups,
		cancelGrace:   cancelGrace,
		cancelWaiter:  cancelWaiter,
		retention:     retention,
	}, nil
}

// Layout returns the workspace layout used by the store.
func (s *Store) Layout() WorkspaceLayout { return s.layout }

func writeWorkspaceManifest(layout WorkspaceLayout) error {
	if layout.Workspace == "" {
		return nil
	}
	manifest := workspaceManifest{
		Version: 1,
		CWD:     layout.Workspace,
		Key:     layout.Key,
	}
	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return atomicWriteFile(filepath.Join(layout.Namespace, workspaceManifestFile), raw, 0o600)
}

func readWorkspaceManifestCWD(namespace, key string) string {
	raw, err := os.ReadFile(filepath.Join(namespace, workspaceManifestFile))
	if err != nil {
		return ""
	}
	var manifest workspaceManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return ""
	}
	if manifest.Key != "" && manifest.Key != key {
		return ""
	}
	if manifest.CWD == "" {
		return ""
	}
	cwd, err := CanonicalWorkspace(manifest.CWD)
	if err != nil {
		return ""
	}
	if WorkspaceKey(cwd) != key {
		return ""
	}
	return cwd
}

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
		return s.saveRecordLocked(record, path)
	})
}

// Update loads, mutates, and saves a job record while holding the per-job lock.
func (s *Store) Update(jobID string, mutate func(*JobRecord) (bool, error)) (*JobRecord, error) {
	if mutate == nil {
		return nil, errors.New("nil job update")
	}
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	path, err := s.jobPath(jobID)
	if err != nil {
		return nil, err
	}
	var out *JobRecord
	if err := s.withJobLock(jobID, func() error {
		record, err := s.loadPath(path)
		if err != nil {
			return err
		}
		before := *record
		changed, err := mutate(record)
		if err != nil {
			return err
		}
		if err := validateGuardedStateChange(before, *record); err != nil {
			return err
		}
		if changed {
			if err := s.saveRecordLocked(record, path); err != nil {
				return err
			}
			if IsTerminal(record.State) {
				s.sweepTerminalJobInput(record.JobID)
			}
		}
		status := record.StatusRecord(s.clock.Now().UTC())
		out = &status
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// HasJob reports whether a persisted job record exists in this workspace store.
func (s *Store) HasJob(jobID string) (bool, error) {
	if err := validateJobID(jobID); err != nil {
		return false, err
	}
	path, err := s.jobPath(jobID)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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
	listAfterReapHook()
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
		jobID := strings.TrimSuffix(entry.Name(), ".json")
		if err := validateJobID(jobID); err != nil {
			if qerr := s.quarantine(path, err); qerr != nil {
				return nil, qerr
			}
			continue
		}
		var record *JobRecord
		if err := s.withJobLock(jobID, func() error {
			loaded, err := s.loadPath(path)
			if err != nil {
				listLoadErrorHook(path, err)
				if qerr := s.quarantine(path, err); qerr != nil {
					return qerr
				}
				return nil
			}
			record = loaded
			return nil
		}); err != nil {
			return nil, err
		}
		if record == nil {
			continue
		}
		out = append(out, *record)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].JobID < out[j].JobID })
	return out, nil
}

// Interrupt transitions a queued or active foreground turn record to interrupted.
func (s *Store) Interrupt(jobID string) (*JobRecord, error) {
	return s.terminate(jobID, StateInterrupted)
}

// Cancel transitions a queued or active background job record to canceled.
func (s *Store) Cancel(jobID string) (*JobRecord, error) {
	return s.terminate(jobID, StateCanceled)
}

func (s *Store) terminate(jobID string, state JobState) (*JobRecord, error) {
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	if state != StateInterrupted && state != StateCanceled {
		return nil, fmt.Errorf("unsupported terminal state %q", state)
	}
	path, err := s.jobPath(jobID)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now().UTC()
	var out *JobRecord
	if err := s.withJobLock(jobID, func() error {
		record, original, err := s.loadPathWithBytes(path)
		if err != nil {
			return err
		}
		if IsTerminal(record.State) {
			status := record.StatusRecord(now)
			out = &status
			return nil
		}
		if err := s.cancelLiveProcessGroup(record); err != nil {
			return err
		}
		if err := record.Transition(state, now); err != nil {
			return err
		}
		if err := s.saveIfUnchanged(record, path, original); err != nil {
			return err
		}
		if IsTerminal(record.State) {
			s.sweepTerminalJobInput(record.JobID)
		}
		status := record.StatusRecord(now)
		out = &status
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

type cancelProcessGroupTarget struct {
	pgid            int
	worker          ProcessRef
	backendChildPID int
}

func (s *Store) cancelLiveProcessGroup(record *JobRecord) error {
	switch record.State {
	case StateStarting, StateRunning, StateRetrying:
	default:
		return nil
	}
	target, ok, err := s.liveCancelProcessGroup(record)
	if err != nil || !ok {
		return err
	}
	if err := s.processGroups.SignalProcessGroup(target.pgid, syscall.SIGTERM); err != nil {
		return err
	}
	s.cancelWaiter.Wait(s.cancelGrace)
	alive, err := s.cancelProcessGroupStillAlive(target)
	if err != nil || !alive {
		return err
	}
	return s.processGroups.SignalProcessGroup(target.pgid, syscall.SIGKILL)
}

func (s *Store) liveCancelProcessGroup(record *JobRecord) (cancelProcessGroupTarget, bool, error) {
	if record.Worker.PGID <= 0 {
		return cancelProcessGroupTarget{}, false, nil
	}
	workerAlive, workerErr := s.processRefAlive(record.Worker)
	childAlive, childErr := s.backendChildAlive(record.BackendChildPID)
	if workerAlive || childAlive {
		return cancelProcessGroupTarget{
			pgid:            record.Worker.PGID,
			worker:          record.Worker,
			backendChildPID: record.BackendChildPID,
		}, true, nil
	}
	if workerErr != nil {
		return cancelProcessGroupTarget{}, false, workerErr
	}
	if childErr != nil {
		return cancelProcessGroupTarget{}, false, childErr
	}
	return cancelProcessGroupTarget{}, false, nil
}

func (s *Store) backendChildAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}
	_, alive, err := s.processes.Lookup(pid)
	return alive, err
}

func (s *Store) cancelProcessGroupStillAlive(target cancelProcessGroupTarget) (bool, error) {
	alive, err := s.processRefAlive(target.worker)
	if err != nil || alive {
		return alive, err
	}
	if target.backendChildPID <= 0 {
		return false, nil
	}
	_, childAlive, err := s.processes.Lookup(target.backendChildPID)
	return childAlive, err
}

func (s *Store) processRefAlive(ref ProcessRef) (bool, error) {
	if ref.PID <= 0 {
		return false, nil
	}
	info, alive, err := s.processes.Lookup(ref.PID)
	if err != nil || !alive {
		return false, err
	}
	if ref.StartTime != "" {
		if info.StartTime == "" {
			return false, fmt.Errorf("cannot verify process %d start time", ref.PID)
		}
		return ref.StartTime == info.StartTime, nil
	}
	return true, nil
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
			resultPath := filepath.Clean(record.Result.ResultPath)
			if !pathWithinDir(s.layout.Results, resultPath) {
				continue
			}
			if IsTerminal(record.State) && now.Sub(record.UpdatedAt) >= s.retention.ResultTTL {
				logRemoveIfExists(resultPath)
				continue
			}
			protectedResults[resultPath] = struct{}{}
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
			if !IsTerminal(record.State) {
				return nil
			}
			s.sweepTerminalJobInput(record.JobID)
			if now.Sub(record.UpdatedAt) < s.retention.TerminalJobTTL {
				return nil
			}
			logRemoveContainedIfExists(s.layout.Logs, record.LogPaths.Stdout)
			logRemoveContainedIfExists(s.layout.Logs, record.LogPaths.Stderr)
			logRemoveIfExists(path)
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
				logRemoveIfExists(path)
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

func (s *Store) saveRecordLocked(record *JobRecord, path string) error {
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
	return atomicWriteFile(path, b, 0o600)
}

func validateGuardedStateChange(before, after JobRecord) error {
	if before.State == after.State {
		return nil
	}
	if !LegalTransition(before.State, after.State, before.RetryCount) {
		return fmt.Errorf("illegal job state transition %q -> %q", before.State, after.State)
	}
	return nil
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

func logRemoveIfExists(path string) {
	if err := removeIfExists(path); err != nil {
		log.Printf("agentbus gc: failed to remove %s: %v", path, err)
	}
}

func (s *Store) sweepTerminalJobInput(jobID string) {
	inputPath, err := safePathForID(s.layout.Inputs, jobID, ".json")
	if err != nil {
		log.Printf("agentbus gc: invalid terminal input path for %s: %v", jobID, err)
		return
	}
	if err := removeIfExists(inputPath); err != nil {
		log.Printf("agentbus gc: failed to remove terminal input %s: %v", inputPath, err)
	}
}

func logRemoveContainedIfExists(dir, path string) {
	if path == "" {
		return
	}
	clean := filepath.Clean(path)
	if !pathWithinDir(dir, clean) {
		return
	}
	logRemoveIfExists(clean)
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

var listAfterReapHook = func() {}

var listLoadErrorHook = func(string, error) {}

func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
