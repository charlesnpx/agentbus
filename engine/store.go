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
	"sync"
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

// DefaultReapInterval bounds how often a store performs a full reconciliation
// sweep in response to callers that enumerate jobs.
const DefaultReapInterval = 2 * time.Second

// DefaultGCInterval bounds how often a store performs retention housekeeping.
const DefaultGCInterval = 30 * time.Second

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
	LeaseDuration time.Duration
	OrphanGrace   time.Duration
	BeforeUpdate  func(string)
	ReapInterval  time.Duration
	GCInterval    time.Duration
	// BeforeReap, OnReapWait, and BeforeRecordLoad are deterministic
	// instrumentation hooks used by store tests and embedders.
	BeforeReap       func()
	OnReapWait       func()
	BeforeRecordLoad func(string)
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
	leaseDuration time.Duration
	orphanGrace   time.Duration
	beforeUpdate  func(string)
	reapInterval  time.Duration
	gcInterval    time.Duration
	beforeReap    func()
	onReapWait    func()
	beforeLoad    func(string)

	reapMu         sync.Mutex
	reapRunning    bool
	reapGeneration *reapGeneration
	lastReap       time.Time
	lastGC         time.Time
}

const staleHeartbeatWarning = "stale-heartbeat: lease expired while process identity remained alive; lease renewed"

const DefaultLeaseDuration = 5 * time.Minute

type reapGeneration struct {
	done chan struct{}
	err  error
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
	leaseDuration := cfg.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = DefaultLeaseDuration
	}
	orphanGrace := cfg.OrphanGrace
	if orphanGrace <= 0 {
		orphanGrace = leaseDuration
	}
	if orphanGrace < leaseDuration {
		return nil, errors.New("orphan grace cannot be shorter than lease duration")
	}
	reapInterval := cfg.ReapInterval
	if reapInterval < 0 {
		return nil, errors.New("reap interval cannot be negative")
	}
	if reapInterval == 0 {
		reapInterval = DefaultReapInterval
	}
	gcInterval := cfg.GCInterval
	if gcInterval < 0 {
		return nil, errors.New("gc interval cannot be negative")
	}
	if gcInterval == 0 {
		gcInterval = DefaultGCInterval
	}
	return &Store{
		layout:        layout,
		clock:         clock,
		processes:     processes,
		processGroups: processGroups,
		cancelGrace:   cancelGrace,
		cancelWaiter:  cancelWaiter,
		retention:     retention,
		leaseDuration: leaseDuration,
		orphanGrace:   orphanGrace,
		beforeUpdate:  cfg.BeforeUpdate,
		reapInterval:  reapInterval,
		gcInterval:    gcInterval,
		beforeReap:    cfg.BeforeReap,
		onReapWait:    cfg.OnReapWait,
		beforeLoad:    cfg.BeforeRecordLoad,
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
		if s.beforeUpdate != nil {
			s.beforeUpdate(jobID)
		}
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
				s.sweepTerminalJobArtifacts(record.JobID)
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

// TouchHeartbeat records a heartbeat without waiting for the contended per-job lock.
func (s *Store) TouchHeartbeat(jobID string, now time.Time, leaseDuration time.Duration) (bool, error) {
	if err := validateJobID(jobID); err != nil {
		return false, err
	}
	jobPath, err := s.jobPath(jobID)
	if err != nil {
		return false, err
	}
	record, err := s.loadPath(jobPath)
	if err != nil {
		return false, err
	}
	if IsTerminal(record.State) {
		return false, nil
	}
	path, err := safePathForID(s.layout.Jobs, jobID, ".heartbeat")
	if err != nil {
		return false, err
	}
	payload := []byte(now.UTC().Format(time.RFC3339Nano) + "\n" + now.UTC().Add(leaseDuration).Format(time.RFC3339Nano) + "\n")
	return true, atomicWriteFile(path, payload, 0o600)
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

// Load reads one job record, lazily reaping only that record before computing
// status-only lease fields. It deliberately does not trigger a store-wide
// reconciliation pass.
func (s *Store) Load(jobID string) (*JobRecord, error) {
	if err := validateJobID(jobID); err != nil {
		return nil, err
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
			if readErr := unwrapRecordReadError(err); readErr != nil {
				if errors.Is(readErr, os.ErrNotExist) {
					return fmt.Errorf("job %q not found: %w", jobID, os.ErrNotExist)
				}
				return readErr
			}
			if qerr := s.quarantine(path, unwrapRecordLoadError(err)); qerr != nil {
				return qerr
			}
			return unwrapRecordLoadError(err)
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
		status := record.StatusRecord(now)
		out = &status
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
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
			loaded, _, err := s.loadPathWithBytes(path)
			if err != nil {
				if readErr := unwrapRecordReadError(err); readErr != nil {
					if errors.Is(readErr, os.ErrNotExist) {
						return nil
					}
					return readErr
				}
				cause := unwrapRecordLoadError(err)
				listLoadErrorHook(path, cause)
				if qerr := s.quarantine(path, cause); qerr != nil {
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
			s.sweepTerminalJobArtifacts(record.JobID)
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

// Reap performs a debounced, single-flighted full reconciliation pass. The
// reap coordinator is never held while the pass acquires per-job locks.
func (s *Store) Reap() error {
	now := s.clock.Now().UTC()
	s.reapMu.Lock()
	if elapsed := now.Sub(s.lastReap); !s.lastReap.IsZero() && elapsed >= 0 && elapsed < s.reapInterval {
		// A negative elapsed duration means the clock moved backward; treat
		// the debounce window as expired instead of suspending reconciliation.
		s.reapMu.Unlock()
		return nil
	}
	if s.reapRunning {
		generation := s.reapGeneration
		onReapWait := s.onReapWait
		s.reapMu.Unlock()
		if onReapWait != nil {
			onReapWait()
		}
		<-generation.done
		return generation.err
	}
	generation := &reapGeneration{done: make(chan struct{})}
	s.reapRunning = true
	s.reapGeneration = generation
	s.reapMu.Unlock()

	err := s.reapFull(now)

	s.reapMu.Lock()
	if err == nil {
		s.lastReap = s.clock.Now().UTC()
	}
	generation.err = err
	s.reapRunning = false
	close(generation.done)
	s.reapMu.Unlock()
	return err
}

type reapedRecord struct {
	jobID  string
	path   string
	record *JobRecord
}

type processLookupResult struct {
	info  ProcessInfo
	alive bool
	err   error
}

func (s *Store) reapFull(now time.Time) error {
	if s.beforeReap != nil {
		s.beforeReap()
	}
	entries, err := os.ReadDir(s.layout.Jobs)
	if err != nil {
		return err
	}
	lookups := make(map[int]processLookupResult)
	lookup := func(pid int) (ProcessInfo, bool, error) {
		if cached, ok := lookups[pid]; ok {
			return cached.info, cached.alive, cached.err
		}
		info, alive, err := s.processes.Lookup(pid)
		lookups[pid] = processLookupResult{info: info, alive: alive, err: err}
		return info, alive, err
	}
	records := make([]reapedRecord, 0, len(entries))
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
				if readErr := unwrapRecordReadError(err); readErr != nil {
					if errors.Is(readErr, os.ErrNotExist) {
						return nil
					}
					return readErr
				}
				if qerr := s.quarantine(path, unwrapRecordLoadError(err)); qerr != nil {
					return qerr
				}
				return nil
			}
			changed, err := s.reapRecordWithLookup(record, now, lookup)
			if err != nil {
				return err
			}
			if changed {
				if err := s.saveIfUnchanged(record, path, original); err != nil {
					return err
				}
			}
			records = append(records, reapedRecord{jobID: jobID, path: path, record: record})
			return nil
		}); err != nil {
			return err
		}
	}
	if elapsed := now.Sub(s.lastGC); !s.lastGC.IsZero() && elapsed >= 0 && elapsed < s.gcInterval {
		// Backward clock movement expires the gc throttle rather than
		// suspending retention housekeeping until time catches up.
		return nil
	}
	if err := s.gc(now, records); err != nil {
		return err
	}
	s.lastGC = s.clock.Now().UTC()
	return nil
}

func (s *Store) reapRecord(record *JobRecord, now time.Time) (bool, error) {
	return s.reapRecordWithLookup(record, now, s.processes.Lookup)
}

func (s *Store) reapRecordWithLookup(record *JobRecord, now time.Time, lookup func(int) (ProcessInfo, bool, error)) (bool, error) {
	changed := false
	if heartbeatAt, expiresAt, ok := s.readHeartbeat(record.JobID); ok && heartbeatAt.After(record.HeartbeatAt) {
		record.HeartbeatAt = heartbeatAt
		record.Lease = Lease{ExpiresAt: expiresAt}
		changed = true
	}
	switch record.State {
	case StateOrphaned:
		if record.UpdatedAt.IsZero() || now.Sub(record.UpdatedAt) < s.orphanGrace {
			return changed, nil
		}
		return true, record.Transition(StateReaped, now)
	case StateQueued, StateStarting:
		if !record.UpdatedAt.IsZero() && now.Sub(record.UpdatedAt) >= s.retention.StaleJobAfter {
			return true, record.Transition(StateOrphaned, now)
		}
	case StateRunning, StateRetrying:
		if processGoneOrReused(record.Worker, lookup) {
			return true, record.Transition(StateOrphaned, now)
		}
		if processGoneOrReused(record.Supervisor, lookup) {
			return true, record.Transition(StateOrphaned, now)
		}
		if !record.Lease.ExpiresAt.IsZero() && !now.Before(record.Lease.ExpiresAt) {
			if processIdentityConfirmed(record, lookup) {
				record.HeartbeatAt = now
				record.Lease = Lease{ExpiresAt: now.Add(s.leaseDuration)}
				record.Warnings = appendWarning(record.Warnings, staleHeartbeatWarning)
				return true, nil
			}
			return true, record.Transition(StateOrphaned, now)
		}
	}
	return changed, nil
}

func (s *Store) processIdentityConfirmed(record *JobRecord) bool {
	return processIdentityConfirmed(record, s.processes.Lookup)
}

func processIdentityConfirmed(record *JobRecord, lookup func(int) (ProcessInfo, bool, error)) bool {
	refs := []ProcessRef{record.Worker, record.Supervisor}
	if record.BackendChildPID > 0 {
		refs = append(refs, ProcessRef{PID: record.BackendChildPID, StartTime: record.BackendChildStartTime})
	}
	confirmed := false
	for _, ref := range refs {
		if ref.PID <= 0 || ref.StartTime == "" {
			continue
		}
		confirmed = true
		info, alive, err := lookup(ref.PID)
		if err != nil || !alive || info.StartTime == "" || info.StartTime != ref.StartTime {
			return false
		}
	}
	return confirmed
}

func (s *Store) readHeartbeat(jobID string) (time.Time, time.Time, bool) {
	path, err := safePathForID(s.layout.Jobs, jobID, ".heartbeat")
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, time.Time{}, false
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		return time.Time{}, time.Time{}, false
	}
	heartbeatAt, err1 := time.Parse(time.RFC3339Nano, lines[0])
	expiresAt, err2 := time.Parse(time.RFC3339Nano, lines[1])
	return heartbeatAt, expiresAt, err1 == nil && err2 == nil
}

func appendWarning(warnings []string, warning string) []string {
	for _, existing := range warnings {
		if existing == warning {
			return warnings
		}
	}
	return append(warnings, warning)
}

func (s *Store) processGoneOrReused(ref ProcessRef) bool {
	return processGoneOrReused(ref, s.processes.Lookup)
}

func processGoneOrReused(ref ProcessRef, lookup func(int) (ProcessInfo, bool, error)) bool {
	if ref.PID <= 0 {
		return false
	}
	info, alive, err := lookup(ref.PID)
	if err != nil || !alive {
		return true
	}
	return ref.StartTime != "" && info.StartTime != "" && ref.StartTime != info.StartTime
}

func (s *Store) quarantine(path string, cause error) error {
	base := filepath.Base(path)
	stamp := s.clock.Now().UTC().Format("20060102T150405.000000000Z")
	target := filepath.Join(s.layout.Quarantine, stamp+"-"+base)
	if err := os.Rename(path, target); err != nil {
		return err
	}
	jobID := strings.TrimSuffix(base, ".json")
	if validateJobID(jobID) == nil {
		s.sweepHeartbeat(jobID)
	}
	diag := []byte(fmt.Sprintf("recordPath: %s\nfailure: %v\n", path, cause))
	if err := atomicWriteFile(target+".diagnostic.txt", diag, 0o600); err != nil {
		return err
	}
	return fsyncDir(s.layout.Quarantine)
}

func (s *Store) gc(now time.Time, records []reapedRecord) error {
	protectedResults := make(map[string]struct{})
	for _, item := range records {
		record := item.record
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
	for _, item := range records {
		item := item
		if err := s.withJobLock(item.jobID, func() error {
			record := item.record
			if !IsTerminal(record.State) {
				return nil
			}
			if now.Sub(record.UpdatedAt) < s.retention.TerminalJobTTL {
				s.sweepTerminalJobArtifacts(record.JobID)
				return nil
			}
			// A deletion candidate may have been finalized after the reap pass.
			// Re-read it while holding the job lock before removing any artifacts.
			record, err := s.loadPath(item.path)
			if err != nil {
				return nil
			}
			if !IsTerminal(record.State) || now.Sub(record.UpdatedAt) < s.retention.TerminalJobTTL {
				return nil
			}
			s.sweepTerminalJobArtifacts(record.JobID)
			logRemoveContainedIfExists(s.layout.Logs, record.LogPaths.Stdout)
			logRemoveContainedIfExists(s.layout.Logs, record.LogPaths.Stderr)
			logRemoveIfExists(item.path)
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
	return record, unwrapRecordLoadError(err)
}

// recordReadError marks failures reading a record's bytes. Callers must return
// these filesystem errors rather than treating the record as corrupt.
type recordReadError struct{ err error }

func (e *recordReadError) Error() string { return e.err.Error() }

func (e *recordReadError) Unwrap() error { return e.err }

// recordContentError marks successfully read records whose contents are not a
// valid job record. These are eligible for quarantine.
type recordContentError struct{ err error }

func (e *recordContentError) Error() string { return e.err.Error() }

func (e *recordContentError) Unwrap() error { return e.err }

func unwrapRecordReadError(err error) error {
	var readErr *recordReadError
	if errors.As(err, &readErr) {
		return readErr.err
	}
	return nil
}

func unwrapRecordLoadError(err error) error {
	if err == nil {
		return nil
	}
	if readErr := unwrapRecordReadError(err); readErr != nil {
		return readErr
	}
	var contentErr *recordContentError
	if errors.As(err, &contentErr) {
		return contentErr.err
	}
	return err
}

func (s *Store) loadPathWithBytes(path string) (*JobRecord, []byte, error) {
	if s.beforeLoad != nil {
		s.beforeLoad(path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, &recordReadError{err: err}
	}
	var record JobRecord
	if err := json.Unmarshal(b, &record); err != nil {
		return nil, nil, &recordContentError{err: err}
	}
	if record.JobID == "" || record.State == "" {
		return nil, nil, &recordContentError{err: errors.New("invalid job record: missing jobId or state")}
	}
	if err := validateJobID(record.JobID); err != nil {
		return nil, nil, &recordContentError{err: err}
	}
	expected := strings.TrimSuffix(filepath.Base(path), ".json")
	if record.JobID != expected {
		return nil, nil, &recordContentError{err: fmt.Errorf("invalid job record: jobId %q does not match path %q", record.JobID, filepath.Base(path))}
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

func (s *Store) sweepTerminalJobArtifacts(jobID string) {
	inputPath, err := safePathForID(s.layout.Inputs, jobID, ".json")
	if err != nil {
		log.Printf("agentbus gc: invalid terminal input path for %s: %v", jobID, err)
		return
	}
	if err := removeIfExists(inputPath); err != nil {
		log.Printf("agentbus gc: failed to remove terminal input %s: %v", inputPath, err)
	}
	s.sweepHeartbeat(jobID)
}

func (s *Store) sweepHeartbeat(jobID string) {
	heartbeatPath, err := safePathForID(s.layout.Jobs, jobID, ".heartbeat")
	if err != nil {
		log.Printf("agentbus gc: invalid heartbeat path for %s: %v", jobID, err)
		return
	}
	if err := removeIfExists(heartbeatPath); err != nil {
		log.Printf("agentbus gc: failed to remove heartbeat %s: %v", heartbeatPath, err)
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
