package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

type fakeProcessTable struct {
	entries map[int]ProcessInfo
	errs    map[int]error
}

func (p fakeProcessTable) Lookup(pid int) (ProcessInfo, bool, error) {
	if err := p.errs[pid]; err != nil {
		return ProcessInfo{}, false, err
	}
	info, ok := p.entries[pid]
	return info, ok, nil
}

type countingProcessTable struct {
	entries map[int]ProcessInfo

	mu      sync.Mutex
	lookups map[int]int
}

func (p *countingProcessTable) Lookup(pid int) (ProcessInfo, bool, error) {
	p.mu.Lock()
	p.lookups[pid]++
	p.mu.Unlock()
	info, ok := p.entries[pid]
	return info, ok, nil
}

func (p *countingProcessTable) lookupCount(pid int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lookups[pid]
}

type testStoreOptions struct {
	processGroups    ProcessGroupSignaler
	cancelWaiter     Waiter
	cancelGrace      time.Duration
	retention        RetentionConfig
	leaseDuration    time.Duration
	orphanGrace      time.Duration
	beforeUpdate     func(string)
	reapInterval     time.Duration
	gcInterval       time.Duration
	beforeReap       func()
	onReapWait       func()
	beforeRecordLoad func(string)
}

func newTestStore(t *testing.T, now time.Time, pt ProcessTable) *Store {
	return newTestStoreWithOptions(t, now, pt, testStoreOptions{})
}

func newTestStoreWithOptions(t *testing.T, now time.Time, pt ProcessTable, opts testStoreOptions) *Store {
	t.Helper()
	retention := opts.retention
	if retention.TerminalJobTTL == 0 {
		retention.TerminalJobTTL = time.Hour
	}
	if retention.ResultTTL == 0 {
		retention.ResultTTL = time.Hour
	}
	if retention.StaleJobAfter == 0 {
		retention.StaleJobAfter = time.Minute
	}
	store, err := NewStore(StoreConfig{
		Root:             filepath.Join(t.TempDir(), "state"),
		CWD:              t.TempDir(),
		Clock:            &fakeClock{now: now},
		Processes:        pt,
		ProcessGroups:    opts.processGroups,
		CancelWaiter:     opts.cancelWaiter,
		CancelGrace:      opts.cancelGrace,
		Retention:        retention,
		LeaseDuration:    opts.leaseDuration,
		OrphanGrace:      opts.orphanGrace,
		BeforeUpdate:     opts.beforeUpdate,
		ReapInterval:     opts.reapInterval,
		GCInterval:       opts.gcInterval,
		BeforeReap:       opts.beforeReap,
		OnReapWait:       opts.onReapWait,
		BeforeRecordLoad: opts.beforeRecordLoad,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type processGroupSignal struct {
	pgid   int
	signal syscall.Signal
}

type recordingProcessGroupSignaler struct {
	signals  []processGroupSignal
	onSignal func(pgid int, signal syscall.Signal)
}

func (s *recordingProcessGroupSignaler) SignalProcessGroup(pgid int, signal syscall.Signal) error {
	s.signals = append(s.signals, processGroupSignal{pgid: pgid, signal: signal})
	if s.onSignal != nil {
		s.onSignal(pgid, signal)
	}
	return nil
}

type recordingWaiter struct {
	waits []time.Duration
}

func (w *recordingWaiter) Wait(d time.Duration) {
	w.waits = append(w.waits, d)
}

func assertProcessGroupSignals(t *testing.T, got, want []processGroupSignal) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("signals = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("signals = %+v, want %+v", got, want)
		}
	}
}

func assertWaits(t *testing.T, got, want []time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("waits = %v, want %v", got, want)
		}
	}
}

func TestStateLayoutAndPermissions(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "root")
	workspace := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if runtime.GOOS != "windows" {
		if err := os.Symlink(workspace, link); err != nil {
			t.Fatal(err)
		}
		workspace = link
	}
	store, err := NewStore(StoreConfig{Root: root, CWD: workspace, Clock: ClockFunc(time.Now), Processes: fakeProcessTable{entries: map[int]ProcessInfo{}}})
	if err != nil {
		t.Fatal(err)
	}
	canon, err := CanonicalWorkspace(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if got := store.Layout().Key; got != WorkspaceKey(canon) || len(got) != 64 {
		t.Fatalf("workspace key = %q", got)
	}
	for _, dir := range []string{store.Layout().Root, store.Layout().Namespace, store.Layout().Jobs, store.Layout().Logs, store.Layout().Results, store.Layout().Inputs, store.Layout().Quarantine} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s mode = %o, want 700", dir, got)
		}
	}
	record := &JobRecord{JobID: "job_perm", State: StateQueued}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("record mode = %o, want 600", got)
	}
}

func TestOpenWorkspaceStoresIgnoresManifestWithMismatchedCWD(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "root")
	workspace := t.TempDir()
	alias := t.TempDir()
	store, err := NewStore(StoreConfig{Root: root, CWD: workspace, Clock: ClockFunc(time.Now), Processes: fakeProcessTable{entries: map[int]ProcessInfo{}}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(workspaceManifest{
		Version: 1,
		CWD:     alias,
		Key:     store.Layout().Key,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(store.Layout().Namespace, workspaceManifestFile), raw, 0o600); err != nil {
		t.Fatal(err)
	}

	stores, err := OpenWorkspaceStores(StoreConfig{Root: root, Clock: ClockFunc(time.Now), Processes: fakeProcessTable{entries: map[int]ProcessInfo{}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(stores) != 1 {
		t.Fatalf("stores = %d, want 1", len(stores))
	}
	if got := stores[0].Layout().Key; got != store.Layout().Key {
		t.Fatalf("workspace key = %q, want %q", got, store.Layout().Key)
	}
	if got := stores[0].Layout().Workspace; got != "" {
		t.Fatalf("workspace alias = %q, want empty for mismatched manifest cwd", got)
	}
}

func TestJobIDPathsStayInNamespace(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	for _, id := range []string{"../../escape", "job_../escape", "job_bad/name", "job_bad\\name", "turn_123", "job_"} {
		id := id
		t.Run(id, func(t *testing.T) {
			if err := store.Save(&JobRecord{JobID: id, State: StateQueued}); err == nil {
				t.Fatalf("Save(%q) succeeded", id)
			}
			if _, err := store.Load(id); err == nil {
				t.Fatalf("Load(%q) succeeded", id)
			}
			if _, err := store.WriteResult(id, []byte("x"), 1); err == nil {
				t.Fatalf("WriteResult(%q) succeeded", id)
			}
		})
	}
	if _, err := os.Stat(filepath.Join(store.Layout().Root, "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("escape path exists or unexpected error: %v", err)
	}
}

func TestStateMachineAndExitCodes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		from       JobState
		to         JobState
		retryCount int
		want       bool
	}{
		{StateQueued, StateStarting, 0, true},
		{StateStarting, StateRunning, 0, true},
		{StateRunning, StateRetrying, 0, true},
		{StateRunning, StateRetrying, 1, false},
		{StateRetrying, StateCompletedNoncompliant, 1, true},
		{StateCompleted, StateRunning, 0, false},
		{StateOrphaned, StateCompleted, 0, true},
		{StateOrphaned, StateCompletedNoncompliant, 0, true},
		{StateOrphaned, StateFailed, 0, true},
		{StateOrphaned, StateReaped, 0, true},
		{StateQueued, StateCompleted, 0, false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(string(tt.from)+"_"+string(tt.to), func(t *testing.T) {
			t.Parallel()
			record := JobRecord{JobID: "job", State: tt.from, RetryCount: tt.retryCount}
			err := record.Transition(tt.to, now)
			if (err == nil) != tt.want {
				t.Fatalf("Transition(%s,%s) err=%v want allowed=%v", tt.from, tt.to, err, tt.want)
			}
		})
	}
	exitTests := map[JobState]int{
		StateCompleted:             0,
		StateQueued:                2,
		StateCompletedNoncompliant: 3,
		StateFailed:                4,
		StateTimedOut:              5,
		StateInterrupted:           6,
		StateCanceled:              7,
		StateReaped:                8,
		StateQuarantined:           9,
	}
	for state, want := range exitTests {
		if got := ExitCodeForState(state); got != want {
			t.Fatalf("ExitCodeForState(%s) = %d, want %d", state, got, want)
		}
	}
}

func TestStoreUpdateRejectsIllegalTerminalTransition(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	record := &JobRecord{JobID: "job_guarded_update", State: StateCompleted, UpdatedAt: now}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(record.JobID, func(record *JobRecord) (bool, error) {
		record.State = StateRunning
		record.UpdatedAt = now.Add(time.Second)
		return true, nil
	}); err == nil {
		t.Fatal("Update allowed completed -> running")
	}
	persisted, err := store.Load(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != StateCompleted || !persisted.UpdatedAt.Equal(now) {
		t.Fatalf("illegal update changed record: %+v", persisted)
	}
}

func TestStoreLeaseStatusAndReaper(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		job  JobRecord
		pt   fakeProcessTable
		want JobState
	}{
		{
			name: "expired lease with alive worker is renewed",
			job:  JobRecord{JobID: "job_lease", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(-time.Second)}, Worker: ProcessRef{PID: 101, StartTime: "a"}},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{101: {PID: 101, StartTime: "a"}}},
			want: StateRunning,
		},
		{
			name: "stale queued orphaned",
			job:  JobRecord{JobID: "job_stale", State: StateQueued, UpdatedAt: base.Add(-2 * time.Minute)},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{}},
			want: StateOrphaned,
		},
		{
			name: "worker crash orphaned",
			job:  JobRecord{JobID: "job_crash", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(time.Minute)}, Worker: ProcessRef{PID: 102, StartTime: "a"}},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{}},
			want: StateOrphaned,
		},
		{
			name: "pid reuse orphaned",
			job:  JobRecord{JobID: "job_reuse", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(time.Minute)}, Worker: ProcessRef{PID: 103, StartTime: "old"}},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{103: {PID: 103, StartTime: "new"}}},
			want: StateOrphaned,
		},
		{
			name: "backend child exit does not orphan live worker and supervisor",
			job:  JobRecord{JobID: "job_child", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(time.Minute)}, Worker: ProcessRef{PID: 104, StartTime: "worker"}, Supervisor: ProcessRef{PID: 105, StartTime: "supervisor"}, BackendChildPID: 106, BackendChildStartTime: "child"},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{104: {PID: 104, StartTime: "worker"}, 105: {PID: 105, StartTime: "supervisor"}}},
			want: StateRunning,
		},
		{
			name: "orphaned reaped",
			job:  JobRecord{JobID: "job_orphan", State: StateOrphaned, UpdatedAt: base.Add(-DefaultLeaseDuration)},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{}},
			want: StateReaped,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, base, tt.pt)
			job := tt.job
			if err := store.Save(&job); err != nil {
				t.Fatal(err)
			}
			loaded, err := store.loadPath(job.StatePath)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.Lease.ExpiresAt.IsZero() && loaded.Lease.Expired != !base.Before(loaded.Lease.ExpiresAt) {
				t.Fatalf("computed lease expired = %v", loaded.Lease.Expired)
			}
			if err := store.Reap(); err != nil {
				t.Fatal(err)
			}
			got, err := store.loadPath(job.StatePath)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tt.want {
				t.Fatalf("state = %s, want %s", got.State, tt.want)
			}
			if tt.name == "expired lease with alive worker is renewed" {
				if !got.Lease.ExpiresAt.Equal(base.Add(DefaultLeaseDuration)) || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "stale-heartbeat") {
					t.Fatalf("renewed record = %+v", got)
				}
			}
		})
	}
}

func TestReaperUsesConfiguredLeaseAndOrphanGrace(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: base}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{101: {PID: 101, StartTime: "a"}}}, testStoreOptions{leaseDuration: 20 * time.Second, orphanGrace: 30 * time.Second, reapInterval: time.Nanosecond})
	store.clock = clock
	record := &JobRecord{JobID: "job_configured_lease", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(-time.Second)}, Worker: ProcessRef{PID: 101, StartTime: "a"}}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	got, err := store.loadPath(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Lease.ExpiresAt.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("lease = %s", got.Lease.ExpiresAt)
	}
	got.State = StateOrphaned
	got.UpdatedAt = base
	if err := store.Save(got); err != nil {
		t.Fatal(err)
	}
	clock.now = base.Add(29 * time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	got, _ = store.loadPath(record.StatePath)
	if got.State != StateOrphaned {
		t.Fatalf("state before grace = %s", got.State)
	}
	clock.now = base.Add(30 * time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	got, _ = store.loadPath(record.StatePath)
	if got.State != StateReaped {
		t.Fatalf("state after grace = %s", got.State)
	}
}

func TestLeaseRenewalRequiresAllPersistedProcessIdentities(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		jobID     string
		processes map[int]ProcessInfo
	}{
		{
			name:      "backend child mismatch",
			jobID:     "job_backend_identity",
			processes: map[int]ProcessInfo{101: {PID: 101, StartTime: "worker"}, 202: {PID: 202, StartTime: "reused"}, 303: {PID: 303, StartTime: "supervisor"}},
		},
		{
			name:      "supervisor identity unavailable",
			jobID:     "job_supervisor_identity",
			processes: map[int]ProcessInfo{101: {PID: 101, StartTime: "worker"}, 202: {PID: 202, StartTime: "backend"}, 303: {PID: 303}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t, base, fakeProcessTable{entries: tt.processes})
			record := &JobRecord{JobID: tt.jobID, State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(-time.Second)}, Worker: ProcessRef{PID: 101, StartTime: "worker"}, Supervisor: ProcessRef{PID: 303, StartTime: "supervisor"}, BackendChildPID: 202, BackendChildStartTime: "backend"}
			if err := store.Save(record); err != nil {
				t.Fatal(err)
			}
			if err := store.Reap(); err != nil {
				t.Fatal(err)
			}
			got, _ := store.loadPath(record.StatePath)
			if got.State != StateOrphaned {
				t.Fatalf("state = %s", got.State)
			}
		})
	}
}

func TestTerminalAndQuarantineRemoveHeartbeatSidecar(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	record := &JobRecord{JobID: "job_heartbeat_cleanup", State: StateRunning, UpdatedAt: base}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TouchHeartbeat(record.JobID, base, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(record.JobID, func(r *JobRecord) (bool, error) { return true, r.Transition(StateFailed, base) }); err != nil {
		t.Fatal(err)
	}
	heartbeat := filepath.Join(store.Layout().Jobs, record.JobID+".heartbeat")
	if _, err := os.Stat(heartbeat); !os.IsNotExist(err) {
		t.Fatalf("terminal heartbeat remains: %v", err)
	}
	badID := "job_bad_heartbeat"
	badPath := filepath.Join(store.Layout().Jobs, badID+".json")
	if err := os.WriteFile(badPath, []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Layout().Jobs, badID+".heartbeat"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store.Layout().Jobs, badID+".heartbeat")); !os.IsNotExist(err) {
		t.Fatalf("quarantine heartbeat remains: %v", err)
	}
}

func TestCancelRunningLiveProcessSignalsTermThenKillBeforeCanceled(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	entries := map[int]ProcessInfo{201: {PID: 201, StartTime: "worker-start"}}
	signaler := &recordingProcessGroupSignaler{}
	waiter := &recordingWaiter{}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: entries}, testStoreOptions{
		processGroups: signaler,
		cancelWaiter:  waiter,
		cancelGrace:   250 * time.Millisecond,
	})
	record := &JobRecord{
		JobID:     "job_cancel_live",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(time.Minute)},
		Worker:    ProcessRef{PID: 201, PGID: 301, StartTime: "worker-start"},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	signaler.onSignal = func(pgid int, signal syscall.Signal) {
		if pgid != 301 {
			t.Fatalf("signaled pgid = %d, want 301", pgid)
		}
		loaded, err := store.loadPath(record.StatePath)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.State != StateRunning {
			t.Fatalf("state during %s = %s, want running", signal, loaded.State)
		}
	}

	canceled, err := store.Cancel(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != StateCanceled {
		t.Fatalf("cancel state = %s, want canceled", canceled.State)
	}
	assertProcessGroupSignals(t, signaler.signals, []processGroupSignal{
		{pgid: 301, signal: syscall.SIGTERM},
		{pgid: 301, signal: syscall.SIGKILL},
	})
	assertWaits(t, waiter.waits, []time.Duration{250 * time.Millisecond})
	persisted, err := store.loadPath(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != StateCanceled {
		t.Fatalf("persisted state = %s, want canceled", persisted.State)
	}
}

func TestCancelRunningProcessDyingOnTermSkipsKill(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	entries := map[int]ProcessInfo{202: {PID: 202, StartTime: "worker-start"}}
	signaler := &recordingProcessGroupSignaler{}
	waiter := &recordingWaiter{}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: entries}, testStoreOptions{
		processGroups: signaler,
		cancelWaiter:  waiter,
		cancelGrace:   time.Second,
	})
	record := &JobRecord{
		JobID:     "job_cancel_term",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(time.Minute)},
		Worker:    ProcessRef{PID: 202, PGID: 302, StartTime: "worker-start"},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	signaler.onSignal = func(_ int, signal syscall.Signal) {
		if signal == syscall.SIGTERM {
			delete(entries, 202)
		}
	}

	canceled, err := store.Cancel(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != StateCanceled {
		t.Fatalf("cancel state = %s, want canceled", canceled.State)
	}
	assertProcessGroupSignals(t, signaler.signals, []processGroupSignal{
		{pgid: 302, signal: syscall.SIGTERM},
	})
	assertWaits(t, waiter.waits, []time.Duration{time.Second})
}

func TestCancelRunningDeadProcessPersistsCanceledWithoutSignals(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	signaler := &recordingProcessGroupSignaler{}
	waiter := &recordingWaiter{}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		processGroups: signaler,
		cancelWaiter:  waiter,
		cancelGrace:   time.Second,
	})
	record := &JobRecord{
		JobID:     "job_cancel_dead",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(time.Minute)},
		Worker:    ProcessRef{PID: 203, PGID: 303, StartTime: "worker-start"},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}

	canceled, err := store.Cancel(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != StateCanceled {
		t.Fatalf("cancel state = %s, want canceled", canceled.State)
	}
	assertProcessGroupSignals(t, signaler.signals, nil)
	assertWaits(t, waiter.waits, nil)
	persisted, err := store.loadPath(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != StateCanceled {
		t.Fatalf("persisted state = %s, want canceled", persisted.State)
	}
}

func TestCancelRunningLiveBackendChildSignalsEvenWhenWorkerGone(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	entries := map[int]ProcessInfo{204: {PID: 204, StartTime: "child-start"}}
	signaler := &recordingProcessGroupSignaler{}
	waiter := &recordingWaiter{}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: entries}, testStoreOptions{
		processGroups: signaler,
		cancelWaiter:  waiter,
		cancelGrace:   500 * time.Millisecond,
	})
	record := &JobRecord{
		JobID:           "job_cancel_child",
		State:           StateRunning,
		UpdatedAt:       base,
		Lease:           Lease{ExpiresAt: base.Add(time.Minute)},
		Worker:          ProcessRef{PID: 203, PGID: 303, StartTime: "worker-start"},
		BackendChildPID: 204,
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}

	canceled, err := store.Cancel(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if canceled.State != StateCanceled {
		t.Fatalf("cancel state = %s, want canceled", canceled.State)
	}
	assertProcessGroupSignals(t, signaler.signals, []processGroupSignal{
		{pgid: 303, signal: syscall.SIGTERM},
		{pgid: 303, signal: syscall.SIGKILL},
	})
	assertWaits(t, waiter.waits, []time.Duration{500 * time.Millisecond})
}

func TestLoadLazilyReapsTargetAndListReapsAndQuarantines(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	expired := &JobRecord{
		JobID:     "job_status_reap",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(-time.Second)},
	}
	if err := store.Save(expired); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(expired.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != StateOrphaned {
		t.Fatalf("Load state = %s, want orphaned", loaded.State)
	}

	badPath := filepath.Join(store.Layout().Jobs, "job_list_bad.json")
	if err := os.WriteFile(badPath, []byte("{bad json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.List(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(badPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt record still exists after List: %v", err)
	}
	entries, err := os.ReadDir(store.Layout().Quarantine)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("List did not quarantine corrupt record")
	}
}

func TestLoadDoesNotRunFullStoreReap(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var loaded []string
	fullReaps := 0
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		beforeReap: func() {
			mu.Lock()
			fullReaps++
			mu.Unlock()
		},
		beforeRecordLoad: func(path string) {
			mu.Lock()
			loaded = append(loaded, path)
			mu.Unlock()
		},
	})
	target := &JobRecord{JobID: "job_load_target", State: StateQueued, UpdatedAt: base}
	other := &JobRecord{JobID: "job_load_other", State: StateQueued, UpdatedAt: base}
	for _, record := range []*JobRecord{target, other} {
		if err := store.Save(record); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := store.Load(target.JobID); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fullReaps != 0 {
		t.Fatalf("full reap count = %d, want 0", fullReaps)
	}
	if len(loaded) != 1 || loaded[0] != target.StatePath {
		t.Fatalf("loaded paths = %v, want only %s", loaded, target.StatePath)
	}
}

func TestReapDebouncesWithinInterval(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	fullReaps := 0
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		reapInterval: time.Minute,
		beforeReap:   func() { fullReaps++ },
	})
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if fullReaps != 1 {
		t.Fatalf("full reap count = %d, want 1", fullReaps)
	}
}

func TestReapRunsAfterClockRollback(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: base}
	fullReaps := 0
	store, err := NewStore(StoreConfig{
		Root:         filepath.Join(t.TempDir(), "state"),
		CWD:          t.TempDir(),
		Clock:        clk,
		Processes:    fakeProcessTable{entries: map[int]ProcessInfo{}},
		Retention:    RetentionConfig{TerminalJobTTL: time.Hour, ResultTTL: time.Hour, StaleJobAfter: time.Minute},
		ReapInterval: time.Minute,
		GCInterval:   time.Minute,
		BeforeReap:   func() { fullReaps++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if fullReaps != 1 {
		t.Fatalf("initial full reap count = %d, want 1", fullReaps)
	}
	// A rollback expires the debounce rather than suspending reconciliation.
	clk.now = base.Add(-time.Hour)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if fullReaps != 2 {
		t.Fatalf("full reap count after clock rollback = %d, want 2", fullReaps)
	}
	// Debounce still applies at the rolled-back time.
	clk.now = clk.now.Add(time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if fullReaps != 2 {
		t.Fatalf("in-window reap after rollback ran; count = %d, want 2", fullReaps)
	}
}

func TestGCRunsAfterClockRollback(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	rolledBack := base.Add(-time.Hour)
	clk := &fakeClock{now: base}
	store, err := NewStore(StoreConfig{
		Root:         filepath.Join(t.TempDir(), "state"),
		CWD:          t.TempDir(),
		Clock:        clk,
		Processes:    fakeProcessTable{entries: map[int]ProcessInfo{}},
		Retention:    RetentionConfig{TerminalJobTTL: time.Hour, ResultTTL: time.Hour, StaleJobAfter: time.Minute},
		ReapInterval: time.Nanosecond,
		GCInterval:   time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !store.lastGC.Equal(base) {
		t.Fatalf("initial gc time = %v, want %v", store.lastGC, base)
	}

	// A full reap within the normal gc interval leaves the throttle timestamp
	// unchanged.
	clk.now = base.Add(time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !store.lastGC.Equal(base) {
		t.Fatalf("in-window gc updated its throttle time to %v, want %v", store.lastGC, base)
	}

	// Clock rollback expires the gc throttle and records a new gc run.
	clk.now = rolledBack
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !store.lastGC.Equal(rolledBack) {
		t.Fatalf("gc time after rollback = %v, want %v", store.lastGC, rolledBack)
	}

	// The throttle is again active within the rolled-back time window.
	clk.now = rolledBack.Add(time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !store.lastGC.Equal(rolledBack) {
		t.Fatalf("in-window gc after rollback updated its throttle time to %v, want %v", store.lastGC, rolledBack)
	}
}

func TestReapSingleFlightsConcurrentLists(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	waiting := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var waitOnce sync.Once
	var mu sync.Mutex
	fullReaps := 0
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		reapInterval: time.Minute,
		beforeReap: func() {
			mu.Lock()
			fullReaps++
			mu.Unlock()
			startOnce.Do(func() { close(started) })
			<-release
		},
		onReapWait: func() { waitOnce.Do(func() { close(waiting) }) },
	})
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() {
		_, err := store.List()
		first <- err
	}()
	<-started
	go func() {
		_, err := store.List()
		second <- err
	}()
	<-waiting
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fullReaps != 1 {
		t.Fatalf("full reap count = %d, want 1", fullReaps)
	}
}

func TestReapSharesFailedPassWithWaiters(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	started := make(chan struct{})
	waitersReady := make(chan struct{})
	release := make(chan struct{})
	const waiterCount = 3
	var startOnce sync.Once
	var mu sync.Mutex
	fullReaps := 0
	waiters := 0
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		beforeReap: func() {
			mu.Lock()
			fullReaps++
			mu.Unlock()
			startOnce.Do(func() { close(started) })
			<-release
		},
		onReapWait: func() {
			mu.Lock()
			defer mu.Unlock()
			waiters++
			if waiters == waiterCount {
				close(waitersReady)
			}
		},
	})
	if err := os.Remove(store.Layout().Jobs); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() { first <- store.Reap() }()
	<-started
	results := make(chan error, waiterCount)
	for range waiterCount {
		go func() { results <- store.Reap() }()
	}
	<-waitersReady
	close(release)

	firstErr := <-first
	if !errors.Is(firstErr, os.ErrNotExist) {
		t.Fatalf("first reap error = %v, want missing jobs directory", firstErr)
	}
	for range waiterCount {
		if err := <-results; err != firstErr {
			t.Fatalf("waiter reap error = %v, want shared error %v", err, firstErr)
		}
	}
	mu.Lock()
	if fullReaps != 1 {
		mu.Unlock()
		t.Fatalf("full reap count after waiters = %d, want 1", fullReaps)
	}
	mu.Unlock()

	if err := os.Mkdir(store.Layout().Jobs, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if fullReaps != 2 {
		t.Fatalf("full reap count after new caller = %d, want 2", fullReaps)
	}
}

func TestReapCachesProcessLookupsPerPass(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	processes := &countingProcessTable{
		entries: map[int]ProcessInfo{101: {PID: 101, StartTime: "same"}},
		lookups: make(map[int]int),
	}
	store := newTestStore(t, base, processes)
	for _, jobID := range []string{"job_lookup_one", "job_lookup_two"} {
		if err := store.Save(&JobRecord{
			JobID: jobID, State: StateRunning, UpdatedAt: base,
			Worker: ProcessRef{PID: 101, StartTime: "same"},
			Lease:  Lease{ExpiresAt: base.Add(time.Hour)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if got := processes.lookupCount(101); got != 1 {
		t.Fatalf("process lookups for shared pid = %d, want 1", got)
	}
}

func TestGCReusesReapParsedRecords(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	loads := make(map[string]int)
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		beforeRecordLoad: func(path string) {
			mu.Lock()
			loads[path]++
			mu.Unlock()
		},
	})
	record := &JobRecord{JobID: "job_gc_single_parse", State: StateCompleted, UpdatedAt: base}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got := loads[record.StatePath]; got != 1 {
		t.Fatalf("record parses during reap and gc = %d, want 1", got)
	}
}

func TestGCDoesNotDeleteRefreshedTerminalCandidate(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	old := base.Add(-2 * time.Hour)
	var store *Store
	var triggerPath string
	refreshed := false
	store = newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		beforeRecordLoad: func(path string) {
			if path != triggerPath || refreshed {
				return
			}
			refreshed = true
			_, err := store.Update("job_gc_refresh", func(record *JobRecord) (bool, error) {
				return true, record.Transition(StateCompleted, base)
			})
			if err != nil {
				t.Errorf("refresh terminal record: %v", err)
			}
		},
	})
	logPath := filepath.Join(store.Layout().Logs, "job_gc_refresh.stdout.log")
	if err := os.WriteFile(logPath, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	stale := &JobRecord{
		JobID:     "job_gc_refresh",
		State:     StateReaped,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: logPath},
	}
	if err := store.Save(stale); err != nil {
		t.Fatal(err)
	}
	trigger := &JobRecord{JobID: "job_gc_trigger", State: StateQueued, UpdatedAt: base}
	if err := store.Save(trigger); err != nil {
		t.Fatal(err)
	}
	triggerPath = trigger.StatePath

	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("test hook did not refresh the deletion candidate")
	}
	loaded, err := store.loadPath(stale.StatePath)
	if err != nil {
		t.Fatalf("refreshed record was deleted: %v", err)
	}
	if loaded.State != StateCompleted || !loaded.UpdatedAt.Equal(base) {
		t.Fatalf("refreshed record = state %s at %s, want completed at %s", loaded.State, loaded.UpdatedAt, base)
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("refreshed record log was deleted: %v", err)
	}
}

func TestGCDeletesFreshTerminalCandidateStillPastTTL(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	old := base.Add(-2 * time.Hour)
	var store *Store
	var triggerPath string
	refreshed := false
	store = newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		beforeRecordLoad: func(path string) {
			if path != triggerPath || refreshed {
				return
			}
			refreshed = true
			_, err := store.Update("job_gc_expired", func(record *JobRecord) (bool, error) {
				record.UpdatedAt = old
				return true, nil
			})
			if err != nil {
				t.Errorf("refresh expired terminal record: %v", err)
			}
		},
	})
	logPath := filepath.Join(store.Layout().Logs, "job_gc_expired.stdout.log")
	if err := os.WriteFile(logPath, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	expired := &JobRecord{
		JobID:     "job_gc_expired",
		State:     StateCompleted,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: logPath},
	}
	if err := store.Save(expired); err != nil {
		t.Fatal(err)
	}
	trigger := &JobRecord{JobID: "job_gc_trigger", State: StateQueued, UpdatedAt: base}
	if err := store.Save(trigger); err != nil {
		t.Fatal(err)
	}
	triggerPath = trigger.StatePath

	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !refreshed {
		t.Fatal("test hook did not rewrite the deletion candidate")
	}
	for _, path := range []string{expired.StatePath, logPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expired terminal artifact %s still exists or stat failed: %v", path, err)
		}
	}
}

func TestGCThrottlesBetweenFullReaps(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: base}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		reapInterval: time.Nanosecond,
		gcInterval:   time.Minute,
	})
	store.clock = clock
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(store.Layout().Results, "stale-result.txt")
	if err := os.WriteFile(resultPath, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := base.Add(-2 * time.Hour)
	if err := os.Chtimes(resultPath, old, old); err != nil {
		t.Fatal(err)
	}
	clock.now = base.Add(time.Second)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resultPath); err != nil {
		t.Fatalf("gc ran before its interval elapsed: %v", err)
	}
	clock.now = base.Add(time.Minute)
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
		t.Fatalf("gc did not run after its interval: %v", err)
	}
}

func TestReaperDoesNotClobberRefreshedHeartbeat(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	stale := &JobRecord{
		JobID:     "job_live_refresh",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(time.Hour)},
		Worker:    ProcessRef{PID: 200, StartTime: "old"},
	}
	if err := store.Save(stale); err != nil {
		t.Fatal(err)
	}

	oldHook := atomicWriteFileCrashHook
	defer func() { atomicWriteFileCrashHook = oldHook }()
	saveDone := make(chan error, 1)
	var once sync.Once
	saveCompletedInHook := false
	var saveErr error
	atomicWriteFileCrashHook = func(stage, path string) {
		if stage != "after-temp-sync" || !strings.HasPrefix(filepath.Base(path), "job_live_refresh.json.tmp-") {
			return
		}
		once.Do(func() {
			go func() {
				fresh := &JobRecord{
					JobID:     "job_live_refresh",
					State:     StateRunning,
					UpdatedAt: base.Add(time.Second),
					Lease:     Lease{ExpiresAt: base.Add(time.Hour)},
					Worker:    ProcessRef{PID: 200, StartTime: "fresh"},
				}
				saveDone <- store.Save(fresh)
			}()
			select {
			case saveErr = <-saveDone:
				saveCompletedInHook = true
			case <-time.After(50 * time.Millisecond):
			}
		})
	}

	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if !saveCompletedInHook {
		saveErr = <-saveDone
	}
	if saveErr != nil {
		t.Fatal(saveErr)
	}
	got, err := store.loadPath(stale.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateRunning || got.Worker.StartTime != "fresh" {
		t.Fatalf("reaper clobbered refreshed record: %+v", got)
	}
}

func TestListDoesNotQuarantineConcurrentFreshSave(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	jobID := "job_list_fresh_save"
	path := filepath.Join(store.Layout().Jobs, jobID+".json")

	oldAfterReapHook := listAfterReapHook
	oldHook := listLoadErrorHook
	defer func() {
		listAfterReapHook = oldAfterReapHook
		listLoadErrorHook = oldHook
	}()
	saveDone := make(chan error, 1)
	var once sync.Once
	saveCompletedBeforeQuarantine := false
	listAfterReapHook = func() {
		if err := os.WriteFile(path, []byte("{bad json"), 0o600); err != nil {
			t.Fatalf("write corrupt record after Reap: %v", err)
		}
	}
	listLoadErrorHook = func(hookPath string, _ error) {
		if hookPath != path {
			return
		}
		once.Do(func() {
			go func() {
				saveDone <- store.Save(&JobRecord{
					JobID:     jobID,
					State:     StateQueued,
					UpdatedAt: base.Add(time.Second),
				})
			}()
			select {
			case err := <-saveDone:
				saveCompletedBeforeQuarantine = true
				saveDone <- err
			case <-time.After(50 * time.Millisecond):
			}
		})
	}

	if _, err := store.List(); err != nil {
		t.Fatal(err)
	}
	saveErr := <-saveDone
	if saveErr != nil {
		t.Fatal(saveErr)
	}
	if saveCompletedBeforeQuarantine {
		t.Fatal("Save completed while List was between corrupt read and quarantine")
	}
	loaded, err := store.loadPath(path)
	if err != nil {
		t.Fatalf("fresh save was quarantined or left unreadable: %v", err)
	}
	if loaded.JobID != jobID || loaded.State != StateQueued {
		t.Fatalf("loaded record = %+v, want fresh queued record", loaded)
	}
}

func TestGCIgnoresLogPathsOutsideLayout(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	outside := filepath.Join(t.TempDir(), "must-not-delete.log")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := base.Add(-2 * time.Hour)
	record := &JobRecord{
		JobID:     "job_malicious_logs",
		State:     StateCompleted,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: outside, Stderr: filepath.Join("..", "escape.log")},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	record.UpdatedAt = old
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("outside log path was removed or became inaccessible: %v", err)
	}
	if _, err := os.Stat(record.StatePath); !os.IsNotExist(err) {
		t.Fatalf("expired record still exists or unexpected err: %v", err)
	}
}

func TestQuarantineCorruptRecord(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	badPath := filepath.Join(store.Layout().Jobs, "job_bad.json")
	if err := os.WriteFile(badPath, []byte("{bad json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(badPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt record still exists: %v", err)
	}
	entries, err := os.ReadDir(store.Layout().Quarantine)
	if err != nil {
		t.Fatal(err)
	}
	var foundDiag bool
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".diagnostic.txt") {
			foundDiag = true
			b, err := os.ReadFile(filepath.Join(store.Layout().Quarantine, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(b), "job_bad.json") || !strings.Contains(string(b), "failure:") {
				t.Fatalf("diagnostic missing details: %s", b)
			}
		}
	}
	if !foundDiag {
		t.Fatal("missing quarantine diagnostic")
	}
}

func TestAtomicRecordWriteSurvivesCrashMidWrite(t *testing.T) {
	if os.Getenv("AGENTBUS_ATOMIC_CRASH_CHILD") == "1" {
		atomicWriteFileCrashHook = func(stage, _ string) {
			if stage == "after-temp-sync" {
				os.Exit(23)
			}
		}
		if err := atomicWriteFile(os.Getenv("AGENTBUS_ATOMIC_TARGET"), []byte(os.Getenv("AGENTBUS_ATOMIC_NEW_DATA")), 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	record := &JobRecord{JobID: "job_atomic", State: StateQueued}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAtomicRecordWriteSurvivesCrashMidWrite$")
	cmd.Env = append(os.Environ(),
		"AGENTBUS_ATOMIC_CRASH_CHILD=1",
		"AGENTBUS_ATOMIC_TARGET="+record.StatePath,
		"AGENTBUS_ATOMIC_NEW_DATA={\"jobId\":\"job_atomic\",\"state\":\"starting\"}\n",
	)
	err = cmd.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 23 {
		t.Fatalf("crash child err = %v, want exit 23", err)
	}
	after, err := os.ReadFile(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("target changed across pre-rename crash:\nbefore=%s\nafter=%s", before, after)
	}
	loaded, err := store.Load(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != StateQueued {
		t.Fatalf("state = %s, want queued", loaded.State)
	}
	jobs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs = %d, want 1", len(jobs))
	}
}

func TestGCRetention(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	logPath := filepath.Join(store.Layout().Logs, "job_gc.stdout.log")
	inputPath := filepath.Join(store.Layout().Inputs, "job_gc.json")
	resultPath := filepath.Join(store.Layout().Results, "job_gc.txt")
	for _, path := range []string{logPath, inputPath, resultPath} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := base.Add(-2 * time.Hour)
	if err := os.Chtimes(resultPath, old, old); err != nil {
		t.Fatal(err)
	}
	record := &JobRecord{
		JobID:     "job_gc",
		State:     StateCompleted,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: logPath},
		Result:    &ResultInfo{ResultPath: resultPath},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	record.UpdatedAt = old
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(store.Layout().Jobs, "job_gc.json")
	for _, path := range []string{record.StatePath, recordPath, logPath, inputPath, resultPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still exists or unexpected err: %v", path, err)
		}
	}
}

func TestTerminalUpdateSweepsJobInputImmediately(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	inputPath := filepath.Join(store.Layout().Inputs, "job_input_done.json")
	if err := os.WriteFile(inputPath, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	record := &JobRecord{
		JobID:     "job_input_done",
		State:     StateRunning,
		UpdatedAt: base,
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Update(record.JobID, func(record *JobRecord) (bool, error) {
		return true, record.Transition(StateCompleted, base)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
		t.Fatalf("terminal input still exists or unexpected err: %v", err)
	}
	if _, err := os.Stat(record.StatePath); err != nil {
		t.Fatalf("terminal record was removed before TTL: %v", err)
	}
}

func TestGCKeepsReferencedResultUntilResultTTLAfterRecordTTL(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		retention: RetentionConfig{
			TerminalJobTTL: time.Hour,
			ResultTTL:      14 * 24 * time.Hour,
			StaleJobAfter:  time.Minute,
		},
	})
	logPath := filepath.Join(store.Layout().Logs, "job_result_retained.stdout.log")
	inputPath := filepath.Join(store.Layout().Inputs, "job_result_retained.json")
	resultPath := filepath.Join(store.Layout().Results, "job_result_retained.txt")
	for _, path := range []string{logPath, inputPath, resultPath} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := base.Add(-2 * time.Hour)
	if err := os.Chtimes(resultPath, old, old); err != nil {
		t.Fatal(err)
	}
	record := &JobRecord{
		JobID:     "job_result_retained",
		State:     StateCompleted,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: logPath},
		Result:    &ResultInfo{ResultPath: resultPath},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	record.UpdatedAt = old
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{record.StatePath, logPath, inputPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still exists or unexpected err: %v", path, err)
		}
	}
	if _, err := os.Stat(resultPath); err != nil {
		t.Fatalf("referenced result was removed before ResultTTL: %v", err)
	}
}

func TestGCRemovesReferencedResultAtResultTTLBeforeRecordTTL(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}}, testStoreOptions{
		retention: RetentionConfig{
			TerminalJobTTL: 14 * 24 * time.Hour,
			ResultTTL:      time.Hour,
			StaleJobAfter:  time.Minute,
		},
	})
	logPath := filepath.Join(store.Layout().Logs, "job_result_expired.stdout.log")
	inputPath := filepath.Join(store.Layout().Inputs, "job_result_expired.json")
	resultPath := filepath.Join(store.Layout().Results, "job_result_expired.txt")
	for _, path := range []string{logPath, inputPath, resultPath} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := base.Add(-2 * time.Hour)
	if err := os.Chtimes(resultPath, old, old); err != nil {
		t.Fatal(err)
	}
	record := &JobRecord{
		JobID:     "job_result_expired",
		State:     StateCompleted,
		UpdatedAt: old,
		LogPaths:  LogPaths{Stdout: logPath},
		Result:    &ResultInfo{ResultPath: resultPath},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	record.UpdatedAt = old
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
		t.Fatalf("expired result still exists or unexpected err: %v", err)
	}
	if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
		t.Fatalf("terminal input still exists or unexpected err: %v", err)
	}
	for _, path := range []string{record.StatePath, logPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s was removed before TerminalJobTTL: %v", path, err)
		}
	}
}

func TestGCKeepsResultReferencedByRetainedJob(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, base, fakeProcessTable{entries: map[int]ProcessInfo{}})
	resultPath := filepath.Join(store.Layout().Results, "job_keep_result.txt")
	if err := os.WriteFile(resultPath, []byte("authoritative"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := base.Add(-2 * time.Hour)
	if err := os.Chtimes(resultPath, old, old); err != nil {
		t.Fatal(err)
	}
	record := &JobRecord{
		JobID:     "job_keep_result",
		State:     StateCompleted,
		UpdatedAt: base,
		Result:    &ResultInfo{ResultPath: resultPath},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(resultPath); err != nil {
		t.Fatalf("referenced result was removed: %v", err)
	}
}

func TestResultSpillAndEventTruncation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	raw := []byte("abcdef")
	info, err := store.WriteResult("job_inline", raw, len(raw)+1)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if info.SHA256 != hex.EncodeToString(sum[:]) || info.Bytes != int64(len(raw)) || info.Text != string(raw) {
		t.Fatalf("bad inline result: %+v", info)
	}
	spilled, err := store.WriteResult("job_spill", raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	if spilled.Text != "" {
		t.Fatalf("spilled-only result included text: %+v", spilled)
	}
	onDisk, err := os.ReadFile(spilled.ResultPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(onDisk) != string(raw) {
		t.Fatalf("result bytes = %q", onDisk)
	}
	ev := TruncateEventText([]byte("abcdef"), 3)
	if ev.Text != "abc" || !ev.Truncated {
		t.Fatalf("event = %+v", ev)
	}
}

func TestCappedLogWriter(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "log.txt")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	capBytes := int64(len(truncationMarker()) + 5)
	w, err := NewCappedLogWriter(path, capBytes)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := w.Write([]byte("abcdefghi")); err != nil || n != 9 {
		t.Fatalf("Write n=%d err=%v", n, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) > capBytes {
		t.Fatalf("log size = %d, want <= %d", len(b), capBytes)
	}
	if got := string(b); !strings.HasPrefix(got, "abcde") || !strings.Contains(got, "[agentbus: log truncated]") {
		t.Fatalf("log contents = %q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("log mode = %o, want 600", got)
	}
}

func TestLinuxProcStatStartTimeWithSpacesInComm(t *testing.T) {
	t.Parallel()
	stat := "123 (name with spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987654 20"
	got, ok := linuxProcStatStartTime(stat)
	if !ok {
		t.Fatal("linuxProcStatStartTime returned !ok")
	}
	if got != "987654" {
		t.Fatalf("start time = %q, want 987654", got)
	}
}

type processTableFunc func(pid int) (ProcessInfo, bool, error)

func (f processTableFunc) Lookup(pid int) (ProcessInfo, bool, error) {
	return f(pid)
}
