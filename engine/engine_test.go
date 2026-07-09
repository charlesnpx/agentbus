package engine

import (
	"crypto/sha256"
	"encoding/hex"
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

type testStoreOptions struct {
	processGroups ProcessGroupSignaler
	cancelWaiter  Waiter
	cancelGrace   time.Duration
}

func newTestStore(t *testing.T, now time.Time, pt ProcessTable) *Store {
	return newTestStoreWithOptions(t, now, pt, testStoreOptions{})
}

func newTestStoreWithOptions(t *testing.T, now time.Time, pt ProcessTable, opts testStoreOptions) *Store {
	t.Helper()
	store, err := NewStore(StoreConfig{
		Root:          filepath.Join(t.TempDir(), "state"),
		CWD:           t.TempDir(),
		Clock:         &fakeClock{now: now},
		Processes:     pt,
		ProcessGroups: opts.processGroups,
		CancelWaiter:  opts.cancelWaiter,
		CancelGrace:   opts.cancelGrace,
		Retention: RetentionConfig{
			TerminalJobTTL: time.Hour,
			ResultTTL:      time.Hour,
			StaleJobAfter:  time.Minute,
		},
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
			name: "expired lease becomes orphaned",
			job:  JobRecord{JobID: "job_lease", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(-time.Second)}, Worker: ProcessRef{PID: 101, StartTime: "a"}},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{101: {PID: 101, StartTime: "a"}}},
			want: StateOrphaned,
		},
		{
			name: "stale queued reaped",
			job:  JobRecord{JobID: "job_stale", State: StateQueued, UpdatedAt: base.Add(-2 * time.Minute)},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{}},
			want: StateReaped,
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
			name: "backend child crash orphaned",
			job:  JobRecord{JobID: "job_child", State: StateRunning, UpdatedAt: base, Lease: Lease{ExpiresAt: base.Add(time.Minute)}, BackendChildPID: 104},
			pt:   fakeProcessTable{entries: map[int]ProcessInfo{}},
			want: StateOrphaned,
		},
		{
			name: "orphaned reaped",
			job:  JobRecord{JobID: "job_orphan", State: StateOrphaned, UpdatedAt: base},
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
		})
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

func TestLoadAndListRunReaperAndQuarantine(t *testing.T) {
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
	for _, path := range []string{record.StatePath, logPath, inputPath, resultPath} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s still exists or unexpected err: %v", path, err)
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
