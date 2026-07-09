package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func newTestStore(t *testing.T, now time.Time, pt ProcessTable) *Store {
	t.Helper()
	store, err := NewStore(StoreConfig{
		Root:      filepath.Join(t.TempDir(), "state"),
		CWD:       t.TempDir(),
		Clock:     &fakeClock{now: now},
		Processes: pt,
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
			loaded, err := store.Load(job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if !loaded.Lease.ExpiresAt.IsZero() && loaded.Lease.Expired != !base.Before(loaded.Lease.ExpiresAt) {
				t.Fatalf("computed lease expired = %v", loaded.Lease.Expired)
			}
			if err := store.Reap(); err != nil {
				t.Fatal(err)
			}
			got, err := store.Load(job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tt.want {
				t.Fatalf("state = %s, want %s", got.State, tt.want)
			}
		})
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

func TestAtomicRecordWriteIgnoresMidWriteTemp(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	record := &JobRecord{JobID: "job_atomic", State: StateQueued}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.Layout().Jobs, "job_atomic.json.tmp-dead"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	record.State = StateStarting
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != StateStarting {
		t.Fatalf("state = %s, want starting", loaded.State)
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
	w, err := NewCappedLogWriter(path, 5)
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
