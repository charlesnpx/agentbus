package jobstore

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

func TestSubmitReplaySkipsDeletedCWD(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-replay", RequestID: "request-replay"}
	taskSpec := testTaskSpec("same exact task spec")
	cwd := filepath.Join(t.TempDir(), "deleted-workspace")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	original, deduplicated, err := store.SubmitTx(key, taskSpec, func(string) (Record, error) {
		return Record{Backend: "codex", CWD: cwd}, nil
	})
	if err != nil || deduplicated {
		t.Fatalf("first SubmitTx = (%+v, deduplicated=%t, %v), want new record", original, deduplicated, err)
	}
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}

	factoryCalled := false
	replayed, deduplicated, err := store.SubmitTx(key, taskSpec, func(string) (Record, error) {
		factoryCalled = true
		if _, err := os.Stat(cwd); err != nil {
			t.Fatalf("replay tried to stat deleted cwd: %v", err)
		}
		return Record{Backend: "must-not-be-created"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !deduplicated || replayed.JobID != original.JobID {
		t.Fatalf("replay = (%+v, deduplicated=%t), want original %q and true", replayed, deduplicated, original.JobID)
	}
	if factoryCalled {
		t.Fatal("same-hash replay invoked new-record factory")
	}
}

func TestSubmitReplayMatchesOnlyCanonicalTaskSpec(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-conflict", RequestID: "request-conflict"}
	firstTaskSpec := testTaskSpec("one")
	expectedTaskSpec := append([]byte(nil), firstTaskSpec...)
	secondTaskSpec := testTaskSpec("two")
	original, _, err := store.SubmitTx(key, firstTaskSpec, func(string) (Record, error) {
		copy(firstTaskSpec, secondTaskSpec)
		return Record{Backend: "codex", TaskSpec: testTaskSpec("factory supplied a different task")}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(original.TaskSpec), string(expectedTaskSpec); got != want {
		t.Fatalf("stored task spec = %s, want canonical submit task %s", got, want)
	}
	stored, err := store.Get(original.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(stored.TaskSpec), string(expectedTaskSpec); got != want {
		t.Fatalf("durable task spec = %s, want canonical submit task %s", got, want)
	}
	factoryCalled := false
	_, _, err = store.SubmitTx(key, secondTaskSpec, func(string) (Record, error) {
		factoryCalled = true
		return Record{Backend: "must-not-be-created"}, nil
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting SubmitTx error = %v, want ErrConflict", err)
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.ExistingJobID != original.JobID {
		t.Fatalf("conflict = %+v, want existing job %q", conflict, original.JobID)
	}
	if factoryCalled {
		t.Fatal("different-hash replay invoked new-record factory")
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].JobID != original.JobID {
		t.Fatalf("records after conflict = %+v, want only %q", records, original.JobID)
	}
	replayed, deduplicated, err := store.SubmitTx(key, expectedTaskSpec, func(string) (Record, error) {
		t.Fatal("matching replay invoked new-record factory")
		return Record{}, nil
	})
	if err != nil || !deduplicated || replayed.JobID != original.JobID {
		t.Fatalf("matching replay after conflict = (%+v, %t, %v), want original replay", replayed, deduplicated, err)
	}
}

func TestSubmitNewRecordForcesQueuedState(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	finished := time.Now().UTC()
	record, deduplicated, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-queued", RequestID: "request-queued"},
		testTaskSpec("queued admission"),
		func(string) (Record, error) {
			return Record{
				Backend:    "codex",
				State:      protocol.PublicStateCompleted,
				FinishedAt: &finished,
			}, nil
		},
	)
	if err != nil {
		t.Fatalf("SubmitTx = %v", err)
	}
	if deduplicated {
		t.Fatal("SubmitTx reported a new record as deduplicated")
	}
	if record.State != protocol.PublicStateQueued {
		t.Fatalf("submitted state = %q, want %q", record.State, protocol.PublicStateQueued)
	}
	persisted, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != protocol.PublicStateQueued {
		t.Fatalf("persisted state = %q, want %q", persisted.State, protocol.PublicStateQueued)
	}
}

func TestSubmitFactoryErrorLeavesJobAndBindingAbsent(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-factory-error", RequestID: "request-factory-error"}
	factoryErr := errors.New("backend unavailable")
	var generatedID string
	record, deduplicated, err := store.SubmitTx(key, testTaskSpec("factory validation"), func(id string) (Record, error) {
		generatedID = id
		return Record{}, factoryErr
	})
	if !errors.Is(err, factoryErr) {
		t.Fatalf("SubmitTx factory error = %v, want %v", err, factoryErr)
	}
	if deduplicated {
		t.Fatal("SubmitTx factory error reported deduplicated")
	}
	if record.JobID != "" {
		t.Fatalf("SubmitTx factory error returned job %q, want zero record", record.JobID)
	}
	if generatedID == "" {
		t.Fatal("factory did not receive generated job ID")
	}
	if err := store.view(func(tx *bolt.Tx) error {
		requests, jobs, err := requiredBuckets(tx)
		if err != nil {
			return err
		}
		if jobs.Get([]byte(generatedID)) != nil {
			t.Errorf("job %q written despite factory error", generatedID)
		}
		if requests.Get(key.storageKey()) != nil {
			t.Errorf("request binding for %q written despite factory error", key.storageKey())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedJobIDIsOpaque(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-opaque-sentinel", RequestID: "request-opaque-sentinel"}
	record, _, err := store.SubmitTx(key, testTaskSpec("opaque task"), func(string) (Record, error) {
		return Record{Backend: "codex", CWD: "/workspace/opaque-sentinel"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateJobID(record.JobID); err != nil {
		t.Fatalf("job ID %q is not valid: %v", record.JobID, err)
	}
	for _, secret := range []string{key.WorkspaceKey, key.RequestID, "opaque-sentinel", "/workspace"} {
		if strings.Contains(record.JobID, secret) {
			t.Fatalf("job ID %q embeds %q", record.JobID, secret)
		}
	}
}

func TestMarkStartingTransitionsQueuedAndRefusesTerminal(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-starting", RequestID: "request-starting"}
	taskSpec := testTaskSpec("durable starting transition")
	record, _, err := store.SubmitTx(key, taskSpec, func(string) (Record, error) {
		return Record{Backend: "codex", Model: "gpt-test", CWD: "/workspace/starting"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	started, err := store.MarkStarting(record.JobID)
	if err != nil {
		t.Fatalf("MarkStarting = %v", err)
	}
	if started.State != protocol.PublicStateRunning || !started.Starting || started.StartedAt == nil {
		t.Fatalf("started record = %+v, want private starting running record", started)
	}
	if started.Backend != record.Backend || started.CWD != record.CWD || string(started.TaskSpec) != string(record.TaskSpec) {
		t.Fatalf("MarkStarting changed immutable request fields: got %+v, want backend=%q cwd=%q taskSpec=%s", started, record.Backend, record.CWD, record.TaskSpec)
	}
	if _, err := store.MarkStarting(record.JobID); !errors.Is(err, ErrStarting) {
		t.Fatalf("second MarkStarting error = %v, want ErrStarting", err)
	}

	replayed, deduplicated, err := store.SubmitTx(key, taskSpec, func(string) (Record, error) {
		t.Fatal("matching replay invoked new-record factory")
		return Record{}, nil
	})
	if err != nil || !deduplicated || replayed.JobID != record.JobID {
		t.Fatalf("matching replay after MarkStarting = (%+v, %t, %v), want original replay", replayed, deduplicated, err)
	}

	if _, err := store.MarkTerminal(record.JobID, TerminalUpdate{
		State:   protocol.PublicStateCompleted,
		Cleanup: protocol.CleanupClean,
	}); err != nil {
		t.Fatalf("MarkTerminal = %v", err)
	}
	if _, err := store.MarkStarting(record.JobID); !errors.Is(err, ErrTerminal) {
		t.Fatalf("MarkStarting terminal record error = %v, want ErrTerminal", err)
	}
}

func TestRecordProcessClaimUsesSeparateTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	key := RequestKey{WorkspaceKey: "workspace-claim", RequestID: "request-claim"}
	taskSpec := testTaskSpec("separate claim transaction")
	record, _, err := store.SubmitTx(key, taskSpec, func(string) (Record, error) {
		return Record{Backend: "codex", Model: "gpt-test", CWD: "/workspace/claim"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatalf("MarkStarting = %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	claim := ProcessClaim{PID: 4242, PGID: 4242, StartToken: "claim-start-token"}
	if _, err := store.RecordProcessClaim(record.JobID, claim); err == nil {
		t.Fatal("RecordProcessClaim succeeded after its database was closed")
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, reopened)
	persisted, err := reopened.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.State != protocol.PublicStateRunning || !persisted.Starting || persisted.ProcessClaim != nil {
		t.Fatalf("record after failed claim transaction = %+v, want intact starting record", persisted)
	}
	if persisted.Backend != record.Backend || persisted.CWD != record.CWD || string(persisted.TaskSpec) != string(record.TaskSpec) {
		t.Fatalf("failed claim changed immutable request fields: got %+v, want backend=%q cwd=%q taskSpec=%s", persisted, record.Backend, record.CWD, record.TaskSpec)
	}

	claimed, err := reopened.RecordProcessClaim(record.JobID, claim)
	if err != nil {
		t.Fatalf("RecordProcessClaim = %v", err)
	}
	if claimed.State != protocol.PublicStateRunning || claimed.Starting || claimed.ProcessClaim == nil || *claimed.ProcessClaim != claim {
		t.Fatalf("claimed record = %+v, want running record with %+v", claimed, claim)
	}
	if claimed.Backend != record.Backend || claimed.CWD != record.CWD || string(claimed.TaskSpec) != string(record.TaskSpec) {
		t.Fatalf("RecordProcessClaim changed immutable request fields: got %+v, want backend=%q cwd=%q taskSpec=%s", claimed, record.Backend, record.CWD, record.TaskSpec)
	}
	replayed, deduplicated, err := reopened.SubmitTx(key, taskSpec, func(string) (Record, error) {
		t.Fatal("matching replay invoked new-record factory")
		return Record{}, nil
	})
	if err != nil || !deduplicated || replayed.JobID != record.JobID {
		t.Fatalf("matching replay after RecordProcessClaim = (%+v, %t, %v), want original replay", replayed, deduplicated, err)
	}
}

func TestV0131RecordDecodesWithoutInventedRetirementEvidence(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	const jobID = "job_0123456789abcdef0123456789abcdef"
	legacy := []byte(`{"jobId":"job_0123456789abcdef0123456789abcdef","workspaceKey":"legacy-workspace","requestId":"legacy-request","backend":"codex","backendSessionId":"thread-v0131","state":"running","cleanup":"clean","createdAt":"2026-09-05T12:00:00Z","updatedAt":"2026-09-05T12:00:00Z","diagnostics":[],"contract":{"violations":[]}}`)
	if err := store.update(func(tx *bolt.Tx) error {
		jobs := tx.Bucket(bucketJobs)
		if jobs == nil {
			return errors.New("jobs bucket is missing")
		}
		return jobs.Put([]byte(jobID), legacy)
	}); err != nil {
		t.Fatal(err)
	}

	decoded, err := store.Get(jobID)
	if err != nil {
		t.Fatalf("decode v0.13.1 record: %v", err)
	}
	if decoded.State != protocol.PublicStateRunning || decoded.BackendSessionID != "thread-v0131" {
		t.Fatalf("decoded v0.13.1 record = %#v", decoded)
	}
	receipt := RetirementReceipt{
		BackendSessionID: "thread-current",
		Diagnostics:      []string{"turn retirement observation"},
	}
	retired, err := store.RetireTurn(jobID, receipt)
	if err != nil {
		t.Fatalf("RetireTurn current receipt: %v", err)
	}
	if retired.BackendSessionID != "thread-v0131" || retired.Retirement == nil || retired.Retirement.BackendSessionID != "thread-current" || len(retired.Retirement.Diagnostics) != 1 {
		t.Fatalf("nonterminal receipt projection = %#v, want legacy field unchanged and current receipt retained", retired)
	}
	if _, err := store.RetireTurn(jobID, receipt); err != nil {
		t.Fatalf("replay aggregate receipt: %v", err)
	}
	terminal, err := store.MarkTerminal(jobID, TerminalUpdate{
		State:       protocol.PublicStateUnknown,
		Cleanup:     protocol.CleanupClean,
		Diagnostics: []string{"restart reconciliation: no relaunch"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != protocol.PublicStateUnknown || terminal.Cleanup != protocol.CleanupClean || terminal.BackendSessionID != "thread-current" || len(terminal.Diagnostics) != 2 || terminal.Diagnostics[0] != "turn retirement observation" || terminal.Diagnostics[1] != "restart reconciliation: no relaunch" {
		t.Fatalf("terminalized v0.13.1 record = %#v, want current receipt projection and new recovery observation", terminal)
	}
}

func TestMarkedTerminalSuppressesStaleRetirementProjection(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	record, _, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-incomplete-evidence", RequestID: "request-incomplete-evidence"},
		testTaskSpec("terminalize incomplete evidence"),
		func(string) (Record, error) { return Record{Backend: "codex"}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkEvidenceIncomplete(record.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireTurn(record.JobID, RetirementReceipt{BackendSessionID: "thread-stale"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClearEvidenceIncomplete(record.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkEvidenceIncomplete(record.JobID); err != nil {
		t.Fatal(err)
	}

	terminal, err := store.MarkTerminal(record.JobID, TerminalUpdate{
		State:   protocol.PublicStateCanceled,
		Cleanup: protocol.CleanupClean,
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != protocol.PublicStateCanceled || !terminal.EvidenceIncomplete || terminal.Cleanup != protocol.CleanupUncertain || terminal.BackendSessionID != "" || terminal.Retirement == nil || terminal.Retirement.BackendSessionID != "thread-stale" {
		t.Fatalf("marked terminal record = %#v, want uncertain cleanup and no stale session projection", terminal)
	}
}

func TestReopenRecoversTerminalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-reopen", RequestID: "request-reopen"},
		testTaskSpec("terminal task"),
		func(string) (Record, error) { return Record{Backend: "codex", Model: "gpt-test"}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.MarkTerminal(record.JobID, TerminalUpdate{
		State:       protocol.PublicStateCompleted,
		Cleanup:     protocol.CleanupUncertain,
		Diagnostics: []string{"cleanup was uncertain"},
		Contract: protocol.ContractResult{
			Evaluated:  true,
			Compliant:  true,
			Attempts:   1,
			Violations: []string{},
		},
		ResultText: "authoritative result",
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, _, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-reopen", RequestID: "request-queued"},
		testTaskSpec("queued task"),
		func(string) (Record, error) { return Record{Backend: "codex"}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, reopened)
	got, err := reopened.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.Cleanup != protocol.CleanupUncertain || got.ResultText != terminal.ResultText || !got.Contract.Compliant {
		t.Fatalf("reopened record = %+v, want persisted terminal record %+v", got, terminal)
	}
	records, err := reopened.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("reopened records = %+v, want terminal %q and queued %q", records, record.JobID, queued.JobID)
	}
}

func TestOpenTruncatedDatabaseReturnsTypedCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, 1); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open(truncated) error = %v, want ErrCorrupt", err)
	}
	var corrupt *CorruptError
	if !errors.As(err, &corrupt) {
		t.Fatalf("Open(truncated) error = %T, want *CorruptError", err)
	}
}

func TestOpenHeldDatabaseReturnsTypedBusyWithinTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	held, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestStore(t, held)

	started := time.Now()
	second, err := Open(path)
	elapsed := time.Since(started)
	if second != nil {
		_ = second.Close()
		t.Fatal("Open succeeded while the database was already exclusively held")
	}
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("Open(held) error = %v, want ErrBusy", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Open(held) took %s, want bounded timeout", elapsed)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func closeTestStore(t *testing.T, store *Store) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func testTaskSpec(value string) []byte {
	return []byte(`{"prompt":` + strconv.Quote(value) + `}`)
}
