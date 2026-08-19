package jobstore

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestSubmitReplaySkipsDeletedCWD(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-replay", RequestID: "request-replay"}
	hash := testHash("same exact task spec")
	cwd := filepath.Join(t.TempDir(), "deleted-workspace")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	original, deduplicated, err := store.SubmitTx(key, hash, func(string) Record {
		return Record{Backend: "codex", CWD: cwd}
	})
	if err != nil || deduplicated {
		t.Fatalf("first SubmitTx = (%+v, deduplicated=%t, %v), want new record", original, deduplicated, err)
	}
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}

	factoryCalled := false
	replayed, deduplicated, err := store.SubmitTx(key, hash, func(string) Record {
		factoryCalled = true
		if _, err := os.Stat(cwd); err != nil {
			t.Fatalf("replay tried to stat deleted cwd: %v", err)
		}
		return Record{Backend: "must-not-be-created"}
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

func TestSubmitDifferentHashConflictsWithoutMutation(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-conflict", RequestID: "request-conflict"}
	original, _, err := store.SubmitTx(key, testHash("one"), func(string) Record {
		return Record{Backend: "codex"}
	})
	if err != nil {
		t.Fatal(err)
	}
	factoryCalled := false
	_, _, err = store.SubmitTx(key, testHash("two"), func(string) Record {
		factoryCalled = true
		return Record{Backend: "must-not-be-created"}
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
	replayed, deduplicated, err := store.SubmitTx(key, testHash("one"), func(string) Record {
		t.Fatal("matching replay invoked new-record factory")
		return Record{}
	})
	if err != nil || !deduplicated || replayed.JobID != original.JobID {
		t.Fatalf("matching replay after conflict = (%+v, %t, %v), want original replay", replayed, deduplicated, err)
	}
}

func TestSubmitTransactionRollsBackJobAndBinding(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-atomic", RequestID: "request-atomic"}
	injected := errors.New("injected after job put")
	store.setSubmitFailureForTest(injected)
	_, _, err := store.SubmitTx(key, testHash("atomic task"), func(string) Record {
		return Record{Backend: "codex"}
	})
	if !errors.Is(err, injected) {
		t.Fatalf("injected SubmitTx error = %v, want %v", err, injected)
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after injected transaction failure = %+v, want none", records)
	}
	store.setSubmitFailureForTest(nil)
	record, deduplicated, err := store.SubmitTx(key, testHash("atomic task"), func(string) Record {
		return Record{Backend: "codex"}
	})
	if err != nil || deduplicated || record.JobID == "" {
		t.Fatalf("SubmitTx after rollback = (%+v, %t, %v), want new record", record, deduplicated, err)
	}
}

func TestGeneratedJobIDIsOpaque(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	key := RequestKey{WorkspaceKey: "workspace-opaque-sentinel", RequestID: "request-opaque-sentinel"}
	record, _, err := store.SubmitTx(key, testHash("opaque task"), func(string) Record {
		return Record{Backend: "codex", CWD: "/workspace/opaque-sentinel"}
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

func TestReopenRecoversTerminalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-reopen", RequestID: "request-reopen"},
		testHash("terminal task"),
		func(string) Record { return Record{Backend: "codex", Model: "gpt-test"} },
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
		testHash("queued task"),
		func(string) Record { return Record{Backend: "codex"} },
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

func TestSweepArtifactsDeletesExpiredFilesButRetainsRecord(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)

	record, _, err := store.SubmitTx(
		RequestKey{WorkspaceKey: "workspace-artifacts", RequestID: "request-artifacts"},
		testHash("artifact task"),
		func(string) Record { return Record{Backend: "codex"} },
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{record.Artifacts.Prompt, record.Artifacts.Log, record.Artifacts.Result} {
		if err := atomicWrite(path, []byte("expired artifact"), 0o600); err != nil {
			t.Fatalf("write artifact %s: %v", path, err)
		}
	}
	now := time.Now().UTC()
	expired := now.Add(-31 * 24 * time.Hour)
	for _, path := range []string{record.Artifacts.Prompt, record.Artifacts.Log, record.Artifacts.Result} {
		if err := os.Chtimes(path, expired, expired); err != nil {
			t.Fatalf("age artifact %s: %v", path, err)
		}
	}
	if err := store.SweepArtifacts(now); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{record.Artifacts.Prompt, record.Artifacts.Log, record.Artifacts.Result} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact %s remains after sweep: %v", path, err)
		}
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatalf("job record disappeared during artifact sweep: %v", err)
	}
	if got.JobID != record.JobID {
		t.Fatalf("record after artifact sweep = %q, want %q", got.JobID, record.JobID)
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

func testHash(value string) [32]byte {
	return sha256.Sum256([]byte(value))
}
