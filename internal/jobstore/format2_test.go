package jobstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

func TestFormat2OwnershipAndRefusals(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "jobs.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatalf("reopen same root: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copyRoot := t.TempDir()
	copyPath := filepath.Join(copyRoot, "jobs.db")
	if err := os.WriteFile(copyPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(copyPath); !errors.Is(err, ErrIncompatible) || !strings.Contains(err.Error(), "ownership") {
		t.Fatalf("copied database open: %v", err)
	}
	after, err := os.ReadFile(copyPath)
	if err != nil || !bytes.Equal(data, after) {
		t.Fatalf("refusal modified copied database: %v", err)
	}

	for _, corrupt := range []struct{ name, key, value string }{
		{"missing owner", "ownerRoot", ""},
		{"unsupported format", "format", "unsupported"},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			caseRoot := t.TempDir()
			casePath := filepath.Join(caseRoot, "jobs.db")
			s, err := Open(casePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(casePath, 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				if corrupt.value == "" {
					return tx.Bucket(bucketMeta).Delete([]byte(corrupt.key))
				}
				return tx.Bucket(bucketMeta).Put([]byte(corrupt.key), []byte(corrupt.value))
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(casePath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Open(casePath); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("open: %v", err)
			}
			after, err := os.ReadFile(casePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refusal modified database: %v", err)
			}
		})
	}
}

func TestMissingDatabaseRefusesOwnedData(t *testing.T) {
	for _, dir := range []string{"artifacts/logs", "artifacts/results", "sessions/codex"} {
		t.Run(dir, func(t *testing.T) {
			root := t.TempDir()
			owned := filepath.Join(root, dir)
			if err := os.MkdirAll(owned, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(owned, "owned")
			if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(filepath.Join(root, "jobs.db")); !errors.Is(err, ErrIncompatible) {
				t.Fatalf("open: %v", err)
			}
			if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
				t.Fatalf("owned data changed: %q %v", data, err)
			}
			if _, err := os.Stat(filepath.Join(root, "jobs.db")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("database was created: %v", err)
			}
		})
	}
}

func TestStartingSelectionIsAtomicAndAgreesWithTask(t *testing.T) {
	store := newTestStore(t)
	defer closeTestStore(t, store)
	task := []byte(`{"effort":"high","model":"chosen","prompt":"task"}`)
	key := RequestKey{WorkspaceKey: "workspace", RequestID: "request"}
	record, _, err := store.SubmitTx(key, task, func(string) (Record, error) { return Record{Backend: "codex"}, nil })
	if err != nil {
		t.Fatal(err)
	}
	if record.Selection != nil || record.Starting {
		t.Fatalf("queued selection: %+v", record)
	}
	if _, err := store.MarkStartingWithSelection(record.JobID, protocol.InvocationSelection{Model: "other", Effort: "high"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched selection: %v", err)
	}
	queued, err := store.Get(record.JobID)
	if err != nil || queued.Selection != nil || queued.Starting {
		t.Fatalf("failed transition changed record: %+v, %v", queued, err)
	}
	want := protocol.InvocationSelection{Model: "chosen", Effort: "high"}
	started, err := store.MarkStartingWithSelection(record.JobID, want)
	if err != nil || !started.Starting || started.Selection == nil || *started.Selection != want {
		t.Fatalf("start: %+v, %v", started, err)
	}
	replayed, dedup, err := store.SubmitTx(key, task, func(string) (Record, error) { t.Fatal("replay ran factory"); return Record{}, nil })
	if err != nil || !dedup || replayed.Selection == nil || *replayed.Selection != want {
		t.Fatalf("replay: %+v %t %v", replayed, dedup, err)
	}
	if _, err := store.MarkStartingWithSelection(record.JobID, want); !errors.Is(err, ErrStarting) {
		t.Fatalf("second transition: %v", err)
	}
}
