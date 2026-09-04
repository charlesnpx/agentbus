package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteResultForLayoutAndEventTruncation(t *testing.T) {
	t.Parallel()
	layout := WorkspaceLayout{Results: filepath.Join(t.TempDir(), "results")}
	raw := []byte("abcdef")
	info, err := WriteResultForLayout(layout, "job_inline", raw, len(raw)+1)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if info.SHA256 != hex.EncodeToString(sum[:]) || info.Bytes != int64(len(raw)) || info.Text != string(raw) {
		t.Fatalf("bad inline result: %+v", info)
	}
	spilled, err := WriteResultForLayout(layout, "job_spill", raw, len(raw))
	if err != nil {
		t.Fatal(err)
	}
	if spilled.Text != "" || !spilled.TextElided {
		t.Fatalf("spilled-only result = %+v", spilled)
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

func TestResultPathsStayInNamespace(t *testing.T) {
	t.Parallel()
	layout := WorkspaceLayout{
		Results: filepath.Join(t.TempDir(), "results"),
		Logs:    filepath.Join(t.TempDir(), "logs"),
	}
	for _, id := range []string{"../../escape", "job_../escape", "job_bad/name", "job_bad\\name", "turn_123", "job_"} {
		id := id
		t.Run(id, func(t *testing.T) {
			if _, err := ResultPathForLayout(layout, id); err == nil {
				t.Fatalf("ResultPathForLayout(%q) succeeded", id)
			}
			if _, err := LogPathsForLayout(layout, id); err == nil {
				t.Fatalf("LogPathsForLayout(%q) succeeded", id)
			}
		})
	}
}

func TestCappedLogWriter(t *testing.T) {
	t.Parallel()
	const defaultCap = 64 * 1024 * 1024
	path := filepath.Join(t.TempDir(), "log.txt")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := NewCappedLogWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	chunk := bytes.Repeat([]byte("x"), 1024*1024)
	for range 65 {
		if n, err := w.Write(chunk); err != nil || n != len(chunk) {
			t.Fatalf("Write n=%d err=%v", n, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Size(); got != defaultCap {
		t.Fatalf("default-capped log size = %d, want %d", got, defaultCap)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("log mode = %o, want 600", got)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte(TruncationMarker())
	if !bytes.HasSuffix(contents, marker) || bytes.Count(contents, marker) != 1 {
		t.Fatal("default-capped log does not end with exactly one truncation marker")
	}
}
