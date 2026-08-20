package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
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
