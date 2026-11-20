package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

func TestCappedLogWriterAppendsAcrossTurns(t *testing.T) {
	marker := TruncationMarker()
	const allowance = 16
	capBytes := int64(allowance + len(marker))
	path := filepath.Join(t.TempDir(), "capture.log")
	write := func(data string) {
		t.Helper()
		w, err := NewCappedLogWriter(path, capBytes)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := w.Write([]byte(data)); err != nil || n != len(data) {
			t.Fatalf("Write %d, %v", n, err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	read := func() string {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	write("initial-")
	write("12345678")
	if got := read(); got != "initial-12345678" {
		t.Fatalf("exact allowance: %q", got)
	}
	write("")
	if got := read(); got != "initial-12345678" {
		t.Fatalf("zero write added marker: %q", got)
	}
	write("correction")
	want := "initial-12345678" + marker
	if got := read(); got != want {
		t.Fatalf("capped = %q, want %q", got, want)
	}
	write("ignored")
	if got := read(); got != want {
		t.Fatalf("already capped changed: %q", got)
	}
}

func TestCappedLogWriterMarkerLikeTextAndInconsistentFiles(t *testing.T) {
	marker := TruncationMarker()
	path := filepath.Join(t.TempDir(), "capture.log")
	capBytes := int64(len(marker) * 3)
	initial := "ordinary" + marker
	if err := os.WriteFile(path, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewCappedLogWriter(path, capBytes)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != initial+"tail" {
		t.Fatalf("got %q, err %v", got, err)
	}
	for _, data := range []string{strings.Repeat("x", int(capBytes)), strings.Repeat("y", int(capBytes)+1)} {
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewCappedLogWriter(path, capBytes); err == nil {
			t.Fatalf("accepted inconsistent size %d", len(data))
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != data {
			t.Fatalf("inconsistent evidence changed")
		}
	}
}

type failingLogFile struct {
	writes int
	short  bool
}

func (f *failingLogFile) Write(p []byte) (int, error) {
	f.writes++
	if f.short {
		return len(p) / 2, nil
	}
	return 0, syscall.ENOSPC
}
func (*failingLogFile) Sync() error  { return nil }
func (*failingLogFile) Close() error { return nil }

func TestCappedLogWriterDisablesAfterSinkFailure(t *testing.T) {
	for _, short := range []bool{false, true} {
		f := &failingLogFile{short: short}
		w := &CappedLogWriter{file: f, cap: 100}
		if _, err := w.Write([]byte("payload")); err == nil {
			t.Fatal("missing sink failure")
		}
		if _, err := w.Write([]byte("again")); err == nil || f.writes != 1 {
			t.Fatalf("sink was retried: writes %d, err %v", f.writes, err)
		}
	}
}
