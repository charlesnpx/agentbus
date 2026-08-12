package served

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
)

func TestResolveCodexAuthHomeUsesAmbientThenHomeFallback(t *testing.T) {
	ambient := filepath.Join(t.TempDir(), "ambient-codex")
	t.Setenv("CODEX_HOME", ambient)
	got, err := resolveCodexAuthHome("")
	if err != nil || got != ambient {
		t.Fatalf("ambient Codex home = %q err=%v, want %q", got, err, ambient)
	}

	t.Setenv("CODEX_HOME", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	got, err = resolveCodexAuthHome("")
	want := filepath.Join(home, ".codex")
	if err != nil || got != want {
		t.Fatalf("fallback Codex home = %q err=%v, want %q", got, err, want)
	}
}

func TestCleanupManagedCodexHomeRetainsForensicOutcomes(t *testing.T) {
	server := &Server{}
	for _, state := range []engine.JobState{engine.StateFailed, engine.StateCanceled, engine.StateQuarantined} {
		state := state
		t.Run(string(state), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "codex")
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			server.cleanupManagedCodexHome(path, true, state)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("%s Codex home stat = %v, want retained", state, err)
			}
		})
	}
}

func TestCleanupManagedCodexHomeRemovesOnlyManagedCompletedHome(t *testing.T) {
	server := &Server{}
	managed := filepath.Join(t.TempDir(), "managed")
	if err := os.Mkdir(managed, 0o700); err != nil {
		t.Fatal(err)
	}
	server.cleanupManagedCodexHome(managed, true, engine.StateCompleted)
	if _, err := os.Stat(managed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed completed Codex home stat = %v, want removed", err)
	}

	fixed := filepath.Join(t.TempDir(), "fixed")
	if err := os.Mkdir(fixed, 0o700); err != nil {
		t.Fatal(err)
	}
	server.cleanupManagedCodexHome(fixed, false, engine.StateCompleted)
	if _, err := os.Stat(fixed); err != nil {
		t.Fatalf("fixed Codex home stat = %v, want retained", err)
	}
}
