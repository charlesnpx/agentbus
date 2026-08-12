package served

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
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
	for _, outcome := range []model.Outcome{model.OutcomeFailed, model.OutcomeCanceled, model.OutcomeQuarantined} {
		outcome := outcome
		t.Run(outcome.String(), func(t *testing.T) {
			server, _, path, managed := prepareTestManagedCodexHome(t, "retained")
			server.cleanupManagedCodexHome(managed, outcome)
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("%s Codex home stat = %v, want retained", outcome, err)
			}
		})
	}
}

func TestCleanupManagedCodexHomeRemovesOnlyManagedCompletedHome(t *testing.T) {
	server, _, managedPath, managed := prepareTestManagedCodexHome(t, "completed")
	server.cleanupManagedCodexHome(managed, model.OutcomeCompleted)
	if _, err := os.Stat(managedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed completed Codex home stat = %v, want removed", err)
	}

	fixed := filepath.Join(t.TempDir(), "fixed")
	if err := os.Mkdir(fixed, 0o700); err != nil {
		t.Fatal(err)
	}
	server.cleanupManagedCodexHome(nil, model.OutcomeCompleted)
	if _, err := os.Stat(fixed); err != nil {
		t.Fatalf("fixed Codex home stat = %v, want retained", err)
	}
}

func TestPrepareManagedCodexHomeRequiresExclusiveLeaf(t *testing.T) {
	server, layout, path, managed := prepareTestManagedCodexHome(t, "exclusive")
	managed.close()
	if _, again, err := server.prepareCodexHome(layout, "exclusive"); err == nil {
		t.Fatal("second managed Codex home creation succeeded, want exclusive-create failure")
	} else if again != nil {
		t.Fatal("pre-existing managed Codex home returned managed ownership")
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("existing Codex home stat = %v info=%v, want retained directory", err, info)
	}
}

func TestPrepareManagedCodexHomeRefusesExistingSymlink(t *testing.T) {
	server := &Server{codexAuthHome: filepath.Join(t.TempDir(), "missing-codex-auth")}
	layout := testCodexHomeLayout(t)
	if err := os.MkdirAll(layout.Codex, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(layout.Codex, "symlinked")); err != nil {
		t.Fatal(err)
	}
	if _, managed, err := server.prepareCodexHome(layout, "symlinked"); err == nil {
		t.Fatal("managed Codex home creation through existing symlink succeeded")
	} else if managed != nil {
		t.Fatal("existing symlink returned managed ownership")
	}
	if info, err := os.Stat(target); err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("symlink target stat = %v mode=%v, want untouched 0755 target", err, info.Mode())
	}
}

func TestCleanupManagedCodexHomeRetainsReplacementLeaf(t *testing.T) {
	server, _, path, managed := prepareTestManagedCodexHome(t, "replacement")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(path, "must-remain")
	if err := os.WriteFile(marker, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	server.cleanupManagedCodexHome(managed, model.OutcomeCompleted)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("replacement Codex home marker stat = %v, want retained", err)
	}
}

func prepareTestManagedCodexHome(t *testing.T, jobID string) (*Server, engine.WorkspaceLayout, string, *managedCodexHome) {
	t.Helper()
	server := &Server{codexAuthHome: filepath.Join(t.TempDir(), "missing-codex-auth")}
	layout := testCodexHomeLayout(t)
	path, managed, err := server.prepareCodexHome(layout, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if managed == nil {
		t.Fatal("managed Codex home ownership is nil")
	}
	return server, layout, path, managed
}

func testCodexHomeLayout(t *testing.T) engine.WorkspaceLayout {
	t.Helper()
	root := t.TempDir()
	namespace := filepath.Join(root, "workspaces", "namespace")
	if err := os.MkdirAll(namespace, 0o700); err != nil {
		t.Fatal(err)
	}
	return engine.WorkspaceLayout{
		Root:      root,
		Namespace: namespace,
		Codex:     filepath.Join(namespace, "codex"),
	}
}
