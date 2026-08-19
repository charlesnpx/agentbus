//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

func runtimeHygieneRecord(t *testing.T, server *Server, cwd, requestID string, write bool) jobstore.Record {
	t.Helper()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	spec := protocol.TaskSpecV3{
		Backend: "codex",
		CWD:     cwd,
		Prompt:  "runtime hygiene test",
		Write:   write,
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: "runtime-hygiene-" + requestID, RequestID: requestID},
		raw,
		func(id string) (jobstore.Record, error) {
			return jobstore.Record{JobID: id, Backend: spec.Backend, CWD: canonicalCWD, Write: spec.Write}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("new runtime-hygiene job was unexpectedly deduplicated")
	}
	return record
}

func TestCodexWriteTurnReceivesPerJobSandboxCache(t *testing.T) {
	var started engine.SessionOpts
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		started = opts
		if opts.WriteSandboxRoot == "" {
			t.Fatal("write Codex session missing WriteSandboxRoot")
		}
		for _, path := range []string{
			filepath.Join(opts.WriteSandboxRoot, "cache", "go-build"),
			filepath.Join(opts.WriteSandboxRoot, "cache", "go-mod"),
			filepath.Join(opts.WriteSandboxRoot, "tmp"),
		} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("write cache path %s: %v", path, err)
			}
			if !info.IsDir() || info.Mode().Perm() != 0o700 {
				t.Fatalf("write cache path %s mode/type = %s, want directory 0700", path, info.Mode())
			}
		}
		return claimedResultSession("write result"), nil
	}
	server := newTestServer(t, t.TempDir(), Config{
		Backends:      []engine.Backend{backend},
		CodexAuthHome: filepath.Join(t.TempDir(), "missing-auth-home"),
	})
	record := runtimeHygieneRecord(t, server, t.TempDir(), "write", true)
	runExecution(t, server, record)

	if started.WriteSandboxRoot == "" {
		t.Fatal("write Codex session did not start")
	}
	want := map[string]string{
		"GOCACHE":    filepath.Join(started.WriteSandboxRoot, "cache", "go-build"),
		"GOMODCACHE": filepath.Join(started.WriteSandboxRoot, "cache", "go-mod"),
		"TMPDIR":     filepath.Join(started.WriteSandboxRoot, "tmp"),
	}
	if len(started.WriteEnvOverlay) != len(want) {
		t.Fatalf("WriteEnvOverlay = %#v, want exactly %#v", started.WriteEnvOverlay, want)
	}
	for key, path := range want {
		if got := started.WriteEnvOverlay[key]; got != path {
			t.Fatalf("WriteEnvOverlay[%q] = %q, want %q", key, got, path)
		}
		relative, err := filepath.Rel(started.WriteSandboxRoot, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatalf("WriteEnvOverlay[%q] = %q is outside sandbox root %q", key, path, started.WriteSandboxRoot)
		}
	}
}

func TestCodexReadOnlyTurnReceivesNoWriteSandboxCache(t *testing.T) {
	var started engine.SessionOpts
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		started = opts
		return claimedResultSession("read result"), nil
	}
	server := newTestServer(t, t.TempDir(), Config{
		Backends:      []engine.Backend{backend},
		CodexAuthHome: filepath.Join(t.TempDir(), "missing-auth-home"),
	})
	record := runtimeHygieneRecord(t, server, t.TempDir(), "read", false)
	runExecution(t, server, record)

	if started.WriteSandboxRoot != "" {
		t.Fatalf("read-only WriteSandboxRoot = %q, want empty", started.WriteSandboxRoot)
	}
	if started.WriteEnvOverlay != nil {
		t.Fatalf("read-only WriteEnvOverlay = %#v, want nil", started.WriteEnvOverlay)
	}
}

func TestCodexJobGetsPrivateHomeWithLinkedAuthFiles(t *testing.T) {
	authHome := t.TempDir()
	for _, name := range codexHomeLinkedFiles {
		if err := os.WriteFile(filepath.Join(authHome, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		home := opts.EnvOverlay["CODEX_HOME"]
		if home == "" || !filepath.IsAbs(home) {
			t.Fatalf("CODEX_HOME = %q, want private absolute home", home)
		}
		info, err := os.Stat(home)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("private Codex home mode/type = %s, want directory 0700", info.Mode())
		}
		for _, name := range codexHomeLinkedFiles {
			path := filepath.Join(home, name)
			info, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("%s mode = %s, want symlink", path, info.Mode())
			}
			target, err := os.Readlink(path)
			if err != nil {
				t.Fatal(err)
			}
			if want := filepath.Join(authHome, name); target != want {
				t.Fatalf("%s target = %q, want %q", path, target, want)
			}
		}
		return claimedResultSession("private home result"), nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}, CodexAuthHome: authHome})
	record := runtimeHygieneRecord(t, server, t.TempDir(), "private-home", false)
	runExecution(t, server, record)
}

func TestManagedCodexHomeIsRemovedAfterCleanCompletion(t *testing.T) {
	var home string
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		home = opts.EnvOverlay["CODEX_HOME"]
		marker := filepath.Join(opts.WriteSandboxRoot, "cache", "go-mod", "cached-module")
		if err := os.WriteFile(marker, []byte("cached"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(filepath.Dir(marker), 0o555); err != nil {
			t.Fatal(err)
		}
		return claimedResultSession("clean completion"), nil
	}
	server := newTestServer(t, t.TempDir(), Config{
		Backends:      []engine.Backend{backend},
		CodexAuthHome: filepath.Join(t.TempDir(), "missing-auth-home"),
	})
	record := runtimeHygieneRecord(t, server, t.TempDir(), "clean-completion", true)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.Cleanup != protocol.CleanupClean {
		t.Fatalf("terminal record = %+v, want completed with clean cleanup", got)
	}
	if _, err := os.Stat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean terminal managed home stat = %v, want removed", err)
	}
}

func TestManagedCodexHomeRetainedWhenCleanupUncertainAndResultSurvives(t *testing.T) {
	var home string
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		home = opts.EnvOverlay["CODEX_HOME"]
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 5191, PGID: 5191, StartTime: "uncertain-cleanup-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventResultMessage, Text: "authoritative result survives"},
					engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{CleanupFailed: true}},
				), nil
			},
		}, nil
	}
	server := newTestServer(t, t.TempDir(), Config{
		Backends:      []engine.Backend{backend},
		CodexAuthHome: filepath.Join(t.TempDir(), "missing-auth-home"),
	})
	record := runtimeHygieneRecord(t, server, t.TempDir(), "uncertain-cleanup", false)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.Cleanup != protocol.CleanupUncertain || got.ResultText != "authoritative result survives" {
		t.Fatalf("terminal record = %+v, want completed result with uncertain cleanup", got)
	}
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("uncertain-cleanup managed home stat = %v, want retained", err)
	}
}

func TestStartupCodexHomeSweepRemovesTerminalAndRetainsOtherLeaves(t *testing.T) {
	backend := &executionFakeBackend{name: "codex"}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	workspace := t.TempDir()
	terminal := runtimeHygieneRecord(t, server, workspace, "sweep-terminal", false)
	live := runtimeHygieneRecord(t, server, workspace, "sweep-live", false)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(terminal.JobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCompleted,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: terminal.CreatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	layout, err := engine.LayoutForWorkspace(server.stateRoot, workspace)
	if err != nil {
		t.Fatal(err)
	}
	terminalHome, err := createManagedCodexHome(layout, terminal.JobID)
	if err != nil {
		t.Fatal(err)
	}
	terminalPath := terminalHome.path
	terminalHome.close()
	liveHome, err := createManagedCodexHome(layout, live.JobID)
	if err != nil {
		t.Fatal(err)
	}
	livePath := liveHome.path
	liveHome.close()
	unknownHome, err := createManagedCodexHome(layout, "job_unrecognized")
	if err != nil {
		t.Fatal(err)
	}
	unknownPath := unknownHome.path
	unknownHome.close()

	if err := server.sweepManagedCodexHomes(store); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(terminalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal managed home stat = %v, want removed", err)
	}
	if _, err := os.Stat(livePath); err != nil {
		t.Fatalf("live managed home stat = %v, want retained", err)
	}
	if _, err := os.Stat(unknownPath); err != nil {
		t.Fatalf("unrecognized managed home stat = %v, want retained", err)
	}
}

func TestManagedCodexHomeIdentityMismatchIsRetained(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	layout, err := engine.LayoutForWorkspace(server.stateRoot, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home, err := createManagedCodexHome(layout, "job_identity")
	if err != nil {
		t.Fatal(err)
	}
	originalName := home.name + "-original"
	if err := unix.Renameat(home.parentFD, home.name, home.parentFD, originalName); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkdirat(home.parentFD, home.name, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(home.path, "replacement-marker")
	if err := os.WriteFile(marker, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	server.cleanupManagedCodexHome(home, protocol.PublicStateCompleted)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("replacement managed home marker stat = %v, want retained", err)
	}
}

func TestContentPolicyFailurePrecedesModelUnavailable(t *testing.T) {
	err := errors.New("unknown model: requested content was flagged for possible policy violation")
	if got := classifyTerminalFailure(terminalFailureBackendRan, err, false); got != protocol.FailureClassContentPolicy {
		t.Fatalf("failure class = %q, want %q", got, protocol.FailureClassContentPolicy)
	}
}

func TestAccountFlaggedForAbuseStaysBackendError(t *testing.T) {
	err := errors.New("This account was flagged for possible abuse")
	if got := classifyTerminalFailure(terminalFailureBackendRan, err, false); got != protocol.FailureClassBackendError {
		t.Fatalf("failure class = %q, want %q", got, protocol.FailureClassBackendError)
	}
}
