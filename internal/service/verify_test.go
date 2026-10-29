//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestJobSubmitVerifyValidationAndOmissionReplayHash(t *testing.T) {
	originalGOOS := localIPCGOOS
	localIPCGOOS = "darwin"
	t.Cleanup(func() { localIPCGOOS = originalGOOS })
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{
		helloBackend{name: "codex"}, helloBackend{name: "claude"}, helloBackend{name: "cursor"},
	}})

	for _, test := range []struct {
		name    string
		backend string
		write   bool
		resume  string
		retain  bool
		want    string
	}{
		{name: "write enabled", backend: "codex", write: true, want: "verify requires write: false"},
		{name: "claude", backend: "claude", want: "Codex-only"},
		{name: "cursor", backend: "cursor", want: "Codex-only"},
		{name: "resume", backend: "codex", resume: "job_previous", want: "cannot be combined with resumeJobId"},
		{name: "retain session", backend: "codex", retain: true, want: "cannot retain a session"},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := submissionParams("verify-invalid-"+strings.ReplaceAll(test.name, " ", "-"), "request", test.backend, t.TempDir(), "verify")
			params.TaskSpec.Verify = true
			params.TaskSpec.Write = test.write
			params.TaskSpec.ResumeJobID = test.resume
			params.TaskSpec.RetainSession = test.retain
			outcome := submitForTest(t, server, params)
			if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec || !strings.Contains(outcome.err.Message, test.want) {
				t.Fatalf("verify rejection = %#v, want invalid_task_spec containing %q", outcome.err, test.want)
			}
		})
	}

	nonGit := submissionParams("verify-non-git", "request", "codex", t.TempDir(), "verify")
	nonGit.TaskSpec.Verify = true
	if outcome := submitForTest(t, server, nonGit); outcome.err == nil || !strings.Contains(outcome.err.Message, "verify requires cwd inside a Git work tree") {
		t.Fatalf("non-Git verify submission = %#v, want Git work tree validation", outcome.err)
	}

	repo := newFrozenTestRepo(t)
	accepted := submissionParams("verify-local-ipc", "request", "codex", repo, "verify")
	accepted.TaskSpec.Verify = true
	accepted.TaskSpec.LocalIPC = true
	if outcome := submitForTest(t, server, accepted); outcome.err != nil {
		t.Fatalf("verify with localIPC on macOS = %#v, want accepted", outcome.err)
	}

	localIPCGOOS = "linux"
	linux := submissionParams("verify-local-ipc-linux", "request", "codex", repo, "verify")
	linux.TaskSpec.Verify = true
	linux.TaskSpec.LocalIPC = true
	if outcome := submitForTest(t, server, linux); outcome.err == nil || !strings.Contains(outcome.err.Message, "Unix sockets") {
		t.Fatalf("verify with localIPC on Linux = %#v, want C2 sandbox refusal", outcome.err)
	}

	oldRaw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"verify-hash","requestId":"same","taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"same"}}`, repo))
	oldInput, err := parseJobSubmitInput(oldRaw)
	if err != nil {
		t.Fatal(err)
	}
	withoutVerify, err := json.Marshal(protocol.TaskSpec{Backend: "codex", CWD: repo, Write: false, Prompt: "same", Verify: false})
	if err != nil {
		t.Fatal(err)
	}
	newInput, err := parseJobSubmitInput(json.RawMessage(`{"workspaceKey":"verify-hash","requestId":"same","taskSpec":` + string(withoutVerify) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(oldInput.canonicalTaskSpec) != sha256.Sum256(newInput.canonicalTaskSpec) {
		t.Fatalf("omitted verify changed TaskSpec replay hash: old=%s new=%s", oldInput.canonicalTaskSpec, newInput.canonicalTaskSpec)
	}
	if strings.Contains(string(withoutVerify), `"verify"`) {
		t.Fatalf("Verify false serialized as an explicit field: %s", withoutVerify)
	}

	null := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"verify-null","requestId":"request","taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"verify","verify":null}}`, repo))
	if outcome := server.handleJobSubmit(null); outcome.err == nil || !strings.Contains(outcome.err.Message, "verify cannot be null") {
		t.Fatalf("null verify submission = %#v, want explicit null rejection", outcome.err)
	}
	durable, err := taskSpecFromRecord(jobstore.Record{
		Backend: "codex", CWD: repo, Write: true,
		TaskSpec: json.RawMessage(fmt.Sprintf(`{"backend":"codex","cwd":%q,"write":true,"prompt":"verify","verify":true}`, repo)),
	})
	if err == nil || durable.Backend != "codex" || !strings.Contains(err.Error(), "durable task spec has invalid verify") {
		t.Fatalf("invalid durable verify spec = (%+v, %v), want durable validation failure", durable, err)
	}
}

func TestCreateFrozenCopyPreservesEligibleFilesAndMetadata(t *testing.T) {
	repo := newFrozenTestRepo(t)
	writeFrozenTestFile(t, filepath.Join(repo, "tracked.sh"), "#!/bin/sh\nexit 0\n", 0o751)
	writeFrozenTestFile(t, filepath.Join(repo, ".gitignore"), "ignored.txt\n", 0o644)
	writeFrozenTestFile(t, filepath.Join(repo, "deleted.txt"), "gone after commit", 0o644)
	if err := os.Mkdir(filepath.Join(repo, "subdir"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFrozenTestFile(t, filepath.Join(repo, "subdir", "nested.txt"), "nested contents", 0o644)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeFrozenTestFile(t, outside, "outside target", 0o600)
	if err := os.Symlink(outside, filepath.Join(repo, "external-link")); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, repo, "add", "tracked.sh", ".gitignore", "deleted.txt", "subdir/nested.txt", "external-link")
	runGitTest(t, repo, "commit", "-m", "add copy fixtures")
	if err := os.Remove(filepath.Join(repo, "deleted.txt")); err != nil {
		t.Fatal(err)
	}
	writeFrozenTestFile(t, filepath.Join(repo, "untracked.txt"), "untracked contents", 0o640)
	writeFrozenTestFile(t, filepath.Join(repo, "ignored.txt"), "ignored contents", 0o600)

	head := strings.TrimSpace(runGitTest(t, repo, "rev-parse", "HEAD"))
	destination := filepath.Join(t.TempDir(), "src")
	identity, err := createFrozenCopy(repo, destination)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Head != head || !identity.Dirty {
		t.Fatalf("frozen source identity = %+v, want HEAD %q and dirty=true", identity, head)
	}
	if info, err := os.Stat(filepath.Join(destination, "tracked.sh")); err != nil || info.Mode().Perm() != 0o751 {
		t.Fatalf("copied tracked mode info=%v err=%v, want mode 0751", info, err)
	}
	for name, want := range map[string]string{
		"tracked.sh":    "#!/bin/sh\nexit 0\n",
		"untracked.txt": "untracked contents",
	} {
		got, err := os.ReadFile(filepath.Join(destination, name))
		if err != nil || string(got) != want {
			t.Fatalf("copied %s = %q err=%v, want %q", name, got, err, want)
		}
	}
	for _, name := range []string{"ignored.txt", "deleted.txt", ".git"} {
		if _, err := os.Lstat(filepath.Join(destination, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("copy path %q stat error = %v, want absent", name, err)
		}
	}
	linkInfo, err := os.Lstat(filepath.Join(destination, "external-link"))
	if err != nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("copied external link info=%v err=%v, want symlink", linkInfo, err)
	}
	linkTarget, err := os.Readlink(filepath.Join(destination, "external-link"))
	if err != nil || linkTarget != outside {
		t.Fatalf("copied symlink target=%q err=%v, want %q", linkTarget, err, outside)
	}
	nestedDestination := filepath.Join(t.TempDir(), "nested-src")
	if _, err := createFrozenCopy(filepath.Join(repo, "subdir"), nestedDestination); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(nestedDestination, "nested.txt")); err != nil || string(got) != "nested contents" {
		t.Fatalf("nested cwd copy = %q err=%v, want file at copy root", got, err)
	}
	if _, err := os.Lstat(filepath.Join(nestedDestination, "subdir")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("nested cwd copy has repository-relative prefix: %v", err)
	}
}

func TestFrozenSourceIdentityCountsUntrackedFilesWhenGitHidesThem(t *testing.T) {
	repo := newFrozenTestRepo(t)
	runGitTest(t, repo, "config", "status.showUntrackedFiles", "no")
	writeFrozenTestFile(t, filepath.Join(repo, "untracked.txt"), "untracked contents", 0o644)

	identity, err := frozenSourceIdentity(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if !identity.Dirty {
		t.Fatalf("frozen source identity = %+v, want dirty=true for untracked input", identity)
	}
}

func TestVerifyCopyBoundFailsBeforeBackendStart(t *testing.T) {
	originalLimit := frozenCopyMaxBytes
	frozenCopyMaxBytes = 8
	t.Cleanup(func() { frozenCopyMaxBytes = originalLimit })
	repo := newFrozenTestRepo(t)
	writeFrozenTestFile(t, filepath.Join(repo, "payload.txt"), "more than eight bytes", 0o644)
	var starts atomic.Int64
	backend := newCodexSandboxTestBackend(t, func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return claimedResultSession("should not start"), nil
	})
	server := newExecutionServer(t, backend)
	server.codexSandboxProbe = func(context.Context, string) error { return nil }
	record := queuedVerifyExecutionRecord(t, server, repo, false)
	runExecution(t, server, record)
	got := mustGetStoredJob(t, server, record.JobID)
	if got.State != protocol.PublicStateFailed || !strings.Contains(got.FailureReason, "byte bound") {
		t.Fatalf("verify bound result = state:%s reason:%q, want byte-bound failure", got.State, got.FailureReason)
	}
	if starts.Load() != 0 || got.ProcessClaim != nil {
		t.Fatalf("backend starts=%d processClaim=%#v, want failure before spawn", starts.Load(), got.ProcessClaim)
	}
}

func TestVerifyExecutionUsesAndRemovesFrozenScratch(t *testing.T) {
	originalGOOS := localIPCGOOS
	localIPCGOOS = "darwin"
	t.Cleanup(func() { localIPCGOOS = originalGOOS })
	originalTempRoot := localIPCTempRoot
	localIPCTempRoot = "/tmp"
	t.Cleanup(func() { localIPCTempRoot = originalTempRoot })
	repo := newFrozenTestRepo(t)
	writeFrozenTestFile(t, filepath.Join(repo, "source.txt"), "submitted bytes", 0o644)
	runGitTest(t, repo, "add", "source.txt")
	runGitTest(t, repo, "commit", "-m", "source")
	head := strings.TrimSpace(runGitTest(t, repo, "rev-parse", "HEAD"))
	indexBefore, err := os.ReadFile(filepath.Join(repo, ".git", "index"))
	if err != nil {
		t.Fatal(err)
	}
	var started engine.SessionOpts
	backend := newCodexSandboxTestBackend(t, func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		started = opts
		if !opts.Verify || opts.Write || opts.CWD == repo || opts.WriteSandboxRoot == "" {
			return nil, fmt.Errorf("unexpected verify launch options: %+v", opts)
		}
		if err := os.WriteFile(filepath.Join(opts.CWD, "verification-output.txt"), []byte("discarded"), 0o600); err != nil {
			return nil, err
		}
		return claimedResultSession("verification complete"), nil
	})
	server := newExecutionServer(t, backend)
	server.codexSandboxProbe = func(context.Context, string) error { return nil }
	record := queuedVerifyExecutionRecord(t, server, repo, true)
	runExecution(t, server, record)
	got := mustGetStoredJob(t, server, record.JobID)
	if got.State != protocol.PublicStateCompleted || got.Execution == nil {
		t.Fatalf("verify terminal = %+v, want completed with posture", got)
	}
	posture := got.Execution
	if posture.Intent != "verify" || posture.SourceWritable || posture.Enforcement != "os-sandbox" || posture.GitMetadata != "none" || posture.LocalIPC != "private-dir" {
		t.Fatalf("verify posture = %+v, want read-only source, OS sandbox, and no Git metadata", posture)
	}
	if posture.FrozenCopy != started.CWD || posture.FrozenSource == nil || posture.FrozenSource.Head != head || posture.FrozenSource.Dirty {
		t.Fatalf("frozen source posture = %+v, want copy %q at clean HEAD %q", posture, started.CWD, head)
	}
	if started.WriteSandboxRoot != posture.Scratch {
		t.Fatalf("scratch root=%q posture scratch=%q", started.WriteSandboxRoot, posture.Scratch)
	}
	wantWriteEnv := map[string]string{
		"GOCACHE":              filepath.Join(posture.Scratch, "cache", "go-build"),
		"TMPDIR":               started.LocalIPCRoot,
		"npm_config_cache":     filepath.Join(posture.Scratch, "cache", "npm"),
		"COREPACK_HOME":        filepath.Join(posture.Scratch, "cache", "corepack"),
		"XDG_CACHE_HOME":       filepath.Join(posture.Scratch, "cache", "xdg"),
		"npm_config_store_dir": filepath.Join(posture.Scratch, "cache", "pnpm"),
		"YARN_CACHE_FOLDER":    filepath.Join(posture.Scratch, "cache", "yarn"),
	}
	if len(started.WriteEnvOverlay) != len(wantWriteEnv) {
		t.Fatalf("verify scratch overlay = %#v, want %#v", started.WriteEnvOverlay, wantWriteEnv)
	}
	for key, want := range wantWriteEnv {
		if got := started.WriteEnvOverlay[key]; got != want {
			t.Fatalf("verify WriteEnvOverlay[%q] = %q, want %q", key, got, want)
		}
	}
	if started.EnvOverlay["GOPROXY"] != "off" || (server.moduleCache != "" && started.EnvOverlay["GOMODCACHE"] != server.moduleCache) {
		t.Fatalf("verify Go module environment = %#v, want offline GOPROXY and populated host GOMODCACHE %q", started.EnvOverlay, server.moduleCache)
	}
	if _, err := os.Stat(filepath.Join(repo, "verification-output.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verification output in submitted source: stat error %v, want absent", err)
	}
	indexAfter, err := os.ReadFile(filepath.Join(repo, ".git", "index"))
	if err != nil || sha256.Sum256(indexAfter) != sha256.Sum256(indexBefore) {
		t.Fatalf("submitted Git index changed during verification: read error %v", err)
	}
	if _, err := os.Stat(posture.Scratch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed verification scratch stat error = %v, want removed after clean finish", err)
	}
	if _, err := os.Stat(started.LocalIPCRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private local IPC directory stat error = %v, want removed after clean finish", err)
	}
	if _, err := os.Stat(filepath.Join(started.CWD, "verification-output.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("frozen copy stat after scratch cleanup = %v, want removed", err)
	}
}

func TestFailedVerifyRemovesFrozenCopyAndRetainsSessionHome(t *testing.T) {
	repo := newFrozenTestRepo(t)
	var started engine.SessionOpts
	backend := newCodexSandboxTestBackend(t, func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		started = opts
		return nil, errors.New("provider start failed")
	})
	server := newExecutionServer(t, backend)
	server.codexSandboxProbe = func(context.Context, string) error { return nil }
	record := queuedVerifyExecutionRecord(t, server, repo, false)

	runExecution(t, server, record)
	got := mustGetStoredJob(t, server, record.JobID)
	if got.State != protocol.PublicStateFailed {
		t.Fatalf("verify state = %q, want failed", got.State)
	}
	if started.CWD == "" {
		t.Fatal("verify backend did not receive a frozen copy path")
	}
	if _, err := os.Lstat(started.CWD); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed verify frozen copy stat error = %v, want removed", err)
	}
	home := started.EnvOverlay["CODEX_HOME"]
	if home == "" {
		t.Fatal("verify backend did not receive its managed session home")
	}
	if info, err := os.Stat(home); err != nil || !info.IsDir() {
		t.Fatalf("failed verify session home info=%v err=%v, want retained directory", info, err)
	}
}

func TestVerifyFrozenCopyCleanupFailureIsRecordedAsUncertain(t *testing.T) {
	repo := newFrozenTestRepo(t)
	server := newExecutionServer(t, newCodexSandboxTestBackend(t, nil))
	record := queuedVerifyExecutionRecord(t, server, repo, false)
	layout, err := engine.LayoutForWorkspace(server.stateRoot, record.CWD)
	if err != nil {
		t.Fatal(err)
	}
	home, err := createManagedCodexHome(layout, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	defer home.close()
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(home.path, "src")); err != nil {
		t.Fatal(err)
	}
	server.rememberManagedCodexHome(record.JobID, home)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	server.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State: protocol.PublicStateFailed, Cleanup: protocol.CleanupClean,
		FailureClass: protocol.FailureClassInternal, FailureReason: "verification failed",
	})
	got := mustGetStoredJob(t, server, record.JobID)
	if got.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("cleanup = %q, want uncertain", got.Cleanup)
	}
	if !strings.Contains(strings.Join(got.Diagnostics, "\n"), "frozen verification copy cleanup") {
		t.Fatalf("diagnostics = %q, want frozen-copy cleanup failure", got.Diagnostics)
	}
	if info, err := os.Lstat(filepath.Join(home.path, "src")); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("unexpected src path info=%v err=%v, want retained symlink", info, err)
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("symlink target info=%v err=%v, want untouched directory", info, err)
	}
}

func TestVerifyRestartReconciliationRemovesLeftoverFrozenCopy(t *testing.T) {
	repo := newFrozenTestRepo(t)
	server := newExecutionServer(t, newCodexSandboxTestBackend(t, nil))
	record := queuedVerifyExecutionRecord(t, server, repo, false)
	layout, err := engine.LayoutForWorkspace(server.stateRoot, record.CWD)
	if err != nil {
		t.Fatal(err)
	}
	home, err := createManagedCodexHome(layout, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	defer home.close()
	src := filepath.Join(home.path, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFrozenTestFile(t, filepath.Join(src, "leftover.txt"), "left behind before restart", 0o600)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed {
		t.Fatalf("reconciled verify state = %q, want failed", got.State)
	}
	if _, err := os.Lstat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reconciled frozen copy stat error = %v, want removed", err)
	}
	if info, err := os.Stat(home.path); err != nil || !info.IsDir() {
		t.Fatalf("reconciled session home info=%v err=%v, want retained directory", info, err)
	}
}

func TestVerifyLocalIPCCreationFollowsFrozenCopy(t *testing.T) {
	originalGOOS := localIPCGOOS
	localIPCGOOS = "darwin"
	t.Cleanup(func() { localIPCGOOS = originalGOOS })
	originalTempRoot := localIPCTempRoot
	localIPCTempRoot = filepath.Join(t.TempDir(), "missing-parent")
	t.Cleanup(func() { localIPCTempRoot = originalTempRoot })

	t.Run("copy failure precedes IPC failure", func(t *testing.T) {
		originalLimit := frozenCopyMaxBytes
		frozenCopyMaxBytes = 1
		t.Cleanup(func() { frozenCopyMaxBytes = originalLimit })
		repo := newFrozenTestRepo(t)
		backend := newCodexSandboxTestBackend(t, nil)
		server := newExecutionServer(t, backend)
		server.codexSandboxProbe = func(context.Context, string) error { return nil }
		record := queuedVerifyExecutionRecord(t, server, repo, true)

		runExecution(t, server, record)
		got := mustGetStoredJob(t, server, record.JobID)
		if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassInternal || !strings.Contains(got.FailureReason, "byte bound") {
			t.Fatalf("copy failure = state:%s class:%s reason:%q, want internal byte-bound failure before IPC setup", got.State, got.FailureClass, got.FailureReason)
		}
	})

	t.Run("IPC failure removes the completed copy", func(t *testing.T) {
		repo := newFrozenTestRepo(t)
		backend := newCodexSandboxTestBackend(t, nil)
		server := newExecutionServer(t, backend)
		server.codexSandboxProbe = func(context.Context, string) error { return nil }
		record := queuedVerifyExecutionRecord(t, server, repo, true)

		runExecution(t, server, record)
		got := mustGetStoredJob(t, server, record.JobID)
		if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendUnavailable {
			t.Fatalf("IPC failure = state:%s class:%s reason:%q, want backend_unavailable", got.State, got.FailureClass, got.FailureReason)
		}
		layout, err := engine.LayoutForWorkspace(server.stateRoot, record.CWD)
		if err != nil {
			t.Fatal(err)
		}
		src := filepath.Join(layout.Codex, record.JobID, "src")
		if _, err := os.Lstat(src); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("frozen copy after IPC failure = %v, want removed", err)
		}
	})
}

func TestPruneReclaimsLeftoverVerifySourceCopy(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	cwd := t.TempDir()
	layout, err := engine.LayoutForWorkspace(server.stateRoot, cwd)
	if err != nil {
		t.Fatal(err)
	}
	const jobID = "job_prune_verify_src"
	home, err := createManagedCodexHome(layout, jobID)
	if err != nil {
		t.Fatal(err)
	}
	defer home.close()
	src := filepath.Join(home.path, "src")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	writeFrozenTestFile(t, filepath.Join(src, "leftover.txt"), "left behind after a crash", 0o600)
	outside := filepath.Join(t.TempDir(), "outside.txt")
	writeFrozenTestFile(t, outside, "must remain", 0o600)
	if err := os.Symlink(outside, filepath.Join(src, "external-link")); err != nil {
		t.Fatal(err)
	}

	var result protocol.ArtifactsPruneResult
	server.pruneJobCodexArtifacts(server.stateRoot, jobstore.Record{JobID: jobID, CWD: layout.Workspace}, false, &result)
	found := false
	for _, item := range result.Removed {
		if item.Kind == "src" {
			found = true
		}
	}
	if !found {
		t.Fatalf("prune removed items = %+v, skipped=%+v; want src", result.Removed, result.Skipped)
	}
	if _, err := os.Lstat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pruned source copy stat error = %v, want removed", err)
	}
	if contents, err := os.ReadFile(outside); err != nil || string(contents) != "must remain" {
		t.Fatalf("symlink target after pruning = %q, err=%v, want untouched", contents, err)
	}
}

func queuedVerifyExecutionRecord(t *testing.T, server *Server, cwd string, localIPC bool) jobstore.Record {
	t.Helper()
	spec := protocol.TaskSpec{Backend: "codex", CWD: cwd, Prompt: "run verification", Verify: true, LocalIPC: localIPC}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(jobstore.RequestKey{WorkspaceKey: "verify-execution", RequestID: filepath.Base(t.Name())}, raw, func(id string) (jobstore.Record, error) {
		canonical, err := engine.CanonicalWorkspace(cwd)
		if err != nil {
			return jobstore.Record{}, err
		}
		return jobstore.Record{JobID: id, Backend: spec.Backend, CWD: canonical, Write: false}, nil
	})
	if err != nil || deduplicated {
		t.Fatalf("create queued verify record = (%+v, deduplicated=%t, err=%v)", record, deduplicated, err)
	}
	return record
}

func newFrozenTestRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	runGitTest(t, repo, "init", "-q")
	runGitTest(t, repo, "config", "user.email", "agentbus-test@example.invalid")
	runGitTest(t, repo, "config", "user.name", "Agentbus Test")
	writeFrozenTestFile(t, filepath.Join(repo, "seed.txt"), "seed", 0o644)
	runGitTest(t, repo, "add", "seed.txt")
	runGitTest(t, repo, "commit", "-m", "seed")
	return repo
}

func writeFrozenTestFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func runGitTest(t *testing.T, repo string, args ...string) string {
	t.Helper()
	commandArgs := append([]string{"-C", repo}, args...)
	output, err := exec.Command("git", commandArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
	return string(output)
}
