//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type codexSandboxTestBackend struct {
	*executionFakeBackend
	executable string
}

func (backend *codexSandboxTestBackend) BackendInspection() engine.BackendInspection {
	return engine.BackendInspection{Executable: backend.executable}
}

func (*codexSandboxTestBackend) CodexSandboxRequired(write, localIPC bool) bool {
	return !write || localIPC
}

func (*codexSandboxTestBackend) CodexSandboxArgs(engine.SessionOpts) ([]string, error) {
	return []string{"-P", "agentbus_local_ipc"}, nil
}

func newCodexSandboxTestBackend(t *testing.T, start func(context.Context, engine.SessionOpts) (engine.Session, error)) *codexSandboxTestBackend {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nprintf 'codex-cli 0.165.0\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	backend := &executionFakeBackend{name: "codex", start: start}
	return &codexSandboxTestBackend{executionFakeBackend: backend, executable: resolved}
}

func queuedSandboxTestRecord(t *testing.T, server *Server, workspaceKey, requestID, cwd string) jobstore.Record {
	t.Helper()
	spec := protocol.TaskSpec{Backend: "codex", CWD: cwd, Prompt: "sandbox probe test", Write: false}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: workspaceKey, RequestID: requestID}, raw,
		func(id string) (jobstore.Record, error) {
			return jobstore.Record{JobID: id, Backend: "codex", CWD: cwd, Write: false}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("new sandbox test record was unexpectedly deduplicated")
	}
	return record
}

func TestCodexSandboxStartFailureStopsBeforeBackendStart(t *testing.T) {
	var starts atomic.Int64
	backend := newCodexSandboxTestBackend(t, func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return claimedResultSession("unexpected start"), nil
	})
	server := newExecutionServer(t, backend)
	server.codexSandboxProbe = func(_ context.Context, binary string) error {
		if binary != backend.executable {
			t.Fatalf("sandbox probe binary = %q, want resolved %q", binary, backend.executable)
		}
		return errors.New("bwrap: No permissions to create new namespace")
	}
	record := queuedSandboxTestRecord(t, server, "sandbox-fail-workspace", "sandbox-fail-request", t.TempDir())

	runExecution(t, server, record)
	got := mustGetStoredJob(t, server, record.JobID)
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendUnavailable {
		t.Fatalf("sandbox failure record = state:%s class:%s reason:%q, want backend_unavailable", got.State, got.FailureClass, got.FailureReason)
	}
	if !strings.Contains(got.FailureReason, "Codex OS sandbox cannot start on this host") || !strings.Contains(got.FailureReason, "bwrap: No permissions to create new namespace") {
		t.Fatalf("sandbox failure reason = %q, want sandbox diagnosis and stderr", got.FailureReason)
	}
	if starts.Load() != 0 || got.ProcessClaim != nil {
		t.Fatalf("backend starts=%d processClaim=%#v, want no app-server launch", starts.Load(), got.ProcessClaim)
	}
}

func TestCodexSandboxStartSuccessIsCachedForTwoJobs(t *testing.T) {
	var starts atomic.Int64
	backend := newCodexSandboxTestBackend(t, func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return claimedResultSession("sandbox passed"), nil
	})
	server := newExecutionServer(t, backend)
	var probes atomic.Int64
	server.codexSandboxProbe = func(_ context.Context, binary string) error {
		if binary != backend.executable {
			t.Fatalf("sandbox probe binary = %q, want resolved %q", binary, backend.executable)
		}
		probes.Add(1)
		return nil
	}

	for index := 0; index < 2; index++ {
		record := queuedSandboxTestRecord(t, server, fmt.Sprintf("sandbox-pass-workspace-%d", index), fmt.Sprintf("sandbox-pass-request-%d", index), t.TempDir())
		runExecution(t, server, record)
		if got := mustGetStoredJob(t, server, record.JobID); got.State != protocol.PublicStateCompleted {
			t.Fatalf("job %d state=%s failure=%s, want completed", index, got.State, got.FailureReason)
		}
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("sandbox probes=%d, want one cached result", got)
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("backend starts=%d, want one app-server start per job", got)
	}
}

func TestCodexLocalIPCProbeFailureIsCachedAndCleansRoots(t *testing.T) {
	originalGOOS := localIPCGOOS
	localIPCGOOS = "linux"
	t.Cleanup(func() { localIPCGOOS = originalGOOS })
	originalTempRoot := localIPCTempRoot
	tempRoot, err := os.MkdirTemp("/tmp", "ipct-")
	if err != nil {
		t.Fatal(err)
	}
	localIPCTempRoot = tempRoot
	t.Cleanup(func() {
		localIPCTempRoot = originalTempRoot
		_ = os.RemoveAll(tempRoot)
	})

	var starts atomic.Int64
	backend := newCodexSandboxTestBackend(t, func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return claimedResultSession("unexpected backend start"), nil
	})
	server := newExecutionServer(t, backend)
	server.codexSandboxProbe = func(context.Context, string) error { return nil }
	var probes atomic.Int64
	var firstRoot string
	server.codexLocalIPCProbe = func(_ context.Context, binary string, args []string, cwd, executable, ipcRoot string) error {
		if binary != backend.executable || cwd == "" || executable == "" || !filepath.IsAbs(ipcRoot) {
			t.Fatalf("local-IPC probe args: binary=%q cwd=%q executable=%q IPC root=%q", binary, cwd, executable, ipcRoot)
		}
		if !reflect.DeepEqual(args[len(args)-2:], []string{"-P", "agentbus_local_ipc"}) {
			t.Fatalf("local-IPC sandbox args = %#v, want profile selector", args)
		}
		if _, err := os.Stat(ipcRoot); err != nil {
			t.Fatalf("IPC root does not exist during capability probe: %v", err)
		}
		probes.Add(1)
		firstRoot = ipcRoot
		return errors.New("bind(AF_UNIX) = EPERM")
	}

	for index := 0; index < 2; index++ {
		cwd := t.TempDir()
		record := queuedLocalIPCSandboxTestRecord(t, server, fmt.Sprintf("local-ipc-probe-workspace-%d", index), fmt.Sprintf("local-ipc-probe-request-%d", index), cwd)
		runExecution(t, server, record)
		got := mustGetStoredJob(t, server, record.JobID)
		if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendUnavailable || !strings.HasPrefix(got.FailureReason, "local IPC is unsupported by the Codex sandbox on this host:") || !strings.Contains(got.FailureReason, "bind(AF_UNIX) = EPERM") {
			t.Fatalf("local-IPC probe failure record = state:%s class:%s reason:%q", got.State, got.FailureClass, got.FailureReason)
		}
	}
	if probes.Load() != 1 || starts.Load() != 0 {
		t.Fatalf("local-IPC probes=%d backend starts=%d, want one cached probe and no start", probes.Load(), starts.Load())
	}
	if firstRoot == "" {
		t.Fatal("local-IPC probe did not observe a root")
	}
	if _, err := os.Stat(firstRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed probe IPC root = %v, want removed", err)
	}
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("IPC roots after cached probe failure = %#v, want all removed", entries)
	}
}

func queuedLocalIPCSandboxTestRecord(t *testing.T, server *Server, workspaceKey, requestID, cwd string) jobstore.Record {
	t.Helper()
	spec := protocol.TaskSpec{Backend: "codex", CWD: cwd, Prompt: "local IPC sandbox probe test", Write: true, LocalIPC: true}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: workspaceKey, RequestID: requestID}, raw,
		func(id string) (jobstore.Record, error) {
			return jobstore.Record{JobID: id, Backend: "codex", CWD: cwd, Write: true}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("new local-IPC sandbox test record was unexpectedly deduplicated")
	}
	return record
}
