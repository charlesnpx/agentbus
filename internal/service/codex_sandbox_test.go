//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
