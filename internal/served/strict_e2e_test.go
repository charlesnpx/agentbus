//go:build abd_strict_e2e

package served

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const strictE2ERunEnv = "AGENTBUS_RUN_STRICT_E2E"

func TestProductionStrictServe(t *testing.T) {
	if strings.TrimSpace(os.Getenv(strictE2ERunEnv)) != "1" {
		t.Skipf("set %s=1 to run strict e2e", strictE2ERunEnv)
	}
	if testing.Short() {
		t.Skip("strict e2e is not run in short mode")
	}
	if runtime.GOOS != "linux" {
		// The authoritative strict native gate runs under Linux cgroup-v2 Docker.
		// Non-Linux hosts skip instead of asserting platform-specific runtime details.
		t.Skip("strict native e2e gate runs on linux")
	}
	stateRoot := t.TempDir()
	cwd := t.TempDir()

	// Serve bootstrap probes strict backends before listening. Give the
	// default composition a PROBEABLE codex (fixture on PATH answering
	// --version at the minimum known-good version) so the strict submit
	// below travels PAST backend fenceability and is rejected by the thing
	// this gate exists to observe: the unavailable native runtime. Without
	// a probeable binary the rejection cause would be unfenceable_backend —
	// an environment artifact, not the boundary under test.
	binDir := t.TempDir()
	fakeCodex := filepath.Join(binDir, "codex")
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo \"codex-cli " + codexcli.MinimumKnownGoodVersion + "\"; exit 0; fi\nif [ \"$1\" = \"--help\" ]; then echo \"codex help\"; exit 0; fi\nexit 0\n"
	if err := os.WriteFile(fakeCodex, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	server, err := New(Config{
		StateRoot: stateRoot,
		CWD:       cwd,
		Backends: []engine.Backend{
			codexcli.New(codexcli.Options{}),
		},
	})
	if err != nil {
		t.Fatalf("New production strict server: %v", err)
	}
	if server.admissionRuntimeFactory != nil {
		t.Fatal("production strict e2e must not install an admission runtime factory")
	}

	serveCtx, stopServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		// This is the unmodified exported Serve entrypoint used by
		// cmd/agentbus runServe; the test does not inject runtime,
		// custodian, authority, listener, or jobs.requestId hooks.
		serveDone <- server.Serve(serveCtx)
	}()
	t.Cleanup(func() {
		stopServe()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("Serve returned during cleanup: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve did not stop after cleanup cancellation")
		}
	})

	client := connectProductionStrictClient(t, stateRoot, serveDone)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client close: %v", err)
		}
	})

	hello := client.HelloResult()
	// The default composition really does register the configured backend, so the
	// rejection below cannot be an incidental "unknown backend" error masquerading
	// as the admission gate.
	if !slices.Contains(hello.Backends, "codex") {
		t.Fatalf("default production Serve did not advertise the codex backend: %v", hello.Backends)
	}
	// jobs.requestId stays unadvertised in the default composition; strict
	// identified admission is no longer gated by this legacy capability flag.
	if hello.Capabilities["jobs.requestId"] {
		t.Fatal("jobs.requestId capability is unexpectedly advertised by the default composition")
	}

	// RED baseline: the default production composition does NOT accept a strict
	// identified submission.
	//
	// Current meaning: the strict route exists; rejection is now caused by the
	// unavailable native runtime in the real default Serve composition, not by
	// the legacy jobs.requestId capability gate.
	//
	// TODO(R7B): flip to GREEN — assert the real strict identified job launches,
	//   completes, and is independently proven contained.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.JobSubmit(ctx, agentclient.JobSubmitParams{
		WorkspaceKey: "workspace-production-strict-e2e",
		RequestID:    "request-production-strict-e2e",
		TaskSpec: agentclient.TaskSpec{
			Backend: "codex",
			CWD:     cwd,
			Write:   false,
			Prompt:  "strict e2e should be rejected before backend launch",
		},
	})
	if err == nil {
		t.Fatal("strict identified job unexpectedly submitted by the default composition")
	}
	assertProductionStrictUnavailable(t, err)
	t.Log("strict_admission_native_runtime_unavailable")
}

func connectProductionStrictClient(t *testing.T, stateRoot string, serveDone <-chan error) *agentclient.Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-serveDone:
			t.Fatalf("Serve exited before client connection: %v", err)
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		client, err := agentclient.Connect(ctx, agentclient.Options{
			StateRoot:        stateRoot,
			DisableAutoStart: true,
		})
		cancel()
		if err == nil {
			return client
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("client did not connect to production strict server: %v", lastErr)
	return nil
}

func assertProductionStrictUnavailable(t *testing.T, err error) {
	t.Helper()
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("strict identified submit error = %T %v, want RPCError", err, err)
	}
	if rpcErr.Object.Data.Code != protocol.ErrorCapabilityMissing {
		t.Fatalf("strict identified submit code = %q message = %q, want %q", rpcErr.Object.Data.Code, rpcErr.Object.Message, protocol.ErrorCapabilityMissing)
	}
	if rpcErr.Object.Data.AdmissionCause != protocol.AdmissionRejectUnavailableNativeRuntime {
		t.Fatalf("strict identified submit admission cause = %q message = %q, want %q", rpcErr.Object.Data.AdmissionCause, rpcErr.Object.Message, protocol.AdmissionRejectUnavailableNativeRuntime)
	}
	if rpcErr.Object.Data.RuntimeSupport == nil {
		t.Fatalf("strict identified submit runtime support = nil, want unavailable native runtime support assessment")
	}
	if rpcErr.Object.Data.RuntimeSupport.Class == "" || rpcErr.Object.Data.RuntimeSupport.Class == "available" {
		t.Fatalf("strict identified submit runtime support = %+v, want non-available native runtime", rpcErr.Object.Data.RuntimeSupport)
	}
	if strings.Contains(rpcErr.Object.Message, "jobs.requestId capability is disabled") {
		t.Fatalf("strict identified submit message = %q, want native-runtime rejection not jobs.requestId gate", rpcErr.Object.Message)
	}
}
