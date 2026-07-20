//go:build abd_strict_e2e

package served

import (
	"context"
	"errors"
	"os"
	"runtime"
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

	if client.HelloResult().Capabilities["jobs.requestId"] {
		t.Fatal("jobs.requestId capability is unexpectedly advertised")
	}

	// Current RED baseline: production Serve does not advertise the strict
	// request-id capability, so this identified submission is rejected before
	// any backend launch or fixture runtime can be involved.
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
		// TODO(R7B): replace this RED assertion with a GREEN assertion that the
		// real strict identified job launches, completes, and is contained.
		t.Fatal("strict identified job unexpectedly submitted")
	}
	assertProductionStrictUnavailable(t, err)
	t.Log("strict_native_runtime_unavailable")
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
	if !strings.Contains(rpcErr.Object.Message, "jobs.requestId capability is disabled") {
		t.Fatalf("strict identified submit message = %q, want production strict admission unavailable", rpcErr.Object.Message)
	}
}
