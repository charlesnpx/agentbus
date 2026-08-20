package agentbusserve

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/service"
)

func TestStartupFailureCodeRetainsLostRaceSignal(t *testing.T) {
	if got := startupFailureCode(service.DaemonAlreadyListeningError{}); got != daemonlaunch.CodeAlreadyListening {
		t.Fatalf("already-listening code = %q, want %q", got, daemonlaunch.CodeAlreadyListening)
	}
	if got := startupFailureCode(errors.New("store open failed")); got != "error" {
		t.Fatalf("ordinary failure code = %q, want error", got)
	}
}

func TestServeUsesServiceServerAndGracefullyShutsItDown(t *testing.T) {
	server := &fakeServiceServer{serving: make(chan struct{}), stopped: make(chan struct{})}
	previous := newServiceServer
	newServiceServer = func(cfg service.Config) (serviceServer, error) {
		if cfg.StateRoot != "state-root" {
			t.Fatalf("service config state root = %q", cfg.StateRoot)
		}
		return server, nil
	}
	t.Cleanup(func() { newServiceServer = previous })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, Config{StateRoot: "state-root"}) }()
	select {
	case <-server.serving:
	case <-time.After(time.Second):
		t.Fatal("service server did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not shut down")
	}
	if !server.shutdownCalled {
		t.Fatal("service Shutdown was not called")
	}
}

func TestCancellationDoesNotMaskTypedBootstrapFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := errors.Join(service.DaemonAlreadyListeningError{}, context.Canceled)
	if cleanServeTerminationAfterCancel(ctx, err) {
		t.Fatal("typed startup failure was treated as clean cancellation")
	}
}

type fakeServiceServer struct {
	serving        chan struct{}
	stopped        chan struct{}
	shutdownCalled bool
}

func (server *fakeServiceServer) ServeWithStartupContext(ctx, _ context.Context) error {
	close(server.serving)
	select {
	case <-ctx.Done():
		return nil
	case <-server.stopped:
		return nil
	}
}

func (server *fakeServiceServer) Shutdown(context.Context) error {
	server.shutdownCalled = true
	close(server.stopped)
	return nil
}

func (server *fakeServiceServer) ShutdownTimeout() time.Duration { return time.Second }
