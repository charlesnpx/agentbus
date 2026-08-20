package agentbusserve

import (
	"context"
	"errors"
	"testing"

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

func TestCancellationDoesNotMaskTypedBootstrapFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := errors.Join(service.DaemonAlreadyListeningError{}, context.Canceled)
	if cleanServeTerminationAfterCancel(ctx, err) {
		t.Fatal("typed startup failure was treated as clean cancellation")
	}
}
