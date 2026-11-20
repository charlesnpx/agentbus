package command

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestSessionInterruptUsesProtocolDefaultGrace(t *testing.T) {
	original := terminateProcessGroup
	defer func() { terminateProcessGroup = original }()
	seen := make(chan time.Duration, 1)
	terminateProcessGroup = func(_ *exec.Cmd, _ engine.ProcessRef, grace time.Duration) error {
		seen <- grace
		return nil
	}
	command := &directRunningCommand{
		cmd:         &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
		cancelGrace: engine.DefaultCancelGrace,
	}
	if err := command.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-seen:
		if got != engine.DefaultCancelGrace {
			t.Fatalf("grace = %s, want %s", got, engine.DefaultCancelGrace)
		}
	default:
		t.Fatal("interrupt did not terminate the active process group")
	}
}

func TestDirectCommandRunnerClosesRetainedPipeAfterLeaderExit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("retained stdout fixture is unix shell-specific")
	}
	running, err := (DirectCommandRunner{CancelGrace: 25 * time.Millisecond}).Start(context.Background(), ExecSpec{
		Argv: []string{"/bin/sh", "-c", "printf 'leader-output\\n'; (sleep 1) & exit 0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer running.Stdin().Close()
	defer running.Stdout().Close()
	defer running.Stderr().Close()
	done := make(chan struct{})
	go func() { defer close(done); _, _ = io.Copy(io.Discard, running.Stderr()) }()
	stdout, readErr := io.ReadAll(running.Stdout())
	if string(stdout) != "leader-output\n" || !errors.Is(readErr, ErrOutputDrainExpired) {
		t.Fatalf("stdout = %q, err = %v, want output and drain expiry", stdout, readErr)
	}
	<-done
	exit, err := running.Wait(context.Background())
	if err != nil || !exit.Exited || exit.Code != 0 {
		t.Fatalf("exit = %+v, err = %v", exit, err)
	}
}

func TestDirectCommandRunnerTimeoutTerminatesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	assertDirectCommandRunnerContextDoneTerminatesOnce(t, ctx, func() {})
}

func TestDirectCommandRunnerContextCancelTerminatesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	assertDirectCommandRunnerContextDoneTerminatesOnce(t, ctx, cancel)
}

func assertDirectCommandRunnerContextDoneTerminatesOnce(t *testing.T, ctx context.Context, triggerCancel func()) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("process group signal assertions are unix-only")
	}
	original := terminateProcessGroup
	var calls atomic.Int32
	terminateProcessGroup = func(cmd *exec.Cmd, ref engine.ProcessRef, grace time.Duration) error {
		calls.Add(1)
		return original(cmd, ref, grace)
	}
	t.Cleanup(func() { terminateProcessGroup = original })

	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, ExecSpec{
		Argv: []string{"/bin/sh", "-c", "trap '' TERM; while IFS= read -r ignored; do :; done"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Stdin().Close() }()
	defer func() { _ = running.Stdout().Close() }()
	defer func() { _ = running.Stderr().Close() }()

	triggerCancel()
	_, err = running.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait succeeded, want cancellation error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("terminateProcessGroup calls = %d, want 1", got)
	}
}
