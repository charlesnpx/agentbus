package command

import (
	"context"
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
	terminateProcessGroup = func(_ *exec.Cmd, grace time.Duration) error {
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

func TestDirectCommandRunnerBuffersUndrainedStreams(t *testing.T) {
	running, err := (DirectCommandRunner{}).Start(context.Background(), ExecSpec{
		Argv: []string{"/bin/sh", "-c", "printf stdout; printf stderr >&2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Stdin().Close() }()
	defer func() { _ = running.Stdout().Close() }()
	defer func() { _ = running.Stderr().Close() }()

	exit, err := running.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait err = %v", err)
	}
	if !exit.Exited || exit.Code != 0 {
		t.Fatalf("exit = %+v, want clean exit", exit)
	}
	stdout, err := io.ReadAll(running.Stdout())
	if err != nil {
		t.Fatalf("stdout read err = %v", err)
	}
	if string(stdout) != "stdout" {
		t.Fatalf("stdout = %q, want stdout", string(stdout))
	}
	stderr, err := io.ReadAll(running.Stderr())
	if err != nil {
		t.Fatalf("stderr read err = %v", err)
	}
	if string(stderr) != "stderr" {
		t.Fatalf("stderr = %q, want stderr", string(stderr))
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
	terminateProcessGroup = func(cmd *exec.Cmd, grace time.Duration) error {
		calls.Add(1)
		return original(cmd, grace)
	}
	t.Cleanup(func() { terminateProcessGroup = original })

	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, ExecSpec{
		Argv: []string{"/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done"},
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
