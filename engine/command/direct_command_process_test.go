//go:build !windows

package command

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestTerminateProcessGroupExitsOnSIGTERMWithoutEscalation(t *testing.T) {
	const grace = 300 * time.Millisecond
	cmd, terminator, _ := startTerminationTestProcess(t, "trap 'exit 0' TERM; printf 'ready\\n'; while :; do :; done", grace)
	requireStartToken(t, terminator.capturedProcessRef())
	var calls []syscall.Signal
	groupGone := false
	setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		switch signal {
		case 0:
			if groupGone {
				return syscall.ESRCH
			}
		case syscall.SIGTERM:
			groupGone = true
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	})

	started := time.Now()
	if err := terminator.terminate(); err != nil {
		t.Fatalf("terminate() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= grace {
		t.Fatalf("terminate() took %s, want return before the %s escalation grace", elapsed, grace)
	}
	assertNoProcessGroupSignal(t, calls, syscall.SIGKILL)
	_ = cmd.Wait()
}

func TestTerminateProcessGroupEscalatesProcessIgnoringSIGTERM(t *testing.T) {
	const grace = 50 * time.Millisecond
	cmd, terminator, _ := startTerminationTestProcess(t, "trap '' TERM; printf 'ready\\n'; while :; do :; done", grace)
	requireStartToken(t, terminator.capturedProcessRef())
	var calls []syscall.Signal
	setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		if signal == syscall.SIGKILL {
			return cmd.Process.Kill()
		}
		return nil
	})

	if err := terminator.terminate(); err != nil {
		t.Fatalf("terminate() error = %v", err)
	}
	assertProcessGroupSignal(t, calls, syscall.SIGTERM)
	assertProcessGroupSignal(t, calls, syscall.SIGKILL)
	if err := cmd.Wait(); err == nil {
		t.Fatal("Wait() succeeded, want process group killed after ignoring SIGTERM")
	}
	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("exit status = %#v, want SIGKILL", cmd.ProcessState.Sys())
	}
}

func TestTerminateProcessGroupMismatchedTokenSendsNoSignal(t *testing.T) {
	const grace = 300 * time.Millisecond
	cmd, terminator, ref := startTerminationTestProcess(t, "trap '' TERM; printf 'ready\\n'; while :; do :; done", grace)
	requireStartToken(t, ref)
	ref.StartTime += "-wrong"
	setTerminatorProcessRef(terminator, ref)
	var calls []syscall.Signal
	setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		return nil
	})

	started := time.Now()
	err := terminator.terminate()
	if !errors.Is(err, engine.ErrProcessIdentityUnverifiable) {
		t.Fatalf("terminate() error = %v, want ErrProcessIdentityUnverifiable", err)
	}
	if elapsed := time.Since(started); elapsed >= grace/2 {
		t.Fatalf("terminate() took %s, want prompt token rejection without signaling", elapsed)
	}
	assertNoTerminationSignal(t, calls)
	assertProcessRunning(t, cmd.Process.Pid)
}

func TestTerminateProcessGroupMissingTokenSendsNoSignal(t *testing.T) {
	const grace = 300 * time.Millisecond
	cmd, terminator, ref := startTerminationTestProcess(t, "trap '' TERM; printf 'ready\\n'; while :; do :; done", grace)
	requireStartToken(t, ref)
	ref.StartTime = ""
	setTerminatorProcessRef(terminator, ref)
	var calls []syscall.Signal
	setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		return nil
	})

	started := time.Now()
	err := terminator.terminate()
	if !errors.Is(err, engine.ErrProcessIdentityUnverifiable) {
		t.Fatalf("terminate() error = %v, want ErrProcessIdentityUnverifiable", err)
	}
	if elapsed := time.Since(started); elapsed >= grace/2 {
		t.Fatalf("terminate() took %s, want prompt token rejection without signaling", elapsed)
	}
	assertNoTerminationSignal(t, calls)
	assertProcessRunning(t, cmd.Process.Pid)
}

func TestDirectCommandRunnerCanceledWithMissingTokenReturnsPromptlyAndLeavesProcessAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, ExecSpec{
		Argv: []string{"/bin/sh", "-c", "read line"},
	})
	if err != nil {
		t.Fatal(err)
	}
	command, ok := running.(*directRunningCommand)
	if !ok {
		t.Fatalf("running command type = %T, want *directRunningCommand", running)
	}
	t.Cleanup(func() {
		_, _ = command.Stdin().Write([]byte("done\n"))
		_ = command.Stdin().Close()
		_ = command.Stdout().Close()
		_ = command.Stderr().Close()
		select {
		case <-command.waitDone:
		case <-time.After(time.Second):
			t.Error("asynchronous reaper did not reap process")
		}
	})

	ref := command.terminator.capturedProcessRef()
	ref.StartTime = ""
	setTerminatorProcessRef(command.terminator, ref)

	cancel()
	waitDone := make(chan error, 1)
	go func() {
		_, err := command.Wait(context.Background())
		waitDone <- err
	}()
	select {
	case err := <-waitDone:
		if !errors.Is(err, engine.ErrProcessIdentityUnverifiable) {
			t.Fatalf("Wait() error = %v, want ErrProcessIdentityUnverifiable", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait blocked after cancellation identity rejection")
	}
	assertProcessRunning(t, command.cmd.Process.Pid)
}

func TestDirectCommandRunnerCanceledWithMismatchedTokenKeepsWritingProcessAlive(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group fixtures require a native Linux or Darwin start token")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, ExecSpec{
		Argv: []string{"/usr/bin/yes"},
	})
	if err != nil {
		t.Fatal(err)
	}
	command, ok := running.(*directRunningCommand)
	if !ok {
		t.Fatalf("running command type = %T, want *directRunningCommand", running)
	}
	capturedRef := command.terminator.capturedProcessRef()
	requireStartToken(t, capturedRef)
	t.Cleanup(func() { stopDirectRunningCommand(t, command, capturedRef) })

	outputDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 1)
		_, err := command.Stdout().Read(buf)
		outputDone <- err
	}()
	select {
	case err := <-outputDone:
		if err != nil {
			t.Fatalf("initial stdout read error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("writing process produced no stdout")
	}

	mismatchedRef := capturedRef
	mismatchedRef.StartTime += "-wrong"
	setTerminatorProcessRef(command.terminator, mismatchedRef)
	cancel()
	waitDone := make(chan error, 1)
	go func() {
		_, err := command.Wait(context.Background())
		waitDone <- err
	}()
	select {
	case err := <-waitDone:
		if !errors.Is(err, engine.ErrProcessIdentityUnverifiable) {
			t.Fatalf("Wait() error = %v, want ErrProcessIdentityUnverifiable", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait blocked after cancellation identity rejection")
	}

	select {
	case <-time.After(100 * time.Millisecond):
		assertProcessRunning(t, command.cmd.Process.Pid)
	}
}

func TestDirectCommandRunnerKilledCancellationRetainsExecutionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, ExecSpec{
		Argv: []string{"/bin/sh", "-c", "trap '' TERM; printf 'ready\\n'; while :; do :; done"},
	})
	if err != nil {
		t.Fatal(err)
	}
	command, ok := running.(*directRunningCommand)
	if !ok {
		t.Fatalf("running command type = %T, want *directRunningCommand", running)
	}
	capturedRef := command.terminator.capturedProcessRef()
	requireStartToken(t, capturedRef)
	t.Cleanup(func() { stopDirectRunningCommand(t, command, capturedRef) })

	ready := make(chan []byte, 1)
	readyErr := make(chan error, 1)
	go func() {
		output, err := io.ReadAll(io.LimitReader(command.Stdout(), int64(len("ready\n"))))
		if err != nil {
			readyErr <- err
			return
		}
		ready <- output
	}()
	select {
	case err := <-readyErr:
		t.Fatalf("ready read error = %v", err)
	case output := <-ready:
		if string(output) != "ready\n" {
			t.Fatalf("ready output = %q, want ready", output)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("TERM-ignoring process did not become ready")
	}

	cancel()
	type waitResult struct {
		exit ExitObservation
		err  error
	}
	done := make(chan waitResult, 1)
	go func() {
		exit, err := command.Wait(context.Background())
		done <- waitResult{exit: exit, err: err}
	}()
	var result waitResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("Wait blocked after KILL escalation")
	}

	observation := FinalObservation{Exit: result.exit, ExecutionErr: result.err}
	if observation.Exit.Signal != syscall.SIGKILL.String() {
		t.Fatalf("observation exit signal = %q, want %q", observation.Exit.Signal, syscall.SIGKILL.String())
	}
	var exitErr *exec.ExitError
	if !errors.As(observation.ExecutionErr, &exitErr) {
		t.Fatalf("observation execution error = %v, want *exec.ExitError", observation.ExecutionErr)
	}
	if errors.Is(observation.ExecutionErr, context.Canceled) {
		t.Fatalf("observation execution error = %v, must retain execution failure instead of context.Canceled", observation.ExecutionErr)
	}
}

func TestTerminateProcessGroupAlreadyExitedReturnsPromptly(t *testing.T) {
	const grace = 300 * time.Millisecond
	cmd, terminator, _ := startTerminationTestProcess(t, "printf 'ready\\n'; exit 0", grace)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	var calls []syscall.Signal
	setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
		calls = append(calls, signal)
		if signal == 0 {
			return syscall.ESRCH
		}
		return nil
	})

	started := time.Now()
	if err := terminator.terminate(); err != nil {
		t.Fatalf("terminate() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed >= grace/2 {
		t.Fatalf("terminate() took %s, want prompt already-exited success", elapsed)
	}
	assertNoTerminationSignal(t, calls)
}

func TestTerminateProcessGroupPermissionDeniedStopsWithoutWaitingOrEscalating(t *testing.T) {
	const grace = 300 * time.Millisecond
	tests := []struct {
		name string
		fail func(probeCalls int, signal syscall.Signal) error
	}{
		{
			name: "SIGTERM",
			fail: func(_ int, signal syscall.Signal) error {
				if signal == syscall.SIGTERM {
					return syscall.EPERM
				}
				return nil
			},
		},
		{
			name: "wait probe",
			fail: func(probeCalls int, signal syscall.Signal) error {
				if signal == 0 && probeCalls > 1 {
					return syscall.EPERM
				}
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, terminator, ref := startTerminationTestProcess(t, "trap '' TERM; printf 'ready\\n'; while :; do :; done", grace)
			requireStartToken(t, ref)
			var calls []syscall.Signal
			probeCalls := 0
			setProcessGroupSignal(t, func(_ int, signal syscall.Signal) error {
				calls = append(calls, signal)
				if signal == 0 {
					probeCalls++
				}
				return tt.fail(probeCalls, signal)
			})

			started := time.Now()
			err := terminator.terminate()
			if !errors.Is(err, syscall.EPERM) {
				t.Fatalf("terminate() error = %v, want EPERM", err)
			}
			if elapsed := time.Since(started); elapsed >= grace/2 {
				t.Fatalf("terminate() took %s, want prompt permission failure", elapsed)
			}
			assertNoProcessGroupSignal(t, calls, syscall.SIGKILL)
			assertProcessRunning(t, cmd.Process.Pid)
		})
	}
}

func TestProcessGroupSignalResultClassifiesPermissionDenied(t *testing.T) {
	for _, action := range []string{"send SIGTERM to", "inspect"} {
		gone, err := processGroupSignalResult(action, 123, syscall.EPERM)
		if gone {
			t.Fatalf("%s: gone = true, want false", action)
		}
		if !errors.Is(err, syscall.EPERM) {
			t.Fatalf("%s: error = %v, want EPERM", action, err)
		}
	}
}

func startTerminationTestProcess(t *testing.T, script string, grace time.Duration) (*exec.Cmd, *directTerminator, engine.ProcessRef) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group fixtures require a native Linux or Darwin start token")
	}

	cmd := exec.Command("/bin/sh", "-c", script)
	setProcessGroup(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	terminator := &directTerminator{cmd: cmd, grace: grace}
	terminator.captureProcessRef()
	ref := terminator.capturedProcessRef()
	t.Cleanup(func() { stopTerminationTestProcess(t, cmd) })

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "ready\n" {
		t.Fatalf("ready line = %q, want ready", line)
	}
	_ = stdout.Close()
	if ref.PID != cmd.Process.Pid {
		t.Fatalf("captured pid = %d, want %d", ref.PID, cmd.Process.Pid)
	}
	if ref.PGID <= 0 {
		t.Fatal("captured process group id is missing")
	}
	return cmd, terminator, ref
}

func setTerminatorProcessRef(terminator *directTerminator, ref engine.ProcessRef) {
	terminator.processRef = ref
}

func setProcessGroupSignal(t *testing.T, signal func(int, syscall.Signal) error) {
	t.Helper()
	original := processGroupSignal
	processGroupSignal = signal
	t.Cleanup(func() { processGroupSignal = original })
}

func requireStartToken(t *testing.T, ref engine.ProcessRef) {
	t.Helper()
	if ref.StartTime == "" {
		t.Skip("native process table did not provide a start token")
	}
}

func assertProcessGroupSignal(t *testing.T, calls []syscall.Signal, want syscall.Signal) {
	t.Helper()
	for _, signal := range calls {
		if signal == want {
			return
		}
	}
	t.Fatalf("signals = %v, want %s", calls, want)
}

func assertNoProcessGroupSignal(t *testing.T, calls []syscall.Signal, unwanted syscall.Signal) {
	t.Helper()
	for _, signal := range calls {
		if signal == unwanted {
			t.Fatalf("signals = %v, do not want %s", calls, unwanted)
		}
	}
}

func assertNoTerminationSignal(t *testing.T, calls []syscall.Signal) {
	t.Helper()
	assertNoProcessGroupSignal(t, calls, syscall.SIGTERM)
	assertNoProcessGroupSignal(t, calls, syscall.SIGKILL)
}

func assertProcessRunning(t *testing.T, pid int) {
	t.Helper()
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("process %d is not running after identity rejection: %v", pid, err)
	}
}

func stopTerminationTestProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Kill()
	if err := cmd.Wait(); err != nil && cmd.ProcessState == nil {
		t.Errorf("cleanup Wait() error = %v", err)
	}
}

func stopDirectRunningCommand(t *testing.T, command *directRunningCommand, ref engine.ProcessRef) {
	t.Helper()
	if command == nil || command.cmd == nil {
		return
	}
	command.startWait()
	terminator := &directTerminator{cmd: command.cmd, grace: 10 * time.Millisecond, processRef: ref}
	if err := terminator.terminate(); err != nil && !errors.Is(err, engine.ErrProcessIdentityUnverifiable) {
		t.Errorf("cleanup terminate() error = %v", err)
	}
	if command.stdin != nil {
		_ = command.stdin.Close()
	}
	if command.stdout != nil {
		_ = command.stdout.Close()
	}
	if command.stderr != nil {
		_ = command.stderr.Close()
	}
	select {
	case <-command.waitDone:
	case <-time.After(time.Second):
		t.Error("asynchronous reaper did not reap process")
	}
}
