package cliadapter

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
)

// DirectCommandRunner preserves the legacy adapter subprocess path.
type DirectCommandRunner struct {
	CancelGrace time.Duration
}

func (r DirectCommandRunner) Start(ctx context.Context, spec custodian.ExecSpec) (custodian.RunningCommand, error) {
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return nil, errors.New("command argv is required")
	}
	grace := r.CancelGrace
	if grace == 0 {
		grace = engine.DefaultCancelGrace
	}
	cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...)
	cmd.Cancel = func() error { return terminateProcessGroup(cmd, grace) }
	cmd.WaitDelay = 200 * time.Millisecond
	if spec.Dir != "" {
		cmd.Dir = spec.Dir
	}
	if spec.Env != nil {
		cmd.Env = append([]string(nil), spec.Env...)
	}
	setProcessGroup(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	return &directRunningCommand{
		cmd:         cmd,
		stdin:       stdin,
		stdout:      stdout,
		stderr:      stderr,
		cancelGrace: grace,
	}, nil
}

type directRunningCommand struct {
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	stderr      io.ReadCloser
	cancelGrace time.Duration
}

func (r *directRunningCommand) Stdin() io.WriteCloser {
	return r.stdin
}

func (r *directRunningCommand) Stdout() io.ReadCloser {
	return r.stdout
}

func (r *directRunningCommand) Stderr() io.ReadCloser {
	return r.stderr
}

func (r *directRunningCommand) Wait(ctx context.Context) (custodian.ExitObservation, error) {
	done := make(chan error, 1)
	go func() {
		done <- r.cmd.Wait()
	}()
	select {
	case err := <-done:
		return exitObservationForCmd(r.cmd), err
	case <-ctx.Done():
		_ = r.Interrupt(context.Background())
		err := <-done
		return exitObservationForCmd(r.cmd), err
	}
}

func (r *directRunningCommand) Interrupt(ctx context.Context) error {
	done := make(chan error, 1)
	go func() { done <- terminateProcessGroup(r.cmd, r.cancelGrace) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (r *directRunningCommand) ProcessRef() (engine.ProcessRef, int) {
	ref := processRefForCmd(r.cmd)
	return ref, ref.PID
}

func processRefForCmd(cmd *exec.Cmd) engine.ProcessRef {
	ref := engine.ProcessRef{}
	if cmd == nil || cmd.Process == nil {
		return ref
	}
	ref.PID = cmd.Process.Pid
	if runtime.GOOS != "windows" {
		if pgid, err := syscall.Getpgid(ref.PID); err == nil {
			ref.PGID = pgid
		}
	}
	if info, alive, err := (engine.NativeProcessTable{}).Lookup(ref.PID); err == nil && alive {
		ref.StartTime = info.StartTime
	}
	return ref
}

func exitObservationForCmd(cmd *exec.Cmd) custodian.ExitObservation {
	observation := custodian.ExitObservation{}
	if cmd == nil || cmd.ProcessState == nil {
		return observation
	}
	observation.Exited = cmd.ProcessState.Exited()
	observation.Code = cmd.ProcessState.ExitCode()
	if runtime.GOOS != "windows" {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			observation.Signal = status.Signal().String()
		}
	}
	return observation
}
