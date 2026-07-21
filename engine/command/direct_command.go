package command

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

// DirectCommandRunner preserves the legacy subprocess launch path.
type DirectCommandRunner struct {
	CancelGrace time.Duration
}

const (
	directOutputBufferLimit = 16 << 20
	directStderrDrainGrace  = 200 * time.Millisecond
)

func (r DirectCommandRunner) Start(ctx context.Context, spec ExecSpec) (RunningCommand, error) {
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return nil, errors.New("command argv is required")
	}
	grace := r.CancelGrace
	if grace == 0 {
		grace = engine.DefaultCancelGrace
	}
	cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...)
	terminator := &directTerminator{cmd: cmd, grace: grace}
	cmd.Cancel = terminator.terminate
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
	stdout := newDirectOutputBuffer(directOutputBufferLimit)
	stderr := newDirectOutputBuffer(directOutputBufferLimit)
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		stdout.closeWriter(err)
		stderr.closeWriter(err)
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		stdout.closeWriter(err)
		stderr.closeWriter(err)
		return nil, err
	}
	terminator.pipes = []*os.File{stdoutReader, stderrReader}
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
		stdout.closeWriter(err)
		stderr.closeWriter(err)
		return nil, err
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	stdoutDrain := drainDirectOutput(stdoutReader, stdout)
	stderrDrain := drainDirectOutput(stderrReader, stderr)
	return &directRunningCommand{
		cmd:         cmd,
		stdin:       stdin,
		stdout:      stdout,
		stderr:      stderr,
		cancelGrace: grace,
		terminator:  terminator,
		stdoutDrain: stdoutDrain,
		stderrDrain: stderrDrain,
		stderrPipe:  stderrReader,
	}, nil
}

type directRunningCommand struct {
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      *directOutputBuffer
	stderr      *directOutputBuffer
	cancelGrace time.Duration
	terminator  *directTerminator
	stdoutDrain <-chan struct{}
	stderrDrain <-chan struct{}
	stderrPipe  *os.File
	waitOnce    sync.Once
	waitExit    ExitObservation
	waitErr     error
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

func (r *directRunningCommand) Wait(ctx context.Context) (ExitObservation, error) {
	stopWatching := r.watchWaitContext(ctx)
	defer stopWatching()
	r.waitOnce.Do(func() {
		err := r.cmd.Wait()
		if errors.Is(err, exec.ErrWaitDelay) {
			err = nil
		}
		if r.stdoutDrain != nil {
			<-r.stdoutDrain
		}
		if r.stderrDrain != nil {
			if !waitDirectDrain(r.stderrDrain, directStderrDrainGrace) && r.stderrPipe != nil {
				_ = r.stderrPipe.Close()
			}
			<-r.stderrDrain
		}
		r.stdout.closeWriter(nil)
		r.stderr.closeWriter(nil)
		r.waitExit = exitObservationForCmd(r.cmd)
		r.waitErr = err
	})
	return r.waitExit, r.waitErr
}

func (r *directRunningCommand) watchWaitContext(ctx context.Context) func() {
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if r.terminator != nil {
				_ = r.terminator.terminate()
			}
		case <-stop:
		}
	}()
	return func() { close(stop) }
}

func (r *directRunningCommand) Interrupt(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		if r.terminator != nil {
			done <- r.terminator.terminate()
			return
		}
		done <- terminateProcessGroup(r.cmd, r.cancelGrace)
	}()
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

func exitObservationForCmd(cmd *exec.Cmd) ExitObservation {
	observation := ExitObservation{}
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

type directTerminator struct {
	cmd   *exec.Cmd
	grace time.Duration
	pipes []*os.File
	once  sync.Once
	err   error
}

func (t *directTerminator) terminate() error {
	if t == nil {
		return nil
	}
	t.once.Do(func() {
		t.err = terminateProcessGroup(t.cmd, t.grace)
		for _, pipe := range t.pipes {
			_ = pipe.Close()
		}
	})
	return t.err
}

func drainDirectOutput(src *os.File, dst *directOutputBuffer) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(dst, src)
		_ = src.Close()
		dst.closeWriter(nil)
		close(done)
	}()
	return done
}

func waitDirectDrain(done <-chan struct{}, timeout time.Duration) bool {
	if done == nil {
		return true
	}
	if timeout <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

type directOutputBuffer struct {
	mu           sync.Mutex
	cond         *sync.Cond
	buf          []byte
	limit        int
	writerClosed bool
	readerClosed bool
	err          error
}

func newDirectOutputBuffer(limit int) *directOutputBuffer {
	b := &directOutputBuffer{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *directOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.writerClosed || b.readerClosed || b.limit <= 0 {
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	if overflow := len(b.buf) - b.limit; overflow > 0 {
		copy(b.buf, b.buf[overflow:])
		b.buf = b.buf[:b.limit]
	}
	b.cond.Broadcast()
	return len(p), nil
}

func (b *directOutputBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.writerClosed && !b.readerClosed {
		b.cond.Wait()
	}
	if b.readerClosed {
		return 0, io.ErrClosedPipe
	}
	if len(b.buf) > 0 {
		n := copy(p, b.buf)
		copy(b.buf, b.buf[n:])
		b.buf = b.buf[:len(b.buf)-n]
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	return 0, io.EOF
}

func (b *directOutputBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readerClosed = true
	b.buf = nil
	b.cond.Broadcast()
	return nil
}

func (b *directOutputBuffer) closeWriter(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.writerClosed {
		return
	}
	b.writerClosed = true
	b.err = err
	b.cond.Broadcast()
}

func setProcessGroup(cmd *exec.Cmd) {
	if runtime.GOOS == "windows" {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

var terminateProcessGroup = terminateProcessGroupImpl

func terminateProcessGroupImpl(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return nil
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		pgid = pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	waitForProcessGroupExit(pgid, grace)
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return nil
}

func waitForProcessGroupExit(pgid int, grace time.Duration) {
	if grace <= 0 {
		return
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); err != nil {
			if err == syscall.ESRCH {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
