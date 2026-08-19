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

var ErrOutputTruncated = errors.New("command output truncated")

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
	terminator := &directTerminator{cmd: cmd, grace: grace, processRefReady: make(chan struct{})}
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
		terminator.markProcessRefReady()
		_ = stdin.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
		_ = stderrReader.Close()
		_ = stderrWriter.Close()
		stdout.closeWriter(err)
		stderr.closeWriter(err)
		return nil, err
	}
	terminator.captureProcessRef()
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
		stdoutPipe:  stdoutReader,
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
	stdoutPipe  *os.File
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
			// Stdout is the live-leader stream boundary. Once the leader has
			// exited, descendants retaining stdout get only the runner cancel
			// grace before the local read end is closed.
			if !waitDirectDrain(r.stdoutDrain, r.cancelGrace) && r.stdoutPipe != nil {
				r.closeOutputPipes()
			}
			<-r.stdoutDrain
		}
		if r.stderrDrain != nil {
			if !waitDirectDrain(r.stderrDrain, directStderrDrainGrace) && r.stderrPipe != nil {
				r.closeOutputPipes()
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

func (r *directRunningCommand) closeOutputPipes() {
	if r.terminator != nil {
		r.terminator.closePipes()
		return
	}
	if r.stdoutPipe != nil {
		_ = r.stdoutPipe.Close()
	}
	if r.stderrPipe != nil {
		_ = r.stderrPipe.Close()
	}
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
	if r.terminator != nil {
		ref := r.terminator.capturedProcessRef()
		return ref, ref.PID
	}
	ref := processRefForCmd(r.cmd)
	return ref, ref.PID
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
	cmd                 *exec.Cmd
	grace               time.Duration
	pipes               []*os.File
	once                sync.Once
	pipesOnce           sync.Once
	processRefMu        sync.RWMutex
	processRef          engine.ProcessRef
	processRefReady     chan struct{}
	processRefReadyOnce sync.Once
	err                 error
}

// capturedDirectProcessRefs passes the identity captured immediately after
// Start to the platform terminator while retaining terminateProcessGroup as a
// test seam for the command package.
var capturedDirectProcessRefs sync.Map // map[*exec.Cmd]engine.ProcessRef

func (t *directTerminator) captureProcessRef() {
	if t == nil {
		return
	}
	ref := processRefForCmd(t.cmd)
	t.processRefMu.Lock()
	t.processRef = ref
	t.processRefMu.Unlock()
	t.markProcessRefReady()
}

func (t *directTerminator) capturedProcessRef() engine.ProcessRef {
	if t == nil {
		return engine.ProcessRef{}
	}
	t.processRefMu.RLock()
	defer t.processRefMu.RUnlock()
	return t.processRef
}

func (t *directTerminator) markProcessRefReady() {
	if t == nil || t.processRefReady == nil {
		return
	}
	t.processRefReadyOnce.Do(func() { close(t.processRefReady) })
}

func (t *directTerminator) waitForProcessRef() {
	if t == nil || t.processRefReady == nil {
		return
	}
	<-t.processRefReady
}

func (t *directTerminator) terminate() error {
	if t == nil {
		return nil
	}
	t.waitForProcessRef()
	t.once.Do(func() {
		if t.cmd != nil {
			capturedDirectProcessRefs.Store(t.cmd, t.capturedProcessRef())
			defer capturedDirectProcessRefs.Delete(t.cmd)
		}
		t.err = terminateProcessGroup(t.cmd, t.grace)
		t.closePipes()
	})
	return t.err
}

func (t *directTerminator) closePipes() {
	if t == nil {
		return
	}
	t.pipesOnce.Do(func() {
		for _, pipe := range t.pipes {
			_ = pipe.Close()
		}
	})
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
	truncated    bool
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
		b.truncated = true
	}
	b.cond.Broadcast()
	return len(p), nil
}

func (b *directOutputBuffer) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.writerClosed && !b.readerClosed {
		if b.truncated {
			return 0, ErrOutputTruncated
		}
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
	if b.truncated {
		return 0, ErrOutputTruncated
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
