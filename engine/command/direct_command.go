package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

// DirectCommandRunner starts and supervises subprocesses with owned pipes.
type DirectCommandRunner struct {
	CancelGrace time.Duration
}

// ErrOutputDrainExpired means a descendant held an output pipe open beyond
// the single post-leader-exit drain deadline.
var ErrOutputDrainExpired = errors.New("incomplete backend transport: output drain deadline expired")

func (r DirectCommandRunner) Start(ctx context.Context, spec ExecSpec) (RunningCommand, error) {
	if ctx == nil {
		panic("nil Context")
	}
	if len(spec.Argv) == 0 || strings.TrimSpace(spec.Argv[0]) == "" {
		return nil, errors.New("command argv is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	grace := r.CancelGrace
	if grace == 0 {
		grace = engine.DefaultCancelGrace
	}
	// DirectCommandRunner owns cancellation so every termination attempt reaches
	// the identity-fenced terminator.
	cmd := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	terminator := &directTerminator{cmd: cmd, grace: grace}
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
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdoutReader.Close()
		_ = stdoutWriter.Close()
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
		return nil, err
	}
	terminator.captureProcessRef()
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	stdout := newOwnedOutputReader(stdoutReader)
	stderr := newOwnedOutputReader(stderrReader)
	running := &directRunningCommand{
		cmd:              cmd,
		stdin:            stdin,
		stdout:           stdout,
		stderr:           stderr,
		cancelGrace:      grace,
		terminator:       terminator,
		startContextStop: make(chan struct{}),
		startContextDone: make(chan struct{}),
		cleanupDone:      make(chan struct{}),
		drainExpired:     make(chan struct{}),
	}
	running.watchStartContext(ctx)
	running.startWait()
	return running, nil
}

type directRunningCommand struct {
	cmd              *exec.Cmd
	stdin            io.WriteCloser
	stdout           *ownedOutputReader
	stderr           *ownedOutputReader
	cancelGrace      time.Duration
	terminator       *directTerminator
	startContextStop chan struct{}
	startContextDone chan struct{}
	cleanupOnce      sync.Once
	cleanupDone      chan struct{}
	drainExpired     chan struct{}
	cleanupErr       error
	waitOnce         sync.Once
	waitDone         chan struct{}
	waitExit         ExitObservation
	waitErr          error
}

// OutputDrainExpired closes when the post-leader-exit reader deadline expires.
func (r *directRunningCommand) OutputDrainExpired() <-chan struct{} {
	return r.drainExpired
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
	r.startWait()
	if r.cleanupDone == nil {
		<-r.waitDone
		return r.waitExit, r.waitErr
	}
	select {
	case <-r.waitDone:
		return r.waitExit, r.waitErr
	default:
	}
	select {
	case <-r.waitDone:
		return r.waitExit, r.waitErr
	case <-r.cleanupDone:
		return ExitObservation{}, r.cleanupErr
	}
}

// FinalObservation keeps execution and cleanup outcomes independent. A failed
// termination attempt is reported as cleanup uncertainty immediately, while
// the single waiter continues to reap the command if it exits later.
func (r *directRunningCommand) FinalObservation(ctx context.Context) (FinalObservation, error) {
	r.startWait()
	if cleanupErr, reported := r.cleanupResult(); reported {
		return r.finalObservationWithCleanup(cleanupErr), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.waitDone:
		observation := FinalObservation{
			Exit:         r.waitExit,
			ExecutionErr: r.waitErr,
		}
		if cleanupErr, reported := r.cleanupResult(); reported {
			observation.CleanupErr = cleanupErr
		}
		return observation, nil
	case <-r.cleanupDone:
		return r.finalObservationWithCleanup(r.cleanupErr), nil
	case <-ctx.Done():
		return FinalObservation{}, ctx.Err()
	}
}

func (r *directRunningCommand) cleanupResult() (error, bool) {
	if r.cleanupDone == nil {
		return nil, false
	}
	select {
	case <-r.cleanupDone:
		return r.cleanupErr, true
	default:
		return nil, false
	}
}

func (r *directRunningCommand) finalObservationWithCleanup(cleanupErr error) FinalObservation {
	observation := FinalObservation{CleanupErr: cleanupErr}
	select {
	case <-r.waitDone:
		observation.Exit = r.waitExit
		observation.ExecutionErr = r.waitErr
	default:
	}
	return observation
}

func (r *directRunningCommand) startWait() {
	r.waitOnce.Do(func() {
		if r.waitDone == nil {
			r.waitDone = make(chan struct{})
		}
		go func() {
			r.waitForExit()
			close(r.waitDone)
		}()
	})
}

func (r *directRunningCommand) waitForExit() {
	err := r.cmd.Wait()
	if r.terminator != nil {
		r.terminator.markLeaderReaped()
	}
	// The deadline starts at leader exit, independently of any decoder. Each
	// reader may finish naturally while buffered pipe data is consumed.
	if r.stdout != nil && r.stderr != nil {
		deadline := time.Now().Add(r.cancelGrace)
		go expireOutputReaders(deadline, r.stdout, r.stderr, r.drainExpired)
	}
	r.stopStartContext()
	r.waitExit = exitObservationForCmd(r.cmd)
	r.waitErr = err
}

func (r *directRunningCommand) watchStartContext(ctx context.Context) {
	if r.startContextDone == nil {
		return
	}
	if ctx == nil || ctx.Done() == nil {
		close(r.startContextDone)
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = r.terminate()
		case <-r.startContextStop:
		}
		close(r.startContextDone)
	}()
}

func (r *directRunningCommand) terminate() error {
	var err error
	if r.terminator != nil {
		err = r.terminator.terminate()
	} else {
		err = terminateProcessGroup(r.cmd, processRefForCmd(r.cmd), r.cancelGrace)
	}
	r.publishCleanupResult(err)
	return err
}

func (r *directRunningCommand) publishCleanupResult(err error) {
	if err == nil {
		return
	}
	r.cleanupOnce.Do(func() {
		r.cleanupErr = fmt.Errorf("exec: canceling Cmd: %w", err)
		if r.cleanupDone != nil {
			close(r.cleanupDone)
		}
		// A cleanup failure must not trigger another signal attempt, but it
		// still needs one waiter to reap the command if it exits later.
		r.startWait()
	})
}

func (r *directRunningCommand) stopStartContext() {
	if r.startContextStop == nil || r.startContextDone == nil {
		return
	}
	close(r.startContextStop)
	<-r.startContextDone
}

func (r *directRunningCommand) watchWaitContext(ctx context.Context) func() {
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = r.terminate()
		case <-stop:
		}
	}()
	return func() { close(stop) }
}

func (r *directRunningCommand) Interrupt(ctx context.Context) error {
	done := make(chan error, 1)
	go func() {
		done <- r.terminate()
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
	cmd          *exec.Cmd
	grace        time.Duration
	pipes        []*os.File
	once         sync.Once
	pipesOnce    sync.Once
	processRef   engine.ProcessRef
	leaderReaped atomic.Bool
	err          error
}

func (t *directTerminator) captureProcessRef() {
	if t == nil {
		return
	}
	t.processRef = processRefForCmd(t.cmd)
}

func (t *directTerminator) capturedProcessRef() engine.ProcessRef {
	if t == nil {
		return engine.ProcessRef{}
	}
	return t.processRef
}

// markLeaderReaped records the sole cmd.Wait completion, so a later termination
// attempt does not signal an orphaned process group whose identity cannot be
// fenced.
func (t *directTerminator) markLeaderReaped() {
	if t != nil {
		t.leaderReaped.Store(true)
	}
}

func (t *directTerminator) terminate() error {
	if t == nil {
		return nil
	}
	t.once.Do(func() {
		// cmd.Wait has established that this direct execution is over. Do not
		// attempt a group signal after its leader has been reaped.
		if t.leaderReaped.Load() {
			t.closePipes()
			return
		}
		t.err = terminateProcessGroup(t.cmd, t.processRef, t.grace)
		if t.err == nil {
			t.closePipes()
		}
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

// ownedOutputReader keeps one consumer and makes forced closure observable.
// It does not buffer: the operating system pipe supplies backpressure.
type ownedOutputReader struct {
	file     *os.File
	mu       sync.Mutex
	cause    error
	finished bool
	closed   bool
	done     chan struct{}
}

func newOwnedOutputReader(file *os.File) *ownedOutputReader {
	return &ownedOutputReader{file: file, done: make(chan struct{})}
}

func (r *ownedOutputReader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	if err != nil {
		r.mu.Lock()
		if r.cause != nil {
			err = r.cause
		}
		r.finishLocked()
		r.mu.Unlock()
	}
	return n, err
}

func (r *ownedOutputReader) finishLocked() {
	if !r.finished {
		r.finished = true
		close(r.done)
	}
}

func (r *ownedOutputReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.finishLocked()
	return r.file.Close()
}

func (r *ownedOutputReader) expire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return false
	}
	r.cause = ErrOutputDrainExpired
	r.closed = true
	r.finishLocked()
	_ = r.file.Close()
	return true
}

func expireOutputReaders(deadline time.Time, stdout, stderr *ownedOutputReader, expired chan<- struct{}) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	stdoutDone, stderrDone := stdout.done, stderr.done
	for stdoutDone != nil || stderrDone != nil {
		select {
		case <-stdoutDone:
			stdoutDone = nil
		case <-stderrDone:
			stderrDone = nil
		case <-timer.C:
			stdoutExpired := stdout.expire()
			stderrExpired := stderr.expire()
			if stdoutExpired || stderrExpired {
				close(expired)
			}
			return
		}
	}
}
