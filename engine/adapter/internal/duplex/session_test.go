package duplex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestFixtureDriverHappyPath(t *testing.T) {
	proc := newFakeCommand()
	observed := newFakeObservedCommand(proc, command.FinalObservation{
		Exit: command.ExitObservation{Exited: true, Code: 0},
	}, nil)
	runner := &fakeRunner{running: observed}
	session := newFixtureSession(t, runner, 0, "resume-0")
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		scanner := bufio.NewScanner(proc.stdinR)
		if !scanner.Scan() {
			t.Errorf("prompt frame missing: %v", scanner.Err())
			return
		}
		prompt := decodeLine(t, scanner.Bytes())
		if prompt["type"] != "prompt" || prompt["prompt"] != "hello" || prompt["resumeId"] != "resume-0" {
			t.Errorf("prompt frame = %#v", prompt)
			return
		}
		writePeerJSON(t, proc.stdoutW, map[string]any{"type": "message", "text": "working"})
		writePeerJSON(t, proc.stdoutW, map[string]any{"type": "complete", "text": "done", "resumeId": "resume-1"})
		if scanner.Scan() {
			t.Errorf("unexpected stdin frame after completion: %s", scanner.Text())
			return
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("stdin scan error: %v", err)
			return
		}
		_ = proc.stdoutW.Close()
		_ = proc.stderrW.Close()
		proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}()

	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	<-peerDone

	if len(got) != 2 {
		t.Fatalf("events = %#v, want agent text and result", got)
	}
	if got[0].Type != engine.EventAgentText || got[0].Text != "working" {
		t.Fatalf("first event = %#v", got[0])
	}
	if got[1].Type != engine.EventResultMessage || got[1].Text != "done" {
		t.Fatalf("second event = %#v", got[1])
	}
	if session.ID() != "resume-1" {
		t.Fatalf("session id = %q, want resume-1", session.ID())
	}
	if !proc.stdinClosed.Load() {
		t.Fatal("runtime did not half-close stdin")
	}
	assertFinalObservationCalled(t, observed)
	if strings.Join(runner.spec.Argv, "\x00") != "fixture" {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
}

func TestMalformedStdoutSurfacesDecodeError(t *testing.T) {
	proc := newFakeCommand()
	observed := newFakeObservedCommand(proc, command.FinalObservation{
		Exit: command.ExitObservation{Exited: true, Code: 0},
	}, nil)
	errs := make(chan error, 1)
	runner := &fakeRunner{running: observed}
	session := newSessionWithDriver(t, runner, &decodeCapturingDriver{
		FixtureDriver: FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}},
		errs:          errs,
	}, 0, "")
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		if !scanner.Scan() {
			t.Errorf("prompt frame missing: %v", scanner.Err())
			return
		}
		_, _ = io.WriteString(proc.stdoutW, "not-json\n")
		if scanner.Scan() {
			t.Errorf("unexpected stdin frame after malformed stdout: %s", scanner.Text())
			return
		}
		_ = proc.stdoutW.Close()
		_ = proc.stderrW.Close()
		proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}()

	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	var decodeErr *DecodeError
	select {
	case err := <-errs:
		if !errors.As(err, &decodeErr) {
			t.Fatalf("driver error = %T %v, want DecodeError", err, err)
		}
	default:
		t.Fatal("driver did not receive typed decode error")
	}
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "malformed duplex stdout") {
		t.Fatalf("events = %#v, want terminal decode error", got)
	}
}

func TestProcessExitBeforeSemanticTerminalSplitsCleanupAndExecutionAxes(t *testing.T) {
	proc := newFakeCommand()
	cleanupErr := errors.New("cleanup failed after process exit")
	executionErr := errors.New("backend execution failed")
	observed := newFakeObservedCommand(proc, command.FinalObservation{
		Exit:         command.ExitObservation{Exited: true, Code: 7},
		ExecutionErr: executionErr,
		CleanupErr:   cleanupErr,
	}, nil)
	runner := &fakeRunner{running: observed}
	session := newFixtureSession(t, runner, 0, "")
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		if !scanner.Scan() {
			t.Errorf("prompt frame missing: %v", scanner.Err())
			return
		}
		_ = proc.stdoutW.Close()
		_ = proc.stderrW.Close()
		proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}()

	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	var sawCleanupWarning, sawTerminal bool
	for _, ev := range got {
		if ev.Type == engine.EventWarning && strings.Contains(ev.Text, cleanupErr.Error()) {
			sawCleanupWarning = true
		}
		if ev.Type == engine.EventTerminalError && strings.Contains(ev.Text, executionErr.Error()) {
			sawTerminal = true
		}
		if ev.Type == engine.EventTerminalError && strings.Contains(ev.Text, cleanupErr.Error()) {
			t.Fatalf("cleanup error was emitted as terminal: %#v", got)
		}
	}
	if !sawCleanupWarning || !sawTerminal {
		t.Fatalf("events = %#v, want cleanup warning and early-exit terminal", got)
	}
}

func TestNativeInterruptWritesFrameThenFallsBackToProcessInterrupt(t *testing.T) {
	proc := newFakeCommand()
	runner := &fakeRunner{running: proc}
	session := newFixtureSession(t, runner, 10*time.Millisecond, "")
	promptSeen := make(chan struct{})
	interruptSeen := make(chan map[string]any, 1)
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		for scanner.Scan() {
			frame := decodeLine(t, scanner.Bytes())
			switch frame["type"] {
			case "prompt":
				close(promptSeen)
			case "interrupt":
				interruptSeen <- frame
				return
			}
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("stdin scan error: %v", err)
		}
	}()

	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-promptSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for prompt frame")
	}
	if err := session.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-interruptSeen:
		if frame["type"] != "interrupt" {
			t.Fatalf("interrupt frame = %#v", frame)
		}
	default:
		t.Fatal("native interrupt frame was not written")
	}
	if got := proc.interrupts.Load(); got != 1 {
		t.Fatalf("process interrupt count = %d, want 1", got)
	}

	_ = proc.stdoutW.Close()
	_ = proc.stderrW.Close()
	proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
}

func TestWriteJSONSerializesConcurrentFrames(t *testing.T) {
	proc := newFakeCommand()
	conn := &Conn{stdin: proc.Stdin()}
	const n = 64
	lines := make(chan string, n)
	readDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
		readDone <- scanner.Err()
	}()

	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs <- conn.WriteJSON(map[string]any{
				"i":       i,
				"payload": strings.Repeat(fmt.Sprintf("%02d", i), 32),
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	if err := conn.CloseStdin(); err != nil {
		t.Fatal(err)
	}
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("WriteJSON error = %v", err)
		}
	}

	seen := make(map[int]bool, n)
	for line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("corrupt JSON line %q: %v", line, err)
		}
		i, ok := obj["i"].(float64)
		if !ok {
			t.Fatalf("line missing numeric i: %s", line)
		}
		seen[int(i)] = true
	}
	if len(seen) != n {
		t.Fatalf("read %d unique frames, want %d", len(seen), n)
	}
}

type decodeCapturingDriver struct {
	FixtureDriver
	errs chan<- error
}

func (d *decodeCapturingDriver) RunTurn(ctx context.Context, conn *Conn, resumeID string, opts engine.SessionOpts, input engine.TurnInput, emit EmitFunc) (string, error) {
	id, err := d.FixtureDriver.RunTurn(ctx, conn, resumeID, opts, input, emit)
	if err != nil {
		d.errs <- err
	}
	return id, err
}

func newFixtureSession(t *testing.T, runner *fakeRunner, grace time.Duration, resumeID string) *Session {
	t.Helper()
	return newSessionWithDriver(t, runner, FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}}, grace, resumeID)
}

func newSessionWithDriver(t *testing.T, runner *fakeRunner, driver Driver, grace time.Duration, resumeID string) *Session {
	t.Helper()
	session, err := NewSession(SessionConfig{
		Runner:         runner,
		Driver:         driver,
		ResumeID:       resumeID,
		InterruptGrace: grace,
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func collectEventsWithTimeout(t *testing.T, ch <-chan engine.Event, timeout time.Duration) []engine.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var out []engine.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timer.C:
			t.Fatalf("timed out collecting events after %s; collected %#v", timeout, out)
		}
	}
}

func decodeLine(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		t.Fatalf("decode line %q: %v", line, err)
	}
	return obj
}

func writePeerJSON(t *testing.T, w io.Writer, v any) {
	t.Helper()
	payload, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	payload = append(payload, '\n')
	if _, err := w.Write(payload); err != nil {
		t.Fatal(err)
	}
}

func assertFinalObservationCalled(t *testing.T, observed *fakeObservedCommand) {
	t.Helper()
	select {
	case <-observed.finalCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("FinalObservation was not awaited")
	}
}

type fakeRunner struct {
	spec    command.ExecSpec
	running command.RunningCommand
	err     error
}

func (r *fakeRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.spec = spec
	return r.running, r.err
}

type fakeCommand struct {
	stdinR  *io.PipeReader
	stdinW  *trackedPipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	waitCh      chan struct{}
	finishOnce  sync.Once
	exit        command.ExitObservation
	waitErr     error
	interrupts  atomic.Int32
	stdinClosed atomic.Bool
}

func newFakeCommand() *fakeCommand {
	stdinR, stdinPipeW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	cmd := &fakeCommand{
		stdinR:  stdinR,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
		waitCh:  make(chan struct{}),
	}
	cmd.stdinW = &trackedPipeWriter{PipeWriter: stdinPipeW, closed: &cmd.stdinClosed}
	return cmd
}

func (c *fakeCommand) Stdin() io.WriteCloser {
	return c.stdinW
}

func (c *fakeCommand) Stdout() io.ReadCloser {
	return c.stdoutR
}

func (c *fakeCommand) Stderr() io.ReadCloser {
	return c.stderrR
}

func (c *fakeCommand) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-c.waitCh:
		return c.exit, c.waitErr
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (c *fakeCommand) Interrupt(context.Context) error {
	c.interrupts.Add(1)
	return nil
}

func (c *fakeCommand) finish(exit command.ExitObservation, err error) {
	c.finishOnce.Do(func() {
		c.exit = exit
		c.waitErr = err
		close(c.waitCh)
	})
}

type trackedPipeWriter struct {
	*io.PipeWriter
	closed *atomic.Bool
}

func (w *trackedPipeWriter) Close() error {
	w.closed.Store(true)
	return w.PipeWriter.Close()
}

type fakeObservedCommand struct {
	*fakeCommand
	final       command.FinalObservation
	finalErr    error
	finalCalled chan struct{}
	finalOnce   sync.Once
}

func newFakeObservedCommand(cmd *fakeCommand, final command.FinalObservation, finalErr error) *fakeObservedCommand {
	return &fakeObservedCommand{
		fakeCommand: cmd,
		final:       final,
		finalErr:    finalErr,
		finalCalled: make(chan struct{}),
	}
}

func (c *fakeObservedCommand) FinalObservation(context.Context) (command.FinalObservation, error) {
	c.finalOnce.Do(func() {
		close(c.finalCalled)
	})
	return c.final, c.finalErr
}
