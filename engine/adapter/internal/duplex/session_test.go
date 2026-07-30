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

func TestTurnWithRunnerUsesInjectedRunnerWithoutDefault(t *testing.T) {
	proc := newFakeCommand()
	runner := &fakeRunner{running: proc}
	session, err := NewSession(SessionConfig{
		Driver:   FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}},
		ResumeID: "resume-0",
	})
	if err != nil {
		t.Fatal(err)
	}
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

	events, err := session.TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	<-peerDone

	if len(got) != 1 || got[0].Type != engine.EventResultMessage || got[0].Text != "done" {
		t.Fatalf("events = %#v, want result", got)
	}
	if session.ID() != "resume-1" {
		t.Fatalf("session id = %q, want resume-1", session.ID())
	}
	if strings.Join(runner.spec.Argv, "\x00") != "fixture" {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
}

func TestTurnWithRunnerRequiresRunner(t *testing.T) {
	session, err := NewSession(SessionConfig{
		Driver: FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = session.TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, nil)
	if err == nil || !strings.Contains(err.Error(), "command runner is required") {
		t.Fatalf("TurnWithRunner error = %v, want command runner required", err)
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

func TestBufferedTerminalFrameSurvivesImmediateRetirement(t *testing.T) {
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
			proc := newFakeCommand()
			runner := &fakeRunner{running: proc}
			session := newSessionWithDriver(t, runner, &delayedFrameConsumerDriver{
				FixtureDriver: FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}},
				readDelay:     25 * time.Millisecond,
			}, 0, "resume-0")
			peerDone := make(chan struct{})
			go func() {
				defer close(peerDone)
				scanner := bufio.NewScanner(proc.stdinR)
				if !scanner.Scan() {
					t.Errorf("prompt frame missing: %v", scanner.Err())
					return
				}
				writePeerJSON(t, proc.stdoutW, map[string]any{"type": "message", "text": "working"})
				writePeerJSON(t, proc.stdoutW, map[string]any{"type": "complete", "text": "done", "resumeId": "resume-1"})
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

			var sawAgent, sawResult bool
			for _, ev := range got {
				if ev.Type == engine.EventTerminalError {
					t.Fatalf("events = %#v, did not want terminal error", got)
				}
				if ev.Type == engine.EventAgentText && ev.Text == "working" {
					sawAgent = true
				}
				if ev.Type == engine.EventResultMessage && ev.Text == "done" {
					sawResult = true
				}
			}
			if !sawAgent || !sawResult {
				t.Fatalf("events = %#v, want agent text and result", got)
			}
			if session.ID() != "resume-1" {
				t.Fatalf("session id = %q, want resume-1", session.ID())
			}
		})
	}
}

func TestTrailingFramesAfterTerminalDoNotWedgeSession(t *testing.T) {
	proc := newFakeCommand()
	runner := &fakeRunner{running: proc}
	session := newFixtureSession(t, runner, 0, "")
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		scanner := bufio.NewScanner(proc.stdinR)
		if !scanner.Scan() {
			t.Errorf("prompt frame missing: %v", scanner.Err())
			return
		}
		writePeerJSON(t, proc.stdoutW, map[string]any{"type": "complete", "text": "done", "resumeId": "resume-1"})
		for i := 0; i < eventBufferSize+8; i++ {
			writePeerJSON(t, proc.stdoutW, map[string]any{"type": "message", "text": fmt.Sprintf("trailing-%d", i)})
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
	if len(got) != 1 || got[0].Type != engine.EventResultMessage || got[0].Text != "done" {
		t.Fatalf("events = %#v, want result only", got)
	}

	proc2 := newFakeCommand()
	runner.running = proc2
	go func() {
		scanner := bufio.NewScanner(proc2.stdinR)
		if !scanner.Scan() {
			t.Errorf("second prompt frame missing: %v", scanner.Err())
			return
		}
		writePeerJSON(t, proc2.stdoutW, map[string]any{"type": "complete", "text": "again", "resumeId": "resume-2"})
		_ = proc2.stdoutW.Close()
		_ = proc2.stderrW.Close()
		proc2.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}()
	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "again"})
	if err != nil {
		t.Fatalf("second turn error = %v, want no session_busy", err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
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

func TestNativeInterruptDoesNotCallProcessInterrupt(t *testing.T) {
	t.Run("returns on retirement", func(t *testing.T) {
		proc := newFakeCommand()
		runner := &fakeRunner{running: proc}
		session := newFixtureSession(t, runner, 0, "")
		promptSeen, interruptSeen, scanDone := startInterruptFrameScanner(t, proc)

		events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		waitForSignal(t, promptSeen, "prompt frame")

		nativeDone := make(chan error, 1)
		go func() {
			nativeDone <- session.NativeInterrupt(context.Background())
		}()
		frame := waitForInterruptFrame(t, interruptSeen)
		if frame["type"] != "interrupt" {
			t.Fatalf("interrupt frame = %#v", frame)
		}
		if got := proc.interrupts.Load(); got != 0 {
			t.Fatalf("process interrupt count = %d, want 0", got)
		}
		select {
		case err := <-nativeDone:
			t.Fatalf("NativeInterrupt returned before retirement: %v", err)
		default:
		}

		_ = proc.stdoutW.Close()
		_ = proc.stderrW.Close()
		proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
		select {
		case err := <-nativeDone:
			if err != nil {
				t.Fatalf("NativeInterrupt error = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for NativeInterrupt to return after retirement")
		}
		_ = collectEventsWithTimeout(t, events, 2*time.Second)
		waitForSignal(t, scanDone, "stdin scanner shutdown")
		if got := proc.interrupts.Load(); got != 0 {
			t.Fatalf("process interrupt count after retirement = %d, want 0", got)
		}
	})

	t.Run("returns on context cancellation", func(t *testing.T) {
		proc := newFakeCommand()
		runner := &fakeRunner{running: proc}
		session := newFixtureSession(t, runner, 0, "")
		promptSeen, interruptSeen, scanDone := startInterruptFrameScanner(t, proc)

		events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		waitForSignal(t, promptSeen, "prompt frame")

		ctx, cancel := context.WithCancel(context.Background())
		nativeDone := make(chan error, 1)
		go func() {
			nativeDone <- session.NativeInterrupt(ctx)
		}()
		frame := waitForInterruptFrame(t, interruptSeen)
		if frame["type"] != "interrupt" {
			t.Fatalf("interrupt frame = %#v", frame)
		}
		if got := proc.interrupts.Load(); got != 0 {
			t.Fatalf("process interrupt count = %d, want 0", got)
		}
		cancel()
		select {
		case err := <-nativeDone:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("NativeInterrupt error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for NativeInterrupt to return after context cancellation")
		}

		_ = proc.stdoutW.Close()
		_ = proc.stderrW.Close()
		proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
		_ = collectEventsWithTimeout(t, events, 2*time.Second)
		waitForSignal(t, scanDone, "stdin scanner shutdown")
		if got := proc.interrupts.Load(); got != 0 {
			t.Fatalf("process interrupt count after context cancellation = %d, want 0", got)
		}
	})
}

func TestInterruptFallsBackWhenNativeInterruptBlocks(t *testing.T) {
	proc := newFakeCommand()
	var closeOnce sync.Once
	proc.onInterrupt = func() error {
		var err error
		closeOnce.Do(func() {
			err = proc.stdinR.CloseWithError(errors.New("process interrupted"))
		})
		return err
	}
	runner := &fakeRunner{running: proc}
	session := newFixtureSession(t, runner, 20*time.Millisecond, "")
	promptSeen := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		if scanner.Scan() {
			close(promptSeen)
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

	start := time.Now()
	interruptDone := make(chan error, 1)
	go func() {
		interruptDone <- session.Interrupt(context.Background())
	}()
	select {
	case <-interruptDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("interrupt did not return within bounded time")
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("interrupt took %s, want bounded by fallback grace", elapsed)
	}
	if got := proc.interrupts.Load(); got != 1 {
		t.Fatalf("process interrupt count = %d, want 1", got)
	}

	_ = proc.stdoutW.Close()
	_ = proc.stderrW.Close()
	proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
}

func TestInterruptWithExpiredContextUsesFreshFallbackContext(t *testing.T) {
	proc := newFakeCommand()
	fallbackEntered := make(chan struct{})
	proc.onInterruptCtx = func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("fallback interrupt context expired: %w", err)
		}
		close(fallbackEntered)
		return nil
	}
	runner := &fakeRunner{running: proc}
	session := newFixtureSession(t, runner, 50*time.Millisecond, "")
	promptSeen := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		if scanner.Scan() {
			close(promptSeen)
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

	expiredCtx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	err = session.Interrupt(expiredCtx)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("interrupt error = %v, want original deadline", err)
	}
	select {
	case <-fallbackEntered:
	default:
		t.Fatalf("fallback interrupt did not enter with a non-expired context; err = %v", err)
	}
	if got := proc.interrupts.Load(); got != 1 {
		t.Fatalf("process interrupt count = %d, want 1", got)
	}

	_ = proc.stdoutW.Close()
	_ = proc.stderrW.Close()
	proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
}

func TestConcurrentInterruptsShareSingleSequence(t *testing.T) {
	proc := newFakeCommand()
	var closeOnce sync.Once
	proc.onInterrupt = func() error {
		var err error
		closeOnce.Do(func() {
			err = proc.stdinR.CloseWithError(errors.New("process interrupted"))
		})
		return err
	}
	runner := &fakeRunner{running: proc}
	driver := &countingInterruptDriver{
		FixtureDriver: FixtureDriver{Spec: command.ExecSpec{Argv: []string{"fixture"}}},
	}
	session := newSessionWithDriver(t, runner, driver, 20*time.Millisecond, "")
	promptSeen := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(proc.stdinR)
		if scanner.Scan() {
			close(promptSeen)
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

	startInterrupts := make(chan struct{})
	interruptDone := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-startInterrupts
			interruptDone <- session.Interrupt(context.Background())
		}()
	}
	close(startInterrupts)
	errs := make([]error, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case err := <-interruptDone:
			errs = append(errs, err)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("concurrent interrupt did not return within bounded time")
		}
	}
	if (errs[0] == nil) != (errs[1] == nil) {
		t.Fatalf("interrupt errors = %v and %v, want shared result", errs[0], errs[1])
	}
	if errs[0] != nil && errs[0].Error() != errs[1].Error() {
		t.Fatalf("interrupt errors = %v and %v, want shared result", errs[0], errs[1])
	}
	if got := driver.nativeInterrupts.Load(); got != 1 {
		t.Fatalf("native interrupt count = %d, want 1", got)
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

type delayedFrameConsumerDriver struct {
	FixtureDriver
	readDelay time.Duration
}

func (d *delayedFrameConsumerDriver) RunTurn(ctx context.Context, conn *Conn, resumeID string, _ engine.SessionOpts, input engine.TurnInput, emit EmitFunc) (string, error) {
	prompt := map[string]any{
		"type":   "prompt",
		"prompt": input.Prompt,
		"write":  input.Write,
	}
	if resumeID != "" {
		prompt["resumeId"] = resumeID
	}
	if err := conn.WriteJSON(prompt); err != nil {
		return "", err
	}
	if d.readDelay > 0 {
		select {
		case <-time.After(d.readDelay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	for {
		select {
		case frame, ok := <-conn.Frames():
			if !ok {
				if err := pendingDecodeError(conn); err != nil {
					return "", err
				}
				return "", ErrBackendExitedBeforeTerminal
			}
			kind, _ := frame.Object["type"].(string)
			switch kind {
			case "message":
				text, _ := frame.Object["text"].(string)
				emit(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: frame.Object})
			case "complete":
				text, _ := frame.Object["text"].(string)
				nextID, _ := frame.Object["resumeId"].(string)
				if nextID == "" {
					nextID = resumeID
				}
				emit(engine.Event{Type: engine.EventResultMessage, Text: text, Metadata: frame.Object})
				return nextID, nil
			}
		case err, ok := <-conn.DecodeErrors():
			if ok && err != nil {
				return "", err
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

type countingInterruptDriver struct {
	FixtureDriver
	nativeInterrupts atomic.Int32
}

func (d *countingInterruptDriver) Interrupt(ctx context.Context, conn *Conn) error {
	d.nativeInterrupts.Add(1)
	return d.FixtureDriver.Interrupt(ctx, conn)
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

func startInterruptFrameScanner(t *testing.T, proc *fakeCommand) (<-chan struct{}, <-chan map[string]any, <-chan struct{}) {
	t.Helper()
	promptSeen := make(chan struct{})
	interruptSeen := make(chan map[string]any, 1)
	scanDone := make(chan struct{})
	var promptOnce sync.Once
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(proc.stdinR)
		for scanner.Scan() {
			frame := decodeLine(t, scanner.Bytes())
			switch frame["type"] {
			case "prompt":
				promptOnce.Do(func() { close(promptSeen) })
			case "interrupt":
				interruptSeen <- frame
			}
		}
		if err := scanner.Err(); err != nil {
			t.Errorf("stdin scan error: %v", err)
		}
	}()
	return promptSeen, interruptSeen, scanDone
}

func waitForSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForInterruptFrame(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case frame := <-ch:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for interrupt frame")
	}
	return nil
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

	waitCh         chan struct{}
	finishOnce     sync.Once
	exit           command.ExitObservation
	waitErr        error
	onInterrupt    func() error
	onInterruptCtx func(context.Context) error
	interrupts     atomic.Int32
	stdinClosed    atomic.Bool
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

func (c *fakeCommand) Interrupt(ctx context.Context) error {
	c.interrupts.Add(1)
	if c.onInterruptCtx != nil {
		return c.onInterruptCtx(ctx)
	}
	if c.onInterrupt != nil {
		return c.onInterrupt()
	}
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
