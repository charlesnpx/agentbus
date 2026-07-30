package duplex

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

const (
	// DefaultInterruptGrace mirrors the engine cancellation grace without
	// coupling this adapter package to store internals.
	DefaultInterruptGrace = 10 * time.Second
	eventBufferSize       = 16
)

var (
	// ErrBackendExitedBeforeTerminal marks a process retirement before a driver
	// reported semantic turn completion.
	ErrBackendExitedBeforeTerminal = errors.New("backend exited before turn completed")
)

// EmitFunc emits one engine event from a driver.
type EmitFunc func(engine.Event)

// Driver owns provider-specific turn semantics over a live duplex connection.
type Driver interface {
	ExecSpec(resumeID string, opts engine.SessionOpts, input engine.TurnInput) (command.ExecSpec, error)
	RunTurn(ctx context.Context, conn *Conn, resumeID string, opts engine.SessionOpts, input engine.TurnInput, emit EmitFunc) (string, error)
	Interrupt(ctx context.Context, conn *Conn) error
}

// SessionConfig configures a duplex runtime session.
type SessionConfig struct {
	Runner         command.Runner
	Driver         Driver
	Options        engine.SessionOpts
	ResumeID       string
	InterruptGrace time.Duration
}

// Session is one resumable duplex process session.
type Session struct {
	runner         command.Runner
	driver         Driver
	opts           engine.SessionOpts
	interruptGrace time.Duration

	mu     sync.Mutex
	id     string
	active *activeTurn
}

type activeTurn struct {
	running    command.RunningCommand
	conn       *Conn
	retirement *retirement

	interruptOnce sync.Once
	interruptErr  error
}

type retirement struct {
	done        chan struct{}
	observation command.FinalObservation
}

type turnResult struct {
	resumeID string
	err      error
}

// NewSession constructs a duplex runtime session.
func NewSession(config SessionConfig) (*Session, error) {
	if config.Driver == nil {
		return nil, errors.New("duplex driver is required")
	}
	grace := config.InterruptGrace
	if grace == 0 {
		grace = DefaultInterruptGrace
	}
	return &Session{
		runner:         config.Runner,
		driver:         config.Driver,
		opts:           config.Options,
		interruptGrace: grace,
		id:             config.ResumeID,
	}, nil
}

// ID returns the current backend resume id.
func (s *Session) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

// Turn starts one supervised duplex process turn.
func (s *Session) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	runner := s.runner
	if runner == nil {
		runner = command.DirectCommandRunner{}
	}
	return s.TurnWithRunner(ctx, input, runner)
}

// TurnWithRunner starts one supervised duplex process turn using the supplied
// command runner for this turn.
func (s *Session) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	s.mu.Lock()
	if s.active != nil {
		s.mu.Unlock()
		return nil, errors.New("session_busy")
	}

	timeout := input.Timeout
	if timeout == 0 {
		timeout = s.opts.Timeout
	}
	turnCtx := ctx
	turnCancel := func() {}
	if timeout > 0 {
		var cancel context.CancelFunc
		turnCtx, cancel = context.WithTimeout(ctx, timeout)
		turnCancel = cancel
	}
	driverCtx, driverCancel := context.WithCancel(turnCtx)

	resumeID := s.id
	spec, err := s.driver.ExecSpec(resumeID, s.opts, input)
	if err != nil {
		driverCancel()
		turnCancel()
		s.mu.Unlock()
		return nil, err
	}

	var stderr bytes.Buffer
	var stderrLog *engine.CappedLogWriter
	stderrWriter := io.Writer(&stderr)
	if input.LogPaths.Stderr != "" {
		stderrLog, err = engine.NewCappedLogWriter(input.LogPaths.Stderr, 0)
		if err != nil {
			driverCancel()
			turnCancel()
			s.mu.Unlock()
			return nil, err
		}
		stderrWriter = io.MultiWriter(&stderr, stderrLog)
	}
	var stdoutLog *engine.CappedLogWriter
	if input.LogPaths.Stdout != "" {
		stdoutLog, err = engine.NewCappedLogWriter(input.LogPaths.Stdout, 0)
		if err != nil {
			if stderrLog != nil {
				_ = stderrLog.Close()
			}
			driverCancel()
			turnCancel()
			s.mu.Unlock()
			return nil, err
		}
	}

	running, err := runner.Start(turnCtx, spec)
	if err != nil {
		if stdoutLog != nil {
			_ = stdoutLog.Close()
		}
		if stderrLog != nil {
			_ = stderrLog.Close()
		}
		driverCancel()
		turnCancel()
		s.mu.Unlock()
		return nil, err
	}
	var stdoutWriter io.Writer
	if stdoutLog != nil {
		stdoutWriter = stdoutLog
	}
	retirement := startRetirement(turnCtx, running)
	conn := newConn(running.Stdin(), running.Stdout(), stdoutWriter, running, retirement)
	active := &activeTurn{running: running, conn: conn, retirement: retirement}
	s.active = active
	s.mu.Unlock()

	// Notify AFTER the turn is registered active and the mutex is released, so a
	// callback that re-enters ID()/Turn()/Interrupt() cannot deadlock on s.mu.
	if input.OnProcessStart != nil {
		if reporter, ok := running.(interface {
			ProcessRef() (engine.ProcessRef, int)
		}); ok {
			ref, backendChildPID := reporter.ProcessRef()
			input.OnProcessStart(ref, backendChildPID)
		}
	}

	events := make(chan engine.Event, eventBufferSize)
	go s.runTurn(driverCtx, driverCancel, turnCtx, turnCancel, input, resumeID, active, running.Stderr(), stderrWriter, &stderr, stdoutLog, stderrLog, events)
	return events, nil
}

func (s *Session) runTurn(driverCtx context.Context, driverCancel context.CancelFunc, turnCtx context.Context, turnCancel context.CancelFunc, input engine.TurnInput, resumeID string, active *activeTurn, stderrPipe io.ReadCloser, stderrWriter io.Writer, stderr *bytes.Buffer, stdoutLog *engine.CappedLogWriter, stderrLog *engine.CappedLogWriter, events chan<- engine.Event) {
	defer close(events)
	defer turnCancel()
	defer driverCancel()
	defer s.clearActive(active)
	var drainDone chan struct{}
	defer func() {
		if drainDone == nil {
			active.conn.drainReader()
		} else {
			<-drainDone
		}
		if stdoutLog != nil {
			_ = stdoutLog.Close()
		}
		if stderrLog != nil {
			_ = stderrLog.Close()
		}
	}()

	stderrDone := make(chan error, 1)
	go func() {
		stderrDone <- copyAndClose(stderrWriter, stderrPipe)
	}()

	driverDone := make(chan turnResult, 1)
	emit := turnEmitter(events)
	go func() {
		id, err := s.driver.RunTurn(driverCtx, active.conn, resumeID, s.opts, input, emit)
		driverDone <- turnResult{resumeID: id, err: err}
	}()

	result, earlyExit := waitForTurnResult(driverDone, active.retirement)
	result, earlyExit = classifyTurnResult(result, earlyExit)

	_ = active.conn.CloseStdin()
	drainDone = make(chan struct{})
	go func() {
		active.conn.drainReader()
		close(drainDone)
	}()
	observation, _ := active.retirement.wait(context.Background())
	stderrCopyErr := <-stderrDone

	if result.err == nil && result.resumeID != "" {
		s.setID(result.resumeID)
	}
	emitCompletionEvents(turnCtx, events, result.err, earlyExit, observation, stderr, stderrCopyErr)
}

func waitForTurnResult(driverDone <-chan turnResult, retirement *retirement) (turnResult, bool) {
	select {
	case result := <-driverDone:
		return result, false
	case <-retirement.done:
		select {
		case result := <-driverDone:
			return result, false
		default:
		}
		result := <-driverDone
		return result, true
	}
}

func classifyTurnResult(result turnResult, earlyExit bool) (turnResult, bool) {
	if errors.Is(result.err, ErrBackendExitedBeforeTerminal) {
		earlyExit = true
		result.err = nil
	} else if result.err == nil {
		earlyExit = false
	} else if earlyExit && errors.Is(result.err, context.Canceled) {
		result.err = nil
	}
	return result, earlyExit
}

// Interrupt asks the driver to send its native interrupt frame, waits a bounded
// grace for process retirement, and falls back to the process interrupt path.
func (s *Session) Interrupt(ctx context.Context) error {
	s.mu.Lock()
	active := s.active
	driver := s.driver
	grace := s.interruptGrace
	s.mu.Unlock()
	if active == nil {
		return nil
	}
	if grace == 0 {
		grace = DefaultInterruptGrace
	}

	active.interruptOnce.Do(func() {
		active.interruptErr = interruptActiveTurn(ctx, driver, active, grace)
	})
	return active.interruptErr
}

// NativeInterrupt asks the driver to send only its provider-native interrupt
// frame and then waits for process retirement or ctx cancellation. It never
// invokes the process interrupt fallback path.
func (s *Session) NativeInterrupt(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	active := s.active
	driver := s.driver
	s.mu.Unlock()
	if active == nil {
		return nil
	}
	return nativeInterruptActiveTurn(ctx, driver, active)
}

func interruptActiveTurn(ctx context.Context, driver Driver, active *activeTurn, grace time.Duration) error {
	nativeDone := make(chan error, 1)
	go func() {
		nativeDone <- driver.Interrupt(ctx, active.conn)
	}()

	timer := time.NewTimer(grace)
	defer timer.Stop()

	var nativeErr error
	for {
		select {
		case err := <-nativeDone:
			nativeErr = err
			nativeDone = nil
		case <-active.retirement.done:
			nativeErr = collectNativeInterruptErr(nativeDone, nativeErr)
			return nativeErr
		case <-timer.C:
			interruptErr := interruptWithFreshContext(ctx, active.running, grace)
			return errors.Join(collectNativeInterruptErr(nativeDone, nativeErr), interruptErr)
		case <-ctx.Done():
			interruptErr := interruptWithFreshContext(ctx, active.running, grace)
			return errors.Join(collectNativeInterruptErr(nativeDone, nativeErr), interruptErr, ctx.Err())
		}
	}
}

func nativeInterruptActiveTurn(ctx context.Context, driver Driver, active *activeTurn) error {
	nativeDone := make(chan error, 1)
	go func() {
		nativeDone <- driver.Interrupt(ctx, active.conn)
	}()

	for {
		select {
		case err := <-nativeDone:
			if err != nil {
				return err
			}
			nativeDone = nil
		case <-active.retirement.done:
			if err := collectNativeInterruptErr(nativeDone, nil); err != nil {
				return err
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func interruptWithFreshContext(ctx context.Context, running command.RunningCommand, grace time.Duration) error {
	fallbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancel()
	return running.Interrupt(fallbackCtx)
}

func collectNativeInterruptErr(nativeDone <-chan error, nativeErr error) error {
	if nativeDone == nil {
		return nativeErr
	}
	select {
	case err := <-nativeDone:
		return err
	default:
		return nativeErr
	}
}

func startRetirement(ctx context.Context, running command.RunningCommand) *retirement {
	retirement := &retirement{done: make(chan struct{})}
	go func() {
		retirement.observation = finalObservation(ctx, running)
		close(retirement.done)
	}()
	return retirement
}

func (r *retirement) wait(ctx context.Context) (command.FinalObservation, error) {
	if ctx == nil {
		<-r.done
		return r.observation, nil
	}
	select {
	case <-r.done:
		return r.observation, nil
	case <-ctx.Done():
		return command.FinalObservation{}, ctx.Err()
	}
}

func finalObservation(ctx context.Context, running command.RunningCommand) command.FinalObservation {
	exit, waitErr := running.Wait(ctx)
	if observer, ok := running.(command.FinalObserver); ok {
		observation, err := observer.FinalObservation(context.WithoutCancel(ctx))
		if observation.Exit == (command.ExitObservation{}) {
			observation.Exit = exit
		}
		if observation.ExecutionErr == nil && observation.CleanupErr == nil {
			observation.ExecutionErr = errors.Join(waitErr, err)
		}
		return observation
	}
	return command.FinalObservation{Exit: exit, ExecutionErr: waitErr}
}

func (s *Session) setID(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.id = id
}

func (s *Session) clearActive(active *activeTurn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == active {
		s.active = nil
	}
}

func emitCompletionEvents(ctx context.Context, events chan<- engine.Event, driverErr error, earlyExit bool, observation command.FinalObservation, stderr *bytes.Buffer, stderrCopyErr error) {
	if ctx.Err() == context.DeadlineExceeded {
		events <- warning("backend turn timed out")
		return
	}
	if driverErr != nil {
		events <- terminalError(driverErr.Error())
	}
	if observation.CleanupErr != nil {
		events <- warning(observation.CleanupErr.Error())
	}
	if observation.ExecutionErr != nil || earlyExit {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" && stderrCopyErr != nil {
			msg = stderrCopyErr.Error()
		}
		if msg == "" && observation.ExecutionErr != nil {
			msg = observation.ExecutionErr.Error()
		}
		if msg == "" {
			msg = ErrBackendExitedBeforeTerminal.Error()
		}
		events <- terminalError(msg)
		return
	}
	if stderrCopyErr != nil {
		events <- terminalError(stderrCopyErr.Error())
	}
}

func turnEmitter(events chan<- engine.Event) EmitFunc {
	var lastAgentMessage string
	return func(ev engine.Event) {
		if ev.Type == engine.EventAgentText && ev.Text != "" {
			lastAgentMessage = ev.Text
		}
		if ev.Type == engine.EventResultMessage && ev.Text == "" {
			ev.Text = lastAgentMessage
		}
		events <- capEvent(ev)
	}
}

func copyAndClose(dst io.Writer, src io.ReadCloser) error {
	defer func() { _ = src.Close() }()
	_, err := io.Copy(dst, src)
	return err
}

func capEvent(ev engine.Event) engine.Event {
	ev.RawText = ev.Text
	text := engine.TruncateEventText([]byte(ev.Text), engine.DefaultEventTextCap)
	ev.Text = text.Text
	ev.Truncated = ev.Truncated || text.Truncated
	ev.Metadata = engine.SanitizeEventMetadata(ev.Metadata)
	return ev
}

func warning(text string) engine.Event {
	return engine.Event{Type: engine.EventWarning, Text: text}
}

func terminalError(text string) engine.Event {
	return engine.Event{Type: engine.EventTerminalError, Text: text}
}
