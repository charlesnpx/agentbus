package duplex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
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
	runner          command.Runner
	driver          Driver
	opts            engine.SessionOpts
	envOverlay      []string
	writeEnvOverlay []string
	interruptGrace  time.Duration

	mu sync.Mutex
	// id is the provider-confirmed resume ID supplied to the next turn. It is
	// retained after errored or interrupted turns, so that turn resumes the same
	// conversation. Callers needing a fresh conversation must discard this
	// Session and Start a new one.
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
	envOverlay, err := validatedEnvironmentOverlay(config.Options.EnvOverlay)
	if err != nil {
		return nil, err
	}
	writeEnvOverlay, err := validatedEnvironmentOverlay(config.Options.WriteEnvOverlay)
	if err != nil {
		return nil, err
	}

	opts := config.Options
	// The shared runtime owns the validated overlays and applies them after
	// every driver ExecSpec. Drivers do not need the caller-owned maps.
	opts.EnvOverlay = nil
	opts.WriteEnvOverlay = nil
	grace := config.InterruptGrace
	if grace == 0 {
		grace = DefaultInterruptGrace
	}
	return &Session{
		runner:          config.Runner,
		driver:          config.Driver,
		opts:            opts,
		envOverlay:      envOverlay,
		writeEnvOverlay: writeEnvOverlay,
		interruptGrace:  grace,
		id:              config.ResumeID,
	}, nil
}

func validatedEnvironmentOverlay(overlay map[string]string) ([]string, error) {
	envOverlay := make([]string, 0, len(overlay))
	for key, value := range overlay {
		switch {
		case key == "":
			return nil, errors.New("environment overlay key is required")
		case strings.ContainsRune(key, '\x00'):
			return nil, errors.New("environment overlay key must not contain NUL")
		case strings.Contains(key, "="):
			return nil, errors.New("environment overlay key must not contain equals")
		case strings.ContainsRune(value, '\x00'):
			return nil, errors.New("environment overlay value must not contain NUL")
		}
		envOverlay = append(envOverlay, key+"="+value)
	}
	// Map input has no insertion order, so make Windows key collision handling
	// deterministic before the shared environment normalization below.
	sort.Strings(envOverlay)
	return normalizeEnvironment(runtime.GOOS, envOverlay), nil
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

	// Contract: driver ExecSpec.Env is an ADDITIVE layer over the full inherited
	// process environment, not a replacement or isolation mechanism (no current
	// driver sets it). The session overlay is applied last and wins. The
	// write-only overlay is deliberately absent from read-only turns, including
	// a read-only retry after a write attempt.
	if input.Write {
		spec.Env = normalizeEnvironment(runtime.GOOS, os.Environ(), spec.Env, s.envOverlay, s.writeEnvOverlay)
	} else {
		spec.Env = normalizeEnvironment(runtime.GOOS, os.Environ(), spec.Env, s.envOverlay)
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

// normalizeEnvironment applies layers in order, retaining the last value for
// each key. Windows environment variable names are case-insensitive.
func normalizeEnvironment(goos string, layers ...[]string) []string {
	count := 0
	for _, layer := range layers {
		count += len(layer)
	}
	env := make([]string, 0, count)
	index := make(map[string]int, count)
	for _, layer := range layers {
		for _, entry := range layer {
			key, _, hasValue := strings.Cut(entry, "=")
			// Windows per-drive working-directory variables begin with "=";
			// their key ends at the following "=".
			if strings.HasPrefix(entry, "=") {
				if driveKey, _, hasDriveValue := strings.Cut(entry[1:], "="); hasDriveValue {
					key = "=" + driveKey
				}
			}
			if !hasValue {
				env = append(env, entry)
				continue
			}
			if goos == "windows" {
				key = strings.ToUpper(key)
			}
			if existing, ok := index[key]; ok {
				env[existing] = entry
				continue
			}
			index[key] = len(env)
			env = append(env, entry)
		}
	}
	return env
}

func (s *Session) runTurn(driverCtx context.Context, driverCancel context.CancelFunc, turnCtx context.Context, turnCancel context.CancelFunc, input engine.TurnInput, resumeID string, active *activeTurn, stderrPipe io.ReadCloser, stderrWriter io.Writer, stderr *bytes.Buffer, stdoutLog *engine.CappedLogWriter, stderrLog *engine.CappedLogWriter, events chan<- engine.Event) {
	defer close(events)
	defer turnCancel()
	defer driverCancel()
	defer s.clearActive(active)
	var drainDone chan struct{}

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
	if errors.Is(result.err, ErrFrameTooLarge) && !errors.Is(result.err, engine.ErrTransportFrameTooLarge) {
		result.err = fmt.Errorf("%w: %w", engine.ErrTransportFrameTooLarge, result.err)
	}
	_ = active.conn.CloseStdin()
	drainDone = make(chan struct{})
	go func() {
		active.conn.drainReader()
		close(drainDone)
	}()
	observation, _ := active.retirement.wait(context.Background())
	stderrCopyErr := <-stderrDone

	if result.resumeID != "" {
		s.setID(result.resumeID)
	}
	// Capture settled state before teardown releases the session for another turn.
	// A later turn may install a new resume ID, and teardown itself can outlive a
	// caller's deadline.
	completedSessionID := s.ID()
	disposition := captureTurnDisposition(turnCtx)
	if drainDone == nil {
		active.conn.drainReader()
	} else {
		<-drainDone
	}
	frameDrops := active.conn.FrameDrops()
	if !frameDrops.Empty() {
		events <- warningWithMetadata(frameDropWarning(frameDrops), frameDrops.EventMetadata())
	}
	emitCompletionEvents(disposition, events, result.err, earlyExit, observation, stderr, stderrCopyErr)
	if stdoutLog != nil {
		_ = stdoutLog.Close()
	}
	if stderrLog != nil {
		_ = stderrLog.Close()
	}

	// Clear active before channel closure, preserving the existing guarantee that
	// a caller that observes closure can begin its next turn. The deferred call
	// remains as the panic-safe fallback.
	s.clearActive(active)
	events <- turnFinalEvent(completedSessionID, disposition, observation)
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
// frame and then waits for process retirement or ctx cancellation. It returns
// settled=true only when the process retired during the native attempt. It
// never invokes the process interrupt fallback path.
func (s *Session) NativeInterrupt(ctx context.Context) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	active := s.active
	driver := s.driver
	s.mu.Unlock()
	if active == nil {
		// No live turn. s.active is set under s.mu before the process work
		// begins (Turn) and cleared only after retirement completes
		// (clearActive), so a nil active means either nothing launched yet or
		// the turn already retired — in both cases there is no process to
		// natively interrupt. Report settled: forcing a raw containment
		// fallback here would, in the post-completion window, re-touch an
		// already-final launch and can surface its cached execution error.
		return true, nil
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

func nativeInterruptActiveTurn(ctx context.Context, driver Driver, active *activeTurn) (bool, error) {
	nativeDone := make(chan error, 1)
	go func() {
		nativeDone <- driver.Interrupt(ctx, active.conn)
	}()

	var nativeErr error
	for {
		select {
		case <-active.retirement.done:
			// The process retired: the turn is settled regardless of whether
			// the native interrupt write itself errored. A write that loses the
			// race with process exit yields a closed-pipe error, which must not
			// mask the fact that the process is gone.
			return true, nil
		case err := <-nativeDone:
			nativeDone = nil
			// A native write error alone does not mean "not settled" — the
			// process may simply be exiting. Retain it as a warning and keep
			// awaiting retirement (bounded by ctx), mirroring interruptActiveTurn.
			if err != nil {
				nativeErr = err
			}
		case <-ctx.Done():
			return false, errors.Join(nativeErr, ctx.Err())
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

type turnDisposition struct {
	timedOut bool
	canceled bool
}

func captureTurnDisposition(ctx context.Context) turnDisposition {
	if ctx == nil {
		return turnDisposition{}
	}
	err := ctx.Err()
	return turnDisposition{
		timedOut: errors.Is(err, context.DeadlineExceeded),
		canceled: errors.Is(err, context.Canceled),
	}
}

func emitCompletionEvents(disposition turnDisposition, events chan<- engine.Event, driverErr error, earlyExit bool, observation command.FinalObservation, stderr *bytes.Buffer, stderrCopyErr error) {
	if disposition.timedOut {
		events <- warning("backend turn timed out")
		return
	}
	if driverErr != nil {
		events <- terminalError(driverErr)
	}
	if observation.CleanupErr != nil {
		events <- warning(observation.CleanupErr.Error())
	}
	if observation.ExecutionErr != nil || (earlyExit && driverErr == nil) {
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
		events <- terminalError(errors.New(msg))
		return
	}
	if stderrCopyErr != nil {
		events <- terminalError(stderrCopyErr)
	}
}

func turnFinalEvent(backendSessionID string, disposition turnDisposition, observation command.FinalObservation) engine.Event {
	return engine.Event{
		Type: engine.EventTurnFinal,
		TurnFinal: &engine.TurnFinalObservation{
			BackendSessionID: backendSessionID,
			ReturnCodeKnown:  observation.Exit.Exited,
			ReturnCode:       observation.Exit.Code,
			Signal:           observation.Exit.Signal,
			TimedOut:         disposition.timedOut,
			Canceled:         disposition.canceled,
			ExecutionFailed:  hasExecutionFailure(observation.ExecutionErr),
			CleanupFailed:    observation.CleanupErr != nil,
		},
	}
}

func hasExecutionFailure(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, wrapped := range joined.Unwrap() {
			if hasExecutionFailure(wrapped) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return hasExecutionFailure(wrapped.Unwrap())
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
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

func warningWithMetadata(text string, metadata map[string]any) engine.Event {
	return engine.Event{Type: engine.EventWarning, Text: text, Metadata: metadata}
}

func frameDropWarning(drops engine.TransportFrameDrops) string {
	return fmt.Sprintf("discarded %d backend transport frame(s), totaling %d bytes", drops.Count, drops.Bytes)
}

func terminalError(err error) engine.Event {
	if err == nil {
		err = errors.New("backend failed")
	}
	return engine.Event{Type: engine.EventTerminalError, Text: err.Error(), Err: err}
}
