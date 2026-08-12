package served

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type fakeBackend struct {
	name            string
	delay           time.Duration
	block           <-chan struct{}
	started         chan struct{}
	turns           chan fakeTurn
	count           atomic.Int64
	interrupts      atomic.Int64
	events          func(prompt string, write bool) []engine.Event
	startHook       func(engine.SessionOpts)
	processRef      engine.ProcessRef
	backendChildPID int
	resumes         chan resumedSession
	parkable        bool
	controlled      bool
	startPathDone   chan struct{}
}

type fakeTurn struct {
	Prompt string
	Write  bool
}

type logCaptureBackend struct {
	*fakeBackend
	input              chan engine.TurnInput
	terminal           engine.Event
	logContent         map[string]string
	writeAfterTerminal <-chan struct{}
	writersOpened      chan struct{}
}

func newLogCaptureBackend(name string) *logCaptureBackend {
	return &logCaptureBackend{
		fakeBackend: newFakeBackend(name),
		input:       make(chan engine.TurnInput, 8),
		terminal:    engine.Event{Type: engine.EventTerminalError, Text: "backend exploded"},
	}
}

func (b *logCaptureBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return b, nil
}

func (b *logCaptureBackend) Start(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
	b.count.Add(1)
	return &logCaptureSession{id: b.name + "-session", backend: b}, nil
}

func (b *logCaptureBackend) Resume(_ context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	return &logCaptureSession{id: id, backend: b}, nil
}

type logCaptureSession struct {
	id      string
	backend *logCaptureBackend
}

func (s *logCaptureSession) ID() string { return s.id }

func (s *logCaptureSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	return nil, errors.New("direct turn must not be used")
}

func (s *logCaptureSession) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	running, err := runner.Start(ctx, command.ExecSpec{Argv: []string{"/bin/fake-agent"}})
	if err != nil {
		return nil, err
	}
	s.backend.input <- input
	if stdin := running.Stdin(); stdin != nil {
		_ = stdin.Close()
	}
	if stdout := running.Stdout(); stdout != nil {
		go func() { _, _ = io.Copy(io.Discard, stdout) }()
	}
	if stderr := running.Stderr(); stderr != nil {
		go func() { _, _ = io.Copy(io.Discard, stderr) }()
	}
	if observer, ok := running.(command.FinalObserver); ok {
		go func() { _, _ = observer.FinalObservation(context.Background()) }()
	}
	content := s.backend.logContent
	if content == nil {
		content = map[string]string{
			input.LogPaths.Stdout: "backend stdout\n",
			input.LogPaths.Stderr: "backend stderr\n",
		}
	}
	if s.backend.writeAfterTerminal != nil {
		type delayedLogWriter struct {
			writer *engine.CappedLogWriter
			text   string
		}
		writers := make([]delayedLogWriter, 0, len(content))
		for path, text := range content {
			writer, err := engine.NewCappedLogWriter(path, 0)
			if err != nil {
				for _, opened := range writers {
					_ = opened.writer.Close()
				}
				return nil, err
			}
			writers = append(writers, delayedLogWriter{writer: writer, text: text})
		}
		if s.backend.writersOpened != nil {
			close(s.backend.writersOpened)
		}
		events := make(chan engine.Event, 1)
		events <- s.backend.terminal
		go func() {
			<-s.backend.writeAfterTerminal
			for _, writer := range writers {
				_, _ = io.WriteString(writer.writer, writer.text)
				_ = writer.writer.Close()
			}
			close(events)
		}()
		return events, nil
	}
	for path, text := range content {
		writer, err := engine.NewCappedLogWriter(path, 0)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(writer, text); err != nil {
			_ = writer.Close()
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
	}
	events := make(chan engine.Event, 1)
	events <- s.backend.terminal
	close(events)
	return events, nil
}

func (*logCaptureSession) Interrupt(context.Context) error { return nil }

type resumedSession struct {
	ID   string
	Opts engine.SessionOpts
}

type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppedClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type cancelOnAdvanceAnchor struct {
	authority.Anchor
	cancel context.CancelFunc
	once   sync.Once
}

func (a *cancelOnAdvanceAnchor) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	a.once.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
	})
	return a.Anchor.Advance(ctx, boot, generation)
}

type causeErrContext struct {
	context.Context
}

func (ctx causeErrContext) Err() error {
	return context.Cause(ctx.Context)
}

type probeableFakeBackend struct {
	*fakeBackend
	probes atomic.Int64
}

func (b *probeableFakeBackend) ProbeBackend(ctx context.Context, runner command.ProbeRunner) (engine.Backend, error) {
	b.probes.Add(1)
	if runner == nil {
		return nil, errors.New("probe runner is required")
	}
	path, err := runner.LookPath(b.Name())
	if err != nil {
		return nil, err
	}
	if _, err := runner.Run(ctx, command.ProbeSpec{Argv: []string{path, "--version"}}); err != nil {
		return nil, err
	}
	return b, nil
}

type recordingProbeRunner struct {
	lookups atomic.Int64
	runs    atomic.Int64
}

func (r *recordingProbeRunner) LookPath(file string) (string, error) {
	r.lookups.Add(1)
	if strings.TrimSpace(file) == "" {
		return "", errors.New("missing probe path")
	}
	return "/probe/" + file, nil
}

func (r *recordingProbeRunner) Run(_ context.Context, spec command.ProbeSpec) (command.ProbeResult, error) {
	r.runs.Add(1)
	if len(spec.Argv) != 2 || spec.Argv[1] != "--version" {
		return command.ProbeResult{}, fmt.Errorf("probe argv = %#v, want --version", spec.Argv)
	}
	return command.ProbeResult{Stdout: []byte("fake 1.0.0\n")}, nil
}

func newFakeBackend(name string) *fakeBackend {
	return &fakeBackend{
		name:       name,
		turns:      make(chan fakeTurn, 32),
		parkable:   true,
		controlled: true,
		events: func(prompt string, write bool) []engine.Event {
			return []engine.Event{{Type: engine.EventAgentText, Text: "PASS\n\n## Findings\nNone.\n"}}
		},
	}
}

func (b *fakeBackend) Name() string { return b.name }

func (b *fakeBackend) AdmissionParkable() bool { return b.parkable }

func (b *fakeBackend) AdmissionControlledRunner() bool { return b.controlled }

func (b *fakeBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return b, nil
}

func (b *fakeBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: b.name}, nil
}

func (b *fakeBackend) signalStartPathDone() {
	if b.startPathDone == nil {
		return
	}
	select {
	case b.startPathDone <- struct{}{}:
	default:
	}
}

func (b *fakeBackend) Start(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
	n := b.count.Add(1)
	if b.startHook != nil {
		b.startHook(opts)
	}
	return &fakeSession{id: b.name + "-session-" + stringID(n), backend: b}, nil
}

func (b *fakeBackend) Resume(_ context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	if b.resumes != nil {
		b.resumes <- resumedSession{ID: id, Opts: opts}
	}
	return &fakeSession{id: id, backend: b}, nil
}

type admissionLivenessBackend struct {
	*fakeBackend
	eventStream    chan engine.Event
	runnerObserved chan struct{}
}

func newAdmissionLivenessBackend(name string) *admissionLivenessBackend {
	backend := &admissionLivenessBackend{
		fakeBackend:    newFakeBackend(name),
		eventStream:    make(chan engine.Event),
		runnerObserved: make(chan struct{}),
	}
	backend.started = make(chan struct{}, 1)
	return backend
}

func (b *admissionLivenessBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return b, nil
}

func (b *admissionLivenessBackend) Start(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
	n := b.count.Add(1)
	if b.startHook != nil {
		b.startHook(opts)
	}
	return &admissionLivenessSession{id: b.name + "-session-" + stringID(n), backend: b}, nil
}

func (b *admissionLivenessBackend) Resume(_ context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	if b.resumes != nil {
		b.resumes <- resumedSession{ID: id, Opts: opts}
	}
	return &admissionLivenessSession{id: id, backend: b}, nil
}

type admissionLivenessSession struct {
	id      string
	backend *admissionLivenessBackend
}

func (s *admissionLivenessSession) ID() string { return s.id }

func (s *admissionLivenessSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	return s.backend.eventStream, nil
}

func (s *admissionLivenessSession) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	running, err := runner.Start(ctx, command.ExecSpec{Argv: []string{"/bin/fake-agent"}})
	if err != nil {
		return nil, err
	}
	s.backend.turns <- fakeTurn{Prompt: input.Prompt, Write: input.Write}
	if stdin := running.Stdin(); stdin != nil {
		_ = stdin.Close()
	}
	if stdout := running.Stdout(); stdout != nil {
		go func() { _, _ = io.Copy(io.Discard, stdout) }()
	}
	if stderr := running.Stderr(); stderr != nil {
		go func() { _, _ = io.Copy(io.Discard, stderr) }()
	}
	go func() {
		_ = testFinalObservation(ctx, running)
		close(s.backend.runnerObserved)
	}()
	if s.backend.started != nil {
		s.backend.started <- struct{}{}
	}
	return s.backend.eventStream, nil
}

func (s *admissionLivenessSession) Interrupt(context.Context) error {
	s.backend.interrupts.Add(1)
	return nil
}

type fakeSession struct {
	id      string
	backend *fakeBackend
}

func (s *fakeSession) ID() string { return s.id }

func (s *fakeSession) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	ch := make(chan engine.Event, 8)
	s.backend.turns <- fakeTurn{Prompt: input.Prompt, Write: input.Write}
	if input.OnProcessStart != nil && (s.backend.processRef.PID > 0 || s.backend.processRef.PGID > 0 || s.backend.processRef.StartTime != "") {
		input.OnProcessStart(s.backend.processRef, s.backend.backendChildPID)
	}
	if s.backend.started != nil {
		s.backend.started <- struct{}{}
	}
	go func() {
		defer close(ch)
		if s.backend.block != nil {
			select {
			case <-ctx.Done():
				return
			case <-s.backend.block:
			}
		}
		if s.backend.delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.backend.delay):
			}
		}
		for _, event := range s.backend.events(input.Prompt, input.Write) {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

func (s *fakeSession) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	running, err := runner.Start(ctx, command.ExecSpec{Argv: []string{"/bin/fake-agent"}})
	if err != nil {
		s.backend.signalStartPathDone()
		return nil, err
	}
	ch := make(chan engine.Event, 8)
	s.backend.turns <- fakeTurn{Prompt: input.Prompt, Write: input.Write}
	if input.OnProcessStart != nil {
		if reporter, ok := running.(interface {
			ProcessRef() (engine.ProcessRef, int)
		}); ok {
			ref, backendChildPID := reporter.ProcessRef()
			input.OnProcessStart(ref, backendChildPID)
		}
	}
	if s.backend.started != nil {
		s.backend.started <- struct{}{}
	}
	s.backend.signalStartPathDone()
	go func() {
		defer close(ch)
		if stdin := running.Stdin(); stdin != nil {
			_, _ = io.WriteString(stdin, input.Prompt)
			_ = stdin.Close()
		}
		stderrDone := make(chan struct{})
		go func() {
			if stderr := running.Stderr(); stderr != nil {
				_, _ = io.Copy(io.Discard, stderr)
			}
			close(stderrDone)
		}()
		if stdout := running.Stdout(); stdout != nil {
			_, _ = io.Copy(io.Discard, stdout)
		}
		observation := testFinalObservation(ctx, running)
		if observation.CleanupErr != nil {
			ch <- engine.Event{Type: engine.EventWarning, Text: observation.CleanupErr.Error()}
		}
		if observation.ExecutionErr != nil {
			ch <- engine.Event{Type: engine.EventTerminalError, Text: observation.ExecutionErr.Error()}
			<-stderrDone
			return
		}
		<-stderrDone
		if s.backend.block != nil {
			select {
			case <-ctx.Done():
				return
			case <-s.backend.block:
			}
		}
		if s.backend.delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.backend.delay):
			}
		}
		for _, event := range s.backend.events(input.Prompt, input.Write) {
			select {
			case <-ctx.Done():
				return
			case ch <- event:
			}
		}
	}()
	return ch, nil
}

type nonOrdinalSession struct {
	id    string
	turns atomic.Int64
}

func (s *nonOrdinalSession) ID() string { return s.id }

func (s *nonOrdinalSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	s.turns.Add(1)
	ch := make(chan engine.Event)
	close(ch)
	return ch, nil
}

func (s *nonOrdinalSession) Interrupt(context.Context) error { return nil }

func (s *fakeSession) Interrupt(context.Context) error {
	s.backend.interrupts.Add(1)
	return nil
}

func testFinalObservation(ctx context.Context, running command.RunningCommand) command.FinalObservation {
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

type unsafeNamedBackend struct {
	name   string
	starts atomic.Int64
}

func (b *unsafeNamedBackend) Name() string { return b.name }

func (b *unsafeNamedBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: b.name}, nil
}

func (b *unsafeNamedBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	b.starts.Add(1)
	return unsafeNamedSession{id: b.name + "-session"}, nil
}

func (b *unsafeNamedBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return unsafeNamedSession{id: b.name + "-resumed"}, nil
}

type unsafeNamedSession struct {
	id string
}

func (s unsafeNamedSession) ID() string { return s.id }

func (s unsafeNamedSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	ch := make(chan engine.Event)
	close(ch)
	return ch, nil
}

func (s unsafeNamedSession) Interrupt(context.Context) error { return nil }

type controlledSession struct {
	id         string
	events     chan engine.Event
	started    chan struct{}
	interrupts atomic.Int64
}

func (s *controlledSession) ID() string { return s.id }

func (s *controlledSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	if s.started != nil {
		s.started <- struct{}{}
	}
	return s.events, nil
}

func (s *controlledSession) Interrupt(context.Context) error {
	s.interrupts.Add(1)
	return nil
}

type nativeSettledSession struct {
	id          string
	settled     bool
	err         error
	nativeCalls atomic.Int64
}

func (s *nativeSettledSession) ID() string { return s.id }

func (s *nativeSettledSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	events := make(chan engine.Event)
	close(events)
	return events, nil
}

func (s *nativeSettledSession) Interrupt(context.Context) error { return nil }

func (s *nativeSettledSession) NativeInterrupt(ctx context.Context) (bool, error) {
	s.nativeCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.settled, s.err
}

func TestHelloTokenCapabilitiesAndSocketPermissions(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	h := startTestServer(t, backend, Config{IdleTimeout: -1})

	info, err := os.Stat(h.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket mode = %o, want 600", got)
	}
	tokenInfo, err := os.Stat(filepath.Join(h.root, protocol.TokenFileName))
	if err != nil {
		t.Fatal(err)
	}
	if got := tokenInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("token mode = %o, want 600", got)
	}

	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	resp := rpc(t, conn, r, "1", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: h.token})
	if resp.Error != nil {
		t.Fatalf("hello error = %+v", resp.Error)
	}
	var hello protocol.HelloResult
	decodeResult(t, resp, &hello)
	if hello.ProtocolVersion != protocol.Version || len(hello.Backends) != 1 || hello.Backends[0] != "fake" {
		t.Fatalf("hello = %+v", hello)
	}
	if len(hello.BackendMetadata) != 1 || hello.BackendMetadata[0].Backend != "fake" || hello.BackendMetadata[0].Models == nil || hello.BackendMetadata[0].Efforts == nil {
		t.Fatalf("hello backend metadata = %+v", hello.BackendMetadata)
	}
	for _, capability := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude", "models.discovery", "models.reported"} {
		if _, ok := hello.Capabilities[capability]; !ok {
			t.Fatalf("missing capability %s in %+v", capability, hello.Capabilities)
		}
	}
	if hello.Capabilities["policy.shape"] {
		t.Fatalf("policy.shape capability is enabled in hello: %+v", hello.Capabilities)
	}
	if _, ok := hello.Capabilities["jobs.requestId"]; ok {
		t.Fatalf("jobs.requestId capability is advertised in hello: %+v", hello.Capabilities)
	}
	if !hello.Capabilities[protocol.CapabilityAdmissionStrictContainment] {
		t.Fatalf("strict containment capability absent in default strict hello: %+v", hello.Capabilities)
	}
	resp = rpc(t, conn, r, "dup", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: h.token})
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)

	bad := dialRaw(t, h.socketPath)
	defer bad.Close()
	badReader := bufio.NewReader(bad)
	resp = rpc(t, bad, badReader, "1", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: "wrong"})
	assertRPCCode(t, resp, protocol.ErrorUnauthorized)

	mismatch := dialRaw(t, h.socketPath)
	defer mismatch.Close()
	mismatchReader := bufio.NewReader(mismatch)
	resp = rpc(t, mismatch, mismatchReader, "1", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version + 1, Token: h.token})
	assertRPCCode(t, resp, protocol.ErrorVersionMismatch)
	if resp.Error.Data.Code != "protocol_version_mismatch" {
		t.Fatalf("version mismatch data code = %q", resp.Error.Data.Code)
	}
	if resp.Error.Data.ServerProtocolVersion != protocol.Version {
		t.Fatalf("server protocol version = %d", resp.Error.Data.ServerProtocolVersion)
	}
}

func TestListenUnixSocketPrivateChmodsBeforeListen(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	var beforeListenIdentity socketFileIdentity
	beforeListenCalled := false

	server.unixSocketPrivateListenHooks = unixSocketPrivateListenHooks{
		beforeListen: func(identity socketFileIdentity) error {
			beforeListenCalled = true
			beforeListenIdentity = identity
			info, err := os.Stat(server.socketPath)
			if err != nil {
				return err
			}
			if got := info.Mode().Perm(); got != 0o600 {
				return fmt.Errorf("socket mode before listen = %o, want 600", got)
			}
			gotIdentity, err := statSocketFileIdentity(server.socketPath)
			if err != nil {
				return err
			}
			if gotIdentity != identity {
				return fmt.Errorf("socket identity before listen = %+v, want %+v", gotIdentity, identity)
			}
			if conn, err := net.DialTimeout("unix", server.socketPath, 20*time.Millisecond); err == nil {
				_ = conn.Close()
				return errors.New("socket accepted a connection before listen")
			}
			return nil
		},
	}

	ln, identity, err := server.listen()
	if err != nil {
		if servedTestBindDeniedOutput(err.Error()) {
			servedTestSkipOrFailBindDenied(t, "server listen", err)
		}
		t.Fatal(err)
	}
	defer func() {
		_ = ln.Close()
		server.removeOwnedSocket(identity, "test cleanup")
	}()
	if !beforeListenCalled {
		t.Fatal("before-listen hook was not called")
	}
	if identity != beforeListenIdentity {
		t.Fatalf("listen identity = %+v, want before-listen identity %+v", identity, beforeListenIdentity)
	}
	info, err := os.Stat(server.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket mode after listen helper = %o, want 600", got)
	}
	afterListenIdentity, err := statSocketFileIdentity(server.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	if afterListenIdentity != beforeListenIdentity {
		t.Fatalf("socket identity after listen = %+v, want %+v", afterListenIdentity, beforeListenIdentity)
	}
}

func TestListenUnixSocketPrivateDetectsPreListenPathReplacement(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	var replacement net.Listener
	hookCalled := false
	server.unixSocketPrivateListenHooks = unixSocketPrivateListenHooks{
		beforeListen: func(identity socketFileIdentity) error {
			hookCalled = true
			gotIdentity, err := statSocketFileIdentity(server.socketPath)
			if err != nil {
				return err
			}
			if gotIdentity != identity {
				return fmt.Errorf("socket identity before replacement = %+v, want %+v", gotIdentity, identity)
			}
			if err := os.Remove(server.socketPath); err != nil {
				return err
			}
			ln, err := net.Listen("unix", server.socketPath)
			if err != nil {
				return err
			}
			replacement = ln
			return nil
		},
	}
	defer func() {
		if replacement != nil {
			_ = replacement.Close()
		}
	}()

	ln, _, err := server.listen()
	if err == nil {
		_ = ln.Close()
		t.Fatal("listen succeeded after pre-listen socket replacement")
	}
	if servedTestBindDeniedOutput(err.Error()) {
		servedTestSkipOrFailBindDenied(t, "server listen replacement", err)
	}
	if !hookCalled {
		t.Fatal("before-listen hook was not called")
	}
	if !errors.Is(err, ErrDaemonAlreadyListening) {
		t.Fatalf("listen error = %v, want ErrDaemonAlreadyListening", err)
	}
	var alreadyListening DaemonAlreadyListeningError
	if !errors.As(err, &alreadyListening) {
		t.Fatalf("listen error = %T, want DaemonAlreadyListeningError", err)
	}
	if alreadyListening.SocketPath != server.socketPath {
		t.Fatalf("already-listening socket path = %q, want %q", alreadyListening.SocketPath, server.socketPath)
	}
	if replacement == nil {
		t.Fatal("replacement listener was not created")
	}
	conn, dialErr := net.DialTimeout("unix", server.socketPath, time.Second)
	if dialErr != nil {
		t.Fatalf("replacement socket is not dialable after failed listen: %v", dialErr)
	}
	_ = conn.Close()
}

func TestHelloAdvertisesStrictContainmentOnlyWhenPolicyServesStrict(t *testing.T) {
	t.Parallel()
	launcher := newAdmissionFakeLaunchCustodian(t)
	h := startTestServerWithHooks(t, newFakeBackend("fake"), Config{IdleTimeout: -1}, func(server *Server) {
		configureTestAdmissionRuntime(t, server, launcher, true)
	})

	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	resp := rpc(t, conn, r, "1", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: h.token})
	if resp.Error != nil {
		t.Fatalf("hello error = %+v", resp.Error)
	}
	var hello protocol.HelloResult
	decodeResult(t, resp, &hello)
	if !hello.Capabilities[protocol.CapabilityAdmissionStrictContainment] {
		t.Fatalf("strict containment capability absent in strict-active hello: %+v", hello.Capabilities)
	}
	if _, ok := hello.Capabilities["jobs.requestId"]; ok {
		t.Fatalf("jobs.requestId capability is advertised in strict-active hello: %+v", hello.Capabilities)
	}
}

func TestServeConstructsAdmissionInstanceBeforeListenWithRequestIDCapabilityDisabled(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	listenErr := errors.New("listener reached after admission bootstrap")
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		if server.jobsRequestIDEnabled {
			return nil, socketFileIdentity{}, errors.New("jobs.requestId was enabled during default admission bootstrap")
		}
		if server.admissionInstance == nil || server.admissionInstance.ready == nil || server.admissionInstance.coordinator == nil || server.admissionInstance.submission == nil {
			return nil, socketFileIdentity{}, errors.New("admission instance was not ready before listen")
		}
		if server.admissionInstance.policy.runtimeAssessment.Class != custodian.SupportAvailable {
			return nil, socketFileIdentity{}, fmt.Errorf("runtime assessment = %s, want available", server.admissionInstance.policy.runtimeAssessment.Class)
		}
		return nil, socketFileIdentity{}, listenErr
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want %v", err, listenErr)
	}
}

func TestBootstrapAdmissionBuildsUnavailableRuntimePolicy(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))

	err := server.bootstrapAdmission(context.Background())
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("bootstrapAdmission error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("bootstrapAdmission error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if server.admissionInstance != nil {
		t.Fatal("admission instance was constructed for unavailable runtime")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, err)
		}
	}
}

func TestAdmissionBootstrapFailureFailsServeClosedBeforeListen(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	bootstrapErr := errors.New("admission bootstrap failed")
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		return nil, nil, nil, bootstrapErr
	}
	listenCalled := false
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		return nil, socketFileIdentity{}, errors.New("listener should not be reached")
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, bootstrapErr) {
		t.Fatalf("Serve error = %v, want %v", err, bootstrapErr)
	}
	if listenCalled {
		t.Fatal("listener was called after admission bootstrap failure")
	}
}

func TestAdmissionDaemonBootIdentityCarriesPerServerEntropy(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	clock := engine.ClockFunc(func() time.Time { return base })
	newServer := func() *Server {
		t.Helper()
		server, err := New(Config{
			StateRoot:    shortTempDir(t),
			CWD:          shortTempDir(t),
			Token:        "test-token",
			Backends:     []engine.Backend{newFakeBackend("fake")},
			ProcessTable: fakeProcessTable{},
			Clock:        clock,
			IdleTimeout:  -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return server
	}

	first := newServer()
	second := newServer()
	firstBoot, err := first.admissionDaemonBoot()
	if err != nil {
		t.Fatal(err)
	}
	_ = first.nextID("job")
	firstBootAgain, err := first.admissionDaemonBoot()
	if err != nil {
		t.Fatal(err)
	}
	if firstBootAgain != firstBoot {
		t.Fatalf("admission daemon boot ref changed within one server: first=%+v second=%+v", firstBoot, firstBootAgain)
	}
	secondBoot, err := second.admissionDaemonBoot()
	if err != nil {
		t.Fatal(err)
	}
	if secondBoot.BootID == firstBoot.BootID {
		t.Fatalf("same fixed-clock server boot ids collided: %q", firstBoot.BootID)
	}
	if secondBoot.OwnerID == firstBoot.OwnerID {
		t.Fatalf("same fixed-clock server owner ids collided: %q", firstBoot.OwnerID)
	}
}

func TestServeClearsAdmissionStateAndSequentialReserveRecovers(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	start := func() (context.CancelFunc, chan error) {
		t.Helper()
		configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- server.Serve(ctx)
			close(done)
		}()
		waitForSocket(t, server.socketPath, done)
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done <-chan error) {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Serve stop error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not stop")
		}
	}

	cancelFirst, doneFirst := start()
	accepted := acceptIdentifiedAuthorityWork(t, server, "sequential-reserve")
	server.admissionStateMu.RLock()
	firstBoot := server.admissionReady.Boot()
	server.admissionStateMu.RUnlock()
	stop(cancelFirst, doneFirst)
	if server.admissionInstance != nil || server.admissionReady != nil || server.admissionSubmission != nil || server.admissionCoordinator != nil || server.admissionRepository != nil || server.admissionClose != nil {
		t.Fatalf("admission state after Serve#1 stop: instance=%p ready=%p submission=%p coordinator=%p repository=%v close=%v",
			server.admissionInstance, server.admissionReady, server.admissionSubmission, server.admissionCoordinator, server.admissionRepository, server.admissionClose)
	}

	cancelSecond, doneSecond := start()
	defer stop(cancelSecond, doneSecond)
	server.admissionStateMu.RLock()
	secondBoot := server.admissionReady.Boot()
	server.admissionStateMu.RUnlock()
	if secondBoot.BootID == firstBoot.BootID {
		t.Fatalf("sequential Serve reused boot id %q", secondBoot.BootID)
	}

	jobID := accepted.Record.JobID.String()
	record := loadAdmissionSafetyRecord(t, server, jobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed || record.Terminal.Cause != model.CauseDaemonRestartedBeforeAuthorization {
		t.Fatalf("sequential recovery terminal = %+v, want failed daemon-restarted-before-authorization", record.Terminal)
	}

	conn := dialRaw(t, server.socketPath)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	helloRaw(t, conn, reader, server.token)
	resp := rpc(t, conn, reader, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-sequential-runtime",
		RequestID:    "request-sequential-runtime",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	})
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("job.submit = %+v, want new strict job", submitted)
	}

	resp = rpc(t, conn, reader, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: jobID})
	var statuses protocol.JobStatusResult
	decodeResult(t, resp, &statuses)
	if len(statuses.Jobs) != 1 || statuses.Jobs[0].JobID != jobID || statuses.Jobs[0].State != engine.StateFailed {
		t.Fatalf("job.status = %+v, want recovered failed authority job %s", statuses, jobID)
	}
}

func TestAdmissionRecoveryExecutorUnboundControlLossFinalizesWithoutBackendStart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	launcher := newAdmissionFakeLaunchCustodian(t)
	_, accepted := newPriorBootAuthorityWork(t, repo, anchorStore, launcher, "recovery-finalize")

	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	if server.jobsRequestIDEnabled {
		t.Fatal("jobs.requestId default = true before recovery bootstrap")
	}
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	if server.jobsRequestIDEnabled {
		t.Fatal("jobs.requestId was enabled by recovery bootstrap")
	}

	image, err := server.admissionReady.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	record := image.Safety.Value
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed || record.Terminal.Cause != model.CauseDaemonRestartedBeforeAuthorization {
		t.Fatalf("terminal = %+v, want failed daemon-restarted-before-authorization", record.Terminal)
	}
	if record.Terminal.Proof != model.ProofNeverPermittedAndRetired {
		t.Fatalf("terminal proof = %s, want %s", record.Terminal.Proof, model.ProofNeverPermittedAndRetired)
	}
	if got := launcher.containCount(); got != 0 {
		t.Fatalf("containments = %d, want none for unlaunched finalization", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("prepared launches = %d, want 0", got)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("releases = %d, want 0", got)
	}
	if got := launcher.abortCount(); got != 0 {
		t.Fatalf("aborts = %d, want 0", got)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestCompleteAdmissionRunLogsRecoveryObligationWhenAuthorityCleared(t *testing.T) {
	var logs bytes.Buffer
	oldLogWriter := log.Writer()
	oldLogFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldLogWriter)
		log.SetFlags(oldLogFlags)
	}()

	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	accepted := acceptIdentifiedAuthorityWork(t, server, "complete-after-close")
	jobID := accepted.Record.JobID.String()
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}

	if err := server.completeAdmissionRun(jobRun{jobID: jobID}, engine.StateCompleted, "done", nil); err != nil {
		t.Fatal(err)
	}
	got := logs.String()
	if !strings.Contains(got, jobID) || !strings.Contains(got, "startup recovery must finalize the durably accepted obligation") {
		t.Fatalf("completion log = %q, want job id and recovery obligation", got)
	}
}

func TestAdmissionSnapshotCorruptionTripsFailStopDuringLaunchPreparation(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		corrupt func(*testing.T, *Server, model.RequestKey, model.JobID)
	}{
		{
			name: "binding",
			kind: "binding",
			corrupt: func(t *testing.T, server *Server, key model.RequestKey, _ model.JobID) {
				t.Helper()
				corruptAdmissionBindingForTest(t, server, key, "binding checksum")
			},
		},
		{
			name: "safety",
			kind: "safety",
			corrupt: func(t *testing.T, server *Server, _ model.RequestKey, jobID model.JobID) {
				t.Helper()
				corruptAdmissionSafetyForTest(t, server, jobID, "safety checksum")
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			server, root, _ := newUnstartedTestServer(t, backend)
			enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
			accepted := acceptIdentifiedAuthorityWork(t, server, "snapshot-launch-"+tt.name)
			tt.corrupt(t, server, accepted.Binding.RequestKey, accepted.Record.JobID)

			run := jobRun{
				jobID:     accepted.Record.JobID.String(),
				sessionID: accepted.Projection.SessionID,
				session:   &fakeSession{id: accepted.Projection.SessionID, backend: backend},
			}
			_, _, ok, err := server.prepareAdmittedJobLaunch(context.Background(), run)
			if ok {
				t.Fatal("prepareAdmittedJobLaunch ok = true, want corruption rejection")
			}
			requireServedCorruptKind(t, err, tt.kind)
			if !errors.Is(err, authority.ErrFailStopped) {
				t.Fatalf("prepareAdmittedJobLaunch error = %v, want durable fail-stop", err)
			}
			assertAdmissionSnapshotFailStopTrip(t, server, root, err)

			next := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-after-snapshot-launch-" + tt.name,
				RequestID:    "request-after-snapshot-launch-" + tt.name,
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: root, Write: false, Prompt: "after"},
			}))
			assertJobHandlerError(t, next, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
		})
	}
}

func TestAdmissionSnapshotCorruptionTripsFailStopDuringCompletion(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		corrupt func(*testing.T, *Server, model.RequestKey, model.JobID)
	}{
		{
			name: "binding",
			kind: "binding",
			corrupt: func(t *testing.T, server *Server, key model.RequestKey, _ model.JobID) {
				t.Helper()
				corruptAdmissionBindingForTest(t, server, key, "binding checksum")
			},
		},
		{
			name: "safety",
			kind: "safety",
			corrupt: func(t *testing.T, server *Server, _ model.RequestKey, jobID model.JobID) {
				t.Helper()
				corruptAdmissionSafetyForTest(t, server, jobID, "safety checksum")
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
			enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
			accepted := acceptIdentifiedAuthorityWork(t, server, "snapshot-complete-"+tt.name)
			tt.corrupt(t, server, accepted.Binding.RequestKey, accepted.Record.JobID)

			err := server.completeAdmissionRun(jobRun{jobID: accepted.Record.JobID.String()}, engine.StateCompleted, "done", nil)
			requireServedCorruptKind(t, err, tt.kind)
			if !errors.Is(err, authority.ErrFailStopped) {
				t.Fatalf("completeAdmissionRun error = %v, want durable fail-stop", err)
			}
			assertAdmissionSnapshotFailStopTrip(t, server, root, err)

			next := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-after-snapshot-complete-" + tt.name,
				RequestID:    "request-after-snapshot-complete-" + tt.name,
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: root, Write: false, Prompt: "after"},
			}))
			assertJobHandlerError(t, next, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
		})
	}
}

func TestServeCloseReturnsWhenIdentifiedSubmitWedgedInStorage(t *testing.T) {
	var logs bytes.Buffer
	oldLogWriter := log.Writer()
	oldLogFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldLogWriter)
		log.SetFlags(oldLogFlags)
	}()

	ctx := context.Background()
	repo := memory.NewRepository()
	blockingRepo := newBlockingUpdateRepository(repo)
	anchorStore := authority.NewAnchorStore()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(blockingRepo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, blockingRepo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	configureTestAdmissionRuntime(t, server, launcher, true)
	cancel, done, _ := startTestServerWithBlockingListener(t, server)

	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-wedged-submit",
		RequestID:    "request-wedged-submit",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	rawParams := mustMarshal(t, params)
	blockingRepo.Arm()
	submitDone := make(chan requestOutcome, 1)
	go func() {
		submitDone <- server.handleJobSubmit(ctx, rawParams)
	}()
	select {
	case <-blockingRepo.started:
	case <-time.After(2 * time.Second):
		t.Fatal("submit did not wedge inside repository update")
	}

	closeStarted := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve close error = %v", err)
		}
		if elapsed := time.Since(closeStarted); elapsed > 2*admissionRepositoryCloseTimeout {
			t.Fatalf("Serve close elapsed = %s, want bounded within %s", elapsed, 2*admissionRepositoryCloseTimeout)
		}
	case <-time.After(2*admissionRepositoryCloseTimeout + time.Second):
		t.Fatal("Serve close did not return while submit was wedged")
	}
	if got := logs.String(); !strings.Contains(got, "submit is wedged") || !strings.Contains(got, "admission repository close skipped after submit serialization timeout") {
		t.Fatalf("shutdown log = %q, want wedged-submit and repository-leak messages", got)
	}

	rejected := server.handleJobSubmit(ctx, mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-after-close",
		RequestID:    "request-after-close",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if rejected.err == nil {
		t.Fatalf("post-close submit result = %+v, want not-ready rejection", rejected.result)
	}
	resp := protocol.Response{Error: rejected.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectAdmissionClosing)
	if !strings.Contains(rejected.err.Message, "shutting down") {
		t.Fatalf("post-close rejection message = %q, want shutting down", rejected.err.Message)
	}

	blockingRepo.Release()
	select {
	case outcome := <-submitDone:
		if outcome.err == nil {
			t.Fatalf("wedged submit result = %+v, want non-success after close", outcome.result)
		}
		if outcome.after != nil {
			t.Fatal("wedged submit returned a launch hook after close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wedged submit did not return after storage release")
	}

	secondBackend := newFakeBackend("fake")
	secondServer, _, _ := newUnstartedTestServer(t, secondBackend)
	secondLauncher := newAdmissionFakeLaunchCustodian(t)
	secondServer.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(blockingRepo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, blockingRepo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	enableTestAdmission(t, secondServer, secondLauncher)
	record := singleAuthoritySafetyRecord(t, secondServer)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed || record.Terminal.Cause != model.CauseDaemonRestartedBeforeAuthorization {
		t.Fatalf("recovered terminal = %+v, want failed daemon-restarted-before-authorization", record.Terminal)
	}
	replay := secondServer.handleJobSubmit(ctx, rawParams)
	if replay.err != nil {
		t.Fatalf("replay submit error = %+v", replay.err)
	}
	replayed, ok := replay.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("replay result type = %T", replay.result)
	}
	if replayed.JobID != record.JobID.String() || !replayed.Deduplicated || replayed.State != engine.StateFailed {
		t.Fatalf("replay result = %+v, want terminal replay for %s", replayed, record.JobID)
	}
	if got := secondBackend.count.Load(); got != 0 {
		t.Fatalf("second backend starts = %d, want 0 for recovery replay", got)
	}
}

func TestIdentifiedSubmitReplayAfterRestartReturnsPreAuthorizationTerminal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	firstBackend := newFakeBackend("fake")
	firstLauncher := newAdmissionFakeLaunchCustodian(t)
	firstServer := newTestServerWithRoot(t, root, cwd, firstBackend, Config{IdleTimeout: -1})
	configureTestAdmissionRuntime(t, firstServer, firstLauncher, true)
	firstCancel, firstDone, _ := startTestServerWithBlockingListener(t, firstServer)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-preauth-crash",
		RequestID:    "request-preauth-crash",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	outcome := firstServer.handleJobSubmit(ctx, mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("initial submit error = %+v", outcome.err)
	}
	submitted, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("initial submit result type = %T", outcome.result)
	}
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("initial submit result = %+v, want accepted non-replay", submitted)
	}
	if outcome.after == nil {
		t.Fatal("initial submit did not return response hook")
	}
	if got := firstBackend.count.Load(); got != 1 {
		t.Fatalf("first backend starts = %d, want construction before durable accept", got)
	}
	select {
	case turn := <-firstBackend.turns:
		t.Fatalf("backend turn = %+v, want none before response hook", turn)
	default:
	}
	firstCancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Serve stop error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Serve did not stop")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("%s stat error = %v, want persistent production state", name, err)
		}
	}

	secondBackend := newFakeBackend("fake")
	secondLauncher := newAdmissionFakeLaunchCustodian(t)
	secondServer := newTestServerWithRoot(t, root, cwd, secondBackend, Config{IdleTimeout: -1})
	configureTestAdmissionRuntime(t, secondServer, secondLauncher, true)
	secondCancel, secondDone, _ := startTestServerWithBlockingListener(t, secondServer)
	defer func() {
		secondCancel()
		select {
		case <-secondDone:
		case <-time.After(5 * time.Second):
			t.Fatal("second Serve did not stop")
		}
	}()

	record := loadAdmissionSafetyRecord(t, secondServer, submitted.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed || record.Terminal.Cause != model.CauseDaemonRestartedBeforeAuthorization {
		t.Fatalf("recovered terminal = %+v, want failed daemon-restarted-before-authorization", record.Terminal)
	}
	if got := len(secondLauncher.preparedOrdinals()); got != 0 {
		t.Fatalf("recovery prepared launches = %d, want 0", got)
	}

	replay := secondServer.handleJobSubmit(ctx, mustMarshal(t, params))
	if replay.err != nil {
		t.Fatalf("replay submit error = %+v", replay.err)
	}
	replayed, ok := replay.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("replay submit result type = %T", replay.result)
	}
	if replayed.JobID != submitted.JobID || !replayed.Deduplicated || replayed.State != engine.StateFailed {
		t.Fatalf("replay result = %+v, want same failed terminal job %s", replayed, submitted.JobID)
	}
	if replay.after != nil {
		t.Fatal("replay returned a launch hook")
	}
	if got := secondBackend.count.Load(); got != 0 {
		t.Fatalf("second backend starts = %d, want 0 for terminal replay", got)
	}
	assertAuthoritySafetyRecordCount(t, secondServer, 1)
}

func TestIdentifiedSubmitReplayAfterRestartPreservesRecordedOutcome(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	firstBackend := newFakeBackend("fake")
	firstLauncher := newAdmissionFakeLaunchCustodian(t)
	firstServer := newTestServerWithRoot(t, root, cwd, firstBackend, Config{IdleTimeout: -1})
	configureTestAdmissionRuntime(t, firstServer, firstLauncher, true)
	firstCancel, firstDone, _ := startTestServerWithBlockingListener(t, firstServer)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-recorded-outcome-crash",
		RequestID:    "request-recorded-outcome-crash",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	outcome := firstServer.handleJobSubmit(ctx, mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("initial submit error = %+v", outcome.err)
	}
	submitted, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("initial submit result type = %T", outcome.result)
	}
	if submitted.JobID == "" || submitted.Deduplicated || outcome.after == nil {
		t.Fatalf("initial submit result = %+v hasHook=%t, want accepted non-replay with response hook", submitted, outcome.after != nil)
	}
	if got := firstBackend.count.Load(); got != 1 {
		t.Fatalf("first backend starts = %d, want construction before durable accept", got)
	}
	select {
	case turn := <-firstBackend.turns:
		t.Fatalf("backend turn = %+v, want none before response hook", turn)
	default:
	}

	record := loadAdmissionSafetyRecord(t, firstServer, submitted.JobID)
	ref := record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := firstServer.admissionReady.BindGroup(ctx, record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := firstServer.admissionReady.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}
	child, err := model.NewChildIdentity(5201, "recorded-outcome-child")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := model.NewEvidence("released", "recorded outcome release")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstServer.admissionReady.RecordRelease(ctx, record.JobID, ref, model.LaunchOrdinalOne, child, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := firstServer.admissionReady.RecordOutcome(ctx, record.JobID, ref, model.OutcomeFailed, nil); err != nil {
		t.Fatal(err)
	}
	record = loadAdmissionSafetyRecord(t, firstServer, submitted.JobID)
	if record.Outcome == nil || record.Outcome.Outcome != model.OutcomeFailed || record.Terminal != nil {
		t.Fatalf("crash-window safety = %+v, want recorded failed outcome without terminal", record)
	}

	firstCancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Serve stop error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Serve did not stop")
	}

	secondBackend := newFakeBackend("fake")
	secondLauncher := newAdmissionFakeLaunchCustodian(t)
	secondServer := newTestServerWithRoot(t, root, cwd, secondBackend, Config{IdleTimeout: -1})
	configureTestAdmissionRuntime(t, secondServer, secondLauncher, true)
	secondCancel, secondDone, _ := startTestServerWithBlockingListener(t, secondServer)
	defer func() {
		secondCancel()
		select {
		case <-secondDone:
		case <-time.After(5 * time.Second):
			t.Fatal("second Serve did not stop")
		}
	}()

	record = loadAdmissionSafetyRecord(t, secondServer, submitted.JobID)
	if record.Terminal == nil ||
		record.Terminal.Outcome != model.OutcomeFailed ||
		record.Terminal.Cause != model.CauseCompletedNormally ||
		record.Terminal.Proof != model.ProofCleanQuiescentOutcomeAndRetired {
		t.Fatalf("recovered terminal = %+v, want recorded failed clean terminal", record.Terminal)
	}
	if got := secondLauncher.containCount(); got != 1 {
		t.Fatalf("recovery containments = %d, want 1", got)
	}
	if reason := secondServer.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}

	replay := secondServer.handleJobSubmit(ctx, mustMarshal(t, params))
	if replay.err != nil {
		t.Fatalf("replay submit error = %+v", replay.err)
	}
	replayed, ok := replay.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("replay submit result type = %T", replay.result)
	}
	if replayed.JobID != submitted.JobID || !replayed.Deduplicated || replayed.State != engine.StateFailed {
		t.Fatalf("replay result = %+v, want same failed terminal job %s", replayed, submitted.JobID)
	}
	if replay.after != nil {
		t.Fatal("replay returned a launch hook")
	}
	if got := secondBackend.count.Load(); got != 0 {
		t.Fatalf("second backend starts = %d, want 0 for terminal replay", got)
	}
	assertAuthoritySafetyRecordCount(t, secondServer, 1)
}

func TestIdentifiedSubmitReplayIgnoresDeletedWorkspace(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-deleted",
		RequestID:    "request-replay-deleted",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}

	submitted := submitIdentifiedForReplayTest(t, server, params)
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}
	replayed := replayIdentifiedSubmit(t, server, params)
	if replayed.JobID != submitted.JobID || !replayed.Deduplicated {
		t.Fatalf("replay result = %+v, want same job %s deduplicated", replayed, submitted.JobID)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want only initial accept", got)
	}
}

func TestIdentifiedSubmitReplayIgnoresBrokenSymlinkedWorkspace(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	target := shortTempDir(t)
	link := filepath.Join(t.TempDir(), "workspace-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-symlink",
		RequestID:    "request-replay-symlink",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: link, Write: false, Prompt: "hold"},
	}

	submitted := submitIdentifiedForReplayTest(t, server, params)
	if err := os.RemoveAll(target); err != nil {
		t.Fatal(err)
	}
	replayed := replayIdentifiedSubmit(t, server, params)
	if replayed.JobID != submitted.JobID || !replayed.Deduplicated {
		t.Fatalf("replay result = %+v, want same job %s deduplicated", replayed, submitted.JobID)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want only initial accept", got)
	}
}

func TestIdentifiedSubmitReplayIgnoresRemovedBackendComposition(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-backend",
		RequestID:    "request-replay-backend",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}

	submitted := submitIdentifiedForReplayTest(t, server, params)
	server.admissionStateMu.Lock()
	server.admissionInstance.backendMu.Lock()
	delete(server.admissionInstance.descriptors, "fake")
	delete(server.admissionInstance.policy.backends, "fake")
	server.admissionInstance.backendMu.Unlock()
	server.admissionStateMu.Unlock()
	server.removeBackend("fake")

	replayed := replayIdentifiedSubmit(t, server, params)
	if replayed.JobID != submitted.JobID || !replayed.Deduplicated {
		t.Fatalf("replay result = %+v, want same job %s deduplicated", replayed, submitted.JobID)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want only initial accept", got)
	}
}

func TestIdentifiedSubmitReplayConflictIgnoresDeletedWorkspace(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-conflict",
		RequestID:    "request-replay-conflict",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	submitIdentifiedForReplayTest(t, server, params)
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}
	changed := params
	changed.TaskSpec.Prompt = "changed task"

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, changed))
	if outcome.err == nil {
		t.Fatalf("changed replay result = %+v, want replay conflict", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectReplayConflict)
	if strings.Contains(outcome.err.Message, "no such file") || strings.Contains(outcome.err.Message, "stat ") {
		t.Fatalf("changed replay error = %q, want replay conflict before path validation", outcome.err.Message)
	}
}

func TestIdentifiedSubmitReplayConflictWithMalformedTypedTaskSpec(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-malformed-typed-task",
		RequestID:    "request-replay-malformed-typed-task",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	submitIdentifiedForReplayTest(t, server, params)

	raw := json.RawMessage(fmt.Sprintf(
		`{"workspaceKey":%q,"requestId":%q,"taskSpec":{"backend":123}}`,
		params.WorkspaceKey,
		params.RequestID,
	))
	outcome := server.handleJobSubmit(context.Background(), raw)
	if outcome.err == nil {
		t.Fatalf("malformed typed replay result = %+v, want replay conflict", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectReplayConflict)
	if resp.Error.Data.AdmissionCause == protocol.AdmissionRejectInvalidStrictConfig {
		t.Fatalf("admission cause = %q, want replay_conflict before typed taskSpec decode", resp.Error.Data.AdmissionCause)
	}
	if strings.Contains(outcome.err.Message, "cannot unmarshal") {
		t.Fatalf("malformed typed replay error = %q, want replay conflict before typed taskSpec decode", outcome.err.Message)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want only initial accept", got)
	}
}

func TestIdentifiedSubmitReplayRejectsUnknownRecordedFingerprintVersion(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-fingerprint",
		RequestID:    "request-replay-fingerprint",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	raw := mustMarshal(t, params)
	rawTaskSpec, err := rawTaskSpecFromSubmitParams(raw)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := model.TaskIdentityFromRawTaskSpec(rawTaskSpec)
	if err != nil {
		t.Fatal(err)
	}
	identity.Version = model.CurrentTaskIdentityVersion + 1
	requestKey, err := model.NewRequestKey(params.WorkspaceKey, params.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	server.admissionStateMu.RLock()
	_, err = server.admissionSubmission.SubmitIdentified(context.Background(), authority.AcceptRequest{
		RequestKey:   requestKey,
		TaskIdentity: identity,
		Mode:         model.ModeIdentifiedFenced,
		SessionID:    "session-future-fingerprint",
	})
	server.admissionStateMu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobSubmit(context.Background(), raw)
	if outcome.err == nil {
		t.Fatalf("future fingerprint replay result = %+v, want unsupported fingerprint", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectRequestFingerprintUnsupported)
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want none for replay rejection", got)
	}
}

func TestIdentifiedSubmitTombstoneReplayIgnoresDeletedWorkspace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-replay-tombstone",
		RequestID:    "request-replay-tombstone",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	submitted := submitIdentifiedForReplayTest(t, server, params)
	record := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	if _, err := server.admissionReady.Finalize(ctx, record.JobID, record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	}); err != nil {
		t.Fatal(err)
	}
	requestKey, err := model.NewRequestKey(params.WorkspaceKey, params.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.Expire(ctx, requestKey); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobSubmit(ctx, mustMarshal(t, params))
	if outcome.err == nil {
		t.Fatalf("tombstone replay result = %+v, want request expired", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectRequestExpired)
	if strings.Contains(outcome.err.Message, "no such file") || strings.Contains(outcome.err.Message, "stat ") {
		t.Fatalf("tombstone replay error = %q, want request_expired before path validation", outcome.err.Message)
	}
}

func TestAdmissionRecoveryExecutorBoundCurrentObligationContainsOnlyDurableGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	launcher := newAdmissionFakeLaunchCustodian(t)
	oldReady, accepted := newPriorBootAuthorityWork(t, repo, anchorStore, launcher, "recovery-contain")
	ref := accepted.Record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := oldReady.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := oldReady.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}

	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)

	image, err := server.admissionReady.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	record := image.Safety.Value
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence == nil {
		t.Fatalf("recovered launch = %+v, want verified quiescence", first)
	}
	if first.Quiescence.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("quiescence method = %s, want %s", first.Quiescence.Method, model.QuiescenceAlreadyAbsent)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeReaped || record.Terminal.Cause != model.CauseDaemonRestartedAfterAuthorization {
		t.Fatalf("terminal = %+v, want reaped daemon-restarted-after-authorization", record.Terminal)
	}
	if record.Terminal.Proof != model.ProofContained {
		t.Fatalf("terminal proof = %s, want %s", record.Terminal.Proof, model.ProofContained)
	}
	if got := launcher.containCount(); got != 1 {
		t.Fatalf("containments = %d, want one residual group containment", got)
	}
	contains := launcher.containObservations()
	if len(contains) != 1 {
		t.Fatalf("contain observations = %d, want 1", len(contains))
	}
	if contains[0].cause != custodian.QuiescenceCauseRecovery {
		t.Fatalf("contain cause = %s, want %s", contains[0].cause, custodian.QuiescenceCauseRecovery)
	}
	if !contains[0].group.Equal(group) {
		t.Fatalf("contain group = %+v, want durable group %+v", contains[0].group, group)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("prepared launches = %d, want 0", got)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("releases = %d, want 0", got)
	}
	if got := launcher.abortCount(); got != 0 {
		t.Fatalf("aborts = %d, want 0", got)
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestAdmissionRecoveryPersistsFinalAttemptTimingForRestartedRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	launcher := newAdmissionFakeLaunchCustodian(t)
	oldReady, accepted := newPriorBootAuthorityWork(t, repo, anchorStore, launcher, "recovery-final-attempt-timing")
	ref := accepted.Record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := oldReady.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := oldReady.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 8, 11, 15, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(12 * time.Second)
	if _, err := oldReady.RecordFinalAttemptStart(ctx, accepted.Record.JobID, startedAt); err != nil {
		t.Fatalf("RecordFinalAttemptStart() error = %v", err)
	}

	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.clock = &steppedClock{now: endedAt}
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)

	image, err := server.admissionReady.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.Value.FinalAttemptStartedAt == nil || !image.Safety.Value.FinalAttemptStartedAt.Equal(startedAt) || image.Safety.Value.FinalAttemptEndedAt == nil || !image.Safety.Value.FinalAttemptEndedAt.Equal(endedAt) {
		t.Fatalf("recovered safety timing = start:%v end:%v, want %s/%s", image.Safety.Value.FinalAttemptStartedAt, image.Safety.Value.FinalAttemptEndedAt, startedAt, endedAt)
	}
	if image.Projection.Value.FinalAttemptStartedAt == nil || !image.Projection.Value.FinalAttemptStartedAt.Equal(startedAt) || image.Projection.Value.FinalAttemptEndedAt == nil || !image.Projection.Value.FinalAttemptEndedAt.Equal(endedAt) {
		t.Fatalf("recovered projection timing = start:%v end:%v, want %s/%s", image.Projection.Value.FinalAttemptStartedAt, image.Projection.Value.FinalAttemptEndedAt, startedAt, endedAt)
	}

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: accepted.Record.JobID.String()})
	if len(status.Jobs) != 1 || status.Jobs[0].FinalAttemptStartedAt == nil || !status.Jobs[0].FinalAttemptStartedAt.Equal(startedAt) || status.Jobs[0].FinalAttemptEndedAt == nil || !status.Jobs[0].FinalAttemptEndedAt.Equal(endedAt) {
		t.Fatalf("recovered status timing = %+v, want %s/%s", status, startedAt, endedAt)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: accepted.Record.JobID.String()})
	if result.FinalAttemptStartedAt == nil || !result.FinalAttemptStartedAt.Equal(startedAt) || result.FinalAttemptEndedAt == nil || !result.FinalAttemptEndedAt.Equal(endedAt) {
		t.Fatalf("recovered result timing = start:%v end:%v, want %s/%s", result.FinalAttemptStartedAt, result.FinalAttemptEndedAt, startedAt, endedAt)
	}
}

func TestStrictAuthorityTerminalTimingSuppressesPartialPair(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "partial-terminal-timing")
	startedAt := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)

	if _, err := server.admissionReady.RecordFinalAttemptStart(ctx, accepted.Record.JobID, startedAt); err != nil {
		t.Fatalf("RecordFinalAttemptStart() error = %v", err)
	}
	if _, err := server.admissionReady.Finalize(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	}); err != nil {
		t.Fatalf("Finalize() error = %v", err)
	}

	image, err := server.admissionReady.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.Value.FinalAttemptStartedAt == nil || !image.Safety.Value.FinalAttemptStartedAt.Equal(startedAt) || image.Safety.Value.FinalAttemptEndedAt != nil {
		t.Fatalf("durable partial timing = start:%v end:%v, want %s/<nil>", image.Safety.Value.FinalAttemptStartedAt, image.Safety.Value.FinalAttemptEndedAt, startedAt)
	}
	if image.Projection.Value.FinalAttemptStartedAt != nil || image.Projection.Value.FinalAttemptEndedAt != nil {
		t.Fatalf("partial terminal projection timing = start:%v end:%v, want absent", image.Projection.Value.FinalAttemptStartedAt, image.Projection.Value.FinalAttemptEndedAt)
	}

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: accepted.Record.JobID.String()})
	if len(status.Jobs) != 1 || status.Jobs[0].FinalAttemptStartedAt != nil || status.Jobs[0].FinalAttemptEndedAt != nil {
		t.Fatalf("partial terminal status timing = %+v, want absent", status)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: accepted.Record.JobID.String()})
	if result.FinalAttemptStartedAt != nil || result.FinalAttemptEndedAt != nil {
		t.Fatalf("partial terminal result timing = start:%v end:%v, want absent", result.FinalAttemptStartedAt, result.FinalAttemptEndedAt)
	}
}

func TestAdmissionRecoveryExecutorAttemptsRemainingLaunchBeforeUnresolvedFinalization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	launcher := newAdmissionFakeLaunchCustodian(t)
	jobID := model.JobID("job-recovery-all-ordinals")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-recovery-all-ordinals", Epoch: 1}
	secondGroup := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalTwo})
	boot, err := model.NewBootRef("boot-recovery-all-ordinals-new", "owner-recovery-all-ordinals-new")
	if err != nil {
		t.Fatal(err)
	}
	token := model.RecoveryToken{JobID: jobID, BasedOnRevision: 3, RecoveryBoot: boot, Opaque: "recovery-all-ordinals-token"}
	item := model.RecoveryWorkItem{
		Token:           token,
		JobID:           jobID,
		BasedOnRevision: token.BasedOnRevision,
		Trigger:         model.RecoveryStartupLoss,
		// Reachable production topology: ordinal 1 has already reached durable
		// quiescence, so startup recovery only receives the remaining live
		// ordinal.
		Launches: []model.RecoveryLaunch{{Ordinal: model.LaunchOrdinalTwo, Group: secondGroup}},
	}
	session := &admissionRecoveryFakeSession{
		items: []model.RecoveryWorkItem{item},
		finalized: model.SafetyRecord{
			JobID: jobID,
			Terminal: &model.TerminalCertificate{
				Outcome: model.OutcomeOrphaned,
				Proof:   model.ProofUnresolvedAbsence,
				Cause:   model.CauseDaemonRestartedAfterAuthorization,
			},
		},
	}
	launcher.containErrByOrdinal = map[model.LaunchOrdinal]error{
		model.LaunchOrdinalTwo: &custodian.CleanupUnresolvedError{Reason: containment.ReasonProbeUnprovable, Decision: model.Unprovable},
	}

	latch := NewSafetyLatch()
	report, err := recoverAdmissionBeforeReadyReport(ctx, session, launcher, latch)
	if err != nil {
		t.Fatal(err)
	}
	if report.QuiescedLaunches != 0 || report.UnresolvedLaunches != 1 || report.FinalizedJobs != 1 || report.OrphanedJobs != 1 {
		t.Fatalf("recovery report = %+v, want one unresolved remaining launch and one orphaned job", report)
	}
	if session.finalizeUnresolvedCalls != 1 {
		t.Fatalf("FinalizeUnresolved calls = %d, want 1", session.finalizeUnresolvedCalls)
	}
	if len(session.recordedOrdinals) != 0 {
		t.Fatalf("recorded ordinals = %v, want none for already-quiesced ordinal 1", session.recordedOrdinals)
	}
	contains := launcher.containObservations()
	if len(contains) != 1 {
		t.Fatalf("contain attempts = %d, want remaining ordinal attempted", len(contains))
	}
	if contains[0].group.Launch.Ordinal != model.LaunchOrdinalTwo {
		t.Fatalf("contain ordinal = %s; want 2", contains[0].group.Launch.Ordinal)
	}
	if reason := latch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestAdmissionRecoveryExecutorFinalizePlannedCancellationAbortsWithoutFailStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	launcher := newAdmissionFakeLaunchCustodian(t)
	jobID := model.JobID("job-recovery-finalize-planned-cancel")
	boot, err := model.NewBootRef("boot-recovery-finalize-planned-cancel", "owner-recovery-finalize-planned-cancel")
	if err != nil {
		t.Fatal(err)
	}
	token := model.RecoveryToken{JobID: jobID, BasedOnRevision: 2, RecoveryBoot: boot, Opaque: "recovery-finalize-planned-cancel-token"}
	session := &admissionRecoveryFakeSession{
		items: []model.RecoveryWorkItem{{
			Token:           token,
			JobID:           jobID,
			BasedOnRevision: token.BasedOnRevision,
			Trigger:         model.RecoveryStartupLoss,
		}},
		finalizePlannedErr: context.Canceled,
	}
	latch := NewSafetyLatch()

	_, err = recoverAdmissionBeforeReadyReport(ctx, session, launcher, latch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("recovery error = %v, want context.Canceled", err)
	}
	if session.finalizePlannedCalls != 1 {
		t.Fatalf("FinalizePlanned calls = %d, want 1", session.finalizePlannedCalls)
	}
	if reason := latch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestAdmissionRecoveryExecutorReportsCleanupWarningAfterQuiescence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cleanupErr := errors.New("recovery cleanup failed after proof")
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.cleanupErr = cleanupErr
	jobID := model.JobID("job-recovery-cleanup-warning")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-recovery-cleanup-warning", Epoch: 1}
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	boot, err := model.NewBootRef("boot-recovery-cleanup-warning", "owner-recovery-cleanup-warning")
	if err != nil {
		t.Fatal(err)
	}
	token := model.RecoveryToken{JobID: jobID, BasedOnRevision: 2, RecoveryBoot: boot, Opaque: "recovery-cleanup-warning-token"}
	session := &admissionRecoveryFakeSession{
		items: []model.RecoveryWorkItem{{
			Token:           token,
			JobID:           jobID,
			BasedOnRevision: token.BasedOnRevision,
			Trigger:         model.RecoveryStartupLoss,
			Launches:        []model.RecoveryLaunch{{Ordinal: model.LaunchOrdinalOne, Group: group}},
		}},
		afterAdvance: model.RecoveryWorkItem{
			Token:           token,
			JobID:           jobID,
			BasedOnRevision: token.BasedOnRevision,
			Trigger:         model.RecoveryStartupLoss,
		},
	}
	latch := NewSafetyLatch()

	report, err := recoverAdmissionBeforeReadyReport(ctx, session, launcher, latch)
	if err != nil {
		t.Fatal(err)
	}
	if report.QuiescedLaunches != 1 || report.CleanupWarnings != 1 || report.FinalizedJobs != 1 {
		t.Fatalf("recovery report = %+v, want one quiesced launch, cleanup warning, and finalized job", report)
	}
	if session.finalizePlannedCalls != 1 {
		t.Fatalf("FinalizePlanned calls = %d, want 1", session.finalizePlannedCalls)
	}
	if len(session.recordedOrdinals) != 1 || session.recordedOrdinals[0] != model.LaunchOrdinalOne {
		t.Fatalf("recorded ordinals = %v, want ordinal 1", session.recordedOrdinals)
	}
	if reason := latch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestAdmissionRecoveryExecutorTypedUnresolvedContinuesStartupWithoutRelaunch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	launcher := newAdmissionFakeLaunchCustodian(t)
	oldReady, accepted := newPriorBootAuthorityWork(t, repo, anchorStore, launcher, "recovery-orphan-continues")
	ref := accepted.Record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := oldReady.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := oldReady.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}
	launcher.containErrByOrdinal = map[model.LaunchOrdinal]error{
		model.LaunchOrdinalOne: &custodian.CleanupUnresolvedError{Reason: containment.ReasonAbsenceDeadlineExceeded, Decision: model.SignalDirectly},
	}

	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)

	if server.admissionReady == nil {
		t.Fatal("admission did not seal ready after typed unresolved recovery")
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
	if snapshot := admissionAnchorSnapshot(t, anchorStore); snapshot.Phase == "fail_stopped" {
		t.Fatalf("anchor phase = %q, want non-fail-stopped after typed unresolved recovery", snapshot.Phase)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want no recovery relaunch", got)
	}
	image, err := server.admissionReady.LoadJob(ctx, accepted.Record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	record := image.Safety.Value
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeOrphaned || record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("terminal = %+v, want orphaned unresolved", record.Terminal)
	}
	if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
	replay, err := server.admissionReady.LookupReplay(ctx, accepted.Binding.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	if replay.State != authority.ReplayLive || replay.Record.JobID != accepted.Record.JobID || replay.Record.Terminal == nil || replay.Projection.Public != model.PublicOrphaned {
		t.Fatalf("orphaned replay = %+v, want original terminal orphaned job %s", replay, accepted.Record.JobID)
	}
	newSubmit := submitIdentifiedForReplayTest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-recovery-orphan-continues-new",
		RequestID:    "request-recovery-orphan-continues-new",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "after orphaned recovery",
		},
	})
	if newSubmit.JobID == string(accepted.Record.JobID) || newSubmit.Deduplicated {
		t.Fatalf("new submit after orphaned recovery = %+v, want new accepted job", newSubmit)
	}
}

type admissionRecoveryFakeSession struct {
	items                   []model.RecoveryWorkItem
	afterAdvance            model.RecoveryWorkItem
	finalized               model.SafetyRecord
	finalizePlannedErr      error
	recordedOrdinals        []model.LaunchOrdinal
	finalizePlannedCalls    int
	finalizeUnresolvedCalls int
}

func (s *admissionRecoveryFakeSession) WorkItems(context.Context) ([]model.RecoveryWorkItem, error) {
	return append([]model.RecoveryWorkItem(nil), s.items...), nil
}

func (s *admissionRecoveryFakeSession) AdvanceRecovery(context.Context, model.RecoveryToken) (model.RecoveryWorkItem, error) {
	return s.afterAdvance, nil
}

func (s *admissionRecoveryFakeSession) FinalizePlanned(context.Context, model.RecoveryToken) error {
	s.finalizePlannedCalls++
	if s.finalizePlannedErr != nil {
		return s.finalizePlannedErr
	}
	s.items = nil
	return nil
}

func (s *admissionRecoveryFakeSession) FinalizeUnresolved(context.Context, model.RecoveryToken) (model.SafetyRecord, error) {
	s.finalizeUnresolvedCalls++
	s.items = nil
	return s.finalized, nil
}

func (s *admissionRecoveryFakeSession) RecordQuiescence(_ context.Context, _ any, ordinal model.LaunchOrdinal, _ custodian.VerifiedQuiescence) error {
	s.recordedOrdinals = append(s.recordedOrdinals, ordinal)
	return nil
}

func TestAdmissionRecoveryExecutorTripsLatchWhenContainmentUnprovable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	launcher := newAdmissionFakeLaunchCustodian(t)
	oldReady, accepted := newPriorBootAuthorityWork(t, repo, anchorStore, launcher, "recovery-unprovable")
	ref := accepted.Record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := oldReady.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := oldReady.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}
	launcher.containErr = errors.New("containment unprovable")

	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()), authority.WithSafetyLatch(s.safetyLatch))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	configureTestAdmissionRuntime(t, server, launcher, true)

	err := server.bootstrapAdmission(ctx)
	if !errors.Is(err, ErrSafetyFailStopped) {
		t.Fatalf("bootstrapAdmission error = %v, want safety fail-stop", err)
	}
	if server.admissionReady != nil {
		t.Fatal("admission ready was sealed after unresolved recovery")
	}
	if reason := server.safetyLatch.Reason(); reason == nil || !strings.Contains(reason.Error(), "containment unprovable") {
		t.Fatalf("safety latch reason = %v, want containment failure", reason)
	}
}

func TestDefaultStrictServeRejectsUnavailableRuntimeBeforeListen(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	var listened atomic.Bool
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listened.Store(true)
		return nil, socketFileIdentity{}, errors.New("listener should not start without strict admission support")
	}

	err := server.Serve(context.Background())
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("Serve error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if diagnostic.Assessment.Class != custodian.SupportUnsupported {
		t.Fatalf("diagnostic assessment = %+v, want unsupported", diagnostic.Assessment)
	}
	if listened.Load() {
		t.Fatal("listener started before strict admission support was available")
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
}

func TestJobSubmitWithoutIdentityRejectedMissingIdentity(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	resp := rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"},
	})
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectMissingIdentity)
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
}

func TestUnknownAuthorityJobReturnsStableUnknownJobCode(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	jobID := "job_missing"

	assertJobHandlerError(t, server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{JobID: jobID})), protocol.ErrorUnknownJob, "", jobID)
	assertJobHandlerError(t, server.handleJobResult(mustMarshal(t, protocol.JobResultParams{JobID: jobID})), protocol.ErrorUnknownJob, "", jobID)
	assertJobHandlerError(t, server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: jobID})), protocol.ErrorUnknownJob, "", jobID)
}

func TestStrictRequestedUnavailableRuntimeFailsStartupWithSupportDiagnostic(t *testing.T) {
	t.Parallel()
	server, err := New(Config{
		StateRoot:   shortTempDir(t),
		CWD:         shortTempDir(t),
		Runtime:     custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		IdleTimeout: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Serve(context.Background())
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("Serve error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if diagnostic.Assessment.Class != custodian.SupportUnsupported {
		t.Fatalf("diagnostic assessment = %+v, want unsupported", diagnostic.Assessment)
	}
}

func TestInvalidStrictSubmitDoesNotProbeBackendVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	marker := filepath.Join(dir, "version-probe-marker")
	backend := codexcli.New(codexcli.Options{
		Binary:    markerCodexCLI(t, marker),
		CachePath: filepath.Join(dir, "setup-probes.json"),
	})
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-missing-request-id",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectMissingIdentity)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("version probe marker after invalid submit stat err = %v, want not exist", err)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestBootstrapAdmissionProbesStrictBackendsWithConfiguredRunner(t *testing.T) {
	t.Parallel()
	backend := &probeableFakeBackend{fakeBackend: newFakeBackend("fake")}
	server, _, _ := newUnstartedTestServer(t, backend)
	runner := &recordingProbeRunner{}
	server.admissionProbeRunner = runner
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("bootstrap probes = %d, want 1", got)
	}
	if got := runner.lookups.Load(); got != 1 {
		t.Fatalf("probe runner lookups = %d, want 1", got)
	}
	if got := runner.runs.Load(); got != 1 {
		t.Fatalf("probe runner runs = %d, want 1", got)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts during bootstrap = %d, want 0", got)
	}
}

func TestServeAdmissionPolicyImmutableAfterBootstrapInputMutation(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	unavailable := custodian.NewUnavailableRuntime(errors.New("mutated runtime after bootstrap")).Support()
	server.admissionRuntime.supportOverride = &unavailable
	backend.controlled = false
	backend.parkable = false

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-policy-immutable",
		RequestID:    "request-policy-immutable",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want accepted non-replay", submitted)
	}
	waitBackendStarted(t, backend)
}

func TestStrictIdentifiedOrderedRejectionsBeforeMutationOrRequestProbe(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name             string
		configureBackend func(*fakeBackend)
		configureServer  func(*Server)
		params           func(cwd string) protocol.JobSubmitParams
		wantCode         string
		wantCause        string
	}
	cases := []testCase{
		{
			name: "missing identity before backend",
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-missing-identity",
					TaskSpec:     protocol.TaskSpec{Backend: "missing", CWD: cwd, Write: false},
				}
			},
			wantCode:  protocol.ErrorInvalidTaskSpec,
			wantCause: protocol.AdmissionRejectMissingIdentity,
		},
		{
			name: "unsupported backend before invalid config",
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-unsupported",
					RequestID:    "request-unsupported",
					TaskSpec:     protocol.TaskSpec{Backend: "missing", CWD: cwd, Write: false},
				}
			},
			wantCode:  protocol.ErrorBackendUnavailable,
			wantCause: protocol.AdmissionRejectUnsupportedBackend,
		},
		{
			name: "unfenceable before invalid config",
			configureBackend: func(backend *fakeBackend) {
				backend.controlled = false
			},
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-unfenceable",
					RequestID:    "request-unfenceable",
					TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false},
				}
			},
			wantCode:  protocol.ErrorCapabilityMissing,
			wantCause: protocol.AdmissionRejectUnfenceableBackend,
		},
		{
			name: "admission unavailable before missing identity",
			configureServer: func(server *Server) {
				server.admissionInstance.policy.strictRouteEnabled = false
				server.admissionInstance.policy.strictRouteDisabledReason = "strict route disabled for test"
			},
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-route-disabled",
					TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
				}
			},
			wantCode:  protocol.ErrorCapabilityMissing,
			wantCause: protocol.AdmissionRejectUnavailableNativeRuntime,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeBackend("fake")
			if tt.configureBackend != nil {
				tt.configureBackend(fake)
			}
			backend := &probeableFakeBackend{fakeBackend: fake}
			server, root, cwd := newUnstartedTestServer(t, backend)
			runner := &recordingProbeRunner{}
			server.admissionProbeRunner = runner
			launcher := newAdmissionFakeLaunchCustodian(t)
			enableTestAdmission(t, server, launcher)
			if tt.configureServer != nil {
				tt.configureServer(server)
			}
			probeLookups := runner.lookups.Load()
			probeRuns := runner.runs.Load()

			outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, tt.params(cwd)))
			if outcome.err == nil {
				t.Fatalf("submit result = %+v, want error", outcome.result)
			}
			resp := protocol.Response{Error: outcome.err}
			assertRPCCode(t, resp, tt.wantCode)
			assertRPCAdmissionCause(t, resp, tt.wantCause)
			if got := backend.count.Load(); got != 0 {
				t.Fatalf("backend starts = %d, want 0 before strict rejection", got)
			}
			if got := runner.lookups.Load(); got != probeLookups {
				t.Fatalf("probe lookups after request = %d, want unchanged %d", got, probeLookups)
			}
			if got := runner.runs.Load(); got != probeRuns {
				t.Fatalf("probe runs after request = %d, want unchanged %d", got, probeRuns)
			}
			assertNoAcceptedJobsInAdmission(t, server)
			assertNoWorkspaceNamespaceForCWD(t, root, cwd)
		})
	}
}

type failingProbeBackend struct {
	*fakeBackend
}

func (b *failingProbeBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return nil, errors.New("codex binary not found: executable file not found in $PATH")
}

type probeErrorBackend struct {
	*fakeBackend
	err     error
	started chan struct{}
	probes  atomic.Int64
}

func (b *probeErrorBackend) ProbeBackend(ctx context.Context, _ command.ProbeRunner) (engine.Backend, error) {
	b.probes.Add(1)
	if b.started != nil {
		select {
		case b.started <- struct{}{}:
		default:
		}
	}
	if b.err != nil {
		return nil, b.err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type setupCacheRefreshProbeBackend struct {
	*fakeBackend
	mu               sync.Mutex
	fingerprint      string
	fingerprintReads []setupCacheRefreshFingerprintRead
	err              error
	gate             <-chan struct{}
	started          chan struct{}
	probed           engine.Backend
	probes           atomic.Int64
}

type setupCacheRefreshFingerprintRead struct {
	fingerprint string
	err         error
}

func (b *setupCacheRefreshProbeBackend) SetupProbeCacheFingerprint() (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.fingerprintReads) > 0 {
		read := b.fingerprintReads[0]
		b.fingerprintReads = b.fingerprintReads[1:]
		return read.fingerprint, read.err
	}
	return b.fingerprint, nil
}

func (b *setupCacheRefreshProbeBackend) ProbeBackend(ctx context.Context, _ command.ProbeRunner) (engine.Backend, error) {
	b.probes.Add(1)
	b.mu.Lock()
	err := b.err
	gate := b.gate
	started := b.started
	probed := b.probed
	b.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if probed != nil {
		return probed, nil
	}
	return b, nil
}

func (b *setupCacheRefreshProbeBackend) setSetupCacheRefreshState(fingerprint string, err error, gate <-chan struct{}) {
	b.mu.Lock()
	b.fingerprint = fingerprint
	b.err = err
	b.gate = gate
	b.mu.Unlock()
}

func (b *setupCacheRefreshProbeBackend) setSetupCacheRefreshProbedBackend(probed engine.Backend) {
	b.mu.Lock()
	b.probed = probed
	b.mu.Unlock()
}

func (b *setupCacheRefreshProbeBackend) setSetupCacheRefreshFingerprintReads(reads ...setupCacheRefreshFingerprintRead) {
	b.mu.Lock()
	b.fingerprintReads = append(b.fingerprintReads[:0], reads...)
	b.mu.Unlock()
}

type lyingControlledBackend struct {
	*fakeBackend
	session *nonOrdinalSession
	probes  atomic.Int64
}

func (b *lyingControlledBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	b.probes.Add(1)
	return b, nil
}

func (b *lyingControlledBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	n := b.count.Add(1)
	if b.session == nil {
		b.session = &nonOrdinalSession{id: b.Name() + "-non-ordinal-" + stringID(n)}
	}
	return b.session, nil
}

type invalidSessionIDBackend struct {
	*fakeBackend
	sessionID string
}

func (b *invalidSessionIDBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return b, nil
}

func (b *invalidSessionIDBackend) Start(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
	b.count.Add(1)
	if b.startHook != nil {
		b.startHook(opts)
	}
	return &fakeSession{id: b.sessionID, backend: b.fakeBackend}, nil
}

// A strict backend whose probe fails for environment reasons (missing binary,
// bad version) must NOT fail Serve bootstrap: the daemon keeps serving and the
// backend is recorded unfenceable so strict identified admission rejects it
// pre-accept with the probe failure as the cause.
func TestServeBootstrapRecordsProbeFailureUnfenceableWithoutFailingClosed(t *testing.T) {
	t.Parallel()
	backend := &failingProbeBackend{fakeBackend: newFakeBackend("fake")}
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	// Available runtime: proves the rejection below is about the BACKEND
	// probe failure, not the runtime.
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-probe-failure",
		RequestID:    "request-probe-failure",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if !strings.Contains(outcome.err.Message, "probe strict backend") {
		t.Fatalf("rejection message = %q, want probe failure cause", outcome.err.Message)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestAdmissionSetupCacheRefreshRecoversFromUnknownBootstrapFingerprint(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "readable-cache",
		fingerprintReads: []setupCacheRefreshFingerprintRead{{
			err: errors.New("setup cache unreadable during bootstrap"),
		}},
		err: errors.New("setup cache is stale"),
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("bootstrap probes = %d, want 1", got)
	}

	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	instance.backendMu.RLock()
	pinned, stillPinned := instance.pinned["fake"]
	instance.backendMu.RUnlock()
	server.admissionStateMu.RUnlock()
	if !stillPinned || pinned.setupCacheFingerprintKnown || pinned.setupCacheFingerprint != "" || pinned.reprobeInFlight {
		t.Fatalf("bootstrap pin = %+v stillPinned=%t, want an idle unknown-fingerprint pin", pinned, stillPinned)
	}

	backend.setSetupCacheRefreshState("readable-cache", nil, nil)
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-unknown-bootstrap",
		RequestID:    "request-refresh-unknown-bootstrap",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err != nil {
		t.Fatalf("submit error = %+v, want refresh to publish", outcome.err)
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after readable cache = %d, want 2", got)
	}
	server.admissionStateMu.RLock()
	instance = server.admissionInstance
	_, stillPinned = server.admissionUnprobeableBackends["fake"]
	server.admissionStateMu.RUnlock()
	descriptor, ok := instance.descriptor("fake")
	if !ok || !descriptor.fenceable || stillPinned {
		t.Fatalf("refresh state descriptor=%+v stillPinned=%t, want fenceable and unpinned", descriptor, stillPinned)
	}
}

func TestAdmissionSetupCacheRefreshDoesNotReprobeUnchangedCache(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
	}
	server, root, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("bootstrap probes = %d, want 1", got)
	}

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-unchanged",
		RequestID:    "request-refresh-unchanged",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want pinned-backend rejection", outcome.result)
	}
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("probes after unchanged cache = %d, want 1", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestAdmissionSetupCacheRefreshReprobesChangedCacheAndUnpins(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	backend.setSetupCacheRefreshState("after-refresh", nil, nil)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-changed",
		RequestID:    "request-refresh-changed",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err != nil {
		t.Fatalf("submit error = %+v", outcome.err)
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after changed cache = %d, want 2", got)
	}
	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	_, stillPinned := server.admissionUnprobeableBackends["fake"]
	server.admissionStateMu.RUnlock()
	descriptor, ok := instance.descriptor("fake")
	if !ok || !descriptor.fenceable || stillPinned {
		t.Fatalf("refresh state descriptor=%+v stillPinned=%t, want fenceable and unpinned", descriptor, stillPinned)
	}
}

func TestAdmissionSetupCacheRefreshPublishesProbedBackendToCanonicalMap(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	probed := newFakeBackend("fake")
	backend.setSetupCacheRefreshState("after-refresh", nil, nil)
	backend.setSetupCacheRefreshProbedBackend(probed)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-canonical-backend",
		RequestID:    "request-refresh-canonical-backend",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err != nil {
		t.Fatalf("submit error = %+v, want refresh to publish", outcome.err)
	}
	canonical, ok := server.backendFor("fake")
	if !ok {
		t.Fatal("canonical backend is missing")
	}
	if canonical != probed {
		t.Fatalf("canonical backend = %T %p, want probed backend %T %p", canonical, canonical, probed, probed)
	}
}

func TestAdmissionSetupCacheRefreshIsSafeWithConcurrentBackendReaders(t *testing.T) {
	backendProbeStarted := make(chan struct{}, 2)
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
		started:     backendProbeStarted,
	}
	server, _, _ := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	select {
	case <-backendProbeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap probe did not start")
	}

	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	server.admissionStateMu.RUnlock()
	if instance == nil {
		t.Fatal("admission instance is nil")
	}

	gate := make(chan struct{})
	probed := newFakeBackend("fake")
	backend.setSetupCacheRefreshState("after-refresh", nil, gate)
	backend.setSetupCacheRefreshProbedBackend(probed)
	refreshed := make(chan struct{})
	go func() {
		server.refreshAdmissionBackendOnSetupCacheChange(context.Background(), instance, "fake")
		close(refreshed)
	}()
	select {
	case <-backendProbeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh probe did not start")
	}

	stopReaders := make(chan struct{})
	readerStarted := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			_ = server.backendMetadata()
			_ = server.backendNames()
			select {
			case <-readerStarted:
			default:
				close(readerStarted)
			}
			select {
			case <-stopReaders:
				return
			default:
			}
		}
	}()
	select {
	case <-readerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("backend reader did not start")
	}

	close(gate)
	select {
	case <-refreshed:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not finish")
	}
	close(stopReaders)
	readers.Wait()

	canonical, ok := server.backendFor("fake")
	if !ok || canonical != probed {
		t.Fatalf("canonical backend = %T %p, want probed backend %T %p", canonical, canonical, probed, probed)
	}
}

func TestAdmissionSetupCacheRefreshUsesRewrittenCodexCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	binary := markerCodexCLI(t, filepath.Join(dir, "version-probe-marker"))
	cachePath := filepath.Join(dir, "setup-probes.json")
	if err := engine.WriteSetupProbeCache(cachePath, engine.SetupProbeCache{Backends: []engine.BackendSetupProbe{{
		Backend:          "codex",
		BinaryPath:       binary,
		Version:          codexcli.MinimumKnownGoodVersion,
		StreamSchema:     "legacy-stream-json",
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	server, _, _ := newUnstartedTestServer(t, codexcli.New(codexcli.Options{Binary: binary, CachePath: cachePath}))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	if err := engine.WriteSetupProbeCache(cachePath, engine.SetupProbeCache{Backends: []engine.BackendSetupProbe{{
		Backend:          "codex",
		BinaryPath:       binary,
		Version:          codexcli.MinimumKnownGoodVersion,
		StreamSchema:     codexcli.StreamSchema,
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}}}); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-cache-refresh",
		RequestID:    "request-codex-cache-refresh",
		TaskSpec:     protocol.TaskSpec{Backend: "codex"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want invalid task-spec rejection after re-probe", outcome.result)
	}
	assertRPCCode(t, protocol.Response{Error: outcome.err}, protocol.ErrorInvalidTaskSpec)
	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	_, stillPinned := server.admissionUnprobeableBackends["codex"]
	server.admissionStateMu.RUnlock()
	descriptor, ok := instance.descriptor("codex")
	if !ok || !descriptor.fenceable || stillPinned {
		t.Fatalf("rewritten cache descriptor=%+v stillPinned=%t, want fenceable and unpinned", descriptor, stillPinned)
	}
}

func TestAdmissionSetupCacheRefreshFailureKeepsBackendPinned(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
	}
	server, root, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	backend.setSetupCacheRefreshState("after-refresh", errors.New("refreshed setup cache is still invalid"), nil)

	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-failed",
		RequestID:    "request-refresh-failed",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want pinned-backend rejection", outcome.result)
	}
	if !strings.Contains(outcome.err.Message, "refreshed setup cache is still invalid") {
		t.Fatalf("rejection message = %q, want refreshed probe failure", outcome.err.Message)
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after failed refresh = %d, want 2", got)
	}
	params.RequestID = "request-refresh-failed-unchanged"
	outcome = server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if outcome.err == nil {
		t.Fatalf("unchanged failed-refresh submit result = %+v, want rejection", outcome.result)
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after unchanged failed refresh = %d, want 2", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestAdmissionSetupCacheRefreshUnknownPostProbeFingerprintDoesNotConsumeRevision(t *testing.T) {
	t.Parallel()
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	backend.setSetupCacheRefreshState("after-refresh", nil, nil)
	backend.setSetupCacheRefreshFingerprintReads(
		setupCacheRefreshFingerprintRead{fingerprint: "after-refresh"},
		setupCacheRefreshFingerprintRead{err: errors.New("transient setup cache read failure")},
	)

	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-refresh-unknown-post",
		RequestID:    "request-refresh-unknown-post-first",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	first := server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if first.err == nil {
		t.Fatalf("first submit result = %+v, want pinned-backend rejection", first.result)
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after unknown post-probe fingerprint = %d, want 2", got)
	}

	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	instance.backendMu.RLock()
	pinned, stillPinned := instance.pinned["fake"]
	instance.backendMu.RUnlock()
	server.admissionStateMu.RUnlock()
	if !stillPinned || pinned.setupCacheFingerprint != "before-refresh" || pinned.reprobeInFlight {
		t.Fatalf("pin after unknown post-probe fingerprint = %+v stillPinned=%t, want prior fingerprint and no re-probe in flight", pinned, stillPinned)
	}

	params.RequestID = "request-refresh-unknown-post-follow-up"
	followUp := server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if followUp.err != nil {
		t.Fatalf("follow-up submit error = %+v, want refresh to publish", followUp.err)
	}
	if got := backend.probes.Load(); got != 3 {
		t.Fatalf("probes after follow-up at same revision = %d, want 3", got)
	}
	server.admissionStateMu.RLock()
	instance = server.admissionInstance
	_, stillPinned = server.admissionUnprobeableBackends["fake"]
	server.admissionStateMu.RUnlock()
	descriptor, ok := instance.descriptor("fake")
	if !ok || !descriptor.fenceable || stillPinned {
		t.Fatalf("follow-up refresh descriptor=%+v stillPinned=%t, want fenceable and unpinned", descriptor, stillPinned)
	}
}

func TestAdmissionSetupCacheRefreshMovedCacheConsumesCandidateRevision(t *testing.T) {
	t.Parallel()
	probeStarted := make(chan struct{}, 2)
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
		started:     probeStarted,
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap probe did not start")
	}

	gate := make(chan struct{})
	backend.setSetupCacheRefreshState("candidate-refresh", nil, gate)
	done := make(chan requestOutcome, 1)
	go func() {
		done <- server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-refresh-moved-cache",
			RequestID:    "request-refresh-moved-cache",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}))
	}()
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("candidate refresh probe did not start")
	}
	backend.setSetupCacheRefreshState("later-refresh", nil, nil)
	close(gate)
	select {
	case outcome := <-done:
		if outcome.err == nil {
			t.Fatalf("submit result = %+v, want pinned-backend rejection while cache moved", outcome.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("candidate refresh submit did not finish")
	}

	server.admissionStateMu.RLock()
	instance := server.admissionInstance
	instance.backendMu.RLock()
	pinned, stillPinned := instance.pinned["fake"]
	instance.backendMu.RUnlock()
	server.admissionStateMu.RUnlock()
	if !stillPinned || pinned.setupCacheFingerprint != "candidate-refresh" || pinned.reprobeInFlight {
		t.Fatalf("pin after moved cache = %+v stillPinned=%t, want consumed candidate revision and no re-probe in flight", pinned, stillPinned)
	}
}

func TestAdmissionSetupCacheRefreshConcurrentSubmittersProbeAtMostOnce(t *testing.T) {
	t.Parallel()
	probeStarted := make(chan struct{}, 2)
	backend := &setupCacheRefreshProbeBackend{
		fakeBackend: newFakeBackend("fake"),
		fingerprint: "before-refresh",
		err:         errors.New("setup cache is stale"),
		started:     probeStarted,
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap probe did not start")
	}

	gate := make(chan struct{})
	backend.setSetupCacheRefreshState("after-refresh", nil, gate)
	firstDone := make(chan requestOutcome, 1)
	go func() {
		firstDone <- server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-refresh-concurrent-first",
			RequestID:    "request-refresh-concurrent-first",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}))
	}()
	select {
	case <-probeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh probe did not start")
	}

	secondDone := make(chan requestOutcome, 1)
	go func() {
		secondDone <- server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-refresh-concurrent-second",
			RequestID:    "request-refresh-concurrent-second",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}))
	}()
	select {
	case outcome := <-secondDone:
		if outcome.err == nil {
			t.Fatalf("concurrent submit result = %+v, want pinned-backend rejection while re-probe runs", outcome.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent submit waited for the in-flight re-probe")
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes while refresh is gated = %d, want 2", got)
	}
	close(gate)
	select {
	case outcome := <-firstDone:
		if outcome.err != nil {
			t.Fatalf("first submit error after refresh = %+v", outcome.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first submit did not finish after refresh probe released")
	}
	if got := backend.probes.Load(); got != 2 {
		t.Fatalf("probes after concurrent refresh = %d, want 2", got)
	}
}

func TestServeBootstrapRecordsCodexSetupSchemaMismatchUnfenceable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	marker := filepath.Join(dir, "version-probe-marker")
	binary := markerCodexCLI(t, marker)
	cachePath := filepath.Join(dir, "setup-probes.json")
	if err := engine.WriteSetupProbeCache(cachePath, engine.SetupProbeCache{Backends: []engine.BackendSetupProbe{{
		Backend:          "codex",
		BinaryPath:       binary,
		Version:          codexcli.MinimumKnownGoodVersion,
		StreamSchema:     "legacy-stream-json",
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}}}); err != nil {
		t.Fatal(err)
	}
	backend := codexcli.New(codexcli.Options{Binary: binary, CachePath: cachePath})
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("version probe marker stat error = %v, want probe to run", err)
	}
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-schema-mismatch",
		RequestID:    "request-codex-schema-mismatch",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if !strings.Contains(outcome.err.Message, `setup cache for codex lacks stream schema "codex-appserver-v1"`) {
		t.Fatalf("rejection message = %q, want setup schema mismatch cause", outcome.err.Message)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestServeBootstrapRecordsLiveParentContextProbeFailureUnfenceable(t *testing.T) {
	t.Parallel()
	backend := &probeErrorBackend{fakeBackend: newFakeBackend("fake"), err: context.DeadlineExceeded}
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-live-context-probe",
		RequestID:    "request-live-context-probe",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if !strings.Contains(outcome.err.Message, "probe strict backend fake failed: context deadline exceeded") {
		t.Fatalf("rejection message = %q, want sanitized probe context failure", outcome.err.Message)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestServeBootstrapSanitizesProbeFailureReason(t *testing.T) {
	t.Parallel()
	hostile := "line1\nline2\t" + strings.Repeat("界", admissionProbeReasonMaxRunes) + "\x00tail"
	backend := &probeErrorBackend{fakeBackend: newFakeBackend("fake"), err: errors.New(hostile)}
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-hostile-probe",
		RequestID:    "request-hostile-probe",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	prefix := "probe strict backend fake failed: "
	reason, ok := strings.CutPrefix(outcome.err.Message, prefix)
	if !ok {
		t.Fatalf("rejection message = %q, want probe failure prefix", outcome.err.Message)
	}
	if strings.ContainsAny(reason, "\x00\n\r\t") {
		t.Fatalf("sanitized probe reason contains control characters: %q", reason)
	}
	if !utf8.ValidString(reason) {
		t.Fatalf("sanitized probe reason is not valid UTF-8: %q", reason)
	}
	if got := utf8.RuneCountInString(reason); got > admissionProbeReasonMaxRunes {
		t.Fatalf("sanitized probe reason runes = %d, want <= %d", got, admissionProbeReasonMaxRunes)
	}
	if !strings.HasSuffix(reason, "...") {
		t.Fatalf("sanitized probe reason = %q, want truncation suffix", reason)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestServeBootstrapParentCanceledMidProbeFailsBootstrap(t *testing.T) {
	t.Parallel()
	backend := &probeErrorBackend{fakeBackend: newFakeBackend("fake"), started: make(chan struct{}, 1)}
	server, _, _ := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	configureTestAdmissionRuntime(t, server, launcher, true)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.bootstrapAdmission(ctx)
	}()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("probe did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("bootstrapAdmission error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrapAdmission did not return after parent cancellation")
	}
	if server.admissionInstance != nil {
		t.Fatal("admission instance was published after canceled probe")
	}
}

func TestServeCancelDuringBootstrapAnchorAdvanceReportsFailStop(t *testing.T) {
	t.Parallel()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	server, _, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		t.Fatal(err)
	}
	anchor := &cancelOnAdvanceAnchor{
		Anchor: anchorStore.Adapter(dbUUID, schemaMajor),
		cancel: cancel,
	}
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchor(anchor), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	configureTestAdmissionRuntime(t, server, launcher, true)
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open after bootstrap fail-stop")
	}

	err = server.Serve(ctx)
	if !errors.Is(err, ErrSafetyFailStopped) {
		t.Fatalf("Serve error = %v, want safety fail-stop", err)
	}
	if !errors.Is(err, authority.ErrFailStopped) {
		t.Fatalf("Serve error = %v, want authority fail-stop", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want original cancellation preserved", err)
	}
	snapshot := admissionAnchorSnapshot(t, anchorStore)
	if snapshot.Phase != "fail_stopped" {
		t.Fatalf("anchor phase = %q, want fail_stopped", snapshot.Phase)
	}
	if !strings.Contains(snapshot.Reason, "anchor advance") || !strings.Contains(snapshot.Reason, context.Canceled.Error()) {
		t.Fatalf("anchor fail-stop reason = %q, want anchor advance cancellation", snapshot.Reason)
	}

	restart := newTestServerAtRoot(t, server.stateRoot, cwd, newFakeBackend("fake"))
	restart.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for persisted fail-stop")
	}
	restartErr := restart.Serve(context.Background())
	if !errors.Is(restartErr, ErrSafetyFailStopped) {
		t.Fatalf("restart Serve error = %v, want safety fail-stop", restartErr)
	}
	if !errors.Is(restartErr, authority.ErrFailStopped) {
		t.Fatalf("restart Serve error = %v, want persisted authority fail-stop", restartErr)
	}
}

func TestIdentifiedSubmitRejectsSessionContractViolationBeforeDurableAccept(t *testing.T) {
	t.Parallel()
	session := &nonOrdinalSession{id: "lying-session"}
	backend := &lyingControlledBackend{fakeBackend: newFakeBackend("fake"), session: session}
	server, root, cwd := newUnstartedTestServer(t, backend)
	runner := &recordingProbeRunner{}
	server.admissionProbeRunner = runner
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("bootstrap probes = %d, want 1", got)
	}
	probeLookups := runner.lookups.Load()
	probeRuns := runner.runs.Load()

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-contract-violation",
		RequestID:    "request-contract-violation",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if !strings.Contains(outcome.err.Message, "backend contract violation") ||
		!strings.Contains(outcome.err.Message, "descriptor claimed controlled-runner") ||
		!strings.Contains(outcome.err.Message, "session lacks ordinal-bound runner capability") {
		t.Fatalf("rejection message = %q, want backend contract violation reason", outcome.err.Message)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want exactly one construction attempt", got)
	}
	if got := session.turns.Load(); got != 0 {
		t.Fatalf("session turns = %d, want 0", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("prepared launches = %d, want 0", got)
	}
	if got := runner.lookups.Load(); got != probeLookups {
		t.Fatalf("probe lookups after request = %d, want unchanged %d", got, probeLookups)
	}
	if got := runner.runs.Load(); got != probeRuns {
		t.Fatalf("probe runs after request = %d, want unchanged %d", got, probeRuns)
	}
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("probe calls after request = %d, want bootstrap-only", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoAuthoritySafetyRecords(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestIdentifiedSubmitRejectsInvalidBackendSessionIDBeforeDurableAccept(t *testing.T) {
	t.Parallel()
	backend := &invalidSessionIDBackend{fakeBackend: newFakeBackend("fake"), sessionID: "bad id"}
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-invalid-backend-session",
		RequestID:    "request-invalid-backend-session",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want backend metadata rejection", outcome.result)
	}
	if outcome.after != nil {
		t.Fatal("invalid backend session returned a launch hook")
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorBackendUnavailable)
	if resp.Error.Data.AdmissionCause != "" {
		t.Fatalf("admission cause = %q, want none for backend metadata failure", resp.Error.Data.AdmissionCause)
	}
	if resp.Error.Data.Backend != "fake" || resp.Error.Data.SessionID != "bad id" {
		t.Fatalf("error data = %+v, want backend fake session bad id", resp.Error.Data)
	}
	if !strings.Contains(outcome.err.Message, "backend returned invalid session id") {
		t.Fatalf("error message = %q, want backend session id defect", outcome.err.Message)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want one construction attempt", got)
	}
	select {
	case turn := <-backend.turns:
		t.Fatalf("backend turn = %+v, want none before durable accept", turn)
	default:
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoAuthoritySafetyRecords(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)

	malformed := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-malformed-client",
		RequestID:    "request-malformed-client",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false},
	}))
	if malformed.err == nil {
		t.Fatalf("malformed client submit result = %+v, want invalid_task_spec", malformed.result)
	}
	malformedResp := protocol.Response{Error: malformed.err}
	assertRPCCode(t, malformedResp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, malformedResp, protocol.AdmissionRejectInvalidStrictConfig)
	assertNoAuthoritySafetyRecords(t, server)
}

// Pins the REAL controlled-backend contract: CLI-adapter sessions have no id
// at Start time (the backend stream assigns it during the first turn), so an
// empty Session.ID() at submit MUST be accepted with a served-generated
// admission session id — never rejected as a backend metadata defect.
// Acceptance alone does NOT prove the fallback engaged (projection metadata
// treats the session id as an optional token, so an empty id would persist
// silently); the durable projection is asserted to carry the generated
// ses_-form id explicitly.
func TestIdentifiedSubmitAcceptsEmptyBackendSessionIDWithGeneratedID(t *testing.T) {
	t.Parallel()
	backend := &invalidSessionIDBackend{fakeBackend: newFakeBackend("fake"), sessionID: ""}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-empty-backend-session",
		RequestID:    "request-empty-backend-session",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err != nil {
		t.Fatalf("submit with empty backend session id rejected: %+v", outcome.err)
	}
	if outcome.result == nil {
		t.Fatal("submit result missing")
	}
	result, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("submit result type = %T, want JobSubmitResult", outcome.result)
	}
	if result.JobID == "" {
		t.Fatal("accepted job id empty")
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want one", got)
	}
	// The proving assertion: the durable authority projection must carry the
	// served-GENERATED session id — an empty id would validate (optional token)
	// and persist silently if the fallback were removed.
	_, projection, ok, errObj := server.authorityJobProjection(result.JobID)
	if errObj != nil || !ok {
		t.Fatalf("authority projection ok=%v err=%+v", ok, errObj)
	}
	if projection.SessionID == "" {
		t.Fatal("projection session id empty: generated-id fallback did not engage")
	}
	if !strings.HasPrefix(projection.SessionID, "ses_") {
		t.Fatalf("projection session id = %q, want served-generated ses_ form", projection.SessionID)
	}
}

func TestIdentifiedSubmitClosedAdmissionRepositoryReturnsStrictRouteNotReady(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if server.admissionClose == nil {
		t.Fatal("admission repository closer is nil")
	}
	if err := server.admissionClose.Close(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.closeServeAdmission() })

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-closed-repo",
		RequestID:    "request-closed-repo",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want closed-repository rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnavailableNativeRuntime)
	if outcome.err.Data.Code == protocol.ErrorInvalidTaskSpec {
		t.Fatalf("closed repository mapped to invalid_task_spec: %+v", outcome.err)
	}
}

func TestIdentifiedSubmitStorageFailureReturnsBackendUnavailable(t *testing.T) {
	t.Parallel()
	repo := memory.NewRepository()
	failingRepo := &failingUpdateRepository{inner: repo}
	anchorStore := authority.NewAnchorStore()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(failingRepo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, failingRepo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	enableTestAdmission(t, server, launcher)
	failingRepo.err = fmt.Errorf("repository update failed: %w", syscall.EIO)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-storage-failure",
		RequestID:    "request-storage-failure",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want storage failure rejection", outcome.result)
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorBackendUnavailable)
	if outcome.err.Data.Code == protocol.ErrorInvalidTaskSpec {
		t.Fatalf("storage failure mapped to invalid_task_spec: %+v", outcome.err)
	}
}

func TestIdentifiedFencedSubmitUsesLaunchControllerAndCompletes(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-identified",
		RequestID:    "request-identified",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v", submitted)
	}
	waitBackendStarted(t, backend)

	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend sessions = %d, want 1", got)
	}
	if got := launcher.preparedOrdinals(); len(got) != 1 || got[0] != model.LaunchOrdinalOne {
		t.Fatalf("prepared ordinals = %v, want [1]", got)
	}
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Mode != model.ModeIdentifiedFenced {
		t.Fatalf("admission mode = %s, want IdentifiedFenced", record.Mode)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Grant == nil || first.Released == nil || first.Quiescence == nil {
		t.Fatalf("ordinal 1 launch proof incomplete: %+v", first)
	}
}

func TestIdentifiedSubmitReportsAndPersistsResolvedTimeout(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name          string
		timeout       *int64
		wantRequested *int64
		wantEffective int64
		wantSource    string
	}{
		{
			name:          "explicit client timeout",
			timeout:       int64Pointer(45_000),
			wantRequested: int64Pointer(45_000),
			wantEffective: 45_000,
			wantSource:    engine.TimeoutSourceClient,
		},
		{
			name:          "omitted daemon default",
			wantEffective: protocol.DefaultTimeout.Milliseconds(),
			wantSource:    engine.TimeoutSourceDaemonDefault,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := newFakeBackend("fake")
			server, _, cwd := newUnstartedTestServer(t, backend)
			enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

			submitted := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-timeout-" + strings.ReplaceAll(tt.name, " ", "-"),
				RequestID:    "request-timeout-" + strings.ReplaceAll(tt.name, " ", "-"),
				TaskSpec: protocol.TaskSpec{
					Backend:   "fake",
					CWD:       cwd,
					Write:     false,
					Prompt:    "report timeout",
					TimeoutMs: tt.timeout,
				},
			})
			assertTimeoutResolution(t, submitted.Timeout, tt.wantRequested, tt.wantEffective, tt.wantSource)

			record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
			assertTimeoutResolution(t, record.Timeout, tt.wantRequested, tt.wantEffective, tt.wantSource)

			status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: submitted.JobID})
			if len(status.Jobs) != 1 {
				t.Fatalf("job.status = %+v, want one job", status)
			}
			assertTimeoutResolution(t, status.Jobs[0].Timeout, tt.wantRequested, tt.wantEffective, tt.wantSource)

			result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
			assertTimeoutResolution(t, result.Timeout, tt.wantRequested, tt.wantEffective, tt.wantSource)
		})
	}
}

func TestIdentifiedSubmitRejectsOverflowTimeoutBeforeDurableAccept(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, root, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	overflow := int64(9223372036854775807)

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-overflow-timeout",
		RequestID:    "request-overflow-timeout",
		TaskSpec: protocol.TaskSpec{
			Backend:   "fake",
			CWD:       cwd,
			Write:     false,
			Prompt:    "reject overflow timeout",
			TimeoutMs: &overflow,
		},
	}))
	if outcome.err == nil {
		t.Fatalf("submit result = %+v, want timeout rejection", outcome.result)
	}
	if outcome.after != nil {
		t.Fatal("overflow timeout returned a launch hook")
	}
	resp := protocol.Response{Error: outcome.err}
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectInvalidStrictConfig)
	if !strings.Contains(outcome.err.Message, "timeoutMs exceeds maximum") {
		t.Fatalf("rejection message = %q, want maximum timeout error", outcome.err.Message)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoAuthoritySafetyRecords(t, server)
	assertNoWorkspaceNamespaceForCWD(t, root, cwd)
}

func TestIdentifiedSubmitRejectsNoControlledRunnerBeforeBackendStart(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.controlled = false
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-no-controlled-runner",
		RequestID:    "request-no-controlled-runner",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before controlled-runner pre-accept reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
}

func TestIdentifiedSubmitRejectsNamedBackendWithoutProbeOrRunnerCapabilitiesBeforeStart(t *testing.T) {
	t.Parallel()
	backend := &unsafeNamedBackend{name: "codex"}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-no-capabilities",
		RequestID:    "request-codex-no-capabilities",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)
	if got := backend.starts.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before unfenceable pre-accept reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestStrictIdentifiedRejectsExternalRunnerWithoutLegacyFencedFallback(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.parkable = false
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-legacy-fenced",
		RequestID:    "request-legacy-fenced",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)

	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before strict unfenceable reject", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("prepared launches = %d, want 0 before strict unfenceable reject", got)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after strict unfenceable reject", got)
	}
	if got := launcher.abortCount(); got != 0 {
		t.Fatalf("legacy fenced aborts = %d, want 0 after strict unfenceable reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestStrictIdentifiedUnfenceableRejectionDoesNotCreateLegacyRecordWhenResponseLost(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.parkable = false
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-legacy-fenced-loss",
		RequestID:    "request-legacy-fenced-loss",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, errors.New("ack write failed"))

	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before strict unfenceable reject", got)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after strict unfenceable reject", got)
	}
	if got := launcher.abortCount(); got != 0 {
		t.Fatalf("legacy fenced aborts = %d, want 0 after strict unfenceable reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestStrictIdentifiedRejectsExternalRunnerWithoutLegacyUnfencedFallback(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.parkable = false
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-legacy-unfenced",
		RequestID:    "request-legacy-unfenced",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnfenceableBackend)

	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before strict unfenceable reject", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("legacy unfenced prepared launches = %d, want 0", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestStrictIdentifiedUnfenceableDeliveryFailureDoesNotCreateLegacyUnfencedRecord(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.parkable = false
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-legacy-unfenced-loss",
		RequestID:    "request-legacy-unfenced-loss",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, errors.New("ack write failed"))

	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 after failed response", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("legacy unfenced prepared launches = %d, want 0", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
	assertNoEngineJobRecordsForCWD(t, server, cwd)
}

func TestIdentifiedFencedResponseLossRetainsObligationAndReplayReturnsSameJob(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-response-loss",
		RequestID:    "request-response-loss",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}

	serveScriptedRequest(t, server, protocol.MethodJobSubmit, params, errors.New("ack write failed"))
	waitBackendStarted(t, backend)
	accepted := waitSingleAdmissionSafetyTerminal(t, server)
	if accepted.Terminal == nil || accepted.Terminal.Outcome == model.OutcomeCanceled {
		t.Fatal("response-loss canceled an accepted identified fenced job")
	}

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, params, nil)
	resp := responseFromScriptedConn(t, conn)
	var replay protocol.JobSubmitResult
	decodeResult(t, resp, &replay)
	if replay.JobID != accepted.JobID.String() || !replay.Deduplicated {
		t.Fatalf("replay result = %+v, want same job %s deduplicated", replay, accepted.JobID)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend sessions = %d, want exactly once", got)
	}
	if got := len(launcher.preparedOrdinals()); got != 1 {
		t.Fatalf("launch prepare calls = %d, want exactly once", got)
	}
}

func TestIdentifiedFencedJobReadsAuthorityOnlyWithoutJSONFallback(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-reads",
		RequestID:    "request-authority-reads",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, params, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v", submitted)
	}
	assertNoJSONJobRecord(t, server, submitted.JobID)
	waitBackendStarted(t, backend)

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: submitted.JobID})
	if len(status.Jobs) != 1 || status.Jobs[0].JobID != submitted.JobID {
		t.Fatalf("authority status = %+v, want one submitted job", status)
	}
	assertNoJSONJobRecord(t, server, submitted.JobID)
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if result.JobID != submitted.JobID || result.State != engine.StateCompleted || result.Result == nil || result.Result.ResultPath == "" {
		t.Fatalf("authority result = %+v, want completed result receipt", result)
	}

	store := server.storeForJob(submitted.JobID)
	if store == nil {
		t.Fatalf("job store missing for %s", submitted.JobID)
	}
	if err := server.createQueuedRecord(store, submitted.JobID, "ses_stale_json", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	clearAdmissionJobMarkersForTest(t, server)
	staleExactStatus := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: submitted.JobID})
	if len(staleExactStatus.Jobs) != 1 || staleExactStatus.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("stale duplicate exact status = %+v, want authority completed with admission marker cleared", staleExactStatus)
	}
	staleExactResult := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if staleExactResult.JobID != submitted.JobID || staleExactResult.State != engine.StateCompleted || staleExactResult.Result == nil || staleExactResult.Result.ResultPath == "" {
		t.Fatalf("stale duplicate exact result = %+v, want authority completed result with admission marker cleared", staleExactResult)
	}

	legacyID := server.nextID("job")
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_json", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	assertJobHandlerError(t, server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{JobID: legacyID})), protocol.ErrorUnknownJob, "", legacyID)
	assertJobHandlerError(t, server.handleJobResult(mustMarshal(t, protocol.JobResultParams{JobID: legacyID})), protocol.ErrorUnknownJob, "", legacyID)

	all := jobStatusViaHandler(t, server, protocol.JobStatusParams{All: true})
	byID := map[string]protocol.JobStatus{}
	for _, job := range all.Jobs {
		if _, exists := byID[job.JobID]; exists {
			t.Fatalf("duplicate all-status row for %s in %+v", job.JobID, all)
		}
		byID[job.JobID] = job
	}
	if got, ok := byID[submitted.JobID]; !ok || got.State != engine.StateCompleted || len(got.Warnings) != 0 {
		t.Fatalf("authority status = %+v ok=%t, want completed without JSON duplicate warning", got, ok)
	}
	if got, ok := byID[legacyID]; ok {
		t.Fatalf("legacy JSON all-status row = %+v, want omitted", got)
	}
}

func TestStrictAuthorityResultSurfacesBackendReportedModel(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name     string
		reported string
	}{
		{name: "reported", reported: "claude-opus-4-1-20250805"},
		{name: "not_reported"},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := newFakeBackend("fake")
			backend.events = func(string, bool) []engine.Event {
				events := []engine.Event{}
				if tt.reported != "" {
					events = append(events, engine.Event{Type: engine.EventModelReported, ModelReported: tt.reported})
				}
				return append(events, engine.Event{Type: engine.EventResultMessage, Text: "final report"})
			}
			server, _, cwd := newUnstartedTestServer(t, backend)
			launcher := newAdmissionFakeLaunchCustodian(t)
			enableTestAdmission(t, server, launcher)

			job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-model-reported-" + tt.name,
				RequestID:    "request-model-reported-" + tt.name,
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "report model"},
			})
			waitAdmissionSafetyTerminal(t, server, job.JobID)
			result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
			if result.ModelReported != tt.reported {
				t.Fatalf("job.result modelReported = %q, want %q", result.ModelReported, tt.reported)
			}
			if result.Result == nil || result.Result.Text != "final report" || result.Result.ModelReported != tt.reported {
				t.Fatalf("job.result payload = %+v, want text and modelReported %q", result.Result, tt.reported)
			}
			status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
			if len(status.Jobs) != 1 || status.Jobs[0].ModelReported != tt.reported {
				t.Fatalf("job.status = %+v, want modelReported %q", status, tt.reported)
			}
			// The all-jobs list projection must also surface the reported model
			// (guards the separate listAuthorityStatuses assignment).
			list := jobStatusViaHandler(t, server, protocol.JobStatusParams{All: true})
			found := false
			for _, js := range list.Jobs {
				if js.JobID != job.JobID {
					continue
				}
				found = true
				if js.ModelReported != tt.reported {
					t.Fatalf("job.status All=true modelReported = %q, want %q", js.ModelReported, tt.reported)
				}
			}
			if !found {
				t.Fatalf("job.status All=true did not include job %s", job.JobID)
			}
		})
	}
}

func TestStrictAuthorityStatusSurfacesRuntimeLivenessOnlyWhileRunning(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	clock := &steppedClock{now: base}
	backend := newAdmissionLivenessBackend("fake")
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	server := newTestServerWithRoot(t, root, cwd, backend, Config{
		Clock:        clock,
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
	})
	releaseRunner := make(chan struct{})
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = releaseRunner
	enableTestAdmission(t, server, launcher)
	releaseEvents := sync.OnceFunc(func() { close(backend.eventStream) })
	t.Cleanup(releaseEvents)
	releaseRunnerOnce := sync.OnceFunc(func() { close(releaseRunner) })
	t.Cleanup(releaseRunnerOnce)

	submitted := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-liveness-status",
		RequestID:    "request-liveness-status",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold for status"},
	})
	waitBackendStarted(t, backend.fakeBackend)
	clock.Set(base.Add(2 * time.Second))
	select {
	case backend.eventStream <- engine.Event{Type: engine.EventAgentText, Text: "PASS\n\n## Findings\nNone.\n"}:
	case <-time.After(5 * time.Second):
		t.Fatal("admission liveness event was not received")
	}

	var running protocol.JobStatus
	var lastRunningStatus protocol.JobStatusResult
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: submitted.JobID})
		lastRunningStatus = status
		if len(status.Jobs) == 1 &&
			status.Jobs[0].State == engine.StateRunning &&
			status.Jobs[0].StartedAt != nil &&
			status.Jobs[0].HeartbeatAt != nil &&
			status.Jobs[0].UpdatedAt != nil &&
			// Wait until the recorded event has advanced the heartbeat past the
			// start stamp, so the assertions below deterministically prove event
			// recording actually ran (not just the first-write start stamp).
			status.Jobs[0].HeartbeatAt.After(*status.Jobs[0].StartedAt) {
			running = status.Jobs[0]
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if running.JobID == "" {
		t.Fatalf("running admission status did not expose liveness; last status = %+v", lastRunningStatus)
	}
	// Discriminating exact-time assertions on the stepped clock: startedAt is
	// stamped at attempt start (base), and the recorded event advanced the
	// heartbeat to base+2s. If recordJobLivenessEvent were removed or misplaced,
	// heartbeatAt would stay at base and this would fail.
	if !running.StartedAt.Equal(base) {
		t.Fatalf("startedAt = %s, want %s", running.StartedAt, base)
	}
	if !running.HeartbeatAt.Equal(base.Add(2 * time.Second)) {
		t.Fatalf("heartbeatAt = %s, want %s (advanced by the recorded event)", running.HeartbeatAt, base.Add(2*time.Second))
	}
	if !running.UpdatedAt.Equal(*running.HeartbeatAt) {
		t.Fatalf("updatedAt = %s, want heartbeatAt %s", running.UpdatedAt, running.HeartbeatAt)
	}
	if running.FinalAttemptStartedAt != nil || running.FinalAttemptEndedAt != nil {
		t.Fatalf("running terminal timing = start:%v end:%v, want absent", running.FinalAttemptStartedAt, running.FinalAttemptEndedAt)
	}
	runningRecord := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	if runningRecord.FinalAttemptStartedAt == nil || !runningRecord.FinalAttemptStartedAt.Equal(base) || runningRecord.FinalAttemptEndedAt != nil {
		t.Fatalf("running durable timing = start:%v end:%v, want %s/<nil>", runningRecord.FinalAttemptStartedAt, runningRecord.FinalAttemptEndedAt, base)
	}

	all := jobStatusViaHandler(t, server, protocol.JobStatusParams{All: true})
	var listed protocol.JobStatus
	for _, status := range all.Jobs {
		if status.JobID == submitted.JobID {
			listed = status
			break
		}
	}
	if listed.JobID == "" {
		t.Fatalf("job.status All=true did not include job %s", submitted.JobID)
	}
	if listed.State != engine.StateRunning || listed.StartedAt == nil || listed.HeartbeatAt == nil || listed.UpdatedAt == nil {
		t.Fatalf("job.status All=true liveness = %+v, want running status with liveness", listed)
	}
	if listed.HeartbeatAt.Before(*listed.StartedAt) {
		t.Fatalf("All=true heartbeatAt = %s, want >= startedAt %s", listed.HeartbeatAt, listed.StartedAt)
	}
	if listed.FinalAttemptStartedAt != nil || listed.FinalAttemptEndedAt != nil {
		t.Fatalf("All=true running terminal timing = start:%v end:%v, want absent", listed.FinalAttemptStartedAt, listed.FinalAttemptEndedAt)
	}

	releaseRunnerOnce()
	select {
	case <-backend.runnerObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("admission runner observation did not finish")
	}
	releaseEvents()
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	waitActiveJobGone(t, server, submitted.JobID)
	terminal := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: submitted.JobID})
	if len(terminal.Jobs) != 1 || terminal.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("terminal status = %+v, want completed job", terminal)
	}
	if terminal.Jobs[0].StartedAt != nil || terminal.Jobs[0].HeartbeatAt != nil || terminal.Jobs[0].UpdatedAt != nil {
		t.Fatalf("terminal status liveness = %+v, want nil liveness fields after active-job eviction", terminal.Jobs[0])
	}
	if terminal.Jobs[0].FinalAttemptStartedAt == nil || !terminal.Jobs[0].FinalAttemptStartedAt.Equal(base) || terminal.Jobs[0].FinalAttemptEndedAt == nil || !terminal.Jobs[0].FinalAttemptEndedAt.Equal(base.Add(2*time.Second)) {
		t.Fatalf("terminal status timing = start:%v end:%v, want %s/%s", terminal.Jobs[0].FinalAttemptStartedAt, terminal.Jobs[0].FinalAttemptEndedAt, base, base.Add(2*time.Second))
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if result.FinalAttemptStartedAt == nil || !result.FinalAttemptStartedAt.Equal(base) || result.FinalAttemptEndedAt == nil || !result.FinalAttemptEndedAt.Equal(base.Add(2*time.Second)) {
		t.Fatalf("terminal result timing = start:%v end:%v, want %s/%s", result.FinalAttemptStartedAt, result.FinalAttemptEndedAt, base, base.Add(2*time.Second))
	}
}

// TODO(E5A/E5B): Add submit-time named-policy authority-record coverage once
// model.SafetyRecord carries policy/contract fields; today it only records the
// task identity, so there is no authority-owned named-policy field to assert.

func TestAuthorityResultHidesCertifiedResultUntilTerminalRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	accepted := acceptIdentifiedAuthorityWork(t, server, "authority-result-terminal-only")
	jobID := accepted.Record.JobID
	ref := accepted.Record.Attempt.Ref
	ordinal := model.LaunchOrdinalOne
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: ordinal})
	if _, err := server.admissionReady.BindGroup(ctx, jobID, ref, ordinal, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.admissionReady.AllocateGrant(ctx, ref, ordinal); err != nil {
		t.Fatal(err)
	}
	child, err := model.NewChildIdentity(5202, "terminal-only-result-child")
	if err != nil {
		t.Fatal(err)
	}
	releaseEvidence, err := model.NewEvidence("released", "terminal-only result release")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.RecordRelease(ctx, jobID, ref, ordinal, child, releaseEvidence); err != nil {
		t.Fatal(err)
	}
	verified, err := launcher.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: model.QuiescenceAlreadyAbsent})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.RecordQuiescence(ctx, jobID, ordinal, verified); err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.RecordOutcome(ctx, jobID, ref, model.OutcomeCompleted, nil); err != nil {
		t.Fatal(err)
	}
	dirSynced, err := model.NewEvidence("dir_synced", "terminal-only result directory fsynced")
	if err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(server.stateRoot, "results", jobID.String()+".txt")
	receipt := model.ResultReceipt{
		JobID: jobID,
		Result: model.ResultRef{
			Path:   resultPath,
			Digest: "terminalonlydigest",
			Bytes:  4,
		},
		DirSynced:   dirSynced,
		CertifiedBy: accepted.Record.AdmittedBy,
	}
	if _, err := server.admissionReady.RecordResult(ctx, jobID, ref, receipt); err != nil {
		t.Fatal(err)
	}
	preTerminal := loadAdmissionSafetyRecord(t, server, jobID.String())
	if preTerminal.Result == nil || preTerminal.Terminal != nil {
		t.Fatalf("pre-terminal authority record = %+v, want result certificate without terminal", preTerminal)
	}
	publicPreTerminal := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: jobID.String()})
	if publicPreTerminal.Result != nil {
		t.Fatalf("pre-terminal job.result = %+v, want no public result content", publicPreTerminal)
	}
	if publicPreTerminal.FinalAttemptStartedAt != nil || publicPreTerminal.FinalAttemptEndedAt != nil {
		t.Fatalf("pre-terminal timing = start:%v end:%v, want absent", publicPreTerminal.FinalAttemptStartedAt, publicPreTerminal.FinalAttemptEndedAt)
	}

	if _, err := server.admissionReady.Finalize(ctx, jobID, ref, model.TerminalIntent{
		Outcome: model.OutcomeCompleted,
		Cause:   model.CauseCompletedNormally,
	}); err != nil {
		t.Fatal(err)
	}
	publicTerminal := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: jobID.String()})
	if publicTerminal.Result == nil || publicTerminal.Result.ResultPath != resultPath || publicTerminal.Result.SHA256 != receipt.Result.Digest || publicTerminal.Result.Bytes != receipt.Result.Bytes {
		t.Fatalf("terminal job.result = %+v, want terminal-derived result metadata", publicTerminal)
	}
	if publicTerminal.FinalAttemptStartedAt != nil || publicTerminal.FinalAttemptEndedAt != nil {
		t.Fatalf("legacy terminal timing = start:%v end:%v, want absent", publicTerminal.FinalAttemptStartedAt, publicTerminal.FinalAttemptEndedAt)
	}
	terminalStatus := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: jobID.String()})
	if len(terminalStatus.Jobs) != 1 || terminalStatus.Jobs[0].FinalAttemptStartedAt != nil || terminalStatus.Jobs[0].FinalAttemptEndedAt != nil {
		t.Fatalf("legacy terminal status = %+v, want served status without timing", terminalStatus)
	}
}

func TestIdentifiedFencedResultPublishUsesDurableWorkspaceLayoutKeyWithoutJobStore(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(holdNaturalExit) })
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, root, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)

	requestWorkspaceKey := "workspace-identified"
	if _, err := engine.LayoutForWorkspaceKey(root, requestWorkspaceKey); err == nil {
		t.Fatalf("request workspace key %q unexpectedly accepted as engine layout key", requestWorkspaceKey)
	}
	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: requestWorkspaceKey,
		RequestID:    "request-authority-result-publish",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v", submitted)
	}
	waitBackendStarted(t, backend)

	initialStore := server.storeForJob(submitted.JobID)
	if initialStore == nil {
		t.Fatalf("job store missing before simulated restart for %s", submitted.JobID)
	}
	if _, err := initialStore.Load(submitted.JobID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("JSON job record load error = %v, want not exist", err)
	}
	server.mu.Lock()
	server.jobStores = make(map[string]*engine.Store)
	server.mu.Unlock()
	if store := server.storeForJob(submitted.JobID); store != nil {
		t.Fatalf("storeForJob(%s) = %+v after clearing jobStores with no JSON record, want nil", submitted.JobID, store.Layout())
	}

	release.Do(func() { close(holdNaturalExit) })
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if got := record.RequestKey.WorkspaceKey.String(); got != requestWorkspaceKey {
		t.Fatalf("record request workspace key = %q, want %q", got, requestWorkspaceKey)
	}
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	wantLayoutKey := engine.WorkspaceKey(canonicalCWD)
	if got := record.WorkspaceLayoutKey.String(); got != wantLayoutKey {
		t.Fatalf("record workspace layout key = %q, want %q", got, wantLayoutKey)
	}
	if record.WorkspaceLayoutKey.String() == record.RequestKey.WorkspaceKey.String() {
		t.Fatalf("workspace layout key conflated with request workspace key %q", record.RequestKey.WorkspaceKey)
	}
	layout, err := engine.LayoutForWorkspaceKey(root, record.WorkspaceLayoutKey.String())
	if err != nil {
		t.Fatal(err)
	}
	expectedPath, err := engine.ResultPathForLayout(layout, submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !servedPathWithinDir(layout.Results, expectedPath) {
		t.Fatalf("expected result path %q outside results root %q", expectedPath, layout.Results)
	}
	if record.Result == nil || record.Result.Result.Path != expectedPath {
		t.Fatalf("record result = %+v, want path %q", record.Result, expectedPath)
	}
	if record.Terminal == nil || record.Terminal.Result == nil || record.Terminal.Result.Path != expectedPath {
		t.Fatalf("terminal result = %+v, want path %q", record.Terminal, expectedPath)
	}
	raw, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "PASS\n\n## Findings\nNone.\n" {
		t.Fatalf("result artifact = %q, want backend final text", string(raw))
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if result.JobID != submitted.JobID || result.State != engine.StateCompleted || result.Result == nil || result.Result.ResultPath != expectedPath || result.Result.Text != "PASS\n\n## Findings\nNone.\n" {
		t.Fatalf("authority result = %+v, want completed result at %q with inline backend final text", result, expectedPath)
	}
	server.mu.Lock()
	server.jobStores = make(map[string]*engine.Store)
	server.mu.Unlock()
	again := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if again.Result == nil || again.Result.ResultPath != expectedPath || again.Result.Text != "PASS\n\n## Findings\nNone.\n" {
		t.Fatalf("authority result after clearing jobStores again = %+v, want path %q with inline backend final text", again, expectedPath)
	}

	outsidePath := filepath.Join(root, "outside-results", submitted.JobID+".txt")
	if err := os.MkdirAll(filepath.Dir(outsidePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outsidePath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = (servedResultPublisher{server: server}).Verify(context.Background(), model.ResultRef{
		Path:   outsidePath,
		Digest: record.Result.Result.Digest,
		Bytes:  record.Result.Result.Bytes,
	})
	if err == nil || !strings.Contains(err.Error(), "escapes results root") {
		t.Fatalf("Verify outside results root error = %v, want path escape rejection", err)
	}

	// Negative hydration cases: post-certification tampering must omit inline
	// text (path/digest metadata stays authoritative) and must never read more
	// than the inline cap allows.
	// 1. Digest mismatch: same length, different content.
	tampered := []byte(strings.Repeat("x", len(raw)))
	if err := os.WriteFile(expectedPath, tampered, 0o600); err != nil {
		t.Fatal(err)
	}
	mismatch := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if mismatch.Result == nil || mismatch.Result.Text != "" || mismatch.Result.ResultPath != expectedPath {
		t.Fatalf("digest-mismatch result = %+v, want omitted inline text with authoritative path", mismatch)
	}
	// 2. Oversized replacement: certified-small artifact replaced by a huge
	// file — hydration must omit text (and its read is bounded by inlineCap+1).
	oversized := make([]byte, engine.DefaultInlineResultCap+4096)
	if err := os.WriteFile(expectedPath, oversized, 0o600); err != nil {
		t.Fatal(err)
	}
	grown := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if grown.Result == nil || grown.Result.Text != "" {
		t.Fatalf("oversized-replacement result = %+v, want omitted inline text", grown)
	}
	// 3. Read failure: artifact removed — text omitted, metadata retained.
	if err := os.Remove(expectedPath); err != nil {
		t.Fatal(err)
	}
	missing := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if missing.Result == nil || missing.Result.Text != "" || missing.Result.ResultPath != expectedPath {
		t.Fatalf("missing-artifact result = %+v, want omitted inline text with authoritative path", missing)
	}
}

func TestStrictCodexElidedResultRetainsCertifiedResultRef(t *testing.T) {
	t.Parallel()
	large := strings.Repeat("strict codex result\n", 16)
	backend := newFakeBackend("codex")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventResultMessage, Text: large}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.inlineResultCap = 32
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-strict-codex-elided-result",
		RequestID:    "request-strict-codex-elided-result",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "large result"},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Result == nil {
		t.Fatal("SafetyRecord.Result is nil, want certified result")
	}
	if record.Terminal == nil || record.Terminal.Result == nil {
		t.Fatalf("terminal = %+v, want terminal result receipt", record.Terminal)
	}
	if *record.Terminal.Result != record.Result.Result {
		t.Fatalf("terminal result = %+v, want certified receipt %+v", *record.Terminal.Result, record.Result.Result)
	}
	if _, err := os.Stat(record.Result.Result.Path); err != nil {
		t.Fatalf("workspace result file stat error = %v", err)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.Result == nil {
		t.Fatal("served job.result Result is nil, want elided ResultRef")
	}
	if result.Result.ResultPath != record.Result.Result.Path ||
		result.Result.SHA256 != record.Result.Result.Digest ||
		result.Result.Bytes != record.Result.Result.Bytes {
		t.Fatalf("served result = %+v, want certified %+v", result.Result, record.Result.Result)
	}
	if !result.Result.TextElided || result.Result.Text != "" {
		t.Fatalf("served result text = %q elided=%t, want empty elided text", result.Result.Text, result.Result.TextElided)
	}
}

func TestIdentifiedCodexJobUsesPrivateHomeWithLinkedCredentials(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("codex")
	block := make(chan struct{})
	backend.block = block
	started := make(chan engine.SessionOpts, 1)
	backend.startHook = func(opts engine.SessionOpts) { started <- opts }
	server, root, cwd := newUnstartedTestServer(t, backend)
	authHome := filepath.Join(t.TempDir(), "operator-codex")
	for name, content := range map[string]string{"auth.json": `{"token":"test"}`, "config.toml": "model = \"test\"\n"} {
		if err := os.MkdirAll(authHome, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(authHome, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	server.codexAuthHome = authHome
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-private-home",
		RequestID:    "request-codex-private-home",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
	})
	var opts engine.SessionOpts
	select {
	case opts = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Codex session construction did not run")
	}
	wantHome := testCodexHomePath(t, root, cwd, job.JobID)
	if got := opts.EnvOverlay["CODEX_HOME"]; got != wantHome {
		t.Fatalf("CODEX_HOME overlay = %q, want %q", got, wantHome)
	}
	if info, err := os.Stat(wantHome); err != nil {
		t.Fatalf("private Codex home stat = %v", err)
	} else if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("private Codex home mode = %o, want 700", got)
	}
	for _, name := range codexHomeLinkedFiles {
		path := filepath.Join(wantHome, name)
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("%s lstat = %v", name, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("%s mode = %v, want symlink", name, info.Mode())
		}
		if target, err := os.Readlink(path); err != nil || target != filepath.Join(authHome, name) {
			t.Fatalf("%s target = %q err=%v, want %q", name, target, err, filepath.Join(authHome, name))
		}
	}
	close(block)
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	waitActiveJobGone(t, server, job.JobID)
	if _, err := os.Stat(wantHome); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed private Codex home stat = %v, want removed", err)
	}
}

func TestIdentifiedCodexJobSkipsMissingCredentialLinks(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("codex")
	block := make(chan struct{})
	backend.block = block
	started := make(chan engine.SessionOpts, 1)
	backend.startHook = func(opts engine.SessionOpts) { started <- opts }
	server, root, cwd := newUnstartedTestServer(t, backend)
	server.codexAuthHome = filepath.Join(t.TempDir(), "empty-codex-home")
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-missing-links",
		RequestID:    "request-codex-missing-links",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Codex session construction did not run")
	}
	home := testCodexHomePath(t, root, cwd, job.JobID)
	for _, name := range codexHomeLinkedFiles {
		if _, err := os.Lstat(filepath.Join(home, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing source produced %s: %v", name, err)
		}
	}
	close(block)
	waitAdmissionSafetyTerminal(t, server, job.JobID)
}

func TestIdentifiedCodexJobRetainsPrivateHomeAfterFailure(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("codex")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventTerminalError, Text: "backend failed"}}
	}
	server, root, cwd := newUnstartedTestServer(t, backend)
	server.codexAuthHome = filepath.Join(t.TempDir(), "missing-codex-home")
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-failure-retained",
		RequestID:    "request-codex-failure-retained",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "fail"},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed {
		t.Fatalf("terminal = %+v, want failed", record.Terminal)
	}
	home := testCodexHomePath(t, root, cwd, job.JobID)
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("failed job private Codex home stat = %v, want retained", err)
	}
}

func TestIdentifiedCodexPreparationFailureFinalizesAndRetainsPrivateHome(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("codex")
	server, root, cwd := newUnstartedTestServer(t, backend)
	authHome := filepath.Join(t.TempDir(), "operator-codex")
	if err := os.MkdirAll(filepath.Join(authHome, "auth.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	server.codexAuthHome = authHome
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-codex-preparation-failure",
		RequestID:    "request-codex-preparation-failure",
		TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "prepare fails"},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeFailed {
		t.Fatalf("terminal = %+v, want failed", record.Terminal)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("Codex backend Start calls = %d, want none after home preparation failure", got)
	}
	home := testCodexHomePath(t, root, cwd, job.JobID)
	if _, err := os.Stat(home); err != nil {
		t.Fatalf("preparation-failed private Codex home stat = %v, want retained", err)
	}
}

func TestIdentifiedCodexHomeCleanupUsesCommittedTerminalOutcome(t *testing.T) {
	for _, tt := range []struct {
		name             string
		suffix           string
		events           func(string, bool) []engine.Event
		attemptOutcome   model.Outcome
		committedOutcome model.Outcome
		wantRetained     bool
	}{
		{
			name:             "successful attempt commits failed terminal and retains",
			suffix:           "success-to-failed",
			attemptOutcome:   model.OutcomeCompleted,
			committedOutcome: model.OutcomeFailed,
			wantRetained:     true,
		},
		{
			name:   "failed attempt commits completed terminal and removes",
			suffix: "failed-to-success",
			events: func(string, bool) []engine.Event {
				return []engine.Event{{Type: engine.EventTerminalError, Text: "attempt failed"}}
			},
			attemptOutcome:   model.OutcomeFailed,
			committedOutcome: model.OutcomeCompleted,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("codex")
			if tt.events != nil {
				backend.events = tt.events
			}
			server, root, cwd := newUnstartedTestServer(t, backend)
			server.codexAuthHome = filepath.Join(t.TempDir(), "missing-codex-auth")
			launcher := newAdmissionFakeLaunchCustodian(t)
			enableTestAdmission(t, server, launcher)
			recorder := installRecordingAdmissionAuthorityForTest(t, server)

			var injected sync.Once
			recorder.beforeRecordOutcome = func(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome) error {
				if outcome != tt.attemptOutcome {
					return nil
				}
				var err error
				injected.Do(func() {
					switch tt.committedOutcome {
					case model.OutcomeFailed:
						_, err = recorder.servedAdmissionAuthority.RecordOutcome(ctx, jobID, ref, model.OutcomeFailed, nil)
						if err == nil {
							_, err = recorder.servedAdmissionAuthority.Finalize(ctx, jobID, ref, model.TerminalIntent{Outcome: model.OutcomeFailed, Cause: model.CauseCompletedNormally})
						}
					case model.OutcomeCompleted:
						_, err = recorder.servedAdmissionAuthority.RecordOutcome(ctx, jobID, ref, model.OutcomeCompleted, nil)
						if err != nil {
							return
						}
						publisher := servedResultPublisher{server: server}
						receipt, publishErr := publisher.Publish(ctx, jobID, []byte("committed success"))
						if publishErr != nil {
							err = publishErr
							return
						}
						verified, verifyErr := publisher.Verify(ctx, receipt.Result)
						if verifyErr != nil {
							err = verifyErr
							return
						}
						_, err = recorder.servedAdmissionAuthority.RecordResult(ctx, jobID, ref, verified)
						if err == nil {
							_, err = recorder.servedAdmissionAuthority.Finalize(ctx, jobID, ref, model.TerminalIntent{Outcome: model.OutcomeCompleted, Cause: model.CauseCompletedNormally})
						}
					default:
						err = fmt.Errorf("unsupported committed outcome %s", tt.committedOutcome)
					}
				})
				return err
			}

			job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-codex-committed-cleanup-" + tt.suffix,
				RequestID:    "request-codex-committed-cleanup-" + tt.suffix,
				TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "cleanup follows committed terminal"},
			})
			record := waitAdmissionSafetyTerminal(t, server, job.JobID)
			if record.Terminal == nil || record.Terminal.Outcome != tt.committedOutcome {
				t.Fatalf("terminal = %+v, want committed %s", record.Terminal, tt.committedOutcome)
			}
			waitActiveJobGone(t, server, job.JobID)
			home := testCodexHomePath(t, root, cwd, job.JobID)
			_, err := os.Stat(home)
			if tt.wantRetained && err != nil {
				t.Fatalf("committed failed Codex home stat = %v, want retained", err)
			}
			if !tt.wantRetained && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("committed completed Codex home stat = %v, want removed", err)
			}
		})
	}
}

func TestIdentifiedCodexHomeOverrideAndInheritanceOptOut(t *testing.T) {
	for _, tt := range []struct {
		name        string
		override    string
		inherit     bool
		wantOverlay bool
	}{
		{
			name:        "override",
			override:    filepath.Join(t.TempDir(), "fixed-codex-home"),
			wantOverlay: true,
		},
		{
			name:        "inherit wins over override",
			override:    filepath.Join(t.TempDir(), "ignored-fixed-codex-home"),
			inherit:     true,
			wantOverlay: false,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("codex")
			block := make(chan struct{})
			backend.block = block
			started := make(chan engine.SessionOpts, 1)
			backend.startHook = func(opts engine.SessionOpts) { started <- opts }
			server, _, cwd := newUnstartedTestServer(t, backend)
			server.codexHomeOverride = tt.override
			server.codexHomeInherit = tt.inherit
			server.codexAuthHome = filepath.Join(t.TempDir(), "missing-codex-home")
			launcher := newAdmissionFakeLaunchCustodian(t)
			enableTestAdmission(t, server, launcher)

			job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-codex-" + strings.ReplaceAll(tt.name, " ", "-"),
				RequestID:    "request-codex-" + strings.ReplaceAll(tt.name, " ", "-"),
				TaskSpec:     protocol.TaskSpec{Backend: "codex", CWD: cwd, Write: false, Prompt: "hold"},
			})
			select {
			case opts := <-started:
				got, ok := opts.EnvOverlay["CODEX_HOME"]
				if ok != tt.wantOverlay {
					t.Fatalf("CODEX_HOME present = %t, want %t; overlay=%v", ok, tt.wantOverlay, opts.EnvOverlay)
				}
				if tt.wantOverlay && got != tt.override {
					t.Fatalf("CODEX_HOME overlay = %q, want fixed %q", got, tt.override)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Codex session construction did not run")
			}
			if tt.wantOverlay {
				info, err := os.Stat(tt.override)
				if err != nil {
					t.Fatalf("fixed Codex home stat = %v", err)
				}
				if info.Mode().Perm() != 0o700 {
					t.Fatalf("fixed Codex home mode = %o, want 0700", info.Mode())
				}
			}
			close(block)
			waitAdmissionSafetyTerminal(t, server, job.JobID)
		})
	}
}

func TestIdentifiedNonCodexJobHasNoCodexHomeOverlay(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("claude")
	block := make(chan struct{})
	backend.block = block
	started := make(chan engine.SessionOpts, 1)
	backend.startHook = func(opts engine.SessionOpts) { started <- opts }
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.codexHomeOverride = filepath.Join(t.TempDir(), "must-not-be-used")
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-claude-no-codex-home",
		RequestID:    "request-claude-no-codex-home",
		TaskSpec:     protocol.TaskSpec{Backend: "claude", CWD: cwd, Write: false, Prompt: "hold"},
	})
	select {
	case opts := <-started:
		if _, ok := opts.EnvOverlay["CODEX_HOME"]; ok {
			t.Fatalf("non-Codex EnvOverlay = %v, want no CODEX_HOME", opts.EnvOverlay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Claude session construction did not run")
	}
	close(block)
	waitAdmissionSafetyTerminal(t, server, job.JobID)
}

func TestAuthorityJobCancelQueuedBeforeLaunch(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	accepted := acceptIdentifiedAuthorityWork(t, server, "authority-cancel-queued")
	jobID := accepted.Record.JobID.String()

	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: jobID})
	if canceled.JobID != jobID || canceled.State != engine.StateCanceled {
		t.Fatalf("authority queued cancel = %+v, want canceled job %s", canceled, jobID)
	}
	record := waitAdmissionSafetyTerminal(t, server, jobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledBeforeAuthorization {
		t.Fatalf("queued cancel terminal = %+v, want canceled before authorization", record.Terminal)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want none for queued cancel", got)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("launch releases = %d, want none for queued cancel", got)
	}
}

func TestAuthorityJobCancelTerminalReturnsAuthorityState(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-cancel-terminal",
		RequestID:    "request-authority-cancel-terminal",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "done"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("terminal = %+v, want completed", record.Terminal)
	}

	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: submitted.JobID})
	if canceled.JobID != submitted.JobID || canceled.State != engine.StateCompleted {
		t.Fatalf("terminal cancel = %+v, want completed state for %s", canceled, submitted.JobID)
	}
	after := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	if after.Cancel != record.Cancel || after.Terminal.Outcome != record.Terminal.Outcome {
		t.Fatalf("terminal cancel mutated authority record: before=%+v after=%+v", record, after)
	}
}

func TestIdentifiedFencedCancelUsesAuthorityWhenAdmissionMarkerCleared(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-cancel",
		RequestID:    "request-authority-cancel",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarted(t, backend)
	assertNoJSONJobRecord(t, server, submitted.JobID)

	clearAdmissionJobMarkersForTest(t, server)
	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: submitted.JobID})
	if canceled.JobID != submitted.JobID || canceled.State != engine.StateCanceled {
		t.Fatalf("authority cancel = %+v, want canceled job %s", canceled, submitted.JobID)
	}
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Cancel == nil {
		t.Fatalf("authority cancel fact missing: %+v", record)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Quiescence == nil || first.Quiescence.Method != model.QuiescenceTermKill {
		t.Fatalf("authority launch proof = %+v, want contained quiescence", first)
	}
	if got := launcher.containCount(); got != 0 {
		t.Fatalf("authority cancel coordinator contain calls = %d, want command-level containment", got)
	}
	if !launcher.runningContained(first.Group.CustodyID) {
		t.Fatalf("authority launch %s was not contained by active command interrupt", first.Group.CustodyID)
	}
	assertNoJSONJobRecord(t, server, submitted.JobID)
}

func TestAuthorityJobCancelHeldStartPostReleasePreRegistration(t *testing.T) {
	backend := newFakeBackend("fake")
	backend.startPathDone = make(chan struct{}, 1)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = make(chan struct{})
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, launcher)
	finalizes := installRecordingAdmissionAuthorityForTest(t, server)

	recordReleaseEntered := make(chan struct{})
	allowRecordRelease := make(chan struct{})
	var enteredOnce sync.Once
	setAdmissionRecordReleaseBeforeCommitHookForTest(t, func() error {
		enteredOnce.Do(func() { close(recordReleaseEntered) })
		select {
		case <-allowRecordRelease:
			return nil
		case <-time.After(time.Second):
			return errors.New("held record release timed out")
		}
	})

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-cancel-held-start",
		RequestID:    "request-authority-cancel-held-start",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)

	select {
	case <-recordReleaseEntered:
	case <-time.After(time.Second):
		t.Fatal("launch Start did not reach post-release/pre-registration hold")
	}

	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: submitted.JobID})
	if canceled.JobID != submitted.JobID || canceled.State != engine.StateCanceled {
		t.Fatalf("authority cancel = %+v, want canceled job %s", canceled, submitted.JobID)
	}
	close(allowRecordRelease)
	waitBackendRunnerStartPathDone(t, backend)
	waitActiveJobGone(t, server, submitted.JobID)

	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
		t.Fatalf("authority terminal = %+v, want canceled after authorization", record.Terminal)
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Quiescence == nil || first.Quiescence.Method != model.QuiescenceTermKill {
		t.Fatalf("authority launch proof = %+v, want contained quiescence", first)
	}
	intents := finalizes.finalizeIntents()
	if len(intents) != 1 || intents[0].Outcome != model.OutcomeCanceled || intents[0].Cause != model.CauseCanceledAfterAuthorization {
		t.Fatalf("authority finalize intents = %+v, want exactly one canceled-after-authorization terminal", intents)
	}
	after := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	if after.Terminal == nil || after.Terminal.Outcome != model.OutcomeCanceled || after.Terminal.Cause != model.CauseCanceledAfterAuthorization {
		t.Fatalf("authority terminal after held Start resumed = %+v, want unchanged canceled terminal", after.Terminal)
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped after held Start resumed: %v", reason)
	}
}

func TestAuthorityJobCancelInterruptRaceTerminalizesCanceled(t *testing.T) {
	t.Run("runner finalizes before handler cancel completes", func(t *testing.T) {
		holdNaturalExit := make(chan struct{})
		defer close(holdNaturalExit)
		releaseRunner := make(chan struct{})
		var releaseRunnerOnce sync.Once
		releaseRun := func() { releaseRunnerOnce.Do(func() { close(releaseRunner) }) }
		defer releaseRun()

		backend := newFakeBackend("fake")
		backend.started = make(chan struct{}, 1)
		backend.block = releaseRunner
		server, _, cwd := newUnstartedTestServer(t, backend)
		launcher := newAdmissionFakeLaunchCustodian(t)
		launcher.waitAndVerify = holdNaturalExit
		enableTestAdmission(t, server, launcher)
		finalizes := installRecordingAdmissionAuthorityForTest(t, server)

		firstRequestCancel := make(chan struct{})
		secondRequestCancel := make(chan struct{})
		allowFirstRequestCancel := make(chan struct{})
		var requestCancels atomic.Int32
		var releaseFirstOnce sync.Once
		releaseFirstRequestCancel := func() {
			releaseFirstOnce.Do(func() { close(allowFirstRequestCancel) })
		}
		defer releaseFirstRequestCancel()
		finalizes.beforeRequestCancel = func(ctx context.Context, jobID model.JobID) error {
			switch requestCancels.Add(1) {
			case 1:
				close(firstRequestCancel)
				select {
				case <-allowFirstRequestCancel:
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return errors.New("held first request cancel timed out")
				}
			case 2:
				close(secondRequestCancel)
			}
			return nil
		}

		conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-authority-cancel-interrupt-race",
			RequestID:    "request-authority-cancel-interrupt-race",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}, nil)
		resp := responseFromScriptedConn(t, conn)
		var submitted protocol.JobSubmitResult
		decodeResult(t, resp, &submitted)
		waitBackendStarted(t, backend)

		cancelDone := make(chan requestOutcome, 1)
		go func() {
			cancelDone <- server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: submitted.JobID}))
		}()

		select {
		case <-firstRequestCancel:
		case <-time.After(time.Second):
			t.Fatal("handler cancel did not enter RequestCancel")
		}
		releaseRun()
		select {
		case <-secondRequestCancel:
		case <-time.After(time.Second):
			t.Fatal("runner finalization did not enter cancel-derived RequestCancel")
		}

		record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
		if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority terminal = %+v, want canceled after authorization", record.Terminal)
		}
		if record.Cancel == nil {
			t.Fatalf("cancel fact missing after runner finalization: %+v", record)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped: %v", reason)
		}
		select {
		case outcome := <-cancelDone:
			t.Fatalf("handler cancel completed before its held RequestCancel was released: %+v", outcome)
		default:
		}

		releaseFirstRequestCancel()
		select {
		case outcome := <-cancelDone:
			if outcome.err != nil {
				t.Fatalf("job.cancel error = %+v", outcome.err)
			}
			canceled, ok := outcome.result.(protocol.JobCancelResult)
			if !ok {
				t.Fatalf("job.cancel result type = %T", outcome.result)
			}
			if canceled.JobID != submitted.JobID || canceled.State != engine.StateCanceled {
				t.Fatalf("job.cancel result = %+v, want canceled job %s", canceled, submitted.JobID)
			}
		case <-time.After(time.Second):
			t.Fatal("handler cancel did not finish after held RequestCancel was released")
		}
		waitActiveJobGone(t, server, submitted.JobID)
		intents := finalizes.finalizeIntents()
		if len(intents) != 1 || intents[0].Outcome != model.OutcomeCanceled || intents[0].Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority finalize intents = %+v, want exactly one canceled-after-authorization terminal", intents)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped after handler cancel completed: %v", reason)
		}
	})

	t.Run("handler cancel finalizes before runner", func(t *testing.T) {
		holdNaturalExit := make(chan struct{})
		defer close(holdNaturalExit)
		releaseRunner := make(chan struct{})
		var releaseRunnerOnce sync.Once
		releaseRun := func() { releaseRunnerOnce.Do(func() { close(releaseRunner) }) }
		defer releaseRun()

		backend := newFakeBackend("fake")
		backend.started = make(chan struct{}, 1)
		backend.block = releaseRunner
		server, _, cwd := newUnstartedTestServer(t, backend)
		launcher := newAdmissionFakeLaunchCustodian(t)
		launcher.waitAndVerify = holdNaturalExit
		enableTestAdmission(t, server, launcher)
		finalizes := installRecordingAdmissionAuthorityForTest(t, server)

		conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-authority-cancel-handler-first",
			RequestID:    "request-authority-cancel-handler-first",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}, nil)
		resp := responseFromScriptedConn(t, conn)
		var submitted protocol.JobSubmitResult
		decodeResult(t, resp, &submitted)
		waitBackendStarted(t, backend)

		canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: submitted.JobID})
		if canceled.JobID != submitted.JobID || canceled.State != engine.StateCanceled {
			t.Fatalf("job.cancel result = %+v, want canceled job %s", canceled, submitted.JobID)
		}
		record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
		if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority terminal = %+v, want canceled after authorization", record.Terminal)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped: %v", reason)
		}
		releaseRun()
		waitActiveJobGone(t, server, submitted.JobID)
		after := loadAdmissionSafetyRecord(t, server, submitted.JobID)
		if after.Terminal == nil || after.Terminal.Outcome != model.OutcomeCanceled || after.Terminal.Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority terminal after runner resumed = %+v, want unchanged canceled terminal", after.Terminal)
		}
		intents := finalizes.finalizeIntents()
		if len(intents) != 1 || intents[0].Outcome != model.OutcomeCanceled || intents[0].Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority finalize intents = %+v, want exactly one canceled-after-authorization terminal", intents)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped after runner resumed: %v", reason)
		}
	})

	t.Run("late completed sample reconciles already canceled terminal", func(t *testing.T) {
		backend := newFakeBackend("fake")
		backend.started = make(chan struct{}, 1)
		server, _, cwd := newUnstartedTestServer(t, backend)
		launcher := newAdmissionFakeLaunchCustodian(t)
		enableTestAdmission(t, server, launcher)
		finalizes := installRecordingAdmissionAuthorityForTest(t, server)

		runnerRecordOutcome := make(chan struct{})
		allowRunnerRecordOutcome := make(chan struct{})
		var enteredOutcomeOnce sync.Once
		var releaseOutcomeOnce sync.Once
		releaseRunnerOutcome := func() {
			releaseOutcomeOnce.Do(func() { close(allowRunnerRecordOutcome) })
		}
		defer releaseRunnerOutcome()
		finalizes.beforeRecordOutcome = func(ctx context.Context, _ model.JobID, _ model.AttemptRef, outcome model.Outcome) error {
			if outcome != model.OutcomeCompleted {
				return nil
			}
			enteredOutcomeOnce.Do(func() { close(runnerRecordOutcome) })
			select {
			case <-allowRunnerRecordOutcome:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return errors.New("held runner RecordOutcome timed out")
			}
		}

		conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-authority-cancel-late-completed-sample",
			RequestID:    "request-authority-cancel-late-completed-sample",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "done"},
		}, nil)
		resp := responseFromScriptedConn(t, conn)
		var submitted protocol.JobSubmitResult
		decodeResult(t, resp, &submitted)
		waitBackendStarted(t, backend)

		select {
		case <-runnerRecordOutcome:
		case <-time.After(5 * time.Second):
			t.Fatal("runner completion did not enter RecordOutcome")
		}

		canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: submitted.JobID})
		if canceled.JobID != submitted.JobID || canceled.State != engine.StateCanceled {
			t.Fatalf("job.cancel result = %+v, want canceled job %s", canceled, submitted.JobID)
		}
		record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
		if err := model.ValidateSafetyRecord(record); err != nil {
			t.Fatalf("canceled terminal record is invalid: %v", err)
		}
		if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority terminal = %+v, want canceled after authorization", record.Terminal)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped before runner resumed: %v", reason)
		}

		releaseRunnerOutcome()
		waitActiveJobGone(t, server, submitted.JobID)
		after := loadAdmissionSafetyRecord(t, server, submitted.JobID)
		if err := model.ValidateSafetyRecord(after); err != nil {
			t.Fatalf("terminal record after runner resumed is invalid: %v", err)
		}
		if after.Terminal == nil || after.Terminal.Outcome != model.OutcomeCanceled || after.Terminal.Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority terminal after runner resumed = %+v, want unchanged canceled terminal", after.Terminal)
		}
		intents := finalizes.finalizeIntents()
		if len(intents) != 1 || intents[0].Outcome != model.OutcomeCanceled || intents[0].Cause != model.CauseCanceledAfterAuthorization {
			t.Fatalf("authority finalize intents = %+v, want exactly one canceled-after-authorization terminal", intents)
		}
		if reason := server.safetyLatch.Reason(); reason != nil {
			t.Fatalf("safety latch tripped after runner reconciled: %v", reason)
		}
	})
}

func TestAdmissionFinalizationUnexpectedFailureStillFailStops(t *testing.T) {
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "unexpected-finalization-failure")
	jobID := accepted.Record.JobID.String()

	server.completeRunTerminal(jobRun{jobID: jobID, admissionControlled: true}, engine.StateCompleted, "done", nil)

	reason := server.safetyLatch.Reason()
	if reason == nil {
		t.Fatal("safety latch did not trip after unexpected finalization failure")
	}
	if errors.Is(reason, coordinator.ErrAlreadyFinalized) {
		t.Fatalf("safety latch reason = %v, want non-reconciled finalization failure", reason)
	}
	record := loadAuthoritySafetyRecordFromRepository(t, server.admissionRepository, jobID)
	if record.Terminal != nil {
		t.Fatalf("unexpected finalization terminalized record: %+v", record.Terminal)
	}
}

func TestAdmissionActiveRunnerInterruptsCommandForAuthorityCancel(t *testing.T) {
	t.Parallel()
	t.Run("later cancel", func(t *testing.T) {
		t.Parallel()
		active := &activeJob{jobID: "job-recorded"}
		running := &recordingRunningCommand{}
		runner := admissionActiveRunner{inner: recordingCommandRunner{running: running}, active: active}
		got, err := runner.Start(context.Background(), command.ExecSpec{Argv: []string{"/bin/fake-agent"}})
		if err != nil {
			t.Fatal(err)
		}
		if got != running {
			t.Fatalf("running command = %T, want recording command", got)
		}
		if got := running.interrupts.Load(); got != 0 {
			t.Fatalf("interrupts before cancel = %d, want 0", got)
		}
		active.requestTerminal(engine.StateCanceled)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := active.interruptAdmissionCommand(ctx); err != nil {
			t.Fatal(err)
		}
		if got := running.interrupts.Load(); got != 1 {
			t.Fatalf("interrupts after cancel = %d, want 1", got)
		}
		if !running.interruptHadDeadline.Load() {
			t.Fatal("interrupt context had no deadline")
		}
	})
	t.Run("already canceled", func(t *testing.T) {
		t.Parallel()
		active := &activeJob{jobID: "job-already-canceled"}
		active.requestTerminal(engine.StateCanceled)
		running := &recordingRunningCommand{}
		runner := admissionActiveRunner{inner: recordingCommandRunner{running: running}, active: active}
		if _, err := runner.Start(context.Background(), command.ExecSpec{Argv: []string{"/bin/fake-agent"}}); err != nil {
			t.Fatal(err)
		}
		if got := running.interrupts.Load(); got != 1 {
			t.Fatalf("interrupts after start = %d, want 1", got)
		}
		if !running.interruptHadDeadline.Load() {
			t.Fatal("interrupt context had no deadline")
		}
	})
}

func TestJobCancelUnknownWhenAuthorityCleanlyDisclaims(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	legacyID := server.nextID("job")
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_cancel_authority_down", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, errObj := server.authorityJobProjection(legacyID); ok || errObj != nil {
		t.Fatalf("authority projection ok=%v err=%+v, want clean disclaimer", ok, errObj)
	}

	assertJobHandlerError(t, server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: legacyID})), protocol.ErrorUnknownJob, "", legacyID)
	record, err := store.Load(legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State == engine.StateCanceled {
		t.Fatalf("legacy record state = %s, want not canceled", record.State)
	}
}

func TestJobCancelFailsClosedWhenAuthorityDegradedWithStaleJSONDuplicate(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-cancel-degraded",
		RequestID:    "request-authority-cancel-degraded",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarted(t, backend)
	_, projection, ok, errObj := server.authorityJobProjection(submitted.JobID)
	if errObj != nil || !ok {
		t.Fatalf("authority projection ok=%v err=%+v", ok, errObj)
	}
	store := server.storeForJob(submitted.JobID)
	if store == nil {
		t.Fatalf("job store missing for %s", submitted.JobID)
	}
	if err := server.createQueuedRecord(store, submitted.JobID, projection.SessionID, "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	server.removeActiveJob(submitted.JobID)
	if err := server.admissionReady.FailStop(context.Background(), "test authority degraded during cancel"); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: submitted.JobID}))
	assertJobHandlerError(t, outcome, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
	if outcome.result != nil {
		t.Fatalf("job.cancel result = %+v, want no canceled result", outcome.result)
	}
	record, err := store.Load(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State == engine.StateCanceled {
		t.Fatalf("stale JSON record state = %s, want not canceled", record.State)
	}
	if got := launcher.containCount(); got != 0 {
		t.Fatalf("authority cancel contain calls = %d, want none under degraded authority", got)
	}
}

func TestExactReadsReturnTypedFailStopWithoutJSONFallback(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	legacyID := server.nextID("job")
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_json_authority_down", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, state := range []engine.JobState{engine.StateStarting, engine.StateRunning} {
		if err := server.transitionRecord(store, legacyID, state); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.finalizeTerminal(jobRun{jobID: legacyID, store: store}, engine.StateCompleted, "legacy result", nil); err != nil {
		t.Fatal(err)
	}

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-failstopped-reads",
		RequestID:    "request-authority-failstopped-reads",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var fenced protocol.JobSubmitResult
	decodeResult(t, resp, &fenced)
	if fenced.JobID == "" || fenced.Deduplicated {
		t.Fatalf("submit result = %+v", fenced)
	}
	assertNoJSONJobRecord(t, server, fenced.JobID)
	waitBackendStarted(t, backend)
	waitAdmissionSafetyTerminal(t, server, fenced.JobID)

	fencedStore := server.storeForJob(fenced.JobID)
	if fencedStore == nil {
		t.Fatalf("job store missing for %s", fenced.JobID)
	}
	if err := server.createQueuedRecord(fencedStore, fenced.JobID, "ses_stale_json_authority_down", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.admissionReady.FailStop(context.Background(), "test authority degraded"); err != nil {
		t.Fatal(err)
	}

	assertJobHandlerError(t, server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{JobID: legacyID})), protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
	assertJobHandlerError(t, server.handleJobResult(mustMarshal(t, protocol.JobResultParams{JobID: legacyID})), protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
	assertJobHandlerError(t, server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{JobID: fenced.JobID})), protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
	assertJobHandlerError(t, server.handleJobResult(mustMarshal(t, protocol.JobResultParams{JobID: fenced.JobID})), protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")
}

func TestIdentifiedFencedPostAcceptAdvanceFailureFailsClosedAndRetainsAuthorityRecord(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	advanceErr := errors.New("advance fsync failed")
	anchorStore.FailNextForTest(authority.AnchorAdvance, advanceErr)

	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-post-accept-advance",
		RequestID:    "request-post-accept-advance",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}
	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, params, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorBackendUnavailable)
	if resp.Result != nil {
		t.Fatalf("post-accept failure returned success-shaped result: %+v", resp.Result)
	}
	jobID := resp.Error.Data.JobID
	if jobID == "" {
		t.Fatalf("post-accept failure response missing accepted job id: %+v", resp.Error)
	}
	if !strings.Contains(resp.Error.Message, advanceErr.Error()) {
		t.Fatalf("post-accept failure message = %q, want injected error", resp.Error.Message)
	}

	record := loadAuthoritySafetyRecordFromRepository(t, repo, jobID)
	if record.Cancel != nil {
		t.Fatalf("accepted authority record has cancel after post-accept failure: %+v", record.Cancel)
	}
	if record.Terminal != nil {
		t.Fatalf("accepted authority record terminal after post-accept failure: %+v", record.Terminal)
	}
	if snapshot := admissionAnchorSnapshot(t, anchorStore); snapshot.Phase != "fail_stopped" {
		t.Fatalf("anchor phase = %q, want fail_stopped", snapshot.Phase)
	}
	assertNoEngineJobRecordsForCWD(t, server, cwd)
	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("launch prepare calls = %d, want 0 after fail-closed authority submit", got)
	}
	select {
	case <-backend.started:
		t.Fatal("backend turn ran after post-accept fail-stop")
	case <-time.After(80 * time.Millisecond):
	}
}

func TestServeStopsOnDurableFailStopAndForceClosesConnections(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	var server *Server
	h := startTestServerWithHooks(t, backend, Config{IdleTimeout: -1}, func(s *Server) {
		server = s
		s.safetyDrainTimeout = 100 * time.Millisecond
		enableTestAdmission(t, s, launcher)
	})

	conn := dialRaw(t, h.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	reason := "test durable fail-stop closes listener"
	if err := server.admissionReady.FailStop(context.Background(), reason); err != nil {
		t.Fatal(err)
	}
	if got := server.safetyLatch.Reason(); got == nil || !strings.Contains(got.Error(), reason) {
		t.Fatalf("safety latch reason = %v, want %q", got, reason)
	}
	waitForSocketRemoved(t, h.socketPath, h.done)

	waitForConnClosed(t, conn, r, "safety fail-stop")

	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-h.done:
		if err == nil || !errors.Is(err, ErrSafetyFailStopped) || !strings.Contains(err.Error(), reason) || !strings.Contains(err.Error(), "safety drain timed out") {
			t.Fatalf("Serve error = %v, want non-nil timed-out safety fail-stop with reason", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not exit after fail-stop drain bound")
	}

	server.safetyLatch.Trip(errors.New("second fail-stop reason"))
	if got := server.safetyLatch.Reason(); got == nil || !strings.Contains(got.Error(), reason) {
		t.Fatalf("safety latch reason after second trip = %v, want first reason", got)
	}
}

func TestSubmitReplaySafetyCorruptionTripsFailStopAndPersists(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*testing.T, *Server, model.RequestKey, model.JobID)
	}{
		{
			name: "binding",
			corrupt: func(t *testing.T, server *Server, key model.RequestKey, _ model.JobID) {
				t.Helper()
				corruptor, ok := server.admissionRepository.(interface {
					InjectCorruptBindingForTest(model.RequestKey, string)
				})
				if !ok {
					t.Fatalf("admission repository type = %T, want InjectCorruptBindingForTest", server.admissionRepository)
				}
				corruptor.InjectCorruptBindingForTest(key, "binding checksum")
			},
		},
		{
			name: "safety",
			corrupt: func(t *testing.T, server *Server, _ model.RequestKey, jobID model.JobID) {
				t.Helper()
				corruptor, ok := server.admissionRepository.(interface {
					InjectCorruptSafetyForTest(model.JobID, string)
				})
				if !ok {
					t.Fatalf("admission repository type = %T, want InjectCorruptSafetyForTest", server.admissionRepository)
				}
				corruptor.InjectCorruptSafetyForTest(jobID, "safety checksum")
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			releaseBackend := make(chan struct{})
			backend.block = releaseBackend
			launcher := newAdmissionFakeLaunchCustodian(t)
			var server *Server
			h := startTestServerWithHooks(t, backend, Config{IdleTimeout: -1}, func(s *Server) {
				server = s
				s.safetyDrainTimeout = 100 * time.Millisecond
				enableTestAdmission(t, s, launcher)
			})

			conn := dialRaw(t, h.socketPath)
			reader := bufio.NewReader(conn)
			helloRaw(t, conn, reader, h.token)
			requestKey, err := model.NewRequestKey("workspace-replay-corrupt-"+tt.name, "request-replay-corrupt-"+tt.name)
			if err != nil {
				t.Fatal(err)
			}
			params := protocol.JobSubmitParams{
				WorkspaceKey: requestKey.WorkspaceKey.String(),
				RequestID:    requestKey.RequestID.String(),
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"},
			}
			resp := rpc(t, conn, reader, "submit", protocol.MethodJobSubmit, params)
			var submitted protocol.JobSubmitResult
			decodeResult(t, resp, &submitted)
			if submitted.JobID == "" || submitted.Deduplicated {
				t.Fatalf("submit result = %+v, want accepted non-replay", submitted)
			}
			jobID := model.JobID(submitted.JobID)
			tt.corrupt(t, server, requestKey, jobID)
			dbPath := filepath.Join(h.root, admissionRepositoryFile)
			corruptDB, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatalf("read corrupt admission DB: %v", err)
			}

			replay, err := rpcResponseOrClose(t, conn, reader, "replay", protocol.MethodJobSubmit, params)
			if err == nil {
				assertRPCCode(t, replay, protocol.ErrorBackendUnavailable)
				assertRPCAdmissionCause(t, replay, protocol.AdmissionRejectRootFailStopped)
			} else if !isExpectedFailStopClose(err) {
				t.Fatalf("replay error = %v, want root fail-stop response or connection close", err)
			}
			if reason := server.safetyLatch.Reason(); reason == nil || !errors.Is(reason, repository.ErrCorruptRecord) {
				t.Fatalf("safety latch reason = %v, want corrupt record trip reason", reason)
			}
			next := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-after-failstop-" + tt.name,
				RequestID:    "request-after-failstop-" + tt.name,
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "after"},
			}))
			assertJobHandlerError(t, next, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")

			waitForSocketRemoved(t, h.socketPath, h.done)
			if retry, err := net.DialTimeout("unix", h.socketPath, 50*time.Millisecond); err == nil {
				_ = retry.Close()
				t.Fatal("dial after fail-stop succeeded, want closed listener")
			}
			close(releaseBackend)
			waitForConnClosed(t, conn, reader, "safety corruption fail-stop")
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-h.done:
				if err == nil || !errors.Is(err, ErrSafetyFailStopped) {
					t.Fatalf("Serve error = %v, want safety fail-stop", err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not exit after safety corruption fail-stop")
			}

			snapshot, err := authority.LoadFileAnchorSnapshot(filepath.Join(h.root, admissionAnchorFile))
			if err != nil {
				t.Fatalf("load admission anchor snapshot: %v", err)
			}
			if snapshot.Phase != "fail_stopped" {
				t.Fatalf("anchor phase = %q, want fail_stopped", snapshot.Phase)
			}
			afterDB, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatalf("read preserved admission DB: %v", err)
			}
			if !bytes.Equal(afterDB, corruptDB) {
				t.Fatal("admission DB changed after safety fail-stop, want preserved corrupt file")
			}

			restart := newTestServerAtRoot(t, h.root, h.cwd, newFakeBackend("fake"))
			configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
			restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
				return nil, socketFileIdentity{}, errors.New("listener must not open for persisted safety corruption fail-stop")
			}
			// Restart must fail closed pre-listener: either the startup
			// corruption matrix fires on the still-corrupt record, or the
			// persisted fail-stop is surfaced — both typed, neither serving.
			err = restart.Serve(context.Background())
			persistedFailStop := errors.Is(err, ErrSafetyFailStopped) || errors.Is(err, authority.ErrFailStopped)
			startupCorruption := errors.Is(err, repository.ErrCorruptRecord)
			if err == nil || (!persistedFailStop && !startupCorruption) {
				t.Fatalf("restart Serve error = %v, want persisted fail-stop or typed startup corruption", err)
			}
		})
	}
}

func TestStatusAllSafetyCorruptionTripsFailStopAndPersists(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*testing.T, *Server, model.RequestKey, model.JobID)
	}{
		{
			name: "binding",
			corrupt: func(t *testing.T, server *Server, key model.RequestKey, _ model.JobID) {
				t.Helper()
				corruptor, ok := server.admissionRepository.(interface {
					InjectCorruptBindingForTest(model.RequestKey, string)
				})
				if !ok {
					t.Fatalf("admission repository type = %T, want InjectCorruptBindingForTest", server.admissionRepository)
				}
				corruptor.InjectCorruptBindingForTest(key, "binding checksum")
			},
		},
		{
			name: "safety",
			corrupt: func(t *testing.T, server *Server, _ model.RequestKey, jobID model.JobID) {
				t.Helper()
				corruptor, ok := server.admissionRepository.(interface {
					InjectCorruptSafetyForTest(model.JobID, string)
				})
				if !ok {
					t.Fatalf("admission repository type = %T, want InjectCorruptSafetyForTest", server.admissionRepository)
				}
				corruptor.InjectCorruptSafetyForTest(jobID, "safety checksum")
			},
		},
		{
			name: "binding_index",
			corrupt: func(t *testing.T, server *Server, _ model.RequestKey, jobID model.JobID) {
				t.Helper()
				corruptor, ok := server.admissionRepository.(interface {
					InjectCorruptBindingIndexValueForTest(model.JobID, model.RequestKey)
				})
				if !ok {
					t.Fatalf("admission repository type = %T, want InjectCorruptBindingIndexValueForTest", server.admissionRepository)
				}
				corruptor.InjectCorruptBindingIndexValueForTest(jobID, mustServedRequestKey(t, "workspace-status-corrupt-index-other", "request-status-corrupt-index-other"))
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			releaseBackend := make(chan struct{})
			backend.block = releaseBackend
			launcher := newAdmissionFakeLaunchCustodian(t)
			var server *Server
			h := startTestServerWithHooks(t, backend, Config{IdleTimeout: -1}, func(s *Server) {
				server = s
				s.safetyDrainTimeout = 100 * time.Millisecond
				enableTestAdmission(t, s, launcher)
			})

			conn := dialRaw(t, h.socketPath)
			reader := bufio.NewReader(conn)
			helloRaw(t, conn, reader, h.token)
			requestKey := mustServedRequestKey(t, "workspace-status-corrupt-"+tt.name, "request-status-corrupt-"+tt.name)
			params := protocol.JobSubmitParams{
				WorkspaceKey: requestKey.WorkspaceKey.String(),
				RequestID:    requestKey.RequestID.String(),
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"},
			}
			resp := rpc(t, conn, reader, "submit", protocol.MethodJobSubmit, params)
			var submitted protocol.JobSubmitResult
			decodeResult(t, resp, &submitted)
			if submitted.JobID == "" || submitted.Deduplicated {
				t.Fatalf("submit result = %+v, want accepted non-replay", submitted)
			}
			jobID := model.JobID(submitted.JobID)
			tt.corrupt(t, server, requestKey, jobID)
			dbPath := filepath.Join(h.root, admissionRepositoryFile)
			corruptDB, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatalf("read corrupt admission DB: %v", err)
			}

			outcome := server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{All: true}))
			assertJobHandlerError(t, outcome, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootCorrupt, "")
			if reason := server.safetyLatch.Reason(); reason == nil || !errors.Is(reason, repository.ErrCorruptRecord) {
				t.Fatalf("safety latch reason = %v, want corrupt record trip reason", reason)
			}
			next := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
				WorkspaceKey: "workspace-status-after-failstop-" + tt.name,
				RequestID:    "request-status-after-failstop-" + tt.name,
				TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "after"},
			}))
			assertJobHandlerError(t, next, protocol.ErrorBackendUnavailable, protocol.AdmissionRejectRootFailStopped, "")

			waitForSocketRemoved(t, h.socketPath, h.done)
			if retry, err := net.DialTimeout("unix", h.socketPath, 50*time.Millisecond); err == nil {
				_ = retry.Close()
				t.Fatal("dial after fail-stop succeeded, want closed listener")
			}
			close(releaseBackend)
			waitForConnClosed(t, conn, reader, "safety corruption fail-stop")
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-h.done:
				if err == nil || !errors.Is(err, ErrSafetyFailStopped) {
					t.Fatalf("Serve error = %v, want safety fail-stop", err)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not exit after safety corruption fail-stop")
			}

			snapshot, err := authority.LoadFileAnchorSnapshot(filepath.Join(h.root, admissionAnchorFile))
			if err != nil {
				t.Fatalf("load admission anchor snapshot: %v", err)
			}
			if snapshot.Phase != "fail_stopped" {
				t.Fatalf("anchor phase = %q, want fail_stopped", snapshot.Phase)
			}
			afterDB, err := os.ReadFile(dbPath)
			if err != nil {
				t.Fatalf("read preserved admission DB: %v", err)
			}
			if !bytes.Equal(afterDB, corruptDB) {
				t.Fatal("admission DB changed after safety fail-stop, want preserved corrupt file")
			}
		})
	}
}

func TestServeFailsPreListenerOnStartupCorruptBindingIndexValue(t *testing.T) {
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	var server *Server
	h := startTestServerWithHooks(t, backend, Config{IdleTimeout: -1}, func(s *Server) {
		server = s
		enableTestAdmission(t, s, launcher)
	})

	conn := dialRaw(t, h.socketPath)
	reader := bufio.NewReader(conn)
	helloRaw(t, conn, reader, h.token)
	requestKey := mustServedRequestKey(t, "workspace-startup-corrupt-index", "request-startup-corrupt-index")
	resp := rpc(t, conn, reader, "submit", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: requestKey.WorkspaceKey.String(),
		RequestID:    requestKey.RequestID.String(),
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "complete"},
	})
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	h.cancel()
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatal("server did not stop before startup corruption test")
	}

	dbPath := filepath.Join(h.root, admissionRepositoryFile)
	repo, err := bboltrepo.OpenExisting(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptBindingIndexValueForTest(model.JobID(submitted.JobID), mustServedRequestKey(t, "workspace-startup-corrupt-index-other", "request-startup-corrupt-index-other"))
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	restart := newTestServerAtRoot(t, h.root, h.cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for corrupt binding index")
	}
	err = restart.Serve(context.Background())
	requireServedCorruptKind(t, err, "binding_index")
	// Record-level corruption is never repaired or deleted: a second startup
	// attempt must detect the same corruption and refuse to serve.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("admission DB missing after startup corruption failure: %v", err)
	}
	second := newTestServerAtRoot(t, h.root, h.cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, second, newAdmissionFakeLaunchCustodian(t), true)
	second.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for corrupt binding index")
	}
	err = second.Serve(context.Background())
	requireServedCorruptKind(t, err, "binding_index")
}

func TestServeRepairsProjectionOnlyStartupAuditBeforeListen(t *testing.T) {
	server, root, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "startup-projection-repair")
	finalizeAcceptedAuthorityWork(t, server, accepted)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	repo, err := bboltrepo.OpenExisting(filepath.Join(root, admissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptProjectionForTest(accepted.Record.JobID, "projection checksum")
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	restart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	listenErr := errors.New("listener reached after projection repair")
	listenCalled := false
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		image, err := restart.admissionReady.LoadJob(context.Background(), accepted.Record.JobID)
		if err != nil {
			return nil, socketFileIdentity{}, err
		}
		if image.Projection.State != repository.RecordValid {
			return nil, socketFileIdentity{}, fmt.Errorf("projection state = %s, want valid", image.Projection.State)
		}
		if image.Quarantine.State != repository.RecordValid {
			return nil, socketFileIdentity{}, fmt.Errorf("quarantine state = %s, want valid", image.Quarantine.State)
		}
		return nil, socketFileIdentity{}, listenErr
	}

	err = restart.Serve(context.Background())
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want listener sentinel", err)
	}
	if !listenCalled {
		t.Fatal("listener was not reached after projection-only repair")
	}
}

func TestServeFailsPreListenerWhenProjectionAndBindingCorrupt(t *testing.T) {
	server, root, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "startup-projection-binding-corrupt")
	finalizeAcceptedAuthorityWork(t, server, accepted)
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	repo, err := bboltrepo.OpenExisting(filepath.Join(root, admissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptProjectionForTest(accepted.Record.JobID, "projection checksum")
	repo.InjectCorruptBindingForTest(accepted.Binding.RequestKey, "binding checksum")
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}

	restart := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	listenCalled := false
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		return nil, socketFileIdentity{}, errors.New("listener must not open for binding corruption")
	}

	err = restart.Serve(context.Background())
	if !errors.Is(err, repository.ErrCorruptRecord) || !servedErrorTreeHasCorruptKind(err, "binding") {
		t.Fatalf("Serve error = %T %v, want binding corruption", err, err)
	}
	if listenCalled {
		t.Fatal("listener was called despite authoritative binding corruption")
	}
}

func TestServeFailsPreListenerOnStartupStructuralCorruption(t *testing.T) {
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(root, admissionRepositoryFile)
	truncateAdmissionRepository(t, repoPath)
	before := readFileBytes(t, repoPath)

	server := newTestServerAtRoot(t, root, cwd, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	listenCalled := false
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		return nil, socketFileIdentity{}, errors.New("listener must not open for structural corruption")
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("Serve error = %T %v, want structural corruption", err, err)
	}
	if listenCalled {
		t.Fatal("listener was called despite startup structural corruption")
	}
	assertNoServeAdmissionPublished(t, server)
	after := readFileBytes(t, repoPath)
	if !bytes.Equal(before, after) {
		t.Fatal("structural startup failure mutated admission repository")
	}
}

func TestServeBootstrapRootExistenceMatrix(t *testing.T) {
	tests := []struct {
		name      string
		arrange   func(*testing.T, string)
		want      error
		assertion func(*testing.T, string, error)
	}{
		{
			name: "missing db missing anchor creates",
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("bootstrapAdmission error = %v, want nil", err)
				}
				assertAdmissionRootFilesExist(t, root)
			},
		},
		{
			name: "valid db matching anchor opens",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
					t.Fatal(err)
				}
			},
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("bootstrapAdmission error = %v, want nil", err)
				}
				assertAdmissionRootFilesExist(t, root)
			},
		},
		{
			name: "missing db present anchor rejects without recreate",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, admissionAnchorFile), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: authority.ErrAnchorInvariant,
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				assertPathMissing(t, filepath.Join(root, admissionRepositoryFile))
				assertPathExists(t, filepath.Join(root, admissionAnchorFile))
			},
		},
		{
			name: "missing db dangling anchor symlink rejects without recreate",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if err := os.Symlink(filepath.Join(root, "missing-anchor-target"), filepath.Join(root, admissionAnchorFile)); err != nil {
					t.Fatal(err)
				}
			},
			want: authority.ErrAnchorInvariant,
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				assertPathMissing(t, filepath.Join(root, admissionRepositoryFile))
				info, statErr := os.Lstat(filepath.Join(root, admissionAnchorFile))
				if statErr != nil {
					t.Fatalf("anchor lstat = %v, want dangling symlink present", statErr)
				}
				if info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("anchor mode = %v, want symlink", info.Mode())
				}
			},
		},
		{
			name: "zero length db rejects without initialize",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(root, admissionRepositoryFile), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: repository.ErrInvalidRecord,
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				info, statErr := os.Stat(filepath.Join(root, admissionRepositoryFile))
				if statErr != nil {
					t.Fatal(statErr)
				}
				if info.Size() != 0 {
					t.Fatalf("zero-length db size = %d, want 0", info.Size())
				}
				assertPathMissing(t, filepath.Join(root, admissionAnchorFile))
			},
		},
		{
			name: "present db missing anchor rejects",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(root, admissionAnchorFile)); err != nil {
					t.Fatal(err)
				}
			},
			want: authority.ErrAnchorInvariant,
			assertion: func(t *testing.T, root string, err error) {
				t.Helper()
				assertPathExists(t, filepath.Join(root, admissionRepositoryFile))
				assertPathMissing(t, filepath.Join(root, admissionAnchorFile))
			},
		},
		{
			name: "anchor mismatch rejects",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				anchorPath := filepath.Join(root, admissionAnchorFile)
				snapshot, err := authority.LoadFileAnchorSnapshot(anchorPath)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.DBUUID = "bbolt-db-mismatched"
				raw, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(anchorPath, append(raw, '\n'), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: authority.ErrAnchorInvariant,
		},
		{
			name: "old schema precedes missing bucket",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				repoPath := filepath.Join(root, admissionRepositoryFile)
				setAdmissionRepositoryMetaSchemaVersion(t, repoPath, 1)
				deleteAdmissionRepositoryBucket(t, repoPath, "binding_index")
			},
			want: repository.ErrInvalidRecord,
			assertion: func(t *testing.T, _ string, err error) {
				t.Helper()
				var schemaErr AdmissionRootIncompatibleSchemaError
				if !errors.As(err, &schemaErr) {
					t.Fatalf("bootstrapAdmission error = %T %v, want AdmissionRootIncompatibleSchemaError", err, err)
				}
				if errors.Is(err, repository.ErrCorruptRecord) {
					t.Fatalf("bootstrapAdmission error = %v, want schema before missing bucket", err)
				}
			},
		},
		{
			name: "missing binding index rejects",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
					t.Fatal(err)
				}
				deleteAdmissionRepositoryBucket(t, filepath.Join(root, admissionRepositoryFile), "binding_index")
			},
			want: repository.ErrCorruptRecord,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			root := shortTempDir(t)
			if tt.arrange != nil {
				tt.arrange(t, root)
			}
			server, err := bootstrapAdmissionRootForTest(t, root)
			if tt.want == nil {
				if tt.assertion != nil {
					tt.assertion(t, root, err)
				}
				if err == nil {
					if closeErr := server.closeServeAdmission(); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("bootstrapAdmission error = %v, want %v", err, tt.want)
			}
			assertNoServeAdmissionPublished(t, server)
			if tt.assertion != nil {
				tt.assertion(t, root, err)
			}
		})
	}
}

func TestServeBootstrapStructuralCorruptionFixturesAreFatalAndUntouched(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(*testing.T, string)
		want    error
	}{
		{
			name: "truncated file",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				truncateAdmissionRepository(t, filepath.Join(root, admissionRepositoryFile))
			},
			want: repository.ErrCorruptRecord,
		},
		{
			name: "page header flip",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				flipPageHeaderContaining(t, filepath.Join(root, admissionRepositoryFile), []byte("binding_index"))
			},
			want: repository.ErrCorruptRecord,
		},
		{
			name: "leaf page damage",
			arrange: func(t *testing.T, root string) {
				t.Helper()
				_, jobID := initializedServedRootWithPendingJobForTest(t, root, "leaf-page-damage")
				flipPageHeaderContaining(t, filepath.Join(root, admissionRepositoryFile), []byte(jobID))
			},
			want: repository.ErrCorruptRecord,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			root := shortTempDir(t)
			if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			tt.arrange(t, root)
			repoPath := filepath.Join(root, admissionRepositoryFile)
			before := readFileBytes(t, repoPath)
			server, err := bootstrapAdmissionRootForTest(t, root)
			if !errors.Is(err, tt.want) {
				t.Fatalf("bootstrapAdmission error = %v, want %v", err, tt.want)
			}
			assertNoServeAdmissionPublished(t, server)
			after := readFileBytes(t, repoPath)
			if !bytes.Equal(before, after) {
				t.Fatal("structural startup failure mutated admission repository")
			}
		})
	}
}

func TestServeBootstrapCorruptEnvelopeRepeatsWithoutRepair(t *testing.T) {
	root := shortTempDir(t)
	_, jobID := initializedServedRootWithPendingJobForTest(t, root, "corrupt-envelope")
	repoPath := filepath.Join(root, admissionRepositoryFile)
	repo, err := bboltrepo.OpenExisting(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	repo.InjectCorruptSafetyForTest(model.JobID(jobID), "safety checksum")
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	before := readFileBytes(t, repoPath)

	server, err := bootstrapAdmissionRootForTest(t, root)
	requireServedCorruptKind(t, err, "safety")
	assertNoServeAdmissionPublished(t, server)
	afterFirst := readFileBytes(t, repoPath)
	if !bytes.Equal(before, afterFirst) {
		t.Fatal("corrupt envelope startup failure repaired or rewrote repository")
	}

	second, err := bootstrapAdmissionRootForTest(t, root)
	requireServedCorruptKind(t, err, "safety")
	assertNoServeAdmissionPublished(t, second)
	afterSecond := readFileBytes(t, repoPath)
	if !bytes.Equal(before, afterSecond) {
		t.Fatal("second corrupt envelope startup failure repaired or rewrote repository")
	}
}

func TestServerContextCancelDoesNotForceCloseEstablishedConnection(t *testing.T) {
	t.Parallel()
	h := startTestServer(t, newFakeBackend("fake"), Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Serve error after context cancel = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop after context cancel")
	}
	assertConnRemainsOpenFor(t, conn, r, 80*time.Millisecond, "ordinary server context cancellation")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownRejectsAdmissionBeforeListenerClose(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	listener := newBlockingTestListener()
	ready := make(chan struct{})
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return listener, socketFileIdentity{}, nil
	}
	server.readyHook = func(ServeReadyInfo) error {
		close(ready)
		return nil
	}
	listener.onClose = func() {
		if !server.admissionCurrentServeClosing() {
			t.Error("listener closed before admission close epoch was marked")
		}
		outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
			WorkspaceKey: "workspace-shutdown-order",
			RequestID:    "request-shutdown-order",
			TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
		}))
		assertJobHandlerError(t, outcome, protocol.ErrorCapabilityMissing, protocol.AdmissionRejectAdmissionClosing, "")
	}

	done := make(chan error, 1)
	go func() { done <- server.Serve(context.Background()) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("Serve returned before ready: %v", err)
	case <-time.After(time.Second):
		t.Fatal("server did not become ready")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown error = %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}
}

func TestShutdownCancelsPendingAuthorityWorkBeforeClose(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	accepted := acceptIdentifiedAuthorityWork(t, server, "shutdown-pending")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown error = %v", err)
	}
	record := loadAuthoritySafetyRecordFromRepository(t, repo, accepted.Record.JobID.String())
	if record.Terminal == nil {
		t.Fatal("shutdown left pending authority work nonterminal")
	}
	if record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Cause != model.CauseCanceledBeforeAuthorization {
		t.Fatalf("terminal = %+v, want canceled before authorization", record.Terminal)
	}
	if record.CancellationOrigin != engine.CancellationOriginDaemonShutdown || record.CancellationReason != "daemon shutdown requested cancellation" {
		t.Fatalf("shutdown cancellation metadata = (%q, %q), want daemon shutdown", record.CancellationOrigin, record.CancellationReason)
	}
	if server.admissionInstance != nil || server.admissionReady != nil || server.admissionCoordinator != nil || server.admissionRuntime != nil || server.admissionRepository != nil {
		t.Fatalf("admission state after Shutdown: instance=%p ready=%p coord=%p runtime=%p repo=%v",
			server.admissionInstance, server.admissionReady, server.admissionCoordinator, server.admissionRuntime, server.admissionRepository)
	}
}

func TestShutdownDeadlineExceededLeavesForcedRecoveryPath(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	launcher.activeCustodies.Store(true)
	t.Cleanup(func() {
		launcher.activeCustodies.Store(false)
		_ = server.closeServeAdmission()
	})

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := server.shutdown(shutdownCtx)
	if !errors.Is(err, ErrShutdownDeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want ErrShutdownDeadlineExceeded", err)
	}
	if server.admissionInstance == nil || server.admissionRuntime == nil {
		t.Fatalf("forced shutdown closed admission state; instance=%p runtime=%p", server.admissionInstance, server.admissionRuntime)
	}
}

type blockingCloser struct {
	started  chan<- struct{}
	release  <-chan struct{}
	returned chan<- struct{}
}

func (c blockingCloser) Close() error {
	close(c.started)
	defer close(c.returned)
	<-c.release
	return nil
}

type notifyCloser struct {
	once   sync.Once
	closed chan struct{}
}

func newNotifyCloser() *notifyCloser {
	return &notifyCloser{closed: make(chan struct{})}
}

func (c *notifyCloser) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *notifyCloser) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(time.Second):
		t.Fatal("admission closer did not close")
	}
}

func TestShutdownAdmissionCloseDeadlineExceededReturnsTypedError(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	closeStarted := make(chan struct{})
	closeReturned := make(chan struct{})
	server.admissionClose = blockingCloser{
		started:  closeStarted,
		release:  releaseClose,
		returned: closeReturned,
	}
	pidPath := filepath.Join(root, "agentbus.pid")
	ownPID := strconv.Itoa(os.Getpid())
	if err := os.WriteFile(pidPath, []byte(ownPID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseClose) })
		select {
		case <-closeReturned:
		case <-time.After(time.Second):
			t.Fatal("admission close did not return after release")
		}
	})

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := server.shutdown(shutdownCtx)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrShutdownDeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want ErrShutdownDeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Fatalf("Shutdown elapsed = %s, want caller deadline shorter than close cap honored", elapsed)
	}
	select {
	case <-closeStarted:
	default:
		t.Fatal("admission close did not start before deadline error")
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != ownPID {
		t.Fatalf("pid file after failed close = %q, want owned pid retained", raw)
	}
	releaseOnce.Do(func() { close(releaseClose) })
}

func TestShutdownDeadlineDuringPIDTeardownReturnsTypedErrorAndRetainsPID(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	_, serveDone, _ := startTestServerWithBlockingListener(t, server)
	pidPath := filepath.Join(root, "agentbus.pid")
	ownPID := strconv.Itoa(os.Getpid())
	if err := os.WriteFile(pidPath, []byte(ownPID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	server.beforePIDFileQuarantineHook = func() {
		<-shutdownCtx.Done()
	}
	err := server.Shutdown(shutdownCtx)
	if !errors.Is(err, ErrShutdownDeadlineExceeded) {
		t.Fatalf("Shutdown error = %v, want ErrShutdownDeadlineExceeded", err)
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != ownPID {
		t.Fatalf("pid file after pid teardown deadline = %q, want owned pid retained", raw)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after shutdown deadline")
	}
}

func TestShutdownDeadlineAfterPIDQuarantineAbandonsRestore(t *testing.T) {
	t.Parallel()
	root := shortTempDir(t)
	pidPath := filepath.Join(root, "agentbus.pid")
	quarantineDir, quarantinePath, err := createPIDFileQuarantine(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(quarantineDir) })
	if err := os.WriteFile(quarantinePath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	cleanupQuarantineDir := true

	start := time.Now()
	err = abortQuarantinedPIDFileIfContextDone(deadlineCtx, pidPath, quarantinePath, "test deadline", &cleanupQuarantineDir)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("abort error = %v, want context deadline", err)
	}
	if elapsed > time.Second {
		t.Fatalf("abort elapsed = %s, want bounded abandonment", elapsed)
	}
	if cleanupQuarantineDir {
		t.Fatal("cleanupQuarantineDir = true, want orphaned quarantine retained")
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical pid stat = %v, want absent", err)
	}
	if _, err := os.Stat(quarantinePath); err != nil {
		t.Fatalf("quarantine pid stat = %v, want retained", err)
	}
}

func TestShutdownPIDQuarantineReadErrorAfterDeadlineAbandonsRestore(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	pidPath := filepath.Join(root, "agentbus.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	readStarted := make(chan struct{})
	var readOnce sync.Once
	shutdownBase, expire := context.WithCancelCause(context.Background())
	shutdownCtx := causeErrContext{Context: shutdownBase}
	server.readPIDFileNoFollowHook = func(path string) ([]byte, socketFileIdentity, error) {
		if filepath.Base(filepath.Dir(path)) != "." && strings.HasPrefix(filepath.Base(filepath.Dir(path)), "agentbus.pid.quarantine.") {
			readOnce.Do(func() {
				close(readStarted)
				expire(context.DeadlineExceeded)
			})
			<-shutdownCtx.Done()
			return nil, socketFileIdentity{}, errors.New("forced quarantined pid read failure")
		}
		return readPIDFileNoFollow(path)
	}

	err := server.removeOwnedPIDFile(shutdownCtx, "test quarantined read error")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("removeOwnedPIDFile error = %v, want context deadline", err)
	}
	select {
	case <-readStarted:
	default:
		t.Fatal("quarantined pid read hook did not run")
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical pid stat after abandoned restore = %v, want absent", err)
	}
	matches, err := filepath.Glob(filepath.Join(root, "agentbus.pid.quarantine.*", "agentbus.pid"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("quarantined pid files = %v, want one retained", matches)
	}
}

func TestConcurrentShutdownWaitingCallerHonorsContext(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	closeStarted := make(chan struct{})
	closeReturned := make(chan struct{})
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), blockingCloser{
		started:  closeStarted,
		release:  releaseClose,
		returned: closeReturned,
	})
	cancelServe, serveDone, _ := startTestServerWithBlockingListener(t, server)
	t.Cleanup(func() {
		cancelServe()
		releaseOnce.Do(func() { close(releaseClose) })
		select {
		case <-closeReturned:
		case <-time.After(time.Second):
			t.Fatal("admission close did not return after release")
		}
	})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.Shutdown(context.Background())
	}()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("first shutdown did not enter admission close")
	}

	secondCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := server.Shutdown(secondCtx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second Shutdown error = %v, want context deadline", err)
	}
	if errors.Is(err, ErrShutdownDeadlineExceeded) {
		t.Fatalf("second Shutdown error = %v, want raw waiting caller context error", err)
	}
	if elapsed > time.Second {
		t.Fatalf("second Shutdown elapsed = %s, want prompt context return", elapsed)
	}

	releaseOnce.Do(func() { close(releaseClose) })
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not complete after close release")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop after shutdown")
	}
	if err := server.Shutdown(context.Background()); !errors.Is(err, ErrShutdownNotServing) {
		t.Fatalf("terminal Shutdown error = %v, want ErrShutdownNotServing", err)
	}
}

func TestShutdownLateWaiterReceivesServeGenerationResultAcrossReserve(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	releaseFirstClose := make(chan struct{})
	var releaseFirstOnce sync.Once
	firstCloseStarted := make(chan struct{})
	firstCloseReturned := make(chan struct{})
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), blockingCloser{
		started:  firstCloseStarted,
		release:  releaseFirstClose,
		returned: firstCloseReturned,
	})
	cancelFirstServe, firstServeDone, _ := startTestServerWithBlockingListener(t, server)
	t.Cleanup(func() {
		cancelFirstServe()
		releaseFirstOnce.Do(func() { close(releaseFirstClose) })
		select {
		case <-firstCloseReturned:
		case <-time.After(time.Second):
			t.Fatal("first admission close did not return after release")
		}
	})

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.Shutdown(context.Background())
	}()
	select {
	case <-firstCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("first shutdown did not enter admission close")
	}

	lateDone := make(chan error, 1)
	go func() {
		lateDone <- server.Shutdown(context.Background())
	}()
	select {
	case err := <-lateDone:
		t.Fatalf("late Shutdown returned before generation result was available: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	cancelFirstServe()
	select {
	case err := <-firstServeDone:
		if err != nil {
			t.Fatalf("first Serve after external cancel = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Serve did not stop after external cancel")
	}

	releaseSecondClose := make(chan struct{})
	var releaseSecondOnce sync.Once
	secondCloseStarted := make(chan struct{})
	secondCloseReturned := make(chan struct{})
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), blockingCloser{
		started:  secondCloseStarted,
		release:  releaseSecondClose,
		returned: secondCloseReturned,
	})
	secondListener := newBlockingTestListener()
	cancelSecondServe, secondServeDone, secondListening := startTestServerWithProvidedListenerAsync(t, server, secondListener)
	t.Cleanup(func() {
		cancelSecondServe()
		releaseSecondOnce.Do(func() { close(releaseSecondClose) })
		select {
		case <-secondCloseReturned:
		case <-time.After(time.Second):
			t.Fatal("second admission close did not return after release")
		}
	})
	select {
	case <-secondListening:
	case err := <-secondServeDone:
		t.Fatalf("second Serve exited before lifecycle registration gate released: %v", err)
	case <-time.After(time.Second):
		t.Fatal("second Serve did not reach listener setup")
	}
	select {
	case err := <-secondServeDone:
		t.Fatalf("second Serve exited while waiting for first shutdown gate: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	server.serveStateMu.Lock()
	secondRegistered := server.serveListener == secondListener
	server.serveStateMu.Unlock()
	if secondRegistered {
		t.Fatal("second generation registered lifecycle while first generation teardown was still running")
	}

	releaseFirstOnce.Do(func() { close(releaseFirstClose) })
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not complete after close release")
	}
	select {
	case err := <-lateDone:
		if err != nil {
			t.Fatalf("late Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late Shutdown did not receive generation result")
	}
	waitForServeLifecycle(t, server, secondListener, secondServeDone)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- server.Shutdown(context.Background())
	}()
	select {
	case <-secondCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("second generation shutdown did not enter teardown after first completed")
	}
	releaseSecondOnce.Do(func() { close(releaseSecondClose) })
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Shutdown did not complete after close release")
	}
	select {
	case err := <-secondServeDone:
		if err != nil {
			t.Fatalf("second Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Serve did not stop after shutdown")
	}
}

func TestShutdownGenerationDoesNotStopNextServe(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	releaseFirstClose := make(chan struct{})
	var releaseFirstOnce sync.Once
	firstCloseStarted := make(chan struct{})
	firstCloseReturned := make(chan struct{})
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), blockingCloser{
		started:  firstCloseStarted,
		release:  releaseFirstClose,
		returned: firstCloseReturned,
	})
	cancelFirstServe, firstServeDone, _ := startTestServerWithBlockingListener(t, server)
	t.Cleanup(func() {
		cancelFirstServe()
		releaseFirstOnce.Do(func() { close(releaseFirstClose) })
		select {
		case <-firstCloseReturned:
		case <-time.After(time.Second):
			t.Fatal("first admission close did not return after release")
		}
	})

	pidPath := filepath.Join(root, "agentbus.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- server.Shutdown(context.Background())
	}()
	select {
	case <-firstCloseStarted:
	case <-time.After(time.Second):
		t.Fatal("first shutdown did not enter admission close")
	}

	cancelFirstServe()
	select {
	case err := <-firstServeDone:
		if err != nil {
			t.Fatalf("first Serve after external cancel = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Serve did not stop after external cancel")
	}

	secondClose := newNotifyCloser()
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), secondClose)
	secondListener := newPipeTestListener()
	cancelSecondServe, secondServeDone, secondListening := startTestServerWithProvidedListenerAsync(t, server, secondListener)
	t.Cleanup(func() {
		cancelSecondServe()
		select {
		case <-secondServeDone:
		case <-time.After(time.Second):
			t.Fatal("second Serve did not stop")
		}
	})
	select {
	case <-secondListening:
	case err := <-secondServeDone:
		t.Fatalf("second Serve exited before lifecycle registration gate released: %v", err)
	case <-time.After(time.Second):
		t.Fatal("second Serve did not reach listener setup")
	}
	select {
	case err := <-secondServeDone:
		t.Fatalf("second Serve exited while waiting for first shutdown gate: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	server.serveStateMu.Lock()
	secondRegistered := server.serveListener == secondListener
	server.serveStateMu.Unlock()
	if secondRegistered {
		t.Fatal("second generation registered lifecycle while first generation teardown was still running")
	}
	replacementPath := filepath.Join(root, "agentbus.pid.next")
	if err := os.WriteFile(replacementPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacementPath, pidPath); err != nil {
		t.Fatal(err)
	}
	_, wantPIDIdentity, err := readPIDFileNoFollow(pidPath)
	if err != nil {
		t.Fatal(err)
	}

	releaseFirstOnce.Do(func() { close(releaseFirstClose) })
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Shutdown did not complete after close release")
	}
	waitForServeLifecycle(t, server, secondListener, secondServeDone)
	assertPipeListenerAccepts(t, secondListener, "after first generation shutdown completed")
	_, gotPIDIdentity, err := readPIDFileNoFollow(pidPath)
	if err != nil {
		t.Fatalf("second generation pid was removed: %v", err)
	}
	if gotPIDIdentity != wantPIDIdentity {
		t.Fatalf("second generation pid identity = %+v, want %+v", gotPIDIdentity, wantPIDIdentity)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown error = %v", err)
	}
	secondClose.waitClosed(t)
	select {
	case err := <-secondServeDone:
		if err != nil {
			t.Fatalf("second Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Serve did not stop after shutdown")
	}
}

func TestAdmissionClosingSkipsReplacementAdmissionInstance(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	oldAdmission := server.currentServeAdmissionSnapshot()
	if oldAdmission == nil || oldAdmission.instance == nil {
		t.Fatal("old admission snapshot missing instance")
	}
	if err := server.closeServeAdmission(); err != nil {
		t.Fatalf("close old admission error = %v", err)
	}
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	if server.admissionCurrentServeClosing() {
		t.Fatal("replacement admission started closed")
	}

	if err := server.beginAdmissionClosing(context.Background(), oldAdmission); err != nil {
		t.Fatalf("beginAdmissionClosing old snapshot error = %v", err)
	}
	if server.admissionCurrentServeClosing() {
		t.Fatal("old admission snapshot marked replacement admission closing")
	}
}

func TestShutdownAdmissionJobsDoesNotCancelReplacementActiveJobs(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	oldAdmission := server.currentServeAdmissionSnapshot()
	if oldAdmission == nil || oldAdmission.instance == nil {
		t.Fatal("old admission snapshot missing instance")
	}
	canceled := make(chan struct{})
	var cancelOnce sync.Once
	jobID := "job-replacement-active"
	active := &activeJob{
		jobID:             jobID,
		cancel:            func() { cancelOnce.Do(func() { close(canceled) }) },
		containmentIntent: &launch.ContainmentIntent{},
	}
	server.addActiveJob(active)
	server.markAdmissionJob(jobID, &admissionInstance{})
	t.Cleanup(func() {
		server.removeActiveJob(jobID)
	})

	if err := server.cancelAdmissionWorkForShutdown(context.Background(), oldAdmission); err != nil {
		t.Fatalf("cancelAdmissionWorkForShutdown old snapshot error = %v", err)
	}
	select {
	case <-canceled:
		t.Fatal("old admission shutdown canceled replacement active job")
	default:
	}
	if got := active.requestedTerminal(); got != "" {
		t.Fatalf("replacement active job terminal request = %s, want none", got)
	}
}

func TestShutdownStateScopedToSequentialServeGeneration(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	if err := server.Shutdown(context.Background()); !errors.Is(err, ErrShutdownNotServing) {
		t.Fatalf("pre-Serve Shutdown error = %v, want ErrShutdownNotServing", err)
	}

	firstClose := newNotifyCloser()
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), firstClose)
	_, firstDone, firstListener := startTestServerWithBlockingListener(t, server)
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown error = %v", err)
	}
	firstListener.waitClosed(t)
	firstClose.waitClosed(t)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Serve did not stop after shutdown")
	}
	if err := server.Shutdown(context.Background()); !errors.Is(err, ErrShutdownNotServing) {
		t.Fatalf("between-Serve Shutdown error = %v, want ErrShutdownNotServing", err)
	}

	secondClose := newNotifyCloser()
	configureServeAdmissionCloser(t, server, newAdmissionFakeLaunchCustodian(t), memory.NewRepository(), authority.NewAnchorStore(), secondClose)
	_, secondDone, secondListener := startTestServerWithBlockingListener(t, server)
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown error = %v", err)
	}
	secondListener.waitClosed(t)
	secondClose.waitClosed(t)
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second Serve after Shutdown = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second Serve did not stop after shutdown")
	}
}

func TestShutdownRemovesOnlyOwnedPIDFile(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	pidPath := filepath.Join(root, "agentbus.pid")
	if err := os.WriteFile(pidPath, []byte("999999\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := server.removeOwnedPIDFile(context.Background(), "test mismatch"); err != nil {
		t.Fatalf("remove mismatched pid file error = %v", err)
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != "999999" {
		t.Fatalf("pid file after mismatch removal = %q", raw)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := server.removeOwnedPIDFile(context.Background(), "test owner"); err != nil {
		t.Fatalf("remove owned pid file error = %v", err)
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pid file stat after owner removal = %v, want not exist", err)
	}
}

func TestShutdownPreservesReplacementPIDFile(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	pidPath := filepath.Join(root, "agentbus.pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	replacementPID := strconv.Itoa(os.Getpid() + 100000)
	server.beforePIDFileQuarantineHook = func() {
		replacementPath := filepath.Join(root, "agentbus.pid.replacement")
		if err := os.WriteFile(replacementPath, []byte(replacementPID+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacementPath, pidPath); err != nil {
			t.Fatal(err)
		}
	}

	if err := server.shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error = %v", err)
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != replacementPID {
		t.Fatalf("pid file after replacement race = %q, want replacement pid %s", raw, replacementPID)
	}
}

func TestActiveWorkCountsAuthorityOwnedCustodyAndResultPublication(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	if server.activeWork() {
		t.Fatal("fresh admission server reported active work")
	}

	accepted := acceptIdentifiedAuthorityWork(t, server, "active-work-owned")
	if !server.activeWork() {
		t.Fatal("authority-owned accepted work was not counted as active")
	}
	finalizeAcceptedAuthorityWork(t, server, accepted)
	if server.activeWork() {
		t.Fatal("terminal authority work was still counted as active")
	}

	server.resultPublications.Add(1)
	if !server.activeWork() {
		t.Fatal("in-flight result publication was not counted as active")
	}
	server.resultPublications.Add(-1)
	if server.activeWork() {
		t.Fatal("completed result publication was still counted as active")
	}

	launcher.activeCustodies.Store(true)
	if !server.activeWork() {
		t.Fatal("active custody was not counted as active")
	}
	launcher.activeCustodies.Store(false)
	if server.activeWork() {
		t.Fatal("cleared active custody was still counted as active")
	}
}

func TestActiveWorkFailsClosedWhenAuthorityOwnershipCheckErrors(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	_ = acceptIdentifiedAuthorityWork(t, server, "active-work-fail-closed")

	reason := "test fail-stop makes ownership read unavailable"
	if err := server.admissionReady.FailStop(context.Background(), reason); err != nil {
		t.Fatal(err)
	}
	if !server.activeWork() {
		t.Fatal("activeWork returned idle after authority ownership check failed")
	}
}

func TestSafetyFailStopDrainWaitsForBoundWhenAuthorityOwnershipCheckErrors(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	drainTimeout := 120 * time.Millisecond
	var server *Server
	h := startTestServerWithHooks(t, backend, Config{IdleTimeout: -1}, func(s *Server) {
		server = s
		s.safetyDrainTimeout = drainTimeout
		enableTestAdmission(t, s, launcher)
	})
	_ = acceptIdentifiedAuthorityWork(t, server, "fail-stop-drain-owned")

	reason := "test fail-stop drain waits for bounded timeout"
	if err := server.admissionReady.FailStop(context.Background(), reason); err != nil {
		t.Fatal(err)
	}
	assertServerStillRunning(t, h.done, "authority ownership check failed during fail-stop drain")
	select {
	case err := <-h.done:
		if err == nil || !errors.Is(err, ErrSafetyFailStopped) || !strings.Contains(err.Error(), reason) || !strings.Contains(err.Error(), "safety drain timed out") {
			t.Fatalf("Serve error = %v, want timed-out safety fail-stop with reason", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not exit at the safety drain bound")
	}
}

func TestSafetyFailStopDrainDeadlineWinsOverStalledOwnershipProbe(t *testing.T) {
	var logs bytes.Buffer
	oldLogWriter := log.Writer()
	oldLogFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldLogWriter)
		log.SetFlags(oldLogFlags)
	}()

	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	drainTimeout := 80 * time.Millisecond
	var blocker *blockingRepositoryOwnedWorkChecker
	t.Cleanup(func() {
		if blocker != nil {
			blocker.releaseProbe()
		}
	})
	server, _, _ := newUnstartedTestServer(t, backend)
	server.safetyDrainTimeout = drainTimeout
	available := admissionSupportForClass(t, custodian.SupportAvailable, true, 1)
	var runtimeCloses atomic.Int64
	closeCapableRuntime := custodian.NewUnavailableRuntimeForTest(custodian.ErrSupervisorUnavailable, func() error {
		runtimeCloses.Add(1)
		return nil
	})
	servedRuntime := &servedAdmissionRuntime{
		runtime:          closeCapableRuntime,
		launchCustodian:  launcher,
		supportOverride:  &available,
		verifierOverride: launcher.verifier,
	}
	server.admissionRuntime = servedRuntime
	server.admissionRuntimeFactory = func(*Server) *servedAdmissionRuntime {
		return servedRuntime
	}
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	blocker = newBlockingRepositoryOwnedWorkChecker(server.admissionRepository)
	server.admissionOwnedWorkChecker = blocker
	cancel, done, listener := startTestServerWithBlockingListener(t, server)
	defer cancel()

	reason := "test fail-stop drain deadline ignores stalled ownership probe"
	trippedAt := time.Now()
	if err := server.admissionReady.FailStop(context.Background(), reason); err != nil {
		t.Fatal(err)
	}
	listener.waitClosed(t)
	select {
	case <-blocker.started:
	case <-time.After(time.Second):
		t.Fatal("ownership probe was not started during fail-stop drain")
	}
	if errObj := server.failStoppedRequestError(protocol.MethodJobSubmit); errObj == nil || errObj.Data.Code != protocol.ErrorBackendUnavailable || errObj.Data.AdmissionCause != protocol.AdmissionRejectRootFailStopped {
		t.Fatalf("job.submit rejection = %+v, want backend_unavailable after fail-stop", errObj)
	}

	select {
	case err := <-done:
		if err == nil || !errors.Is(err, ErrSafetyFailStopped) || !strings.Contains(err.Error(), reason) || !strings.Contains(err.Error(), "safety drain timed out") {
			t.Fatalf("Serve error = %v, want timed-out safety fail-stop with reason", err)
		}
		if elapsed := time.Since(trippedAt); elapsed > admissionRepositoryCloseTimeout+2*time.Second {
			t.Fatalf("Serve returned after %s, want bounded exit within admission repository close timeout", elapsed)
		}
	case <-time.After(admissionRepositoryCloseTimeout + 3*time.Second):
		t.Fatal("server did not exit while ownership probe was stalled")
	}
	if got := logs.String(); !strings.Contains(got, "admission repository close timed out; leaking handle at shutdown") {
		t.Fatalf("shutdown log = %q, want admission repository close timeout", got)
	}
	if got := runtimeCloses.Load(); got != 0 {
		t.Fatalf("runtime closes = %d, want skipped after repository close consumed deadline", got)
	}
	if !closeCapableRuntime.Consumed() {
		t.Fatal("runtime was not marked consumed when repository close consumed the deadline")
	}
	if err := server.Serve(context.Background()); !errors.Is(err, ErrRuntimeConsumed) {
		t.Fatalf("second Serve error = %v, want ErrRuntimeConsumed", err)
	}
}

func TestIdleShutdownFailsClosedOnAdmissionOwnershipReadError(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	var server *Server
	h := startTestServerWithHooks(t, backend, Config{
		IdleTimeout:       80 * time.Millisecond,
		IdleCheckInterval: 20 * time.Millisecond,
	}, func(s *Server) {
		server = s
		enableTestAdmission(t, s, launcher)
	})
	if server.admissionClose == nil {
		t.Fatal("admission repository closer is nil")
	}
	if err := server.admissionClose.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-h.done:
		t.Fatalf("server idle-shutdown after admission ownership read error: %v", err)
	case <-time.After(180 * time.Millisecond):
	}
	stopTestServer(t, h)
}

func TestIdleShutdownWaitsForAuthorityOwnedWork(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	var server *Server
	h := startTestServerWithHooks(t, backend, Config{
		IdleTimeout:       120 * time.Millisecond,
		IdleCheckInterval: 20 * time.Millisecond,
	}, func(s *Server) {
		server = s
		enableTestAdmission(t, s, launcher)
	})

	accepted := acceptIdentifiedAuthorityWork(t, server, "idle-authority-owned")
	select {
	case err := <-h.done:
		t.Fatalf("server stopped while authority work was active: %v", err)
	case <-time.After(260 * time.Millisecond):
	}

	finalizeAcceptedAuthorityWork(t, server, accepted)
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("server idle-shutdown error = %v", err)
		}
	case <-time.After(5 * time.Second):
		// Positive wait: idle shutdown needs a quiet IdleTimeout window plus
		// check-interval scheduling; under whole-repo -race sweep load a 1s
		// deadline flaked. The negative assertion above (no shutdown while
		// work is active) is unchanged.
		t.Fatal("server did not idle-shutdown after authority work became quiet")
	}
}

func TestIdentifiedFencedRetryBindsOrdinalTwoToDistinctLaunch(t *testing.T) {
	t.Parallel()
	var turns atomic.Int64
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		if turns.Add(1) == 1 {
			return []engine.Event{{Type: engine.EventAgentText, Text: `{"status":"bad"}`}}
		}
		return []engine.Event{{Type: engine.EventAgentText, Text: `{"status":"pass"}`}}
	}
	backend.started = make(chan struct{}, 2)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-retry",
		RequestID:    "request-retry",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "retry",
			Policy: &engine.TurnPolicy{
				Contract: &engine.ContractSpec{JSONSchema: json.RawMessage(`{
					"type":"object",
					"required":["status"],
					"properties":{"status":{"const":"pass"}}
				}`)},
				Retry: &engine.RetryPolicy{
					Max:      1,
					Template: "Your response missed: {{missing}}.",
				},
			},
		},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarts(t, backend, 2)
	var firstTurn, retryTurn fakeTurn
	select {
	case firstTurn = <-backend.turns:
	case <-time.After(5 * time.Second):
		t.Fatal("first backend turn was not recorded")
	}
	select {
	case retryTurn = <-backend.turns:
	case <-time.After(5 * time.Second):
		t.Fatal("retry backend turn was not recorded")
	}
	if firstTurn.Prompt != "retry" {
		t.Fatalf("first prompt = %q, want original task prompt", firstTurn.Prompt)
	}
	wantRetryInstruction := "/status: value must be 'pass'"
	if !strings.Contains(retryTurn.Prompt, wantRetryInstruction) {
		t.Fatalf("retry prompt = %q, want instruction %q", retryTurn.Prompt, wantRetryInstruction)
	}
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)

	ordinals := launcher.preparedOrdinals()
	if len(ordinals) != 2 || ordinals[0] != model.LaunchOrdinalOne || ordinals[1] != model.LaunchOrdinalTwo {
		t.Fatalf("prepared ordinals = %v, want [1 2]", ordinals)
	}
	record := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	first := record.Attempt.Launches.First
	second := record.Attempt.Launches.Second
	if first == nil || first.Group == nil || first.Grant == nil || second == nil || second.Group == nil || second.Grant == nil {
		t.Fatalf("launch proofs incomplete: first=%+v second=%+v", first, second)
	}
	if first.Group.Equal(*second.Group) {
		t.Fatalf("ordinal groups are not distinct: %+v", first.Group)
	}
	if first.Grant.Nonce == second.Grant.Nonce {
		t.Fatalf("ordinal grants reused nonce %s", first.Grant.Nonce)
	}
	if !first.Group.Launch.Equal(model.LaunchKey{Attempt: record.Attempt.Ref, Ordinal: model.LaunchOrdinalOne}) {
		t.Fatalf("ordinal 1 group launch key = %+v", first.Group.Launch)
	}
	if !second.Group.Launch.Equal(model.LaunchKey{Attempt: record.Attempt.Ref, Ordinal: model.LaunchOrdinalTwo}) {
		t.Fatalf("ordinal 2 group launch key = %+v", second.Group.Launch)
	}
}

func TestCorrectiveRetryReplacesObservedWorkspaceWriteItemCount(t *testing.T) {
	t.Parallel()
	var turns atomic.Int64
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		if turns.Add(1) == 1 {
			return []engine.Event{
				{Type: engine.EventToolUse, ObservedWorkspaceWriteItem: true},
				{Type: engine.EventToolUse, ObservedWorkspaceWriteItem: true},
				{Type: engine.EventAgentText, Text: `{"status":"bad"}`},
			}
		}
		return []engine.Event{
			{Type: engine.EventToolUse, ObservedWorkspaceWriteItem: true},
			{Type: engine.EventAgentText, Text: `{"status":"pass"}`},
		}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-observed-write-retry",
		RequestID:    "request-observed-write-retry",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "retry observed writes",
			Policy: &engine.TurnPolicy{
				Contract: &engine.ContractSpec{JSONSchema: json.RawMessage(`{"type":"object","required":["status"],"properties":{"status":{"const":"pass"}}}`)},
				Retry:    &engine.RetryPolicy{Max: 1, Template: "Your response missed: {{missing}}."},
			},
		},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if got := record.ObservedWorkspaceWriteItemCount; got != 1 {
		t.Fatalf("terminal observed workspace-write count = %d, want retry count 1", got)
	}
	if got := record.ObservedWorkspaceWriteItemCountAttemptOrdinal; got != model.LaunchOrdinalTwo {
		t.Fatalf("terminal observed workspace-write ordinal = %s, want retry ordinal %s", got, model.LaunchOrdinalTwo)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if got := result.ObservedWorkspaceWriteItemCount; got == nil || *got != 1 {
		t.Fatalf("job.result observed workspace-write count = %v, want retry count 1", got)
	}
}

func TestCancellationSettlesBufferedWorkspaceWriteBeforeTerminalCommit(t *testing.T) {
	t.Parallel()
	backend := newAdmissionLivenessBackend("fake")
	backend.eventStream = make(chan engine.Event, 2)
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-cancel-buffered-write",
		RequestID:    "request-cancel-buffered-write",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: true, Prompt: "cancel after buffered write"},
	})
	waitBackendStarted(t, backend.fakeBackend)
	cancelParams := mustMarshal(t, protocol.JobCancelParams{JobID: job.JobID})
	cancelDone := make(chan requestOutcome, 1)
	go func() {
		cancelDone <- server.handleJobCancel(cancelParams)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		active := server.lookupActiveJob(job.JobID)
		if active != nil && active.requestedTerminal() == engine.StateCanceled {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if active := server.lookupActiveJob(job.JobID); active == nil || active.requestedTerminal() != engine.StateCanceled {
		t.Fatal("cancellation did not reach the active attempt")
	}
	backend.eventStream <- engine.Event{Type: engine.EventToolUse, ObservedWorkspaceWriteItem: true}
	close(backend.eventStream)
	select {
	case outcome := <-cancelDone:
		if outcome.err != nil {
			t.Fatalf("job.cancel error = %+v", outcome.err)
		}
		canceled, ok := outcome.result.(protocol.JobCancelResult)
		if !ok {
			t.Fatalf("job.cancel result type = %T", outcome.result)
		}
		if canceled.State != engine.StateCanceled {
			t.Fatalf("job.cancel = %+v, want canceled", canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job.cancel did not return after buffered workspace-write event")
	}
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if got := record.ObservedWorkspaceWriteItemCount; got != 1 {
		t.Fatalf("terminal observed workspace-write count = %d, want 1", got)
	}
}

func TestDeferredDrainRecordsLateTransportFrameDropsAfterTerminalCommit(t *testing.T) {
	t.Parallel()
	backend := newAdmissionLivenessBackend("fake")
	backend.eventStream = make(chan engine.Event)
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-cancel-late-drops",
		RequestID:    "request-cancel-late-drops",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "cancel before late frame drops"},
	})
	waitBackendStarted(t, backend.fakeBackend)

	cancelParams := mustMarshal(t, protocol.JobCancelParams{JobID: job.JobID})
	cancelDone := make(chan requestOutcome, 1)
	go func() {
		cancelDone <- server.handleJobCancel(cancelParams)
	}()
	select {
	case outcome := <-cancelDone:
		if outcome.err != nil {
			t.Fatalf("job.cancel error = %+v", outcome.err)
		}
		canceled, ok := outcome.result.(protocol.JobCancelResult)
		if !ok {
			t.Fatalf("job.cancel result type = %T", outcome.result)
		}
		if canceled.State != engine.StateCanceled {
			t.Fatalf("job.cancel = %+v, want canceled", canceled)
		}
	case <-time.After(admissionNativeInterruptGrace + 2*time.Second):
		t.Fatal("job.cancel did not return after bounded diagnostic settle")
	}
	waitAdmissionSafetyTerminal(t, server, job.JobID)

	drops := engine.TransportFrameDrops{Count: 1, Bytes: 1024, RedactedPrefix: "method=turn/completed"}
	backend.eventStream <- engine.Event{Type: engine.EventWarning, Metadata: drops.EventMetadata()}
	close(backend.eventStream)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		record := loadAdmissionSafetyRecord(t, server, job.JobID)
		if record.TransportFrameDrops != nil {
			if *record.TransportFrameDrops != drops {
				t.Fatalf("terminal transport frame drops = %#v, want %#v", record.TransportFrameDrops, drops)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("late deferred frame-drop metadata was not recorded")
}

func TestObservedWorkspaceWriteAttemptSnapshotIsAtomic(t *testing.T) {
	active := &activeJob{
		observedWorkspaceWriteItemCount: 7,
		observedWorkspaceWriteAttempt:   model.LaunchOrdinalOne,
	}
	resetReached := make(chan struct{})
	allowOrdinalStore := make(chan struct{})
	active.observedWorkspaceWriteAfterCountResetForTest = func() {
		close(resetReached)
		<-allowOrdinalStore
	}

	beginDone := make(chan struct{})
	go func() {
		active.beginObservedWorkspaceWriteAttempt(model.LaunchOrdinalTwo)
		close(beginDone)
	}()
	<-resetReached

	type snapshot struct {
		count   uint64
		ordinal model.LaunchOrdinal
	}
	snapshots := make(chan snapshot, 1)
	snapshotStarted := make(chan struct{})
	active.observedWorkspaceWriteBeforeTerminalSnapshotForTest = func() {
		close(snapshotStarted)
	}
	go func() {
		count, ordinal := active.observedWorkspaceWriteItemCountForTerminal()
		snapshots <- snapshot{count: count, ordinal: ordinal}
	}()
	<-snapshotStarted

	close(allowOrdinalStore)
	<-beginDone
	got := <-snapshots
	if got.count != 0 || got.ordinal != model.LaunchOrdinalTwo {
		t.Fatalf("snapshot = (%d, %s), want (0, %s)", got.count, got.ordinal, model.LaunchOrdinalTwo)
	}
}

func TestRemovedAndUnknownMethodsReturnMethodNotFound(t *testing.T) {
	t.Parallel()
	for _, method := range []string{
		"session.start",
		"session.resume",
		"session.list",
		"turn.start",
		"turn.interrupt",
		"turn.event",
		"turn.result",
		"unknown.method",
	} {
		method := method
		for _, failStopped := range []bool{false, true} {
			failStopped := failStopped
			t.Run(fmt.Sprintf("%s/failStopped=%t", method, failStopped), func(t *testing.T) {
				server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
				if failStopped {
					server.safetyLatch.Trip(errors.New("test fail-stop"))
				}
				conn := &connection{server: server, hello: true}
				outcome := server.handle(context.Background(), conn, protocol.Request{
					JSONRPC: "2.0",
					ID:      json.RawMessage(`"removed"`),
					Method:  method,
				})
				if outcome.err == nil {
					t.Fatalf("%s result = %+v, want method_not_found", method, outcome.result)
				}
				if outcome.err.Code != -32601 || outcome.err.Data.Code != protocol.ErrorMethodNotFound {
					t.Fatalf("%s error = %+v, want -32601/%s", method, outcome.err, protocol.ErrorMethodNotFound)
				}
			})
		}
	}
}

func TestTerminalErrorEventFailsJob(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{
			{Type: engine.EventAgentText, Text: "partial"},
			{Type: engine.EventTerminalError, Text: "backend exploded"},
		}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-terminal-error",
		RequestID:    "request-terminal-error",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "fail"},
	})
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateFailed || result.Result != nil {
		t.Fatalf("terminal error result = %+v", result)
	}
}

func TestFailedAdmissionJobCapturesAndAdvertisesBackendLogs(t *testing.T) {
	t.Parallel()
	backend := newLogCaptureBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-log-capture",
		RequestID:    "request-log-capture",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "fail"},
	})

	var input engine.TurnInput
	select {
	case input = <-backend.input:
	case <-time.After(5 * time.Second):
		t.Fatal("admission backend did not receive a turn input")
	}
	if input.LogPaths.Stdout == "" || input.LogPaths.Stderr == "" {
		t.Fatalf("turn input log paths = %+v, want stdout and stderr paths", input.LogPaths)
	}
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateFailed || status.Jobs[0].LogPaths == nil {
		t.Fatalf("terminal status = %+v, want failed job with log paths", status)
	}
	if got := *status.Jobs[0].LogPaths; got != input.LogPaths {
		t.Fatalf("advertised log paths = %+v, want %+v", got, input.LogPaths)
	}
	for path, want := range map[string]string{input.LogPaths.Stdout: "backend stdout", input.LogPaths.Stderr: "backend stderr"} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read backend log %s: %v", path, err)
		}
		if !strings.Contains(string(content), want) {
			t.Fatalf("backend log %s = %q, want %q", path, content, want)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("backend log %s mode = %o, want 600", path, got)
		}
	}
}

func TestSuccessfulAdmissionJobDoesNotAdvertiseEmptyLogPaths(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-empty-logs",
		RequestID:    "request-empty-logs",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "succeed"},
	})
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("terminal status = %+v, want completed job", status)
	}
	if status.Jobs[0].LogPaths != nil {
		t.Fatalf("completed status log paths = %+v, want omitted empty paths", status.Jobs[0].LogPaths)
	}
	raw, err := json.Marshal(status.Jobs[0])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["logPaths"]; ok {
		t.Fatalf("completed status JSON = %s, unexpectedly contains logPaths", raw)
	}
}

func TestSuccessfulAdmissionJobDiscardsNonemptyBackendLogs(t *testing.T) {
	t.Parallel()
	backend := newLogCaptureBackend("fake")
	backend.terminal = engine.Event{Type: engine.EventAgentText, Text: "done"}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-successful-log-discard",
		RequestID:    "request-successful-log-discard",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "succeed"},
	})
	input := <-backend.input
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	waitAdmissionLogPairAbsent(t, input.LogPaths)
	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 || status.Jobs[0].LogPaths != nil {
		t.Fatalf("successful terminal status = %+v, want no log paths", status)
	}
}

func TestTerminalErrorWaitsForLogWritersBeforeCleanup(t *testing.T) {
	t.Parallel()
	backend := newLogCaptureBackend("fake")
	releaseWriters := make(chan struct{})
	backend.writeAfterTerminal = releaseWriters
	backend.writersOpened = make(chan struct{})
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-terminal-log-drain",
		RequestID:    "request-terminal-log-drain",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "fail"},
	})
	input := <-backend.input
	select {
	case <-backend.writersOpened:
	case <-time.After(time.Second):
		t.Fatal("backend did not open log writers")
	}
	time.Sleep(100 * time.Millisecond)
	if record := loadAdmissionSafetyRecord(t, server, job.JobID); record.Terminal != nil {
		t.Fatalf("job terminalized before backend log writers finished: %+v", record.Terminal)
	}
	close(releaseWriters)
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	for _, path := range []string{input.LogPaths.Stdout, input.LogPaths.Stderr} {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read retained backend log %s: %v", path, err)
		}
		if !strings.Contains(string(content), "backend") {
			t.Fatalf("retained backend log %s = %q, want backend content", path, content)
		}
	}
}

func TestAdmissionLogSweepExpiresAndCapsTerminalLogs(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	clock := &steppedClock{now: time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)}
	server.clock = clock
	server.admissionLogRetentionCap = 20
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	expired := acceptAndFailAuthorityWorkForLogRetention(t, server, "retention-expired", clock.Now())
	writeAdmissionLogPair(t, server, expired.Record, "expired")
	clock.Set(clock.Now().Add(engine.DefaultLogTTL + time.Second))
	old := acceptAndFailAuthorityWorkForLogRetention(t, server, "retention-old", clock.Now())
	writeAdmissionLogPair(t, server, old.Record, "old")
	clock.Set(clock.Now().Add(time.Second))
	newer := acceptAndFailAuthorityWorkForLogRetention(t, server, "retention-new", clock.Now())
	writeAdmissionLogPair(t, server, newer.Record, "new")
	active := acceptAuthorityWorkForLogRetention(t, server, "retention-active")
	writeAdmissionLogPair(t, server, active.Record, "active")

	server.gcInterval = 10 * time.Millisecond
	sweepCtx, stopSweep := context.WithCancel(context.Background())
	defer stopSweep()
	go server.admissionLogSweepLoop(sweepCtx)
	assertAdmissionLogPairAbsent(t, server, expired.Record)
	assertAdmissionLogPairAbsent(t, server, old.Record)
	assertAdmissionLogPairPresent(t, server, newer.Record)
	assertAdmissionLogPairPresent(t, server, active.Record)
}

func TestAdmissionTerminalLogSettlementImmediatelyEnforcesRetentionCap(t *testing.T) {
	t.Parallel()
	backend := newLogCaptureBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.gcInterval = time.Hour
	// The test backend writes two 15-byte log files per failed turn. Keep one
	// pair but not two, so only terminal settlement can evict the older pair.
	server.admissionLogRetentionCap = 31
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	first := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-immediate-log-retention",
		RequestID:    "request-immediate-log-retention-first",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "first failure"},
	})
	firstInput := <-backend.input
	waitAdmissionSafetyTerminal(t, server, first.JobID)
	if firstBytes := backendLogBytes(firstInput.LogPaths); firstBytes != 30 {
		t.Fatalf("first terminal log bytes = %d, want 30", firstBytes)
	}

	second := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-immediate-log-retention",
		RequestID:    "request-immediate-log-retention-second",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "second failure"},
	})
	secondInput := <-backend.input
	waitAdmissionSafetyTerminal(t, server, second.JobID)
	waitAdmissionLogPairAbsent(t, firstInput.LogPaths)
	for _, path := range []string{secondInput.LogPaths.Stdout, secondInput.LogPaths.Stderr} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("newest terminal log %s stat error = %v, want retained log", path, err)
		}
	}
}

func TestImmediateAdmissionLogRetentionPreservesActiveAndDrainingLogs(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.admissionLogRetentionCap = 1
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	terminalAt := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	active := acceptAuthorityWorkForLogRetention(t, server, "immediate-retention-active")
	writeAdmissionLogPair(t, server, active.Record, "active")
	server.addActiveJob(&activeJob{jobID: active.Record.JobID.String()})
	t.Cleanup(func() { server.removeActiveJob(active.Record.JobID.String()) })

	draining := acceptAndFailAuthorityWorkForLogRetention(t, server, "immediate-retention-draining", terminalAt)
	writeAdmissionLogPair(t, server, draining.Record, "draining")
	drainDone := make(chan struct{})
	server.mu.Lock()
	server.admissionLogDrains[draining.Record.JobID.String()] = drainDone
	server.mu.Unlock()
	t.Cleanup(func() {
		server.finishAdmissionLogDrain(draining.Record.JobID.String(), drainDone)
		close(drainDone)
	})

	evictable := acceptAndFailAuthorityWorkForLogRetention(t, server, "immediate-retention-evictable", terminalAt.Add(time.Second))
	writeAdmissionLogPair(t, server, evictable.Record, "evictable")
	server.enforceAdmissionLogRetention(active.Record.WorkspaceLayoutKey.String())

	assertAdmissionLogPairPresent(t, server, active.Record)
	assertAdmissionLogPairPresent(t, server, draining.Record)
	assertAdmissionLogPairAbsent(t, server, evictable.Record)
}

func acceptAndFailAuthorityWorkForLogRetention(t *testing.T, server *Server, requestID string, terminalAt time.Time) authority.AcceptResult {
	t.Helper()
	accepted := acceptAuthorityWorkForLogRetention(t, server, requestID)
	if _, err := server.admissionReady.Finalize(context.Background(), accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome:             model.OutcomeFailed,
		Cause:               model.CauseDaemonRestartedBeforeAuthorization,
		FinalAttemptEndedAt: &terminalAt,
	}); err != nil {
		t.Fatal(err)
	}
	return accepted
}

func acceptAuthorityWorkForLogRetention(t *testing.T, server *Server, requestID string) authority.AcceptResult {
	t.Helper()
	accepted, err := server.admissionReady.Accept(context.Background(), authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: model.WorkspaceKey("workspace-log-retention"),
			RequestID:    model.RequestID(requestID),
		},
		WorkspaceLayoutKey: model.WorkspaceKey(strings.Repeat("a", 64)),
		TaskIdentity:       model.NewSHA256TaskIdentity([]byte(requestID)),
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          "session-" + requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return accepted
}

func writeAdmissionLogPair(t *testing.T, server *Server, record model.SafetyRecord, text string) {
	t.Helper()
	layout, err := authorityResultLayout(server.stateRoot, record)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := engine.LogPathsForLayout(layout, record.JobID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		writer, err := engine.NewCappedLogWriter(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(writer, text); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func assertAdmissionLogPairAbsent(t *testing.T, server *Server, record model.SafetyRecord) {
	t.Helper()
	layout, err := authorityResultLayout(server.stateRoot, record)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := engine.LogPathsForLayout(layout, record.JobID.String())
	if err != nil {
		t.Fatal(err)
	}
	waitAdmissionLogPairAbsent(t, paths)
}

func waitAdmissionLogPairAbsent(t *testing.T, paths engine.LogPaths) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		missing := true
		for _, path := range []string{paths.Stdout, paths.Stderr} {
			if _, err := os.Stat(path); err == nil {
				missing = false
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backend log %s stat error = %v", path, err)
			}
		}
		if missing {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("backend logs %+v still exist after cleanup", paths)
}

func assertAdmissionLogPairPresent(t *testing.T, server *Server, record model.SafetyRecord) {
	t.Helper()
	layout, err := authorityResultLayout(server.stateRoot, record)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := engine.LogPathsForLayout(layout, record.JobID.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("backend log %s stat error = %v, want retained log", path, err)
		}
	}
}

func TestTerminalErrorEventDrainsStreamBeforeFailingJob(t *testing.T) {
	t.Parallel()
	backend := newAdmissionLivenessBackend("fake")
	backend.eventStream = make(chan engine.Event, 16)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-terminal-error-drain",
		RequestID:    "request-terminal-error-drain",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "fail after cleanup"},
	})
	waitBackendStarted(t, backend.fakeBackend)
	select {
	case <-backend.runnerObserved:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal-error runner observation did not finish")
	}

	stopProducer := make(chan struct{})
	stop := sync.OnceFunc(func() { close(stopProducer) })
	producerClosed := make(chan struct{})
	t.Cleanup(func() {
		stop()
		select {
		case <-producerClosed:
		case <-time.After(time.Second):
			t.Error("terminal-error producer did not stop")
		}
	})
	go func() {
		defer close(producerClosed)
		defer close(backend.eventStream)
		send := func(event engine.Event) bool {
			select {
			case backend.eventStream <- event:
				return true
			case <-stopProducer:
				return false
			}
		}
		if !send(engine.Event{Type: engine.EventTerminalError, Text: "backend exploded"}) {
			return
		}
		// Seventeen tail events overflow the 16-event source buffer unless the
		// consumer keeps draining after it sees the terminal error.
		for i := 0; i < 17; i++ {
			if !send(engine.Event{Type: engine.EventAgentText, Text: "discarded tail"}) {
				return
			}
		}
	}()

	select {
	case <-producerClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal-error producer blocked before closing its event stream")
	}
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateFailed || result.Result != nil {
		t.Fatalf("terminal error result = %+v", result)
	}
}

func TestCanceledAttemptReturnsWhileEventStreamRemainsOpen(t *testing.T) {
	t.Parallel()
	backend := newAdmissionLivenessBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	t.Cleanup(func() { close(backend.eventStream) })

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-canceled-open-stream",
		RequestID:    "request-canceled-open-stream",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "cancel with open stream"},
	})
	waitBackendStarted(t, backend.fakeBackend)

	if err := server.requestActiveJobShutdownCancel(context.Background(), job.JobID); err != nil {
		t.Fatal(err)
	}
	waitActiveJobGone(t, server, job.JobID)
}

func TestResultMessageWinsOverAssistantTextWithoutDuplication(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{
			{Type: engine.EventAgentText, Text: "hello"},
			{Type: engine.EventResultMessage, Text: "hello"},
		}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-result-message",
		RequestID:    "request-result-message",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "dedupe"},
	})
	waitAdmissionSafetyTerminal(t, server, job.JobID)
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.Result == nil || result.Result.Text != "hello" {
		t.Fatalf("result = %+v, want single authoritative hello", result.Result)
	}
}

func TestAdmissionRunPreservesResultWhenCleanupUnresolvedAfterZeroExit(t *testing.T) {
	t.Parallel()
	unresolved := &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonAbsenceDeadlineExceeded,
		Decision: model.SignalDirectly,
	}
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventResultMessage, Text: "final report"}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitErr = unresolved
	launcher.containErr = unresolved
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-cleanup-unresolved-complete",
		RequestID:    "request-cleanup-unresolved-complete",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "cleanup unresolved"},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCompleted || record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("terminal = %+v, want completed unresolved absence", record.Terminal)
	}
	if first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne); !ok || first.Quiescence != nil {
		t.Fatalf("ordinal 1 quiescence = %+v, want absent", first.Quiescence)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateCompleted || result.CleanupDisposition != model.CleanupDispositionUnresolved.String() {
		t.Fatalf("job result state/cleanup = %s/%s, want completed/unresolved", result.State, result.CleanupDisposition)
	}
	if result.Result == nil || result.Result.Text != "final report" {
		t.Fatalf("result payload = %+v, want preserved final report", result.Result)
	}
}

func TestAdmissionRunPreservesResultWhenPostQuiescenceCleanupWarns(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventResultMessage, Text: "verified report"}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.cleanupErr = errors.New("cleanup warning after proof")
	enableTestAdmission(t, server, launcher)

	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-cleanup-warning-complete",
		RequestID:    "request-cleanup-warning-complete",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "cleanup warning"},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("terminal = %+v, want completed", record.Terminal)
	}
	if first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne); !ok || first.Quiescence == nil {
		t.Fatalf("ordinal 1 launch = %+v, want quiescence recorded", first)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateCompleted || result.CleanupDisposition != model.CleanupDispositionVerifiedAbsent.String() {
		t.Fatalf("job result state/cleanup = %s/%s, want completed/verified_absent", result.State, result.CleanupDisposition)
	}
	if result.Result == nil || result.Result.Text != "verified report" {
		t.Fatalf("result payload = %+v, want preserved verified report", result.Result)
	}
}

func TestAdmissionActiveCancelCommitsUnresolvedCleanupInsteadOfRPCError(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	unresolved := &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonProbeUnprovable,
		Decision: model.Unprovable,
	}
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	launcher.containErr = unresolved
	enableTestAdmission(t, server, launcher)
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-active-cancel-unresolved",
		RequestID:    "request-active-cancel-unresolved",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold for cancel"},
	})
	waitBackendStarted(t, backend)

	cancel := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: job.JobID})
	if cancel.State != engine.StateCanceled {
		t.Fatalf("job.cancel = %+v, want canceled", cancel)
	}
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled || record.Terminal.Proof != model.ProofUnresolvedAbsence {
		t.Fatalf("terminal = %+v, want canceled unresolved absence", record.Terminal)
	}
	if record.CancellationOrigin != engine.CancellationOriginClientRequest || record.CancellationReason != "client requested cancellation" {
		t.Fatalf("admission cancellation metadata = (%q, %q), want client request", record.CancellationOrigin, record.CancellationReason)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateCanceled || result.CleanupDisposition != model.CleanupDispositionUnresolved.String() {
		t.Fatalf("job result state/cleanup = %s/%s, want canceled/unresolved", result.State, result.CleanupDisposition)
	}
	if err := server.safetyFailStopErr(); err != nil {
		t.Fatalf("authority root fail-stopped after cleanup uncertainty: %v", err)
	}
}

func TestAdmissionActiveCancelNativeRetirementSkipsRawExecutionError(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "native-retirement-cancel")
	jobID := accepted.Record.JobID.String()
	executionErr := errors.New("provider process exited with code 37")
	session := &nativeSettledSession{id: "native-settled-session", settled: true}
	running := &recordingRunningCommand{interruptErr: executionErr}
	active := &activeJob{
		jobID:             jobID,
		sessionID:         session.ID(),
		session:           session,
		admissionCommand:  running,
		containmentIntent: &launch.ContainmentIntent{},
	}
	server.addActiveJob(active)
	server.markAdmissionJob(jobID, server.admissionInstance)
	t.Cleanup(func() {
		server.removeActiveJob(jobID)
	})

	outcome := server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: jobID}))
	if outcome.err != nil {
		t.Fatalf("job.cancel error = %+v, want canceled despite raw execution error %q", outcome.err, executionErr)
	}
	canceled, ok := outcome.result.(protocol.JobCancelResult)
	if !ok {
		t.Fatalf("job.cancel result type = %T", outcome.result)
	}
	if canceled.JobID != jobID || canceled.State != engine.StateCanceled {
		t.Fatalf("job.cancel result = %+v, want canceled job %s", canceled, jobID)
	}
	if got := session.nativeCalls.Load(); got != 1 {
		t.Fatalf("native interrupt calls = %d, want 1", got)
	}
	if got := running.interrupts.Load(); got != 0 {
		t.Fatalf("raw admission interrupts = %d, want 0 after native settlement", got)
	}
	record := waitAdmissionSafetyTerminal(t, server, jobID)
	if record.Cancel == nil {
		t.Fatalf("cancel fact missing after native-settled cancel: %+v", record)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled {
		t.Fatalf("terminal = %+v, want canceled", record.Terminal)
	}
	if err := server.safetyFailStopErr(); err != nil {
		t.Fatalf("authority root fail-stopped after native-settled cancel: %v", err)
	}
}

func TestAdmissionUnresolvedTerminalAbandonsActiveCustodyAndDrains(t *testing.T) {
	t.Parallel()
	unresolved := &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonAbsenceDeadlineExceeded,
		Decision: model.SignalDirectly,
	}
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 2)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitErr = unresolved
	launcher.containErr = unresolved
	launcher.activeCustodies.Store(true)
	enableTestAdmission(t, server, launcher)

	first := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-abandon-unresolved",
		RequestID:    "request-abandon-unresolved-first",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "first unresolved"},
	})
	waitBackendStarted(t, backend)
	record := waitAdmissionSafetyTerminal(t, server, first.JobID)
	waitActiveJobGone(t, server, first.JobID)
	if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
	if launcher.HasActiveCustodies() {
		t.Fatal("launcher still reports active custody after unresolved terminal")
	}
	if got := launcher.abandonCount(); got != 1 {
		t.Fatalf("abandon count = %d, want 1", got)
	}
	if server.activeWorkWithContext(context.Background()) {
		t.Fatal("server still treats unresolved terminal job as active work")
	}

	launcher.waitErr = nil
	launcher.containErr = nil
	second := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-abandon-unresolved",
		RequestID:    "request-abandon-unresolved-second",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "second runs"},
	})
	waitBackendStarted(t, backend)
	secondRecord := waitAdmissionSafetyTerminal(t, server, second.JobID)
	if secondRecord.Terminal == nil || secondRecord.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("second terminal = %+v, want completed", secondRecord.Terminal)
	}
}

func TestConcurrentBackgroundJobs(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 2)
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var one, two protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-concurrent-one",
		RequestID:    "request-concurrent-one",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "one"},
	}), &one)
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-concurrent-two",
		RequestID:    "request-concurrent-two",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "two"},
	}), &two)
	for i := 0; i < 2; i++ {
		select {
		case <-backend.started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent jobs to start")
		}
	}
	close(release)
	waitJobState(t, conn, r, one.JobID, engine.StateCompleted)
	waitJobState(t, conn, r, two.JobID, engine.StateCompleted)
}

func TestDeferredLaunchRunsOnlyAfterSuccessfulAck(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-deferred-launch",
		RequestID:    "request-deferred-launch",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	if got := conn.writesString(); !strings.Contains(got, `"result"`) {
		t.Fatalf("response was not written before launch: %s", got)
	}
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("job did not launch after successful ack")
	}
	close(release)
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)
}

func TestLiveReleaseAckLossUnprovableAbsenceTerminalizesOrphanedWithoutRelaunch(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.releaseOutcome = custodian.ReleaseOutcomeUnknown
	launcher.releaseErr = errors.New("release ack lost after send")
	launcher.containErr = &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonProbeUnprovable,
		Decision: model.Unprovable,
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, launcher)
	params := protocol.JobSubmitParams{
		WorkspaceKey: "workspace-live-release-ack-loss-unresolved",
		RequestID:    "request-live-release-ack-loss-unresolved",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "ack lost"},
	}

	submitted := submitIdentifiedViaScriptedRequest(t, server, params)
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Terminal == nil ||
		record.Terminal.Outcome != model.OutcomeOrphaned ||
		record.Terminal.Proof != model.ProofUnresolvedAbsence ||
		record.Terminal.Cause != model.CauseReleaseOutcomeUnknown {
		t.Fatalf("terminal = %+v, want orphaned release_outcome_unknown unresolved absence", record.Terminal)
	}
	if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionUnresolved {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
	}
	if got := launcher.releaseCount(); got != 1 {
		t.Fatalf("release attempts = %d, want 1", got)
	}
	if got := launcher.containCount(); got != 1 {
		t.Fatalf("contain attempts = %d, want 1", got)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend sessions = %d, want 1", got)
	}

	replay := replayIdentifiedSubmit(t, server, params)
	if replay.JobID != submitted.JobID || !replay.Deduplicated || replay.State != engine.StateOrphaned {
		t.Fatalf("replay = %+v, want same orphaned terminal job %s", replay, submitted.JobID)
	}
	if got := launcher.releaseCount(); got != 1 {
		t.Fatalf("release attempts after replay = %d, want unchanged 1", got)
	}
	if got := launcher.containCount(); got != 1 {
		t.Fatalf("contain attempts after replay = %d, want unchanged 1", got)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend sessions after replay = %d, want unchanged 1", got)
	}
	if reason := server.safetyLatch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func TestHeartbeatRacingCompletionDoesNotResurrectTerminalRecord(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	contract := &engine.ContractSpec{Shape: json.RawMessage(`{"delegateContract":"report-v1"}`)}
	if err := server.createQueuedRecord(store, jobID, "ses_race", "fake", nil, &engine.TurnPolicy{Contract: contract}, contract, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateStarting); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateRunning); err != nil {
		t.Fatal(err)
	}

	stamp := &engine.ContractStamp{Status: engine.ContractCompliant, Attempts: 1, ValidatedAt: time.Now().UTC()}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- server.finalizeTerminal(jobRun{jobID: jobID, store: store, contract: contract}, engine.StateCompleted, "PASS\n", stamp)
	}()
	go func() {
		<-start
		_, err := server.refreshHeartbeat(store, jobID)
		errs <- err
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	active, err := server.refreshHeartbeat(store, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("heartbeat treated terminal job as active")
	}
	record, err := store.Load(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != engine.StateCompleted || record.Result == nil || record.Contract == nil || record.ResolvedContract == nil {
		t.Fatalf("terminal record was not preserved: %+v", record)
	}
}

func TestFinalizeCompletedSalvagesOrphanedJob(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_salvage", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, state := range []engine.JobState{engine.StateStarting, engine.StateRunning, engine.StateOrphaned} {
		if err := server.transitionRecord(store, jobID, state); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.finalizeTerminal(jobRun{jobID: jobID, store: store, authoritativeCompletion: true}, engine.StateCompleted, "done", nil); err != nil {
		t.Fatal(err)
	}
	record, err := store.Load(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != engine.StateCompleted || record.Result == nil || !record.LateFinalization || len(record.Warnings) != 0 {
		t.Fatalf("salvaged record = %+v", record)
	}
	assertJobHandlerError(t, server.handleJobStatus(mustMarshal(t, protocol.JobStatusParams{JobID: jobID})), protocol.ErrorUnknownJob, "", jobID)
	assertJobHandlerError(t, server.handleJobResult(mustMarshal(t, protocol.JobResultParams{JobID: jobID})), protocol.ErrorUnknownJob, "", jobID)
}

func TestFinalizeCompletedSalvagesReapedJobOnlyWithAuthoritativeCompletion(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_reaped", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, state := range []engine.JobState{engine.StateStarting, engine.StateRunning, engine.StateOrphaned} {
		if err := server.transitionRecord(store, jobID, state); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Update(jobID, func(record *engine.JobRecord) (bool, error) {
		if err := record.Transition(engine.StateReaped, server.clock.Now().UTC()); err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := server.finalizeTerminal(jobRun{jobID: jobID, store: store}, engine.StateCompleted, "ignored", nil); err != nil {
		t.Fatal(err)
	}
	record, err := store.Load(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != engine.StateReaped || record.Result != nil {
		t.Fatalf("reaped record changed = %+v", record)
	}
	if err := server.finalizeTerminal(jobRun{jobID: jobID, store: store, authoritativeCompletion: true}, engine.StateCompleted, "salvaged", nil); err != nil {
		t.Fatal(err)
	}
	record, err = store.Load(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != engine.StateCompleted || record.Result == nil || !record.LateFinalization || !strings.Contains(record.Result.Text, "salvaged") || len(record.Warnings) != 0 {
		t.Fatalf("authoritative completion was not salvaged = %+v", record)
	}
}

func TestBlockedUpdateRacesHeartbeatReaperAndFinalizer(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	store, err := engine.NewStore(engine.StoreConfig{
		Root: filepath.Join(t.TempDir(), "state"), CWD: t.TempDir(), Clock: engine.ClockFunc(func() time.Time { return base }),
		Processes: fakeProcessTable{}, LeaseDuration: time.Minute, OrphanGrace: time.Minute,
		BeforeUpdate: func(string) { once.Do(func() { close(blocked); <-release }) },
	})
	if err != nil {
		t.Fatal(err)
	}
	record := &engine.JobRecord{JobID: "job_blocked_update", State: engine.StateRunning, UpdatedAt: base, Lease: engine.Lease{ExpiresAt: base.Add(time.Minute)}}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	server := &Server{clock: engine.ClockFunc(func() time.Time { return base }), inlineResultCap: 1024}
	finalized := make(chan error, 1)
	go func() {
		finalized <- server.finalizeTerminal(jobRun{jobID: record.JobID, store: store, authoritativeCompletion: true}, engine.StateCompleted, "done", nil)
	}()
	<-blocked
	heartbeatDone := make(chan error, 1)
	go func() {
		_, err := store.TouchHeartbeat(record.JobID, base.Add(time.Second), time.Minute)
		heartbeatDone <- err
	}()
	select {
	case err := <-heartbeatDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat blocked behind Store.Update")
	}
	reaped := make(chan error, 1)
	go func() { reaped <- store.Reap() }()
	close(release)
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
	if err := <-reaped; err != nil {
		t.Fatal(err)
	}
	got, err := store.Load(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != engine.StateCompleted || got.Result == nil {
		t.Fatalf("final record = %+v", got)
	}
	if _, err := os.Stat(filepath.Join(store.Layout().Jobs, record.JobID+".heartbeat")); !os.IsNotExist(err) {
		t.Fatalf("heartbeat sidecar remains: %v", err)
	}
}

func TestHeartbeatDoesNotBlockOnJobLock(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_lock", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(store.Layout().Jobs, jobID+".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	done := make(chan error, 1)
	go func() {
		active, err := server.refreshHeartbeat(store, jobID)
		if err == nil && !active {
			err = errors.New("heartbeat unexpectedly inactive")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("heartbeat blocked on per-job lock")
	}
}

func TestIdleShutdownWaitsForActiveJobs(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	h := startTestServer(t, backend, Config{IdleTimeout: 80 * time.Millisecond, IdleCheckInterval: 20 * time.Millisecond})
	conn := dialRaw(t, h.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-idle-shutdown",
		RequestID:    "request-idle-shutdown",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"},
	}), &job)
	<-backend.started
	_ = conn.Close()

	select {
	case <-h.done:
		t.Fatal("server shut down while a background job was active")
	case <-time.After(160 * time.Millisecond):
	}
	close(release)
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatal("server did not idle-shutdown after active work completed")
	}
}

func TestServeWithStartupContextUsesServiceContextAfterReady(t *testing.T) {
	t.Parallel()
	const startupTimeout = 500 * time.Millisecond
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	server := newTestServerWithRoot(t, root, cwd, newFakeBackend("fake"), Config{IdleTimeout: -1})
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	listener := newPipeTestListener()
	listening := make(chan struct{})
	var listenOnce sync.Once
	ready := make(chan struct{})
	var readyOnce sync.Once
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenOnce.Do(func() { close(listening) })
		return listener, socketFileIdentity{}, nil
	}
	server.readyHook = func(ServeReadyInfo) error {
		readyOnce.Do(func() { close(ready) })
		return nil
	}
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelStartup()
	done := make(chan error, 1)
	go func() {
		done <- server.ServeWithStartupContext(serviceCtx, startupCtx)
		close(done)
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("server exited before ready hook: %v", err)
	case <-time.After(time.Second):
		t.Fatal("server did not reach ready hook")
	}
	if deadline, ok := startupCtx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			time.Sleep(remaining + 100*time.Millisecond)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("server stopped after bootstrap deadline instead of service context cancellation: %v", err)
	default:
	}
	conn, err := listener.Dial()
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	helloRaw(t, conn, reader, "test-token")
	_ = conn.Close()

	cancelService()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server stopped after service context cancellation with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop after service context cancellation")
	}
}

func TestServeWithStartupContextCanceledBeforeRegistrationSkipsReady(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	listener := newBlockingTestListener()
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	defer cancelStartup()
	serviceCtx, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		cancelStartup()
		return listener, socketFileIdentity{}, nil
	}
	readyCalled := make(chan struct{})
	server.readyHook = func(ServeReadyInfo) error {
		close(readyCalled)
		return nil
	}

	err := server.ServeWithStartupContext(serviceCtx, startupCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeWithStartupContext error = %v, want context canceled", err)
	}
	select {
	case <-readyCalled:
		t.Fatal("ready hook ran after startup cancellation")
	default:
	}
	server.serveStateMu.Lock()
	registered := server.serveListener != nil
	server.serveStateMu.Unlock()
	if registered {
		t.Fatal("serve lifecycle registered after startup cancellation")
	}
	listener.waitClosed(t)
}

func TestBinaryChangeExitsPromptlyWhenQuiet(t *testing.T) {
	t.Parallel()
	changed := newBinaryChangeProbe()
	h := startTestServer(t, newFakeBackend("fake"), Config{
		IdleTimeout:         -1,
		IdleCheckInterval:   10 * time.Millisecond,
		BinaryIdentityProbe: changed.probe,
	})
	changed.changed.Store(true)

	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not restart promptly after its binary changed while quiet")
	}
}

func TestBinaryChangeDrainsConnectionAcceptedAtQuietCheck(t *testing.T) {
	t.Parallel()
	changed := newBinaryChangeProbe()
	beforeListenerClose := make(chan struct{})
	releaseListenerClose := make(chan struct{})
	listenerClosed := make(chan struct{})
	var releaseCloseOnce sync.Once
	var srv *Server
	releaseCloseHook := func() { releaseCloseOnce.Do(func() { close(releaseListenerClose) }) }
	t.Cleanup(releaseCloseHook)
	h := startTestServerWithHooks(t, newFakeBackend("fake"), Config{
		IdleTimeout:         -1,
		IdleCheckInterval:   10 * time.Millisecond,
		BinaryIdentityProbe: changed.probe,
	}, func(server *Server) {
		srv = server
		server.beforeStaleCloseHook = func() {
			close(beforeListenerClose)
			<-releaseListenerClose
		}
		server.staleListenerHook = func() { close(listenerClosed) }
	})

	changed.changed.Store(true)
	// The daemon parks in beforeStaleCloseHook at the quiet check, BEFORE the
	// listener closes. A connection dialed now is exactly the race the drain
	// logic must survive: accepted after the quiet decision, before close.
	// (The hook may only fire once the harness readiness dial and any stray
	// backlog connections from waitForSocket have fully drained; the quiet
	// check itself guarantees that, so no extra synchronization is needed.)
	select {
	case <-beforeListenerClose:
	case <-time.After(5 * time.Second):
		t.Fatal("stale daemon did not reach its quiet listener-close check")
	}
	conn, err := net.Dial("unix", h.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Wait until the accept loop has registered the connection so the close
	// below provably happens with a live accepted client.
	deadline := time.Now().Add(5 * time.Second)
	for srv.clients.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if srv.clients.Load() == 0 {
		t.Fatal("accepted connection did not register before listener close")
	}

	releaseCloseHook()
	select {
	case <-listenerClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("stale daemon did not close its listener")
	}

	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	assertServerStillRunning(t, h.done, "an accepted connection was still open")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale daemon did not exit after the accepted connection drained")
	}
}

func TestBinaryChangeWaitsForConnectionsAndActiveJobs(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	changed := newBinaryChangeProbe()
	h := startTestServer(t, backend, Config{
		IdleTimeout:         -1,
		IdleCheckInterval:   10 * time.Millisecond,
		BinaryIdentityProbe: changed.probe,
	})
	conn := dialRaw(t, h.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-binary-change",
		RequestID:    "request-binary-change",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     h.cwd,
			Write:   false,
			Prompt:  "hold",
		},
	}), &job)
	<-backend.started
	changed.changed.Store(true)

	assertServerStillRunning(t, h.done, "a client connection was open")
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	assertServerStillRunning(t, h.done, "a job was active")
	close(release)

	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not restart after its connection and active job cleared")
	}
}

func TestBinaryChangeDoesNotUnlinkReplacementSocket(t *testing.T) {
	t.Parallel()
	changed := newBinaryChangeProbe()
	socketRemoved := make(chan struct{})
	releaseDrain := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseDrain) }) }
	t.Cleanup(release)
	h := startTestServerWithHooks(t, newFakeBackend("fake"), Config{
		IdleTimeout:         -1,
		IdleCheckInterval:   10 * time.Millisecond,
		BinaryIdentityProbe: changed.probe,
	}, func(server *Server) {
		server.staleSocketRemovedHook = func() {
			close(socketRemoved)
			<-releaseDrain
		}
	})

	changed.changed.Store(true)
	select {
	case <-socketRemoved:
	case <-time.After(time.Second):
		t.Fatal("stale daemon did not remove its socket")
	}
	if _, err := os.Lstat(h.socketPath); !os.IsNotExist(err) {
		t.Fatalf("stale daemon socket still exists after owned removal: %v", err)
	}
	replacement, err := net.Listen("unix", h.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	replacementUnix, ok := replacement.(*net.UnixListener)
	if !ok {
		_ = replacement.Close()
		t.Fatalf("replacement listener type = %T, want *net.UnixListener", replacement)
	}
	// A UnixListener unlinks its path on Close by default. The test owns this
	// listener, so prevent its cleanup from being mistaken for daemon removal.
	replacementUnix.SetUnlinkOnClose(false)
	defer replacement.Close()
	wantIdentity, err := statSocketFileIdentity(h.socketPath)
	if err != nil {
		t.Fatal(err)
	}
	release()

	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("server exited with error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale daemon did not exit after closing its listener")
	}
	gotIdentity, err := statSocketFileIdentity(h.socketPath)
	if err != nil {
		t.Fatalf("replacement socket was removed: %v", err)
	}
	if gotIdentity != wantIdentity {
		t.Fatalf("replacement socket identity = %+v, want %+v", gotIdentity, wantIdentity)
	}
}

func TestJobLookupSurvivesRestartForNonStartupWorkspace(t *testing.T) {
	t.Parallel()
	root := shortTempDir(t)
	jobCWD := shortTempDir(t)
	daemonCWD := shortTempDir(t)

	first := startTestServerWithRoot(t, root, daemonCWD, newFakeBackend("fake"), Config{IdleTimeout: -1})
	conn := dialRaw(t, first.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, first.token)
	var submitted protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-restart-lookup",
		RequestID:    "request-restart-lookup",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: jobCWD, Write: false, Prompt: "complete"},
	}), &submitted)
	waitJobState(t, conn, r, submitted.JobID, engine.StateCompleted)
	_ = conn.Close()
	stopTestServer(t, first)

	second := startTestServerWithRoot(t, root, daemonCWD, newFakeBackend("fake"), Config{IdleTimeout: -1})
	conn = dialRaw(t, second.socketPath)
	defer conn.Close()
	r = bufio.NewReader(conn)
	helloRaw(t, conn, r, second.token)

	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: submitted.JobID}), &status)
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("status after restart = %+v", status)
	}

	var result protocol.JobResult
	decodeResult(t, rpc(t, conn, r, "4", protocol.MethodJobResult, protocol.JobResultParams{JobID: submitted.JobID}), &result)
	if result.JobID != submitted.JobID || result.State != engine.StateCompleted || result.Result == nil || result.Result.Text == "" {
		t.Fatalf("result after restart = %+v", result)
	}

	var canceled protocol.JobCancelResult
	decodeResult(t, rpc(t, conn, r, "5", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: submitted.JobID}), &canceled)
	if canceled.JobID != submitted.JobID || canceled.State != engine.StateCompleted {
		t.Fatalf("cancel after restart = %+v", canceled)
	}
}

type fakeProcessTable struct{}

func (fakeProcessTable) Lookup(int) (engine.ProcessInfo, bool, error) {
	return engine.ProcessInfo{}, false, nil
}

type mapProcessTable struct {
	entries map[int]engine.ProcessInfo
}

func (p mapProcessTable) Lookup(pid int) (engine.ProcessInfo, bool, error) {
	info, ok := p.entries[pid]
	return info, ok, nil
}

type processSignal struct {
	PGID   int
	Signal syscall.Signal
}

type recordingProcessGroups struct {
	mu      sync.Mutex
	signals []processSignal
}

func (g *recordingProcessGroups) SignalProcessGroup(pgid int, signal syscall.Signal) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.signals = append(g.signals, processSignal{PGID: pgid, Signal: signal})
	return nil
}

func (g *recordingProcessGroups) snapshot() []processSignal {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]processSignal(nil), g.signals...)
}

type recordingWaiter struct {
	durations chan time.Duration
	release   chan struct{}
	once      sync.Once
}

func newRecordingWaiter() *recordingWaiter {
	return &recordingWaiter{durations: make(chan time.Duration, 4), release: make(chan struct{})}
}

func (w *recordingWaiter) Wait(d time.Duration) {
	w.durations <- d
	<-w.release
}

func (w *recordingWaiter) Release() {
	w.once.Do(func() { close(w.release) })
}

type testServer struct {
	root       string
	cwd        string
	socketPath string
	token      string
	done       chan error
	cancel     context.CancelFunc
}

type binaryChangeProbe struct {
	changed atomic.Bool
}

func newBinaryChangeProbe() *binaryChangeProbe {
	return &binaryChangeProbe{}
}

func (p *binaryChangeProbe) probe(string) (BinaryIdentity, error) {
	size := int64(1)
	if p.changed.Load() {
		size = 2
	}
	return BinaryIdentity{ModTime: time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC), Size: size}, nil
}

type blockingOwnedWorkChecker struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingOwnedWorkChecker() *blockingOwnedWorkChecker {
	return &blockingOwnedWorkChecker{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingOwnedWorkChecker) HasOwnedWork(context.Context) (bool, error) {
	c.startedOnce.Do(func() { close(c.started) })
	<-c.release
	return false, nil
}

func (c *blockingOwnedWorkChecker) releaseProbe() {
	c.releaseOnce.Do(func() { close(c.release) })
}

type blockingRepositoryOwnedWorkChecker struct {
	*blockingOwnedWorkChecker
	repo repository.Repository
}

func newBlockingRepositoryOwnedWorkChecker(repo repository.Repository) *blockingRepositoryOwnedWorkChecker {
	return &blockingRepositoryOwnedWorkChecker{
		blockingOwnedWorkChecker: newBlockingOwnedWorkChecker(),
		repo:                     repo,
	}
}

func (c *blockingRepositoryOwnedWorkChecker) HasOwnedWork(context.Context) (bool, error) {
	if c == nil || c.repo == nil {
		return false, errors.New("admission repository is not ready")
	}
	err := c.repo.View(context.Background(), func(repository.ReadTx) error {
		c.startedOnce.Do(func() { close(c.started) })
		<-c.release
		return nil
	})
	return false, err
}

type blockingUpdateRepository struct {
	inner       repository.Repository
	started     chan struct{}
	release     chan struct{}
	armed       atomic.Bool
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingUpdateRepository(inner repository.Repository) *blockingUpdateRepository {
	return &blockingUpdateRepository{
		inner:   inner,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (r *blockingUpdateRepository) Arm() {
	r.armed.Store(true)
}

func (r *blockingUpdateRepository) Release() {
	r.releaseOnce.Do(func() { close(r.release) })
}

func (r *blockingUpdateRepository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	return r.inner.View(ctx, fn)
}

func (r *blockingUpdateRepository) Update(ctx context.Context, fn func(repository.WriteTx) error) (repository.Commit, error) {
	return r.inner.Update(ctx, func(tx repository.WriteTx) error {
		if r.armed.Load() {
			r.startedOnce.Do(func() { close(r.started) })
			select {
			case <-r.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return fn(tx)
	})
}

type failingUpdateRepository struct {
	inner repository.Repository
	err   error
}

func (r *failingUpdateRepository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	return r.inner.View(ctx, fn)
}

func (r *failingUpdateRepository) Update(ctx context.Context, fn func(repository.WriteTx) error) (repository.Commit, error) {
	if r.err != nil {
		return repository.Commit{}, r.err
	}
	return r.inner.Update(ctx, fn)
}

func assertServerStillRunning(t *testing.T, done <-chan error, reason string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("server stopped while %s: %v", reason, err)
	case <-time.After(80 * time.Millisecond):
	}
}

func startTestServer(t *testing.T, backend engine.Backend, cfg Config) testServer {
	t.Helper()
	return startTestServerWithHooks(t, backend, cfg, nil)
}

func startTestServerWithHooks(t *testing.T, backend engine.Backend, cfg Config, configure func(*Server)) testServer {
	t.Helper()
	return startTestServerWithRootAndHooks(t, shortTempDir(t), shortTempDir(t), backend, cfg, configure)
}

func startTestServerWithRoot(t *testing.T, root, cwd string, backend engine.Backend, cfg Config) testServer {
	t.Helper()
	return startTestServerWithRootAndHooks(t, root, cwd, backend, cfg, nil)
}

func newTestServerWithRoot(t *testing.T, root, cwd string, backend engine.Backend, cfg Config) *Server {
	t.Helper()
	cfg.StateRoot = root
	cfg.CWD = cwd
	cfg.Token = "test-token"
	cfg.Backends = []engine.Backend{backend}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func startTestServerWithRootAndHooks(t *testing.T, root, cwd string, backend engine.Backend, cfg Config, configure func(*Server)) testServer {
	t.Helper()
	server := newTestServerWithRoot(t, root, cwd, backend, cfg)
	if configure != nil {
		configure(server)
	}
	ready := make(chan struct{})
	var readyOnce sync.Once
	readyHook := server.readyHook
	server.readyHook = func(info ServeReadyInfo) error {
		if readyHook != nil {
			if err := readyHook(info); err != nil {
				return err
			}
		}
		readyOnce.Do(func() { close(ready) })
		return nil
	}
	if server.admissionRuntime == nil && server.admissionRuntimeConfig.Process() == nil {
		configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	h := testServer{root: root, cwd: cwd, socketPath: filepath.Join(root, protocol.SocketName), token: "test-token", done: done, cancel: cancel}
	waitForServerReady(t, ready, done)
	waitForSocket(t, h.socketPath, done)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("server did not stop")
		}
	})
	return h
}

func waitForServerReady(t *testing.T, ready <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-ready:
	case err := <-done:
		if err != nil && servedTestBindDeniedOutput(err.Error()) {
			servedTestSkipOrFailBindDenied(t, "server ready wait", err)
		}
		t.Fatalf("server exited before ready hook: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not report ready")
	}
}

func startTestServerWithBlockingListener(t *testing.T, server *Server) (context.CancelFunc, <-chan error, *blockingTestListener) {
	t.Helper()
	listener := newBlockingTestListener()
	cancel, done := startTestServerWithProvidedListener(t, server, listener)
	return cancel, done, listener
}

func startTestServerWithPipeListener(t *testing.T, server *Server, listener *pipeTestListener) (context.CancelFunc, <-chan error) {
	t.Helper()
	return startTestServerWithProvidedListener(t, server, listener)
}

func startTestServerWithProvidedListener(t *testing.T, server *Server, listener net.Listener) (context.CancelFunc, <-chan error) {
	t.Helper()
	cancel, done, listening := startTestServerWithProvidedListenerAsync(t, server, listener)
	select {
	case <-listening:
	case err := <-done:
		t.Fatalf("server exited before listener was ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("blocking listener did not become ready")
	}
	waitForServeLifecycle(t, server, listener, done)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("server did not stop")
		}
	})
	return cancel, done
}

func startTestServerWithProvidedListenerAsync(t *testing.T, server *Server, listener net.Listener) (context.CancelFunc, <-chan error, <-chan struct{}) {
	t.Helper()
	listening := make(chan struct{})
	var listenOnce sync.Once
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenOnce.Do(func() { close(listening) })
		if err := os.WriteFile(server.socketPath, []byte("blocking-listener"), 0o600); err != nil {
			return nil, socketFileIdentity{}, err
		}
		identity, err := statSocketFileIdentity(server.socketPath)
		if err != nil {
			return nil, socketFileIdentity{}, err
		}
		return listener, identity, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	return cancel, done, listening
}

func waitForServeLifecycle(t *testing.T, server *Server, listener net.Listener, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.serveStateMu.Lock()
		ready := server.serveListener == listener
		server.serveStateMu.Unlock()
		if ready {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("server exited before lifecycle was ready: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server lifecycle was not registered")
}

func stopTestServer(t *testing.T, h testServer) {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		// Positive wait: shutdown drains connections and closes the
		// admission repository before done fires; under whole-repo -race
		// sweep load a 1s deadline flaked.
		t.Fatalf("server did not stop")
	}
}

func waitForSocket(t *testing.T, socketPath string, done <-chan error) {
	t.Helper()
	// Positive wait: Serve now bootstraps admission (bbolt open + recovery +
	// backend probing) BEFORE listening; under whole-repo -race sweep load a
	// 1s deadline flaked on restart-family tests.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			if err != nil && servedTestBindDeniedOutput(err.Error()) {
				servedTestSkipOrFailBindDenied(t, "socket ready wait", err)
			}
			t.Fatalf("server exited before socket was ready: %v", err)
		default:
		}
		conn, err := net.Dial("unix", socketPath)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("socket %s did not become ready", socketPath)
}

func waitForSocketRemoved(t *testing.T, socketPath string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(socketPath); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("socket %s was not removed after server exit: %v", socketPath, err)
	default:
	}
	t.Fatalf("socket %s was not removed", socketPath)
}

func assertConnRemainsOpenFor(t *testing.T, conn net.Conn, r *bufio.Reader, duration time.Duration, reason string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(duration)); err != nil {
		t.Fatal(err)
	}
	_, err := r.Peek(1)
	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
		t.Fatal(resetErr)
	}
	if isNetTimeout(err) {
		return
	}
	if err == nil {
		t.Fatalf("connection produced unexpected data after %s", reason)
	}
	t.Fatalf("connection closed after %s: %v", reason, err)
}

func waitForConnClosed(t *testing.T, conn net.Conn, r *bufio.Reader, reason string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		_, err := r.Peek(1)
		if err == nil {
			t.Fatalf("connection produced unexpected data while waiting for close after %s", reason)
		}
		if !isNetTimeout(err) {
			if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
				t.Fatal(resetErr)
			}
			return
		}
	}
	if resetErr := conn.SetReadDeadline(time.Time{}); resetErr != nil {
		t.Fatal(resetErr)
	}
	t.Fatalf("connection was not closed after %s", reason)
}

func isNetTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func dialRaw(t *testing.T, socketPath string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func newUnstartedTestServer(t *testing.T, backend engine.Backend) (*Server, string, string) {
	t.Helper()
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	server, err := New(Config{
		StateRoot:    root,
		CWD:          cwd,
		Token:        "test-token",
		Backends:     []engine.Backend{backend},
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, root, cwd
}

func enableTestAdmission(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian) {
	t.Helper()
	enableTestAdmissionWithParkedExec(t, server, launcher, true)
}

func enableTestAdmissionWithParkedExec(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, parkedExec bool) {
	t.Helper()
	configureTestAdmissionRuntime(t, server, launcher, parkedExec)
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func configureTestAdmissionRuntime(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, parkedExec bool) {
	t.Helper()
	support, err := custodian.NewSupport(custodian.Support{
		ParkedExec:             parkedExec,
		VerifiedContainment:    true,
		ImplementationCompiled: true,
		RuntimeProbePassed:     true,
		FeatureConfigured:      parkedExec,
		FeatureAdvertised:      parkedExec,
		Platform:               "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	server.admissionRuntime = &servedAdmissionRuntime{
		runtime:          custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		launchCustodian:  launcher,
		supportOverride:  &support,
		verifierOverride: launcher.verifier,
	}
}

func enableTestAdmissionWithAuthorityStore(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, repo *memory.Repository, anchorStore *authority.AnchorStore) {
	t.Helper()
	configureServeAdmissionCloser(t, server, launcher, repo, anchorStore, io.NopCloser(bytes.NewReader(nil)))
	enableTestAdmission(t, server, launcher)
}

func configureServeAdmissionCloser(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, repo *memory.Repository, anchorStore *authority.AnchorStore, closer io.Closer) {
	t.Helper()
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, closer, nil
	}
	configureTestAdmissionRuntime(t, server, launcher, true)
}

func newPriorBootAuthorityWork(t *testing.T, repo *memory.Repository, anchorStore *authority.AnchorStore, launcher *admissionFakeLaunchCustodian, name string) (*authority.Ready, authority.AcceptResult) {
	t.Helper()
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(launcher.verifier))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-"+name+"-old", "owner-"+name+"-old")
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(context.Background(), boot)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(context.Background(), authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: model.WorkspaceKey("workspace-" + name),
			RequestID:    model.RequestID("request-" + name),
		},
		WorkspaceLayoutKey: model.WorkspaceKey(strings.Repeat("a", 64)),
		TaskIdentity:       model.NewSHA256TaskIdentity([]byte("task-" + name)),
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          "session-" + name,
	})
	if err != nil {
		t.Fatal(err)
	}
	return ready, accepted
}

type recordingAdmissionAuthority struct {
	*servedAdmissionAuthority

	beforeRequestCancel func(context.Context, model.JobID) error
	beforeRecordOutcome func(context.Context, model.JobID, model.AttemptRef, model.Outcome) error

	mu              sync.Mutex
	finalizeRecords []model.TerminalIntent
}

func (a *recordingAdmissionAuthority) RequestCancel(ctx context.Context, jobID model.JobID) (coordinator.StepResult, error) {
	if a.beforeRequestCancel != nil {
		if err := a.beforeRequestCancel(ctx, jobID); err != nil {
			return coordinator.StepResult{}, err
		}
	}
	return a.servedAdmissionAuthority.RequestCancel(ctx, jobID)
}

func (a *recordingAdmissionAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome, contract *engine.ContractStamp) (coordinator.StepResult, error) {
	if a.beforeRecordOutcome != nil {
		if err := a.beforeRecordOutcome(ctx, jobID, ref, outcome); err != nil {
			return coordinator.StepResult{}, err
		}
	}
	return a.servedAdmissionAuthority.RecordOutcome(ctx, jobID, ref, outcome, contract)
}

func (a *recordingAdmissionAuthority) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent) (coordinator.StepResult, error) {
	a.mu.Lock()
	a.finalizeRecords = append(a.finalizeRecords, intent)
	a.mu.Unlock()
	return a.servedAdmissionAuthority.Finalize(ctx, jobID, ref, intent)
}

func (a *recordingAdmissionAuthority) finalizeIntents() []model.TerminalIntent {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]model.TerminalIntent(nil), a.finalizeRecords...)
}

func installRecordingAdmissionAuthorityForTest(t *testing.T, server *Server) *recordingAdmissionAuthority {
	t.Helper()
	server.admissionStateMu.Lock()
	defer server.admissionStateMu.Unlock()
	if server.admissionReady == nil || server.admissionSubmission == nil || server.admissionSubmission.launch == nil || server.admissionInstance == nil {
		t.Fatal("admission runtime is not ready")
	}
	recorder := &recordingAdmissionAuthority{
		servedAdmissionAuthority: &servedAdmissionAuthority{
			ready: server.admissionReady,
			latch: server.safetyLatch,
			clock: server.clock,
		},
	}
	coord, err := coordinator.New(
		recorder,
		servedCoordinatorLaunchContainment{controller: server.admissionSubmission.launch},
		servedResultPublisher{server: server},
		server.admissionSubmission.owner,
	)
	if err != nil {
		t.Fatal(err)
	}
	server.admissionCoordinator = coord
	server.admissionOwnedWorkChecker = coord
	server.admissionInstance.coordinator = coord
	return recorder
}

func acceptIdentifiedAuthorityWork(t *testing.T, server *Server, requestID string) authority.AcceptResult {
	t.Helper()
	server.admissionStateMu.RLock()
	defer server.admissionStateMu.RUnlock()
	if server.admissionSubmission == nil {
		t.Fatal("admission submission is not ready")
	}
	accepted, err := server.admissionSubmission.SubmitIdentified(context.Background(), authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: model.WorkspaceKey("workspace/" + requestID), RequestID: model.RequestID(requestID)},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte(requestID)),
		SessionID:    "session-" + requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return accepted
}

func finalizeAcceptedAuthorityWork(t *testing.T, server *Server, accepted authority.AcceptResult) {
	t.Helper()
	server.admissionStateMu.RLock()
	defer server.admissionStateMu.RUnlock()
	if server.admissionReady == nil {
		t.Fatal("admission ready is not ready")
	}
	_, err := server.admissionReady.Finalize(context.Background(), accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func admissionAnchorSnapshot(t *testing.T, anchorStore *authority.AnchorStore) authority.AnchorSnapshot {
	t.Helper()
	var snapshot authority.AnchorSnapshot
	if err := json.Unmarshal(anchorStore.SnapshotBytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func newAdmissionFakeLaunchCustodian(t *testing.T) *admissionFakeLaunchCustodian {
	t.Helper()
	issuer, verifier := custodian.NewAttestationChannel()
	return &admissionFakeLaunchCustodian{issuer: issuer, verifier: verifier}
}

type admissionFakeLaunchCustodian struct {
	issuer   custodian.AttestationIssuer
	verifier custodian.AttestationVerifier

	mu           sync.Mutex
	ordinals     []model.LaunchOrdinal
	releases     int
	aborts       int
	contains     int
	abandons     int
	containments []admissionContainObservation
	abandoned    []model.GroupRef
	running      map[string]*admissionFakeRunning

	containCtxErrs      []error
	containCtxDeadlines []bool
	abortCtxErrs        []error
	abortCtxDeadlines   []bool

	abortRespectContext       bool
	abortWaitForContextDone   bool
	releaseWaitForContextDone bool
	releaseOutcome            custodian.ReleaseOutcome
	releaseErr                error
	releaseStarted            chan struct{}
	containErr                error
	containErrByOrdinal       map[model.LaunchOrdinal]error
	cleanupErr                error
	waitErr                   error
	waitExit                  command.ExitObservation
	abortCleanupErr           error
	waitAndVerify             <-chan struct{}
	activeCustodies           atomic.Bool
}

func (c *admissionFakeLaunchCustodian) Prepare(_ context.Context, spec command.ExecSpec, key model.LaunchKey) (launch.PreparedProcess, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" {
		return nil, errors.New("exec argv is required")
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	group := admissionTestGroup(key)
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	go func() {
		_, _ = io.Copy(io.Discard, stdinReader)
		_ = stdinReader.Close()
	}()
	running := &admissionFakeRunning{
		group:        group,
		issuer:       c.issuer,
		stdin:        stdinWriter,
		stdout:       stdoutReader,
		stdoutWriter: stdoutWriter,
		stderr:       stderrReader,
		stderrWriter: stderrWriter,
		contained:    make(chan struct{}),
		wait:         c.waitAndVerify,
		cleanupErr:   c.cleanupErr,
		waitErr:      c.waitErr,
		waitExit:     c.waitExit,
		containErr:   c.containErr,
	}
	c.mu.Lock()
	c.ordinals = append(c.ordinals, key.Ordinal)
	if c.running == nil {
		c.running = make(map[string]*admissionFakeRunning)
	}
	c.running[string(group.CustodyID)] = running
	c.mu.Unlock()
	return &admissionFakePrepared{group: group, running: running, issuer: c.issuer, custodian: c}, nil
}

func (c *admissionFakeLaunchCustodian) ContainAndVerify(ctx context.Context, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	c.contains++
	c.containments = append(c.containments, admissionContainObservation{group: group, cause: cause})
	_, hasDeadline := ctx.Deadline()
	c.containCtxErrs = append(c.containCtxErrs, ctx.Err())
	c.containCtxDeadlines = append(c.containCtxDeadlines, hasDeadline)
	containErr := c.containErr
	if c.containErrByOrdinal != nil {
		if ordinalErr := c.containErrByOrdinal[group.Launch.Ordinal]; ordinalErr != nil {
			containErr = ordinalErr
		}
	}
	running := c.running[string(group.CustodyID)]
	cleanupErr := c.cleanupErr
	c.mu.Unlock()
	if containErr != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, containErr
	}
	if running != nil {
		return running.ContainAndVerify(ctx, cause)
	}
	method := model.QuiescenceTermKill
	if cause == custodian.QuiescenceCauseRecovery {
		method = model.QuiescenceAlreadyAbsent
	}
	verified, err := c.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: method})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: cleanupErr}, nil
}

func (c *admissionFakeLaunchCustodian) preparedOrdinals() []model.LaunchOrdinal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.LaunchOrdinal(nil), c.ordinals...)
}

func (c *admissionFakeLaunchCustodian) releaseCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releases
}

func (c *admissionFakeLaunchCustodian) abortCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.aborts
}

func (c *admissionFakeLaunchCustodian) containCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.contains
}

func (c *admissionFakeLaunchCustodian) containObservations() []admissionContainObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]admissionContainObservation(nil), c.containments...)
}

func (c *admissionFakeLaunchCustodian) runningContained(custodyID model.CustodyID) bool {
	c.mu.Lock()
	running := c.running[string(custodyID)]
	c.mu.Unlock()
	return running != nil && running.WaitContained()
}

func (c *admissionFakeLaunchCustodian) HasActiveCustodies() bool {
	return c != nil && c.activeCustodies.Load()
}

func (c *admissionFakeLaunchCustodian) AbandonUnresolvedCustody(_ context.Context, group model.GroupRef) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.abandons++
	c.abandoned = append(c.abandoned, group)
	running := c.running[string(group.CustodyID)]
	delete(c.running, string(group.CustodyID))
	c.activeCustodies.Store(false)
	c.mu.Unlock()
	if running != nil {
		running.closeStreams()
	}
	return nil
}

func (c *admissionFakeLaunchCustodian) abandonCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.abandons
}

func (c *admissionFakeLaunchCustodian) abortContextObservations() ([]error, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.abortCtxErrs...), append([]bool(nil), c.abortCtxDeadlines...)
}

func (c *admissionFakeLaunchCustodian) containContextObservations() ([]error, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.containCtxErrs...), append([]bool(nil), c.containCtxDeadlines...)
}

type admissionFakePrepared struct {
	group     model.GroupRef
	running   *admissionFakeRunning
	issuer    custodian.AttestationIssuer
	custodian *admissionFakeLaunchCustodian
}

type admissionContainObservation struct {
	group model.GroupRef
	cause custodian.QuiescenceCause
}

func (p *admissionFakePrepared) Ref() model.GroupRef {
	return p.group
}

func (p *admissionFakePrepared) Release(ctx context.Context) (launch.RunningProcess, custodian.ReleaseOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	outcome := custodian.ReleaseAccepted
	var releaseErr error
	waitForContextDone := false
	var releaseStarted chan struct{}
	if p.custodian != nil {
		p.custodian.mu.Lock()
		p.custodian.releases++
		if p.custodian.releaseOutcome != 0 {
			outcome = p.custodian.releaseOutcome
		}
		releaseErr = p.custodian.releaseErr
		waitForContextDone = p.custodian.releaseWaitForContextDone
		releaseStarted = p.custodian.releaseStarted
		p.custodian.mu.Unlock()
	}
	if releaseStarted != nil {
		select {
		case releaseStarted <- struct{}{}:
		default:
		}
	}
	if waitForContextDone {
		<-ctx.Done()
		if releaseErr == nil {
			releaseErr = ctx.Err()
		}
	}
	if outcome != custodian.ReleaseAccepted {
		return nil, outcome, releaseErr
	}
	return p.running, outcome, releaseErr
}

func (p *admissionFakePrepared) AbortAndVerify(ctx context.Context) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	respectContext := false
	waitForContextDone := false
	var cleanupErr error
	if p.custodian != nil {
		p.custodian.mu.Lock()
		respectContext = p.custodian.abortRespectContext
		waitForContextDone = p.custodian.abortWaitForContextDone
		cleanupErr = p.custodian.abortCleanupErr
		p.custodian.mu.Unlock()
	}
	if waitForContextDone {
		<-ctx.Done()
	}
	if p.custodian != nil {
		_, hasDeadline := ctx.Deadline()
		p.custodian.mu.Lock()
		p.custodian.aborts++
		p.custodian.abortCtxErrs = append(p.custodian.abortCtxErrs, ctx.Err())
		p.custodian.abortCtxDeadlines = append(p.custodian.abortCtxDeadlines, hasDeadline)
		p.custodian.mu.Unlock()
	}
	if respectContext {
		if err := ctx.Err(); err != nil {
			return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
		}
	}
	verified, err := p.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: p.group, Method: model.QuiescenceAlreadyAbsent})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: cleanupErr}, nil
}

type admissionFakeRunning struct {
	group        model.GroupRef
	issuer       custodian.AttestationIssuer
	stdin        *io.PipeWriter
	stdout       *io.PipeReader
	stdoutWriter *io.PipeWriter
	stderr       *io.PipeReader
	stderrWriter *io.PipeWriter
	closeOnce    sync.Once
	containOnce  sync.Once
	contained    chan struct{}
	wait         <-chan struct{}
	cleanupErr   error
	waitErr      error
	waitExit     command.ExitObservation
	containErr   error
}

func (r *admissionFakeRunning) Ref() model.GroupRef {
	return r.group
}

func (r *admissionFakeRunning) Stdin() io.WriteCloser {
	return r.stdin
}

func (r *admissionFakeRunning) Stdout() io.ReadCloser {
	return r.stdout
}

func (r *admissionFakeRunning) Stderr() io.ReadCloser {
	return r.stderr
}

func (r *admissionFakeRunning) WaitAndVerify(ctx context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if r.wait != nil {
		select {
		case <-r.wait:
		case <-r.contained:
			return r.finish(model.QuiescenceTermKill)
		case <-ctx.Done():
			return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, ctx.Err()
		}
	}
	if r.waitErr != nil {
		r.closeStreams()
		exit := r.waitExit
		if exit == (command.ExitObservation{}) {
			exit = command.ExitObservation{Exited: true, Code: 0}
		}
		return exit, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{Err: r.cleanupErr}, r.waitErr
	}
	return r.finish(model.QuiescenceNaturalExit)
}

func (r *admissionFakeRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	r.markContained()
	if r.containErr != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, r.containErr
	}
	_, verified, cleanup, err := r.finish(model.QuiescenceTermKill)
	return verified, cleanup, err
}

func (r *admissionFakeRunning) WaitContained() bool {
	select {
	case <-r.contained:
		return true
	default:
		return false
	}
}

func (r *admissionFakeRunning) markContained() {
	r.containOnce.Do(func() {
		close(r.contained)
	})
}

func (r *admissionFakeRunning) finish(method model.QuiescenceMethod) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	r.closeStreams()
	verified, err := r.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: r.group, Method: method})
	if err != nil {
		return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return command.ExitObservation{Exited: true, Code: 0}, verified, custodian.CleanupStatus{Err: r.cleanupErr}, nil
}

func (r *admissionFakeRunning) closeStreams() {
	r.closeOnce.Do(func() {
		_ = r.stdoutWriter.Close()
		_ = r.stderrWriter.Close()
	})
}

func admissionTestGroup(key model.LaunchKey) model.GroupRef {
	name := string(key.Attempt.JobID) + "-" + key.Ordinal.String()
	pgid := 4100 + int(key.Ordinal)
	return model.GroupRef{
		Version:             1,
		CustodyID:           model.CustodyID("custody-" + name),
		Launch:              key,
		HostBootID:          "host-" + name,
		PIDNamespaceState:   model.PIDNamespaceNotApplicable,
		RetainedDomainID:    "retained-domain-" + name,
		RetainedDomainState: model.RetainedDomainKnown,
		PGID:                pgid,
		Leader:              model.ProcessIdentity{PID: pgid, HighResStartToken: "leader-" + name},
		Monitor:             model.ProcessIdentity{PID: pgid + 100, HighResStartToken: "monitor-" + name},
		RetainedID:          "retained-" + name,
	}
}

func waitBackendStarted(t *testing.T, backend *fakeBackend) {
	t.Helper()
	waitBackendStarts(t, backend, 1)
}

func waitBackendStarts(t *testing.T, backend *fakeBackend, count int) {
	t.Helper()
	if backend.started == nil {
		t.Fatal("backend started channel is nil")
	}
	for i := 0; i < count; i++ {
		select {
		case <-backend.started:
		// Positive wait: generous under load (see waitJobState).
		case <-time.After(5 * time.Second):
			t.Fatalf("backend start %d did not happen", i+1)
		}
	}
}

func responseFromScriptedConn(t *testing.T, conn *scriptedConn) protocol.Response {
	t.Helper()
	raw := strings.TrimSpace(conn.writesString())
	if raw == "" {
		t.Fatal("scripted connection wrote no response")
	}
	line := strings.Split(raw, "\n")[0]
	var resp protocol.Response
	mustUnmarshal(t, []byte(line), &resp)
	return resp
}

func loadAdmissionSafetyRecord(t *testing.T, server *Server, jobID string) model.SafetyRecord {
	t.Helper()
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := server.admissionReady.LoadJob(context.Background(), modelJobID)
	if err != nil {
		t.Fatal(err)
	}
	if image.Safety.State != repository.RecordValid {
		t.Fatalf("admission safety state = %s", image.Safety.State)
	}
	return image.Safety.Value
}

func loadAuthoritySafetyRecordFromRepository(t *testing.T, repo repository.Repository, jobID string) model.SafetyRecord {
	t.Helper()
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		t.Fatal(err)
	}
	var record model.SafetyRecord
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(modelJobID)
		if image.Safety.State != repository.RecordValid {
			t.Fatalf("authority safety state = %s", image.Safety.State)
		}
		record = image.Safety.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return record
}

func waitAdmissionSafetyTerminal(t *testing.T, server *Server, jobID string) model.SafetyRecord {
	t.Helper()
	// Positive wait: generous under full-sweep race load (a passing run
	// returns on the first poll after the terminal transition; only genuine
	// failures pay the deadline). A 1s deadline flaked when the record was
	// mid-flight (outcome+result recorded, terminal pending) under -race
	// -count=2 whole-repo sweeps.
	deadline := time.Now().Add(10 * time.Second)
	var last model.SafetyRecord
	for time.Now().Before(deadline) {
		record := loadAdmissionSafetyRecord(t, server, jobID)
		last = record
		if record.Terminal != nil {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("admission safety record %s did not reach terminal state; last = %+v", jobID, last)
	return model.SafetyRecord{}
}

func waitBackendRunnerStartPathDone(t *testing.T, backend *fakeBackend) {
	t.Helper()
	if backend == nil || backend.startPathDone == nil {
		t.Fatal("backend start-path completion channel is nil")
	}
	select {
	case <-backend.startPathDone:
	case <-time.After(5 * time.Second):
		t.Fatal("held runner Start path did not complete after release")
	}
}

func waitActiveJobGone(t *testing.T, server *Server, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if server.lookupActiveJob(jobID) == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("active job %s did not finish after held Start resumed", jobID)
}

func waitSingleAdmissionSafetyTerminal(t *testing.T, server *Server) model.SafetyRecord {
	t.Helper()
	// Positive wait: see waitAdmissionSafetyTerminal for the deadline rationale.
	deadline := time.Now().Add(10 * time.Second)
	var last model.SafetyRecord
	for time.Now().Before(deadline) {
		record := singleAuthoritySafetyRecord(t, server)
		last = record
		if record.Terminal != nil {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("single admission safety record did not reach terminal state; last = %+v", last)
	return model.SafetyRecord{}
}

func singleAuthoritySafetyRecord(t *testing.T, server *Server) model.SafetyRecord {
	t.Helper()
	if server.admissionRepository == nil {
		t.Fatal("admission repository is not ready")
	}
	var records []model.SafetyRecord
	if err := server.admissionRepository.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			if image.Safety.State != repository.RecordValid {
				continue
			}
			records = append(records, image.Safety.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("authority safety records = %+v, want exactly one", records)
	}
	return records[0]
}

func assertNoJSONJobRecord(t *testing.T, server *Server, jobID string) {
	t.Helper()
	store := server.storeForJob(jobID)
	if store == nil {
		t.Fatalf("job store missing for %s", jobID)
	}
	if _, err := store.Load(jobID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("JSON job record load error = %v, want not exist", err)
	}
}

func submitIdentifiedForReplayTest(t *testing.T, server *Server, params protocol.JobSubmitParams) protocol.JobSubmitResult {
	t.Helper()
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("initial submit error = %+v", outcome.err)
	}
	submitted, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("initial submit result type = %T", outcome.result)
	}
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("initial submit result = %+v, want accepted non-replay", submitted)
	}
	if outcome.after == nil {
		t.Fatal("initial submit did not return response hook")
	}
	return submitted
}

func replayIdentifiedSubmit(t *testing.T, server *Server, params protocol.JobSubmitParams) protocol.JobSubmitResult {
	t.Helper()
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("replay submit error = %+v", outcome.err)
	}
	replayed, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("replay submit result type = %T", outcome.result)
	}
	if outcome.after != nil {
		t.Fatal("replay returned a launch hook")
	}
	return replayed
}

func submitIdentifiedViaScriptedRequest(t *testing.T, server *Server, params protocol.JobSubmitParams) protocol.JobSubmitResult {
	t.Helper()
	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, params, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want accepted non-replay", submitted)
	}
	return submitted
}

func clearAdmissionJobMarkersForTest(t *testing.T, server *Server) {
	t.Helper()
	server.mu.Lock()
	server.admissionJobs = make(map[string]*admissionInstance)
	server.mu.Unlock()
}

func setAdmissionRecordReleaseBeforeCommitHookForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := admissionRecordReleaseBeforeCommitForTest
	admissionRecordReleaseBeforeCommitForTest = hook
	t.Cleanup(func() {
		admissionRecordReleaseBeforeCommitForTest = previous
	})
}

func jobStatusViaHandler(t *testing.T, server *Server, params protocol.JobStatusParams) protocol.JobStatusResult {
	t.Helper()
	outcome := server.handleJobStatus(mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("job.status error = %+v", outcome.err)
	}
	status, ok := outcome.result.(protocol.JobStatusResult)
	if !ok {
		t.Fatalf("job.status result type = %T", outcome.result)
	}
	return status
}

func assertJobHandlerError(t *testing.T, outcome requestOutcome, code, admissionCause, jobID string) {
	t.Helper()
	if outcome.err == nil {
		t.Fatalf("handler result = %+v, want error code=%s cause=%s job=%s", outcome.result, code, admissionCause, jobID)
	}
	if outcome.err.Data.Code != code {
		t.Fatalf("handler error = %+v, want code %s", outcome.err, code)
	}
	if admissionCause != "" && outcome.err.Data.AdmissionCause != admissionCause {
		t.Fatalf("handler error = %+v, want admissionCause %s", outcome.err, admissionCause)
	}
	if jobID != "" && outcome.err.Data.JobID != jobID {
		t.Fatalf("handler error = %+v, want jobId %s", outcome.err, jobID)
	}
}

func jobResultViaHandler(t *testing.T, server *Server, params protocol.JobResultParams) protocol.JobResult {
	t.Helper()
	outcome := server.handleJobResult(mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("job.result error = %+v", outcome.err)
	}
	result, ok := outcome.result.(protocol.JobResult)
	if !ok {
		t.Fatalf("job.result result type = %T", outcome.result)
	}
	return result
}

func int64Pointer(value int64) *int64 {
	return &value
}

func assertTimeoutResolution(t *testing.T, got *engine.TimeoutResolution, wantRequested *int64, wantEffective int64, wantSource string) {
	t.Helper()
	if got == nil {
		t.Fatal("timeout resolution is nil")
	}
	if got.Effective != wantEffective || got.Source != wantSource {
		t.Fatalf("timeout resolution = %+v, want effective=%d source=%q", got, wantEffective, wantSource)
	}
	if wantRequested == nil {
		if got.Requested != nil {
			t.Fatalf("timeout requested = %d, want omitted", *got.Requested)
		}
		return
	}
	if got.Requested == nil || *got.Requested != *wantRequested {
		t.Fatalf("timeout requested = %v, want %d", got.Requested, *wantRequested)
	}
}

func jobCancelViaHandler(t *testing.T, server *Server, params protocol.JobCancelParams) protocol.JobCancelResult {
	t.Helper()
	outcome := server.handleJobCancel(mustMarshal(t, params))
	if outcome.err != nil {
		t.Fatalf("job.cancel error = %+v", outcome.err)
	}
	result, ok := outcome.result.(protocol.JobCancelResult)
	if !ok {
		t.Fatalf("job.cancel result type = %T", outcome.result)
	}
	return result
}

func assertNoAcceptedJobsInAdmission(t *testing.T, server *Server) {
	t.Helper()
	server.admissionStateMu.RLock()
	defer server.admissionStateMu.RUnlock()
	if server.admissionReady == nil {
		t.Fatal("admission authority is not ready")
	}
	snapshot, err := server.admissionReady.RuntimeSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0 {
		t.Fatalf("runtime snapshot = %+v, want no accepted work", snapshot)
	}
}

func assertNoAuthoritySafetyRecords(t *testing.T, server *Server) {
	t.Helper()
	assertAuthoritySafetyRecordCount(t, server, 0)
}

func assertAuthoritySafetyRecordCount(t *testing.T, server *Server, want int) {
	t.Helper()
	server.admissionStateMu.RLock()
	defer server.admissionStateMu.RUnlock()
	if server.admissionRepository == nil {
		t.Fatal("admission repository is not ready")
	}
	if err := server.admissionRepository.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		count := 0
		for _, image := range images {
			if image.Safety.State == repository.RecordValid {
				count++
			}
		}
		if count != want {
			t.Fatalf("authority safety records = %d, want %d", count, want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertNoEngineJobRecordsForCWD(t *testing.T, server *Server, cwd string) {
	t.Helper()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	store, err := server.storeForCWDLocked(canonicalCWD)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("engine job records = %+v, want none", records)
	}
}

func assertNoWorkspaceNamespaceForCWD(t *testing.T, root, cwd string) {
	t.Helper()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	namespace := filepath.Join(root, "workspaces", engine.WorkspaceKey(canonicalCWD))
	if _, err := os.Stat(namespace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace namespace stat error = %v, want not exist for %s", err, namespace)
	}
}

func testCodexHomePath(t *testing.T, root, cwd, jobID string) string {
	t.Helper()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "workspaces", engine.WorkspaceKey(canonicalCWD), "codex", jobID)
}

func markerCodexCLI(t *testing.T, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex-marker")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then
  printf probed > %q
  printf 'codex-cli %s\n'
  exit 0
fi
printf '{"type":"thread.started","thread_id":"codex-session"}\n{"type":"turn.completed","last_agent_message":"ok"}\n'
`, marker, codexcli.MinimumKnownGoodVersion)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

type attemptResult struct {
	text  string
	state engine.JobState
	err   error
}

type recordingCommandRunner struct {
	running command.RunningCommand
}

func (r recordingCommandRunner) Start(context.Context, command.ExecSpec) (command.RunningCommand, error) {
	return r.running, nil
}

type recordingRunningCommand struct {
	interrupts           atomic.Int64
	interruptHadDeadline atomic.Bool
	interruptErr         error
}

func (c *recordingRunningCommand) Stdin() io.WriteCloser { return nil }
func (c *recordingRunningCommand) Stdout() io.ReadCloser { return nil }
func (c *recordingRunningCommand) Stderr() io.ReadCloser { return nil }
func (c *recordingRunningCommand) Wait(context.Context) (command.ExitObservation, error) {
	return command.ExitObservation{}, nil
}
func (c *recordingRunningCommand) Interrupt(ctx context.Context) error {
	c.interrupts.Add(1)
	_, hasDeadline := ctx.Deadline()
	c.interruptHadDeadline.Store(hasDeadline)
	return c.interruptErr
}

func newControlledBackgroundRun(t *testing.T) (*Server, jobRun, *controlledSession, context.Context, context.CancelFunc) {
	t.Helper()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_controlled", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateStarting); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateRunning); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	session := &controlledSession{id: "controlled-session", events: make(chan engine.Event), started: make(chan struct{}, 1)}
	active := &activeJob{jobID: jobID, sessionID: "ses_controlled", session: session, cancel: cancel}
	run := jobRun{
		jobID:     jobID,
		sessionID: "ses_controlled",
		backend:   "fake",
		store:     store,
		session:   session,
		active:    active,
	}
	return server, run, session, ctx, cancel
}

func serveScriptedRequest(t *testing.T, server *Server, method string, params any, writeErr error) *scriptedConn {
	t.Helper()
	req := protocol.Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(strconvQuote("1")),
		Method:  method,
		Params:  mustMarshal(t, params),
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	conn := &scriptedConn{read: bytes.NewReader(append(raw, '\n')), writeErr: writeErr}
	c := &connection{server: server, conn: conn, hello: true}
	c.serve(context.Background())
	return conn
}

func TestConnectionServeAnswersOversizedRequest(t *testing.T) {
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	request := append([]byte(`{"jsonrpc":"2.0","id":"1","method":"job.status","params":{"padding":"`), bytes.Repeat([]byte("x"), 4*1024*1024)...)
	request = append(request, []byte(`"}}`+"\n")...)
	conn := &scriptedConn{read: bytes.NewReader(request)}
	(&connection{server: server, conn: conn, hello: true}).serve(context.Background())
	response := responseFromScriptedConn(t, conn)
	if response.Error == nil {
		t.Fatal("oversized request response error = nil")
	}
	if response.Error.Data.Code != protocol.ErrorInvalidTaskSpec || !strings.Contains(response.Error.Message, "exceeded") {
		t.Fatalf("oversized request response = %+v", response.Error)
	}
}

func TestConnectionServeBoundsOversizedRequestErrorWrite(t *testing.T) {
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	request := append([]byte(`{"jsonrpc":"2.0","id":"1","method":"job.status","params":{"padding":"`), bytes.Repeat([]byte("x"), 4*1024*1024)...)
	request = append(request, []byte(`"}}`+"\n")...)
	conn := &deadlineBlockingConn{read: bytes.NewReader(request), deadlineSet: make(chan time.Time, 1)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&connection{server: server, conn: conn, hello: true}).serve(context.Background())
	}()

	select {
	case deadline := <-conn.deadlineSet:
		if deadline.IsZero() || time.Until(deadline) > oversizedRequestWriteTimeout+time.Second {
			t.Fatalf("write deadline = %v, want short non-zero deadline", deadline)
		}
	case <-time.After(time.Second):
		t.Fatal("oversized request response did not set a write deadline")
	}
	select {
	case <-done:
	case <-time.After(oversizedRequestWriteTimeout + time.Second):
		t.Fatal("oversized request response write did not return by its deadline")
	}
	if !conn.isClosed() {
		t.Fatal("connection was not closed after the bounded response write")
	}
}

func waitControlledSessionStarted(t *testing.T, session *controlledSession) {
	t.Helper()
	select {
	case <-session.started:
	case <-time.After(time.Second):
		t.Fatal("controlled session did not start")
	}
}

func waitAttemptResult(t *testing.T, done <-chan attemptResult) attemptResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("runAttempt did not return")
		return attemptResult{}
	}
}

type scriptedConn struct {
	mu       sync.Mutex
	read     *bytes.Reader
	writes   bytes.Buffer
	writeErr error
	closed   bool
}

type deadlineBlockingConn struct {
	read        *bytes.Reader
	mu          sync.Mutex
	deadline    time.Time
	deadlineSet chan time.Time
	closed      bool
}

func (c *deadlineBlockingConn) Read(p []byte) (int, error) {
	return c.read.Read(p)
}

func (c *deadlineBlockingConn) Write([]byte) (int, error) {
	c.mu.Lock()
	deadline := c.deadline
	c.mu.Unlock()
	if deadline.IsZero() {
		return 0, errors.New("write began without a deadline")
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	<-timer.C
	return 0, os.ErrDeadlineExceeded
}

func (c *deadlineBlockingConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *deadlineBlockingConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "deadline-local", Net: "unix"}
}

func (c *deadlineBlockingConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "deadline-remote", Net: "unix"}
}

func (c *deadlineBlockingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *deadlineBlockingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *deadlineBlockingConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	if !deadline.IsZero() {
		select {
		case c.deadlineSet <- deadline:
		default:
		}
	}
	return nil
}

func (c *deadlineBlockingConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	return c.read.Read(p)
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	return c.writes.Write(p)
}

func (c *scriptedConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr {
	return &net.UnixAddr{Name: "scripted-local", Net: "unix"}
}

func (c *scriptedConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: "scripted-remote", Net: "unix"}
}

func (c *scriptedConn) SetDeadline(time.Time) error {
	return nil
}

func (c *scriptedConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *scriptedConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *scriptedConn) writesString() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes.String()
}

type blockingTestListener struct {
	closed    chan struct{}
	closeOnce sync.Once
	onClose   func()
}

func newBlockingTestListener() *blockingTestListener {
	return &blockingTestListener{closed: make(chan struct{})}
}

func (l *blockingTestListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingTestListener) Close() error {
	l.closeOnce.Do(func() {
		if l.onClose != nil {
			l.onClose()
		}
		close(l.closed)
	})
	return nil
}

func (l *blockingTestListener) Addr() net.Addr {
	return testNetAddr("blocking-listener")
}

func (l *blockingTestListener) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-l.closed:
	case <-time.After(time.Second):
		t.Fatal("blocking listener did not close")
	}
}

type pipeTestListener struct {
	conns     chan net.Conn
	closed    chan struct{}
	closeOnce sync.Once
}

func newPipeTestListener() *pipeTestListener {
	return &pipeTestListener{
		conns:  make(chan net.Conn),
		closed: make(chan struct{}),
	}
}

func (l *pipeTestListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeTestListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeTestListener) Addr() net.Addr {
	return testNetAddr("pipe-listener")
}

func (l *pipeTestListener) Dial() (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.conns <- server:
		return client, nil
	case <-l.closed:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	}
}

func assertPipeListenerAccepts(t *testing.T, listener *pipeTestListener, reason string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Dial()
		if err == nil {
			_ = conn.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("pipe listener did not accept %s: %v", reason, err)
		}
	case <-time.After(time.Second):
		t.Fatalf("pipe listener did not accept %s", reason)
	}
	select {
	case <-listener.closed:
		t.Fatalf("pipe listener was closed %s", reason)
	default:
	}
}

type testNetAddr string

func (a testNetAddr) Network() string { return "test" }

func (a testNetAddr) String() string { return string(a) }

func helloRaw(t *testing.T, conn net.Conn, r *bufio.Reader, token string) {
	t.Helper()
	resp := rpc(t, conn, r, "hello", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token})
	if resp.Error != nil {
		t.Fatalf("hello error = %+v", resp.Error)
	}
}

func rpc(t *testing.T, conn net.Conn, r *bufio.Reader, id, method string, params any) protocol.Response {
	t.Helper()
	resp, err := rpcResponseOrClose(t, conn, r, id, method, params)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func rpcResponseOrClose(t *testing.T, conn net.Conn, r *bufio.Reader, id, method string, params any) (protocol.Response, error) {
	t.Helper()
	req := protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(strconvQuote(id)), Method: method, Params: mustMarshal(t, params)}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		return protocol.Response{}, err
	}
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return protocol.Response{}, err
		}
		var head struct {
			ID     json.RawMessage `json:"id,omitempty"`
			Method string          `json:"method,omitempty"`
		}
		mustUnmarshal(t, line, &head)
		if head.Method != "" {
			continue
		}
		var resp protocol.Response
		mustUnmarshal(t, line, &resp)
		return resp, nil
	}
}

func isExpectedFailStopClose(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection reset by peer") || strings.Contains(msg, "use of closed network connection") || strings.Contains(msg, "broken pipe")
}

func readResponse(t *testing.T, r *bufio.Reader) protocol.Response {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var resp protocol.Response
	mustUnmarshal(t, line, &resp)
	return resp
}

func loadJobRecord(t *testing.T, root, cwd, jobID string, processes engine.ProcessTable) *engine.JobRecord {
	t.Helper()
	store, err := engine.NewStore(engine.StoreConfig{
		Root:      root,
		CWD:       cwd,
		Clock:     engine.ClockFunc(time.Now),
		Processes: processes,
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Load(jobID)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func waitJobState(t *testing.T, conn net.Conn, r *bufio.Reader, jobID string, want engine.JobState) {
	t.Helper()
	// Positive wait: generous under load. A 1s deadline failed several tests
	// at once during saturated multi-package race sweeps (a full job lifecycle
	// can exceed 1s when custodian race tests peg every core); polling returns
	// early on success, so a longer deadline costs passing runs nothing.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp := rpc(t, conn, r, "status-"+jobID, protocol.MethodJobResult, protocol.JobResultParams{JobID: jobID})
		var result protocol.JobResult
		decodeResult(t, resp, &result)
		if result.State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s", jobID, want)
}

func decodeResult(t *testing.T, resp protocol.Response, target any) {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("unexpected rpc error: %+v", resp.Error)
	}
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	mustUnmarshal(t, raw, target)
}

func assertRPCCode(t *testing.T, resp protocol.Response, code string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("expected error %s, got result %+v", code, resp.Result)
	}
	if resp.Error.Data.Code != code {
		t.Fatalf("error code = %s, want %s (%+v)", resp.Error.Data.Code, code, resp.Error)
	}
}

func assertRPCAdmissionCause(t *testing.T, resp protocol.Response, cause string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("expected admission cause %s, got result %+v", cause, resp.Result)
	}
	if resp.Error.Data.AdmissionCause != cause {
		t.Fatalf("admission cause = %q, want %q (%+v)", resp.Error.Data.AdmissionCause, cause, resp.Error)
	}
}

func mustServedRequestKey(t *testing.T, workspace, request string) model.RequestKey {
	t.Helper()
	key, err := model.NewRequestKey(workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func requireServedCorruptKind(t *testing.T, err error, kind string) {
	t.Helper()
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("error = %v, want ErrCorruptRecord", err)
	}
	var corrupt repository.CorruptRecordKindError
	if !errors.As(err, &corrupt) || corrupt.Kind != kind {
		t.Fatalf("error = %T %v, want corrupt kind %s", err, err, kind)
	}
}

func servedErrorTreeHasCorruptKind(err error, kind string) bool {
	if err == nil {
		return false
	}
	switch typed := err.(type) {
	case repository.CorruptRecordKindError:
		return typed.Kind == kind
	case interface{ Unwrap() []error }:
		for _, child := range typed.Unwrap() {
			if servedErrorTreeHasCorruptKind(child, kind) {
				return true
			}
		}
		return false
	case interface{ Unwrap() error }:
		return servedErrorTreeHasCorruptKind(typed.Unwrap(), kind)
	default:
		return false
	}
}

func corruptAdmissionBindingForTest(t *testing.T, server *Server, key model.RequestKey, diagnostic string) {
	t.Helper()
	corruptor, ok := server.admissionRepository.(interface {
		InjectCorruptBindingForTest(model.RequestKey, string)
	})
	if !ok {
		t.Fatalf("admission repository type = %T, want InjectCorruptBindingForTest", server.admissionRepository)
	}
	corruptor.InjectCorruptBindingForTest(key, diagnostic)
}

func corruptAdmissionSafetyForTest(t *testing.T, server *Server, jobID model.JobID, diagnostic string) {
	t.Helper()
	corruptor, ok := server.admissionRepository.(interface {
		InjectCorruptSafetyForTest(model.JobID, string)
	})
	if !ok {
		t.Fatalf("admission repository type = %T, want InjectCorruptSafetyForTest", server.admissionRepository)
	}
	corruptor.InjectCorruptSafetyForTest(jobID, diagnostic)
}

func assertAdmissionSnapshotFailStopTrip(t *testing.T, server *Server, root string, err error) {
	t.Helper()
	if reason := server.safetyLatch.Reason(); reason == nil || !errors.Is(reason, repository.ErrCorruptRecord) {
		t.Fatalf("safety latch reason = %v, want corrupt record trip reason", reason)
	}
	snapshot, loadErr := authority.LoadFileAnchorSnapshot(filepath.Join(root, admissionAnchorFile))
	if loadErr != nil {
		t.Fatalf("load admission anchor snapshot: %v", loadErr)
	}
	if snapshot.Phase != "fail_stopped" {
		t.Fatalf("anchor phase = %q, want fail_stopped after %v", snapshot.Phase, err)
	}
}

func bootstrapAdmissionRootForTest(t *testing.T, root string) (*Server, error) {
	t.Helper()
	server := newTestServerAtRoot(t, root, shortTempDir(t), newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, server, newAdmissionFakeLaunchCustodian(t), true)
	return server, server.bootstrapAdmission(context.Background())
}

func assertNoServeAdmissionPublished(t *testing.T, server *Server) {
	t.Helper()
	server.admissionStateMu.RLock()
	defer server.admissionStateMu.RUnlock()
	if server.admissionInstance != nil || server.admissionReady != nil || server.admissionRepository != nil || server.admissionClose != nil {
		t.Fatalf("admission state published after failed bootstrap: instance=%v ready=%v repo=%T close=%T", server.admissionInstance, server.admissionReady, server.admissionRepository, server.admissionClose)
	}
}

func assertAdmissionRootFilesExist(t *testing.T, root string) {
	t.Helper()
	assertPathExists(t, filepath.Join(root, admissionRepositoryFile))
	assertPathExists(t, filepath.Join(root, admissionAnchorFile))
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s stat = %v, want exists", path, err)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s stat = %v, want missing", path, err)
	}
}

func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func truncateAdmissionRepository(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < 2 {
		t.Fatalf("repository size = %d, want truncatable file", info.Size())
	}
	if err := os.Truncate(path, info.Size()/2); err != nil {
		t.Fatal(err)
	}
}

func flipPageHeaderContaining(t *testing.T, path string, needle []byte) {
	t.Helper()
	data := readFileBytes(t, path)
	offset := bytes.Index(data, needle)
	if offset < 0 {
		t.Fatalf("needle %q not found in %s", string(needle), path)
	}
	pageSize := os.Getpagesize()
	pageStart := offset - offset%pageSize
	if pageStart+9 >= len(data) {
		t.Fatalf("page header for %q starts outside file bounds", string(needle))
	}
	data[pageStart+8] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func initializedServedRootWithPendingJobForTest(t *testing.T, root, name string) (*Server, string) {
	t.Helper()
	server, err := bootstrapAdmissionRootForTest(t, root)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := server.admissionReady.Accept(context.Background(), authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: model.WorkspaceKey("workspace-" + name),
			RequestID:    model.RequestID("request-" + name),
		},
		WorkspaceLayoutKey: model.WorkspaceKey(strings.Repeat("b", 64)),
		TaskIdentity:       model.NewSHA256TaskIdentity([]byte("task-" + name)),
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          "session-" + name,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	return server, accepted.Record.JobID.String()
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustUnmarshal(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("unmarshal %q: %v", raw, err)
	}
}

func strconvQuote(s string) json.RawMessage {
	raw, _ := json.Marshal(s)
	return raw
}

func stringID(n int64) string {
	return strconv.FormatInt(n, 10)
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "ab-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
