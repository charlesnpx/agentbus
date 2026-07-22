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
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
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

type resumedSession struct {
	ID   string
	Opts engine.SessionOpts
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
		if _, err := running.Wait(ctx); err != nil {
			ch <- engine.Event{Type: engine.EventTerminalError, Text: err.Error()}
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
	if _, ok := hello.Capabilities["jobs.requestId"]; ok {
		t.Fatalf("jobs.requestId capability is advertised in hello: %+v", hello.Capabilities)
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
}

func TestServeConstructsAdmissionInstanceBeforeListenWithRequestIDCapabilityDisabled(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	listenErr := errors.New("listener reached after admission bootstrap")
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		if server.jobsRequestIDEnabled {
			return nil, socketFileIdentity{}, errors.New("jobs.requestId was enabled during default admission bootstrap")
		}
		if server.admissionInstance == nil || server.admissionInstance.ready == nil || server.admissionInstance.coordinator == nil || server.admissionInstance.submission == nil {
			return nil, socketFileIdentity{}, errors.New("admission instance was not ready before listen")
		}
		if server.admissionInstance.policy.runtimeAssessment.Class != custodian.SupportUnsupported {
			return nil, socketFileIdentity{}, fmt.Errorf("runtime assessment = %s, want unsupported", server.admissionInstance.policy.runtimeAssessment.Class)
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
	if err != nil {
		t.Fatalf("bootstrapAdmission error = %v", err)
	}
	if server.admissionInstance == nil {
		t.Fatal("admission instance was not constructed")
	}
	if server.jobsRequestIDEnabled {
		t.Fatal("jobs.requestId was enabled by admission bootstrap")
	}
	support := server.admissionInstance.policy.runtimeSupport
	if support.ParkedExec || support.VerifiedContainment || !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %+v, want unavailable supervisor", support)
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("%s stat error = %v, want exists", name, err)
		}
	}
}

func TestAdmissionBootstrapFailureFailsServeClosedBeforeListen(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
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

func TestAdmissionStartupRunsLegacyReapBeforeListenWithRequestIDCapabilityDisabled(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	clock := engine.ClockFunc(func() time.Time { return base })
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	server, err := New(Config{
		StateRoot:    root,
		CWD:          cwd,
		Token:        "test-token",
		Backends:     []engine.Backend{newFakeBackend("fake")},
		ProcessTable: fakeProcessTable{},
		Clock:        clock,
		IdleTimeout:  -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_legacy_reap", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateStarting); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, jobID, engine.StateRunning); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(store.Layout().Jobs, jobID+".json")
	listenErr := errors.New("listener reached after legacy reap")
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		if server.jobsRequestIDEnabled {
			return nil, socketFileIdentity{}, errors.New("jobs.requestId was enabled by admission bootstrap")
		}
		if server.admissionInstance == nil {
			return nil, socketFileIdentity{}, errors.New("admission instance was not constructed before listen")
		}
		raw, err := os.ReadFile(statePath)
		if err != nil {
			return nil, socketFileIdentity{}, err
		}
		var record engine.JobRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, socketFileIdentity{}, err
		}
		if record.State != engine.StateOrphaned {
			return nil, socketFileIdentity{}, errors.New("legacy job was not reaped before listen")
		}
		return nil, socketFileIdentity{}, listenErr
	}

	err = server.Serve(context.Background())
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want %v", err, listenErr)
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("%s stat error = %v, want exists", name, err)
		}
	}
}

func TestServeClearsAdmissionStateAndSequentialReserveRecovers(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	start := func() (context.CancelFunc, chan error) {
		t.Helper()
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
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnavailableNativeRuntime)
	if resp.Error.Data.RuntimeSupport == nil || resp.Error.Data.RuntimeSupport.Class != custodian.SupportUnsupported.String() {
		t.Fatalf("runtime support data = %+v, want unsupported assessment", resp.Error.Data.RuntimeSupport)
	}

	resp = rpc(t, conn, reader, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: jobID})
	var statuses protocol.JobStatusResult
	decodeResult(t, resp, &statuses)
	if len(statuses.Jobs) != 1 || statuses.Jobs[0].JobID != jobID || statuses.Jobs[0].State != engine.StateFailed {
		t.Fatalf("job.status = %+v, want recovered failed authority job %s", statuses, jobID)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
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

	if err := server.completeAdmissionRun(jobRun{jobID: jobID}, engine.StateCompleted, "done"); err != nil {
		t.Fatal(err)
	}
	got := logs.String()
	if !strings.Contains(got, jobID) || !strings.Contains(got, "startup recovery must finalize the durably accepted obligation") {
		t.Fatalf("completion log = %q, want job id and recovery obligation", got)
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
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectStrictRouteDisabled)
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
	if _, err := firstServer.admissionReady.RecordOutcome(ctx, record.JobID, ref, model.OutcomeFailed); err != nil {
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
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
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

func TestDefaultStrictIdentifiedSubmitRejectsUnavailableRuntimeWithoutStartingBackend(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	resp := rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-disabled",
		RequestID:    "request-disabled",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"},
	})
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnavailableNativeRuntime)
	if resp.Error.Data.RuntimeSupport == nil || resp.Error.Data.RuntimeSupport.Class != custodian.SupportUnsupported.String() {
		t.Fatalf("runtime support data = %+v, want unsupported assessment", resp.Error.Data.RuntimeSupport)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
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
		availableRuntime bool
		params           func(cwd string) protocol.JobSubmitParams
		wantCode         string
		wantCause        string
	}
	cases := []testCase{
		{
			name:             "missing identity before backend",
			availableRuntime: true,
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
			name:             "unsupported backend before invalid config",
			availableRuntime: true,
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
			availableRuntime: true,
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
			name:             "strict route disabled before missing identity",
			availableRuntime: true,
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
			wantCause: protocol.AdmissionRejectStrictRouteDisabled,
		},
		{
			name:             "invalid config before unavailable runtime",
			availableRuntime: false,
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-invalid-config",
					RequestID:    "request-invalid-config",
					TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false},
				}
			},
			wantCode:  protocol.ErrorInvalidTaskSpec,
			wantCause: protocol.AdmissionRejectInvalidStrictConfig,
		},
		{
			name:             "unavailable runtime terminal cause",
			availableRuntime: false,
			params: func(cwd string) protocol.JobSubmitParams {
				return protocol.JobSubmitParams{
					WorkspaceKey: "workspace-runtime-unavailable",
					RequestID:    "request-runtime-unavailable",
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
			if tt.availableRuntime {
				enableTestAdmission(t, server, launcher)
			} else {
				if err := server.bootstrapAdmission(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
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
			if tt.wantCause == protocol.AdmissionRejectUnavailableNativeRuntime && outcome.err.Data.RuntimeSupport == nil {
				t.Fatal("runtime unavailable rejection did not carry runtime support assessment")
			}
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
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectStrictRouteDisabled)
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

func TestIdentifiedSubmitRejectsBuiltInWhenFencedRuntimeUnavailableBeforeBackendStart(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmissionWithParkedExec(t, server, launcher, false)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-built-in-unfenced",
		RequestID:    "request-built-in-unfenced",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	assertRPCAdmissionCause(t, resp, protocol.AdmissionRejectUnavailableNativeRuntime)
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before incompatible pre-accept reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
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

func TestLegacyFencedWaitCleanupFailureRecordsQuiescenceBeforeError(t *testing.T) {
	t.Parallel()
	cleanupErr := errors.New("retained remove failed")
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.cleanupErr = cleanupErr
	repo, recorder, coordinator := newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t, launcher)
	cmd := prepareLegacyFencedCommandWithCoordinatorForTest(t, coordinator, "request-legacy-fenced-cleanup-failure")

	if err := cmd.grantAndRelease(context.Background()); err != nil {
		t.Fatalf("grantAndRelease error = %v", err)
	}
	select {
	case <-cmd.done:
	case <-time.After(time.Second):
		t.Fatal("legacy fenced command did not finish")
	}
	_, finalErr := cmd.result()
	if !errors.Is(finalErr, cleanupErr) {
		t.Fatalf("legacy fenced command final error = %v, want cleanup failure", finalErr)
	}
	recordErrs, _ := recorder.recordQuiescenceContextObservations()
	if len(recordErrs) != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", len(recordErrs))
	}
	if got := launcher.containCount(); got != 0 {
		t.Fatalf("legacy fenced containments = %d, want 0 cleanup retry", got)
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Grant == nil || first.Released == nil || first.Quiescence == nil {
		t.Fatalf("legacy fenced cleanup-failure launch proof = %+v, want grant/release/quiescence recorded", first)
	}
}

func TestLegacyFencedPreparedAbortCleanupFailureRecordsQuiescenceBeforeError(t *testing.T) {
	t.Parallel()
	cleanupErr := errors.New("prepared abort backend close failed")
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.abortCleanupErr = cleanupErr
	repo, recorder, coordinator := newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t, launcher)
	cmd := prepareLegacyFencedCommandWithCoordinatorForTest(t, coordinator, "request-legacy-fenced-abort-cleanup-failure")

	err := coordinator.abortLegacyPrepared(context.Background(), cmd.prepared, true, cmd.launchContext)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("abortLegacyPrepared error = %v, want cleanup failure", err)
	}
	if got := launcher.abortCount(); got != 1 {
		t.Fatalf("legacy fenced aborts = %d, want 1", got)
	}
	recordErrs, _ := recorder.recordQuiescenceContextObservations()
	if len(recordErrs) != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", len(recordErrs))
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Quiescence == nil || first.Grant != nil || first.Released != nil {
		t.Fatalf("legacy fenced abort-cleanup launch proof = %+v, want retired without grant/release", first)
	}
}

func TestLegacyFencedRejectCleanupFailureRecordsQuiescenceBeforeError(t *testing.T) {
	t.Parallel()
	cleanupErr := errors.New("prepared reject backend close failed")
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.abortCleanupErr = cleanupErr
	repo, recorder, coordinator := newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t, launcher)
	cmd := prepareLegacyFencedCommandWithCoordinatorForTest(t, coordinator, "request-legacy-fenced-reject-cleanup-failure")

	err := coordinator.rejectAndRetireLegacyFenced(context.Background(), cmd)
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("rejectAndRetireLegacyFenced error = %v, want cleanup failure", err)
	}
	if got := launcher.abortCount(); got != 1 {
		t.Fatalf("legacy fenced aborts = %d, want 1", got)
	}
	recordErrs, _ := recorder.recordQuiescenceContextObservations()
	if len(recordErrs) != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", len(recordErrs))
	}
	_, finalErr := cmd.result()
	if !errors.Is(finalErr, cleanupErr) {
		t.Fatalf("legacy fenced command final error = %v, want cleanup failure", finalErr)
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	assertLegacyFencedRejectedWithoutGrant(t, safety)
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

func TestLegacyFencedDeliveryFailureRetiresWithDetachedContextWhenCanceled(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	cmd := prepareLegacyFencedCommandForTest(t, server, "request-legacy-fenced-canceled-context")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := server.admissionSubmission.rejectAndRetireLegacyFenced(ctx, cmd); err != nil {
		t.Fatalf("rejectAndRetireLegacyFenced with canceled context error = %v", err)
	}
	assertLegacyFencedAbortUsedDetachedContext(t, launcher)
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after failed response", got)
	}
	safety := loadAdmissionSafetyRecord(t, server, string(cmd.launchContext.JobID))
	assertLegacyFencedRejectedWithoutGrant(t, safety)
}

func TestLegacyFencedReleaseUnknownContainsWithDetachedContextWhenCanceled(t *testing.T) {
	t.Parallel()
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.releaseOutcome = custodian.ReleaseOutcomeUnknown
	launcher.releaseWaitForContextDone = true
	launcher.releaseStarted = make(chan struct{}, 1)
	repo, recorder, coordinator := newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t, launcher)
	cmd := prepareLegacyFencedCommandWithCoordinatorForTest(t, coordinator, "request-legacy-fenced-release-unknown-canceled")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- cmd.grantAndRelease(ctx)
	}()
	select {
	case <-launcher.releaseStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("legacy fenced fake release did not start")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, launch.ErrReleaseUncertain) {
			t.Fatalf("grantAndRelease error = %v, want release uncertainty", err)
		}
	case <-time.After(time.Second):
		t.Fatal("grantAndRelease did not return after release context cancellation")
	}
	if got := launcher.releaseCount(); got != 1 {
		t.Fatalf("legacy fenced releases = %d, want 1", got)
	}
	if got := launcher.containCount(); got != 1 {
		t.Fatalf("legacy fenced containments = %d, want exactly one", got)
	}
	containErrs, containDeadlines := launcher.containContextObservations()
	if len(containErrs) != 1 || containErrs[0] != nil {
		t.Fatalf("contain context errors = %+v, want one live context", containErrs)
	}
	if len(containDeadlines) != 1 || !containDeadlines[0] {
		t.Fatalf("contain context deadlines = %+v, want bounded cleanup context", containDeadlines)
	}
	recordErrs, recordDeadlines := recorder.recordQuiescenceContextObservations()
	if len(recordErrs) != 1 || recordErrs[0] != nil {
		t.Fatalf("record quiescence context errors = %+v, want one live context", recordErrs)
	}
	if len(recordDeadlines) != 1 || !recordDeadlines[0] {
		t.Fatalf("record quiescence context deadlines = %+v, want bounded cleanup context", recordDeadlines)
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Grant == nil || first.Quiescence == nil {
		t.Fatalf("legacy fenced release-unknown launch proof = %+v, want grant and verified quiescence", first)
	}
	if safety.Terminal == nil || safety.Terminal.Outcome != model.OutcomeFailed || safety.Terminal.Cause != model.CauseReleaseOutcomeUnknown {
		outcomes, errs := recorder.recordReleaseOutcomeObservations()
		t.Fatalf("legacy fenced release-unknown terminal = %+v launch=%+v release_outcomes=%+v release_outcome_errs=%+v, want failed release_outcome_unknown", safety.Terminal, first, outcomes, errs)
	}
	_, finalErr := cmd.result()
	if !errors.Is(finalErr, launch.ErrReleaseUncertain) || errors.Is(finalErr, launch.ErrFailClosed) {
		t.Fatalf("legacy fenced command final error = %v, want release uncertainty without fail-closed", finalErr)
	}
}

func TestLegacyFencedReleaseDefinitelyNotSentFailStopsWhenReleaseOutcomeFactCannotPersist(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		durability launch.DurabilityOutcome
		err        error
		wantErr    error
	}{
		{
			name:       "definitely-not-committed",
			durability: launch.DefinitelyNotCommitted,
			err:        errors.New("injected release outcome not committed"),
			wantErr:    launch.ErrDurabilityNotCommitted,
		},
		{
			name:       "unknown-mutation",
			durability: launch.CommitOutcomeUnknown,
			err:        errors.New("injected release outcome commit unknown"),
			wantErr:    launch.ErrDurabilityUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			launcher := newAdmissionFakeLaunchCustodian(t)
			launcher.releaseOutcome = custodian.ReleaseDefinitelyNotSent
			launcher.releaseErr = errors.New("release frame not written")
			repo, recorder, submission := newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t, launcher)
			recorder.recordReleaseOutcomeDurability = tt.durability
			recorder.recordReleaseOutcomeErr = tt.err
			cmd := prepareLegacyFencedCommandWithCoordinatorForTest(t, submission, "request-legacy-fenced-release-not-sent-"+tt.name)

			err := cmd.grantAndRelease(context.Background())
			if !errors.Is(err, launch.ErrFailClosed) {
				t.Fatalf("grantAndRelease error = %v, want fail-closed", err)
			}
			if !errors.Is(err, launch.ErrReleaseUncertain) {
				t.Fatalf("grantAndRelease error = %v, want release uncertainty", err)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("grantAndRelease error = %v, want %v", err, tt.wantErr)
			}
			reason := submission.latch.Reason()
			if reason == nil {
				t.Fatal("safety latch was not tripped")
			}
			if text := reason.Error(); !strings.Contains(text, "LaunchReleaseNotSent") || !strings.Contains(text, "record_release_outcome") {
				t.Fatalf("fail-stop reason = %q, want unpersisted LaunchReleaseNotSent record_release_outcome fact", text)
			}
			_, finalErr := cmd.result()
			if !errors.Is(finalErr, launch.ErrFailClosed) {
				t.Fatalf("legacy fenced command final error = %v, want fail-closed", finalErr)
			}
			if got := launcher.abortCount(); got != 1 {
				t.Fatalf("legacy fenced aborts = %d, want one retired parked worker", got)
			}
			safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
			first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
			if !ok || first.Group == nil || first.Grant == nil || first.Quiescence == nil {
				t.Fatalf("legacy fenced release-not-sent launch proof = %+v, want grant and verified abort quiescence", first)
			}
			if first.ReleaseOutcome != nil || first.Released != nil {
				t.Fatalf("legacy fenced release-not-sent launch proof = %+v, want no release outcome/release facts", first)
			}
			if safety.Terminal != nil {
				t.Fatalf("legacy fenced release-not-sent terminal = %+v, want no terminal without release outcome fact", safety.Terminal)
			}
			if intent, ok := admissionRecordedReleaseFailureIntent(safety); ok {
				t.Fatalf("derived release-failure terminal intent without release outcome fact: %+v", intent)
			}
			outcomes, errs := recorder.recordReleaseOutcomeObservations()
			if len(outcomes) != 1 || outcomes[0] != model.LaunchReleaseNotSent {
				t.Fatalf("recorded release outcome attempts = %+v, want one LaunchReleaseNotSent attempt", outcomes)
			}
			if len(errs) != 1 || !errors.Is(errs[0], tt.err) {
				t.Fatalf("recorded release outcome errors = %+v, want %v", errs, tt.err)
			}
		})
	}
}

func TestLegacyFencedDeliveryFailureFailStopsWhenAbortCleanupDeadlineExpires(t *testing.T) {
	oldTimeout := admissionDetachedCleanupTimeout
	admissionDetachedCleanupTimeout = 5 * time.Millisecond
	t.Cleanup(func() {
		admissionDetachedCleanupTimeout = oldTimeout
	})

	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.abortWaitForContextDone = true
	launcher.abortRespectContext = true
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	cmd := prepareLegacyFencedCommandForTest(t, server, "request-legacy-fenced-abort-deadline")

	err := server.admissionSubmission.rejectAndRetireLegacyFenced(context.Background(), cmd)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rejectAndRetireLegacyFenced error = %v, want abort deadline exceeded", err)
	}
	if !errors.Is(err, launch.ErrFailClosed) {
		t.Fatalf("rejectAndRetireLegacyFenced error = %v, want fail-closed", err)
	}
	if got := launcher.abortCount(); got != 1 {
		t.Fatalf("legacy fenced aborts = %d, want one failed abort", got)
	}
	errs, deadlines := launcher.abortContextObservations()
	if len(errs) != 1 || !errors.Is(errs[0], context.DeadlineExceeded) {
		t.Fatalf("abort context errors = %+v, want deadline exceeded", errs)
	}
	if len(deadlines) != 1 || !deadlines[0] {
		t.Fatalf("abort context deadlines = %+v, want bounded cleanup context", deadlines)
	}
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after failed response", got)
	}

	snapshot := admissionAnchorSnapshot(t, anchorStore)
	if snapshot.Phase != "fail_stopped" || !strings.Contains(snapshot.Reason, "retire legacy fenced prepared process") || !strings.Contains(snapshot.Reason, context.DeadlineExceeded.Error()) {
		t.Fatalf("anchor snapshot = %+v, want durable fail-stop with abort deadline error", snapshot)
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Quiescence != nil || first.Grant != nil || first.Released != nil {
		t.Fatalf("legacy fenced failed abort launch proof = %+v, want bound but unretired launch under fail-stop", first)
	}
}

func TestLegacyFencedDeliveryFailureRetiresWhenBeginRejectFails(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	enableTestAdmissionWithAuthorityStore(t, server, launcher, repo, anchorStore)
	cmd := prepareLegacyFencedCommandForTest(t, server, "request-legacy-fenced-begin-reject-fails")

	beginRejectErr := errors.New("begin reject advance failed")
	anchorStore.FailNextForTest(authority.AnchorAdvance, beginRejectErr)
	err := server.admissionSubmission.rejectAndRetireLegacyFenced(context.Background(), cmd)
	if !errors.Is(err, beginRejectErr) {
		t.Fatalf("rejectAndRetireLegacyFenced error = %v, want begin reject failure", err)
	}
	if !errors.Is(err, launch.ErrFailClosed) {
		t.Fatalf("rejectAndRetireLegacyFenced error = %v, want fail-closed", err)
	}
	assertLegacyFencedAbortUsedDetachedContext(t, launcher)
	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after failed response", got)
	}
	snapshot := admissionAnchorSnapshot(t, anchorStore)
	if snapshot.Phase != "fail_stopped" || !strings.Contains(snapshot.Reason, beginRejectErr.Error()) {
		t.Fatalf("anchor snapshot = %+v, want durable fail-stop with injected error", snapshot)
	}
	safety := loadAuthoritySafetyRecordFromRepository(t, repo, string(cmd.launchContext.JobID))
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Grant != nil || first.Released != nil {
		t.Fatalf("legacy fenced failed BeginReject launch proof = %+v, want no grant/release", first)
	}
}

func TestStrictIdentifiedRejectsExternalRunnerWithoutLegacyUnfencedFallback(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.parkable = false
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmissionWithParkedExec(t, server, launcher, false)

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
	enableTestAdmissionWithParkedExec(t, server, launcher, false)

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

func TestIdentifiedFencedJobReadsAuthorityWithoutJSONRecordAndDiagnosesDuplicate(t *testing.T) {
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
	if err := server.createQueuedRecord(store, submitted.JobID, "ses_stale_json", "fake", nil, nil, nil, false); err != nil {
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
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_json", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	legacyExactStatus := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: legacyID})
	if len(legacyExactStatus.Jobs) != 1 || legacyExactStatus.Jobs[0].JobID != legacyID || legacyExactStatus.Jobs[0].State != engine.StateQueued {
		t.Fatalf("legacy exact status = %+v, want JSON queued", legacyExactStatus)
	}

	all := jobStatusViaHandler(t, server, protocol.JobStatusParams{All: true})
	byID := map[string]protocol.JobStatus{}
	for _, job := range all.Jobs {
		if _, exists := byID[job.JobID]; exists {
			t.Fatalf("duplicate all-status row for %s in %+v", job.JobID, all)
		}
		byID[job.JobID] = job
	}
	if got := byID[submitted.JobID]; got.State != engine.StateCompleted || !containsString(got.Warnings, duplicateAuthorityJSONWarning) {
		t.Fatalf("duplicate authority status = %+v, want completed with duplicate warning", got)
	}
	if got := byID[legacyID]; got.State != engine.StateQueued {
		t.Fatalf("legacy all-status row = %+v, want queued", got)
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
	if result.JobID != submitted.JobID || result.State != engine.StateCompleted || result.Result == nil || result.Result.ResultPath != expectedPath {
		t.Fatalf("authority result = %+v, want completed result at %q", result, expectedPath)
	}
	server.mu.Lock()
	server.jobStores = make(map[string]*engine.Store)
	server.mu.Unlock()
	again := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: submitted.JobID})
	if again.Result == nil || again.Result.ResultPath != expectedPath {
		t.Fatalf("authority result after clearing jobStores again = %+v, want path %q", again, expectedPath)
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

func TestJobCancelUsesLegacyJSONWhenAuthorityCleanlyDisclaims(t *testing.T) {
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
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_cancel_authority_down", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, errObj := server.authorityJobProjection(legacyID); ok || errObj != nil {
		t.Fatalf("authority projection ok=%v err=%+v, want clean disclaimer", ok, errObj)
	}

	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: legacyID})
	if canceled.JobID != legacyID || canceled.State != engine.StateCanceled {
		t.Fatalf("legacy cancel = %+v, want JSON canceled", canceled)
	}
	record, err := store.Load(legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != engine.StateCanceled {
		t.Fatalf("legacy record state = %s, want canceled", record.State)
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
	if err := server.createQueuedRecord(store, submitted.JobID, projection.SessionID, "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	server.removeActiveJob(submitted.JobID)
	if err := server.admissionReady.FailStop(context.Background(), "test authority degraded during cancel"); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobCancel(mustMarshal(t, protocol.JobCancelParams{JobID: submitted.JobID}))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorBackendUnavailable {
		t.Fatalf("job.cancel outcome = %+v, want backend_unavailable fail-closed error", outcome)
	}
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

func TestReapKnownStoresSkipsAuthorityOwnedFencedJob(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	var clockNanos atomic.Int64
	clockNanos.Store(base.UnixNano())
	clock := engine.ClockFunc(func() time.Time { return time.Unix(0, clockNanos.Load()).UTC() })
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.clock = clock
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-reap",
		RequestID:    "request-authority-reap",
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var fenced protocol.JobSubmitResult
	decodeResult(t, resp, &fenced)
	waitBackendStarted(t, backend)
	assertNoJSONJobRecord(t, server, fenced.JobID)
	before := loadAdmissionSafetyRecord(t, server, fenced.JobID)

	store := server.storeForJob(fenced.JobID)
	if store == nil {
		t.Fatalf("job store missing for %s", fenced.JobID)
	}
	legacyID := server.nextID("job")
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_reap_with_authority", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, legacyID, engine.StateStarting); err != nil {
		t.Fatal(err)
	}
	if err := server.transitionRecord(store, legacyID, engine.StateRunning); err != nil {
		t.Fatal(err)
	}
	// A running record whose supervisor identity CONFIRMS renews its lease
	// instead of orphaning (fail-closed identity cuts both ways). The orphan
	// scenario this test needs is a legacy job whose owning daemon departed:
	// point the record at a supervisor that no longer exists.
	if _, err := store.Update(legacyID, func(record *engine.JobRecord) (bool, error) {
		record.Supervisor = engine.ProcessRef{PID: 4999999, StartTime: "departed-daemon"}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}

	clockNanos.Store(base.Add(31 * time.Minute).UnixNano())
	if err := server.reapKnownStores(); err != nil {
		t.Fatal(err)
	}
	assertNoJSONJobRecord(t, server, fenced.JobID)
	after := loadAdmissionSafetyRecord(t, server, fenced.JobID)
	if after.Revision != before.Revision || after.Terminal != before.Terminal || after.Cancel != before.Cancel {
		t.Fatalf("authority record changed across JSON reap: before=%+v after=%+v", before, after)
	}
	legacy, err := store.Load(legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.State != engine.StateOrphaned {
		t.Fatalf("legacy reap state = %s, want orphaned", legacy.State)
	}
}

func TestSessionResumeDoesNotCreateLegacySessionForAuthorityOwnedJob(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.resumes = make(chan resumedSession, 1)
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-resume",
		RequestID:    "request-authority-resume",
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
	if err := server.createQueuedRecord(store, submitted.JobID, projection.SessionID, "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.updateBackendSessionID(store, submitted.JobID, "stale-backend-session"); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleSessionResume(context.Background(), mustMarshal(t, protocol.SessionResumeParams{SessionID: projection.SessionID}))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorBackendUnavailable {
		t.Fatalf("resume outcome = %+v, want backend_unavailable", outcome)
	}
	select {
	case got := <-backend.resumes:
		t.Fatalf("backend resumed stale authority session: %+v", got)
	default:
	}
}

func TestSessionResumeFailsClosedWhenAuthorityDegradedWithStaleJSONDuplicate(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	backend := newFakeBackend("fake")
	backend.resumes = make(chan resumedSession, 1)
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-authority-resume-degraded",
		RequestID:    "request-authority-resume-degraded",
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
	if err := server.createQueuedRecord(store, submitted.JobID, projection.SessionID, "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.updateBackendSessionID(store, submitted.JobID, "stale-backend-session"); err != nil {
		t.Fatal(err)
	}
	server.removeActiveJob(submitted.JobID)
	if err := server.admissionReady.FailStop(context.Background(), "test authority degraded during resume"); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleSessionResume(context.Background(), mustMarshal(t, protocol.SessionResumeParams{SessionID: projection.SessionID}))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorBackendUnavailable {
		t.Fatalf("resume outcome = %+v, want backend_unavailable fail-closed error", outcome)
	}
	if outcome.result != nil {
		t.Fatalf("resume result = %+v, want no legacy session result", outcome.result)
	}
	select {
	case got := <-backend.resumes:
		t.Fatalf("backend resumed stale authority session: %+v", got)
	default:
	}
}

func TestSessionResumeUsesLegacyJSONWhenAuthorityCleanlyDisclaims(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.resumes = make(chan resumedSession, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	legacyID := server.nextID("job")
	sessionID := "ses_legacy_resume_authority_clean"
	backendSessionID := "backend-session-authority-clean"
	if err := server.createQueuedRecord(store, legacyID, sessionID, "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.updateBackendSessionID(store, legacyID, backendSessionID); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, errObj := server.authorityJobProjection(legacyID); ok || errObj != nil {
		t.Fatalf("authority projection ok=%v err=%+v, want clean disclaimer", ok, errObj)
	}

	outcome := server.handleSessionResume(context.Background(), mustMarshal(t, protocol.SessionResumeParams{SessionID: sessionID}))
	if outcome.err != nil {
		t.Fatalf("resume error = %+v", outcome.err)
	}
	result, ok := outcome.result.(protocol.SessionStartResult)
	if !ok {
		t.Fatalf("resume result type = %T", outcome.result)
	}
	if result.SessionID != sessionID || result.Backend != "fake" {
		t.Fatalf("resume result = %+v, want legacy session", result)
	}
	select {
	case got := <-backend.resumes:
		if got.ID != backendSessionID || got.Opts.CWD != canonicalCWD || got.Opts.Write {
			t.Fatalf("backend resume = %+v, want id %q cwd %q read-only", got, backendSessionID, canonicalCWD)
		}
	default:
		t.Fatal("backend resume was not called for cleanly disclaimed legacy session")
	}
}

func TestExactReadsUseJSONForLegacyWhenAuthorityFailStopped(t *testing.T) {
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
	if err := server.createQueuedRecord(store, legacyID, "ses_legacy_json_authority_down", "fake", nil, nil, nil, false); err != nil {
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
	if err := server.createQueuedRecord(fencedStore, fenced.JobID, "ses_stale_json_authority_down", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := server.admissionReady.FailStop(context.Background(), "test authority degraded"); err != nil {
		t.Fatal(err)
	}

	legacyStatus := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: legacyID})
	if len(legacyStatus.Jobs) != 1 || legacyStatus.Jobs[0].JobID != legacyID || legacyStatus.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("legacy exact status = %+v, want JSON completed despite fail-stopped authority", legacyStatus)
	}
	legacyResult := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: legacyID})
	if legacyResult.JobID != legacyID || legacyResult.State != engine.StateCompleted || legacyResult.Result == nil || legacyResult.Result.Text != "legacy result" {
		t.Fatalf("legacy exact result = %+v, want JSON result despite fail-stopped authority", legacyResult)
	}

	fencedStatus := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: fenced.JobID})
	if len(fencedStatus.Jobs) != 1 || fencedStatus.Jobs[0].JobID != fenced.JobID || fencedStatus.Jobs[0].State != engine.StateQueued || fencedStatus.Jobs[0].SessionID != "ses_stale_json_authority_down" {
		t.Fatalf("fenced job.status = %+v, want stale JSON fallback while authority fail-stopped", fencedStatus)
	}
	fencedResult := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: fenced.JobID})
	if fencedResult.JobID != fenced.JobID || fencedResult.State != engine.StateQueued || fencedResult.Result != nil {
		t.Fatalf("fenced job.result = %+v, want stale JSON fallback while authority fail-stopped", fencedResult)
	}
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
	if got := len(server.listKnownRecords()); got != 0 {
		t.Fatalf("known engine records = %d, want 0 after fail-closed authority submit", got)
	}
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
	if errObj := server.failStoppedRequestError(protocol.MethodJobSubmit); errObj == nil || errObj.Data.Code != protocol.ErrorBackendUnavailable {
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
			return []engine.Event{{Type: engine.EventAgentText, Text: "FAIL\n"}}
		}
		return []engine.Event{{Type: engine.EventAgentText, Text: "PASS\n"}}
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
				Contract: &engine.ContractSpec{Shape: &engine.ShapeSpec{FirstLineEnum: []string{"PASS"}}},
				Retry: &engine.RetryPolicy{
					Max:      1,
					Template: "Your response missed: {{missing}}. Emit the corrected report only; make no further changes.",
				},
			},
		},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarts(t, backend, 2)
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

func TestTurnNotificationsCorrelationPolicyStampAndJobResult(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	start := rpc(t, conn, r, "2", protocol.MethodSessionStart, protocol.SessionStartParams{
		Backend: "fake",
		CWD:     h.cwd,
		Write:   true,
		Tags:    map[string]string{"client": "test"},
	})
	var session protocol.SessionStartResult
	decodeResult(t, start, &session)

	write := false
	turnResp := rpc(t, conn, r, "3", protocol.MethodTurnStart, protocol.TurnStartParams{
		SessionID: session.SessionID,
		Prompt:    "inspect",
		Write:     &write,
		Policy: &engine.TurnPolicy{Contract: &engine.ContractSpec{Shape: &engine.ShapeSpec{
			FirstLineEnum:    []string{"PASS"},
			RequiredSections: []string{"Findings"},
		}}},
	})
	var turn protocol.TurnStartResult
	decodeResult(t, turnResp, &turn)
	if turn.TurnID != turn.JobID {
		t.Fatalf("turnId %q != jobId %q", turn.TurnID, turn.JobID)
	}

	event := readNotification(t, r)
	if event.Method != protocol.NotificationTurnEvent {
		t.Fatalf("method = %s", event.Method)
	}
	var eventParams protocol.TurnEventParams
	mustUnmarshal(t, event.Params, &eventParams)
	if eventParams.SessionID != session.SessionID || eventParams.TurnID != turn.TurnID || eventParams.JobID != turn.JobID {
		t.Fatalf("event correlation = %+v", eventParams)
	}

	resultNotice := readNotification(t, r)
	if resultNotice.Method != protocol.NotificationTurnResult {
		t.Fatalf("method = %s", resultNotice.Method)
	}
	var turnResult protocol.TurnResultParams
	mustUnmarshal(t, resultNotice.Params, &turnResult)
	if turnResult.Contract == nil || turnResult.Contract.Status != engine.ContractCompliant {
		t.Fatalf("contract stamp = %+v", turnResult.Contract)
	}
	if turnResult.Result == nil || turnResult.Result.Text == "" || turnResult.Result.ResultPath == "" {
		t.Fatalf("turn result = %+v", turnResult.Result)
	}

	jobResp := rpc(t, conn, r, "4", protocol.MethodJobResult, protocol.JobResultParams{JobID: turn.JobID})
	var jobResult protocol.JobResult
	decodeResult(t, jobResp, &jobResult)
	if jobResult.JobID != turn.JobID || jobResult.Contract == nil || jobResult.Contract.Status != engine.ContractCompliant {
		t.Fatalf("job.result = %+v", jobResult)
	}
	gotTurn := <-backend.turns
	if gotTurn.Write {
		t.Fatalf("turn write = true, want per-turn downgrade to false")
	}
}

func TestReportedModelPersistsToJobRecordAndWireResults(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{
			{Type: engine.EventModelReported, ModelReported: "gpt-5.4"},
			{Type: engine.EventModelReported},
			{Type: engine.EventAgentText, Text: "hello"},
			{Type: engine.EventResultMessage, Text: "hello"},
		}
	}
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var session protocol.SessionStartResult
	decodeResult(t, rpc(t, conn, r, "1", protocol.MethodSessionStart, protocol.SessionStartParams{Backend: "fake", CWD: h.cwd}), &session)
	var turn protocol.TurnStartResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodTurnStart, protocol.TurnStartParams{SessionID: session.SessionID, Prompt: "hello"}), &turn)

	event := readNotification(t, r)
	if event.Method != protocol.NotificationTurnEvent {
		t.Fatalf("first notification = %s, want turn.event", event.Method)
	}
	var eventParams protocol.TurnEventParams
	mustUnmarshal(t, event.Params, &eventParams)
	if eventParams.Event.Type != engine.EventAgentText {
		t.Fatalf("model event leaked as turn event: %+v", eventParams.Event)
	}
	resultNotice := readNotification(t, r)
	var turnResult protocol.TurnResultParams
	mustUnmarshal(t, resultNotice.Params, &turnResult)
	if turnResult.ModelReported != "gpt-5.4" || turnResult.Result == nil || turnResult.Result.ModelReported != "gpt-5.4" {
		t.Fatalf("turn result model = %+v", turnResult)
	}

	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: turn.JobID}), &status)
	if len(status.Jobs) != 1 || status.Jobs[0].ModelReported != "gpt-5.4" {
		t.Fatalf("job status = %+v", status)
	}
	var result protocol.JobResult
	decodeResult(t, rpc(t, conn, r, "4", protocol.MethodJobResult, protocol.JobResultParams{JobID: turn.JobID}), &result)
	if result.ModelReported != "gpt-5.4" || result.Result == nil || result.Result.ModelReported != "gpt-5.4" {
		t.Fatalf("job result = %+v", result)
	}
	record := loadJobRecord(t, h.root, h.cwd, turn.JobID, engine.NativeProcessTable{})
	if record.ModelReported != "gpt-5.4" || record.Result == nil || record.Result.ModelReported != "gpt-5.4" {
		t.Fatalf("job record = %+v", record)
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
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "fail"},
	}), &job)
	waitJobState(t, conn, r, job.JobID, engine.StateFailed)
	var result protocol.JobResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobResult, protocol.JobResultParams{JobID: job.JobID}), &result)
	if result.State != engine.StateFailed || result.Result != nil {
		t.Fatalf("terminal error result = %+v", result)
	}
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
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "dedupe"},
	}), &job)
	waitJobState(t, conn, r, job.JobID, engine.StateCompleted)
	var result protocol.JobResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobResult, protocol.JobResultParams{JobID: job.JobID}), &result)
	if result.Result == nil || result.Result.Text != "hello" {
		t.Fatalf("result = %+v, want single authoritative hello", result.Result)
	}
}

func TestPrepareWireEventStripsRawTextMetadata(t *testing.T) {
	t.Parallel()
	raw := strings.Repeat("x", engine.DefaultEventTextCap) + "SECRET_RAW_TAIL"
	event := engine.Event{
		Type:    engine.EventAgentText,
		Text:    raw,
		RawText: raw,
		Metadata: map[string]any{
			"agentbusRawText": raw,
			"text":            raw,
		},
	}
	if authoritativeText(event) != raw {
		t.Fatal("authoritative text did not use internal raw text")
	}
	wire := prepareWireEvent(event)
	if wire.RawText != "" || !wire.Truncated || strings.Contains(wire.Text, "SECRET_RAW_TAIL") {
		t.Fatalf("wire event did not cap raw fields: %+v", wire)
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "agentbusRawText") || strings.Contains(string(encoded), "SECRET_RAW_TAIL") {
		t.Fatalf("wire event leaked raw text: %s", encoded)
	}
}

func TestSessionBusy(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	var session protocol.SessionStartResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodSessionStart, protocol.SessionStartParams{Backend: "fake", CWD: h.cwd}), &session)
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodTurnStart, protocol.TurnStartParams{SessionID: session.SessionID, Prompt: "first"}), &protocol.TurnStartResult{})
	resp := rpc(t, conn, r, "4", protocol.MethodTurnStart, protocol.TurnStartParams{SessionID: session.SessionID, Prompt: "second"})
	assertRPCCode(t, resp, protocol.ErrorSessionBusy)
	close(release)
	_ = readNotification(t, r)
	_ = readNotification(t, r)
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
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "one"}}), &one)
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "two"}}), &two)
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

func TestBackendProcessUpdatesWorkerIdentity(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	backend.processRef = engine.ProcessRef{PID: 4242, PGID: 4242, StartTime: "backend-start"}
	backend.backendChildPID = 4243
	// The daemon's own entry needs a start-time token: supervisor identity
	// is fail-closed (an empty token reads as gone/reused and orphans the
	// job before the assertions run).
	processes := mapProcessTable{entries: map[int]engine.ProcessInfo{
		os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"},
		4242:        {PID: 4242, StartTime: "backend-start"},
		4243:        {PID: 4243, StartTime: "child-start"},
	}}
	h := startTestServer(t, backend, Config{IdleTimeout: -1, ProcessTable: processes})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"}}), &job)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for job to start")
	}
	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: job.JobID}), &status)
	if len(status.Jobs) != 1 ||
		status.Jobs[0].WorkerPID != 4242 ||
		status.Jobs[0].WorkerStartTime != "backend-start" ||
		status.Jobs[0].BackendChildPID != 4243 ||
		status.Jobs[0].BackendChildStartTime != "child-start" {
		t.Fatalf("status worker identity = %+v", status)
	}
	record := loadJobRecord(t, h.root, h.cwd, job.JobID, processes)
	if record.Worker.PID != 4242 || record.Worker.PGID != 4242 || record.Worker.StartTime != "backend-start" {
		t.Fatalf("record worker = %+v", record.Worker)
	}
	if record.BackendChildPID != 4243 || record.BackendChildStartTime != "child-start" {
		t.Fatalf("record backend child identity = pid=%d startTime=%q", record.BackendChildPID, record.BackendChildStartTime)
	}
	close(release)
	waitJobState(t, conn, r, job.JobID, engine.StateCompleted)
}

func TestJobCancelUsesStoreGraceAndDoesNotInterruptSession(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	backend.processRef = engine.ProcessRef{PID: 5252, PGID: 5250, StartTime: "worker-start"}
	backend.backendChildPID = 5253
	// See TestJobStatusReportsWorkerIdentity: empty daemon start-time would
	// orphan the running job under fail-closed supervisor identity.
	processes := mapProcessTable{entries: map[int]engine.ProcessInfo{
		os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"},
		5252:        {PID: 5252, StartTime: "worker-start"},
		5253:        {PID: 5253, StartTime: "child-start"},
	}}
	groups := &recordingProcessGroups{}
	waiter := newRecordingWaiter()
	h := startTestServer(t, backend, Config{IdleTimeout: -1, ProcessTable: processes, ProcessGroups: groups, CancelWaiter: waiter})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"}}), &job)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for job to start")
	}
	defer waiter.Release()
	cancelResp := make(chan protocol.Response, 1)
	go func() {
		cancelResp <- rpc(t, conn, r, "3", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: job.JobID})
	}()
	select {
	case got := <-waiter.durations:
		if got != engine.DefaultCancelGrace {
			t.Fatalf("cancel grace = %s, want %s", got, engine.DefaultCancelGrace)
		}
	case <-time.After(time.Second):
		t.Fatal("store cancellation did not enter the protocol grace wait")
	}
	signals := groups.snapshot()
	if len(signals) != 1 || signals[0].Signal != syscall.SIGTERM || signals[0].PGID != 5250 {
		t.Fatalf("signals before grace release = %+v", signals)
	}
	waiter.Release()
	var canceled protocol.JobCancelResult
	select {
	case resp := <-cancelResp:
		decodeResult(t, resp, &canceled)
	case <-time.After(time.Second):
		t.Fatal("cancel RPC did not return after grace release")
	}
	if canceled.State != engine.StateCanceled {
		t.Fatalf("cancel result = %+v", canceled)
	}
	signals = groups.snapshot()
	if len(signals) != 2 || signals[1].Signal != syscall.SIGKILL || signals[1].PGID != 5250 {
		t.Fatalf("signals after grace release = %+v", signals)
	}
	if got := backend.interrupts.Load(); got != 0 {
		t.Fatalf("session interrupts = %d, want 0", got)
	}
	close(release)
	waitJobState(t, conn, r, job.JobID, engine.StateCanceled)
}

func TestBackgroundJobCancelDoesNotInterruptSessionAttemptInterleavings(t *testing.T) {
	t.Parallel()
	t.Run("attempt context cancellation wins", func(t *testing.T) {
		t.Parallel()
		server, run, sess, ctx, cancel := newControlledBackgroundRun(t)
		defer cancel()
		done := make(chan attemptResult, 1)
		go func() {
			text, state, err := server.runAttempt(ctx, run, "hold", false, model.LaunchOrdinalOne)
			done <- attemptResult{text: text, state: state, err: err}
		}()
		waitControlledSessionStarted(t, sess)

		run.active.requestTerminal(engine.StateCanceled)
		cancel()
		result := waitAttemptResult(t, done)
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("runAttempt err = %v, want context.Canceled", result.err)
		}
		if got := sess.interrupts.Load(); got != 0 {
			t.Fatalf("session interrupts = %d, want 0", got)
		}
	})
	t.Run("backend event stream closes before context cancellation", func(t *testing.T) {
		t.Parallel()
		server, run, sess, ctx, cancel := newControlledBackgroundRun(t)
		defer cancel()
		done := make(chan attemptResult, 1)
		go func() {
			text, state, err := server.runAttempt(ctx, run, "hold", false, model.LaunchOrdinalOne)
			done <- attemptResult{text: text, state: state, err: err}
		}()
		waitControlledSessionStarted(t, sess)

		run.active.requestTerminal(engine.StateCanceled)
		close(sess.events)
		result := waitAttemptResult(t, done)
		if result.err != nil || result.state != engine.StateCompleted {
			t.Fatalf("runAttempt result = %+v, want completed without error", result)
		}
		cancel()
		if got := sess.interrupts.Load(); got != 0 {
			t.Fatalf("session interrupts = %d, want 0", got)
		}
	})
}

func TestDeferredLaunchRunsOnlyAfterSuccessfulAck(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)

	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"},
	}, nil)
	if got := conn.writesString(); !strings.Contains(got, `"result"`) {
		t.Fatalf("response was not written before launch: %s", got)
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("job did not launch after successful ack")
	}
	close(release)
	waitKnownRecordState(t, server, engine.StateCompleted)
}

func TestDeferredLaunchAbortsWhenAckWriteFails(t *testing.T) {
	t.Parallel()
	errAck := errors.New("ack write failed")
	tests := []struct {
		name      string
		method    string
		params    func(cwd string) any
		sessionID string
		want      engine.JobState
	}{
		{
			name:   "job submit",
			method: protocol.MethodJobSubmit,
			params: func(cwd string) any {
				return protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "hold"}}
			},
			want: engine.StateCanceled,
		},
		{
			name:      "turn start",
			method:    protocol.MethodTurnStart,
			sessionID: "ses_ack_failure",
			params: func(string) any {
				return protocol.TurnStartParams{SessionID: "ses_ack_failure", Prompt: "hold"}
			},
			want: engine.StateInterrupted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := newFakeBackend("fake")
			backend.started = make(chan struct{}, 1)
			server, _, cwd := newUnstartedTestServer(t, backend)
			if tt.sessionID != "" {
				addScriptedSession(t, server, backend, cwd, tt.sessionID)
			}

			serveScriptedRequest(t, server, tt.method, tt.params(cwd), errAck)
			select {
			case <-backend.started:
				t.Fatal("backend launched after failed ack")
			default:
			}
			select {
			case turn := <-backend.turns:
				t.Fatalf("backend received turn after failed ack: %+v", turn)
			default:
			}
			record := singleKnownRecord(t, server)
			if record.State != tt.want {
				t.Fatalf("record state = %s, want %s", record.State, tt.want)
			}
			if server.activeWork() {
				t.Fatal("failed ack left active work registered")
			}
			if tt.sessionID != "" {
				server.mu.Lock()
				active := server.sessions[tt.sessionID].activeTurnID
				server.mu.Unlock()
				if active != "" {
					t.Fatalf("failed ack left active turn %q", active)
				}
			}
		})
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
	contract := &engine.ContractSpec{Shape: &engine.ShapeSpec{FirstLineEnum: []string{"PASS"}}}
	if err := server.createQueuedRecord(store, jobID, "ses_race", "fake", nil, &engine.TurnPolicy{Contract: contract}, contract, false); err != nil {
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
	if err := server.createQueuedRecord(store, jobID, "ses_salvage", "fake", nil, nil, nil, false); err != nil {
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
	statusRaw, err := json.Marshal(protocol.JobStatusParams{JobID: jobID})
	if err != nil {
		t.Fatal(err)
	}
	statusOutcome := server.handleJobStatus(statusRaw)
	status, ok := statusOutcome.result.(protocol.JobStatusResult)
	if statusOutcome.err != nil || !ok || len(status.Jobs) != 1 || !status.Jobs[0].LateFinalization {
		t.Fatalf("job.status outcome = %+v", statusOutcome)
	}
	resultRaw, err := json.Marshal(protocol.JobResultParams{JobID: jobID})
	if err != nil {
		t.Fatal(err)
	}
	resultOutcome := server.handleJobResult(resultRaw)
	result, ok := resultOutcome.result.(protocol.JobResult)
	if resultOutcome.err != nil || !ok || !result.LateFinalization {
		t.Fatalf("job.result outcome = %+v", resultOutcome)
	}
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
	if err := server.createQueuedRecord(store, jobID, "ses_reaped", "fake", nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, state := range []engine.JobState{engine.StateStarting, engine.StateRunning, engine.StateOrphaned, engine.StateReaped} {
		if err := server.transitionRecord(store, jobID, state); err != nil {
			t.Fatal(err)
		}
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
	if err := server.createQueuedRecord(store, jobID, "ses_lock", "fake", nil, nil, nil, false); err != nil {
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

func TestTurnInterruptRejectsBackgroundJob(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"}}), &job)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for job to start")
	}
	resp := rpc(t, conn, r, "3", protocol.MethodTurnInterrupt, protocol.TurnInterruptParams{TurnID: job.JobID})
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, conn, r, "4", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: job.JobID}), &status)
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateRunning {
		t.Fatalf("background job state changed after turn.interrupt: %+v", status)
	}
	close(release)
	waitJobState(t, conn, r, job.JobID, engine.StateCompleted)
}

func TestJobCancelRejectsForegroundTurn(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	var session protocol.SessionStartResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodSessionStart, protocol.SessionStartParams{Backend: "fake", CWD: h.cwd}), &session)
	var turn protocol.TurnStartResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodTurnStart, protocol.TurnStartParams{SessionID: session.SessionID, Prompt: "hold"}), &turn)
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for turn to start")
	}
	resp := rpc(t, conn, r, "4", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: turn.JobID})
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	var sessions protocol.SessionListResult
	decodeResult(t, rpc(t, conn, r, "5", protocol.MethodSessionList, protocol.SessionListParams{}), &sessions)
	if len(sessions.Sessions) != 1 || sessions.Sessions[0].ActiveTurnID == nil || *sessions.Sessions[0].ActiveTurnID != turn.TurnID {
		t.Fatalf("foreground turn was not left active: %+v", sessions)
	}
	close(release)
	_ = readNotification(t, r)
	_ = readNotification(t, r)
}

func TestSideEffectingNotificationsAreRejectedBeforeMutation(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	h := startTestServer(t, backend, Config{IdleTimeout: -1})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	var session protocol.SessionStartResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodSessionStart, protocol.SessionStartParams{Backend: "fake", CWD: h.cwd}), &session)

	notifyRPC(t, conn, protocol.MethodTurnStart, protocol.TurnStartParams{SessionID: session.SessionID, Prompt: "notification"})
	assertRPCCode(t, readResponse(t, r), protocol.ErrorInvalidTaskSpec)
	notifyRPC(t, conn, protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "notification"}})
	assertRPCCode(t, readResponse(t, r), protocol.ErrorInvalidTaskSpec)

	var sessions protocol.SessionListResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodSessionList, protocol.SessionListParams{}), &sessions)
	if len(sessions.Sessions) != 1 || sessions.Sessions[0].ActiveTurnID != nil {
		t.Fatalf("notification mutated session state: %+v", sessions)
	}
	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, conn, r, "4", protocol.MethodJobStatus, protocol.JobStatusParams{All: true}), &status)
	if len(status.Jobs) != 0 {
		t.Fatalf("notification created jobs: %+v", status)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts = %d, want only session.start", got)
	}
}

func TestNamedPolicyResolvedAtSubmitTime(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	backend := newFakeBackend("fake")
	backend.block = release
	backend.started = make(chan struct{}, 1)
	registry := engine.NewPolicyRegistry()
	spec := engine.ContractSpec{Shape: &engine.ShapeSpec{FirstLineEnum: []string{"PASS"}, RequiredSections: []string{"Findings"}}}
	if _, err := registry.Register("delegate/report@1", spec); err != nil {
		t.Fatal(err)
	}
	h := startTestServer(t, backend, Config{IdleTimeout: -1, Registry: registry})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)

	var job protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{
		Backend: "fake",
		CWD:     h.cwd,
		Write:   false,
		Prompt:  "hold",
		Policy:  &engine.TurnPolicy{Contract: &engine.ContractSpec{Named: "delegate/report@1"}},
	}}), &job)
	record := loadJobRecord(t, h.root, h.cwd, job.JobID, nil)
	if record.ResolvedContract == nil || record.ResolvedContract.Shape == nil || record.ResolvedContract.Named != "" {
		t.Fatalf("resolved contract was not persisted at submit time: %+v", record.ResolvedContract)
	}
	if record.Policy == nil || record.Policy.Contract == nil || record.Policy.Contract.Named != "delegate/report@1" {
		t.Fatalf("submitted policy name was not retained: %+v", record.Policy)
	}
	close(release)
	waitJobState(t, conn, r, job.JobID, engine.StateCompleted)
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
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: h.cwd, Write: false, Prompt: "hold"}}), &job)
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
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{TaskSpec: protocol.TaskSpec{
		Backend: "fake",
		CWD:     h.cwd,
		Write:   false,
		Prompt:  "hold",
	}}), &job)
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

func TestStartReaperRecoversCrashedJob(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	var clockNanos atomic.Int64
	clockNanos.Store(base.UnixNano())
	clock := engine.ClockFunc(func() time.Time { return time.Unix(0, clockNanos.Load()).UTC() })
	root := shortTempDir(t)
	jobCWD := shortTempDir(t)
	daemonCWD := shortTempDir(t)
	store, err := engine.NewStore(engine.StoreConfig{
		Root:      root,
		CWD:       jobCWD,
		Clock:     clock,
		Processes: fakeProcessTable{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&engine.JobRecord{
		JobID:     "job_crashed",
		SessionID: "ses_crashed",
		Backend:   "fake",
		State:     engine.StateRunning,
		Worker:    engine.ProcessRef{PID: 999999, StartTime: "old"},
		Lease:     engine.Lease{ExpiresAt: base.Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend("fake")
	h := startTestServerWithRoot(t, root, daemonCWD, backend, Config{IdleTimeout: -1, ProcessTable: fakeProcessTable{}, Clock: clock})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	status := rpc(t, conn, r, "2", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: "job_crashed"})
	var out protocol.JobStatusResult
	decodeResult(t, status, &out)
	if len(out.Jobs) != 1 || out.Jobs[0].State != engine.StateOrphaned {
		t.Fatalf("status = %+v", out)
	}
	clockNanos.Store(base.Add(engine.DefaultLeaseDuration).UnixNano())
	status = rpc(t, conn, r, "3", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: "job_crashed"})
	decodeResult(t, status, &out)
	if len(out.Jobs) != 1 || out.Jobs[0].State != engine.StateReaped {
		t.Fatalf("status after orphan grace = %+v", out)
	}
}

func TestBackgroundReapTickReapsWithoutClientRead(t *testing.T) {
	base := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	clock := engine.ClockFunc(func() time.Time { return base })
	root := shortTempDir(t)
	jobCWD := shortTempDir(t)
	daemonCWD := shortTempDir(t)
	ticks := make(chan time.Time)
	ticked := make(chan error, 1)
	h := startTestServerWithRootAndHooks(t, root, daemonCWD, newFakeBackend("fake"), Config{
		IdleTimeout:      -1,
		Clock:            clock,
		ProcessTable:     fakeProcessTable{},
		ReapTickInterval: time.Hour,
	}, func(server *Server) {
		server.reapTickFactory = func(time.Duration) tickerSource {
			return tickerSource{c: ticks, stop: func() {}}
		}
		server.afterReapTickHook = func(err error) { ticked <- err }
	})
	_ = h

	store, err := engine.NewStore(engine.StoreConfig{
		Root:      root,
		CWD:       jobCWD,
		Clock:     clock,
		Processes: fakeProcessTable{},
	})
	if err != nil {
		t.Fatal(err)
	}
	record := &engine.JobRecord{
		JobID:     "job_background_reap",
		State:     engine.StateQueued,
		UpdatedAt: base.Add(-time.Hour),
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}

	ticks <- base
	if err := <-ticked; err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(record.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted engine.JobRecord
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.State != engine.StateOrphaned {
		t.Fatalf("background reap state = %s, want orphaned", persisted.State)
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
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: jobCWD, Write: false, Prompt: "complete"},
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

func TestBackendSessionIDPersistsAndSessionResumeUsesRecordAfterRestart(t *testing.T) {
	t.Parallel()
	root := shortTempDir(t)
	jobCWD := shortTempDir(t)
	canonicalJobCWD, err := engine.CanonicalWorkspace(jobCWD)
	if err != nil {
		t.Fatal(err)
	}
	daemonCWD := shortTempDir(t)

	first := startTestServerWithRoot(t, root, daemonCWD, newFakeBackend("fake"), Config{IdleTimeout: -1})
	conn := dialRaw(t, first.socketPath)
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, first.token)
	var submitted protocol.JobSubmitResult
	decodeResult(t, rpc(t, conn, r, "2", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		TaskSpec: protocol.TaskSpec{Backend: "fake", CWD: jobCWD, Write: false, Prompt: "complete", Tags: map[string]string{"client": "resume-test"}},
	}), &submitted)
	waitJobState(t, conn, r, submitted.JobID, engine.StateCompleted)
	record := loadJobRecord(t, root, jobCWD, submitted.JobID, fakeProcessTable{})
	if record.BackendSessionID == "" {
		t.Fatalf("backend session id was not persisted: %+v", record)
	}
	_ = conn.Close()
	stopTestServer(t, first)

	secondBackend := newFakeBackend("fake")
	secondBackend.resumes = make(chan resumedSession, 1)
	second := startTestServerWithRoot(t, root, daemonCWD, secondBackend, Config{IdleTimeout: -1})
	conn = dialRaw(t, second.socketPath)
	defer conn.Close()
	r = bufio.NewReader(conn)
	helloRaw(t, conn, r, second.token)

	var resumed protocol.SessionStartResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodSessionResume, protocol.SessionResumeParams{SessionID: record.SessionID}), &resumed)
	if resumed.SessionID != record.SessionID || resumed.Backend != "fake" {
		t.Fatalf("resume result = %+v", resumed)
	}
	select {
	case got := <-secondBackend.resumes:
		if got.ID != record.BackendSessionID || got.Opts.CWD != canonicalJobCWD || got.Opts.Write {
			t.Fatalf("backend resume = %+v, want id %q cwd %q read-only", got, record.BackendSessionID, canonicalJobCWD)
		}
	case <-time.After(time.Second):
		t.Fatal("backend.Resume was not called from persisted record")
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	h := testServer{root: root, cwd: cwd, socketPath: filepath.Join(root, protocol.SocketName), token: "test-token", done: done, cancel: cancel}
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

func startTestServerWithBlockingListener(t *testing.T, server *Server) (context.CancelFunc, <-chan error, *blockingTestListener) {
	t.Helper()
	listener := newBlockingTestListener()
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
	select {
	case <-listening:
	case err := <-done:
		t.Fatalf("server exited before listener was ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("blocking listener did not become ready")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("server did not stop")
		}
	})
	return cancel, done, listener
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
			if err != nil && strings.Contains(err.Error(), "bind: operation not permitted") {
				t.Skipf("Unix socket bind denied by sandbox: %v", err)
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
		select {
		case err := <-done:
			t.Fatalf("server exited before socket removal was observed: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
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
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	enableTestAdmission(t, server, launcher)
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

func prepareLegacyFencedCommandForTest(t *testing.T, server *Server, requestID string) *legacyFencedCommand {
	t.Helper()
	preparation, err := server.admissionSubmission.PrepareLegacyFenced(context.Background(), authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: model.WorkspaceKey("workspace/" + requestID), RequestID: model.RequestID(requestID)},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte(requestID)),
		Mode:         model.ModeLegacyFenced,
		SessionID:    "session-" + requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd, _, err := server.admissionSubmission.prepareLegacyFencedCommand(context.Background(), preparation.Admission, command.ExecSpec{
		Argv: []string{"fake-parked"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

func newLegacyFencedCoordinatorWithRecordingAuthorityForTest(t *testing.T, launcher *admissionFakeLaunchCustodian) (repository.Repository, *recordingLegacyLaunchAuthority, *servedSubmissionCoordinator) {
	t.Helper()
	repo := memory.NewRepository()
	anchorStore := authority.NewAnchorStore()
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(launcher.verifier))
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-legacy-fenced-release-unknown", "owner-legacy-fenced-release-unknown")
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
	recorder := &recordingLegacyLaunchAuthority{ready: ready}
	controller, err := launch.New(recorder, launcher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &servedSubmissionCoordinator{
		ready:  ready,
		owner:  boot.OwnerID,
		launch: controller,
		latch:  NewSafetyLatch(),
	}
	return repo, recorder, coordinator
}

func prepareLegacyFencedCommandWithCoordinatorForTest(t *testing.T, coordinator *servedSubmissionCoordinator, requestID string) *legacyFencedCommand {
	t.Helper()
	preparation, err := coordinator.PrepareLegacyFenced(context.Background(), authority.AcceptRequest{
		RequestKey:   model.RequestKey{WorkspaceKey: model.WorkspaceKey("workspace/" + requestID), RequestID: model.RequestID(requestID)},
		TaskIdentity: model.NewSHA256TaskIdentity([]byte(requestID)),
		Mode:         model.ModeLegacyFenced,
		SessionID:    "session-" + requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd, _, err := coordinator.prepareLegacyFencedCommand(context.Background(), preparation.Admission, command.ExecSpec{
		Argv: []string{"fake-parked"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

type recordingLegacyLaunchAuthority struct {
	ready *authority.Ready

	mu                             sync.Mutex
	recordQuiescenceCtxErrs        []error
	recordQuiescenceCtxDeadlines   []bool
	recordReleaseOutcomes          []model.LaunchReleaseOutcome
	recordReleaseOutcomeErrs       []error
	recordReleaseOutcomeDurability launch.DurabilityOutcome
	recordReleaseOutcomeErr        error
}

func (a *recordingLegacyLaunchAuthority) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef) (launch.DurabilityOutcome, error) {
	applied, err := a.ready.BindGroup(ctx, jobID, ref, ordinal, group)
	return applied.Durability, err
}

func (a *recordingLegacyLaunchAuthority) AllocateGrant(ctx context.Context, ref model.AttemptRef, ordinal model.LaunchOrdinal) (model.LaunchGrant, launch.DurabilityOutcome, error) {
	return a.ready.AllocateGrant(ctx, ref, ordinal)
}

func (a *recordingLegacyLaunchAuthority) RecordReleaseOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, outcome model.LaunchReleaseOutcome) (launch.DurabilityOutcome, error) {
	a.mu.Lock()
	injectedDurability := a.recordReleaseOutcomeDurability
	injectedErr := a.recordReleaseOutcomeErr
	a.mu.Unlock()
	if injectedDurability != 0 || injectedErr != nil {
		if injectedDurability == 0 {
			injectedDurability = launch.DefinitelyNotCommitted
		}
		a.mu.Lock()
		a.recordReleaseOutcomes = append(a.recordReleaseOutcomes, outcome)
		a.recordReleaseOutcomeErrs = append(a.recordReleaseOutcomeErrs, injectedErr)
		a.mu.Unlock()
		return injectedDurability, injectedErr
	}
	applied, err := a.ready.RecordReleaseOutcome(ctx, jobID, ref, ordinal, outcome)
	a.mu.Lock()
	a.recordReleaseOutcomes = append(a.recordReleaseOutcomes, outcome)
	a.recordReleaseOutcomeErrs = append(a.recordReleaseOutcomeErrs, err)
	a.mu.Unlock()
	return applied.Durability, err
}

func (a *recordingLegacyLaunchAuthority) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (launch.DurabilityOutcome, error) {
	applied, err := a.ready.RecordRelease(ctx, jobID, ref, ordinal, child, evidence)
	return applied.Durability, err
}

func (a *recordingLegacyLaunchAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (launch.DurabilityOutcome, error) {
	_, hasDeadline := ctx.Deadline()
	a.mu.Lock()
	a.recordQuiescenceCtxErrs = append(a.recordQuiescenceCtxErrs, ctx.Err())
	a.recordQuiescenceCtxDeadlines = append(a.recordQuiescenceCtxDeadlines, hasDeadline)
	a.mu.Unlock()
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return applied.Durability, err
}

func (a *recordingLegacyLaunchAuthority) FailStop(ctx context.Context, reason error) error {
	if reason == nil {
		reason = errors.New("launch fail-stop")
	}
	return a.ready.FailStop(ctx, reason.Error())
}

func (a *recordingLegacyLaunchAuthority) recordQuiescenceContextObservations() ([]error, []bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]error(nil), a.recordQuiescenceCtxErrs...), append([]bool(nil), a.recordQuiescenceCtxDeadlines...)
}

func (a *recordingLegacyLaunchAuthority) recordReleaseOutcomeObservations() ([]model.LaunchReleaseOutcome, []error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]model.LaunchReleaseOutcome(nil), a.recordReleaseOutcomes...), append([]error(nil), a.recordReleaseOutcomeErrs...)
}

type recordingAdmissionAuthority struct {
	*servedAdmissionAuthority

	mu              sync.Mutex
	finalizeRecords []model.TerminalIntent
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

func assertLegacyFencedAbortUsedDetachedContext(t *testing.T, launcher *admissionFakeLaunchCustodian) {
	t.Helper()
	if got := launcher.abortCount(); got != 1 {
		t.Fatalf("legacy fenced aborts = %d, want one retired parked worker", got)
	}
	errs, deadlines := launcher.abortContextObservations()
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("abort context errors = %+v, want one live context", errs)
	}
	if len(deadlines) != 1 || !deadlines[0] {
		t.Fatalf("abort context deadlines = %+v, want bounded cleanup context", deadlines)
	}
}

func assertLegacyFencedRejectedWithoutGrant(t *testing.T, safety model.SafetyRecord) {
	t.Helper()
	if safety.Terminal == nil || safety.Terminal.Outcome != model.OutcomeCanceled || safety.Terminal.Cause != model.CauseResponseUndeliverable {
		t.Fatalf("legacy fenced terminal = %+v, want response-undeliverable cancel", safety.Terminal)
	}
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Quiescence == nil || first.Grant != nil || first.Released != nil {
		t.Fatalf("legacy fenced failed-response launch proof = %+v, want retired without grant/release", first)
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
	containments []admissionContainObservation
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
	cleanupErr                error
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
	return r.finish(model.QuiescenceNaturalExit)
}

func (r *admissionFakeRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	r.markContained()
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
		Version:           1,
		CustodyID:         model.CustodyID("custody-" + name),
		Launch:            key,
		HostBootID:        "host-" + name,
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader:            model.ProcessIdentity{PID: pgid, HighResStartToken: "leader-" + name},
		Monitor:           model.ProcessIdentity{PID: pgid + 100, HighResStartToken: "monitor-" + name},
		RetainedID:        "retained-" + name,
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

func clearAdmissionJobMarkersForTest(t *testing.T, server *Server) {
	t.Helper()
	server.mu.Lock()
	server.admissionJobs = make(map[string]struct{})
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

func addScriptedSession(t *testing.T, server *Server, backend *fakeBackend, cwd, sessionID string) {
	t.Helper()
	session, err := backend.Start(context.Background(), engine.SessionOpts{CWD: cwd})
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.sessions[sessionID] = &sessionState{id: sessionID, backend: backend.Name(), cwd: cwd, session: session}
	server.mu.Unlock()
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
	return nil
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
	if err := server.createQueuedRecord(store, jobID, "ses_controlled", "fake", nil, nil, nil, false); err != nil {
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
}

func newBlockingTestListener() *blockingTestListener {
	return &blockingTestListener{closed: make(chan struct{})}
}

func (l *blockingTestListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *blockingTestListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
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

type testNetAddr string

func (a testNetAddr) Network() string { return "test" }

func (a testNetAddr) String() string { return string(a) }

func singleKnownRecord(t *testing.T, server *Server) engine.JobRecord {
	t.Helper()
	records := server.listKnownRecords()
	if len(records) != 1 {
		t.Fatalf("known records = %+v, want exactly one", records)
	}
	return records[0]
}

func waitKnownRecordState(t *testing.T, server *Server, want engine.JobState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last engine.JobRecord
	for time.Now().Before(deadline) {
		record := singleKnownRecord(t, server)
		last = record
		if record.State == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("known record did not reach %s; last = %+v", want, last)
}

type rawNotification struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id,omitempty"`
}

func helloRaw(t *testing.T, conn net.Conn, r *bufio.Reader, token string) {
	t.Helper()
	resp := rpc(t, conn, r, "hello", protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token})
	if resp.Error != nil {
		t.Fatalf("hello error = %+v", resp.Error)
	}
}

func rpc(t *testing.T, conn net.Conn, r *bufio.Reader, id, method string, params any) protocol.Response {
	t.Helper()
	req := protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(strconvQuote(id)), Method: method, Params: mustMarshal(t, params)}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
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
		return resp
	}
}

func notifyRPC(t *testing.T, conn net.Conn, method string, params any) {
	t.Helper()
	req := protocol.Request{JSONRPC: "2.0", Method: method, Params: mustMarshal(t, params)}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
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

func readNotification(t *testing.T, r *bufio.Reader) rawNotification {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var notice rawNotification
	mustUnmarshal(t, line, &notice)
	if len(notice.ID) != 0 {
		t.Fatalf("notification included id: %s", notice.ID)
	}
	return notice
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

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
