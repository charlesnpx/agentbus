package served

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
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
}

type fakeTurn struct {
	Prompt string
	Write  bool
}

type resumedSession struct {
	ID   string
	Opts engine.SessionOpts
}

func newFakeBackend(name string) *fakeBackend {
	return &fakeBackend{
		name:     name,
		turns:    make(chan fakeTurn, 32),
		parkable: true,
		events: func(prompt string, write bool) []engine.Event {
			return []engine.Event{{Type: engine.EventAgentText, Text: "PASS\n\n## Findings\nNone.\n"}}
		},
	}
}

func (b *fakeBackend) Name() string { return b.name }

func (b *fakeBackend) AdmissionParkable() bool { return b.parkable }

func (b *fakeBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: b.name}, nil
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

func (s *fakeSession) Interrupt(context.Context) error {
	s.backend.interrupts.Add(1)
	return nil
}

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

func TestServeRefusesAdmissionBeforeListenWhenCustodianUnavailable(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.jobsRequestIDEnabled = true
	bootstrapCalled := false
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapCalled = true
		return nil, nil, nil, errors.New("bootstrap should not run")
	}
	listenCalled := false
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		return nil, socketFileIdentity{}, errors.New("listener should not be called")
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("Serve error = %v, want supervisor_unavailable", err)
	}
	if listenCalled {
		t.Fatal("listener was called after unavailable custodian")
	}
	if bootstrapCalled {
		t.Fatal("admission bootstrap ran before custodian support was verified")
	}
	if server.jobsRequestIDEnabled {
		t.Fatal("jobs.requestId remained enabled after unavailable custodian")
	}
}

func TestBootstrapAdmissionRefusesUnavailableCustodianBeforeRepositoryOpen(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.jobsRequestIDEnabled = true
	bootstrapCalled := false
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapCalled = true
		return nil, nil, nil, errors.New("bootstrap should not run")
	}

	err := server.bootstrapAdmission(context.Background())
	if !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("bootstrapAdmission error = %v, want supervisor_unavailable", err)
	}
	if bootstrapCalled {
		t.Fatal("admission bootstrap factory ran before custodian support was verified")
	}
	if server.jobsRequestIDEnabled {
		t.Fatal("jobs.requestId remained enabled after unavailable custodian")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, err)
		}
	}
}

func TestCapabilityOffStartupSkipsAdmissionBootstrap(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	bootstrapCalled := false
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapCalled = true
		return nil, nil, nil, errors.New("bootstrap should not run")
	}
	listenErr := errors.New("listener reached with capability off")
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		if server.admissionBootstrapper != nil || server.admissionReady != nil || server.admissionCoordinator != nil {
			return nil, socketFileIdentity{}, errors.New("admission state initialized with capability off")
		}
		return nil, socketFileIdentity{}, listenErr
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want %v", err, listenErr)
	}
	if bootstrapCalled {
		t.Fatal("admission bootstrap ran while jobs.requestId was disabled")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, err)
		}
	}
}

func TestCapabilityOffStartupRunsLegacyReapBeforeListen(t *testing.T) {
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
	bootstrapCalled := false
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapCalled = true
		return nil, nil, nil, errors.New("bootstrap should not run")
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
	if bootstrapCalled {
		t.Fatal("admission bootstrap ran while jobs.requestId was disabled")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, err)
		}
	}
}

func TestJobSubmitRequestIDCapabilityDisabledDoesNotStartBackend(t *testing.T) {
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
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
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
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0 before incompatible pre-accept reject", got)
	}
	assertNoAcceptedJobsInAdmission(t, server)
}

func TestLegacyFencedDeliveredAcknowledgesGrantsAndRunsOnce(t *testing.T) {
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
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarted(t, backend)

	if got := launcher.releaseCount(); got != 1 {
		t.Fatalf("legacy fenced releases = %d, want exactly one", got)
	}
	if got := launcher.abortCount(); got != 0 {
		t.Fatalf("legacy fenced aborts = %d, want 0 after delivered response", got)
	}
	record := waitAdmissionSafetyTerminal(t, server, submitted.JobID)
	if record.Mode != model.ModeLegacyFenced || record.Acknowledgement == nil {
		t.Fatalf("legacy fenced record missing mode/ack: %+v", record)
	}
	first, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Grant == nil || first.Released == nil || first.Quiescence == nil {
		t.Fatalf("legacy fenced launch proof incomplete: %+v", first)
	}
}

func TestLegacyFencedDeliveryFailureRetiresWithoutGrantOrRun(t *testing.T) {
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

	if got := launcher.releaseCount(); got != 0 {
		t.Fatalf("legacy fenced releases = %d, want 0 after failed response", got)
	}
	if got := launcher.abortCount(); got != 1 {
		t.Fatalf("legacy fenced aborts = %d, want one retired parked worker", got)
	}
	safety := waitSingleAdmissionSafetyTerminal(t, server)
	if safety.Terminal == nil || safety.Terminal.Outcome != model.OutcomeCanceled || safety.Terminal.Cause != model.CauseResponseUndeliverable {
		t.Fatalf("legacy fenced terminal = %+v, want response-undeliverable cancel", safety.Terminal)
	}
	first, ok := safety.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || first.Group == nil || first.Quiescence == nil || first.Grant != nil || first.Released != nil {
		t.Fatalf("legacy fenced failed-response launch proof = %+v, want retired without grant/release", first)
	}
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

func TestLegacyUnfencedDeliveredRunsWithoutCustody(t *testing.T) {
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
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	waitBackendStarted(t, backend)
	waitAdmissionSafetyTerminal(t, server, submitted.JobID)

	if got := len(launcher.preparedOrdinals()); got != 0 {
		t.Fatalf("legacy unfenced prepared launches = %d, want 0", got)
	}
	safety := loadAdmissionSafetyRecord(t, server, submitted.JobID)
	if safety.Mode != model.ModeLegacyUnfenced {
		t.Fatalf("admission mode = %s, want LegacyUnfenced", safety.Mode)
	}
	if safety.Attempt.Launches.Count() != 0 {
		t.Fatalf("legacy unfenced launch proofs = %+v, want none", safety.Attempt.Launches)
	}
	if safety.Terminal == nil || safety.Terminal.Proof != model.ProofLegacyUnfencedOutcome {
		t.Fatalf("legacy unfenced terminal = %+v, want legacy-unfenced proof", safety.Terminal)
	}
}

func TestLegacyUnfencedDeliveryFailureRejectsBeforeBackendStart(t *testing.T) {
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
	safety := waitSingleAdmissionSafetyTerminal(t, server)
	if safety.Terminal == nil || safety.Terminal.Outcome != model.OutcomeCanceled || safety.Terminal.Proof != model.ProofLegacyUnfencedOutcome {
		t.Fatalf("legacy unfenced terminal = %+v, want canceled legacy-unfenced proof", safety.Terminal)
	}
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
	processes := mapProcessTable{entries: map[int]engine.ProcessInfo{
		os.Getpid(): {PID: os.Getpid()},
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
	processes := mapProcessTable{entries: map[int]engine.ProcessInfo{
		os.Getpid(): {PID: os.Getpid()},
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

func startTestServerWithRootAndHooks(t *testing.T, root, cwd string, backend engine.Backend, cfg Config, configure func(*Server)) testServer {
	t.Helper()
	cfg.StateRoot = root
	cfg.CWD = cwd
	cfg.Token = "test-token"
	cfg.Backends = []engine.Backend{backend}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
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

func stopTestServer(t *testing.T, h testServer) {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(time.Second):
		t.Fatalf("server did not stop")
	}
}

func waitForSocket(t *testing.T, socketPath string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
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
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid()}}},
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
	server.jobsRequestIDEnabled = true
	server.admissionSupervisor = &servedAdmissionSupervisor{
		runtime:          custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		launchCustodian:  launcher,
		supportOverride:  &support,
		verifierOverride: launcher.verifier,
	}
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func enableTestAdmissionWithAuthorityStore(t *testing.T, server *Server, launcher *admissionFakeLaunchCustodian, repo *memory.Repository, anchorStore *authority.AnchorStore) {
	t.Helper()
	server.admissionBootstrapperFactory = func(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchorStore(anchorStore), authority.WithQuiescenceVerifier(s.admissionSupervisor.quiescenceVerifier()))
		if err != nil {
			return nil, nil, nil, err
		}
		return bootstrapper, repo, io.NopCloser(bytes.NewReader(nil)), nil
	}
	enableTestAdmission(t, server, launcher)
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

	mu       sync.Mutex
	ordinals []model.LaunchOrdinal
	releases int
	aborts   int

	abortCtxErrs      []error
	abortCtxDeadlines []bool

	abortRespectContext     bool
	abortWaitForContextDone bool
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
	}
	c.mu.Lock()
	c.ordinals = append(c.ordinals, key.Ordinal)
	c.mu.Unlock()
	return &admissionFakePrepared{group: group, running: running, issuer: c.issuer, custodian: c}, nil
}

func (c *admissionFakeLaunchCustodian) ContainAndVerify(context.Context, model.GroupRef, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	return custodian.VerifiedQuiescence{}, errors.New("unexpected custodian-level contain")
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

func (c *admissionFakeLaunchCustodian) abortContextObservations() ([]error, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.abortCtxErrs...), append([]bool(nil), c.abortCtxDeadlines...)
}

type admissionFakePrepared struct {
	group     model.GroupRef
	running   *admissionFakeRunning
	issuer    custodian.AttestationIssuer
	custodian *admissionFakeLaunchCustodian
}

func (p *admissionFakePrepared) Ref() model.GroupRef {
	return p.group
}

func (p *admissionFakePrepared) Release(context.Context, custodian.GrantToken) (launch.RunningProcess, error) {
	if p.custodian != nil {
		p.custodian.mu.Lock()
		p.custodian.releases++
		p.custodian.mu.Unlock()
	}
	return p.running, nil
}

func (p *admissionFakePrepared) AbortAndVerify(ctx context.Context) (custodian.VerifiedQuiescence, error) {
	respectContext := false
	waitForContextDone := false
	if p.custodian != nil {
		p.custodian.mu.Lock()
		respectContext = p.custodian.abortRespectContext
		waitForContextDone = p.custodian.abortWaitForContextDone
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
			return custodian.VerifiedQuiescence{}, err
		}
	}
	return p.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: p.group, Method: model.QuiescenceAlreadyAbsent})
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

func (r *admissionFakeRunning) WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, error) {
	r.closeStreams()
	verified, err := r.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: r.group, Method: model.QuiescenceNaturalExit})
	return command.ExitObservation{Exited: true, Code: 0}, verified, err
}

func (r *admissionFakeRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	r.closeStreams()
	return r.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: r.group, Method: model.QuiescenceTermKill})
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
		case <-time.After(time.Second):
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
	deadline := time.Now().Add(time.Second)
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

func waitSingleAdmissionSafetyTerminal(t *testing.T, server *Server) model.SafetyRecord {
	t.Helper()
	deadline := time.Now().Add(time.Second)
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

func assertNoAcceptedJobsInAdmission(t *testing.T, server *Server) {
	t.Helper()
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

type attemptResult struct {
	text  string
	state engine.JobState
	err   error
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
	deadline := time.Now().Add(time.Second)
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
