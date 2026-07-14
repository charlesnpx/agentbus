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
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
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
		name:  name,
		turns: make(chan fakeTurn, 32),
		events: func(prompt string, write bool) []engine.Event {
			return []engine.Event{{Type: engine.EventAgentText, Text: "PASS\n\n## Findings\nNone.\n"}}
		},
	}
}

func (b *fakeBackend) Name() string { return b.name }

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
	for _, capability := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude", "models.discovery", "models.reported", "jobs.requestId"} {
		if _, ok := hello.Capabilities[capability]; !ok {
			t.Fatalf("missing capability %s in %+v", capability, hello.Capabilities)
		}
	}
	if hello.Capabilities["jobs.requestId"] {
		t.Fatalf("jobs.requestId capability is enabled in hello: %+v", hello.Capabilities)
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

func TestServeBootstrapsAdmissionBeforeListen(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.jobsRequestIDEnabled = true
	listenErr := errors.New("listener reached after admission ready")
	listenCalled := false
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		listenCalled = true
		if server.admissionBootstrapper == nil || server.admissionReady == nil || server.admissionCoordinator == nil {
			return nil, socketFileIdentity{}, errors.New("listener called before admission ready")
		}
		return nil, socketFileIdentity{}, listenErr
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, listenErr) {
		t.Fatalf("Serve error = %v, want %v", err, listenErr)
	}
	if !listenCalled {
		t.Fatal("listener was not called")
	}
}

func TestServeDoesNotListenWhenAdmissionBootstrapFails(t *testing.T) {
	t.Parallel()
	server, root, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	server.jobsRequestIDEnabled = true
	bootstrapErr := errors.New("admission bootstrap failed")
	server.admissionBootstrapperFactory = func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
		return nil, nil, nil, bootstrapErr
	}
	server.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener should not be called")
	}

	err := server.Serve(context.Background())
	if !errors.Is(err, bootstrapErr) {
		t.Fatalf("Serve error = %v, want %v", err, bootstrapErr)
	}
	if _, err := os.Stat(filepath.Join(root, protocol.SocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket stat error = %v, want not exist", err)
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

func TestIdentifiedJobSubmitAdmitsBeforeBackendStartAndReplaysBeforeBackendValidation(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.jobsRequestIDEnabled = true
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.admissionClose != nil {
		t.Cleanup(func() { _ = server.admissionClose.Close() })
	}

	key, err := model.NewRequestKey("workspace-enabled", "request-enabled")
	if err != nil {
		t.Fatal(err)
	}
	var startErr error
	var sawAdmission bool
	startObserved := make(chan struct{}, 1)
	backend.startHook = func(engine.SessionOpts) {
		replay, err := server.admissionReady.LookupReplay(context.Background(), key)
		if err != nil {
			startErr = err
			return
		}
		if replay.State != authority.ReplayLive {
			startErr = errors.New("request was not live in authority before backend start")
			return
		}
		if replay.Record.Attempt.Grants.Count() != 1 {
			startErr = errors.New("backend started before authority launch grant")
			return
		}
		sawAdmission = true
		startObserved <- struct{}{}
	}
	params := protocol.JobSubmitParams{
		WorkspaceKey: key.WorkspaceKey.String(),
		RequestID:    key.RequestID.String(),
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "same"},
	}
	raw := mustMarshal(t, params)

	first := server.handleJobSubmit(context.Background(), raw)
	if first.err != nil {
		t.Fatalf("first submit error = %+v", first.err)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts before ack/permit = %d, want 0", got)
	}
	if first.after == nil {
		t.Fatal("identified submit did not defer launch")
	}
	first.after()
	select {
	case <-startObserved:
	case <-time.After(time.Second):
		t.Fatal("backend did not start after deferred launch")
	}
	if startErr != nil {
		t.Fatal(startErr)
	}
	if !sawAdmission {
		t.Fatal("backend started before admission hook observed live request")
	}
	server.backends = map[string]engine.Backend{}

	replay := server.handleJobSubmit(context.Background(), raw)
	if replay.err != nil {
		t.Fatalf("replay submit error = %+v", replay.err)
	}
	result, ok := replay.result.(protocol.JobSubmitResult)
	if !ok || !result.Deduplicated {
		t.Fatalf("replay result = %+v, want deduplicated JobSubmitResult", replay.result)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts after replay = %d, want 1", got)
	}
}

func TestIdentifiedJobSubmitInvalidFirstAttemptDoesNotBindReplayKey(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	server.jobsRequestIDEnabled = true
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.admissionClose != nil {
		t.Cleanup(func() { _ = server.admissionClose.Close() })
	}
	key, err := model.NewRequestKey("workspace-unpoisoned", "request-unpoisoned")
	if err != nil {
		t.Fatal(err)
	}

	bad := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: key.WorkspaceKey.String(),
		RequestID:    key.RequestID.String(),
		TaskSpec:     protocol.TaskSpec{Backend: "missing", CWD: cwd, Write: false, Prompt: "same request"},
	}))
	if bad.err == nil || bad.err.Data.Code != protocol.ErrorBackendUnavailable {
		t.Fatalf("bad submit outcome = %+v, want backend_unavailable", bad)
	}
	replay, err := server.admissionReady.LookupReplay(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if replay.State != authority.ReplayMissing {
		t.Fatalf("replay after invalid first submit = %d, want missing", replay.State)
	}

	good := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: key.WorkspaceKey.String(),
		RequestID:    key.RequestID.String(),
		TaskSpec:     protocol.TaskSpec{Backend: "fake", CWD: cwd, Write: false, Prompt: "same request"},
	}))
	if good.err != nil {
		t.Fatalf("corrected submit error = %+v", good.err)
	}
	if good.after == nil {
		t.Fatal("corrected submit did not defer launch")
	}
	good.after()
	deadline := time.Now().Add(time.Second)
	for backend.count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := backend.count.Load(); got != 1 {
		t.Fatalf("backend starts after corrected submit = %d, want 1", got)
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
			text, state, err := server.runAttempt(ctx, run, "hold", false)
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
			text, state, err := server.runAttempt(ctx, run, "hold", false)
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
