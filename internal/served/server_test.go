package served

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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
	processRef      engine.ProcessRef
	backendChildPID int
}

type fakeTurn struct {
	Prompt string
	Write  bool
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

func (b *fakeBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	n := b.count.Add(1)
	return &fakeSession{id: b.name + "-session-" + stringID(n), backend: b}, nil
}

func (b *fakeBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used")
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
	for _, capability := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude"} {
		if _, ok := hello.Capabilities[capability]; !ok {
			t.Fatalf("missing capability %s in %+v", capability, hello.Capabilities)
		}
	}

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
	if len(status.Jobs) != 1 || status.Jobs[0].WorkerPID != 4242 || status.Jobs[0].BackendChildPID != 4243 {
		t.Fatalf("status worker identity = %+v", status)
	}
	record := loadJobRecord(t, h.root, h.cwd, job.JobID, processes)
	if record.Worker.PID != 4242 || record.Worker.PGID != 4242 || record.Worker.StartTime != "backend-start" {
		t.Fatalf("record worker = %+v", record.Worker)
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
	var canceled protocol.JobCancelResult
	decodeResult(t, rpc(t, conn, r, "3", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: job.JobID}), &canceled)
	if canceled.State != engine.StateCanceled {
		t.Fatalf("cancel result = %+v", canceled)
	}
	select {
	case got := <-waiter.durations:
		if got != engine.DefaultCancelGrace {
			t.Fatalf("cancel grace = %s, want %s", got, engine.DefaultCancelGrace)
		}
	default:
		t.Fatal("store cancellation did not wait the protocol grace")
	}
	signals := groups.snapshot()
	if len(signals) != 2 || signals[0].Signal != syscall.SIGTERM || signals[1].Signal != syscall.SIGKILL || signals[0].PGID != 5250 || signals[1].PGID != 5250 {
		t.Fatalf("signals = %+v", signals)
	}
	if got := backend.interrupts.Load(); got != 0 {
		t.Fatalf("session interrupts = %d, want 0", got)
	}
	close(release)
	waitJobState(t, conn, r, job.JobID, engine.StateCanceled)
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

func TestStartReaperRecoversCrashedJob(t *testing.T) {
	t.Parallel()
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	store, err := engine.NewStore(engine.StoreConfig{
		Root:      root,
		CWD:       cwd,
		Clock:     engine.ClockFunc(time.Now),
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
		Lease:     engine.Lease{ExpiresAt: time.Now().Add(time.Minute)},
	}); err != nil {
		t.Fatal(err)
	}
	backend := newFakeBackend("fake")
	h := startTestServerWithRoot(t, root, cwd, backend, Config{IdleTimeout: -1, ProcessTable: fakeProcessTable{}})
	conn := dialRaw(t, h.socketPath)
	defer conn.Close()
	r := bufio.NewReader(conn)
	helloRaw(t, conn, r, h.token)
	status := rpc(t, conn, r, "2", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: "job_crashed"})
	var out protocol.JobStatusResult
	decodeResult(t, status, &out)
	if len(out.Jobs) != 1 || out.Jobs[0].State != engine.StateReaped {
		t.Fatalf("status = %+v", out)
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
}

func newRecordingWaiter() *recordingWaiter {
	return &recordingWaiter{durations: make(chan time.Duration, 4)}
}

func (w *recordingWaiter) Wait(d time.Duration) {
	w.durations <- d
}

type testServer struct {
	root       string
	cwd        string
	socketPath string
	token      string
	done       chan error
	cancel     context.CancelFunc
}

func startTestServer(t *testing.T, backend engine.Backend, cfg Config) testServer {
	t.Helper()
	return startTestServerWithRoot(t, shortTempDir(t), shortTempDir(t), backend, cfg)
}

func startTestServerWithRoot(t *testing.T, root, cwd string, backend engine.Backend, cfg Config) testServer {
	t.Helper()
	cfg.StateRoot = root
	cfg.CWD = cwd
	cfg.Token = "test-token"
	cfg.Backends = []engine.Backend{backend}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
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
