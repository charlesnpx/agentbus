//go:build darwin || linux

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type helloBackend struct {
	name    string
	models  []string
	efforts []string
}

func (backend helloBackend) Name() string { return backend.name }

func (helloBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{}, nil
}

func (helloBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used by the transport service")
}

func (helloBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used by the transport service")
}

func (backend helloBackend) BackendMetadata(context.Context) engine.BackendMetadata {
	return engine.BackendMetadata{Models: backend.models, Efforts: backend.efforts}
}

func newTestServer(t *testing.T, root string, cfg Config) *Server {
	t.Helper()
	cfg.StateRoot = root
	if cfg.Token == "" {
		cfg.Token = "test-token"
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.closeJobStore)
	return server
}

func shortTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "ab-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func helloResponse(t *testing.T, server *Server, version int, token string) protocol.Response {
	t.Helper()
	client, daemon := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&connection{server: server, conn: daemon}).serve(context.Background())
	}()
	request := protocol.Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`"hello"`),
		Method:  protocol.MethodHello,
		Params:  mustJSON(t, protocol.HelloParams{ClientProtocolVersion: version, Token: token}),
	}
	if err := writeFrame(client, request); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(client).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hello connection did not finish")
	}
	var response protocol.Response
	if err := json.Unmarshal(bytes.TrimSpace(line), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestHelloRejectsWrongClientProtocolVersion(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{
		Backends: []engine.Backend{helloBackend{name: "codex", models: []string{"gpt-5"}, efforts: []string{"high"}}},
	})
	response := helloResponse(t, server, ProtocolVersion-1, "test-token")
	if response.Error == nil {
		t.Fatal("hello response error = nil")
	}
	if response.Error.Data.Code != protocol.ErrorVersionMismatchV3 {
		t.Fatalf("hello error code = %q, want %q", response.Error.Data.Code, protocol.ErrorVersionMismatchV3)
	}
	if response.Error.Data.ServerProtocolVersion != ProtocolVersion {
		t.Fatalf("server protocol version = %d, want %d", response.Error.Data.ServerProtocolVersion, ProtocolVersion)
	}
}

func TestHelloRejectsWrongToken(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	response := helloResponse(t, server, ProtocolVersion, "wrong-token")
	if response.Error == nil {
		t.Fatal("hello response error = nil")
	}
	if response.Error.Data.Code != protocol.ErrorUnauthorizedV3 {
		t.Fatalf("hello error code = %q, want %q", response.Error.Data.Code, protocol.ErrorUnauthorizedV3)
	}
}

func TestHelloSerializesNilProviderMetadataAsEmptyArrays(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{
		Backends: []engine.Backend{helloBackend{name: "codex"}},
	})
	response := helloResponse(t, server, ProtocolVersion, "test-token")
	if response.Error != nil {
		t.Fatalf("hello response error = %#v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("hello result = %T, want object", response.Result)
	}
	backends, ok := result["backends"].([]any)
	if !ok || len(backends) != 1 {
		t.Fatalf("hello backends = %#v, want one backend", result["backends"])
	}
	backend, ok := backends[0].(map[string]any)
	if !ok {
		t.Fatalf("hello backend = %T, want object", backends[0])
	}
	for _, field := range []string{"models", "efforts"} {
		values, ok := backend[field].([]any)
		if !ok || len(values) != 0 {
			t.Fatalf("hello %s = %#v, want []", field, backend[field])
		}
	}
}

func TestListenRefusesSecondDaemonOnLiveSocket(t *testing.T) {
	root := shortTestDir(t)
	first := newTestServer(t, root, Config{})
	listener, identity, err := first.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = listener.Close()
		first.removeOwnedSocket(identity, "test cleanup")
	}()

	second := newTestServer(t, root, Config{})
	_, _, err = second.listen()
	if !errors.Is(err, ErrDaemonAlreadyListening) {
		t.Fatalf("second daemon listen error = %v, want ErrDaemonAlreadyListening", err)
	}
	var typed DaemonAlreadyListeningError
	if !errors.As(err, &typed) || typed.SocketPath != first.socketPath {
		t.Fatalf("second daemon listen error = %#v, want typed socket path %q", err, first.socketPath)
	}
}

func TestListenReplacesStaleSocket(t *testing.T) {
	root := shortTestDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	unixStale, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatalf("stale listener = %T, want *net.UnixListener", stale)
	}
	unixStale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(socketPath); err != nil {
		t.Fatalf("stale socket path = %v, want retained socket", err)
	}

	server := newTestServer(t, root, Config{})
	listener, identity, err := server.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = listener.Close()
		server.removeOwnedSocket(identity, "test cleanup")
	}()
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("replacement socket did not accept connections: %v", err)
	}
	_ = connection.Close()
}

func TestListenRefusesConcurrentStaleSocketReplacement(t *testing.T) {
	root := shortTestDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	unixStale, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatalf("stale listener = %T, want *net.UnixListener", stale)
	}
	unixStale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}

	winner := newTestServer(t, root, Config{})
	paused := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	winner.beforeStaleSocketRemovalHook = func() {
		close(paused)
		<-release
	}
	type listenResult struct {
		listener net.Listener
		identity socketFileIdentity
		err      error
	}
	winnerResult := make(chan listenResult, 1)
	go func() {
		listener, identity, err := winner.listen()
		winnerResult <- listenResult{listener: listener, identity: identity, err: err}
	}()
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("winner did not reach the final stale socket removal window")
	}

	loser := newTestServer(t, root, Config{})
	loserFlock := make(chan error, 1)
	loser.beforeSocketStateFlockHook = func(file *os.File) {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		}
		loserFlock <- err
	}
	loserResult := make(chan listenResult, 1)
	go func() {
		listener, identity, err := loser.listen()
		loserResult <- listenResult{listener: listener, identity: identity, err: err}
	}()
	select {
	case err := <-loserFlock:
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("loser flock while winner held the state-root lock = %v, want would block", err)
		}
	case <-time.After(time.Second):
		t.Fatal("loser did not reach the state-root flock while winner held the final removal window")
	}
	close(release)
	released = true
	var winnerListen listenResult
	select {
	case winnerListen = <-winnerResult:
	case <-time.After(time.Second):
		t.Fatal("winner listen did not return after stale socket removal")
	}
	if winnerListen.err != nil {
		t.Fatalf("winner listen error = %v", winnerListen.err)
	}
	defer func() {
		_ = winnerListen.listener.Close()
		winner.removeOwnedSocket(winnerListen.identity, "test cleanup")
	}()

	var loserListen listenResult
	select {
	case loserListen = <-loserResult:
	case <-time.After(time.Second):
		t.Fatal("loser listen did not return after winner bound the socket")
	}
	if loserListen.listener != nil {
		_ = loserListen.listener.Close()
		loser.removeOwnedSocket(loserListen.identity, "test cleanup")
	}
	if !errors.Is(loserListen.err, ErrDaemonAlreadyListening) {
		t.Fatalf("loser listen error = %v, want ErrDaemonAlreadyListening", loserListen.err)
	}
	var typed DaemonAlreadyListeningError
	if !errors.As(loserListen.err, &typed) || typed.SocketPath != socketPath {
		t.Fatalf("loser listen error = %#v, want typed socket path %q", loserListen.err, socketPath)
	}
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("winner socket did not accept connections: %v", err)
	}
	_ = connection.Close()
}

func TestConnectionReturnsBoundedOversizedFrameError(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	client, daemon := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&connection{server: server, conn: daemon, hello: true}).serve(context.Background())
	}()
	request := append([]byte(`{"jsonrpc":"2.0","id":"oversized","method":"protocol.hello","params":{"padding":"`), bytes.Repeat([]byte("x"), 4*1024*1024)...)
	request = append(request, []byte(`"}}`+"\n")...)
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(request)
		writeDone <- err
	}()
	if err := client.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(client).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var response protocol.Response
	if err := json.Unmarshal(bytes.TrimSpace(line), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Data.Code != protocol.ErrorInvalidTaskSpecV3 {
		t.Fatalf("oversized response = %#v, want bounded invalid-task error", response.Error)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("oversized request writer did not unblock")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oversized connection did not finish")
	}
}

func TestIdleShutdownAfterConfiguredWindow(t *testing.T) {
	server := newTestServer(t, shortTestDir(t), Config{
		IdleTimeout:       40 * time.Millisecond,
		IdleCheckInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("idle shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle shutdown did not fire")
	}
}

type submitProbeBackend struct {
	name       string
	probes     atomic.Int64
	preflights atomic.Int64
	preflight  error
	probe      error
}

func (backend *submitProbeBackend) Name() string { return backend.name }

func (backend *submitProbeBackend) Preflight(context.Context) (engine.Health, error) {
	backend.preflights.Add(1)
	return engine.Health{Backend: backend.name}, backend.preflight
}

func (backend *submitProbeBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used by submission tests")
}

func (backend *submitProbeBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used by submission tests")
}

func (backend *submitProbeBackend) SetupProbe(context.Context) (engine.BackendSetupProbe, error) {
	backend.probes.Add(1)
	if backend.probe != nil {
		return engine.BackendSetupProbe{}, backend.probe
	}
	return engine.BackendSetupProbe{Backend: backend.name, Version: "test"}, nil
}

func submitForTest(t *testing.T, server *Server, params protocol.JobSubmitParamsV3) requestOutcome {
	t.Helper()
	return server.handleJobSubmit(context.Background(), mustJSON(t, params))
}

func submitResultForTest(t *testing.T, outcome requestOutcome) protocol.JobSubmitResultV3 {
	t.Helper()
	if outcome.err != nil {
		t.Fatalf("submit error = %+v", outcome.err)
	}
	result, ok := outcome.result.(protocol.JobSubmitResultV3)
	if !ok {
		t.Fatalf("submit result = %T %#v, want protocol.JobSubmitResultV3", outcome.result, outcome.result)
	}
	if result.Timeout == nil || result.Timeout.Source == "" {
		t.Fatalf("submit timeout = %#v, want populated resolution", result.Timeout)
	}
	return result
}

func submissionParams(workspaceKey, requestID, backend, cwd, prompt string) protocol.JobSubmitParamsV3 {
	return protocol.JobSubmitParamsV3{
		WorkspaceKey: workspaceKey,
		RequestID:    requestID,
		TaskSpec: protocol.TaskSpecV3{
			Backend: backend,
			CWD:     cwd,
			Write:   false,
			Prompt:  prompt,
		},
	}
}

func TestJobSubmitReplaySurvivesDeletedWorkspace(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "workspace")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-delete", "request-delete", "codex", cwd, "same task")
	first := submitResultForTest(t, submitForTest(t, server, params))
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}
	replay := submitResultForTest(t, submitForTest(t, server, params))
	if !replay.Deduplicated || replay.JobID != first.JobID || replay.State != protocol.PublicStateQueued {
		t.Fatalf("deleted-workspace replay = %+v, want queued deduplication of %q", replay, first.JobID)
	}
}

func TestJobSubmitReplaySurvivesWorkspaceSymlinkReplacement(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "workspace")
	if err := os.Mkdir(cwd, 0o700); err != nil {
		t.Fatal(err)
	}
	server := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-symlink", "request-symlink", "codex", cwd, "same task")
	first := submitResultForTest(t, submitForTest(t, server, params))
	if err := os.RemoveAll(cwd); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, cwd); err != nil {
		t.Fatal(err)
	}
	replay := submitResultForTest(t, submitForTest(t, server, params))
	if !replay.Deduplicated || replay.JobID != first.JobID || replay.State != protocol.PublicStateQueued {
		t.Fatalf("symlink-workspace replay = %+v, want queued deduplication of %q", replay, first.JobID)
	}
}

func TestJobSubmitDifferentPromptReturnsTypedConflict(t *testing.T) {
	root := t.TempDir()
	server := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-conflict", "request-conflict", "codex", t.TempDir(), "first prompt")
	first := submitResultForTest(t, submitForTest(t, server, params))
	params.TaskSpec.Prompt = "different prompt"
	conflict := submitForTest(t, server, params)
	if conflict.err == nil || conflict.err.Data.Code != protocol.ErrorInvalidTaskSpecV3 {
		t.Fatalf("conflict error = %#v, want typed invalid-task conflict", conflict.err)
	}
	if conflict.err.Data.JobID != first.JobID || !strings.Contains(conflict.err.Message, "jobstore: request conflict") {
		t.Fatalf("conflict error = %#v, want existing job %q and jobstore conflict", conflict.err, first.JobID)
	}
	result, ok := conflict.result.(protocol.JobSubmitResultV3)
	if !ok || result.Timeout == nil || result.Timeout.Source == "" {
		t.Fatalf("conflict result = %#v, want populated timeout resolution", conflict.result)
	}
}

func TestJobSubmitUnknownBackendNewKeyIsAdmittedFailed(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	result := submitResultForTest(t, submitForTest(t, server, submissionParams("workspace-unknown-new", "request-unknown-new", "missing", t.TempDir(), "task")))
	if result.Deduplicated || result.State != protocol.PublicStateFailed {
		t.Fatalf("unknown-backend new result = %+v, want non-deduplicated failed job", result)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(result.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.FailureClass != protocol.FailureClassBackendUnavailable {
		t.Fatalf("unknown-backend failure class = %q, want %q", record.FailureClass, protocol.FailureClassBackendUnavailable)
	}
}

func TestJobSubmitUnknownBackendReplayDeduplicates(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-unknown-replay", "request-unknown-replay", "codex", t.TempDir(), "task")
	first := submitResultForTest(t, submitForTest(t, server, params))
	delete(server.backends, "codex")
	replay := submitResultForTest(t, submitForTest(t, server, params))
	if !replay.Deduplicated || replay.JobID != first.JobID || replay.State != protocol.PublicStateQueued {
		t.Fatalf("unknown-backend replay = %+v, want queued deduplication of %q", replay, first.JobID)
	}
}

func TestJobSubmitRejectsTimeoutAboveMaximum(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	timeout := protocol.MaxTimeout.Milliseconds() + 1
	params := submissionParams("workspace-timeout", "request-timeout", "codex", t.TempDir(), "task")
	params.TaskSpec.TimeoutMS = &timeout
	outcome := submitForTest(t, server, params)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpecV3 {
		t.Fatalf("oversized timeout outcome = %#v, want invalid task spec", outcome.err)
	}
}

func TestJobSubmitRejectsUnknownTaskSpecField(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	raw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"workspace-unknown-field","requestId":"request-unknown-field","taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"task","unexpected":true}}`, t.TempDir()))
	outcome := server.handleJobSubmit(context.Background(), raw)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpecV3 {
		t.Fatalf("unknown taskSpec field outcome = %#v, want invalid task spec", outcome.err)
	}
}

func TestJobSubmitTimeoutResolutionIsPresentOnNewReplayAndConflict(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	timeout := int64(1234)
	params := submissionParams("workspace-timeout-resolution", "request-timeout-resolution", "codex", t.TempDir(), "first prompt")
	params.TaskSpec.TimeoutMS = &timeout
	first := submitResultForTest(t, submitForTest(t, server, params))
	if first.Timeout.Requested == nil || *first.Timeout.Requested != timeout || first.Timeout.Effective != timeout || first.Timeout.Source != engine.TimeoutSourceClient {
		t.Fatalf("new timeout = %#v, want requested client resolution", first.Timeout)
	}
	replay := submitResultForTest(t, submitForTest(t, server, params))
	if replay.Timeout.Requested == nil || *replay.Timeout.Requested != timeout || replay.Timeout.Effective != timeout || replay.Timeout.Source != engine.TimeoutSourceClient {
		t.Fatalf("replay timeout = %#v, want requested client resolution", replay.Timeout)
	}
	params.TaskSpec.Prompt = "conflicting prompt"
	conflict := submitForTest(t, server, params)
	if conflict.err == nil {
		t.Fatal("conflict error = nil, want typed conflict")
	}
	result, ok := conflict.result.(protocol.JobSubmitResultV3)
	if !ok || result.Timeout == nil || result.Timeout.Requested == nil || *result.Timeout.Requested != timeout || result.Timeout.Effective != timeout || result.Timeout.Source != engine.TimeoutSourceClient {
		t.Fatalf("conflict timeout result = %#v, want requested client resolution", conflict.result)
	}
}

func TestJobSubmitProbesAndCachesBackendOnDemand(t *testing.T) {
	root := t.TempDir()
	backend := &submitProbeBackend{name: "probed"}
	server := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	result := submitResultForTest(t, submitForTest(t, server, submissionParams("workspace-probed", "request-probed", "probed", t.TempDir(), "task")))
	if result.State != protocol.PublicStateQueued {
		t.Fatalf("on-demand probe result = %+v, want queued", result)
	}
	if got := backend.probes.Load(); got != 1 {
		t.Fatalf("on-demand probes = %d, want 1", got)
	}
	cachePath, err := engine.SetupProbeCachePath(root)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := engine.ReadSetupProbeCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if !cacheHasBackend(cache, "probed") {
		t.Fatalf("cached probes = %+v, want probed backend", cache.Backends)
	}
}
