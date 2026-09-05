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
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jcs"
	"github.com/charlesnpx/agentbus/internal/jobstore"
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
		(&connection{server: server, conn: daemon}).serve()
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
	if response.Error.Data.Code != protocol.ErrorVersionMismatch {
		t.Fatalf("hello error code = %q, want %q", response.Error.Data.Code, protocol.ErrorVersionMismatch)
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
	if response.Error.Data.Code != protocol.ErrorUnauthorized {
		t.Fatalf("hello error code = %q, want %q", response.Error.Data.Code, protocol.ErrorUnauthorized)
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
		(&connection{server: server, conn: daemon, hello: true}).serve()
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
	if response.Error == nil || response.Error.Data.Code != protocol.ErrorInvalidTaskSpec {
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

type submitSessionBackend struct {
	name       string
	sessions   atomic.Int64
	preflights atomic.Int64
	preflight  error
}

func (backend *submitSessionBackend) Name() string { return backend.name }

func (backend *submitSessionBackend) Preflight(context.Context) (engine.Health, error) {
	backend.preflights.Add(1)
	return engine.Health{Backend: backend.name}, backend.preflight
}

func (backend *submitSessionBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	backend.sessions.Add(1)
	return nil, errors.New("not used by submission tests")
}

func (backend *submitSessionBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	backend.sessions.Add(1)
	return nil, errors.New("not used by submission tests")
}

func submitForTest(t *testing.T, server *Server, params protocol.JobSubmitParams) requestOutcome {
	t.Helper()
	return server.handleJobSubmit(mustJSON(t, params))
}

func submitResultForTest(t *testing.T, outcome requestOutcome) protocol.JobSubmitResult {
	t.Helper()
	if outcome.err != nil {
		t.Fatalf("submit error = %+v", outcome.err)
	}
	result, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok {
		t.Fatalf("submit result = %T %#v, want protocol.JobSubmitResult", outcome.result, outcome.result)
	}
	if result.Timeout == nil || result.Timeout.Source == "" {
		t.Fatalf("submit timeout = %#v, want populated resolution", result.Timeout)
	}
	return result
}

func submissionParams(workspaceKey, requestID, backend, cwd, prompt string) protocol.JobSubmitParams {
	return protocol.JobSubmitParams{
		WorkspaceKey: workspaceKey,
		RequestID:    requestID,
		TaskSpec: protocol.TaskSpec{
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
	if conflict.err == nil || conflict.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("conflict error = %#v, want typed invalid-task conflict", conflict.err)
	}
	if conflict.err.Data.JobID != first.JobID || !strings.Contains(conflict.err.Message, "jobstore: request conflict") {
		t.Fatalf("conflict error = %#v, want existing job %q and jobstore conflict", conflict.err, first.JobID)
	}
	if conflict.result != nil {
		t.Fatalf("conflict result = %#v, want error-only JSON-RPC outcome", conflict.result)
	}
}

func TestJobSubmitInvalidOutputSchemaLeavesNoBinding(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{name: "null", schema: `null`},
		{name: "array", schema: `[]`},
		{name: "uncompilable", schema: `{"type":7}`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
			workspaceKey := "workspace-invalid-schema-" + tt.name
			requestID := "request-invalid-schema-" + tt.name
			cwd := t.TempDir()
			invalidRaw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":%q,"requestId":%q,"taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"task","outputSchema":%s}}`, workspaceKey, requestID, cwd, tt.schema))
			invalid := server.handleJobSubmit(invalidRaw)
			if invalid.err == nil || invalid.err.Data.Code != protocol.ErrorInvalidTaskSpec {
				t.Fatalf("invalid schema outcome = %#v, want invalid task spec", invalid.err)
			}
			if invalid.result != nil {
				t.Fatalf("invalid schema result = %#v, want none", invalid.result)
			}

			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			records, err := store.List()
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 0 {
				t.Fatalf("records after invalid schema = %+v, want none", records)
			}

			correctedRaw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":%q,"requestId":%q,"taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"task","outputSchema":{}}}`, workspaceKey, requestID, cwd))
			corrected := submitResultForTest(t, server.handleJobSubmit(correctedRaw))
			if corrected.Deduplicated || corrected.State != protocol.PublicStateQueued {
				t.Fatalf("corrected schema result = %+v, want a new queued job", corrected)
			}
		})
	}
}

func TestJobSubmitEquivalentNumericOutputSchemaSpellingsReplay(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	workspaceKey, requestID, cwd := "workspace-jcs-number", "request-jcs-number", t.TempDir()
	firstRaw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":%q,"requestId":%q,"taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"task","outputSchema":{"type":"number","minimum":1.0}}}`, workspaceKey, requestID, cwd))
	first := submitResultForTest(t, server.handleJobSubmit(firstRaw))

	secondRaw := json.RawMessage(fmt.Sprintf(`{"requestId":%q,"taskSpec":{"outputSchema":{"minimum":1e0,"type":"number"},"prompt":"task","write":false,"cwd":%q,"backend":"codex"},"workspaceKey":%q}`, requestID, cwd, workspaceKey))
	replay := submitResultForTest(t, server.handleJobSubmit(secondRaw))
	if !replay.Deduplicated || replay.JobID != first.JobID || replay.State != protocol.PublicStateQueued {
		t.Fatalf("numeric-spelling replay = %+v, want queued deduplication of %q", replay, first.JobID)
	}
}

func TestTaskSpecJCSCanonicalRendering(t *testing.T) {
	canonical, err := jcs.Render([]byte(`{"\uE000":1,"\uD834\uDD1E":2,"z":-0,"a":[true,null,"<tag>"],"n":9007199254740993}`))
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"a\":[true,null,\"<tag>\"],\"n\":9007199254740992,\"z\":0,\"\U0001D11E\":2,\"\uE000\":1}"
	if got := string(canonical); got != want {
		t.Fatalf("JCS canonical = %s, want %s", got, want)
	}

	for _, tt := range []struct {
		raw  string
		want string
	}{
		{raw: `1`, want: `1`},
		{raw: `1.0`, want: `1`},
		{raw: `1e0`, want: `1`},
		{raw: `333333333.33333329`, want: `333333333.3333333`},
		{raw: `4.50`, want: `4.5`},
		{raw: `2e-3`, want: `0.002`},
		{raw: `0.000001`, want: `0.000001`},
		{raw: `1e-7`, want: `1e-7`},
		{raw: `0.000000000000000000000000001`, want: `1e-27`},
		{raw: `100000000000000000000`, want: `100000000000000000000`},
		{raw: `1e21`, want: `1e+21`},
		{raw: `1E30`, want: `1e+30`},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			canonical, err := jcs.Render([]byte(tt.raw))
			if err != nil {
				t.Fatal(err)
			}
			if got := string(canonical); got != tt.want {
				t.Fatalf("JCS number = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestJobSubmitUnknownBackendNewKeyLeavesNoRecordOrBinding(t *testing.T) {
	backend := &submitSessionBackend{name: "configured"}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	params := submissionParams("workspace-unknown-new", "request-unknown-new", "missing", t.TempDir(), "task")
	outcome := submitForTest(t, server, params)
	if outcome.err == nil {
		t.Fatalf("unknown-backend new result = %#v, want submission error", outcome.result)
	}
	if outcome.result != nil {
		t.Fatalf("unknown-backend new result = %#v, want no result", outcome.result)
	}
	if got := backend.sessions.Load(); got != 0 {
		t.Fatalf("provider sessions after unknown-backend submission = %d, want 0", got)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after unknown-backend submission = %+v, want none", records)
	}
	server.backends["missing"] = helloBackend{name: "missing"}
	retried := submitResultForTest(t, submitForTest(t, server, params))
	if retried.Deduplicated || retried.State != protocol.PublicStateQueued {
		t.Fatalf("retried unknown-backend result = %+v, want new queued job", retried)
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

func TestJobSubmitConflictPrecedesSemanticTaskSpecValidation(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-invalid-conflict", "request-invalid-conflict", "codex", t.TempDir(), "first prompt")
	first := submitResultForTest(t, submitForTest(t, server, params))

	params.TaskSpec.CWD = "relative"
	params.TaskSpec.Prompt = ""
	timeout := protocol.MaxTimeout.Milliseconds() + 1
	params.TaskSpec.TimeoutMS = &timeout
	conflict := submitForTest(t, server, params)
	if conflict.err == nil || conflict.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("invalid conflicting task error = %#v, want typed conflict", conflict.err)
	}
	if conflict.err.Data.JobID != first.JobID || !strings.Contains(conflict.err.Message, "jobstore: request conflict") {
		t.Fatalf("invalid conflicting task error = %#v, want existing job %q", conflict.err, first.JobID)
	}
	if conflict.result != nil {
		t.Fatalf("invalid conflicting task result = %#v, want error-only conflict", conflict.result)
	}
}

func TestJobSubmitRejectsTimeoutAboveMaximum(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	timeout := protocol.MaxTimeout.Milliseconds() + 1
	params := submissionParams("workspace-timeout", "request-timeout", "codex", t.TempDir(), "task")
	params.TaskSpec.TimeoutMS = &timeout
	outcome := submitForTest(t, server, params)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("oversized timeout outcome = %#v, want invalid task spec", outcome.err)
	}
}

func TestJobSubmitRejectsUnknownTaskSpecField(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	raw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"workspace-unknown-field","requestId":"request-unknown-field","taskSpec":{"backend":"codex","cwd":%q,"write":false,"prompt":"task","unexpected":true}}`, t.TempDir()))
	outcome := server.handleJobSubmit(raw)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("unknown taskSpec field outcome = %#v, want invalid task spec", outcome.err)
	}
}

func TestJobSubmitTimeoutResolutionIsPresentOnNewAndReplay(t *testing.T) {
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
	if conflict.result != nil {
		t.Fatalf("conflict result = %#v, want error-only JSON-RPC outcome", conflict.result)
	}
}

func TestJobSubmitConfiguredBackendDoesNotPreflightDuringAdmission(t *testing.T) {
	backend := &submitSessionBackend{name: "configured", preflight: errors.New("backend unavailable")}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	result := submitResultForTest(t, submitForTest(t, server, submissionParams("workspace-configured", "request-configured", "configured", t.TempDir(), "task")))
	if result.State != protocol.PublicStateQueued {
		t.Fatalf("configured-backend result = %+v, want queued", result)
	}
	if got := backend.preflights.Load(); got != 0 {
		t.Fatalf("preflight calls during admission = %d, want 0", got)
	}
	if got := backend.sessions.Load(); got != 0 {
		t.Fatalf("provider sessions during admission = %d, want 0", got)
	}
}

func TestJobSubmitResumeTargetWithoutSessionReturnsTypedError(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	source := submitResultForTest(t, submitForTest(t, server, submissionParams("resume-missing-source", "source", "fake", t.TempDir(), "source")))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(source.JobID, jobstore.TerminalUpdate{
		State:         protocol.PublicStateFailed,
		Cleanup:       protocol.CleanupClean,
		FailureClass:  protocol.FailureClassBackendError,
		FailureReason: "failed before its first turn",
	}); err != nil {
		t.Fatal(err)
	}

	params := submissionParams("resume-missing-target", "resume", "fake", t.TempDir(), "continue")
	params.TaskSpec.ResumeJobID = source.JobID
	outcome := submitForTest(t, server, params)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec || outcome.err.Data.JobID != source.JobID {
		t.Fatalf("resume without session = %#v, want typed invalid-task error for %q", outcome.err, source.JobID)
	}
	if outcome.result != nil {
		t.Fatalf("resume without session result = %#v, want error only", outcome.result)
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].JobID != source.JobID {
		t.Fatalf("records after invalid resume = %+v, want only source %q", records, source.JobID)
	}
}

func TestJobSubmitResumeTargetParticipatesInReplayIdentity(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	newSource := func(requestID string, sessionID string) protocol.JobSubmitResult {
		source := submitResultForTest(t, submitForTest(t, server, submissionParams("resume-identity-source", requestID, "fake", t.TempDir(), "source")))
		if _, err := store.MarkTerminal(source.JobID, jobstore.TerminalUpdate{
			State:            protocol.PublicStateFailed,
			Cleanup:          protocol.CleanupClean,
			BackendSessionID: sessionID,
			FailureClass:     protocol.FailureClassBackendError,
			FailureReason:    "timed out after a turn",
		}); err != nil {
			t.Fatal(err)
		}
		return source
	}
	firstSource := newSource("source-one", "thread-one")
	secondSource := newSource("source-two", "thread-two")

	params := submissionParams("resume-identity", "same-request", "fake", t.TempDir(), "continue")
	params.TaskSpec.ResumeJobID = firstSource.JobID
	first := submitResultForTest(t, submitForTest(t, server, params))
	params.TaskSpec.ResumeJobID = secondSource.JobID
	conflict := submitForTest(t, server, params)
	if conflict.err == nil || conflict.err.Data.Code != protocol.ErrorInvalidTaskSpec || conflict.err.Data.JobID != first.JobID {
		t.Fatalf("different resume target replay = %#v, want typed conflict bound to %q", conflict.err, first.JobID)
	}
	records, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("records after conflicting resume identity = %d, want two sources and one new job", len(records))
	}
}
