//go:build darwin || linux

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
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
	if !socketPathMatchesIdentity(socketPath, identity) {
		t.Fatal("stale socket was not replaced by the private listener")
	}
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

	loser := newTestServer(t, root, Config{})
	paused := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	loser.beforeStaleSocketRemovalHook = func() {
		close(paused)
		<-release
	}
	type listenResult struct {
		listener net.Listener
		identity socketFileIdentity
		err      error
	}
	loserResult := make(chan listenResult, 1)
	go func() {
		listener, identity, err := loser.listen()
		loserResult <- listenResult{listener: listener, identity: identity, err: err}
	}()
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("loser did not reach stale socket removal")
	}

	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	winner := newTestServer(t, root, Config{})
	winnerListener, winnerIdentity, err := winner.listen()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = winnerListener.Close()
		winner.removeOwnedSocket(winnerIdentity, "test cleanup")
	}()

	close(release)
	var result listenResult
	select {
	case result = <-loserResult:
	case <-time.After(time.Second):
		t.Fatal("loser listen did not return after replacement")
	}
	if result.listener != nil {
		_ = result.listener.Close()
		loser.removeOwnedSocket(result.identity, "test cleanup")
	}
	if !errors.Is(result.err, ErrDaemonAlreadyListening) {
		t.Fatalf("loser listen error = %v, want ErrDaemonAlreadyListening", result.err)
	}
	var typed DaemonAlreadyListeningError
	if !errors.As(result.err, &typed) || typed.SocketPath != socketPath {
		t.Fatalf("loser listen error = %#v, want typed socket path %q", result.err, socketPath)
	}
	if !socketPathMatchesIdentity(socketPath, winnerIdentity) {
		t.Fatal("loser unlinked the winner socket")
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
