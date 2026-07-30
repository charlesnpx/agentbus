package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/served"
)

func TestMain(m *testing.M) {
	if os.Getenv("AGENTBUS_AUTOSTART_FAILURE_HELPER") == "1" &&
		len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--foreground" {
		os.Exit(runAutostartFailureDaemon())
	}
	if os.Getenv("AGENTBUS_AUTOSTART_DETACH_HELPER") == "1" &&
		len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--foreground" {
		os.Exit(runAutostartDetachDaemon())
	}
	cacheDir, err := os.MkdirTemp(os.TempDir(), "ab-client-cache-")
	if err == nil {
		autostartUserCacheDir = func() (string, error) {
			return cacheDir, nil
		}
	}
	code := m.Run()
	if cacheDir != "" {
		_ = os.RemoveAll(cacheDir)
	}
	os.Exit(code)
}

func runAutostartFailureDaemon() int {
	_, _ = os.Stderr.WriteString("autostart helper startup stderr\n")
	reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		return 2
	}
	if !hasReporter {
		_, _ = os.Stderr.WriteString("missing readiness reporter\n")
		return 2
	}
	_ = reporter.Failed("strict admission support unavailable", "strict diagnostic from autostart helper")
	return 1
}

func runAutostartDetachDaemon() int {
	root := os.Getenv("AGENTBUS_STATE_ROOT")
	if root == "" {
		root = os.Getenv("AGENTBUS_AUTOSTART_DETACH_ROOT")
	}
	if _, err := startClientTestDaemon(context.Background(), root, os.Getenv("AGENTBUS_AUTOSTART_DETACH_TOKEN")); err != nil {
		recordAutostartDetachDaemonError(err)
		return 1
	}
	reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
	if err != nil {
		recordAutostartDetachDaemonError(err)
		return 1
	}
	if hasReporter {
		canonicalRoot, err := daemonlaunch.CanonicalStateRoot(root)
		if err != nil {
			recordAutostartDetachDaemonError(err)
			return 1
		}
		if err := reporter.Ready(canonicalRoot, filepath.Join(root, protocol.SocketName)); err != nil {
			recordAutostartDetachDaemonError(err)
			return 1
		}
	}
	select {}
}

func recordAutostartDetachDaemonError(err error) {
	if path := os.Getenv("AGENTBUS_AUTOSTART_DETACH_ERROR_PATH"); path != "" {
		_ = os.WriteFile(path, []byte(err.Error()), 0o600)
	}
}

const clientTestSandboxBindDeniedEnv = "AGENTBUS_TEST_SANDBOX_BIND_DENIED"

func clientTestBindDeniedOutput(output string) bool {
	output = strings.ToLower(output)
	return strings.Contains(output, "bind: operation not permitted") ||
		strings.Contains(output, "bind: permission denied") ||
		strings.Contains(output, "unix socket bind denied by sandbox")
}

func clientTestSkipOrFailBindDenied(t *testing.T, context string, detail any) {
	t.Helper()
	if os.Getenv(clientTestSandboxBindDeniedEnv) == "1" {
		t.Skipf("Unix socket bind denied by sandbox in %s (%s=1): %v", context, clientTestSandboxBindDeniedEnv, detail)
	}
	t.Fatalf("Unix socket bind denied in %s without %s=1; failing to expose daemon bind regressions: %v", context, clientTestSandboxBindDeniedEnv, detail)
}

func TestClientHelloParsesBackendMetadata(t *testing.T) {
	hello := runClientHello(t, `{"protocolVersion":2,"backends":["codex"],"backendMetadata":[{"backend":"codex","models":["gpt-5"],"efforts":["high"]}],"capabilities":{"models.discovery":true}}`)

	if hello.ProtocolVersion != protocol.Version || len(hello.Backends) != 1 || hello.Backends[0] != "codex" || !hello.Capabilities["models.discovery"] {
		t.Fatalf("hello = %+v", hello)
	}
	if len(hello.BackendMetadata) != 1 {
		t.Fatalf("backend metadata = %+v", hello.BackendMetadata)
	}
	info := hello.BackendMetadata[0]
	if info.Name != "codex" || len(info.Models) != 1 || info.Models[0] != "gpt-5" || len(info.Efforts) != 1 || info.Efforts[0] != "high" {
		t.Fatalf("backend metadata = %+v", hello.BackendMetadata)
	}
}

func TestClientHelloParsesCapabilitiesWithoutBackendMetadata(t *testing.T) {
	hello := runClientHello(t, `{"protocolVersion":2,"backends":["codex"],"capabilities":{"models.discovery":false}}`)

	if hello.ProtocolVersion != protocol.Version || len(hello.Backends) != 1 || hello.Backends[0] != "codex" || hello.Capabilities["models.discovery"] {
		t.Fatalf("hello = %+v", hello)
	}
	if hello.BackendMetadata != nil {
		t.Fatalf("backend metadata = %+v, want nil", hello.BackendMetadata)
	}
}

func TestClientHelloRejectsProtocolVersionMismatch(t *testing.T) {
	err := runClientHelloError(t, `{"protocolVersion":1,"backends":["codex"],"capabilities":{}}`)
	if !errors.Is(err, ErrProtocolVersionMismatch) {
		t.Fatalf("clientHello error = %v, want ErrProtocolVersionMismatch", err)
	}
	if !strings.Contains(err.Error(), "expected 2") || !strings.Contains(err.Error(), "received 1") {
		t.Fatalf("clientHello error message = %q, want expected and received versions", err.Error())
	}
}

func TestConnectProtocolVersionMismatchDoesNotAutostart(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		if clientTestBindDeniedOutput(err.Error()) {
			clientTestSkipOrFailBindDenied(t, "protocol-mismatch listener", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				done <- nil
				return
			}
			done <- err
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			done <- err
			return
		}
		done <- json.NewEncoder(conn).Encode(protocol.Response{
			JSONRPC: "2.0",
			ID:      json.RawMessage(`"hello"`),
			Result: protocol.HelloResult{
				ProtocolVersion: 1,
				Backends:        []string{"codex"},
				Capabilities:    protocol.DefaultCapabilities(),
			},
		})
	}()

	var starts atomic.Int64
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		starts.Add(1)
		return StartResult{}, errors.New("starter should not be invoked for protocol mismatch")
	})
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		StartTimeout: 100 * time.Millisecond,
		Starter:      starter,
	})
	if client != nil {
		_ = client.Close()
	}
	if !errors.Is(err, ErrProtocolVersionMismatch) {
		t.Fatalf("Connect error = %v, want ErrProtocolVersionMismatch", err)
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("starter calls = %d, want 0", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestConnectBadTokenDoesNotAutostart(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	serverCtx, cancelServer := context.WithCancel(context.Background())
	done, err := startClientTestDaemon(serverCtx, root, "server-token")
	if err != nil {
		if clientTestBindDeniedOutput(err.Error()) {
			clientTestSkipOrFailBindDenied(t, "bad-token daemon", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelServer()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})

	var starts atomic.Int64
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		starts.Add(1)
		return StartResult{}, errors.New("starter should not be invoked for bad token")
	})
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "client-token",
		StartTimeout: 100 * time.Millisecond,
		Starter:      starter,
	})
	if client != nil {
		_ = client.Close()
	}
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Object.Data.Code != protocol.ErrorUnauthorized {
		t.Fatalf("Connect error = %v, want unauthorized RPC error", err)
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("starter calls = %d, want 0", got)
	}
}

func runClientHello(t *testing.T, result string) HelloResult {
	t.Helper()
	hello, err := runClientHelloResult(t, result)
	if err != nil {
		t.Fatal(err)
	}
	return hello
}

func runClientHelloError(t *testing.T, result string) error {
	t.Helper()
	_, err := runClientHelloResult(t, result)
	if err == nil {
		t.Fatal("clientHello succeeded, want error")
	}
	return err
}

func runClientHelloResult(t *testing.T, result string) (HelloResult, error) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	errCh := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(serverConn)
		if _, err := reader.ReadBytes('\n'); err != nil {
			errCh <- err
			return
		}
		var value any
		if err := json.Unmarshal([]byte(result), &value); err != nil {
			errCh <- err
			return
		}
		errCh <- json.NewEncoder(serverConn).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      "hello",
			"result":  value,
		})
	}()

	hello, err := clientHello(context.Background(), clientConn, bufio.NewReader(clientConn), "token")
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	return hello, err
}

func TestAutostartRaceStartsOneDaemon(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	var starts atomic.Int64
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (StartResult, error) {
		starts.Add(1)
		_, err := startClientTestDaemon(serverCtx, root, "race-token")
		if err != nil {
			return StartResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return StartResult{}, ctx.Err()
		}
		return StartResult{PID: os.Getpid()}, nil
	})

	opts := Options{
		StateRoot:    root,
		Token:        "race-token",
		StartTimeout: 2 * time.Second,
		Starter:      starter,
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	clients := make(chan *Client, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Connect(context.Background(), opts)
			if err != nil {
				errs <- err
				return
			}
			clients <- c
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if clientTestBindDeniedOutput(err.Error()) ||
			strings.Contains(strings.ToLower(err.Error()), "connect: no such file") {
			clientTestSkipOrFailBindDenied(t, "autostart race", err)
		}
		t.Fatalf("connect error: %v", err)
	}
	close(clients)
	for c := range clients {
		if c.HelloResult().ProtocolVersion != protocol.Version {
			t.Fatalf("hello = %+v", c.HelloResult())
		}
		_ = c.Close()
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}
}

func TestAutostartTempFallbackRejectsUnsafeLockDir(t *testing.T) {
	originalUserCacheDir := autostartUserCacheDir
	originalTempDir := autostartTempDir
	t.Cleanup(func() {
		autostartUserCacheDir = originalUserCacheDir
		autostartTempDir = originalTempDir
	})
	tmp := t.TempDir()
	autostartUserCacheDir = func() (string, error) {
		return "", errors.New("no user cache for test")
	}
	autostartTempDir = func() string {
		return tmp
	}
	lockDir := filepath.Join(tmp, fmt.Sprintf("agentbus-start-locks-%d", os.Getuid()))
	if err := os.Mkdir(lockDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := autostartLockPath("unsafe-temp-dir-test")
	if !errors.Is(err, ErrAutostartLockUnsafe) {
		t.Fatalf("autostartLockPath error = %v, want ErrAutostartLockUnsafe", err)
	}
}

func TestAutostartPrimaryRejectsWorldWritableLockDir(t *testing.T) {
	originalUserCacheDir := autostartUserCacheDir
	t.Cleanup(func() { autostartUserCacheDir = originalUserCacheDir })
	cacheDir := t.TempDir()
	autostartUserCacheDir = func() (string, error) {
		return cacheDir, nil
	}
	lockDir := filepath.Join(cacheDir, "agentbus", "start-locks")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(lockDir, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(lockDir, 0o700) })

	_, err := autostartLockPath("unsafe-primary-dir-test")
	if !errors.Is(err, ErrAutostartLockUnsafe) {
		t.Fatalf("autostartLockPath error = %v, want ErrAutostartLockUnsafe", err)
	}
}

func TestAutostartLockOpenRejectsSymlinkLockFile(t *testing.T) {
	originalUserCacheDir := autostartUserCacheDir
	t.Cleanup(func() { autostartUserCacheDir = originalUserCacheDir })
	cacheDir := t.TempDir()
	autostartUserCacheDir = func() (string, error) {
		return cacheDir, nil
	}
	lockKey := "symlink-lock-file-test"
	lockPath, err := autostartLockPath(lockKey)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	lock, err := openAutostartLockFileForKey(lockKey)
	if err == nil {
		_ = lock.Close()
		t.Fatal("openAutostartLockFileForKey succeeded for symlink lock file")
	}
	if !errors.Is(err, ErrAutostartLockUnsafe) {
		t.Fatalf("openAutostartLockFileForKey error = %v, want ErrAutostartLockUnsafe", err)
	}
}

func TestConnectHelloEOFAutostartsReplacement(t *testing.T) {
	t.Parallel()
	testConnectHelloTransportFailureAutostartsReplacement(t, "eof-token", nil)
}

func TestConnectHelloGarbageAutostartsReplacement(t *testing.T) {
	t.Parallel()
	testConnectHelloTransportFailureAutostartsReplacement(t, "garbage-token", func(conn net.Conn) error {
		_, err := conn.Write([]byte("not json\n"))
		return err
	})
}

func testConnectHelloTransportFailureAutostartsReplacement(t *testing.T, token string, write func(net.Conn) error) {
	t.Helper()
	root := shortClientTempDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	startHelloTransportFailureDaemon(t, socketPath, write)

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	var starts atomic.Int64
	var serverStarted atomic.Bool
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (StartResult, error) {
		starts.Add(1)
		done, err := startClientTestDaemon(serverCtx, root, token)
		if err != nil {
			return StartResult{}, err
		}
		serverStarted.Store(true)
		go func() { serverDone <- <-done }()
		if err := ctx.Err(); err != nil {
			return StartResult{}, ctx.Err()
		}
		return StartResult{PID: os.Getpid()}, nil
	})
	t.Cleanup(func() {
		cancelServer()
		if !serverStarted.Load() {
			return
		}
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("autostarted server exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("autostarted server did not stop")
		}
	})

	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        token,
		StartTimeout: 2 * time.Second,
		Starter:      starter,
	})
	if err != nil {
		if clientTestBindDeniedOutput(err.Error()) {
			clientTestSkipOrFailBindDenied(t, "hello transport replacement", err)
		}
		t.Fatal(err)
	}
	defer client.Close()
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}
	if client.HelloResult().ProtocolVersion != protocol.Version {
		t.Fatalf("hello = %+v", client.HelloResult())
	}
}

func startHelloTransportFailureDaemon(t *testing.T, socketPath string, write func(net.Conn) error) {
	t.Helper()
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		if clientTestBindDeniedOutput(err.Error()) {
			clientTestSkipOrFailBindDenied(t, "hello transport listener", err)
		}
		t.Fatal(err)
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener type = %T, want *net.UnixListener", listener)
	}
	unixListener.SetUnlinkOnClose(false)
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				done <- nil
				return
			}
			done <- err
			return
		}
		_ = listener.Close()
		if write != nil {
			if err := write(conn); err != nil {
				_ = conn.Close()
				done <- err
				return
			}
		}
		done <- conn.Close()
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("hello failure daemon exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("hello failure daemon did not stop")
		}
	})
}

func TestAutostartReplacesRefusedSocket(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	socketPath := filepath.Join(root, "agentbus.sock")
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		if clientTestBindDeniedOutput(err.Error()) {
			clientTestSkipOrFailBindDenied(t, "refused-socket listener", err)
		}
		t.Fatal(err)
	}
	unixListener, ok := stale.(*net.UnixListener)
	if !ok {
		t.Fatalf("listener type = %T, want *net.UnixListener", stale)
	}
	unixListener.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	// A connect can transiently land in the closed listener's dying backlog
	// (observed on macOS under load); the precondition under test is only that
	// the socket BECOMES refused, so retry briefly instead of failing on the
	// first successful dial.
	refusedDeadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			if time.Now().After(refusedDeadline) {
				t.Fatal("socket never became connection-refused after listener close")
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("dial to closed socket error = %v, want connection refused", err)
		}
		break
	}

	serverCtx, cancelServer := context.WithCancel(context.Background())
	serverDone := make(chan error, 1)
	var starts atomic.Int64
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (StartResult, error) {
		starts.Add(1)
		done, err := startClientTestDaemon(serverCtx, root, "refused-token")
		if err != nil {
			return StartResult{}, err
		}
		go func() { serverDone <- <-done }()
		if err := ctx.Err(); err != nil {
			return StartResult{}, ctx.Err()
		}
		return StartResult{PID: os.Getpid()}, nil
	})
	t.Cleanup(func() {
		cancelServer()
		select {
		case err := <-serverDone:
			if err != nil {
				t.Errorf("autostarted server exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("autostarted server did not stop")
		}
	})

	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "refused-token",
		StartTimeout: 2 * time.Second,
		Starter:      starter,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}
}

func TestConnectAutostartSurfacesLauncherFailureOnce(t *testing.T) {
	root := shortClientTempDir(t)
	bin, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBUS_AUTOSTART_FAILURE_HELPER", "1")

	var starts atomic.Int64
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (StartResult, error) {
		starts.Add(1)
		return (defaultStarter{}).StartDaemon(ctx, opts)
	})
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		CommandPath:  bin,
		StartTimeout: 2 * time.Second,
		Starter:      starter,
	})
	if client != nil {
		_ = client.Close()
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(err, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Connect error = %T %v, want daemon launch startup failure", err, err)
	}
	if startup.Code != "strict admission support unavailable" ||
		!strings.Contains(startup.Message, "strict diagnostic from autostart helper") ||
		!strings.Contains(startup.StderrTail, "autostart helper startup stderr") {
		t.Fatalf("startup error = %+v, want typed diagnostic and stderr", startup)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("starter calls = %d, want 1", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("state root entries after unsupported autostart = %v, want empty root", names)
	}
}

func TestConnectAutostartChildExitAuthorityRefusedTyped(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	const startTimeout = time.Second
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		return StartResult{
			PID: 12345,
			Wait: func(context.Context) (int, error) {
				return daemonlaunch.ExitAuthorityFailStopped, errors.New("exit status 14")
			},
		}, nil
	})

	start := time.Now()
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		StartTimeout: startTimeout,
		Starter:      starter,
	})
	elapsed := time.Since(start)
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("Connect succeeded, want startup refusal")
	}
	if !errors.Is(err, ErrRootFailStopped) {
		t.Fatalf("Connect error = %T %v, want ErrRootFailStopped", err, err)
	}
	if errors.Is(err, ErrRootSealed) {
		t.Fatalf("Connect error = %T %v, unexpectedly matched ErrRootSealed", err, err)
	}
	var refused *StartupRefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("Connect error = %T %v, want StartupRefusedError", err, err)
	}
	if refused.Reason != protocol.AdmissionRejectRootFailStopped {
		t.Fatalf("StartupRefusedError reason = %q, want %q", refused.Reason, protocol.AdmissionRejectRootFailStopped)
	}
	if elapsed >= startTimeout/2 {
		t.Fatalf("Connect elapsed = %s, want prompt child-exit classification", elapsed)
	}
}

func TestConnectAutostartChildExitGenericNonZeroPrompt(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	const startTimeout = time.Second
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		return StartResult{
			PID: 12345,
			Wait: func(context.Context) (int, error) {
				return 42, errors.New("exit status 42")
			},
		}, nil
	})

	start := time.Now()
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		StartTimeout: startTimeout,
		Starter:      starter,
	})
	elapsed := time.Since(start)
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("Connect succeeded, want startup failure")
	}
	if errors.Is(err, ErrRootFailStopped) || errors.Is(err, ErrRootSealed) {
		t.Fatalf("Connect error = %T %v, want generic startup error", err, err)
	}
	var refused *StartupRefusedError
	if errors.As(err, &refused) {
		t.Fatalf("Connect error = %T %v, unexpectedly matched StartupRefusedError", err, err)
	}
	if !strings.Contains(err.Error(), "exit code 42") {
		t.Fatalf("Connect error = %v, want exit code diagnostic", err)
	}
	if elapsed >= startTimeout/2 {
		t.Fatalf("Connect elapsed = %s, want prompt child-exit failure", elapsed)
	}
}

func TestConnectAutostartNoChildExitKeepsTimeoutBehavior(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	const startTimeout = 180 * time.Millisecond
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		return StartResult{
			PID: 12345,
			Wait: func(ctx context.Context) (int, error) {
				<-ctx.Done()
				return -1, ctx.Err()
			},
		}, nil
	})

	start := time.Now()
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		StartTimeout: startTimeout,
		Starter:      starter,
	})
	elapsed := time.Since(start)
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("Connect succeeded, want timeout")
	}
	if errors.Is(err, ErrRootFailStopped) || errors.Is(err, ErrRootSealed) {
		t.Fatalf("Connect error = %T %v, want generic timeout", err, err)
	}
	var refused *StartupRefusedError
	if errors.As(err, &refused) {
		t.Fatalf("Connect error = %T %v, unexpectedly matched StartupRefusedError", err, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect error = %v, want deadline exceeded", err)
	}
	if elapsed < startTimeout-40*time.Millisecond {
		t.Fatalf("Connect elapsed = %s, want timeout path near %s", elapsed, startTimeout)
	}
}

func TestConnectAutostartRealUnsupportedHostSurfacesLauncherDiagnosticOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("real unsupported-host autostart diagnostic is macOS-only")
	}
	root := shortClientTempDir(t)
	bin := buildClientRealAgentbusBinary(t)

	var starts atomic.Int64
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (StartResult, error) {
		starts.Add(1)
		return (defaultStarter{}).StartDaemon(ctx, opts)
	})
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		Token:        "token",
		CommandPath:  bin,
		StartTimeout: 2 * time.Second,
		Starter:      starter,
	})
	if client != nil {
		_ = client.Close()
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(err, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Connect error = %T %v, want daemon launch startup failure", err, err)
	}
	if errors.Is(err, daemonlaunch.ErrReadinessTimeout) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect error = %v, want launcher failure code, not timeout", err)
	}
	if clientTestBindDeniedOutput(err.Error()) {
		clientTestSkipOrFailBindDenied(t, "real unsupported-host autostart", err)
	}
	if startup.Code != served.ErrAdmissionStrictSupportUnavailable.Error() ||
		!strings.Contains(startup.Message, served.ErrAdmissionStrictSupportUnavailable.Error()) {
		t.Fatalf("startup error = %+v, want strict support diagnostic", startup)
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("starter calls = %d, want 1", got)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("state root entries after unsupported autostart = %v, want empty root", names)
	}
}

func TestConnectAutostartHelloHangUsesSingleStartTimeout(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	const timeout = 200 * time.Millisecond
	listenerReady := make(chan struct{})
	listenerDone := make(chan error, 1)
	stopListener := make(chan struct{})
	var listenerStarted atomic.Bool
	var listenerMu sync.Mutex
	var hungListener net.Listener
	starter := StartFunc(func(context.Context, StartOptions) (StartResult, error) {
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			return StartResult{}, err
		}
		listenerMu.Lock()
		hungListener = listener
		listenerMu.Unlock()
		listenerStarted.Store(true)
		close(listenerReady)
		go func() {
			defer listener.Close()
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					listenerDone <- nil
					return
				}
				listenerDone <- err
				return
			}
			defer conn.Close()
			_, _ = bufio.NewReader(conn).ReadBytes('\n')
			<-stopListener
			listenerDone <- nil
		}()
		return StartResult{ExistingDaemon: true}, nil
	})
	t.Cleanup(func() {
		if !listenerStarted.Load() {
			return
		}
		close(stopListener)
		listenerMu.Lock()
		if hungListener != nil {
			_ = hungListener.Close()
		}
		listenerMu.Unlock()
		select {
		case err := <-listenerDone:
			if err != nil {
				t.Errorf("hung listener exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("hung listener did not stop")
		}
	})
	start := time.Now()
	client, err := Connect(context.Background(), Options{
		StateRoot:    root,
		SocketPath:   socketPath,
		Token:        "hung-token",
		StartTimeout: timeout,
		Starter:      starter,
	})
	elapsed := time.Since(start)
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("Connect succeeded, want hello timeout")
	}
	if clientTestBindDeniedOutput(err.Error()) {
		clientTestSkipOrFailBindDenied(t, "hello-hang listener", err)
	}
	select {
	case <-listenerReady:
	default:
		t.Fatal("starter did not create hung listener")
	}
	if elapsed > 600*time.Millisecond {
		t.Fatalf("Connect elapsed = %s, want within one StartTimeout budget", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "i/o timeout") {
		t.Fatalf("Connect error = %v, want deadline/timeout", err)
	}
}

func TestDefaultStarterDoesNotCancelDaemonAfterStartupContextEnds(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix process signaling test")
	}
	dir := shortClientTempDir(t)
	bin, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBUS_AUTOSTART_DETACH_HELPER", "1")
	t.Setenv("AGENTBUS_AUTOSTART_DETACH_ROOT", dir)
	t.Setenv("AGENTBUS_AUTOSTART_DETACH_TOKEN", "default-starter-token")
	errorPath := filepath.Join(dir, "default-starter-helper-error")
	t.Setenv("AGENTBUS_AUTOSTART_DETACH_ERROR_PATH", errorPath)
	ctx, cancel := context.WithCancel(context.Background())
	started, err := (defaultStarter{}).StartDaemon(ctx, StartOptions{
		StateRoot:   dir,
		SocketPath:  filepath.Join(dir, "agentbus.sock"),
		TokenPath:   filepath.Join(dir, "token"),
		CommandPath: bin,
	})
	if err != nil {
		if helperErr, readErr := os.ReadFile(errorPath); readErr == nil {
			if strings.Contains(string(helperErr), "operation not permitted") {
				clientTestSkipOrFailBindDenied(t, "default-starter detach helper", string(helperErr))
			}
			t.Fatalf("%v; helper error: %s", err, helperErr)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = started.KillAndWait() })
	cancel()
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(started.PID, 0); err != nil {
		t.Fatalf("autostarted daemon exited after startup context cancel: %v", err)
	}
}

func TestAutostartedDaemonSurvivesLauncherProcessGroupTermination(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("Unix process-group test")
	}
	root := shortClientTempDir(t)
	testBinary, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	const token = "detach-token"
	errorPath := filepath.Join(root, "autostart-detach-daemon-error")

	cmd := exec.Command(testBinary, "-test.run=^TestAutostartDetachHelper$")
	cmd.Env = append(os.Environ(),
		"AGENTBUS_AUTOSTART_DETACH_HELPER=1",
		"AGENTBUS_AUTOSTART_DETACH_ROOT="+root,
		"AGENTBUS_AUTOSTART_DETACH_TOKEN="+token,
		"AGENTBUS_AUTOSTART_DETACH_COMMAND="+testBinary,
		"AGENTBUS_AUTOSTART_DETACH_ERROR_PATH="+errorPath,
	)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helperPID := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		if daemonErr, readErr := os.ReadFile(errorPath); readErr == nil && strings.Contains(string(daemonErr), "operation not permitted") {
			clientTestSkipOrFailBindDenied(t, "process-group detach helper", string(daemonErr))
		}
		t.Fatalf("autostart helper failed: %v\n%s", err, output.String())
	}

	pidData, err := os.ReadFile(filepath.Join(root, "agentbus.pid"))
	if err != nil {
		t.Fatal(err)
	}
	daemonPID, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatalf("parse daemon pid %q: %v", pidData, err)
	}
	t.Cleanup(func() {
		if err := syscall.Kill(daemonPID, syscall.SIGTERM); err != nil {
			if !errors.Is(err, syscall.ESRCH) {
				t.Errorf("stop autostarted daemon: %v", err)
			}
			return
		}
		// Verify the daemon actually exits so a detached process can never
		// outlive the suite; escalate to SIGKILL at the deadline.
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := syscall.Kill(daemonPID, 0); errors.Is(err, syscall.ESRCH) {
				return
			}
			if time.Now().After(deadline) {
				_ = syscall.Kill(daemonPID, syscall.SIGKILL)
				t.Errorf("autostarted daemon %d did not exit after SIGTERM; sent SIGKILL", daemonPID)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	if err := syscall.Kill(-helperPID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("terminate helper process group: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(daemonPID, 0); err != nil {
		t.Fatalf("autostarted daemon exited with launcher process group: %v", err)
	}

	client, err := Connect(context.Background(), Options{
		StateRoot:        root,
		Token:            token,
		DisableAutoStart: true,
	})
	if err != nil {
		t.Fatalf("connect to daemon after launcher process-group termination: %v", err)
	}
	defer client.Close()
}

func TestAutostartDetachHelper(t *testing.T) {
	if os.Getenv("AGENTBUS_AUTOSTART_DETACH_HELPER") != "1" {
		t.Skip("autostart detach helper")
	}
	client, err := Connect(context.Background(), Options{
		StateRoot:    os.Getenv("AGENTBUS_AUTOSTART_DETACH_ROOT"),
		Token:        os.Getenv("AGENTBUS_AUTOSTART_DETACH_TOKEN"),
		CommandPath:  os.Getenv("AGENTBUS_AUTOSTART_DETACH_COMMAND"),
		StartTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func startClientTestDaemon(ctx context.Context, root, token string) (<-chan error, error) {
	if root == "" {
		return nil, errors.New("state root is required")
	}
	if token == "" {
		token = "test-token"
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	if err := atomicWrite(filepath.Join(root, protocol.TokenFileName), []byte(token+"\n"), 0o600); err != nil {
		return nil, err
	}
	socketPath := filepath.Join(root, protocol.SocketName)
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					done <- nil
					return
				}
				done <- err
				return
			}
			go serveClientTestConn(conn, token)
		}
	}()
	return done, nil
}

func serveClientTestConn(conn net.Conn, token string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var req protocol.Request
		if err := json.Unmarshal([]byte(strings.TrimSpace(string(line))), &req); err != nil {
			continue
		}
		resp := protocol.Response{JSONRPC: "2.0", ID: req.ID}
		switch req.Method {
		case protocol.MethodHello:
			var params protocol.HelloParams
			if err := json.Unmarshal(req.Params, &params); err != nil {
				resp.Error = protocol.NewError(protocol.ErrorInvalidTaskSpec, "invalid hello params", protocol.ErrorData{})
				break
			}
			if params.Token != token {
				resp.Error = protocol.NewError(protocol.ErrorUnauthorized, "unauthorized", protocol.ErrorData{})
				break
			}
			if params.ClientProtocolVersion != protocol.Version {
				resp.Error = protocol.NewError(
					protocol.ErrorVersionMismatch,
					"protocol version mismatch",
					protocol.ErrorData{ServerProtocolVersion: protocol.Version},
				)
				break
			}
			resp.Result = protocol.HelloResult{
				ProtocolVersion: protocol.Version,
				Backends:        []string{},
				Capabilities:    protocol.DefaultCapabilities(),
			}
		default:
			resp.Error = protocol.NewError(protocol.ErrorMethodNotFound, "method not found", protocol.ErrorData{})
		}
		if err := encoder.Encode(resp); err != nil {
			return
		}
	}
}

func shortClientTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "ab-client-")
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

func buildClientRealAgentbusBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agentbus")
	cmd := exec.Command("go", "build", "-o", path, "./cmd/agentbus")
	cmd.Dir = clientRepoRootFromCaller(t)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build ./cmd/agentbus: %v\n%s", err, output)
	}
	return path
}

func clientRepoRootFromCaller(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), ".."))
}
