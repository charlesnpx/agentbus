package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
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

	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/served"
)

func TestMain(m *testing.M) {
	if os.Getenv("AGENTBUS_AUTOSTART_DETACH_HELPER") == "1" &&
		len(os.Args) == 3 && os.Args[1] == "serve" && os.Args[2] == "--foreground" {
		os.Exit(runAutostartDetachDaemon())
	}
	os.Exit(m.Run())
}

func runAutostartDetachDaemon() int {
	server, err := served.New(served.Config{
		StateRoot:   os.Getenv("AGENTBUS_STATE_ROOT"),
		Token:       os.Getenv("AGENTBUS_AUTOSTART_DETACH_TOKEN"),
		IdleTimeout: -1,
	})
	if err != nil {
		recordAutostartDetachDaemonError(err)
		return 1
	}
	if err := server.Serve(context.Background()); err != nil {
		recordAutostartDetachDaemonError(err)
		return 1
	}
	return 0
}

func recordAutostartDetachDaemonError(err error) {
	if path := os.Getenv("AGENTBUS_AUTOSTART_DETACH_ERROR_PATH"); path != "" {
		_ = os.WriteFile(path, []byte(err.Error()), 0o600)
	}
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

func runClientHello(t *testing.T, result string) HelloResult {
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
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	return hello
}

func TestAutostartRaceStartsOneDaemon(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	serverCtx, cancelServer := context.WithCancel(context.Background())
	defer cancelServer()
	var starts atomic.Int64
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (int, error) {
		starts.Add(1)
		server, err := served.New(served.Config{
			StateRoot:   root,
			Token:       "race-token",
			IdleTimeout: -1,
		})
		if err != nil {
			return 0, err
		}
		errCh := make(chan error, 1)
		go func() { errCh <- server.Serve(serverCtx) }()
		select {
		case err := <-errCh:
			return 0, err
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return os.Getpid(), nil
		}
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
		if strings.Contains(err.Error(), "bind: operation not permitted") ||
			strings.Contains(err.Error(), "connect: no such file") {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
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

func TestAutostartReplacesRefusedSocket(t *testing.T) {
	t.Parallel()
	root := shortClientTempDir(t)
	socketPath := filepath.Join(root, "agentbus.sock")
	stale, err := net.Listen("unix", socketPath)
	if err != nil {
		if strings.Contains(err.Error(), "bind: operation not permitted") {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
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
	starter := StartFunc(func(ctx context.Context, opts StartOptions) (int, error) {
		starts.Add(1)
		server, err := served.New(served.Config{
			StateRoot:   root,
			Token:       "refused-token",
			IdleTimeout: -1,
		})
		if err != nil {
			return 0, err
		}
		go func() { serverDone <- server.Serve(serverCtx) }()
		select {
		case err := <-serverDone:
			return 0, err
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return os.Getpid(), nil
		}
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

func TestDefaultStarterDoesNotCancelDaemonAfterStartupContextEnds(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("Unix process signaling test")
	}
	dir := shortClientTempDir(t)
	bin := filepath.Join(dir, "agentbus")
	script := "#!/bin/sh\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	pid, err := (defaultStarter{}).StartDaemon(ctx, StartOptions{
		StateRoot:   dir,
		SocketPath:  filepath.Join(dir, "agentbus.sock"),
		TokenPath:   filepath.Join(dir, "token"),
		CommandPath: bin,
	})
	if err != nil {
		t.Fatal(err)
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = proc.Kill()
		_, _ = proc.Wait()
	})
	cancel()
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(pid, 0); err != nil {
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
			t.Skipf("Unix socket bind denied by sandbox: %s", daemonErr)
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
