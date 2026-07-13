package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/served"
)

func TestClientHelloParsesBackendMetadata(t *testing.T) {
	hello := runClientHello(t, `{"protocolVersion":1,"backends":["codex"],"backendMetadata":[{"backend":"codex","models":["gpt-5"],"efforts":["high"]}],"capabilities":{"models.discovery":true}}`)

	if hello.ProtocolVersion != 1 || len(hello.Backends) != 1 || hello.Backends[0] != "codex" || !hello.Capabilities["models.discovery"] {
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

func TestClientHelloParsesOldServerWithoutBackendMetadata(t *testing.T) {
	hello := runClientHello(t, `{"protocolVersion":1,"backends":["codex"],"capabilities":{"models.discovery":false}}`)

	if hello.ProtocolVersion != 1 || len(hello.Backends) != 1 || hello.Backends[0] != "codex" {
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
		if c.HelloResult().ProtocolVersion != 1 {
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
	if conn, err := net.DialTimeout("unix", socketPath, 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("dial to closed socket unexpectedly succeeded")
	} else if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Fatalf("dial to closed socket error = %v, want connection refused", err)
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
