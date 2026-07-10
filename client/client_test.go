package client

import (
	"context"
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
