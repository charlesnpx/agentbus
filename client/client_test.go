package client

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
