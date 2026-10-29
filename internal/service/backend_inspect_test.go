//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type inspectTestBackend struct {
	name string
	path string
}

func (backend inspectTestBackend) Name() string { return backend.name }

func (backend inspectTestBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: backend.name}, nil
}

func (backend inspectTestBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used")
}

func (backend inspectTestBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used")
}

func (backend inspectTestBackend) BackendInspection() engine.BackendInspection {
	return engine.BackendInspection{Executable: backend.path}
}

func TestBackendInspectIsolatesVersionFailureAndCachesSuccessfulProbe(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "version-count")
	goodPath := filepath.Join(dir, "good-backend")
	goodScript := "#!/bin/sh\nprintf x >> " + shellQuote(marker) + "\nprintf 'good-backend 7.8.9\\n'\n"
	if err := os.WriteFile(goodPath, []byte(goodScript), 0o700); err != nil {
		t.Fatal(err)
	}
	badPath := filepath.Join(dir, "bad-backend")
	if err := os.WriteFile(badPath, []byte("#!/bin/sh\nexit 7\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(goodPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(badPath, 0o700); err != nil {
		t.Fatal(err)
	}
	server := newTestServer(t, t.TempDir(), Config{
		Backends: []engine.Backend{
			inspectTestBackend{name: "z-good", path: goodPath},
			inspectTestBackend{name: "a-bad", path: badPath},
		},
		BackendDefaults: map[string]BackendDefaults{
			"z-good": {Model: "configured-model", Effort: "high"},
		},
	})

	inspect := func() protocol.BackendInspectResult {
		t.Helper()
		outcome := server.handleBackendInspect(json.RawMessage(`{}`))
		if outcome.err != nil {
			t.Fatalf("backend.inspect error = %#v", outcome.err)
		}
		result, ok := outcome.result.(protocol.BackendInspectResult)
		if !ok {
			t.Fatalf("backend.inspect result = %T", outcome.result)
		}
		return result
	}
	first := inspect()
	second := inspect()
	if len(first.Backends) != 2 || first.Backends[0].Backend != "a-bad" || first.Backends[1].Backend != "z-good" {
		t.Fatalf("backend entries = %#v, want sorted per-backend entries", first.Backends)
	}
	resolvedBadPath, err := filepath.EvalSymlinks(badPath)
	if err != nil {
		t.Fatal(err)
	}
	if first.Backends[0].ExecutablePath != resolvedBadPath || !strings.Contains(first.Backends[0].Error, "--version failed") {
		t.Fatalf("failed executable entry = %#v", first.Backends[0])
	}
	good := first.Backends[1]
	resolvedGoodPath, err := filepath.EvalSymlinks(goodPath)
	if err != nil {
		t.Fatal(err)
	}
	if good.ExecutablePath != resolvedGoodPath || good.Version != "good-backend 7.8.9" || good.Error != "" || good.DefaultModel != "configured-model" || good.DefaultEffort != "high" {
		t.Fatalf("successful executable entry = %#v", good)
	}
	if second.Backends[1].Version != good.Version {
		t.Fatalf("cached version = %q, want %q", second.Backends[1].Version, good.Version)
	}
	count, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(count) != "x" {
		t.Fatalf("version probe count marker = %q, want one invocation", count)
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
