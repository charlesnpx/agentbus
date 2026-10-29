//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestBackendInspectBoundsVersionProbeWithInheritedOutputPipe(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lingering-backend")
	childPIDPath := filepath.Join(dir, "child-pid")
	script := "#!/bin/sh\nsleep 60 &\necho $! > " + shellQuote(childPIDPath) + "\nprintf 'lingering-backend 1.0\\n'\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	killChild := func() {
		pidText, err := os.ReadFile(childPIDPath)
		if err != nil {
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
		if err != nil {
			return
		}
		child, err := os.FindProcess(pid)
		if err == nil {
			_ = child.Kill()
		}
	}
	t.Cleanup(killChild)

	server := newTestServer(t, t.TempDir(), Config{
		Backends: []engine.Backend{inspectTestBackend{name: "lingering", path: path}},
	})
	type inspectionResult struct {
		result protocol.BackendInspectResult
		err    *protocol.ErrorObject
	}
	done := make(chan inspectionResult, 1)
	started := time.Now()
	go func() {
		outcome := server.handleBackendInspect(json.RawMessage(`{}`))
		result, _ := outcome.result.(protocol.BackendInspectResult)
		done <- inspectionResult{result: result, err: outcome.err}
	}()

	select {
	case inspected := <-done:
		elapsed := time.Since(started)
		if inspected.err != nil {
			t.Fatalf("backend.inspect error = %#v", inspected.err)
		}
		if elapsed > 4*time.Second {
			t.Fatalf("backend.inspect took %s, want return within 4s", elapsed)
		}
		if len(inspected.result.Backends) != 1 || inspected.result.Backends[0].Error == "" {
			t.Fatalf("backend entries = %#v, want one version-probe error entry", inspected.result.Backends)
		}
	case <-time.After(4 * time.Second):
		killChild()
		<-done
		t.Fatal("backend.inspect did not return within 4s while a child held stdout open")
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
