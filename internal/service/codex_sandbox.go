//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const codexSandboxProbeTimeout = 10 * time.Second

type codexSandboxCacheKey struct {
	path    string
	version string
}

type codexSandboxCacheEntry struct {
	err string
}

func (s *Server) ensureCodexSandbox(ctx context.Context, backend engine.Backend) error {
	configured := "codex"
	if provider, ok := backend.(engine.BackendInspectionProvider); ok {
		if executable := strings.TrimSpace(provider.BackendInspection().Executable); executable != "" {
			configured = executable
		}
	}
	path, err := resolveBackendExecutable(configured)
	if err != nil {
		return fmt.Errorf("resolve Codex executable: %w", err)
	}
	version, versionErr := s.backendVersion(path)
	if versionErr != "" {
		return fmt.Errorf("read Codex version: %s", versionErr)
	}
	key := codexSandboxCacheKey{path: path, version: version}

	s.codexSandboxMu.Lock()
	defer s.codexSandboxMu.Unlock()
	if s.codexSandboxResults == nil {
		s.codexSandboxResults = make(map[codexSandboxCacheKey]codexSandboxCacheEntry)
	}
	if cached, ok := s.codexSandboxResults[key]; ok {
		if cached.err != "" {
			return errors.New(cached.err)
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	probeCtx, cancel := context.WithTimeout(ctx, codexSandboxProbeTimeout)
	defer cancel()
	probe := s.codexSandboxProbe
	if probe == nil {
		probe = runCodexSandboxProbe
	}
	probeErr := probe(probeCtx, path)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	entry := codexSandboxCacheEntry{}
	if probeErr != nil {
		entry.err = boundInspectionText(probeErr.Error())
		if entry.err == "" {
			entry.err = "sandbox probe failed"
		}
	}
	s.codexSandboxResults[key] = entry
	if entry.err != "" {
		return errors.New(entry.err)
	}
	return nil
}

func runCodexSandboxProbe(ctx context.Context, binary string) error {
	cmd := exec.CommandContext(ctx, binary, "sandbox", "--", "true")
	cmd.WaitDelay = 500 * time.Millisecond
	cmd.Stdout = io.Discard
	stderr := &boundedOutput{limit: backendVersionOutputCap}
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		diagnostic := strings.TrimSpace(stderr.String())
		if diagnostic == "" {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				diagnostic = "sandbox probe timed out"
			} else {
				diagnostic = err.Error()
			}
		}
		return errors.New(boundInspectionText(diagnostic))
	}
	return nil
}
