//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	backendVersionProbeTimeout = 2 * time.Second
	backendVersionOutputCap    = 4096
	backendVersionStringCap    = 256
)

type backendVersionCacheKey struct {
	path  string
	mtime int64
}

type backendVersionCacheEntry struct {
	version string
	err     string
}

// handleBackendInspect returns local executable information without touching
// provider accounts, billing, or model catalogues.
func (s *Server) handleBackendInspect(raw json.RawMessage) requestOutcome {
	var params protocol.BackendInspectParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}

	result := protocol.BackendInspectResult{Backends: make([]protocol.BackendInspectionWire, 0, len(s.backends))}
	for _, name := range s.backendNames() {
		entry := protocol.BackendInspectionWire{Backend: name}
		defaults := s.backendDefaults[name]
		entry.DefaultModel = defaults.Model
		entry.DefaultEffort = defaults.Effort
		backend := s.backends[name]
		configuredPath := name
		if provider, ok := backend.(engine.BackendInspectionProvider); ok {
			if configured := strings.TrimSpace(provider.BackendInspection().Executable); configured != "" {
				configuredPath = configured
			}
		}
		path, err := resolveBackendExecutable(configuredPath)
		if err != nil {
			entry.Error = boundInspectionText(err.Error())
			result.Backends = append(result.Backends, entry)
			continue
		}
		entry.ExecutablePath = path
		version, inspectErr := s.backendVersion(path)
		entry.Version = version
		if inspectErr != "" {
			entry.Error = inspectErr
		}
		result.Backends = append(result.Backends, entry)
	}
	return requestOutcome{result: result}
}

func resolveBackendExecutable(configured string) (string, error) {
	if strings.TrimSpace(configured) == "" {
		return "", errors.New("configured executable is empty")
	}
	path, err := exec.LookPath(configured)
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("make executable path absolute: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("resolve executable symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect executable: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("configured executable is not a regular file")
	}
	return resolved, nil
}

func (s *Server) backendVersion(path string) (string, string) {
	info, err := os.Stat(path)
	if err != nil {
		return "", boundInspectionText("stat executable: " + err.Error())
	}
	key := backendVersionCacheKey{path: path, mtime: info.ModTime().UnixNano()}
	s.backendVersionMu.Lock()
	defer s.backendVersionMu.Unlock()
	if s.backendVersions == nil {
		s.backendVersions = make(map[backendVersionCacheKey]backendVersionCacheEntry)
	}
	if cached, ok := s.backendVersions[key]; ok {
		return cached.version, cached.err
	}
	entry := backendVersionCacheEntry{}
	ctx, cancel := context.WithTimeout(context.Background(), backendVersionProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "--version")
	stdout := &boundedOutput{limit: backendVersionOutputCap}
	stderr := &boundedOutput{limit: backendVersionOutputCap}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	runErr := cmd.Run()
	if runErr != nil {
		entry.err = "--version failed: " + runErr.Error()
		if ctx.Err() != nil {
			entry.err = "--version timed out"
		}
	} else {
		value := strings.TrimSpace(stdout.String())
		if value == "" {
			value = strings.TrimSpace(stderr.String())
		}
		if value == "" {
			entry.err = "--version returned no version text"
		} else {
			entry.version = boundInspectionText(firstLine(value))
		}
	}
	entry.err = boundInspectionText(entry.err)
	s.backendVersions[key] = entry
	return entry.version, entry.err
}

func firstLine(value string) string {
	if index := strings.IndexAny(value, "\r\n"); index >= 0 {
		return value[:index]
	}
	return value
}

func boundInspectionText(value string) string {
	value = strings.TrimSpace(value)
	if !utf8.ValidString(value) {
		value = strings.ToValidUTF8(value, "�")
	}
	runes := []rune(value)
	if len(runes) > backendVersionStringCap {
		return string(runes[:backendVersionStringCap-3]) + "..."
	}
	return value
}

type boundedOutput struct {
	bytes.Buffer
	limit int
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	count := len(value)
	remaining := output.limit - output.Len()
	if remaining > 0 {
		if remaining > count {
			remaining = count
		}
		_, _ = output.Buffer.Write(value[:remaining])
	}
	return count, nil
}
