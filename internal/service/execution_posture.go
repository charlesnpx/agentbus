//go:build darwin || linux

package service

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

const moduleCacheResolveTimeout = 2 * time.Second

// resolveModuleCache records the daemon's effective Go module cache once at
// startup. If the daemon did not set GOMODCACHE, ask Go directly with a short
// bound; an unavailable or stalled toolchain leaves the value unset.
func resolveModuleCache() string {
	if cache := strings.TrimSpace(os.Getenv("GOMODCACHE")); cache != "" {
		return cache
	}

	ctx, cancel := context.WithTimeout(context.Background(), moduleCacheResolveTimeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, "go", "env", "GOMODCACHE").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func executionPosture(backend string, write, verify, localIPC, codexSandboxed bool, scratch string, caches map[string]string) protocol.ExecutionPosture {
	intent := "inspect"
	if verify {
		intent = "verify"
	} else if write {
		intent = "edit"
	}

	posture := protocol.ExecutionPosture{
		Intent:         intent,
		SourceWritable: write,
		Scratch:        scratch,
		Caches:         caches,
		Shell:          "unenforced",
		Network:        "unenforced",
		LocalIPC:       "unenforced",
		GitMetadata:    "unenforced",
		Enforcement:    "none",
	}
	switch backend {
	case "codex":
		if codexSandboxed {
			posture.Enforcement = "os-sandbox"
			posture.Shell = "sandboxed"
			posture.Network = "denied"
			posture.LocalIPC = "denied"
			if localIPC {
				posture.LocalIPC = "private-dir"
			}
			posture.GitMetadata = "read-only"
			if verify {
				posture.GitMetadata = "none"
			}
		} else {
			posture.Network = "unenforced"
			posture.LocalIPC = "unenforced"
			posture.GitMetadata = "writable"
		}
	case "cursor":
		posture.Enforcement = "agent-permissions"
		if write {
			posture.GitMetadata = "writable"
		} else {
			posture.GitMetadata = "read-only"
		}
	case "claude":
		if write {
			posture.GitMetadata = "writable"
		} else {
			posture.Shell = "restricted"
		}
	}
	return posture
}
