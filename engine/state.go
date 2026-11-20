package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// ResolveStateRoot returns the agentbus state root using the protocol-defined
// XDG fallback: $XDG_STATE_HOME/agentbus/v2 or ~/.local/state/agentbus/v2.
func ResolveStateRoot() (string, error) {
	if root := os.Getenv("AGENTBUS_STATE_ROOT"); root != "" {
		return root, nil
	}
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "agentbus", "v2"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if home == "" {
		return "", errors.New("home directory is empty")
	}
	return filepath.Join(home, ".local", "state", "agentbus", "v2"), nil
}

// CanonicalWorkspace returns an absolute workspace path with symlinks resolved.
func CanonicalWorkspace(cwd string) (string, error) {
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// WorkspaceKey returns the full 64-hex SHA-256 namespace key for canonicalCWD.
func WorkspaceKey(canonicalCWD string) string {
	sum := sha256.Sum256([]byte(canonicalCWD))
	return hex.EncodeToString(sum[:])
}

// WorkspaceLayout names the directories used by artifact and owned-home helpers.
type WorkspaceLayout struct {
	Root      string
	Namespace string
	Logs      string
	Results   string
	Codex     string
}
