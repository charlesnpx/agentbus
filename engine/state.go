package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

// ResolveStateRoot returns the agentbus state root using the protocol-defined
// XDG fallback: $XDG_STATE_HOME/agentbus or ~/.local/state/agentbus.
func ResolveStateRoot() (string, error) {
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "agentbus"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if home == "" {
		return "", errors.New("home directory is empty")
	}
	return filepath.Join(home, ".local", "state", "agentbus"), nil
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

// WorkspaceLayout contains protocol state paths for one workspace namespace.
type WorkspaceLayout struct {
	Root      string
	Workspace string
	Key       string
	Namespace string
	Jobs      string
	Logs      string
	Results   string
	Inputs    string
	// Codex contains per-job CODEX_HOME directories. It is created only for
	// Codex workspaces, which do not all need it.
	Codex      string
	Quarantine string
}

// LayoutForWorkspace resolves and describes the state layout for cwd.
func LayoutForWorkspace(root, cwd string) (WorkspaceLayout, error) {
	canon, err := CanonicalWorkspace(cwd)
	if err != nil {
		return WorkspaceLayout{}, err
	}
	key := WorkspaceKey(canon)
	ns := filepath.Join(root, "workspaces", key)
	return WorkspaceLayout{
		Root:       root,
		Workspace:  canon,
		Key:        key,
		Namespace:  ns,
		Jobs:       filepath.Join(ns, "jobs"),
		Logs:       filepath.Join(ns, "logs"),
		Results:    filepath.Join(ns, "results"),
		Inputs:     filepath.Join(ns, "inputs"),
		Codex:      filepath.Join(ns, "codex"),
		Quarantine: filepath.Join(ns, "quarantine"),
	}, nil
}
