package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// SetupProbeCachePath returns the protocol state path for setup probe cache.
func SetupProbeCachePath(root string) (string, error) {
	if root == "" {
		var err error
		root, err = ResolveStateRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(root, "setup-probes.json"), nil
}

// WriteSetupProbeCache writes the setup probe cache with protocol state permissions.
func WriteSetupProbeCache(path string, cache SetupProbeCache) error {
	if path == "" {
		var err error
		path, err = SetupProbeCachePath("")
		if err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return atomicWriteFile(path, b, 0o600)
}

// ReadSetupProbeCache reads the setup probe cache.
func ReadSetupProbeCache(path string) (SetupProbeCache, error) {
	if path == "" {
		var err error
		path, err = SetupProbeCachePath("")
		if err != nil {
			return SetupProbeCache{}, err
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return SetupProbeCache{}, err
	}
	var cache SetupProbeCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		return SetupProbeCache{}, err
	}
	return cache, nil
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
	Root       string
	Workspace  string
	Key        string
	Namespace  string
	Jobs       string
	Logs       string
	Results    string
	Inputs     string
	Quarantine string
}

const workspaceManifestFile = "workspace.json"

// LayoutForWorkspace resolves and describes the state layout for cwd.
func LayoutForWorkspace(root, cwd string) (WorkspaceLayout, error) {
	canon, err := CanonicalWorkspace(cwd)
	if err != nil {
		return WorkspaceLayout{}, err
	}
	key := WorkspaceKey(canon)
	return layoutForWorkspaceKey(root, key, canon)
}

func layoutForWorkspaceKey(root, key, workspace string) (WorkspaceLayout, error) {
	if err := validateWorkspaceKey(key); err != nil {
		return WorkspaceLayout{}, err
	}
	ns := filepath.Join(root, "workspaces", key)
	return WorkspaceLayout{
		Root:       root,
		Workspace:  workspace,
		Key:        key,
		Namespace:  ns,
		Jobs:       filepath.Join(ns, "jobs"),
		Logs:       filepath.Join(ns, "logs"),
		Results:    filepath.Join(ns, "results"),
		Inputs:     filepath.Join(ns, "inputs"),
		Quarantine: filepath.Join(ns, "quarantine"),
	}, nil
}

func validateWorkspaceKey(key string) error {
	if len(key) != sha256.Size*2 {
		return errors.New("invalid workspace key")
	}
	for _, r := range key {
		if r >= '0' && r <= '9' || r >= 'a' && r <= 'f' {
			continue
		}
		return errors.New("invalid workspace key")
	}
	return nil
}

func ensureLayout(layout WorkspaceLayout) error {
	for _, dir := range []string{layout.Root, filepath.Join(layout.Root, "workspaces"), layout.Namespace, layout.Jobs, layout.Logs, layout.Results, layout.Inputs, layout.Quarantine} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
	}
	return nil
}
