package served

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
)

var codexHomeLinkedFiles = []string{"auth.json", "config.toml"}

// prepareCodexHome creates the private CODEX_HOME for one accepted Codex job.
// Credentials remain in the operator home: only the two files Codex requires
// for authentication and configuration are linked into the private directory.
func (s *Server) prepareCodexHome(layout engine.WorkspaceLayout, jobID string) (path string, managed bool, err error) {
	if s == nil || s.codexHomeInherit {
		return "", false, nil
	}
	if jobID == "" || filepath.Base(jobID) != jobID || jobID == "." || jobID == ".." {
		return "", false, fmt.Errorf("invalid Codex job directory name")
	}
	if s.codexHomeOverride != "" {
		path = strings.TrimSpace(s.codexHomeOverride)
		if !filepath.IsAbs(path) {
			return "", false, fmt.Errorf("AGENTBUS_CODEX_HOME must be an absolute path")
		}
		path = filepath.Clean(path)
	} else {
		path = filepath.Join(layout.Codex, jobID)
		managed = true
	}

	if err := os.MkdirAll(path, 0o700); err != nil {
		return "", false, fmt.Errorf("create Codex home: %w", err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return "", false, fmt.Errorf("secure Codex home: %w", err)
	}
	if managed {
		if err := os.Chmod(layout.Codex, 0o700); err != nil {
			return "", false, fmt.Errorf("secure Codex home root: %w", err)
		}
	}

	sourceHome, err := resolveCodexAuthHome(s.codexAuthHome)
	if err != nil {
		return "", false, err
	}
	for _, name := range codexHomeLinkedFiles {
		if err := linkCodexHomeFile(sourceHome, path, name); err != nil {
			return "", false, err
		}
	}
	return path, managed, nil
}

func resolveCodexAuthHome(configured string) (string, error) {
	home := strings.TrimSpace(configured)
	if home == "" {
		home = strings.TrimSpace(os.Getenv("CODEX_HOME"))
	}
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve operator Codex home: %w", err)
		}
		home = filepath.Join(userHome, ".codex")
	}
	abs, err := filepath.Abs(home)
	if err != nil {
		return "", fmt.Errorf("resolve operator Codex home: %w", err)
	}
	return filepath.Clean(abs), nil
}

func linkCodexHomeFile(sourceHome, destinationHome, name string) error {
	if filepath.Clean(sourceHome) == filepath.Clean(destinationHome) {
		// A fixed override may intentionally be the operator home itself. It
		// already contains its own auth/config; avoid rejecting it or creating
		// a self-referential link.
		return nil
	}
	source := filepath.Join(sourceHome, name)
	info, err := os.Stat(source)
	if errors.Is(err, os.ErrNotExist) {
		// Codex supports homes without either file (for example, before login).
		// Do not leave a broken credential/configuration link behind.
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat Codex %s: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("Codex %s is not a regular file", name)
	}

	destination := filepath.Join(destinationHome, name)
	if existing, err := os.Lstat(destination); err == nil {
		if existing.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(destination)
			if readErr == nil && target == source {
				return nil
			}
		}
		return fmt.Errorf("Codex home %s already exists", name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect Codex home %s: %w", name, err)
	}
	if err := os.Symlink(source, destination); err != nil {
		return fmt.Errorf("link Codex %s: %w", name, err)
	}
	return nil
}

func (s *Server) cleanupManagedCodexHome(path string, managed bool, state engine.JobState) {
	if !managed || path == "" || !codexHomeCleanupEligible(state) {
		return
	}
	if err := os.RemoveAll(path); err != nil {
		log.Printf("agentbus daemon: remove completed Codex home %s: %v", path, err)
	}
}

func codexHomeCleanupEligible(state engine.JobState) bool {
	return state == engine.StateCompleted || state == engine.StateCompletedNoncompliant
}
