package service

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const localIPCSCCacheStartupSocketSuffix = "/sccacheXXXXXX/sock"

// localIPCTempRoot is empty in production, so MkdirTemp uses the daemon's
// operating-system temp directory. Tests set it to exercise path-length cases.
var localIPCTempRoot string

func createLocalIPCRoot() (string, error) {
	created, err := os.MkdirTemp(localIPCTempRoot, "agentbus-ipc-")
	if err != nil {
		return "", fmt.Errorf("create private local IPC temp directory: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(created)
	if err != nil {
		resolveErr := fmt.Errorf("resolve private local IPC temp directory: %w", err)
		if cleanupErr := removeLocalIPCRoot(created); cleanupErr != nil {
			return "", errors.Join(resolveErr, cleanupErr)
		}
		return "", resolveErr
	}
	if err := validateLocalIPCSocketPath(resolved, localIPCGOOS); err != nil {
		_ = removeLocalIPCRoot(resolved)
		return "", err
	}
	return resolved, nil
}

func validateLocalIPCSocketPath(root, goos string) error {
	limit := 107
	if goos == "darwin" {
		limit = 103
	}
	length := len(root) + len(localIPCSCCacheStartupSocketSuffix)
	if length > limit {
		return fmt.Errorf("local IPC sccache startup socket path length is %d bytes; limit is %d bytes", length, limit)
	}
	return nil
}

func localIPCDenyPaths(stateRoot, workspace, scratch, ipcRoot string) []string {
	home, _ := os.UserHomeDir()
	return buildLocalIPCDenyPaths(stateRoot, workspace, scratch, ipcRoot, os.Getenv("SSH_AUTH_SOCK"), home, os.Getuid())
}

func buildLocalIPCDenyPaths(stateRoot, workspace, scratch, ipcRoot, sshAuthSock, home string, uid int) []string {
	protected := make([]string, 0, 3)
	for _, path := range []string{workspace, scratch, ipcRoot} {
		if resolved, err := resolveExistingPath(path); err == nil {
			protected = append(protected, resolved)
		} else if absolute, absErr := filepath.Abs(path); absErr == nil {
			protected = append(protected, filepath.Clean(absolute))
		}
	}
	resolvedHome := ""
	if home != "" {
		if resolved, err := resolveExistingPath(home); err == nil {
			resolvedHome = resolved
		} else if absolute, absErr := filepath.Abs(home); absErr == nil {
			resolvedHome = filepath.Clean(absolute)
		}
	}

	candidates := []string{
		filepath.Join("/run/user", strconv.Itoa(uid)),
		"/run/docker.sock",
		"/var/run/docker.sock",
		filepath.Join(stateRoot, protocol.SocketName),
		filepath.Join(stateRoot, protocol.TokenFileName),
	}
	if sshAuthSock != "" {
		sshDir := filepath.Dir(sshAuthSock)
		resolvedDir, err := resolveExistingPath(sshDir)
		if err == nil && (resolvedDir == string(filepath.Separator) || resolvedDir == "/tmp" || (resolvedHome != "" && resolvedDir == resolvedHome) || isSameOrAncestorOfAny(resolvedDir, protected)) {
			candidates = append(candidates, sshAuthSock)
		} else {
			candidates = append(candidates, sshDir)
		}
	}

	resolved := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		path, err := resolveExistingPath(candidate)
		if err != nil || isSameOrAncestorOfAny(path, protected) {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		resolved = append(resolved, path)
	}
	return resolved
}

func resolveExistingPath(path string) (string, error) {
	if path == "" {
		return "", os.ErrNotExist
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func isSameOrAncestorOfAny(candidate string, paths []string) bool {
	for _, path := range paths {
		relative, err := filepath.Rel(candidate, path)
		if err == nil && (relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative))) {
			return true
		}
	}
	return false
}

func removeLocalIPCRoot(path string) error {
	if path == "" {
		return nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve local IPC temp directory for cleanup: %w", err)
	}
	info, err := os.Lstat(resolved)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect local IPC temp directory for cleanup: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !strings.HasPrefix(filepath.Base(resolved), "agentbus-ipc-") {
		return fmt.Errorf("refusing to remove unexpected local IPC temp path %q", resolved)
	}
	if err := os.RemoveAll(resolved); err != nil {
		return fmt.Errorf("remove local IPC temp directory: %w", err)
	}
	return nil
}

func (s *Server) rememberManagedLocalIPCRoot(jobID, root string) {
	if s == nil || jobID == "" || root == "" {
		return
	}
	s.codexHomesMu.Lock()
	if s.managedLocalIPCRoots == nil {
		s.managedLocalIPCRoots = make(map[string]string)
	}
	previous := s.managedLocalIPCRoots[jobID]
	s.managedLocalIPCRoots[jobID] = root
	s.codexHomesMu.Unlock()
	if previous != "" && previous != root {
		if err := removeLocalIPCRoot(previous); err != nil {
			logLocalIPCCleanup(jobID, previous, err)
		}
	}
}

func (s *Server) takeManagedLocalIPCRoot(jobID string) string {
	if s == nil || jobID == "" {
		return ""
	}
	s.codexHomesMu.Lock()
	root := s.managedLocalIPCRoots[jobID]
	delete(s.managedLocalIPCRoots, jobID)
	s.codexHomesMu.Unlock()
	return root
}

func (s *Server) discardManagedLocalIPCRoot(jobID string, recordPath string) {
	root := s.takeManagedLocalIPCRoot(jobID)
	if root == "" {
		root = recordPath
	}
	if root == "" {
		return
	}
	if err := removeLocalIPCRoot(root); err != nil {
		logLocalIPCCleanup(jobID, root, err)
	}
}

func recordLocalIPCRoot(record jobstore.Record) string {
	if record.Execution == nil || (record.Execution.LocalIPC != "private-dir" && record.Execution.LocalIPC != "all-unix-sockets") {
		return ""
	}
	return record.Execution.Caches["TMPDIR"]
}

func (s *Server) discardRecordedLocalIPCRoot(record jobstore.Record) {
	s.discardManagedLocalIPCRoot(record.JobID, recordLocalIPCRoot(record))
}

func logLocalIPCCleanup(jobID, root string, err error) {
	log.Printf("agentbus service: job %s local IPC temp cleanup %s: %v", jobID, root, err)
}
