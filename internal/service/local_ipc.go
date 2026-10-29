package service

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/internal/jobstore"
)

const localIPCRootMaxBytes = 80

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
	if len(resolved) > localIPCRootMaxBytes {
		_ = removeLocalIPCRoot(resolved)
		return "", fmt.Errorf("local IPC socket directory path is %d bytes; maximum is %d bytes", len(resolved), localIPCRootMaxBytes)
	}
	return resolved, nil
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
	if record.Execution == nil || record.Execution.LocalIPC != "private-dir" {
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
