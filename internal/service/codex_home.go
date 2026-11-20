//go:build darwin || linux

package service

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

var codexHomeLinkedFiles = []string{"auth.json", "config.toml"}

// managedCodexHome pins the private conversation directory and its parent so
// cleanup can verify the directory identity before removing it.
type managedCodexHome struct {
	path        string
	name        string
	parentFD    int
	directoryFD int
	dev         uint64
	ino         uint64
}

func (home *managedCodexHome) close() {
	if home == nil {
		return
	}
	if home.directoryFD >= 0 {
		if err := unix.Close(home.directoryFD); err != nil {
			log.Printf("agentbus daemon: close managed Codex home %s: %v", home.path, err)
		}
		home.directoryFD = -1
	}
	if home.parentFD >= 0 {
		if err := unix.Close(home.parentFD); err != nil {
			log.Printf("agentbus daemon: close managed Codex home parent %s: %v", home.path, err)
		}
		home.parentFD = -1
	}
}

func (home *managedCodexHome) releaseDirectoryFD() {
	if home == nil || home.directoryFD < 0 {
		return
	}
	if err := unix.Close(home.directoryFD); err != nil {
		log.Printf("agentbus daemon: close managed Codex home %s before removal: %v", home.path, err)
	}
	home.directoryFD = -1
}

func (home *managedCodexHome) matches(stat unix.Stat_t) bool {
	return home != nil && home.dev == uint64(stat.Dev) && home.ino == uint64(stat.Ino)
}

// prepareCodexHome creates the private CODEX_HOME for one accepted Codex job.
// Credentials remain in the operator home: only the two files Codex requires
// for authentication and configuration are linked into the private directory.
//
// Workers run as the operator OS identity. They can therefore read the linked
// credential just as they could when CODEX_HOME was inherited. Per-job homes
// provide session hygiene, not credential confidentiality; hostile workers are
// explicitly outside this feature's threat model.
func (s *Server) prepareCodexHome(layout engine.WorkspaceLayout, jobID string) (path string, managed *managedCodexHome, err error) {
	if s == nil {
		return "", nil, errors.New("nil service server")
	}
	if jobID == "" || filepath.Base(jobID) != jobID || jobID == "." || jobID == ".." {
		return "", nil, fmt.Errorf("invalid Codex job directory name")
	}
	path = filepath.Join(layout.Codex, jobID)
	sourceHome, err := resolveCodexAuthHome(s.codexAuthHome)
	if err != nil {
		return path, nil, err
	}
	if filepath.Clean(sourceHome) == filepath.Clean(path) {
		return path, nil, errors.New("operator Codex home must not be the owned destination")
	}
	managed, err = createManagedCodexHome(layout, jobID)
	if err != nil {
		return path, managed, err
	}

	for _, name := range codexHomeLinkedFiles {
		if err = linkManagedCodexHomeFile(sourceHome, managed, name); err != nil {
			return path, managed, err
		}
	}
	return path, managed, nil
}

// createManagedCodexHome exclusively creates a job leaf below a pinned,
// no-symlink sessions directory. The open descriptors keep cleanup anchored
// to that namespace even if an ancestor path is replaced later.
func createManagedCodexHome(layout engine.WorkspaceLayout, jobID string) (*managedCodexHome, error) {
	if err := ensureManagedCodexHomeNamespace(layout); err != nil {
		return nil, err
	}
	parentFD, err := openManagedCodexHomeParent(layout)
	if err != nil {
		return nil, err
	}
	if err := unix.Mkdirat(parentFD, jobID, 0o700); err != nil {
		_ = unix.Close(parentFD)
		if errors.Is(err, unix.EEXIST) {
			return nil, fmt.Errorf("create managed Codex home exclusively: %s already exists", jobID)
		}
		return nil, fmt.Errorf("create managed Codex home: %w", err)
	}
	directoryFD, err := unix.Openat(parentFD, jobID, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("open newly created managed Codex home: %w", err)
	}
	if err := unix.Fchmod(directoryFD, 0o700); err != nil {
		_ = unix.Close(directoryFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("secure managed Codex home: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(directoryFD, &stat); err != nil {
		_ = unix.Close(directoryFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("identify managed Codex home: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		_ = unix.Close(directoryFD)
		_ = unix.Close(parentFD)
		return nil, fmt.Errorf("newly created managed Codex home is not a directory")
	}
	return &managedCodexHome{
		path:        filepath.Join(layout.Codex, jobID),
		name:        jobID,
		parentFD:    parentFD,
		directoryFD: directoryFD,
		dev:         uint64(stat.Dev),
		ino:         uint64(stat.Ino),
	}, nil
}

// ensureManagedCodexHomeNamespace creates the sessions directory using pinned
// descriptors rather than following a path between checks.
func ensureManagedCodexHomeNamespace(layout engine.WorkspaceLayout) error {
	rootFD, err := unix.Open(layout.Root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open state root for managed Codex home: %w", err)
	}
	relativeNamespace, err := filepath.Rel(layout.Root, layout.Namespace)
	if err != nil || relativeNamespace == "." || filepath.IsAbs(relativeNamespace) || strings.HasPrefix(relativeNamespace, ".."+string(filepath.Separator)) {
		_ = unix.Close(rootFD)
		return fmt.Errorf("resolve managed Codex home namespace")
	}
	currentFD := rootFD
	defer func() { _ = unix.Close(currentFD) }()
	for _, component := range strings.Split(relativeNamespace, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("invalid managed Codex home namespace component")
		}
		if err := unix.Mkdirat(currentFD, component, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("create managed Codex home namespace: %w", err)
		}
		nextFD, err := unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return fmt.Errorf("open managed Codex home namespace: %w", err)
		}
		if err := unix.Fchmod(nextFD, 0o700); err != nil {
			_ = unix.Close(nextFD)
			return fmt.Errorf("secure managed Codex home namespace: %w", err)
		}
		if err := unix.Close(currentFD); err != nil {
			_ = unix.Close(nextFD)
			return fmt.Errorf("close managed Codex home namespace: %w", err)
		}
		currentFD = nextFD
	}
	return nil
}

func openManagedCodexHomeParent(layout engine.WorkspaceLayout) (int, error) {
	rootFD, err := unix.Open(layout.Root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open state root for managed Codex home: %w", err)
	}
	relativeNamespace, err := filepath.Rel(layout.Root, layout.Namespace)
	if err != nil || relativeNamespace == "." || filepath.IsAbs(relativeNamespace) || strings.HasPrefix(relativeNamespace, ".."+string(filepath.Separator)) {
		_ = unix.Close(rootFD)
		return -1, fmt.Errorf("resolve managed Codex home namespace")
	}
	currentFD := rootFD
	for _, component := range strings.Split(relativeNamespace, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			_ = unix.Close(currentFD)
			return -1, fmt.Errorf("invalid managed Codex home namespace component")
		}
		nextFD, openErr := unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(currentFD)
		if openErr != nil {
			return -1, fmt.Errorf("open managed Codex home namespace: %w", openErr)
		}
		currentFD = nextFD
	}
	if err := unix.Mkdirat(currentFD, "codex", 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
		_ = unix.Close(currentFD)
		return -1, fmt.Errorf("create managed Codex home root: %w", err)
	}
	parentFD, err := unix.Openat(currentFD, "codex", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	_ = unix.Close(currentFD)
	if err != nil {
		return -1, fmt.Errorf("open managed Codex home root: %w", err)
	}
	if err := unix.Fchmod(parentFD, 0o700); err != nil {
		_ = unix.Close(parentFD)
		return -1, fmt.Errorf("secure managed Codex home root: %w", err)
	}
	return parentFD, nil
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

func linkManagedCodexHomeFile(sourceHome string, destination *managedCodexHome, name string) error {
	if destination == nil || destination.directoryFD < 0 {
		return errors.New("managed Codex home is unavailable")
	}
	source := filepath.Join(sourceHome, name)
	info, err := os.Stat(source)
	if errors.Is(err, os.ErrNotExist) {
		// Codex supports homes without either file (for example, before login).
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat Codex %s: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("Codex %s is not a regular file", name)
	}
	var existing unix.Stat_t
	if err := unix.Fstatat(destination.directoryFD, name, &existing, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		return fmt.Errorf("Codex home %s already exists", name)
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect Codex home %s: %w", name, err)
	}
	if err := unix.Symlinkat(source, destination.directoryFD, name); err != nil {
		return fmt.Errorf("link Codex %s: %w", name, err)
	}
	return nil
}

// finalizeManagedCodexHome executes cleanup after the terminal record is
// committed, so a failed terminal write can never remove a session home still
// named by the record.
func finalizeManagedCodexHome(home *managedCodexHome, record jobstore.Record, cleanup protocol.Cleanup, diagnostics []string) (protocol.Cleanup, []string) {
	if home == nil {
		return cleanup, diagnostics
	}
	defer home.close()
	if cleanup != protocol.CleanupClean || (record.SessionHomeRetained() && strings.TrimSpace(record.BackendSessionID) != "") {
		return cleanup, diagnostics
	}
	if err := removeManagedCodexHome(home); err != nil {
		return cleanup, append(diagnostics, "managed Codex home cleanup: "+err.Error())
	}
	return cleanup, diagnostics
}

func removeManagedCodexHome(home *managedCodexHome) error {
	if home == nil || home.parentFD < 0 || home.directoryFD < 0 {
		return errors.New("managed Codex home identity is unavailable")
	}
	if err := verifyManagedCodexHomeIdentity(home); err != nil {
		return err
	}
	if err := removeManagedCodexHomeContents(home.directoryFD); err != nil {
		return fmt.Errorf("remove managed Codex home contents: %w", err)
	}
	// Check the directory entry one last time immediately before unlinking it.
	// This makes a renamed or replaced leaf a retention event, never a delete.
	if err := verifyManagedCodexHomeIdentity(home); err != nil {
		return err
	}
	// macOS will not remove a directory while it is open. Identity has already
	// been verified through the pinned parent; close only the leaf descriptor,
	// then unlink that exact parent entry.
	home.releaseDirectoryFD()
	if err := unix.Unlinkat(home.parentFD, home.name, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("remove managed Codex home directory: %w", err)
	}
	return nil
}

func verifyManagedCodexHomeIdentity(home *managedCodexHome) error {
	var named unix.Stat_t
	if err := unix.Fstatat(home.parentFD, home.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("managed Codex home identity lookup: %w", err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("managed Codex home identity mismatch: leaf is not a directory")
	}
	if !home.matches(named) {
		return fmt.Errorf("managed Codex home identity mismatch: got dev=%d ino=%d, want dev=%d ino=%d", named.Dev, named.Ino, home.dev, home.ino)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(home.directoryFD, &opened); err != nil {
		return fmt.Errorf("managed Codex home opened identity lookup: %w", err)
	}
	if !home.matches(opened) {
		return fmt.Errorf("managed Codex home opened identity mismatch: got dev=%d ino=%d, want dev=%d ino=%d", opened.Dev, opened.Ino, home.dev, home.ino)
	}
	return nil
}

func removeManagedCodexHomeContents(directoryFD int) error {
	copyFD, err := unix.Dup(directoryFD)
	if err != nil {
		return err
	}
	directory := os.NewFile(uintptr(copyFD), "managed Codex home")
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		name := entry.Name()
		var stat unix.Stat_t
		if err := unix.Fstatat(directoryFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			childFD, err := unix.Openat(directoryFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if err != nil {
				return err
			}
			// Unlinking an entry requires write permission on the directory that
			// contains it. Restore owner write on the descriptor we already opened
			// and identity-checked, so cleanup does not depend on re-resolving the
			// path.
			if err := unix.Fchmod(childFD, 0o700); err != nil {
				closeErr := unix.Close(childFD)
				if closeErr != nil {
					return errors.Join(err, closeErr)
				}
				return err
			}
			err = removeManagedCodexHomeContents(childFD)
			closeErr := unix.Close(childFD)
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if err := unix.Unlinkat(directoryFD, name, unix.AT_REMOVEDIR); err != nil {
				return err
			}
			continue
		}
		if err := unix.Unlinkat(directoryFD, name, 0); err != nil {
			return err
		}
	}
	return nil
}

// openExistingManagedDirectory opens a descendant directory through pinned
// descriptors without creating or following any component.
func openExistingManagedDirectory(root string, components ...string) (int, error) {
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	currentFD := rootFD
	for _, component := range components {
		if component == "" || component == "." || component == ".." || strings.ContainsRune(component, filepath.Separator) {
			_ = unix.Close(currentFD)
			return -1, errors.New("invalid managed directory component")
		}
		nextFD, openErr := unix.Openat(currentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(currentFD)
		if openErr != nil {
			return -1, openErr
		}
		currentFD = nextFD
	}
	return currentFD, nil
}

func openExistingManagedCodexHomeParent(layout engine.WorkspaceLayout) (int, error) {
	return openExistingManagedDirectory(layout.Root, "sessions", "codex")
}

func openExistingManagedCodexHome(layout engine.WorkspaceLayout, jobID string) (*managedCodexHome, error) {
	if jobID == "" || filepath.Base(jobID) != jobID || jobID == "." || jobID == ".." {
		return nil, errors.New("invalid managed Codex home directory name")
	}
	parentFD, err := openExistingManagedCodexHomeParent(layout)
	if err != nil {
		return nil, err
	}
	directoryFD, err := openManagedDirectoryAt(parentFD, jobID)
	if err != nil {
		_ = unix.Close(parentFD)
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(directoryFD, &stat); err != nil {
		_ = unix.Close(directoryFD)
		_ = unix.Close(parentFD)
		return nil, err
	}
	return &managedCodexHome{
		path:        filepath.Join(layout.Codex, jobID),
		name:        jobID,
		parentFD:    parentFD,
		directoryFD: directoryFD,
		dev:         uint64(stat.Dev),
		ino:         uint64(stat.Ino),
	}, nil
}

func openManagedDirectoryAt(parentFD int, name string) (int, error) {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
		return -1, errors.New("invalid managed directory name")
	}
	directoryFD, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	var named, opened unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		_ = unix.Close(directoryFD)
		return -1, err
	}
	if err := unix.Fstat(directoryFD, &opened); err != nil {
		_ = unix.Close(directoryFD)
		return -1, err
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Dev != opened.Dev || named.Ino != opened.Ino {
		_ = unix.Close(directoryFD)
		return -1, errors.New("managed directory identity mismatch")
	}
	return directoryFD, nil
}

func managedRegularFileSize(parentFD int, name string) (int64, bool, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return 0, false, errors.New("artifact is not a regular file")
	}
	return stat.Size, true, nil
}

func removeManagedRegularFile(parentFD int, name string) (int64, error) {
	fileFD, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	var opened, named unix.Stat_t
	if err := unix.Fstat(fileFD, &opened); err != nil {
		_ = unix.Close(fileFD)
		return 0, err
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fileFD)
		return 0, errors.New("artifact is not a regular file")
	}
	if err := unix.Fstatat(parentFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		_ = unix.Close(fileFD)
		return 0, err
	}
	if named.Mode&unix.S_IFMT != unix.S_IFREG || named.Dev != opened.Dev || named.Ino != opened.Ino {
		_ = unix.Close(fileFD)
		return 0, errors.New("artifact identity changed before removal")
	}
	if err := unix.Close(fileFD); err != nil {
		return 0, err
	}
	if err := unix.Unlinkat(parentFD, name, 0); err != nil {
		return 0, err
	}
	return opened.Size, nil
}

func (s *Server) rememberManagedCodexHome(jobID string, home *managedCodexHome) {
	if s == nil || jobID == "" || home == nil {
		return
	}
	s.codexHomesMu.Lock()
	if s.managedCodexHomes == nil {
		s.managedCodexHomes = make(map[string]*managedCodexHome)
	}
	previous := s.managedCodexHomes[jobID]
	s.managedCodexHomes[jobID] = home
	s.codexHomesMu.Unlock()
	if previous != nil && previous != home {
		previous.close()
	}
}

func (s *Server) takeManagedCodexHome(jobID string) *managedCodexHome {
	if s == nil || jobID == "" {
		return nil
	}
	s.codexHomesMu.Lock()
	home := s.managedCodexHomes[jobID]
	delete(s.managedCodexHomes, jobID)
	s.codexHomesMu.Unlock()
	return home
}

func (s *Server) closeManagedCodexHome(jobID string) {
	if home := s.takeManagedCodexHome(jobID); home != nil {
		home.close()
	}
}

// discardManagedCodexHome removes a prepared per-job state leaf when the job
// is canceled or cannot commit its starting transition before a backend runs.
func (s *Server) discardManagedCodexHome(jobID string) {
	home := s.takeManagedCodexHome(jobID)
	if home != nil {
		if err := removeManagedCodexHome(home); err != nil {
			log.Printf("agentbus service: discard managed job state %s: %v", home.path, err)
		}
		home.close()
	}
}
