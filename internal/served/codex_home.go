package served

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

var codexHomeLinkedFiles = []string{"auth.json", "config.toml"}

// managedCodexHome records both the directory identity created for one job and
// directory handles pinned to its original parents. Cleanup only removes a
// leaf after it still resolves to this identity through the pinned parent.
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
	if s == nil || s.codexHomeInherit {
		return "", nil, nil
	}
	if jobID == "" || filepath.Base(jobID) != jobID || jobID == "." || jobID == ".." {
		return "", nil, fmt.Errorf("invalid Codex job directory name")
	}
	if s.codexHomeOverride != "" {
		path = strings.TrimSpace(s.codexHomeOverride)
		if !filepath.IsAbs(path) {
			return "", nil, fmt.Errorf("AGENTBUS_CODEX_HOME must be an absolute path")
		}
		path = filepath.Clean(path)
		if err := os.MkdirAll(path, 0o700); err != nil {
			return "", nil, fmt.Errorf("create Codex home: %w", err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			return "", nil, fmt.Errorf("secure Codex home: %w", err)
		}
	} else {
		path = filepath.Join(layout.Codex, jobID)
		managed, err = createManagedCodexHome(layout, jobID)
		if err != nil {
			return path, managed, err
		}
	}

	sourceHome, err := resolveCodexAuthHome(s.codexAuthHome)
	if err != nil {
		return path, managed, err
	}
	for _, name := range codexHomeLinkedFiles {
		if managed != nil {
			err = linkManagedCodexHomeFile(sourceHome, managed, name)
		} else {
			err = linkCodexHomeFile(sourceHome, path, name)
		}
		if err != nil {
			return path, managed, err
		}
	}
	return path, managed, nil
}

// createManagedCodexHome exclusively creates a job leaf below a pinned,
// no-symlink workspace namespace. The open descriptors keep cleanup anchored
// to that namespace even if an ancestor path is replaced later.
func createManagedCodexHome(layout engine.WorkspaceLayout, jobID string) (*managedCodexHome, error) {
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

func (s *Server) cleanupManagedCodexHome(home *managedCodexHome, outcome model.Outcome) {
	if home == nil {
		return
	}
	defer home.close()
	if !codexHomeCleanupEligible(outcome) {
		return
	}
	if err := removeManagedCodexHome(home); err != nil {
		// Retain the leaf whenever identity cannot be proved. Cleanup must never
		// reach a replacement path simply because it has the same job name.
		log.Printf("agentbus daemon: retain completed managed Codex home %s: %v", home.path, err)
	}
}

func codexHomeCleanupEligible(outcome model.Outcome) bool {
	return outcome == model.OutcomeCompleted || outcome == model.OutcomeCompletedNoncompliant
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
