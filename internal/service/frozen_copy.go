//go:build darwin || linux

package service

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	frozenCopyProductionMaxBytes   int64 = 2 << 30
	frozenCopyProductionMaxEntries       = 200_000
	gitWorkTreeCheckTimeout              = 2 * time.Second
)

// These unexported limits are variables so the service tests can exercise
// bound failures without constructing a multi-gigabyte source tree.
var (
	frozenCopyMaxBytes   = frozenCopyProductionMaxBytes
	frozenCopyMaxEntries = frozenCopyProductionMaxEntries
)

func requireGitWorkTree(cwd string) error {
	ctx, cancel := context.WithTimeout(context.Background(), gitWorkTreeCheckTimeout)
	defer cancel()
	output, err := frozenGitCommand(ctx, cwd, "rev-parse", "--is-inside-work-tree").Output()
	if err != nil || strings.TrimSpace(string(output)) != "true" {
		return errors.New("verify requires cwd inside a Git work tree")
	}
	return nil
}

func createFrozenCopy(source, destination string) (protocol.FrozenSource, error) {
	return createFrozenCopyContext(context.Background(), source, destination)
}

func createFrozenCopyContext(parent context.Context, source, destination string) (protocol.FrozenSource, error) {
	identity, err := frozenSourceIdentity(parent, source)
	if err != nil {
		return protocol.FrozenSource{}, err
	}
	command := frozenGitCommand(parent, source, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	stdout, err := command.StdoutPipe()
	if err != nil {
		return protocol.FrozenSource{}, fmt.Errorf("list frozen source files: %w", err)
	}
	var stderr boundedGitOutput
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return protocol.FrozenSource{}, fmt.Errorf("list frozen source files: %w", err)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return protocol.FrozenSource{}, fmt.Errorf("create frozen source copy: %w", err)
	}
	copyErr := copyFrozenFiles(parent, source, destination, stdout)
	if copyErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if copyErr != nil || waitErr != nil {
		_ = os.RemoveAll(destination)
		if copyErr != nil {
			return protocol.FrozenSource{}, copyErr
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = waitErr.Error()
		}
		return protocol.FrozenSource{}, fmt.Errorf("list frozen source files: %s", message)
	}
	return identity, nil
}

func frozenSourceIdentity(parent context.Context, source string) (protocol.FrozenSource, error) {
	head, err := frozenGitCommand(parent, source, "rev-parse", "HEAD").Output()
	if err != nil || strings.TrimSpace(string(head)) == "" {
		return protocol.FrozenSource{}, errors.New("read frozen source Git HEAD")
	}
	status, err := frozenGitCommand(parent, source, "-c", "core.fsmonitor=false", "status", "--porcelain", "--untracked-files=all").Output()
	if err != nil {
		return protocol.FrozenSource{}, errors.New("read frozen source Git status")
	}
	return protocol.FrozenSource{Head: strings.TrimSpace(string(head)), Dirty: len(status) != 0}, nil
}

func frozenGitCommand(ctx context.Context, cwd string, args ...string) *exec.Cmd {
	commandArgs := append([]string{"-C", cwd}, args...)
	command := exec.CommandContext(ctx, "git", commandArgs...)
	environment := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "GIT_OPTIONAL_LOCKS", "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_CEILING_DIRECTORIES", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT":
			continue
		}
		if strings.HasPrefix(name, "GIT_CONFIG_KEY_") || strings.HasPrefix(name, "GIT_CONFIG_VALUE_") {
			continue
		}
		environment = append(environment, entry)
	}
	command.Env = append(environment, "GIT_OPTIONAL_LOCKS=0")
	return command
}

type boundedGitOutput struct {
	data []byte
}

func (output *boundedGitOutput) Write(data []byte) (int, error) {
	const maxGitDiagnostic = 4096
	originalLength := len(data)
	if len(output.data) < maxGitDiagnostic {
		remaining := maxGitDiagnostic - len(output.data)
		if len(data) > remaining {
			data = data[:remaining]
		}
		output.data = append(output.data, data...)
	}
	return originalLength, nil
}

func (output *boundedGitOutput) String() string { return string(output.data) }

func copyFrozenFiles(ctx context.Context, source, destination string, listing io.Reader) error {
	rootFD, err := unix.Open(source, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open frozen source root: %w", err)
	}
	defer unix.Close(rootFD)

	reader := bufio.NewReader(listing)
	var copiedBytes int64
	var copiedEntries int
	createdDirectories := make(map[string]struct{})
	seen := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		name, err := reader.ReadString('\x00')
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read frozen source file list: %w", err)
		}
		if len(name) != 0 {
			name = strings.TrimSuffix(name, "\x00")
			if _, duplicate := seen[name]; !duplicate {
				seen[name] = struct{}{}
				if err := copyFrozenEntry(ctx, rootFD, destination, name, &copiedBytes, &copiedEntries, createdDirectories); err != nil {
					return err
				}
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func copyFrozenEntry(ctx context.Context, rootFD int, destination, name string, copiedBytes *int64, copiedEntries *int, createdDirectories map[string]struct{}) error {
	if name == "" || filepath.IsAbs(name) {
		return fmt.Errorf("invalid path in frozen source file list")
	}
	relative := filepath.Clean(filepath.FromSlash(name))
	if relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("invalid path in frozen source file list")
	}
	if relative == ".git" || strings.HasPrefix(relative, ".git"+string(filepath.Separator)) {
		return nil
	}

	parts := strings.Split(relative, string(filepath.Separator))
	parentFD, err := openFrozenParent(rootFD, parts[:len(parts)-1])
	if errors.Is(err, unix.ENOENT) {
		return nil // tracked paths deleted from disk are absent from the copy
	}
	if err != nil {
		return fmt.Errorf("open frozen source parent for %q: %w", name, err)
	}
	defer unix.Close(parentFD)

	var stat unix.Stat_t
	if err := unix.Fstatat(parentFD, parts[len(parts)-1], &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return fmt.Errorf("inspect frozen source %q: %w", name, err)
	}
	fileType := stat.Mode & unix.S_IFMT
	if fileType != unix.S_IFREG && fileType != unix.S_IFLNK {
		return nil
	}
	destinationPath := filepath.Join(destination, relative)
	if err := ensureFrozenDestinationParents(destination, filepath.Dir(relative), copiedEntries, createdDirectories); err != nil {
		return fmt.Errorf("create frozen copy parent for %q: %w", name, err)
	}
	if *copiedEntries >= frozenCopyMaxEntries {
		return fmt.Errorf("frozen copy exceeds entry bound of %s (production bound: 200,000 entries)", formatEntryBound(frozenCopyMaxEntries))
	}
	(*copiedEntries)++
	if fileType == unix.S_IFLNK {
		target, err := readFrozenSymlink(parentFD, parts[len(parts)-1])
		if err != nil {
			return fmt.Errorf("read frozen source symlink %q: %w", name, err)
		}
		if int64(len(target)) > frozenCopyMaxBytes-*copiedBytes {
			return fmt.Errorf("frozen copy exceeds byte bound of %s (production bound: 2 GiB)", formatByteBound(frozenCopyMaxBytes))
		}
		if err := os.Symlink(target, destinationPath); err != nil {
			return fmt.Errorf("create frozen source symlink %q: %w", name, err)
		}
		*copiedBytes += int64(len(target))
		return nil
	}

	fileFD, err := unix.Openat(parentFD, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open frozen source file %q: %w", name, err)
	}
	input := os.NewFile(uintptr(fileFD), name)
	defer input.Close()
	if err := unix.Fstat(fileFD, &stat); err != nil {
		return fmt.Errorf("inspect open frozen source file %q: %w", name, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil
	}
	remaining := frozenCopyMaxBytes - *copiedBytes
	if stat.Size > remaining {
		return fmt.Errorf("frozen copy exceeds byte bound of %s (production bound: 2 GiB)", formatByteBound(frozenCopyMaxBytes))
	}
	mode := frozenFileMode(uint32(stat.Mode))
	output, err := os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
	if err != nil {
		return fmt.Errorf("create frozen source file %q: %w", name, err)
	}
	count, copyErr := io.Copy(output, io.LimitReader(contextReader{ctx: ctx, reader: input}, remaining+1))
	if copyErr == nil && count > remaining {
		copyErr = fmt.Errorf("frozen copy exceeds byte bound of %s (production bound: 2 GiB)", formatByteBound(frozenCopyMaxBytes))
	}
	if copyErr == nil {
		copyErr = output.Chmod(mode)
	}
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(destinationPath)
		if copyErr != nil {
			return fmt.Errorf("copy frozen source file %q: %w", name, copyErr)
		}
		return fmt.Errorf("close frozen source file %q: %w", name, closeErr)
	}
	*copiedBytes += count
	return nil
}

func ensureFrozenDestinationParents(root, relative string, copiedEntries *int, created map[string]struct{}) error {
	if relative == "." {
		return nil
	}
	current := root
	parts := strings.Split(relative, string(filepath.Separator))
	for index := range parts {
		current = filepath.Join(current, parts[index])
		key := filepath.Join(parts[:index+1]...)
		if _, ok := created[key]; ok {
			continue
		}
		if *copiedEntries >= frozenCopyMaxEntries {
			return fmt.Errorf("frozen copy exceeds entry bound of %s (production bound: 200,000 entries)", formatEntryBound(frozenCopyMaxEntries))
		}
		if err := os.Mkdir(current, 0o700); err != nil {
			return err
		}
		created[key] = struct{}{}
		(*copiedEntries)++
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func openFrozenParent(rootFD int, components []string) (int, error) {
	current, err := unix.Dup(rootFD)
	if err != nil {
		return -1, err
	}
	for _, component := range components {
		next, err := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(current)
		if err != nil {
			return -1, err
		}
		current = next
	}
	return current, nil
}

func readFrozenSymlink(parentFD int, name string) (string, error) {
	buffer := make([]byte, 64*1024)
	length, err := unix.Readlinkat(parentFD, name, buffer)
	if err != nil {
		return "", err
	}
	if length == len(buffer) {
		return "", errors.New("symlink target exceeds supported path length")
	}
	return string(buffer[:length]), nil
}

func frozenFileMode(mode uint32) os.FileMode {
	result := os.FileMode(mode & 0o777)
	if mode&unix.S_ISUID != 0 {
		result |= os.ModeSetuid
	}
	if mode&unix.S_ISGID != 0 {
		result |= os.ModeSetgid
	}
	if mode&unix.S_ISVTX != 0 {
		result |= os.ModeSticky
	}
	return result
}

func formatEntryBound(value int) string {
	return fmt.Sprintf("%d entries", value)
}

func formatByteBound(value int64) string {
	return fmt.Sprintf("%d bytes", value)
}
