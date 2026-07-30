package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

const defaultStartTimeout = 10 * time.Second

var (
	autostartUserCacheDir = os.UserCacheDir
	autostartTempDir      = os.TempDir
)

// ErrProtocolVersionMismatch identifies a hello result whose protocolVersion
// does not match the client protocol version.
var ErrProtocolVersionMismatch = errors.New("protocol version mismatch")

// ErrAutostartLockUnsafe identifies an autostart lock path that failed the
// ownership, type, or permission checks required before using a shared tmp
// fallback.
var ErrAutostartLockUnsafe = errors.New("agentbus autostart lock path unsafe")

// ErrRootFailStopped identifies daemon startup refusal because the authority
// root has tripped fail-stop before the daemon opened its socket.
var ErrRootFailStopped = errors.New("agentbus authority root fail-stopped")

// ErrRootSealed identifies daemon startup refusal because the authority root
// has been permanently sealed before the daemon opened its socket.
var ErrRootSealed = errors.New("agentbus authority root sealed")

// StartupRefusedError reports a daemon autostart that exited before becoming
// ready because the authority root permanently refused startup. Reason is the
// admission cause, such as "root_fail_stopped" or "root_sealed".
type StartupRefusedError struct {
	Reason string
	Err    error
}

func (e *StartupRefusedError) Error() string {
	if e == nil {
		return "agentbus daemon startup refused"
	}
	message := "agentbus daemon startup refused"
	if sentinel := startupRefusedSentinel(e.Reason); sentinel != nil {
		message += ": " + sentinel.Error()
	} else if reason := strings.TrimSpace(e.Reason); reason != "" {
		message += ": " + reason
	}
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *StartupRefusedError) Is(target error) bool {
	if e == nil {
		return false
	}
	return target != nil && startupRefusedSentinel(e.Reason) == target
}

func (e *StartupRefusedError) Unwrap() error {
	if e == nil {
		return nil
	}
	sentinel := startupRefusedSentinel(e.Reason)
	switch {
	case sentinel != nil && e.Err != nil:
		return errors.Join(sentinel, e.Err)
	case sentinel != nil:
		return sentinel
	default:
		return e.Err
	}
}

func startupRefusedSentinel(reason string) error {
	switch strings.TrimSpace(reason) {
	case protocol.AdmissionRejectRootFailStopped:
		return ErrRootFailStopped
	case protocol.AdmissionRejectRootSealed:
		return ErrRootSealed
	default:
		return nil
	}
}

type AutostartLockUnsafeError struct {
	Path   string
	Reason string
	Cause  error
}

func (e AutostartLockUnsafeError) Error() string {
	message := ErrAutostartLockUnsafe.Error()
	if e.Path != "" {
		message = fmt.Sprintf("%s: %s", message, e.Path)
	}
	if e.Reason != "" {
		message = fmt.Sprintf("%s: %s", message, e.Reason)
	}
	if e.Cause != nil {
		message = fmt.Sprintf("%s: %v", message, e.Cause)
	}
	return message
}

func (e AutostartLockUnsafeError) Is(target error) bool {
	return target == ErrAutostartLockUnsafe
}

func (e AutostartLockUnsafeError) Unwrap() error {
	return e.Cause
}

// ProtocolVersionMismatchError reports the expected and received protocol
// versions from a failed hello exchange.
type ProtocolVersionMismatchError struct {
	Expected int
	Received int
}

func (e *ProtocolVersionMismatchError) Error() string {
	if e == nil {
		return ErrProtocolVersionMismatch.Error()
	}
	return fmt.Sprintf("%s: expected %d received %d", ErrProtocolVersionMismatch, e.Expected, e.Received)
}

func (e *ProtocolVersionMismatchError) Is(target error) bool {
	return target == ErrProtocolVersionMismatch
}

// Options configures a protocol client.
type Options struct {
	StateRoot        string
	SocketPath       string
	Token            string
	DisableAutoStart bool
	CommandPath      string
	StartTimeout     time.Duration
	Starter          DaemonStarter
}

// StartOptions are passed to a daemon starter.
type StartOptions struct {
	StateRoot   string
	SocketPath  string
	TokenPath   string
	CommandPath string
	Timeout     time.Duration
}

type StartResult struct {
	PID            int
	ExistingDaemon bool

	// Wait observes the started daemon process exit without killing it. A nil
	// Wait means the starter does not provide child-exit observation.
	Wait func(context.Context) (exitCode int, err error)

	killAndWait func() error
}

func (result StartResult) KillAndWait() error {
	if result.killAndWait == nil {
		return nil
	}
	return result.killAndWait()
}

// DaemonStarter starts an agentbus foreground daemon process.
type DaemonStarter interface {
	StartDaemon(context.Context, StartOptions) (StartResult, error)
}

// StartFunc adapts a function to DaemonStarter.
type StartFunc func(context.Context, StartOptions) (StartResult, error)

func (f StartFunc) StartDaemon(ctx context.Context, opts StartOptions) (StartResult, error) {
	return f(ctx, opts)
}

// Client is a typed JSON-RPC client for the local agentbus daemon.
type Client struct {
	opts       Options
	stateRoot  string
	socketPath string
	tokenPath  string
	hello      HelloResult

	writeMu sync.Mutex
	mu      sync.Mutex
	conn    net.Conn
	reader  *bufio.Reader
	pending map[string]chan protocol.Response
	closed  bool
	ids     atomic.Uint64
}

// Connect connects to a daemon, autostarting it when configured and necessary.
func Connect(ctx context.Context, opts Options) (*Client, error) {
	c, err := newClient(opts)
	if err != nil {
		return nil, err
	}
	if err := c.connect(ctx); err != nil {
		if opts.DisableAutoStart || !autostartableConnectError(err) {
			return nil, err
		}
		if err := c.autostart(ctx); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func newClient(opts Options) (*Client, error) {
	root := opts.StateRoot
	var err error
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			return nil, err
		}
	}
	socketPath := opts.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(root, protocol.SocketName)
	}
	return &Client{
		opts:       opts,
		stateRoot:  root,
		socketPath: socketPath,
		tokenPath:  filepath.Join(root, protocol.TokenFileName),
		pending:    make(map[string]chan protocol.Response),
	}, nil
}

func (c *Client) connect(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("agentbus client is closed")
	}
	c.mu.Unlock()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return &dialConnectError{err: err}
	}
	token, err := c.readToken()
	if err != nil {
		_ = conn.Close()
		return err
	}
	reader := bufio.NewReader(conn)
	hello, err := clientHello(ctx, conn, reader, token)
	if err != nil {
		_ = conn.Close()
		return err
	}
	c.mu.Lock()
	old := c.conn
	c.conn = conn
	c.reader = reader
	c.hello = hello
	c.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	go c.readLoop(conn, reader)
	return nil
}

func clientHello(ctx context.Context, conn net.Conn, reader *bufio.Reader, token string) (HelloResult, error) {
	req := protocol.Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`"hello"`),
		Method:  protocol.MethodHello,
		Params:  mustMarshal(protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token}),
	}
	if err := writeDeadline(ctx, conn, req); err != nil {
		return HelloResult{}, &helloTransportError{err: err}
	}
	line, err := readLineContext(ctx, conn, reader)
	if err != nil {
		return HelloResult{}, &helloTransportError{err: err}
	}
	var resp protocol.Response
	if err := json.Unmarshal(bytes.TrimSpace(line), &resp); err != nil {
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) {
			return HelloResult{}, &helloTransportError{err: err}
		}
		return HelloResult{}, err
	}
	if resp.Error != nil {
		return HelloResult{}, &protocol.RPCError{Object: *resp.Error}
	}
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		return HelloResult{}, err
	}
	var hello HelloResult
	if err := json.Unmarshal(raw, &hello); err != nil {
		return HelloResult{}, err
	}
	if hello.ProtocolVersion != protocol.Version {
		return HelloResult{}, &ProtocolVersionMismatchError{Expected: protocol.Version, Received: hello.ProtocolVersion}
	}
	return hello, nil
}

func writeDeadline(ctx context.Context, conn net.Conn, v any) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer conn.SetWriteDeadline(time.Time{})
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = conn.Write(b)
	return err
}

func readLineContext(ctx context.Context, conn net.Conn, reader *bufio.Reader) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
		defer conn.SetReadDeadline(time.Time{})
	}
	return reader.ReadBytes('\n')
}

func (c *Client) readToken() (string, error) {
	if c.opts.Token != "" {
		return c.opts.Token, nil
	}
	raw, err := os.ReadFile(c.tokenPath)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", errors.New("agentbus token file is empty")
	}
	return token, nil
}

func (c *Client) autostart(ctx context.Context) error {
	autoCtx, cancel := context.WithTimeout(ctx, c.startTimeout())
	defer cancel()

	unlock, err := c.lockAutostartStateRoot(autoCtx)
	if err != nil {
		return err
	}
	defer unlock()

	if err := c.connect(autoCtx); err == nil {
		return nil
	} else if !autostartableConnectError(err) {
		return err
	}
	starter := c.opts.Starter
	if starter == nil {
		starter = defaultStarter{}
	}
	started, err := starter.StartDaemon(autoCtx, StartOptions{
		StateRoot:   c.stateRoot,
		SocketPath:  c.socketPath,
		TokenPath:   c.tokenPath,
		CommandPath: c.opts.CommandPath,
		Timeout:     remainingTimeout(autoCtx),
	})
	if err != nil {
		return err
	}
	exitCh, cancelWait := autostartExitChannel(started)
	if cancelWait != nil {
		defer cancelWait()
	}
	pidPath := filepath.Join(c.stateRoot, "agentbus.pid")
	pidWritten := false
	cleanupStarted := func(err error, processExited bool) error {
		var cleanupErr error
		if pidWritten {
			if removeErr := os.Remove(pidPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, removeErr)
			}
		}
		if started.ExistingDaemon || started.PID <= 0 {
			return errors.Join(err, cleanupErr)
		}
		if processExited {
			return errors.Join(err, cleanupErr)
		}
		if killErr := started.KillAndWait(); killErr != nil {
			cleanupErr = errors.Join(cleanupErr, killErr)
		}
		return errors.Join(err, cleanupErr)
	}
	if started.PID > 0 && !started.ExistingDaemon {
		if err := atomicWrite(pidPath, []byte(strconv.Itoa(started.PID)+"\n"), 0o600); err != nil {
			return cleanupStarted(err, false)
		}
		pidWritten = true
	}
	var last error
	for autoCtx.Err() == nil {
		if err := c.connect(autoCtx); err == nil {
			return nil
		} else if !autostartableConnectError(err) {
			return cleanupStarted(err, false)
		} else {
			last = err
		}
		select {
		case exit := <-exitCh:
			return cleanupStarted(autostartChildExitError(exit), true)
		case <-autoCtx.Done():
			return cleanupStarted(autoCtx.Err(), false)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if last == nil {
		last = errors.New("daemon did not become ready")
	}
	return cleanupStarted(errors.Join(autoCtx.Err(), last), false)
}

type autostartExitResult struct {
	exitCode int
	err      error
}

func autostartExitChannel(started StartResult) (<-chan autostartExitResult, context.CancelFunc) {
	if started.Wait == nil || started.ExistingDaemon || started.PID <= 0 {
		return nil, nil
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	exitCh := make(chan autostartExitResult, 1)
	go func() {
		exitCode, err := started.Wait(waitCtx)
		exitCh <- autostartExitResult{exitCode: exitCode, err: err}
	}()
	return exitCh, cancel
}

func autostartChildExitError(exit autostartExitResult) error {
	cause := autostartChildExitCause(exit)
	switch exit.exitCode {
	case daemonlaunch.ExitAuthorityFailStopped:
		return &StartupRefusedError{Reason: protocol.AdmissionRejectRootFailStopped, Err: cause}
	case daemonlaunch.ExitAuthorityRootSealed:
		return &StartupRefusedError{Reason: protocol.AdmissionRejectRootSealed, Err: cause}
	default:
		return cause
	}
}

func autostartChildExitCause(exit autostartExitResult) error {
	message := fmt.Sprintf("agentbus daemon exited before becoming ready (exit code %d)", exit.exitCode)
	if exit.err == nil {
		return errors.New(message)
	}
	return fmt.Errorf("%s: %w", message, exit.err)
}

func (c *Client) startTimeout() time.Duration {
	if c.opts.StartTimeout > 0 {
		return c.opts.StartTimeout
	}
	return defaultStartTimeout
}

func (c *Client) lockAutostartStateRoot(ctx context.Context) (func(), error) {
	lockKey, err := stateRootAutostartLockKey(c.stateRoot)
	if err != nil {
		return nil, err
	}
	lock, err := openAutostartLockFileForKey(lockKey)
	if err != nil {
		return nil, err
	}
	locked := false
	unlock := func() {
		if locked {
			_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		}
		_ = lock.Close()
	}
	for {
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			locked = true
			return unlock, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			unlock()
			return nil, err
		}
		select {
		case <-ctx.Done():
			unlock()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func stateRootAutostartLockKey(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(abs)
	if err == nil {
		if identity, ok, err := pathFileIdentity(canonical); err != nil {
			return "", err
		} else if ok {
			// Invariant: every spelling that resolves to the same existing
			// state-root inode maps to the same autostart lock. Missing roots
			// key by nearest existing ancestor inode plus the unresolved suffix,
			// case-folded on case-insensitive platforms.
			return fmt.Sprintf("existing:%x:%x", identity.dev, identity.ino), nil
		}
		return "path:" + canonical, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	return missingStateRootAutostartLockKey(abs)
}

func missingStateRootAutostartLockKey(abs string) (string, error) {
	current := filepath.Clean(abs)
	var missing []string
	for {
		canonical, err := filepath.EvalSymlinks(current)
		if err == nil {
			remainder := filepath.Join(missing...)
			remainder = caseFoldAutostartLockRemainder(remainder)
			if identity, ok, err := pathFileIdentity(canonical); err != nil {
				return "", err
			} else if ok {
				return fmt.Sprintf("missing:%x:%x:%s", identity.dev, identity.ino, remainder), nil
			}
			return "missing-path:" + filepath.Join(canonical, remainder), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		missing = append([]string{filepath.Base(current)}, missing...)
		current = parent
	}
}

type autostartFileIdentity struct {
	dev uint64
	ino uint64
}

func pathFileIdentity(path string) (autostartFileIdentity, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return autostartFileIdentity{}, false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return autostartFileIdentity{}, false, nil
	}
	return autostartFileIdentity{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, true, nil
}

func caseFoldAutostartLockRemainder(path string) string {
	switch runtime.GOOS {
	case "darwin", "windows":
		return strings.ToLower(path)
	default:
		return path
	}
}

func autostartLockPath(lockKey string) (string, error) {
	lockDir, err := openAutostartLockDir()
	if err != nil {
		return "", err
	}
	defer lockDir.close()
	return filepath.Join(lockDir.path, autostartLockFileName(lockKey)), nil
}

func autostartLockFileName(lockKey string) string {
	sum := sha256.Sum256([]byte(lockKey))
	return "start-" + hex.EncodeToString(sum[:]) + ".lock"
}

type autostartLockDirectory struct {
	path string
	fd   int
}

func (dir *autostartLockDirectory) close() error {
	if dir == nil || dir.fd < 0 {
		return nil
	}
	err := unix.Close(dir.fd)
	dir.fd = -1
	return err
}

func openAutostartLockDir() (*autostartLockDirectory, error) {
	cacheDir, err := autostartUserCacheDir()
	if err == nil && strings.TrimSpace(cacheDir) != "" {
		lockDir := filepath.Join(cacheDir, "agentbus", "start-locks")
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			return nil, err
		}
		return openVerifiedAutostartLockDir(lockDir, autostartPrimaryLockDir)
	}
	lockDir := filepath.Join(autostartTempDir(), fmt.Sprintf("agentbus-start-locks-%d", os.Getuid()))
	if err := os.Mkdir(lockDir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return openVerifiedAutostartLockDir(lockDir, autostartTempLockDir)
}

type autostartLockDirKind uint8

const (
	autostartPrimaryLockDir autostartLockDirKind = iota + 1
	autostartTempLockDir
)

func openVerifiedAutostartLockDir(path string, kind autostartLockDirKind) (*autostartLockDirectory, error) {
	flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
	if kind == autostartTempLockDir {
		flags |= unix.O_NOFOLLOW
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, AutostartLockUnsafeError{Path: path, Reason: "open lock directory", Cause: err}
	}
	dir := &autostartLockDirectory{path: path, fd: fd}
	if err := verifyAutostartLockDirFD(path, fd, kind); err != nil {
		_ = dir.close()
		return nil, err
	}
	return dir, nil
}

func verifyAutostartLockDirFD(path string, fd int, kind autostartLockDirKind) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return AutostartLockUnsafeError{Path: path, Reason: "stat opened lock directory", Cause: err}
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return AutostartLockUnsafeError{Path: path, Reason: "lock path is not a directory"}
	}
	if stat.Uid != uint32(os.Getuid()) {
		return AutostartLockUnsafeError{Path: path, Reason: fmt.Sprintf("lock directory owner is %d, want %d", stat.Uid, os.Getuid())}
	}
	mode := os.FileMode(stat.Mode).Perm()
	switch kind {
	case autostartPrimaryLockDir:
		if mode&0o002 != 0 {
			return AutostartLockUnsafeError{Path: path, Reason: fmt.Sprintf("lock directory mode is %o, want not world-writable", mode)}
		}
	case autostartTempLockDir:
		if mode != 0o700 {
			return AutostartLockUnsafeError{Path: path, Reason: fmt.Sprintf("lock directory mode is %o, want 700", mode)}
		}
	default:
		return AutostartLockUnsafeError{Path: path, Reason: "lock directory verification policy unavailable"}
	}
	return nil
}

func openAutostartLockFileForKey(lockKey string) (*os.File, error) {
	lockDir, err := openAutostartLockDir()
	if err != nil {
		return nil, err
	}
	defer lockDir.close()
	return openAutostartLockFileAt(lockDir, autostartLockFileName(lockKey))
}

func openAutostartLockFileAt(lockDir *autostartLockDirectory, name string) (*os.File, error) {
	if lockDir == nil || lockDir.fd < 0 {
		return nil, AutostartLockUnsafeError{Reason: "lock directory is not open"}
	}
	if name == "" || filepath.Base(name) != name {
		return nil, AutostartLockUnsafeError{Path: lockDir.path, Reason: "lock file name is invalid"}
	}
	path := filepath.Join(lockDir.path, name)
	fd, err := unix.Openat(lockDir.fd, name, unix.O_CREAT|unix.O_EXCL|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if errors.Is(err, unix.EEXIST) {
		fd, err = unix.Openat(lockDir.fd, name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, AutostartLockUnsafeError{Path: path, Reason: "open lock file", Cause: err}
	}
	lock := os.NewFile(uintptr(fd), path)
	if err := verifyAutostartLockFile(lock); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return lock, nil
}

func verifyAutostartLockFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return AutostartLockUnsafeError{Path: file.Name(), Reason: "stat lock file", Cause: err}
	}
	if !info.Mode().IsRegular() {
		return AutostartLockUnsafeError{Path: file.Name(), Reason: "lock file is not regular"}
	}
	if info.Mode().Perm()&0o077 != 0 {
		return AutostartLockUnsafeError{Path: file.Name(), Reason: fmt.Sprintf("lock file mode is %o, want owner-only", info.Mode().Perm())}
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return AutostartLockUnsafeError{Path: file.Name(), Reason: "lock file identity unavailable"}
	}
	if stat.Uid != uint32(os.Getuid()) {
		return AutostartLockUnsafeError{Path: file.Name(), Reason: fmt.Sprintf("lock file owner is %d, want %d", stat.Uid, os.Getuid())}
	}
	return nil
}

func remainingTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Nanosecond
	}
	return remaining
}

type defaultStarter struct{}

func (defaultStarter) StartDaemon(ctx context.Context, opts StartOptions) (StartResult, error) {
	command := opts.CommandPath
	if command == "" {
		var err error
		command, err = exec.LookPath("agentbus")
		if err != nil {
			if exe, exeErr := os.Executable(); exeErr == nil && filepath.Base(exe) == "agentbus" {
				command = exe
			} else {
				return StartResult{}, fmt.Errorf("agentbus binary not found for autostart: %w", err)
			}
		}
	}
	select {
	case <-ctx.Done():
		return StartResult{}, ctx.Err()
	default:
	}
	result, err := daemonlaunch.Launch(ctx, daemonlaunch.Options{
		CommandPath: command,
		Args:        []string{"serve", "--foreground"},
		StateRoot:   opts.StateRoot,
		SocketPath:  opts.SocketPath,
		TokenPath:   opts.TokenPath,
		Timeout:     opts.Timeout,
		Starter:     startDaemonProcess,
	})
	if err != nil {
		return StartResult{}, err
	}
	var wait func(context.Context) (int, error)
	if result.PID > 0 && !result.ExistingDaemon {
		wait = result.Wait
	}
	return StartResult{PID: result.PID, ExistingDaemon: result.ExistingDaemon, Wait: wait, killAndWait: result.KillAndWait}, nil
}

type daemonProcess struct {
	cmd *exec.Cmd
}

func startDaemonProcess(config daemonlaunch.ProcessConfig) (daemonlaunch.Process, error) {
	cmd := exec.Command(config.CommandPath, config.Args...)
	cmd.Env = config.Env
	cmd.ExtraFiles = config.ExtraFiles
	cmd.Stdin = config.Stdin
	cmd.Stdout = config.Stdout
	cmd.Stderr = config.Stderr
	if config.Setsid {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return daemonProcess{cmd: cmd}, nil
}

func (process daemonProcess) PID() int {
	return process.cmd.Process.Pid
}

func (process daemonProcess) Kill() error {
	return process.cmd.Process.Kill()
}

func (process daemonProcess) Wait() error {
	return process.cmd.Wait()
}

func (c *Client) readLoop(conn net.Conn, reader *bufio.Reader) {
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			c.failPending(err)
			c.mu.Lock()
			if c.conn == conn {
				c.conn = nil
				c.reader = nil
			}
			c.mu.Unlock()
			return
		}
		var head struct {
			ID     json.RawMessage `json:"id,omitempty"`
			Method string          `json:"method,omitempty"`
		}
		if err := json.Unmarshal(bytes.TrimSpace(line), &head); err != nil {
			continue
		}
		if head.Method != "" && len(head.ID) == 0 {
			continue
		}
		var resp protocol.Response
		if err := json.Unmarshal(bytes.TrimSpace(line), &resp); err != nil {
			continue
		}
		id := strings.Trim(string(resp.ID), `"`)
		c.mu.Lock()
		ch := c.pending[id]
		delete(c.pending, id)
		c.mu.Unlock()
		if ch != nil {
			ch <- resp
			close(ch)
		}
	}
}

func (c *Client) failPending(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- protocol.Response{JSONRPC: "2.0", ID: json.RawMessage(strconv.Quote(id)), Error: protocol.NewError(protocol.ErrorBackendUnavailable, err.Error(), protocol.ErrorData{})}
		close(ch)
	}
}

func (c *Client) do(ctx context.Context, method string, params any, result any) error {
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}
	id := strconv.FormatUint(c.ids.Add(1), 10)
	req := protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(strconv.Quote(id)), Method: method}
	if params != nil {
		req.Params = mustMarshal(params)
	}
	ch := make(chan protocol.Response, 1)
	c.mu.Lock()
	c.pending[id] = ch
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return errors.New("agentbus client is not connected")
	}
	c.writeMu.Lock()
	err := writeDeadline(ctx, conn, req)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		_ = c.reconnect(ctx)
		return err
	}
	select {
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	case resp := <-ch:
		if resp.Error != nil {
			return &protocol.RPCError{Object: *resp.Error}
		}
		if result == nil {
			return nil
		}
		raw, err := json.Marshal(resp.Result)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, result)
	}
}

func (c *Client) ensureConnected(ctx context.Context) error {
	c.mu.Lock()
	connected := c.conn != nil && !c.closed
	c.mu.Unlock()
	if connected {
		return nil
	}
	return c.reconnect(ctx)
}

func (c *Client) reconnect(ctx context.Context) error {
	if c.opts.DisableAutoStart {
		return c.connect(ctx)
	}
	if err := c.connect(ctx); err == nil {
		return nil
	} else if !autostartableConnectError(err) {
		return err
	}
	return c.autostart(ctx)
}

func autostartableConnectError(err error) bool {
	var helloErr *helloTransportError
	if errors.As(err, &helloErr) {
		return true
	}
	var dialErr *dialConnectError
	if !errors.As(err, &dialErr) || dialErr.err == nil {
		return false
	}
	if errors.Is(dialErr.err, os.ErrNotExist) ||
		errors.Is(dialErr.err, syscall.ENOENT) ||
		errors.Is(dialErr.err, syscall.ECONNREFUSED) {
		return true
	}
	var netErr net.Error
	return errors.As(dialErr.err, &netErr) && netErr.Timeout()
}

type helloTransportError struct {
	err error
}

func (e *helloTransportError) Error() string {
	if e == nil || e.err == nil {
		return "hello transport failed"
	}
	return e.err.Error()
}

func (e *helloTransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

type dialConnectError struct {
	err error
}

func (e *dialConnectError) Error() string {
	if e == nil || e.err == nil {
		return "dial failed"
	}
	return e.err.Error()
}

func (e *dialConnectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.reader = nil
	c.mu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// HelloResult returns the negotiated hello result from the active connection.
func (c *Client) HelloResult() HelloResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hello
}

// Hello sends protocol.hello again on the active connection.
func (c *Client) Hello(ctx context.Context) (HelloResult, error) {
	token, err := c.readToken()
	if err != nil {
		return HelloResult{}, err
	}
	var out HelloResult
	err = c.do(ctx, protocol.MethodHello, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token}, &out)
	return out, err
}

func (c *Client) JobSubmit(ctx context.Context, params JobSubmitParams) (JobSubmitResult, error) {
	var out JobSubmitResult
	err := c.do(ctx, protocol.MethodJobSubmit, params, &out)
	return out, err
}

func (c *Client) JobStatus(ctx context.Context, params JobStatusParams) (JobStatusResult, error) {
	var out JobStatusResult
	err := c.do(ctx, protocol.MethodJobStatus, params, &out)
	return out, err
}

func (c *Client) JobResult(ctx context.Context, params JobResultParams) (JobResult, error) {
	var out JobResult
	err := c.do(ctx, protocol.MethodJobResult, params, &out)
	return out, err
}

func (c *Client) JobCancel(ctx context.Context, params JobCancelParams) (JobCancelResult, error) {
	var out JobCancelResult
	err := c.do(ctx, protocol.MethodJobCancel, params, &out)
	return out, err
}

func (c *Client) PolicyValidate(ctx context.Context, params PolicyValidateParams) (PolicyValidateResult, error) {
	var out PolicyValidateResult
	err := c.do(ctx, protocol.MethodPolicyValidate, params, &out)
	return out, err
}

func (c *Client) PolicyRegister(ctx context.Context, params PolicyRegisterParams) (PolicyRegisterResult, error) {
	var out PolicyRegisterResult
	err := c.do(ctx, protocol.MethodPolicyRegister, params, &out)
	return out, err
}

func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}
