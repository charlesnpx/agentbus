//go:build darwin || linux

// Package service exposes the transport and lifecycle for the version-3
// agentbus daemon. It serves protocol.hello and durable job.submit; later
// units add the remaining job methods.
package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	// ProtocolVersion is the only protocol version served by this daemon.
	ProtocolVersion = 3

	defaultIdleTimeout    = 30 * time.Minute
	defaultShutdown       = 30 * time.Second
	oversizedWriteTimeout = time.Second
)

var (
	// ErrDaemonAlreadyListening reports an occupied state root with a live peer.
	ErrDaemonAlreadyListening = errors.New("agentbus daemon already listening")
	// ErrShutdownDeadlineExceeded reports a graceful shutdown that did not drain
	// before its caller's deadline.
	ErrShutdownDeadlineExceeded = errors.New("agentbus graceful shutdown deadline exceeded")
	// ErrShutdownNotServing reports Shutdown before the daemon has started or
	// after it has stopped.
	ErrShutdownNotServing = errors.New("agentbus daemon is not serving")
	// ErrShutdownPIDTeardownFailed reports an error while removing an owned PID
	// file. The file is deliberately retained when ownership cannot be proven.
	ErrShutdownPIDTeardownFailed = errors.New("agentbus graceful shutdown pid teardown failed")
)

// DaemonAlreadyListeningError identifies the live socket that prevented
// startup. It satisfies errors.Is(err, ErrDaemonAlreadyListening).
type DaemonAlreadyListeningError struct {
	SocketPath string
}

func (e DaemonAlreadyListeningError) Error() string {
	if e.SocketPath == "" {
		return ErrDaemonAlreadyListening.Error()
	}
	return fmt.Sprintf("%s at %s", ErrDaemonAlreadyListening, e.SocketPath)
}

func (e DaemonAlreadyListeningError) Is(target error) bool {
	return target == ErrDaemonAlreadyListening
}

// BinaryIdentity identifies an executable on disk by metadata that changes
// when a replacement binary is installed.
type BinaryIdentity struct {
	ModTime time.Time
	Size    int64
}

// Config configures the one local JSON-RPC daemon.
type Config struct {
	StateRoot  string
	SocketPath string
	Token      string
	Backends   []engine.Backend

	// CodexHomeOverride selects a fixed CODEX_HOME instead of a per-job home.
	// It is intended for an operator-owned compatibility configuration.
	CodexHomeOverride string
	// CodexHomeInherit preserves the historical inherited CODEX_HOME behavior.
	// It wins over CodexHomeOverride when both are configured.
	CodexHomeInherit bool
	// CodexAuthHome selects the operator home from which auth.json and
	// config.toml are linked into a managed per-job CODEX_HOME.
	CodexAuthHome string

	IdleTimeout       time.Duration
	IdleCheckInterval time.Duration
	ShutdownTimeout   time.Duration
	ReadyHook         func(ServeReadyInfo) error
}

// ServeReadyInfo identifies the ready daemon for startup launchers.
type ServeReadyInfo struct {
	StateRoot  string
	SocketPath string
}

type socketFileIdentity struct {
	dev uint64
	ino uint64
}

type serveLifecycle struct {
	listener      net.Listener
	socket        socketFileIdentity
	cancel        context.CancelFunc
	acceptSettled chan struct{}
	serveDone     chan struct{}

	acceptSettleOnce sync.Once
	clients          sync.WaitGroup

	shutdownMu   sync.Mutex
	shutdownDone chan struct{}
	shutdownErr  error
}

func (lifecycle *serveLifecycle) settleAccept() {
	if lifecycle != nil {
		lifecycle.acceptSettleOnce.Do(func() { close(lifecycle.acceptSettled) })
	}
}

// Server serves protocol version 3 over one private Unix socket.
type Server struct {
	stateRoot  string
	socketPath string
	token      string
	backends   map[string]engine.Backend

	codexHomeOverride string
	codexHomeInherit  bool
	codexAuthHome     string
	codexHomesMu      sync.Mutex
	managedCodexHomes map[string]*managedCodexHome

	jobStoreMu sync.Mutex
	jobStore   *jobstore.Store

	executionMu     sync.Mutex
	executionCtx    context.Context
	executionCancel context.CancelFunc
	executions      map[string]*activeExecution
	executionWG     sync.WaitGroup

	idleTimeout       time.Duration
	idleCheckInterval time.Duration
	shutdownTimeout   time.Duration
	readyHook         func(ServeReadyInfo) error

	// beforeStaleSocketRemovalHook lets the stale-replacement regression test
	// hold the winner in the final removal window while it holds the state-root
	// socket lock.
	beforeStaleSocketRemovalHook func()
	// beforeSocketStateFlockHook lets the stale-replacement regression test
	// establish that a loser encounters the winner's held flock before release.
	beforeSocketStateFlockHook func(*os.File)

	clients    atomic.Int64
	accepting  atomic.Int64
	activeJobs atomic.Int64

	mu                 sync.Mutex
	lastActivity       time.Time
	executablePath     string
	executableIdentity BinaryIdentity
	binaryStale        bool

	lifecycleMu  sync.Mutex
	lifecycle    *serveLifecycle
	shuttingDown *serveLifecycle
}

// New creates a daemon server and ensures that its state root and token file
// exist. An existing token always wins over Config.Token.
func New(cfg Config) (*Server, error) {
	root, err := ensureStateRoot(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	socketPath := cfg.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(root, protocol.SocketName)
	}
	tokenPath := filepath.Join(root, protocol.TokenFileName)
	token, err := ensureToken(tokenPath, cfg.Token)
	if err != nil {
		return nil, err
	}
	backends := make(map[string]engine.Backend, len(cfg.Backends))
	for _, backend := range cfg.Backends {
		if backend != nil && backend.Name() != "" {
			backends[backend.Name()] = backend
		}
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}
	idleCheckInterval := cfg.IdleCheckInterval
	if idleCheckInterval == 0 {
		idleCheckInterval = time.Minute
		if idleTimeout > 0 && idleTimeout < idleCheckInterval {
			idleCheckInterval = idleTimeout / 4
			if idleCheckInterval <= 0 {
				idleCheckInterval = idleTimeout
			}
		}
	}
	return &Server{
		stateRoot:         root,
		socketPath:        socketPath,
		token:             token,
		backends:          backends,
		codexHomeOverride: cfg.CodexHomeOverride,
		codexHomeInherit:  cfg.CodexHomeInherit,
		codexAuthHome:     cfg.CodexAuthHome,
		managedCodexHomes: make(map[string]*managedCodexHome),
		idleTimeout:       idleTimeout,
		idleCheckInterval: idleCheckInterval,
		shutdownTimeout:   normalizeShutdownTimeout(cfg.ShutdownTimeout),
		readyHook:         cfg.ReadyHook,
		lastActivity:      time.Now().UTC(),
	}, nil
}

// SocketPath returns the protocol socket path for stateRoot.
func SocketPath(stateRoot string) (string, error) {
	if stateRoot == "" {
		var err error
		stateRoot, err = engine.ResolveStateRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(stateRoot, protocol.SocketName), nil
}

// TokenPath returns the protocol token path for stateRoot.
func TokenPath(stateRoot string) (string, error) {
	if stateRoot == "" {
		var err error
		stateRoot, err = engine.ResolveStateRoot()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(stateRoot, protocol.TokenFileName), nil
}

// ShutdownTimeout is the default bound a host should apply when it turns a
// canceled process context into a graceful shutdown request.
func (s *Server) ShutdownTimeout() time.Duration {
	if s == nil {
		return defaultShutdown
	}
	return s.shutdownTimeout
}

// Serve listens until ctx is canceled, an idle timeout fires, or a stale
// binary has drained. It serves protocol.hello and job.submit.
func (s *Server) Serve(ctx context.Context) error {
	return s.serve(ctx, ctx)
}

// ServeWithStartupContext uses startupCtx through the readiness boundary and
// ctx for the daemon lifetime after readiness has been published.
func (s *Server) ServeWithStartupContext(ctx, startupCtx context.Context) error {
	return s.serve(ctx, startupCtx)
}

func (s *Server) serve(ctx, startupCtx context.Context) error {
	if s == nil {
		return errors.New("nil service server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if startupCtx == nil {
		startupCtx = ctx
	}
	_, err := s.ensureJobStore()
	if err != nil {
		return err
	}
	defer s.closeJobStore()
	// Startup sweeping is deliberately absent: SIGKILL-orphaned leaves under
	// the Codex layout are a known residual, and removing them is an operator
	// action rather than a daemon guess.
	if err := s.captureBinaryIdentity(); err != nil {
		return err
	}
	if err := startupCtx.Err(); err != nil {
		return err
	}
	listener, socketIdentity, err := s.listen()
	if err != nil {
		return err
	}
	serveCtx, cancel := context.WithCancel(ctx)
	s.beginExecutions(serveCtx)
	lifecycle := &serveLifecycle{
		listener:      listener,
		socket:        socketIdentity,
		cancel:        cancel,
		acceptSettled: make(chan struct{}),
		serveDone:     make(chan struct{}),
	}
	if err := s.registerLifecycle(lifecycle); err != nil {
		cancel()
		_ = listener.Close()
		s.removeOwnedSocket(socketIdentity, "duplicate serve")
		return err
	}
	defer func() {
		lifecycle.settleAccept()
		_ = listener.Close()
		s.removeOwnedSocket(socketIdentity, "server shutdown")
		cancel()
		s.clearLifecycle(lifecycle)
		close(lifecycle.serveDone)
	}()

	if err := startupCtx.Err(); err != nil {
		return err
	}
	if s.readyHook != nil {
		if err := s.readyHook(ServeReadyInfo{StateRoot: s.stateRoot, SocketPath: s.socketPath}); err != nil {
			return err
		}
	}
	s.touchActivity()
	go func() {
		<-serveCtx.Done()
		_ = listener.Close()
	}()
	go s.idleLoop(serveCtx, cancel, listener, socketIdentity, lifecycle.acceptSettled)

	for {
		conn, err := listener.Accept()
		if err != nil {
			lifecycle.settleAccept()
			if serveCtx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				<-serveCtx.Done()
				return nil
			}
			return err
		}
		s.accepting.Add(1)
		s.clients.Add(1)
		s.accepting.Add(-1)
		s.touchActivity()
		lifecycle.clients.Add(1)
		connection := &connection{server: s, conn: conn}
		go func() {
			defer lifecycle.clients.Done()
			defer s.clients.Add(-1)
			defer s.touchActivity()
			connection.serve()
		}()
	}
}

func (s *Server) registerLifecycle(lifecycle *serveLifecycle) error {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycle != nil {
		return errors.New("agentbus daemon is already serving")
	}
	s.lifecycle = lifecycle
	return nil
}

func (s *Server) clearLifecycle(lifecycle *serveLifecycle) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.lifecycle == lifecycle {
		s.lifecycle = nil
	}
}

// Shutdown stops accepting connections, waits for accepted connections to
// finish, conditionally removes the PID file, and ends Serve.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return ErrShutdownNotServing
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	lifecycle := s.lifecycle
	if lifecycle == nil {
		lifecycle = s.shuttingDown
	}
	if lifecycle == nil {
		s.lifecycleMu.Unlock()
		return ErrShutdownNotServing
	}
	lifecycle.shutdownMu.Lock()
	if lifecycle.shutdownDone != nil {
		done := lifecycle.shutdownDone
		lifecycle.shutdownMu.Unlock()
		s.lifecycleMu.Unlock()
		return waitForShutdown(ctx, lifecycle, done)
	}
	done := make(chan struct{})
	lifecycle.shutdownDone = done
	s.shuttingDown = lifecycle
	lifecycle.shutdownMu.Unlock()
	s.lifecycleMu.Unlock()

	err := s.shutdownLifecycle(ctx, lifecycle)
	lifecycle.shutdownMu.Lock()
	lifecycle.shutdownErr = err
	close(done)
	lifecycle.shutdownMu.Unlock()
	s.lifecycleMu.Lock()
	if s.shuttingDown == lifecycle {
		s.shuttingDown = nil
	}
	s.lifecycleMu.Unlock()
	return err
}

func waitForShutdown(ctx context.Context, lifecycle *serveLifecycle, done <-chan struct{}) error {
	select {
	case <-done:
		lifecycle.shutdownMu.Lock()
		err := lifecycle.shutdownErr
		lifecycle.shutdownMu.Unlock()
		return err
	case <-ctx.Done():
		return shutdownError(ctx.Err())
	}
}

func (s *Server) shutdownLifecycle(ctx context.Context, lifecycle *serveLifecycle) (err error) {
	defer lifecycle.cancel()
	if closeErr := lifecycle.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
		log.Printf("agentbus daemon: close listener during graceful shutdown: %v", closeErr)
	}
	s.removeOwnedSocket(lifecycle.socket, "graceful shutdown")
	if err := waitForChannel(ctx, lifecycle.acceptSettled); err != nil {
		return shutdownError(err)
	}
	clientsDone := make(chan struct{})
	go func() {
		lifecycle.clients.Wait()
		close(clientsDone)
	}()
	if err := waitForChannel(ctx, clientsDone); err != nil {
		return shutdownError(err)
	}
	if err := s.removeOwnedPIDFile(ctx, "graceful shutdown"); err != nil {
		return shutdownError(err)
	}
	// A listener closed by graceful shutdown deliberately leaves the accept loop
	// waiting for this cancellation until accepted connections and PID teardown
	// have completed. Cancel before waiting for Serve's final lifecycle receipt.
	lifecycle.cancel()
	if err := waitForChannel(ctx, lifecycle.serveDone); err != nil {
		return shutdownError(err)
	}
	return nil
}

func waitForChannel(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func shutdownError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrShutdownDeadlineExceeded, err)
	}
	return err
}

func normalizeShutdownTimeout(timeout time.Duration) time.Duration {
	if timeout < 0 {
		return 0
	}
	if timeout == 0 {
		return defaultShutdown
	}
	return timeout
}

func (s *Server) listen() (net.Listener, socketFileIdentity, error) {
	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, socketFileIdentity{}, err
	}
	if err := os.Chmod(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, socketFileIdentity{}, err
	}
	socketLock, err := s.lockSocketState()
	if err != nil {
		return nil, socketFileIdentity{}, err
	}
	defer socketLock.Close()
	defer func() { _ = unlockSocketState(socketLock) }()

	if _, err := os.Lstat(s.socketPath); err == nil {
		if conn, dialErr := net.DialTimeout("unix", s.socketPath, 100*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
		}
		if s.beforeStaleSocketRemovalHook != nil {
			s.beforeStaleSocketRemovalHook()
		}
		if err := os.Remove(s.socketPath); err != nil {
			return nil, socketFileIdentity{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, socketFileIdentity{}, err
	}
	listener, identity, err := listenUnixSocketPrivate(s.socketPath)
	if err != nil {
		if isAddrInUse(err) {
			return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
		}
		return nil, socketFileIdentity{}, err
	}
	if err := os.Chmod(s.socketPath, 0o600); err != nil {
		_ = listener.Close()
		removeSocketPathIfIdentityLocked(s.socketPath, identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	return listener, identity, nil
}

func (s *Server) idleLoop(ctx context.Context, cancel context.CancelFunc, listener net.Listener, socketIdentity socketFileIdentity, acceptSettled <-chan struct{}) {
	ticker := time.NewTicker(s.idleCheckInterval)
	defer ticker.Stop()
	staleDraining := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := s.checkBinaryStale()
			quiet := s.clients.Load() == 0 && s.accepting.Load() == 0 && s.activeJobs.Load() == 0
			if staleDraining {
				settled := false
				select {
				case <-acceptSettled:
					settled = true
				default:
				}
				if !quiet || !settled {
					continue
				}
				cancel()
				return
			}
			if !quiet {
				s.touchActivity()
				continue
			}
			if stale {
				if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					log.Printf("agentbus daemon: close stale listener: %v", err)
				}
				s.removeOwnedSocket(socketIdentity, "stale daemon listener close")
				staleDraining = true
				continue
			}
			if s.idleTimeout < 0 {
				continue
			}
			s.mu.Lock()
			last := s.lastActivity
			s.mu.Unlock()
			if !time.Now().UTC().Before(last.Add(s.idleTimeout)) {
				cancel()
				_ = listener.Close()
				return
			}
		}
	}
}

func (s *Server) touchActivity() {
	s.mu.Lock()
	s.lastActivity = time.Now().UTC()
	s.mu.Unlock()
}

func statBinaryIdentity(path string) (BinaryIdentity, error) {
	info, err := os.Stat(path)
	if err != nil {
		return BinaryIdentity{}, err
	}
	return BinaryIdentity{ModTime: info.ModTime(), Size: info.Size()}, nil
}

func (s *Server) captureBinaryIdentity() error {
	path, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find daemon executable: %w", err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve daemon executable %q: %w", path, err)
	}
	identity, err := statBinaryIdentity(path)
	if err != nil {
		return fmt.Errorf("stat daemon executable %q: %w", path, err)
	}
	s.mu.Lock()
	s.executablePath = path
	s.executableIdentity = identity
	s.mu.Unlock()
	return nil
}

func (s *Server) checkBinaryStale() bool {
	s.mu.Lock()
	if s.binaryStale {
		s.mu.Unlock()
		return true
	}
	path := s.executablePath
	expected := s.executableIdentity
	s.mu.Unlock()

	actual, err := statBinaryIdentity(path)
	if err == nil && actual.Size == expected.Size && actual.ModTime.Equal(expected.ModTime) {
		return false
	}
	s.mu.Lock()
	if !s.binaryStale {
		s.binaryStale = true
		log.Print("agentbus daemon: binary on disk changed; will restart when idle")
	}
	s.mu.Unlock()
	return true
}

func (s *Server) backendNames() []string {
	names := make([]string, 0, len(s.backends))
	for name := range s.backends {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Server) backendMetadata() []protocol.BackendInfo {
	names := s.backendNames()
	metadata := make([]protocol.BackendInfo, 0, len(names))
	for _, name := range names {
		info := protocol.BackendInfo{Name: name, Models: []string{}, Efforts: []string{}}
		if provider, ok := s.backends[name].(engine.BackendMetadataProvider); ok {
			providerMetadata := provider.BackendMetadata(context.Background())
			info.Models = append([]string{}, providerMetadata.Models...)
			info.Efforts = append([]string{}, providerMetadata.Efforts...)
		}
		metadata = append(metadata, info)
	}
	return metadata
}
