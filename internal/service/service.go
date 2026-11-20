//go:build darwin || linux

// Package service exposes the transport and lifecycle for the version-4
// agentbus daemon, including its complete job wire surface.
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
	ProtocolVersion = protocol.Version

	defaultIdleTimeout       = 30 * time.Minute
	defaultShutdown          = 30 * time.Second
	defaultMaxJobs           = 12
	defaultMaxJobsPerBackend = 12
	defaultMaxQueued         = 64
	oversizedWriteTimeout    = time.Second
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
	Release         string
	StateRoot       string
	SocketPath      string
	Token           string
	Backends        []engine.Backend
	BackendDefaults map[string]BackendDefaults

	// CodexAuthHome selects the operator home from which auth.json and
	// config.toml are linked into a managed per-job CODEX_HOME.
	CodexAuthHome string

	IdleTimeout       time.Duration
	IdleCheckInterval time.Duration
	ShutdownTimeout   time.Duration
	MaxJobs           int
	MaxJobsPerBackend int
	MaxQueued         int
	PruneInterval     time.Duration
	ReadyHook         func(ServeReadyInfo) error
}

// BackendDefaults are intentional daemon-side model and effort selections.
// A task's explicit model or effort always takes precedence.
type BackendDefaults struct {
	Model  string
	Effort string
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
	connectionsMu    sync.Mutex
	connections      map[net.Conn]struct{}
	pruneCancel      context.CancelFunc
	pruneWG          sync.WaitGroup
	shutdownOnce     sync.Once
	shutdownDone     chan struct{}
	shutdownErr      error
}

func (lifecycle *serveLifecycle) settleAccept() {
	if lifecycle != nil {
		lifecycle.acceptSettleOnce.Do(func() { close(lifecycle.acceptSettled) })
	}
}

// Server serves protocol version 4 over one private Unix socket.
type Server struct {
	release           string
	stateRoot         string
	socketPath        string
	token             string
	backends          map[string]engine.Backend
	backendDefaults   map[string]BackendDefaults
	backendVersionMu  sync.Mutex
	backendVersions   map[backendVersionCacheKey]backendVersionCacheEntry
	codexAuthHome     string
	codexHomesMu      sync.Mutex
	managedCodexHomes map[string]*managedCodexHome

	jobStoreMu   sync.Mutex
	jobStore     *jobstore.Store
	storeRetired bool

	pruneMu       sync.Mutex
	pruneInterval time.Duration

	// processTable, processGroups, and processGroupGoneFn are the service's restart-recovery
	// dependencies. They deliberately live here instead of in jobstore: the
	// store owns durable records, while service owns the only process action
	// taken from a recovered claim. Tests replace them with deterministic
	// fakes; production uses the native process implementations and leaves
	// processGroupGoneFn nil for its syscall probe.
	processScopeFn     func() (jobstore.ProcessScope, error)
	processTable       engine.ProcessTable
	processGroups      engine.ProcessGroupSignaler
	processGroupGoneFn func(int) (bool, error)
	processGroupGrace  time.Duration
	turnDrainGrace     time.Duration

	executionMu       sync.Mutex
	executionCtx      context.Context
	executionCancel   context.CancelFunc
	executions        map[string]*activeExecution
	executionWG       sync.WaitGroup
	submissionMu      sync.Mutex
	discardMu         sync.Mutex
	queuedJobs        []queuedJob
	runningByBackend  map[string]int
	maxJobs           int
	maxJobsPerBackend int
	maxQueued         int

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
	// Test seams for the revocation transaction and the post-commit removal window.
	discardSessionCommit func(*jobstore.Store, string) (jobstore.Record, error)
	beforeDiscardRemoval func()
	afterDiscardHomeOpen func()

	clients    atomic.Int64
	accepting  atomic.Int64
	activeJobs atomic.Int64

	admissionMu sync.Mutex
	draining    bool
	handlers    int
	handlerWG   sync.WaitGroup
	quietChange chan struct{}

	mu                 sync.Mutex
	lastActivity       time.Time
	executablePath     string
	executableIdentity BinaryIdentity
	binaryStale        bool

	lifecycleMu sync.Mutex
	lifecycle   *serveLifecycle
}

// New creates a daemon server and ensures that its state root and token file
// exist. An existing token always wins over Config.Token.
func New(cfg Config) (*Server, error) {
	if cfg.PruneInterval < 0 {
		return nil, errors.New("prune interval cannot be negative")
	}
	if cfg.MaxJobs < 0 || cfg.MaxJobsPerBackend < 0 || cfg.MaxQueued < 0 {
		return nil, errors.New("max-jobs, max-jobs-per-backend, and max-queued must be positive")
	}
	if cfg.MaxJobs == 0 {
		cfg.MaxJobs = defaultMaxJobs
	}
	if cfg.MaxJobsPerBackend == 0 {
		cfg.MaxJobsPerBackend = defaultMaxJobsPerBackend
	}
	if cfg.MaxQueued == 0 {
		cfg.MaxQueued = defaultMaxQueued
	}
	root, err := ensureStateRoot(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	socketPath := cfg.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(root, protocol.SocketName)
	}
	ownershipStore, err := jobstore.Open(filepath.Join(root, jobStoreFilename))
	if err != nil {
		return nil, fmt.Errorf("validate state root before startup: %w", err)
	}
	if err := ownershipStore.Close(); err != nil {
		return nil, fmt.Errorf("close validated state root: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure state root: %w", err)
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
	backendDefaults := make(map[string]BackendDefaults, len(cfg.BackendDefaults))
	for name, defaults := range cfg.BackendDefaults {
		for field, value := range map[string]string{"model": defaults.Model, "effort": defaults.Effort} {
			if value != "" && !validSelectionValue(value) {
				return nil, fmt.Errorf("invalid %s default for %s", field, name)
			}
		}
		backendDefaults[name] = defaults
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
		release:           cfg.Release,
		stateRoot:         root,
		socketPath:        socketPath,
		token:             token,
		backends:          backends,
		backendDefaults:   backendDefaults,
		backendVersions:   make(map[backendVersionCacheKey]backendVersionCacheEntry),
		codexAuthHome:     cfg.CodexAuthHome,
		managedCodexHomes: make(map[string]*managedCodexHome),
		processTable:      engine.NativeProcessTable{},
		processGroups:     engine.NativeProcessGroupSignaler{},
		processGroupGrace: engine.DefaultCancelGrace,
		turnDrainGrace:    engine.DefaultCancelGrace,
		maxJobs:           cfg.MaxJobs,
		maxJobsPerBackend: cfg.MaxJobsPerBackend,
		maxQueued:         cfg.MaxQueued,
		runningByBackend:  make(map[string]int),
		idleTimeout:       idleTimeout,
		idleCheckInterval: idleCheckInterval,
		shutdownTimeout:   normalizeShutdownTimeout(cfg.ShutdownTimeout),
		pruneInterval:     cfg.PruneInterval,
		readyHook:         cfg.ReadyHook,
		lastActivity:      time.Now().UTC(),
		quietChange:       make(chan struct{}, 1),
	}, nil
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
// binary has drained.
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
	store, err := s.ensureJobStore()
	if err != nil {
		return err
	}
	if err := s.reconcileRecoveredJobs(store); err != nil {
		s.closeJobStore()
		return err
	}
	if err := s.captureBinaryIdentity(); err != nil {
		s.closeJobStore()
		return err
	}
	if err := startupCtx.Err(); err != nil {
		s.closeJobStore()
		return err
	}
	listener, socketIdentity, err := s.listen()
	if err != nil {
		s.closeJobStore()
		return err
	}
	serveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	pruneCtx, pruneCancel := context.WithCancel(serveCtx)
	s.beginExecutions(serveCtx)
	lifecycle := &serveLifecycle{
		listener: listener, socket: socketIdentity, cancel: cancel,
		acceptSettled: make(chan struct{}), serveDone: make(chan struct{}),
		shutdownDone: make(chan struct{}), connections: make(map[net.Conn]struct{}), pruneCancel: pruneCancel,
	}
	if err := s.registerLifecycle(lifecycle); err != nil {
		pruneCancel()
		cancel()
		_ = listener.Close()
		s.removeOwnedSocket(socketIdentity, "duplicate serve")
		s.closeJobStore()
		return err
	}
	defer func() { s.clearLifecycle(lifecycle); close(lifecycle.serveDone) }()
	if err := startupCtx.Err(); err != nil {
		s.startShutdown(lifecycle, false)
		lifecycle.settleAccept()
		<-lifecycle.shutdownDone
		return err
	}
	if s.readyHook != nil {
		if err := s.readyHook(ServeReadyInfo{StateRoot: s.stateRoot, SocketPath: s.socketPath}); err != nil {
			s.startShutdown(lifecycle, false)
			lifecycle.settleAccept()
			<-lifecycle.shutdownDone
			return err
		}
	}
	s.admissionMu.Lock()
	if s.pruneInterval > 0 && !s.draining {
		lifecycle.pruneWG.Add(1)
		go func() { defer lifecycle.pruneWG.Done(); s.automaticPruneLoop(pruneCtx) }()
	}
	s.admissionMu.Unlock()
	s.touchActivity()
	go func() {
		select {
		case <-ctx.Done():
			s.startShutdown(lifecycle, false)
		case <-lifecycle.shutdownDone:
		}
	}()
	go s.idleLoop(serveCtx, cancel, listener, socketIdentity, lifecycle.acceptSettled)
	var acceptErr error
	for {
		conn, err := listener.Accept()
		if err != nil {
			lifecycle.settleAccept()
			s.admissionMu.Lock()
			draining := s.draining
			s.admissionMu.Unlock()
			if !draining {
				acceptErr = err
				s.startShutdown(lifecycle, false)
			}
			break
		}
		s.accepting.Add(1)
		s.clients.Add(1)
		lifecycle.connectionsMu.Lock()
		lifecycle.connections[conn] = struct{}{}
		lifecycle.clients.Add(1)
		lifecycle.connectionsMu.Unlock()
		s.accepting.Add(-1)
		s.touchActivity()
		connection := &connection{server: s, conn: conn}
		go func() {
			defer lifecycle.clients.Done()
			defer s.clients.Add(-1)
			defer s.touchActivity()
			defer func() {
				lifecycle.connectionsMu.Lock()
				delete(lifecycle.connections, conn)
				lifecycle.connectionsMu.Unlock()
			}()
			connection.serve()
		}()
	}
	<-lifecycle.shutdownDone
	return errors.Join(acceptErr, lifecycle.shutdownErr)
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

// Shutdown closes admission first and keeps the listener responsive until the
// store has been released. A caller deadline bounds the wait, not the cleanup.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return ErrShutdownNotServing
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.lifecycleMu.Lock()
	lifecycle := s.lifecycle
	s.lifecycleMu.Unlock()
	if lifecycle == nil {
		return ErrShutdownNotServing
	}
	s.startShutdown(lifecycle, false)
	if err := waitForChannel(ctx, lifecycle.shutdownDone); err != nil {
		return shutdownError(err)
	}
	if err := waitForChannel(ctx, lifecycle.serveDone); err != nil {
		return shutdownError(err)
	}
	return lifecycle.shutdownErr
}

// startShutdown atomically fences method admission. Upgrade and idle drains
// recheck quietness while holding the same gate used to admit handlers.
func (s *Server) startShutdown(lifecycle *serveLifecycle, onlyIfQuiet bool) bool {
	s.admissionMu.Lock()
	if onlyIfQuiet && (s.handlers != 0 || s.activeJobs.Load() != 0 || s.accepting.Load() != 0) {
		s.admissionMu.Unlock()
		return false
	}
	s.draining = true
	lifecycle.shutdownOnce.Do(func() {
		lifecycle.pruneCancel()
		go s.finishShutdown(lifecycle)
	})
	s.admissionMu.Unlock()
	return true
}

func (s *Server) finishShutdown(lifecycle *serveLifecycle) {
	s.handlerWG.Wait()
	s.stopExecutions()
	lifecycle.pruneWG.Wait()
	s.jobStoreMu.Lock()
	store := s.jobStore
	s.jobStore = nil
	s.storeRetired = true
	var err error
	if store != nil {
		err = store.Close()
	}
	s.jobStoreMu.Unlock()
	if closeErr := lifecycle.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
		err = errors.Join(err, closeErr)
	}
	<-lifecycle.acceptSettled
	lifecycle.connectionsMu.Lock()
	for conn := range lifecycle.connections {
		_ = conn.Close()
	}
	lifecycle.connectionsMu.Unlock()
	lifecycle.clients.Wait()
	if pidErr := s.removeOwnedPIDFile(context.Background(), "daemon shutdown"); pidErr != nil {
		err = errors.Join(err, pidErr)
	}
	s.removeOwnedSocket(lifecycle.socket, "daemon shutdown")
	lifecycle.cancel()
	lifecycle.shutdownErr = err
	close(lifecycle.shutdownDone)
}

func (s *Server) beginRequest() bool {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if s.draining {
		return false
	}
	s.handlers++
	s.handlerWG.Add(1)
	return true
}

func (s *Server) endRequest() {
	s.admissionMu.Lock()
	s.handlers--
	s.handlerWG.Done()
	s.admissionMu.Unlock()
	s.notifyQuietChange()
}

func (s *Server) notifyQuietChange() {
	select {
	case s.quietChange <- struct{}{}:
	default:
	}
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
	stale := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale = s.checkBinaryStale()
		case <-s.quietChange:
		}
		if !stale {
			if s.idleTimeout < 0 {
				continue
			}
			// An idle client keeps the daemon alive. Upgrade drains, in
			// contrast, need only wait for admitted work to settle.
			if s.clients.Load() != 0 {
				continue
			}
			s.mu.Lock()
			last := s.lastActivity
			s.mu.Unlock()
			if time.Now().UTC().Before(last.Add(s.idleTimeout)) {
				continue
			}
		}
		s.lifecycleMu.Lock()
		lifecycle := s.lifecycle
		s.lifecycleMu.Unlock()
		if lifecycle != nil {
			if s.startShutdown(lifecycle, true) {
				return
			}
			continue
		}
		// The standalone idle-loop test supplies its own listener.
		if s.activeJobs.Load() == 0 && s.clients.Load() == 0 && s.accepting.Load() == 0 {
			_ = listener.Close()
			select {
			case <-acceptSettled:
			case <-ctx.Done():
				return
			}
			cancel()
			return
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
		log.Print("agentbus daemon: binary on disk changed; will exit after accepted work settles")
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
