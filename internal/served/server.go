package served

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	defaultLeaseDuration = 5 * time.Minute
	defaultHeartbeat     = 30 * time.Second
	defaultSafetyDrain   = 30 * time.Second
	defaultShutdown      = 30 * time.Second

	admissionNativeInterruptGrace = 2 * time.Second
	oversizedRequestWriteTimeout  = time.Second
)

var ErrDaemonAlreadyListening = errors.New("agentbus daemon already listening")

var ErrShutdownDeadlineExceeded = errors.New("agentbus graceful shutdown deadline exceeded")

var ErrShutdownNotServing = errors.New("agentbus daemon is not serving")

var ErrShutdownPIDTeardownFailed = errors.New("agentbus graceful shutdown pid teardown failed")

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

var ErrAdmissionRootBusy = errors.New("agentbus admission root busy")

type AdmissionRootBusyError struct {
	Path       string
	SocketPath string
	Cause      error
}

func (e AdmissionRootBusyError) Error() string {
	message := ErrAdmissionRootBusy.Error()
	if e.Path != "" {
		message = fmt.Sprintf("%s: %s", message, e.Path)
	}
	if e.SocketPath != "" {
		message = fmt.Sprintf("%s; no listening daemon at %s", message, e.SocketPath)
	}
	return message
}

func (e AdmissionRootBusyError) Is(target error) bool {
	return target == ErrAdmissionRootBusy
}

func (e AdmissionRootBusyError) Unwrap() error {
	return e.Cause
}

func admissionSocketDialable(socketPath string) bool {
	return admissionSocketDialableWithin(socketPath, 100*time.Millisecond)
}

func admissionSocketDialableWithin(socketPath string, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	if _, err := os.Lstat(socketPath); err != nil {
		return false
	}
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// BinaryIdentity identifies the on-disk daemon executable by metadata that
// changes when a replacement binary is installed.
type BinaryIdentity struct {
	ModTime time.Time
	Size    int64
}

type socketFileIdentity struct {
	dev uint64
	ino uint64
}

// BinaryIdentityProbe reads the identity of the executable at path. It is
// configurable so tests can model a replaced or removed binary.
type BinaryIdentityProbe func(path string) (BinaryIdentity, error)

// Config configures the local JSON-RPC daemon.
type Config struct {
	StateRoot  string
	CWD        string
	SocketPath string
	Token      string
	// CodexHomeOverride sets a fixed CODEX_HOME for all Codex jobs. When empty,
	// each job receives a private home under its workspace namespace.
	CodexHomeOverride string
	// CodexHomeInherit disables Codex home isolation and preserves the process
	// environment exactly as it was before this setting existed.
	CodexHomeInherit bool
	// CodexAuthHome is the operator Codex home from which auth.json and
	// config.toml are linked. Empty resolves from ambient CODEX_HOME, then
	// ~/.codex. It exists so embedded callers can avoid consulting process
	// environment state.
	CodexAuthHome       string
	Backends            []engine.Backend
	Registry            *engine.PolicyRegistry
	Clock               engine.Clock
	ProcessTable        engine.ProcessTable
	ProcessGroups       engine.ProcessGroupSignaler
	CancelGrace         time.Duration
	CancelWaiter        engine.Waiter
	IdleTimeout         time.Duration
	IdleCheckInterval   time.Duration
	BinaryIdentityProbe BinaryIdentityProbe
	InlineResultCap     int
	LeaseDuration       time.Duration
	HeartbeatInterval   time.Duration
	GCInterval          time.Duration
	ReadyHook           func(ServeReadyInfo) error
	ShutdownTimeout     time.Duration
	// Runtime is an injected strict-admission runtime owned by Serve. A runtime
	// with real close semantics is single-use: once a Serve or recovery-only
	// admin run closes it, a later Serve on the same Server/config is rejected
	// with ErrRuntimeConsumed. custodian.UnavailableRuntime has a no-op close and
	// is reusable for repeated fail-closed Serves.
	Runtime     custodian.Runtime
	ProbeRunner command.ProbeRunner
}

type ServeReadyInfo struct {
	StateRoot  string
	SocketPath string
}

type admissionOwnedWorkChecker interface {
	HasOwnedWork(context.Context) (bool, error)
}

type serveShutdownState struct {
	generation uint64
	lifecycle  serveLifecycleSnapshot
	done       chan struct{}
	err        error
	complete   bool
}

type serveLifecycleSnapshot struct {
	generation uint64
	listener   net.Listener
	socket     socketFileIdentity
	pidFile    pidFileSnapshot
	cancel     context.CancelFunc
	admission  *serveAdmissionSnapshot
	bound      bool
}

type pidFileSnapshot struct {
	identity socketFileIdentity
	known    bool
}

type serveAdmissionSnapshot struct {
	instance     *admissionInstance
	ready        *admissionReady
	coordinator  *admissionCoordinator
	checker      admissionOwnedWorkChecker
	runtime      *servedAdmissionRuntime
	closer       io.Closer
	closeStarted atomic.Bool
	closeErr     error
}

// Server serves the protocol v1 socket API over engine backends.
type Server struct {
	stateRoot                    string
	cwd                          string
	socketPath                   string
	tokenPath                    string
	token                        string
	codexHomeOverride            string
	codexHomeInherit             bool
	codexAuthHome                string
	backendMapMu                 sync.RWMutex
	backends                     map[string]engine.Backend
	registry                     *engine.PolicyRegistry
	clock                        engine.Clock
	processes                    engine.ProcessTable
	processGroups                engine.ProcessGroupSignaler
	cancelGrace                  time.Duration
	cancelWaiter                 engine.Waiter
	id                           atomic.Uint64
	clients                      atomic.Int64
	accepting                    atomic.Int64
	idleTimeout                  time.Duration
	idleCheckInterval            time.Duration
	binaryIdentityProbe          BinaryIdentityProbe
	beforeStaleCloseHook         func()
	staleListenerHook            func()
	staleSocketRemovedHook       func()
	beforePIDFileQuarantineHook  func()
	readPIDFileNoFollowHook      func(string) ([]byte, socketFileIdentity, error)
	inlineResultCap              int
	leaseDuration                time.Duration
	heartbeatInterval            time.Duration
	gcInterval                   time.Duration
	admissionLogRetentionCap     int64
	admissionLogRetentionMu      [admissionLogRetentionLockStripes]sync.Mutex
	readyHook                    func(ServeReadyInfo) error
	listenerFactory              func() (net.Listener, socketFileIdentity, error)
	unixSocketPrivateListenHooks unixSocketPrivateListenHooks
	beforeListenBindHook         func()
	safetyLatch                  *SafetyLatch
	safetyDrainTimeout           time.Duration
	shutdownTimeout              time.Duration
	jobsRequestIDEnabled         bool
	admissionSubmitMu            sync.Mutex
	admissionCloseEpoch          atomic.Uint64
	admissionOpenEpoch           atomic.Uint64
	admissionStateMu             sync.RWMutex
	resultPublications           atomic.Int64

	admissionBootstrapper        *admissionBootstrapper
	admissionReady               *admissionReady
	admissionCoordinator         *admissionCoordinator
	admissionOwnedWorkChecker    admissionOwnedWorkChecker
	admissionSubmission          *servedSubmissionCoordinator
	admissionRuntime             *servedAdmissionRuntime
	admissionInstance            *admissionInstance
	admissionRuntimeFactory      func(*Server) *servedAdmissionRuntime
	admissionRuntimeConfig       custodian.Runtime
	admissionProbeRunner         command.ProbeRunner
	admissionUnprobeableBackends map[string]error
	admissionStartupHooks        admissionStartupHooks
	admissionDaemonBootOnce      sync.Once
	admissionDaemonBootRef       model.BootRef
	admissionDaemonBootRefErr    error
	admissionRepository          repository.Repository
	admissionClose               io.Closer
	admissionBootstrapperFactory admissionBootstrapperFactory

	serveStateMu    sync.Mutex
	serveListener   net.Listener
	serveSocket     socketFileIdentity
	serveCancel     context.CancelFunc
	serveGeneration uint64
	shutdownMu      sync.Mutex
	shutdownRunMu   sync.Mutex
	shutdownState   *serveShutdownState

	mu                 sync.Mutex
	stores             map[string]*engine.Store
	storesByKey        map[string]*engine.Store
	jobStores          map[string]*engine.Store
	admissionJobs      map[string]*admissionInstance
	admissionEffectMu  map[string]*sync.Mutex
	activeJobs         map[string]*activeJob
	admissionLogDrains map[string]<-chan struct{}
	jobLiveness        map[string]*jobLiveness
	reportedModels     map[string]string
	reportedModelOrder []string
	lastActivity       time.Time
	executablePath     string
	executableIdentity BinaryIdentity
	binaryStale        bool
}

type jobLiveness struct {
	startedAt   time.Time
	lastEventAt time.Time
	eventCount  int
}

type activeJob struct {
	jobID     string
	sessionID string
	session   engine.Session
	cancel    context.CancelFunc

	mu                              sync.Mutex
	terminal                        engine.JobState
	cancellation                    terminalCancellation
	admissionCommand                command.RunningCommand
	containmentIntent               *launch.ContainmentIntent
	observedWorkspaceWriteItemCount uint64
	observedWorkspaceWriteAttempt   model.LaunchOrdinal
	diagnosticsSettleRequest        chan struct{}
	diagnosticsSettled              chan struct{}
	diagnosticsSettleRequested      bool
	// These hooks only coordinate deterministic activeJob tests around the pair's
	// synchronization boundary. The reset hook runs while mu is held; the
	// snapshot hook runs immediately before it is acquired.
	observedWorkspaceWriteAfterCountResetForTest        func()
	observedWorkspaceWriteBeforeTerminalSnapshotForTest func()
}

type nativeInterruptSession interface {
	NativeInterrupt(context.Context) (bool, error)
}

func (j *activeJob) requestTerminal(state engine.JobState, metadata ...terminalCancellation) {
	cancellation := terminalCancellationFor(engine.CancellationOriginUnattributable, "canceled without an attributable origin")
	if len(metadata) > 0 {
		cancellation = metadata[0]
	}
	j.mu.Lock()
	if j.terminal == "" {
		j.terminal = state
		if state == engine.StateCanceled {
			j.cancellation = cancellation
		}
	}
	intent := j.containmentIntent
	j.mu.Unlock()
	if state == engine.StateCanceled {
		intent.MarkContaining()
	}
}

func (j *activeJob) requestedTerminal() engine.JobState {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.terminal
}

func (j *activeJob) requestedCancellation() terminalCancellation {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cancellation
}

func (j *activeJob) observeWorkspaceWriteItem() uint64 {
	if j == nil {
		return 1
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.observedWorkspaceWriteItemCount == ^uint64(0) {
		return j.observedWorkspaceWriteItemCount
	}
	j.observedWorkspaceWriteItemCount++
	return j.observedWorkspaceWriteItemCount
}

func (j *activeJob) beginObservedWorkspaceWriteAttempt(ordinal model.LaunchOrdinal) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.observedWorkspaceWriteItemCount = 0
	if j.observedWorkspaceWriteAfterCountResetForTest != nil {
		j.observedWorkspaceWriteAfterCountResetForTest()
	}
	j.observedWorkspaceWriteAttempt = ordinal
}

func (j *activeJob) observedWorkspaceWriteItemCountForTerminal() (uint64, model.LaunchOrdinal) {
	if j == nil {
		return 0, 0
	}
	if j.observedWorkspaceWriteBeforeTerminalSnapshotForTest != nil {
		j.observedWorkspaceWriteBeforeTerminalSnapshotForTest()
	}
	j.mu.Lock()
	if !j.observedWorkspaceWriteAttempt.Valid() {
		j.mu.Unlock()
		return 0, 0
	}
	count, ordinal := j.observedWorkspaceWriteItemCount, j.observedWorkspaceWriteAttempt
	j.mu.Unlock()
	return count, ordinal
}

func activeObservedWorkspaceWriteItemCount(job *activeJob) (uint64, model.LaunchOrdinal) {
	if job == nil {
		return 0, 0
	}
	return job.observedWorkspaceWriteItemCountForTerminal()
}

// beginAdmissionDiagnosticsSettle makes this attempt available to terminal
// paths that need its buffered diagnostics before committing an absorbing
// terminal record. The caller must invoke finish once it will no longer read
// the stream.
func (j *activeJob) beginAdmissionDiagnosticsSettle() (<-chan struct{}, func()) {
	if j == nil {
		return nil, func() {}
	}
	j.mu.Lock()
	request := make(chan struct{})
	settled := make(chan struct{})
	j.diagnosticsSettleRequest = request
	j.diagnosticsSettled = settled
	j.diagnosticsSettleRequested = false
	j.mu.Unlock()
	return request, func() {
		j.mu.Lock()
		if j.diagnosticsSettled == settled {
			close(settled)
		}
		j.mu.Unlock()
	}
}

// settleAdmissionDiagnostics asks the active attempt to drain buffered
// diagnostics before terminalization. A backend that continues streaming past
// the grace interval, or past the caller's shutdown deadline, is intentionally
// left to the deferred drain.
func (j *activeJob) settleAdmissionDiagnostics(ctx context.Context, bound time.Duration) {
	if j == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	j.mu.Lock()
	request := j.diagnosticsSettleRequest
	settled := j.diagnosticsSettled
	if request != nil && !j.diagnosticsSettleRequested {
		close(request)
		j.diagnosticsSettleRequested = true
	}
	j.mu.Unlock()
	if settled == nil {
		return
	}
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-settled:
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (j *activeJob) recordAdmissionCommand(cmd command.RunningCommand) bool {
	if j == nil || cmd == nil {
		return false
	}
	j.mu.Lock()
	j.admissionCommand = cmd
	shouldInterrupt := j.terminal == engine.StateCanceled
	intent := j.containmentIntent
	j.mu.Unlock()
	if shouldInterrupt {
		intent.MarkContaining()
	}
	return shouldInterrupt
}

func (j *activeJob) interruptAdmissionCommand(ctx context.Context) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	cmd := j.admissionCommand
	j.mu.Unlock()
	if cmd == nil {
		return nil
	}
	return cmd.Interrupt(ctx)
}

func (j *activeJob) interruptSessionNativeFirst() bool {
	if j == nil || j.session == nil {
		return false
	}
	session, ok := j.session.(nativeInterruptSession)
	if !ok {
		return false
	}
	jobID := j.jobID
	// NativeInterrupt is provider-only and never performs OS containment. Keep
	// this detached and locally bounded so a launch mutex or blocked native write
	// cannot stall the caller before the foreground interruptAdmissionCommand
	// performs the ctx-bounded containment when settlement is not confirmed.
	done := make(chan bool, 1)
	go func() {
		settled, err := session.NativeInterrupt(context.Background())
		if err != nil {
			log.Printf("agentbus daemon: job %s native session interrupt warning: %v", jobID, err)
		}
		done <- settled
	}()
	timer := time.NewTimer(admissionNativeInterruptGrace)
	defer timer.Stop()
	select {
	case settled := <-done:
		return settled
	case <-timer.C:
		return false
	}
}

func admissionPhysicalCleanupUncertain(err error) bool {
	return custodian.IsCleanupUnresolved(err) || errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved)
}

type requestOutcome struct {
	result       any
	err          *protocol.ErrorObject
	after        func()
	onAckFailure func(error)
}

type resolvedPolicy struct {
	policy   *engine.TurnPolicy
	contract *engine.ContractSpec
	name     string
	hash     string
}

func ensureStateRoot(root string) (string, error) {
	var err error
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return "", err
	}
	return root, nil
}

// New creates a daemon server and ensures state root and token file exist.
func New(cfg Config) (*Server, error) {
	root := cfg.StateRoot
	var err error
	root, err = ensureStateRoot(root)
	if err != nil {
		return nil, err
	}
	cwd := cfg.CWD
	if cwd == "" {
		cwd, err = os.Getwd()
		if err != nil {
			return nil, err
		}
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
	clock := cfg.Clock
	if clock == nil {
		clock = engine.ClockFunc(time.Now)
	}
	processes := cfg.ProcessTable
	if processes == nil {
		processes = engine.NativeProcessTable{}
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = 30 * time.Minute
	}
	idleCheck := cfg.IdleCheckInterval
	if idleCheck == 0 {
		idleCheck = time.Minute
		if idleTimeout > 0 && idleTimeout < idleCheck {
			idleCheck = idleTimeout / 4
			if idleCheck <= 0 {
				idleCheck = idleTimeout
			}
		}
	}
	backends := make(map[string]engine.Backend, len(cfg.Backends))
	for _, backend := range cfg.Backends {
		if backend != nil && backend.Name() != "" {
			backends[backend.Name()] = backend
		}
	}
	registry := cfg.Registry
	if registry == nil {
		registry = engine.NewPolicyRegistry()
	}
	inlineResultCap := cfg.InlineResultCap
	if inlineResultCap <= 0 {
		inlineResultCap = engine.DefaultInlineResultCap
	}
	leaseDuration := cfg.LeaseDuration
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}
	heartbeatInterval := cfg.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeat
	}
	gcInterval := cfg.GCInterval
	if gcInterval < 0 {
		return nil, errors.New("gc interval cannot be negative")
	}
	if gcInterval == 0 {
		gcInterval = engine.DefaultGCInterval
	}
	binaryIdentityProbe := cfg.BinaryIdentityProbe
	if binaryIdentityProbe == nil {
		binaryIdentityProbe = statBinaryIdentity
	}
	probeRunner := cfg.ProbeRunner
	if probeRunner == nil {
		probeRunner = command.DirectProbeRunner{}
	}
	return &Server{
		stateRoot:                root,
		cwd:                      cwd,
		socketPath:               socketPath,
		tokenPath:                tokenPath,
		token:                    token,
		codexHomeOverride:        cfg.CodexHomeOverride,
		codexHomeInherit:         cfg.CodexHomeInherit,
		codexAuthHome:            cfg.CodexAuthHome,
		backends:                 backends,
		registry:                 registry,
		clock:                    clock,
		processes:                processes,
		processGroups:            cfg.ProcessGroups,
		cancelGrace:              cfg.CancelGrace,
		cancelWaiter:             cfg.CancelWaiter,
		idleTimeout:              idleTimeout,
		idleCheckInterval:        idleCheck,
		binaryIdentityProbe:      binaryIdentityProbe,
		inlineResultCap:          inlineResultCap,
		leaseDuration:            leaseDuration,
		heartbeatInterval:        heartbeatInterval,
		gcInterval:               gcInterval,
		admissionLogRetentionCap: defaultAdmissionLogRetentionCap,
		readyHook:                cfg.ReadyHook,
		safetyLatch:              NewSafetyLatch(),
		safetyDrainTimeout:       defaultSafetyDrain,
		shutdownTimeout:          normalizeShutdownTimeout(cfg.ShutdownTimeout),
		stores:                   make(map[string]*engine.Store),
		storesByKey:              make(map[string]*engine.Store),
		jobStores:                make(map[string]*engine.Store),
		admissionJobs:            make(map[string]*admissionInstance),
		admissionEffectMu:        make(map[string]*sync.Mutex),
		admissionLogDrains:       make(map[string]<-chan struct{}),
		admissionRuntimeConfig:   cfg.Runtime,
		admissionProbeRunner:     probeRunner,
		activeJobs:               make(map[string]*activeJob),
		jobLiveness:              make(map[string]*jobLiveness),
		reportedModels:           make(map[string]string),
		lastActivity:             clock.Now().UTC(),
	}, nil
}

// SocketPath returns the protocol socket path for a state root.
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

// TokenPath returns the protocol token path for a state root.
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

// Serve listens on the configured Unix socket until ctx is canceled or idle shutdown fires.
func (s *Server) Serve(ctx context.Context) error {
	return s.serve(ctx, ctx)
}

// ServeWithStartupContext uses startupCtx for pre-ready bootstrap and ctx for
// the daemon lifetime after readiness is reported.
func (s *Server) ServeWithStartupContext(ctx, startupCtx context.Context) error {
	return s.serve(ctx, startupCtx)
}

func (s *Server) serve(ctx, startupCtx context.Context) error {
	s.ensureSafetyLatch()
	if err := s.captureBinaryIdentity(); err != nil {
		return err
	}
	if startupCtx == nil {
		startupCtx = ctx
	}
	if err := s.bootstrapAdmission(startupCtx); err != nil {
		if errors.Is(err, authority.ErrFailStopped) {
			s.safetyLatch.Trip(err)
			return s.safetyFailStopErr()
		}
		return err
	}
	registeredLifecycle := false
	defer func() {
		if registeredLifecycle {
			return
		}
		if err := s.closeServeAdmission(); err != nil {
			log.Printf("agentbus daemon: close admission authority: %v", err)
		}
	}()
	listen := s.listen
	if s.listenerFactory != nil {
		listen = s.listenerFactory
	}
	ln, socketIdentity, err := listen()
	if err != nil {
		return err
	}
	defer func() {
		_ = ln.Close()
		s.removeOwnedSocket(socketIdentity, "server shutdown")
	}()
	ctx, cancel := context.WithCancel(ctx)
	if err := startupCtx.Err(); err != nil {
		cancel()
		return err
	}
	lifecycle, err := s.registerServeLifecycleContext(startupCtx, ln, socketIdentity, cancel)
	if err != nil {
		cancel()
		return err
	}
	registeredLifecycle = true
	generation := lifecycle.generation
	defer s.clearServeLifecycle(generation)
	defer func() {
		if err := s.closeServeAdmissionSnapshot(context.Background(), lifecycle.admission); err != nil {
			log.Printf("agentbus daemon: close admission authority: %v", err)
		}
	}()
	defer cancel()
	if err := startupCtx.Err(); err != nil {
		return err
	}
	if s.readyHook != nil {
		if err := s.readyHook(ServeReadyInfo{StateRoot: s.stateRoot, SocketPath: s.socketPath}); err != nil {
			return err
		}
	}
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	// Invocation-local so a sequential re-Serve on the same Server can never
	// pair a fresh channel with a spent sync.Once.
	acceptSettled := make(chan struct{})
	var settleOnce sync.Once
	safetyDone := make(chan error, 1)
	go s.safetyLoop(ctx, cancel, ln, socketIdentity, acceptSettled, safetyDone)
	go s.idleLoop(ctx, cancel, ln, socketIdentity, acceptSettled)
	go s.admissionLogSweepLoop(ctx)
	for {
		conn, err := ln.Accept()
		if err != nil {
			// The accept loop is sequential: once Accept returns an error,
			// every connection it previously returned has completed
			// registration. Signal that so the stale drain can trust the
			// client counter (closes the post-Accept pre-register window).
			settleOnce.Do(func() { close(acceptSettled) })
			failStopErr := s.safetyFailStopErr()
			if failStopErr != nil {
				select {
				case drainedErr := <-safetyDone:
					return drainedErr
				case <-ctx.Done():
					return failStopErr
				}
			}
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				// A stale daemon closes its listener before it cancels the
				// context, so connections already accepted by this loop can
				// register and drain without accepting any new connections.
				select {
				case failStopErr := <-safetyDone:
					return failStopErr
				case <-ctx.Done():
					if failStopErr := s.safetyFailStopErr(); failStopErr != nil {
						return failStopErr
					}
				}
				return nil
			}
			return err
		}
		s.accepting.Add(1)
		s.clients.Add(1)
		s.accepting.Add(-1)
		s.touchActivity()
		c := &connection{server: s, conn: conn}
		go func() {
			defer s.clients.Add(-1)
			defer s.touchActivity()
			c.serve(ctx)
		}()
	}
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

func (s *Server) registerServeLifecycleContext(ctx context.Context, ln net.Listener, socketIdentity socketFileIdentity, cancel context.CancelFunc) (serveLifecycleSnapshot, error) {
	if err := s.lockShutdownRunContext(ctx); err != nil {
		return serveLifecycleSnapshot{}, err
	}
	defer s.shutdownRunMu.Unlock()
	return s.registerServeLifecycle(ln, socketIdentity, cancel), nil
}

func (s *Server) registerServeLifecycle(ln net.Listener, socketIdentity socketFileIdentity, cancel context.CancelFunc) serveLifecycleSnapshot {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	s.serveStateMu.Lock()
	defer s.serveStateMu.Unlock()
	s.serveGeneration++
	generation := s.serveGeneration
	lifecycle := serveLifecycleSnapshot{
		generation: generation,
		listener:   ln,
		socket:     socketIdentity,
		pidFile:    s.currentOwnedPIDFileSnapshot(),
		cancel:     cancel,
		admission:  s.currentServeAdmissionSnapshot(),
		bound:      true,
	}
	s.serveListener = ln
	s.serveSocket = socketIdentity
	s.serveCancel = cancel
	s.shutdownState = &serveShutdownState{generation: generation, lifecycle: lifecycle}
	return lifecycle
}

func (s *Server) clearServeLifecycle(generation uint64) {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	s.serveStateMu.Lock()
	if s.serveGeneration != generation {
		s.serveStateMu.Unlock()
		return
	}
	s.serveListener = nil
	s.serveSocket = socketFileIdentity{}
	s.serveCancel = nil
	s.serveStateMu.Unlock()
	if s.shutdownState != nil && s.shutdownState.generation == generation && (s.shutdownState.done == nil || s.shutdownState.complete) {
		s.shutdownState = nil
	}
}

func (s *Server) currentServeAdmissionSnapshot() *serveAdmissionSnapshot {
	s.admissionStateMu.RLock()
	defer s.admissionStateMu.RUnlock()
	if s.admissionInstance == nil && s.admissionRuntime == nil && s.admissionClose == nil {
		return nil
	}
	checker := s.admissionOwnedWorkChecker
	if checker == nil && s.admissionCoordinator != nil {
		checker = s.admissionCoordinator
	}
	return &serveAdmissionSnapshot{
		instance:    s.admissionInstance,
		ready:       s.admissionReady,
		coordinator: s.admissionCoordinator,
		checker:     checker,
		runtime:     s.admissionRuntime,
		closer:      s.admissionClose,
	}
}

func (s *Server) serveLifecycleCurrent(lifecycle serveLifecycleSnapshot) bool {
	if !lifecycle.bound {
		return true
	}
	s.serveStateMu.Lock()
	defer s.serveStateMu.Unlock()
	return s.serveGeneration == lifecycle.generation && s.serveListener == lifecycle.listener
}

func (s *Server) ShutdownTimeout() time.Duration {
	if s == nil {
		return defaultShutdown
	}
	return s.shutdownTimeout
}

func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		state, done, run, err := s.beginShutdownState()
		if err != nil {
			return err
		}
		if run {
			return s.runShutdownState(ctx, state, done)
		}
		retry, err := s.waitShutdownState(ctx, state, done)
		if !retry {
			return err
		}
	}
}

func (s *Server) beginShutdownState() (*serveShutdownState, chan struct{}, bool, error) {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	state := s.shutdownState
	if state == nil {
		return nil, nil, false, ErrShutdownNotServing
	}
	if state.complete {
		return state, nil, false, state.err
	}
	if state.done != nil {
		return state, state.done, false, nil
	}
	done := make(chan struct{})
	state.done = done
	return state, done, true, nil
}

func (s *Server) waitShutdownState(ctx context.Context, state *serveShutdownState, done <-chan struct{}) (bool, error) {
	select {
	case <-done:
		s.shutdownMu.Lock()
		complete := state.complete
		err := state.err
		s.shutdownMu.Unlock()
		if !complete {
			return true, nil
		}
		return false, err
	case <-ctx.Done():
		select {
		case <-done:
			s.shutdownMu.Lock()
			complete := state.complete
			err := state.err
			s.shutdownMu.Unlock()
			if !complete {
				return true, nil
			}
			return false, err
		default:
		}
		return false, ctx.Err()
	}
}

func (s *Server) runShutdownState(ctx context.Context, state *serveShutdownState, done chan struct{}) error {
	if err := s.lockShutdownRunContext(ctx); err != nil {
		s.shutdownMu.Lock()
		if state.done == done && !state.complete {
			state.done = nil
			close(done)
		}
		s.shutdownMu.Unlock()
		return err
	}
	err := s.shutdownLifecycle(ctx, state.lifecycle)
	s.shutdownRunMu.Unlock()
	s.shutdownMu.Lock()
	state.err = err
	state.complete = true
	close(done)
	if s.shutdownState == state && !s.serveGenerationServingLocked(state.generation) {
		s.shutdownState = nil
	}
	s.shutdownMu.Unlock()
	return err
}

func (s *Server) lockShutdownRunContext(ctx context.Context) error {
	for {
		if s.shutdownRunMu.TryLock() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Server) serveGenerationServingLocked(generation uint64) bool {
	s.serveStateMu.Lock()
	defer s.serveStateMu.Unlock()
	return s.serveGeneration == generation && s.serveListener != nil
}

func (s *Server) shutdown(ctx context.Context) error {
	return s.shutdownLifecycle(ctx, serveLifecycleSnapshot{
		admission: s.currentServeAdmissionSnapshot(),
	})
}

func (s *Server) shutdownLifecycle(ctx context.Context, lifecycle serveLifecycleSnapshot) error {
	if !s.serveLifecycleCurrent(lifecycle) {
		return nil
	}
	if err := s.beginAdmissionClosing(ctx, lifecycle.admission); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	lifecycle.closeServeListener(s, "graceful shutdown")
	if !s.serveLifecycleCurrent(lifecycle) {
		return nil
	}
	if err := s.cancelAdmissionWorkForShutdown(ctx, lifecycle.admission); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	if err := s.waitShutdownDrained(ctx, lifecycle); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	if err := s.closeServeAdmissionSnapshot(ctx, lifecycle.admission); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	if err := ctx.Err(); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	if !s.serveLifecycleCurrent(lifecycle) {
		return nil
	}
	if err := s.removeOwnedPIDFile(ctx, "graceful shutdown", lifecycle.pidFile); err != nil {
		lifecycle.forceStopServe()
		return shutdownError(err)
	}
	lifecycle.forceStopServe()
	if err := ctx.Err(); err != nil {
		return shutdownError(err)
	}
	return nil
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

func (lifecycle serveLifecycleSnapshot) closeServeListener(s *Server, phase string) {
	if lifecycle.listener == nil {
		return
	}
	if err := lifecycle.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("agentbus daemon: close listener during %s: %v", phase, err)
	}
	s.removeOwnedSocket(lifecycle.socket, phase)
}

func (lifecycle serveLifecycleSnapshot) forceStopServe() {
	if lifecycle.cancel != nil {
		lifecycle.cancel()
	}
}

func (s *Server) cancelAdmissionWorkForShutdown(ctx context.Context, admission *serveAdmissionSnapshot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	coord, jobIDs, err := s.shutdownAdmissionJobs(ctx, admission)
	if err != nil || coord == nil {
		return err
	}
	type shutdownAdmissionJob struct {
		id       model.JobID
		prepared chan error
		finalize chan context.Context
		done     chan error
	}
	jobs := make([]shutdownAdmissionJob, 0, len(jobIDs))
	for _, jobID := range jobIDs {
		job := shutdownAdmissionJob{
			id:       jobID,
			prepared: make(chan error, 1),
			finalize: make(chan context.Context),
			done:     make(chan error, 1),
		}
		jobs = append(jobs, job)
	}
	for i := range jobs {
		job := &jobs[i]
		go func() {
			err := s.withAdmissionJobEffectErr(job.id.String(), func() error {
				if err := s.requestActiveJobShutdownCancel(ctx, job.id.String()); err != nil {
					job.prepared <- err
					return err
				}
				// Retain the per-job effect lock while the coordinator commit is
				// queued. This preserves the existing client-cancel and runner
				// serialization while every job's bounded diagnostic settle runs
				// concurrently.
				job.prepared <- nil
				finalizeCtx, ok := <-job.finalize
				if !ok {
					return nil
				}
				count, ordinal := activeObservedWorkspaceWriteItemCount(s.lookupActiveJob(job.id.String()))
				if err := coord.CancelWithMetadataAndObservedWorkspaceWriteItemCount(finalizeCtx, job.id, engine.CancellationOriginDaemonShutdown, "daemon shutdown requested cancellation", count, ordinal, nil); err != nil {
					return err
				}
				s.abandonAdmissionUnresolvedCustody(finalizeCtx, coord, job.id)
				return nil
			})
			job.done <- err
		}()
	}
	for i := range jobs {
		if err := <-jobs[i].prepared; err != nil {
			for j := range jobs {
				close(jobs[j].finalize)
			}
			for j := range jobs {
				<-jobs[j].done
			}
			return err
		}
	}

	// Commit the accepted work with whatever diagnostics settled before its
	// individual wait ended. A caller deadline may expire while the settles or
	// earlier serialized commits run; it must not turn a later terminal commit
	// into a no-op. shutdownLifecycle observes that deadline immediately after
	// this phase.
	finalizeCtx := context.WithoutCancel(ctx)
	for i := range jobs {
		jobs[i].finalize <- finalizeCtx
		if err := <-jobs[i].done; err != nil {
			for j := i + 1; j < len(jobs); j++ {
				close(jobs[j].finalize)
			}
			for j := i + 1; j < len(jobs); j++ {
				<-jobs[j].done
			}
			return err
		}
	}
	return nil
}

func (s *Server) shutdownAdmissionJobs(ctx context.Context, admission *serveAdmissionSnapshot) (*admissionCoordinator, []model.JobID, error) {
	if admission == nil || admission.ready == nil || admission.coordinator == nil {
		return nil, nil, nil
	}
	snapshot, err := admission.ready.RuntimeSnapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	jobMap := make(map[model.JobID]struct{})
	for _, ref := range snapshot.Pending {
		jobMap[ref.JobID] = struct{}{}
	}
	for _, owned := range snapshot.Owned {
		jobMap[owned.Ref.JobID] = struct{}{}
	}
	for _, jobID := range s.activeAdmissionJobIDs(admission) {
		modelJobID, err := model.NewJobID(jobID)
		if err != nil {
			return nil, nil, err
		}
		jobMap[modelJobID] = struct{}{}
	}
	jobIDs := make([]model.JobID, 0, len(jobMap))
	for jobID := range jobMap {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Slice(jobIDs, func(i, j int) bool { return jobIDs[i] < jobIDs[j] })
	return admission.coordinator, jobIDs, nil
}

func (s *Server) activeAdmissionJobIDs(admission *serveAdmissionSnapshot) []string {
	if admission == nil || admission.instance == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	jobIDs := make([]string, 0, len(s.activeJobs))
	for jobID := range s.activeJobs {
		if s.admissionJobs[jobID] == admission.instance {
			jobIDs = append(jobIDs, jobID)
		}
	}
	sort.Strings(jobIDs)
	return jobIDs
}

func (s *Server) requestActiveJobShutdownCancel(ctx context.Context, jobID string) error {
	active := s.lookupActiveJob(jobID)
	if active == nil {
		return nil
	}
	active.requestTerminal(engine.StateCanceled, terminalCancellationFor(engine.CancellationOriginDaemonShutdown, "daemon shutdown requested cancellation"))
	settled := active.interruptSessionNativeFirst()
	if !settled {
		if err := active.interruptAdmissionCommand(ctx); err != nil {
			if !admissionPhysicalCleanupUncertain(err) {
				return err
			}
		}
	}
	active.settleAdmissionDiagnostics(ctx, admissionNativeInterruptGrace)
	if active.cancel != nil {
		active.cancel()
	}
	return nil
}

func (s *Server) waitShutdownDrained(ctx context.Context, lifecycle serveLifecycleSnapshot) error {
	poll := 10 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !s.serveLifecycleCurrent(lifecycle) {
			return nil
		}
		if !s.activeWorkWithAdmissionSnapshot(ctx, lifecycle.admission) {
			break
		}
		if !s.serveLifecycleCurrent(lifecycle) {
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if lifecycle.admission != nil && lifecycle.admission.coordinator != nil {
		return lifecycle.admission.coordinator.Shutdown(ctx)
	}
	return nil
}

func (s *Server) activeWorkWithAdmissionSnapshot(ctx context.Context, admission *serveAdmissionSnapshot) bool {
	s.mu.Lock()
	activeJobs := len(s.activeJobs)
	s.mu.Unlock()
	if activeJobs > 0 {
		return true
	}
	if s.resultPublications.Load() > 0 {
		return true
	}
	if admission == nil {
		return false
	}
	checker := admission.checker
	if checker == nil && admission.coordinator != nil {
		checker = admission.coordinator
	}
	if checker != nil {
		owned, err := checker.HasOwnedWork(ctx)
		if err != nil && s.safetyFailStopErr() == nil {
			log.Printf("agentbus daemon: admission active-work check failed: %v", err)
		}
		if err != nil || owned {
			return true
		}
	}
	if admission.runtime != nil && admission.runtime.hasActiveCustodies() {
		return true
	}
	return false
}

func (s *Server) ensureSafetyLatch() {
	if s.safetyLatch != nil {
		return
	}
	s.safetyLatch = NewSafetyLatch()
}

func (s *Server) listen() (net.Listener, socketFileIdentity, error) {
	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, socketFileIdentity{}, err
	}
	if err := os.Chmod(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, socketFileIdentity{}, err
	}
	if _, err := os.Lstat(s.socketPath); err == nil {
		if conn, dialErr := net.DialTimeout("unix", s.socketPath, 100*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
		}
		// Production Serve reaches this cleanup only while holding the strict
		// admission startup lease for the delegated root. A concurrent
		// direct/autostart daemon that cannot take the same lease fails or
		// converges before listen(), so a non-dialable path here cannot be a
		// concurrent winner's newly listening socket unless that startup-lease
		// invariant is already broken.
		if err := os.Remove(s.socketPath); err != nil {
			return nil, socketFileIdentity{}, err
		}
	} else if !os.IsNotExist(err) {
		return nil, socketFileIdentity{}, err
	}
	if s.beforeListenBindHook != nil {
		s.beforeListenBindHook()
	}
	ln, identity, err := listenUnixSocketPrivate(s.socketPath, s.unixSocketPrivateListenHooks)
	if err != nil {
		if isAddrInUse(err) {
			return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
		}
		return nil, socketFileIdentity{}, err
	}
	// net.UnixListener normally removes its path on Close. Disable that BEFORE
	// any other setup step so no error-path Close can remove a path that a
	// replacement daemon may have re-bound; every removal goes through the
	// identity-checked removeOwnedSocket instead.
	unixListener, ok := ln.(*net.UnixListener)
	if !ok {
		_ = ln.Close()
		return nil, socketFileIdentity{}, fmt.Errorf("daemon listener for %s is not a Unix listener", s.socketPath)
	}
	unixListener.SetUnlinkOnClose(false)
	if !socketPathMatchesIdentity(s.socketPath, identity) {
		_ = ln.Close()
		return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
	}
	if err := os.Chmod(s.socketPath, 0o600); err != nil {
		_ = ln.Close()
		s.removeOwnedSocket(identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	if !socketPathMatchesIdentity(s.socketPath, identity) {
		_ = ln.Close()
		return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: s.socketPath}
	}
	return ln, identity, nil
}

func listenUnixSocketPrivate(path string, hooks unixSocketPrivateListenHooks) (net.Listener, socketFileIdentity, error) {
	return listenUnixSocketPrivateWithHooks(path, hooks)
}

type unixSocketPrivateListenHooks struct {
	beforeListen func(socketFileIdentity) error
}

func listenUnixSocketPrivateWithHooks(path string, hooks unixSocketPrivateListenHooks) (net.Listener, socketFileIdentity, error) {
	fd, err := openUnixSocketFD()
	if err != nil {
		return nil, socketFileIdentity{}, err
	}
	fdOpen := true
	closeFD := func() {
		if fdOpen {
			_ = syscall.Close(fd)
			fdOpen = false
		}
	}
	failBound := func(identity socketFileIdentity, phase string, err error) (net.Listener, socketFileIdentity, error) {
		closeFD()
		removeSocketPathIfIdentity(path, identity, phase)
		return nil, socketFileIdentity{}, err
	}

	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		closeFD()
		return nil, socketFileIdentity{}, os.NewSyscallError("setsockopt", err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		closeFD()
		return nil, socketFileIdentity{}, os.NewSyscallError("bind", err)
	}
	identity, err := statSocketFileIdentity(path)
	if err != nil {
		closeFD()
		return nil, socketFileIdentity{}, fmt.Errorf("lstat daemon socket %q after bind: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return failBound(identity, "listener setup failure", err)
	}
	if hooks.beforeListen != nil {
		if err := hooks.beforeListen(identity); err != nil {
			return failBound(identity, "listener setup failure", err)
		}
	}
	if err := syscall.Listen(fd, syscall.SOMAXCONN); err != nil {
		return failBound(identity, "listener setup failure", os.NewSyscallError("listen", err))
	}
	if !socketPathMatchesIdentity(path, identity) {
		closeFD()
		return nil, socketFileIdentity{}, DaemonAlreadyListeningError{SocketPath: path}
	}

	file := os.NewFile(uintptr(fd), "agentbus-daemon-listener")
	if file == nil {
		return failBound(identity, "listener setup failure", fmt.Errorf("wrap daemon listener fd %d", fd))
	}
	fdOpen = false
	ln, err := net.FileListener(file)
	if err != nil {
		_ = file.Close()
		removeSocketPathIfIdentity(path, identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	if err := file.Close(); err != nil {
		_ = ln.Close()
		removeSocketPathIfIdentity(path, identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	return ln, identity, nil
}

func openUnixSocketFD() (int, error) {
	socketType, closeOnExecInSocket := unixSocketStreamType()
	if !closeOnExecInSocket {
		syscall.ForkLock.RLock()
	}
	fd, err := syscall.Socket(syscall.AF_UNIX, socketType, 0)
	if err == nil && !closeOnExecInSocket {
		syscall.CloseOnExec(fd)
	}
	if !closeOnExecInSocket {
		syscall.ForkLock.RUnlock()
	}
	if err != nil {
		return -1, os.NewSyscallError("socket", err)
	}
	if closeOnExecInSocket {
		syscall.CloseOnExec(fd)
	}
	return fd, nil
}

func (s *Server) idleLoop(ctx context.Context, cancel context.CancelFunc, ln net.Listener, socketIdentity socketFileIdentity, acceptSettled <-chan struct{}) {
	ticker := time.NewTicker(s.idleCheckInterval)
	defer ticker.Stop()
	staleDraining := false
	drainLogged := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := s.checkBinaryStale()
			quiet := s.clients.Load() == 0 && s.accepting.Load() == 0 && !s.activeWork()
			if staleDraining {
				settled := false
				select {
				case <-acceptSettled:
					settled = true
				default:
				}
				// Quiet counters alone cannot be trusted until the accept
				// loop confirms it has registered every connection Accept
				// returned before the listener closed.
				if !quiet || !settled {
					if !drainLogged {
						log.Print("agentbus daemon: stale listener is closed; draining accepted connections and active work")
						drainLogged = true
					}
					continue
				}
				log.Print("agentbus daemon: stale daemon drained; shutting down")
				cancel()
				return
			}
			if !quiet {
				s.touchActivity()
				continue
			}
			if stale {
				log.Print("agentbus daemon: stale daemon is quiet; closing listener before draining")
				if s.beforeStaleCloseHook != nil {
					s.beforeStaleCloseHook()
				}
				if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
					log.Printf("agentbus daemon: close stale listener: %v", err)
				}
				if s.staleListenerHook != nil {
					s.staleListenerHook()
				}
				s.removeOwnedSocket(socketIdentity, "stale daemon listener close")
				if s.staleSocketRemovedHook != nil {
					s.staleSocketRemovedHook()
				}
				log.Print("agentbus daemon: stale listener closed; waiting for accepted connections and active work to drain")
				staleDraining = true
				continue
			}
			if s.idleTimeout < 0 {
				continue
			}
			s.mu.Lock()
			last := s.lastActivity
			s.mu.Unlock()
			if !s.clock.Now().UTC().Before(last.Add(s.idleTimeout)) {
				cancel()
				_ = ln.Close()
				return
			}
		}
	}
}

func (s *Server) safetyLoop(ctx context.Context, cancel context.CancelFunc, ln net.Listener, socketIdentity socketFileIdentity, acceptSettled <-chan struct{}, done chan<- error) {
	select {
	case <-ctx.Done():
		return
	case <-s.safetyLatch.Done():
	}

	failStopErr := s.safetyFailStopErr()
	log.Printf("agentbus daemon: safety latch tripped; closing listener: %v", failStopErr)
	if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("agentbus daemon: close fail-stopped listener: %v", err)
	}
	s.removeOwnedSocket(socketIdentity, "safety fail-stop")

	drainTimeout := s.safetyDrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = defaultSafetyDrain
	}
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	probeResults := make(chan bool, 1)
	probeRunning := false
	startDrainProbe := func() {
		if probeRunning {
			return
		}
		settled := false
		select {
		case <-acceptSettled:
			settled = true
		default:
		}
		// The accept loop must first confirm that every connection returned
		// before listener close has been registered in the client counters.
		if !settled || s.clients.Load() != 0 || s.accepting.Load() != 0 {
			return
		}
		probeRunning = true
		probeCtx, probeCancel := context.WithTimeout(ctx, drainTimeout)
		go func() {
			defer probeCancel()
			probeResults <- !s.activeWorkWithContext(probeCtx)
		}()
	}
	startDrainProbe()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			err := fmt.Errorf("%w; safety drain timed out after %s", failStopErr, drainTimeout)
			log.Printf("agentbus daemon: safety fail-stop drain timed out; exiting: %v", err)
			select {
			case done <- err:
			default:
			}
			cancel()
			return
		case drained := <-probeResults:
			probeRunning = false
			if drained {
				log.Printf("agentbus daemon: safety fail-stop drain complete; exiting: %v", failStopErr)
				select {
				case done <- failStopErr:
				default:
				}
				cancel()
				return
			}
			startDrainProbe()
		case <-ticker.C:
			startDrainProbe()
		}
	}
}

func (s *Server) safetyFailStopErr() error {
	if s == nil || s.safetyLatch == nil {
		return nil
	}
	reason := s.safetyLatch.Reason()
	if reason == nil {
		return nil
	}
	return SafetyFailStopError{Reason: reason}
}

func statSocketFileIdentity(path string) (socketFileIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return socketFileIdentity{}, err
	}
	return socketFileIdentityFromStat(info.Sys())
}

func socketFileIdentityFromStat(sys any) (socketFileIdentity, error) {
	stat, ok := sys.(*syscall.Stat_t)
	if !ok {
		return socketFileIdentity{}, fmt.Errorf("unexpected socket stat type %T", sys)
	}
	return socketFileIdentity{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, nil
}

func socketPathMatchesIdentity(path string, owned socketFileIdentity) bool {
	actual, err := statSocketFileIdentity(path)
	return err == nil && actual == owned
}

func (s *Server) removeOwnedSocket(owned socketFileIdentity, phase string) {
	removeSocketPathIfIdentity(s.socketPath, owned, phase)
}

func removeSocketPathIfIdentity(path string, owned socketFileIdentity, phase string) {
	actual, err := statSocketFileIdentity(path)
	if err != nil {
		log.Printf("agentbus daemon: skipping socket removal during %s: cannot stat %s (%v); a replacement daemon may own the path", phase, path, err)
		return
	}
	if actual != owned {
		log.Printf("agentbus daemon: skipping socket removal during %s: replacement daemon owns %s", phase, path)
		return
	}
	// dev+inode alone cannot prove ownership: tmpfs (Linux /tmp) reuses inodes
	// immediately, so a replacement daemon's socket can inherit the dead
	// socket's exact identity. Removal only ever targets a socket whose own
	// listener is already closed, so a successful dial proves a LIVE listener —
	// necessarily a replacement — and skipping is always fail-safe (daemon
	// startup dials and clears genuinely stale files itself).
	if conn, dialErr := net.DialTimeout("unix", path, 250*time.Millisecond); dialErr == nil {
		_ = conn.Close()
		log.Printf("agentbus daemon: skipping socket removal during %s: %s accepts connections; a replacement daemon owns it", phase, path)
		return
	}
	if err := os.Remove(path); err != nil {
		log.Printf("agentbus daemon: remove owned socket during %s: %v", phase, err)
		return
	}
	log.Printf("agentbus daemon: removed owned socket during %s", phase)
}

func (s *Server) currentOwnedPIDFileSnapshot() pidFileSnapshot {
	pidPath := filepath.Join(s.stateRoot, "agentbus.pid")
	raw, identity, err := s.readPIDFileNoFollow(pidPath)
	if err != nil {
		return pidFileSnapshot{}
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid != os.Getpid() {
		return pidFileSnapshot{}
	}
	return pidFileSnapshot{identity: identity, known: true}
}

func (s *Server) readPIDFileNoFollow(path string) ([]byte, socketFileIdentity, error) {
	if s.readPIDFileNoFollowHook != nil {
		return s.readPIDFileNoFollowHook(path)
	}
	return readPIDFileNoFollow(path)
}

func (s *Server) removeOwnedPIDFile(ctx context.Context, phase string, expected ...pidFileSnapshot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pidPath := filepath.Join(s.stateRoot, "agentbus.pid")
	raw, owned, err := s.readPIDFileNoFollow(pidPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("agentbus daemon: skipping pid removal during %s: read %s: %v", phase, pidPath, err)
			return fmt.Errorf("%w: read %s: %w", ErrShutdownPIDTeardownFailed, pidPath, err)
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(expected) > 0 && expected[0].known && owned != expected[0].identity {
		log.Printf("agentbus daemon: skipping pid removal during %s: replacement daemon owns %s", phase, pidPath)
		return nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: invalid pid file %s", phase, pidPath)
		return nil
	}
	if pid != os.Getpid() {
		log.Printf("agentbus daemon: skipping pid removal during %s: %s belongs to pid %d", phase, pidPath, pid)
		return nil
	}
	if s.beforePIDFileQuarantineHook != nil {
		s.beforePIDFileQuarantineHook()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	quarantineDir, quarantinePath, err := createPIDFileQuarantine(s.stateRoot)
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: create pid quarantine: %v", phase, err)
		return fmt.Errorf("%w: create pid quarantine: %w", ErrShutdownPIDTeardownFailed, err)
	}
	cleanupQuarantineDir := true
	defer func() {
		if !cleanupQuarantineDir {
			return
		}
		if err := ctx.Err(); err != nil {
			log.Printf("agentbus daemon: abandoning pid quarantine directory cleanup during %s after context cancellation; dir=%s: %v", phase, quarantineDir, err)
			return
		}
		if err := os.Remove(quarantineDir); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("agentbus daemon: remove pid quarantine directory during %s: %v", phase, err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(pidPath, quarantinePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("agentbus daemon: skipping pid removal during %s: quarantine %s: %v", phase, pidPath, err)
			return fmt.Errorf("%w: quarantine %s: %w", ErrShutdownPIDTeardownFailed, pidPath, err)
		}
		return nil
	}
	if err := abortQuarantinedPIDFileIfContextDone(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir); err != nil {
		return err
	}
	quarantinedRaw, quarantined, err := s.readPIDFileNoFollow(quarantinePath)
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: validate quarantined pid %s: %v", phase, quarantinePath, err)
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		if restoreErr != nil && errors.Is(restoreErr, ctx.Err()) {
			return restoreErr
		}
		cleanupQuarantineDir = cleanup
		return errors.Join(
			fmt.Errorf("%w: validate quarantined pid %s: %w", ErrShutdownPIDTeardownFailed, quarantinePath, err),
			wrapPIDRestoreError(restoreErr),
		)
	}
	if err := abortQuarantinedPIDFileIfContextDone(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir); err != nil {
		return err
	}
	quarantinedPID, err := strconv.Atoi(strings.TrimSpace(string(quarantinedRaw)))
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: invalid quarantined pid file %s", phase, quarantinePath)
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		cleanupQuarantineDir = cleanup
		return wrapPIDRestoreError(restoreErr)
	}
	if quarantined != owned || quarantinedPID != os.Getpid() {
		log.Printf("agentbus daemon: skipping pid removal during %s: replacement daemon owns %s", phase, pidPath)
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		cleanupQuarantineDir = cleanup
		return wrapPIDRestoreError(restoreErr)
	}
	if err := abortQuarantinedPIDFileIfContextDone(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir); err != nil {
		return err
	}
	if err := os.Remove(quarantinePath); err != nil {
		log.Printf("agentbus daemon: remove owned pid during %s: %v", phase, err)
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		if restoreErr != nil && errors.Is(restoreErr, ctx.Err()) {
			return restoreErr
		}
		cleanupQuarantineDir = cleanup
		return errors.Join(
			fmt.Errorf("%w: remove owned pid during %s: %w", ErrShutdownPIDTeardownFailed, phase, err),
			wrapPIDRestoreError(restoreErr),
		)
	}
	if err := ctx.Err(); err != nil {
		cleanupQuarantineDir = false
		log.Printf("agentbus daemon: abandoning owned pid restore during %s after context cancellation; canonical=%s quarantine=%s already removed: %v", phase, pidPath, quarantinePath, err)
		return err
	}
	log.Printf("agentbus daemon: removed owned pid during %s", phase)
	return nil
}

func readPIDFileNoFollow(path string) ([]byte, socketFileIdentity, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, socketFileIdentity{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, socketFileIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, socketFileIdentity{}, fmt.Errorf("unexpected pid stat type %T", info.Sys())
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		return nil, socketFileIdentity{}, err
	}
	return raw, socketFileIdentity{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, nil
}

func createPIDFileQuarantine(stateRoot string) (string, string, error) {
	base := fmt.Sprintf("agentbus.pid.quarantine.%d.%d", os.Getpid(), time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		dir := filepath.Join(stateRoot, fmt.Sprintf("%s.%d", base, i))
		if err := os.Mkdir(dir, 0o700); err == nil {
			return dir, filepath.Join(dir, "agentbus.pid"), nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", "", err
		}
	}
	return "", "", fmt.Errorf("could not allocate unique pid quarantine path in %s", stateRoot)
}

func abortQuarantinedPIDFileIfContextDone(ctx context.Context, pidPath, quarantinePath, phase string, cleanupQuarantineDir *bool) error {
	if err := ctx.Err(); err != nil {
		*cleanupQuarantineDir = false
		log.Printf("agentbus daemon: abandoning quarantined pid restore during %s after context cancellation; canonical=%s quarantine=%s: %v", phase, pidPath, quarantinePath, err)
		return err
	}
	return nil
}

func restoreQuarantinedPIDFileContext(ctx context.Context, pidPath, quarantinePath, phase string, cleanupQuarantineDir *bool) (bool, error) {
	if err := abortQuarantinedPIDFileIfContextDone(ctx, pidPath, quarantinePath, phase, cleanupQuarantineDir); err != nil {
		return false, err
	}
	return restoreQuarantinedPIDFile(pidPath, quarantinePath, phase)
}

func restoreQuarantinedPIDFile(pidPath, quarantinePath, phase string) (bool, error) {
	if err := os.Link(quarantinePath, pidPath); err == nil {
		if removeErr := os.Remove(quarantinePath); removeErr != nil {
			log.Printf("agentbus daemon: restored pid during %s but could not remove quarantine %s: %v", phase, quarantinePath, removeErr)
			return false, removeErr
		}
		return true, nil
	} else if errors.Is(err, os.ErrExist) {
		if removeErr := os.Remove(quarantinePath); removeErr != nil {
			log.Printf("agentbus daemon: canonical pid exists during %s and quarantine %s remains: %v", phase, quarantinePath, removeErr)
			return false, removeErr
		}
		return true, nil
	} else {
		log.Printf("agentbus daemon: could not restore replacement pid during %s from %s to %s: %v", phase, quarantinePath, pidPath, err)
		return false, err
	}
}

func wrapPIDRestoreError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: restore pid ownership signal: %w", ErrShutdownPIDTeardownFailed, err)
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
	identity, err := s.binaryIdentityProbe(path)
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

	actual, err := s.binaryIdentityProbe(path)
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

func (s *Server) activeWork() bool {
	return s.activeWorkWithContext(context.Background())
}

func (s *Server) activeWorkWithContext(ctx context.Context) bool {
	s.mu.Lock()
	activeJobs := len(s.activeJobs)
	s.mu.Unlock()
	if activeJobs > 0 {
		return true
	}
	if s.resultPublications.Load() > 0 {
		return true
	}
	// Snapshot-checkout: never hold admissionStateMu across HasOwnedWork —
	// a stalled ownership probe must not block closeServeAdmission's write
	// lock past the safety fail-stop drain deadline.
	s.admissionStateMu.RLock()
	checker := s.admissionOwnedWorkChecker
	if checker == nil && s.admissionCoordinator != nil {
		checker = s.admissionCoordinator
	}
	runtime := s.admissionRuntime
	s.admissionStateMu.RUnlock()
	if checker != nil {
		owned, err := checker.HasOwnedWork(ctx)
		if err != nil && s.safetyFailStopErr() == nil {
			log.Printf("agentbus daemon: admission active-work check failed: %v", err)
		}
		if err != nil || owned {
			return true
		}
	}
	if runtime != nil && runtime.hasActiveCustodies() {
		return true
	}
	// TODO(S4E-b): include pending recovery executor work once recovery execution lands.
	return false
}

func (s *Server) touchActivity() {
	s.mu.Lock()
	s.lastActivity = s.clock.Now().UTC()
	s.mu.Unlock()
}

type connection struct {
	server *Server
	conn   net.Conn
	mu     sync.Mutex
	hello  bool
}

func (c *connection) serve(ctx context.Context) {
	stopFailStopClose := c.closeOnFailStop()
	defer stopFailStopClose()
	defer c.conn.Close()
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var req protocol.Request
		if err := json.Unmarshal(line, &req); err != nil {
			c.writeResponse(protocol.Response{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   protocol.NewError(protocol.ErrorInvalidTaskSpec, "malformed JSON-RPC frame", protocol.ErrorData{}),
			})
			continue
		}
		if len(req.ID) == 0 && requiresRequestID(req.Method) {
			_ = c.writeResponse(protocol.Response{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   protocol.NewError(protocol.ErrorInvalidTaskSpec, req.Method+" requires a JSON-RPC id", protocol.ErrorData{}),
			})
			continue
		}
		out := c.server.handle(ctx, c, req)
		if len(req.ID) != 0 {
			resp := protocol.Response{JSONRPC: "2.0", ID: req.ID, Result: out.result, Error: out.err}
			if err := c.writeResponse(resp); err != nil {
				if out.onAckFailure != nil && out.err == nil {
					out.onAckFailure(err)
				}
				continue
			}
			if out.after != nil && out.err == nil {
				out.after()
			}
		}
	}
	if err := scanner.Err(); err != nil {
		message := "JSON-RPC connection read failed"
		if errors.Is(err, bufio.ErrTooLong) {
			message = "JSON-RPC request frame exceeded 4194304 byte limit"
		}
		_ = c.writeOversizedRequestResponse(protocol.Response{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   protocol.NewError(protocol.ErrorInvalidTaskSpec, message, protocol.ErrorData{}),
		})
	}
}

func (c *connection) writeOversizedRequestResponse(resp protocol.Response) error {
	if c == nil || c.conn == nil {
		return net.ErrClosed
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(oversizedRequestWriteTimeout)); err != nil {
		_ = c.conn.Close()
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})
	return c.writeResponse(resp)
}

func (c *connection) closeOnFailStop() func() {
	if c == nil || c.server == nil || c.server.safetyLatch == nil || c.conn == nil {
		return func() {}
	}
	stop := make(chan struct{})
	go func(done <-chan struct{}) {
		select {
		case <-done:
			_ = c.conn.Close()
		case <-stop:
		}
	}(c.server.safetyLatch.Done())
	return func() { close(stop) }
}

func requiresRequestID(method string) bool {
	switch method {
	case protocol.MethodHello,
		protocol.MethodJobSubmit,
		protocol.MethodJobStatus,
		protocol.MethodJobResult,
		protocol.MethodJobCancel,
		protocol.MethodPolicyValidate,
		protocol.MethodPolicyRegister:
		return true
	default:
		return false
	}
}

func (c *connection) writeResponse(resp protocol.Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeFrame(c.conn, resp)
}

func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.Write(b)
	return err
}

func (s *Server) handle(ctx context.Context, c *connection, req protocol.Request) requestOutcome {
	if req.JSONRPC != "2.0" || req.Method == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "invalid JSON-RPC request", protocol.ErrorData{})}
	}
	if !c.hello && req.Method != protocol.MethodHello {
		return requestOutcome{err: protocol.NewError(protocol.ErrorUnauthorized, "protocol.hello is required before other methods", protocol.ErrorData{})}
	}
	if c.hello && req.Method == protocol.MethodHello {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "protocol.hello has already been completed on this connection", protocol.ErrorData{})}
	}
	switch req.Method {
	case protocol.MethodHello:
		return s.handleHello(c, req.Params)
	case protocol.MethodJobSubmit:
		return s.handleJobSubmit(ctx, req.Params)
	case protocol.MethodJobStatus:
		return s.handleJobStatus(req.Params)
	case protocol.MethodJobResult:
		return s.handleJobResult(req.Params)
	case protocol.MethodJobCancel:
		return s.handleJobCancel(req.Params)
	case protocol.MethodPolicyValidate:
		return s.handlePolicyValidate(req.Params)
	case protocol.MethodPolicyRegister:
		return s.handlePolicyRegister(req.Params)
	default:
		return requestOutcome{err: protocol.NewError(protocol.ErrorMethodNotFound, "method not found", protocol.ErrorData{})}
	}
}

func (s *Server) failStoppedRequestError(method string) *protocol.ErrorObject {
	if failStoppedMethodAllowed(method) {
		return nil
	}
	failStopErr := s.safetyFailStopErr()
	if failStopErr == nil {
		return nil
	}
	return strictAdmissionProtocolError(
		protocol.ErrorBackendUnavailable,
		protocol.AdmissionRejectRootFailStopped,
		failStopErr.Error(),
		protocol.ErrorData{},
	)
}

func failStoppedMethodAllowed(method string) bool {
	switch method {
	case protocol.MethodHello,
		protocol.MethodJobStatus,
		protocol.MethodJobResult,
		protocol.MethodPolicyValidate:
		return true
	default:
		return false
	}
}

func (s *Server) handleHello(c *connection, raw json.RawMessage) requestOutcome {
	var params protocol.HelloParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.Token == "" || params.Token != s.token {
		return requestOutcome{err: protocol.NewError(protocol.ErrorUnauthorized, "missing or invalid hello token", protocol.ErrorData{})}
	}
	if params.ClientProtocolVersion != protocol.Version {
		return requestOutcome{err: protocol.NewError(protocol.ErrorVersionMismatch, "protocol major version mismatch", protocol.ErrorData{ServerProtocolVersion: protocol.Version})}
	}
	c.hello = true
	return requestOutcome{result: protocol.HelloResult{
		ProtocolVersion: protocol.Version,
		Backends:        s.backendNames(),
		BackendMetadata: s.backendMetadata(),
		Capabilities:    s.protocolCapabilities(),
	}}
}

func (s *Server) protocolCapabilities() map[string]bool {
	capabilities := protocol.DefaultCapabilities()
	s.admissionStateMu.RLock()
	instance := s.admissionInstance
	if instance != nil && instance.policy.AdvertiseRequestID {
		capabilities["jobs.requestId"] = true
	}
	if instance != nil &&
		instance.policy.Mode == AdmissionStrictIdentified &&
		instance.policy.AcceptIdentified {
		capabilities[protocol.CapabilityAdmissionStrictContainment] = true
	}
	s.admissionStateMu.RUnlock()
	return capabilities
}

func (s *Server) backendMetadata() []protocol.BackendInfo {
	names := s.backendNames()
	result := make([]protocol.BackendInfo, 0, len(names))
	for _, name := range names {
		info := protocol.BackendInfo{Name: name, Models: []string{}, Efforts: []string{}}
		backend, _ := s.backendFor(name)
		if provider, ok := backend.(engine.BackendMetadataProvider); ok {
			metadata := provider.BackendMetadata(context.Background())
			info.Models = append([]string(nil), metadata.Models...)
			info.Efforts = append([]string(nil), metadata.Efforts...)
		}
		result = append(result, info)
	}
	return result
}

func (s *Server) handleJobSubmit(ctx context.Context, raw json.RawMessage) requestOutcome {
	if errObj := s.failStoppedRequestError(protocol.MethodJobSubmit); errObj != nil {
		return requestOutcome{err: errObj}
	}
	strictByRaw, identityErr := strictIdentityPrecheck(raw)
	if errObj := s.strictRouteDisabledPrecheck(); errObj != nil {
		return requestOutcome{err: errObj}
	}
	if !strictByRaw {
		return requestOutcome{err: strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			"job.submit requires workspaceKey and requestId",
			protocol.ErrorData{},
		)}
	}
	if identityErr != nil {
		return requestOutcome{err: identityErr}
	}
	precheck, errObj := strictJobSubmitReplayPrecheck(raw)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	return s.handleIdentifiedJobSubmit(ctx, raw, precheck)
}

type strictJobSubmitPrecheck struct {
	WorkspaceKey string
	RequestID    string
	RawTaskSpec  json.RawMessage
}

func strictJobSubmitReplayPrecheck(raw json.RawMessage) (strictJobSubmitPrecheck, *protocol.ErrorObject) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return strictJobSubmitPrecheck{}, strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{})
	}
	workspaceKey, err := requiredRawString(envelope, "workspaceKey")
	if err != nil {
		return strictJobSubmitPrecheck{}, strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)
	}
	requestID, err := requiredRawString(envelope, "requestId")
	if err != nil {
		return strictJobSubmitPrecheck{}, strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)
	}
	rawTaskSpec, ok := envelope["taskSpec"]
	if !ok {
		return strictJobSubmitPrecheck{}, strictAdmissionInvalidConfigError("taskSpec is required", protocol.ErrorData{})
	}
	return strictJobSubmitPrecheck{
		WorkspaceKey: workspaceKey,
		RequestID:    requestID,
		RawTaskSpec:  append(json.RawMessage(nil), rawTaskSpec...),
	}, nil
}

func requiredRawString(envelope map[string]json.RawMessage, field string) (string, error) {
	raw, ok := envelope[field]
	if !ok {
		return "", fmt.Errorf("%s is required", field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	return value, nil
}

func strictIdentityPrecheck(raw json.RawMessage) (bool, *protocol.ErrorObject) {
	workspaceKeyPresent, err := jsonFieldPresent(raw, "workspaceKey")
	if err != nil {
		return false, protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
	requestIDPresent, err := jsonFieldPresent(raw, "requestId")
	if err != nil {
		return false, protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
	if !workspaceKeyPresent && !requestIDPresent {
		return false, nil
	}
	var identity struct {
		WorkspaceKey string `json:"workspaceKey"`
		RequestID    string `json:"requestId"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil {
		return true, strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)
	}
	if _, err := model.NewRequestKey(identity.WorkspaceKey, identity.RequestID); err != nil {
		return true, strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)
	}
	return true, nil
}

func (s *Server) handleJobStatus(raw json.RawMessage) requestOutcome {
	var params protocol.JobStatusParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.JobID == "" && !params.All {
		params.All = true
	}
	if params.JobID != "" {
		return s.handleExactJobStatus(params.JobID)
	}
	statuses, errObj := s.listJobStatuses()
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	return requestOutcome{result: protocol.JobStatusResult{Jobs: statuses}}
}

func (s *Server) handleJobResult(raw json.RawMessage) requestOutcome {
	var params protocol.JobResultParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.JobID == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "jobId is required", protocol.ErrorData{})}
	}
	return s.handleExactJobResult(params.JobID)
}

func (s *Server) handleExactJobStatus(jobID string) requestOutcome {
	status, ok, authorityErr := s.authorityStatus(jobID)
	if ok {
		return requestOutcome{result: protocol.JobStatusResult{Jobs: []protocol.JobStatus{status}}}
	}
	if authorityErr != nil {
		return requestOutcome{err: authorityErr}
	}
	return requestOutcome{err: unknownAuthorityJobError(jobID)}
}

func (s *Server) handleExactJobResult(jobID string) requestOutcome {
	result, ok, authorityErr := s.authorityResult(jobID)
	if ok {
		return requestOutcome{result: result}
	}
	if authorityErr != nil {
		return requestOutcome{err: authorityErr}
	}
	return requestOutcome{err: unknownAuthorityJobError(jobID)}
}

func (s *Server) handleJobCancel(raw json.RawMessage) requestOutcome {
	if errObj := s.failStoppedRequestError(protocol.MethodJobCancel); errObj != nil {
		return requestOutcome{err: errObj}
	}
	var params protocol.JobCancelParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.JobID == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "jobId is required", protocol.ErrorData{})}
	}
	if _, _, ok, authorityErr := s.authorityJobProjection(params.JobID); authorityErr != nil {
		return requestOutcome{err: authorityErr}
	} else if ok {
		return s.withAdmissionJobEffect(params.JobID, func() requestOutcome {
			return s.handleAuthorityJobCancelLocked(params.JobID)
		})
	}
	return requestOutcome{err: unknownAuthorityJobError(params.JobID)}
}

func (s *Server) handleAuthorityJobCancelLocked(jobID string) requestOutcome {
	active := s.lookupActiveJob(jobID)
	// Keep the client-cancel snapshot and RequestCancel ordering together. The
	// runner may be between release and registration, or may itself be entering
	// cancel recovery; waiting for its diagnostic drain here lets both paths
	// pass the pre-terminal check and submit the same terminal intent. Runner
	// interruption and shutdown terminalization perform the bounded settle and
	// take their snapshot after it instead.
	count, ordinal := activeObservedWorkspaceWriteItemCount(active)
	if active != nil {
		active.requestTerminal(engine.StateCanceled, terminalCancellationFor(engine.CancellationOriginClientRequest, "client requested cancellation"))
		settled := active.interruptSessionNativeFirst()
		// Admission cancel is intentional containment. Mark the active launch
		// before coordinator containment so a killed process is the cancel
		// terminal path, not an unprovable safety event.
		if !settled {
			interruptCtx, interruptCancel := context.WithTimeout(context.Background(), admissionDetachedCleanupTimeout)
			err := active.interruptAdmissionCommand(interruptCtx)
			interruptCancel()
			if err != nil {
				if !admissionPhysicalCleanupUncertain(err) {
					return requestOutcome{err: admissionProtocolError(err)}
				}
			}
		}
	}
	record, projection, ok, errObj := s.authorityJobProjection(jobID)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	if !ok {
		return requestOutcome{err: unknownAuthorityJobError(jobID)}
	}
	if record.Terminal == nil {
		err := s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
			modelJobID := model.JobID(jobID)
			if err := coord.CancelWithMetadataAndObservedWorkspaceWriteItemCount(context.Background(), modelJobID, engine.CancellationOriginClientRequest, "client requested cancellation", count, ordinal, nil); err != nil {
				return err
			}
			s.abandonAdmissionUnresolvedCustody(context.Background(), coord, modelJobID)
			return nil
		})
		if err != nil {
			var reloadErr *protocol.ErrorObject
			record, projection, ok, reloadErr = s.authorityJobProjection(jobID)
			if reloadErr == nil && ok {
				if errors.Is(err, coordinator.ErrAlreadyFinalized) {
					if validErr := admissionValidTerminalRecord(record); validErr != nil {
						return requestOutcome{err: s.failStopAdmissionFinalizationReconcile(jobID, errors.Join(err, validErr))}
					}
					if abandonErr := s.abandonAdmissionRecordUnresolvedCustody(context.Background(), record); abandonErr != nil {
						log.Printf("agentbus daemon: job %s unresolved custody abandon warning: %v", jobID, abandonErr)
					}
					return requestOutcome{result: protocol.JobCancelResult{JobID: projection.JobID.String(), State: admissionState(projection.Public)}}
				}
				if admissionRecordTerminalCanceledByRequest(record) {
					if abandonErr := s.abandonAdmissionRecordUnresolvedCustody(context.Background(), record); abandonErr != nil {
						log.Printf("agentbus daemon: job %s unresolved custody abandon warning: %v", jobID, abandonErr)
					}
					return requestOutcome{result: protocol.JobCancelResult{JobID: projection.JobID.String(), State: admissionState(projection.Public)}}
				}
			}
			return requestOutcome{err: admissionProtocolError(err)}
		}
		var reloadErr *protocol.ErrorObject
		_, projection, ok, reloadErr = s.authorityJobProjection(jobID)
		if reloadErr != nil {
			return requestOutcome{err: reloadErr}
		}
		if !ok {
			return requestOutcome{err: unknownAuthorityJobError(jobID)}
		}
	}
	return requestOutcome{result: protocol.JobCancelResult{JobID: projection.JobID.String(), State: admissionState(projection.Public)}}
}

func admissionRecordTerminalCanceledByRequest(record model.SafetyRecord) bool {
	if record.Cancel == nil || record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled {
		return false
	}
	switch record.Terminal.Cause {
	case model.CauseCanceledBeforeAuthorization, model.CauseCanceledAfterAuthorization:
		return true
	default:
		return false
	}
}

func (s *Server) handlePolicyValidate(raw json.RawMessage) requestOutcome {
	var params protocol.PolicyValidateParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	contract := params.Contract
	contractName := ""
	if contract.Named != "" {
		resolved, name, _, err := engine.ResolveContract(contract, s.registry)
		if err != nil {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
		}
		contract = resolved
		contractName = name
	}
	result, err := engine.ValidateContract(params.Text, contract)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	result.ContractName = contractName
	return requestOutcome{result: result}
}

func (s *Server) handlePolicyRegister(raw json.RawMessage) requestOutcome {
	if errObj := s.failStoppedRequestError(protocol.MethodPolicyRegister); errObj != nil {
		return requestOutcome{err: errObj}
	}
	var params protocol.PolicyRegisterParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	hash, err := s.registry.Register(params.Name, params.Spec)
	if err != nil {
		var conflict engine.NameConflictError
		if errors.As(err, &conflict) {
			return requestOutcome{err: protocol.NewError(protocol.ErrorNameConflict, "policy name already registered with different spec", protocol.ErrorData{})}
		}
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	return requestOutcome{result: protocol.PolicyRegisterResult{Name: params.Name, ContractSHA256: hash, Registered: true}}
}

type jobRun struct {
	jobID                   string
	sessionID               string
	backend                 string
	backendImpl             engine.Backend
	cwd                     string
	model                   string
	effort                  string
	store                   *engine.Store
	session                 engine.Session
	prompt                  string
	write                   bool
	policy                  *engine.TurnPolicy
	contract                *engine.ContractSpec
	contractName            string
	contractHash            string
	timeout                 time.Duration
	codexIsolated           bool
	codexSessionDeferred    bool
	codexHome               string
	managedCodexHome        *managedCodexHome
	jobCache                jobCachePaths
	active                  *activeJob
	onDone                  func()
	authoritativeCompletion bool
	admissionControlled     bool
	admissionLaunchFailed   bool
	admissionMode           model.Mode
	admissionAccepted       authority.AcceptResult
	admissionLaunch         admissionLaunchBinding
	logPaths                engine.LogPaths
}

func (s *Server) runJob(ctx context.Context, run jobRun) {
	run.authoritativeCompletion = true
	defer func() {
		s.removeActiveJob(run.jobID)
		if run.admissionControlled {
			s.enforceAdmissionLogRetention(run.admissionAccepted.Record.WorkspaceLayoutKey.String())
		}
	}()
	defer s.cleanupManagedCodexHomeForAdmissionRun(run)
	defer func() {
		if run.onDone != nil {
			run.onDone()
		}
	}()
	defer run.active.cancel()
	if run.active.requestedTerminal() != "" {
		if err := s.finalizeRequestedTerminal(run); err != nil {
			s.handleRunFinalizationError(run, err)
		}
		return
	}
	attemptPrompt := applyPrologue(run.policy, run.prompt)
	text, state, err := s.runAttempt(ctx, run, attemptPrompt, run.write, model.LaunchOrdinalOne)
	if requested := run.active.requestedTerminal(); requested != "" {
		s.completeRunTerminal(run, requested, text, nil)
		return
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.completeRunTerminal(run, engine.StateTimedOut, text, skippedStampForRun(run, s.registry, engine.SkipTimeout))
			return
		}
		if errors.Is(err, context.Canceled) {
			state = engine.StateInterrupted
		}
		s.completeRunFailure(run, state, text, skippedStampForRun(run, s.registry, skippedReasonForState(state)), terminalFailureOriginFor(err, terminalFailureBackendRan), err)
		return
	}

	validation, retryPrompt, compliantState, err := s.validateAttempt(text, run, 1, false)
	if err != nil {
		s.completeRunFailure(run, engine.StateFailed, text, skippedStampForRun(run, s.registry, engine.SkipBackendError), terminalFailureInternal, err)
		return
	}
	if retryPrompt != "" {
		retryText, retryState, retryErr := s.runAttempt(ctx, run, retryPrompt, false, model.LaunchOrdinalTwo)
		if requested := run.active.requestedTerminal(); requested != "" {
			s.completeRunTerminal(run, requested, retryText, nil)
			return
		}
		if retryErr != nil {
			if errors.Is(retryErr, context.DeadlineExceeded) {
				s.completeRunTerminal(run, engine.StateTimedOut, retryText, skippedStampForRun(run, s.registry, engine.SkipTimeout))
				return
			}
			s.completeRunFailure(run, retryState, retryText, skippedStampForRun(run, s.registry, skippedReasonForState(retryState)), terminalFailureOriginFor(retryErr, terminalFailureBackendRan), retryErr)
			return
		}
		retryValidation, _, retryCompliantState, err := s.validateAttempt(retryText, run, 2, true)
		if err != nil {
			s.completeRunFailure(run, engine.StateFailed, retryText, skippedStampForRun(run, s.registry, engine.SkipBackendError), terminalFailureInternal, err)
			return
		}
		s.completeRunTerminal(run, retryCompliantState, retryText, retryValidation.Stamp)
		return
	}
	s.completeRunTerminal(run, compliantState, text, validation.Stamp)
}

func (s *Server) runAttempt(ctx context.Context, run jobRun, prompt string, write bool, ordinal model.LaunchOrdinal) (string, engine.JobState, error) {
	attemptCtx := ctx
	var cancel context.CancelFunc
	if run.timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, run.timeout)
	} else {
		attemptCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	input := engine.TurnInput{
		Prompt:   prompt,
		Write:    write,
		Timeout:  run.timeout,
		LogPaths: run.logPaths,
	}
	if run.admissionControlled {
		run.active.beginObservedWorkspaceWriteAttempt(ordinal)
	}
	if run.admissionControlled {
		startedAt := s.clock.Now().UTC()
		s.rememberJobLivenessStartedAt(run.jobID, startedAt)
		if err := s.recordAdmissionFinalAttemptStart(attemptCtx, run.jobID, startedAt); err != nil {
			return "", engine.StateFailed, classifyFailureError(terminalFailureBackendNotStarted, err)
		}
	}
	settleRequested, finishSettling := run.active.beginAdmissionDiagnosticsSettle()
	if write && run.jobCache.root != "" {
		log.Printf("agentbus daemon: job %s write cache root=%s go-build=%s go-mod=%s tmp=%s", run.jobID, run.jobCache.root, run.jobCache.goBuild, run.jobCache.goMod, run.jobCache.tmp)
	}
	events, err := s.admissionTurnEvents(attemptCtx, run, input, ordinal)
	if err != nil {
		finishSettling()
		return "", engine.StateFailed, err
	}
	defer finishSettling()
	var assistantText strings.Builder
	var resultText string
	hasResultMessage := false
	var terminalState engine.JobState
	var terminalErr error
	for {
		select {
		case <-settleRequested:
			s.settleAdmissionEventDrain(run, events)
			return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateCanceled, context.Canceled
		default:
		}
		select {
		case <-attemptCtx.Done():
			if shouldInterruptSessionOnAttemptCancel(run, attemptCtx.Err()) {
				_ = run.session.Interrupt(context.Background())
			}
			s.settleAdmissionEventDrain(run, events)
			return attemptFinalText(hasResultMessage, resultText, assistantText.String()), stateForContext(attemptCtx.Err()), attemptCtx.Err()
		case <-settleRequested:
			s.settleAdmissionEventDrain(run, events)
			return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateCanceled, context.Canceled
		case event, ok := <-events:
			if !ok {
				if terminalErr != nil {
					return attemptFinalText(hasResultMessage, resultText, assistantText.String()), terminalState, terminalErr
				}
				if attemptCtx.Err() != nil {
					return attemptFinalText(hasResultMessage, resultText, assistantText.String()), stateForContext(attemptCtx.Err()), attemptCtx.Err()
				}
				return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateCompleted, nil
			}
			if run.admissionControlled {
				s.recordJobLivenessEvent(run.jobID)
			}
			if err := s.recordAdmissionEventDiagnostics(run, event); err != nil {
				return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateFailed, classifyFailureError(terminalFailureInternal, err)
			}
			rawText := authoritativeText(event)
			switch event.Type {
			case engine.EventAgentText:
				assistantText.WriteString(rawText)
			case engine.EventResultMessage:
				resultText = rawText
				hasResultMessage = true
			case engine.EventModelReported:
				if err := s.recordModelReported(run, event.ModelReported); err != nil {
					s.deferAdmissionEventDrain(run, events)
					return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateFailed, classifyFailureError(terminalFailureInternal, err)
				}
			case engine.EventTerminalError:
				if rawText == "" {
					rawText = "backend failed"
				}
				terminalState = engine.StateFailed
				cause := event.Err
				if cause == nil {
					cause = errors.New(rawText)
				}
				terminalErr = classifyFailureError(terminalFailureBackendRan, cause)
			case engine.EventTurnFinal:
			}
		}
	}
}

func (s *Server) recordAdmissionEventDiagnostics(run jobRun, event engine.Event) error {
	if event.ObservedWorkspaceWriteItem {
		run.active.observeWorkspaceWriteItem()
	}
	if drops, ok := engine.TransportFrameDropsFromMetadata(event.Metadata); ok {
		return s.recordTransportFrameDrops(run, drops)
	}
	return nil
}

// settleAdmissionEventDrain consumes diagnostics until the backend stream
// closes or the native interrupt grace expires. Workspace-write events after
// that boundary cannot be merged into a terminal record; the deferred drain
// still preserves transport-drop diagnostics, which support post-terminal
// recording.
func (s *Server) settleAdmissionEventDrain(run jobRun, events <-chan engine.Event) {
	if !run.admissionControlled || events == nil {
		return
	}
	deadline := time.Now().Add(admissionNativeInterruptGrace)
	for {
		if !time.Now().Before(deadline) {
			s.deferAdmissionEventDrain(run, events)
			return
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case event, ok := <-events:
			timer.Stop()
			if !ok {
				return
			}
			if err := s.recordAdmissionEventDiagnostics(run, event); err != nil {
				log.Printf("agentbus daemon: job %s terminal diagnostic settle warning: %v", run.jobID, err)
			}
		case <-timer.C:
			s.deferAdmissionEventDrain(run, events)
			return
		}
	}
}

func (s *Server) recordTransportFrameDrops(run jobRun, drops engine.TransportFrameDrops) error {
	if drops.Empty() {
		return nil
	}
	if run.admissionControlled {
		jobID, err := model.NewJobID(run.jobID)
		if err != nil {
			return err
		}
		s.admissionStateMu.RLock()
		ready := s.admissionReady
		available := s.admissionInstance != nil && ready != nil
		s.admissionStateMu.RUnlock()
		if !available {
			return authority.ErrNotReady
		}
		_, err = ready.RecordTransportFrameDrops(context.Background(), jobID, drops)
		return err
	}
	if run.store == nil {
		return nil
	}
	_, err := run.store.Update(run.jobID, func(record *engine.JobRecord) (bool, error) {
		if record.TransportFrameDrops == nil {
			copied := drops
			record.TransportFrameDrops = &copied
			return true, nil
		}
		before := *record.TransportFrameDrops
		record.TransportFrameDrops.Merge(drops)
		return *record.TransportFrameDrops != before, nil
	})
	return err
}

// deferAdmissionEventDrain waits for an adapter stream that outlives the
// bounded terminal diagnostic settle. Deferred backend-log cleanup observes
// its completion before inspecting the corresponding files.
func (s *Server) deferAdmissionEventDrain(run jobRun, events <-chan engine.Event) {
	if !run.admissionControlled || run.jobID == "" || events == nil {
		return
	}
	done := make(chan struct{})
	s.mu.Lock()
	if s.admissionLogDrains == nil {
		s.admissionLogDrains = make(map[string]<-chan struct{})
	}
	if _, exists := s.admissionLogDrains[run.jobID]; exists {
		s.mu.Unlock()
		return
	}
	s.admissionLogDrains[run.jobID] = done
	s.mu.Unlock()
	go func() {
		for event := range events {
			if drops, ok := engine.TransportFrameDropsFromMetadata(event.Metadata); ok {
				if err := s.recordTransportFrameDrops(run, drops); err != nil {
					log.Printf("agentbus daemon: job %s deferred transport frame-drop recording warning: %v", run.jobID, err)
				}
			}
		}
		close(done)
	}()
}

func (s *Server) admissionLogDrain(jobID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admissionLogDrains[jobID]
}

func (s *Server) finishAdmissionLogDrain(jobID string, done <-chan struct{}) {
	s.mu.Lock()
	if s.admissionLogDrains[jobID] == done {
		delete(s.admissionLogDrains, jobID)
	}
	s.mu.Unlock()
}

func attemptFinalText(hasResultMessage bool, resultText, assistantText string) string {
	if hasResultMessage {
		return resultText
	}
	return assistantText
}

func shouldInterruptSessionOnAttemptCancel(run jobRun, err error) bool {
	if errors.Is(err, context.Canceled) && run.active != nil && run.active.requestedTerminal() == engine.StateCanceled {
		return false
	}
	return true
}

func (s *Server) validateAttempt(text string, run jobRun, attempts int, retryUsed bool) (engine.PolicyValidation, string, engine.JobState, error) {
	if run.policy == nil || run.policy.Contract == nil {
		return engine.PolicyValidation{}, "", engine.StateCompleted, nil
	}
	if err := validateRetryPolicy(run.policy.Retry); err != nil {
		return engine.PolicyValidation{}, "", engine.StateFailed, err
	}
	var resolved engine.ContractSpec
	name := run.contractName
	if run.contract != nil {
		resolved = *run.contract
	} else {
		var err error
		resolved, name, _, err = engine.ResolveContract(*run.policy.Contract, s.registry)
		if err != nil {
			return engine.PolicyValidation{}, "", engine.StateFailed, err
		}
	}
	result, err := engine.ValidateContract(text, resolved)
	if err != nil {
		return engine.PolicyValidation{}, "", engine.StateFailed, err
	}
	stamp := engine.StampValidation(attempts, retryUsed, name, result, s.clock.Now().UTC())
	validation := engine.PolicyValidation{Stamp: &stamp, ResolvedContract: &resolved}
	if !result.Valid && !retryUsed && run.policy.Retry != nil && run.policy.Retry.Max == 1 {
		retryPrompt := engine.RenderRetryTemplate(run.policy.Retry.Template, result.Missing)
		return validation, retryPrompt, engine.StateCompletedNoncompliant, nil
	}
	if result.Valid {
		return validation, "", engine.StateCompleted, nil
	}
	return validation, "", engine.StateCompletedNoncompliant, nil
}

func validateRetryPolicy(retry *engine.RetryPolicy) error {
	if retry == nil {
		return nil
	}
	if retry.Max != 0 && retry.Max != 1 {
		return errors.New("retry.max must be 0 or 1")
	}
	if retry.Max == 1 {
		if !strings.Contains(retry.Template, "{{missing}}") {
			return errors.New("retry.template must include {{missing}} when retry.max is 1")
		}
	}
	return nil
}

func (s *Server) resolvePolicy(policy *engine.TurnPolicy) (resolvedPolicy, error) {
	if policy == nil || policy.Contract == nil {
		return resolvedPolicy{policy: policy}, nil
	}
	if err := validateRetryPolicy(policy.Retry); err != nil {
		return resolvedPolicy{}, err
	}
	resolved, name, hash, err := engine.ResolveContract(*policy.Contract, s.registry)
	if err != nil {
		return resolvedPolicy{}, err
	}
	return resolvedPolicy{policy: policy, contract: &resolved, name: name, hash: hash}, nil
}

func (s *Server) finalizeFailure(run jobRun, err error) {
	s.completeRunFailure(run, engine.StateFailed, "", nil, terminalFailureInternal, err)
}

func (s *Server) finalizeRequestedTerminal(run jobRun) error {
	state := run.active.requestedTerminal()
	if state == "" {
		state = engine.StateCanceled
	}
	return s.finalizeTerminal(run, state, "", nil)
}

func (s *Server) completeRunTerminal(run jobRun, state engine.JobState, text string, stamp *engine.ContractStamp) {
	if err := s.finalizeTerminal(run, state, text, stamp); err != nil {
		s.handleRunFinalizationError(run, err)
	}
}

func (s *Server) completeRunFailure(run jobRun, state engine.JobState, text string, stamp *engine.ContractStamp, origin terminalFailureOrigin, cause error) {
	failure := terminalFailureFor(origin, cause, terminalFailureStopWasRequestedByAgentbus(run, cause))
	if err := s.recordFailureMetadata(run, failure); err != nil {
		if err := s.recordFailureMetadata(run, failure); err != nil {
			log.Printf("agentbus daemon: job %s failure metadata persistence failed: %v", run.jobID, err)
		}
	}
	s.completeRunTerminal(run, state, text, stamp)
}

func (s *Server) handleRunFinalizationError(run jobRun, err error) {
	if err == nil {
		return
	}
	log.Printf("agentbus daemon: job %s finalization failed: %v", run.jobID, err)
	if failureErr := s.recordFailureMetadata(run, terminalFailureFor(terminalFailureFinalization, err, false)); failureErr != nil {
		log.Printf("agentbus daemon: job %s finalization failure metadata persistence failed: %v", run.jobID, failureErr)
	}
	if !run.admissionControlled {
		return
	}
	failStopCtx, cancel := detachedAdmissionFailStopContext(context.Background())
	defer cancel()
	if stopErr := s.failStopAdmissionReady(failStopCtx, err); stopErr != nil {
		log.Printf("agentbus daemon: job %s finalization fail-stop failed: %v", run.jobID, stopErr)
	}
}

func (s *Server) failStopAdmissionFinalizationReconcile(jobID string, err error) *protocol.ErrorObject {
	failStopCtx, cancel := detachedAdmissionFailStopContext(context.Background())
	defer cancel()
	if stopErr := s.failStopAdmissionReady(failStopCtx, err); stopErr != nil {
		log.Printf("agentbus daemon: job %s finalization reconcile fail-stop failed: %v", jobID, stopErr)
	}
	return admissionProtocolError(err)
}

func (s *Server) finalizeTerminal(run jobRun, state engine.JobState, text string, stamp *engine.ContractStamp) error {
	if run.admissionControlled {
		return s.completeAdmissionRun(run, state, text, stamp)
	}
	backendSessionID := ""
	if run.session != nil {
		backendSessionID = run.session.ID()
	}
	_, err := run.store.Update(run.jobID, func(record *engine.JobRecord) (bool, error) {
		lateFinalization := run.authoritativeCompletion && canLateFinalize(record.State, state)
		if engine.IsTerminal(record.State) && !lateFinalization {
			return false, nil
		}
		var result *engine.ResultInfo
		if state == engine.StateCompleted || state == engine.StateCompletedNoncompliant {
			if text == "" && run.policy != nil && run.policy.Contract != nil && stamp == nil {
				stamp = skippedStampForRun(run, s.registry, engine.SkipNoFinalMessage)
			}
			info, err := run.store.WriteResult(run.jobID, []byte(text), s.inlineResultCap)
			if err != nil {
				return false, err
			}
			info.ModelReported = record.ModelReported
			result = &info
		}
		if lateFinalization && record.State == engine.StateReaped {
			if err := record.Transition(state, s.clock.Now().UTC()); err != nil {
				return false, err
			}
		} else if err := transitionOrSet(record, state, s.clock.Now().UTC()); err != nil {
			return false, err
		}
		record.Result = result
		if stamp != nil {
			record.Contract = stamp
		}
		if run.contract != nil {
			resolved := *run.contract
			record.ResolvedContract = &resolved
		}
		if backendSessionID != "" {
			record.BackendSessionID = backendSessionID
		}
		if lateFinalization {
			record.LateFinalization = true
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func discardEmptyBackendLogs(paths engine.LogPaths) error {
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backend log %q is not a regular file", path)
		}
		if info.Size() == 0 {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func discardBackendLogs(paths engine.LogPaths) error {
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backend log %q is not a regular file", path)
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Server) recordModelReported(run jobRun, reported string) error {
	if strings.TrimSpace(reported) == "" {
		return nil
	}
	if run.admissionControlled {
		s.rememberReportedModel(run.jobID, reported)
		return nil
	}
	if run.store == nil {
		return nil
	}
	_, err := run.store.Update(run.jobID, func(record *engine.JobRecord) (bool, error) {
		if record.ModelReported == reported {
			return false, nil
		}
		record.ModelReported = reported
		return true, nil
	})
	return err
}

// maxReportedModels bounds the transient strict reported-model cache. This is
// best-effort runtime metadata with no durable home: removeActiveJob is too early
// to evict (the terminal job.result read happens after completion), so entries are
// retained past completion and capped here with FIFO eviction to prevent unbounded
// growth on a long-running daemon. The cap comfortably covers the realistic
// submit->poll->result window for concurrently-tracked jobs.
const maxReportedModels = 4096

func (s *Server) rememberReportedModel(jobID, reported string) {
	if strings.TrimSpace(jobID) == "" || strings.TrimSpace(reported) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reportedModels == nil {
		s.reportedModels = make(map[string]string)
	}
	if _, exists := s.reportedModels[jobID]; !exists {
		if len(s.reportedModelOrder) >= maxReportedModels {
			oldest := s.reportedModelOrder[0]
			s.reportedModelOrder = s.reportedModelOrder[1:]
			delete(s.reportedModels, oldest)
		}
		s.reportedModelOrder = append(s.reportedModelOrder, jobID)
	}
	s.reportedModels[jobID] = reported
}

func (s *Server) reportedModel(jobID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reportedModels[jobID]
}

func (s *Server) rememberJobLivenessStarted(jobID string) {
	s.rememberJobLivenessStartedAt(jobID, s.clock.Now().UTC())
}

func (s *Server) rememberJobLivenessStartedAt(jobID string, now time.Time) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobLiveness == nil {
		s.jobLiveness = make(map[string]*jobLiveness)
	}
	if s.jobLiveness[jobID] == nil {
		s.jobLiveness[jobID] = &jobLiveness{startedAt: now, lastEventAt: now}
	}
}

func (s *Server) recordAdmissionFinalAttemptStart(ctx context.Context, jobID string, startedAt time.Time) error {
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		return err
	}
	s.admissionStateMu.RLock()
	ready := s.admissionReady
	available := s.admissionInstance != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !available {
		return authority.ErrNotReady
	}
	_, err = ready.RecordFinalAttemptStart(ctx, modelJobID, startedAt)
	return err
}

func (s *Server) recordJobLivenessEvent(jobID string) {
	if strings.TrimSpace(jobID) == "" {
		return
	}
	now := s.clock.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jobLiveness == nil {
		s.jobLiveness = make(map[string]*jobLiveness)
	}
	liveness := s.jobLiveness[jobID]
	if liveness == nil {
		liveness = &jobLiveness{startedAt: now}
		s.jobLiveness[jobID] = liveness
	}
	liveness.lastEventAt = now
	liveness.eventCount++
}

func (s *Server) jobLivenessSnapshot(jobID string) (time.Time, time.Time, int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	liveness := s.jobLiveness[jobID]
	if liveness == nil {
		return time.Time{}, time.Time{}, 0, false
	}
	return liveness.startedAt, liveness.lastEventAt, liveness.eventCount, true
}

func canLateFinalize(from, to engine.JobState) bool {
	switch from {
	case engine.StateOrphaned:
		return to == engine.StateCompleted || to == engine.StateCompletedNoncompliant || to == engine.StateFailed
	case engine.StateReaped:
		return to == engine.StateCompleted || to == engine.StateCompletedNoncompliant
	default:
		return false
	}
}

func (s *Server) heartbeat(store *engine.Store, jobID string, done <-chan struct{}) {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			active, err := s.refreshHeartbeat(store, jobID)
			if err != nil || !active {
				return
			}
		}
	}
}

func (s *Server) refreshHeartbeat(store *engine.Store, jobID string) (bool, error) {
	return store.TouchHeartbeat(jobID, s.clock.Now().UTC(), s.leaseDuration)
}

func (s *Server) abortUndeliveredRun(run jobRun, state engine.JobState) {
	if run.active != nil && run.active.cancel != nil {
		run.active.cancel()
	}
	switch state {
	case engine.StateInterrupted:
		_, _ = run.store.Interrupt(run.jobID)
	case engine.StateCanceled:
		_, _ = run.store.CancelWithMetadata(run.jobID, engine.CancellationOriginUnattributable, "canceled without an attributable origin")
	default:
		_ = s.finalizeTerminal(run, state, "", nil)
	}
	s.removeActiveJob(run.jobID)
	if run.onDone != nil {
		run.onDone()
	}
}

func (s *Server) createQueuedRecord(store *engine.Store, jobID, sessionID, backend string, tags map[string]string, policy *engine.TurnPolicy, resolved *engine.ContractSpec, timeout *engine.TimeoutResolution, foreground bool) error {
	logPaths, err := ensureLogFiles(store, jobID)
	if err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	ref := s.currentProcessRef()
	var resolvedCopy *engine.ContractSpec
	if resolved != nil {
		copy := *resolved
		resolvedCopy = &copy
	}
	record := &engine.JobRecord{
		JobID:            jobID,
		SessionID:        sessionID,
		Backend:          backend,
		Timeout:          engine.CloneTimeoutResolution(timeout),
		Foreground:       foreground,
		State:            engine.StateQueued,
		Tags:             cloneTags(tags),
		StartedAt:        now,
		UpdatedAt:        now,
		HeartbeatAt:      now,
		Lease:            engine.Lease{ExpiresAt: now.Add(s.leaseDuration)},
		Supervisor:       ref,
		LogPaths:         &logPaths,
		Policy:           policy,
		ResolvedContract: resolvedCopy,
	}
	return store.Save(record)
}

func (s *Server) transitionRecord(store *engine.Store, jobID string, state engine.JobState) error {
	_, err := store.Update(jobID, func(record *engine.JobRecord) (bool, error) {
		if engine.IsTerminal(record.State) {
			return false, nil
		}
		if err := transitionOrSet(record, state, s.clock.Now().UTC()); err != nil {
			return false, err
		}
		now := s.clock.Now().UTC()
		record.HeartbeatAt = now
		record.Lease = engine.Lease{ExpiresAt: now.Add(s.leaseDuration)}
		return true, nil
	})
	return err
}

func transitionOrSet(record *engine.JobRecord, state engine.JobState, now time.Time) error {
	if err := record.Transition(state, now); err != nil {
		if engine.IsTerminal(record.State) {
			return nil
		}
		return err
	}
	return nil
}

func (s *Server) storeForCWDLocked(cwd string) (*engine.Store, error) {
	canon, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		return nil, err
	}
	store := s.stores[canon]
	if store != nil {
		return store, nil
	}
	key := engine.WorkspaceKey(canon)
	if store := s.storesByKey[key]; store != nil {
		s.stores[canon] = store
		return store, nil
	}
	store, err = engine.NewStore(engine.StoreConfig{
		Root:          s.stateRoot,
		CWD:           canon,
		Clock:         s.clock,
		Processes:     s.processes,
		ProcessGroups: s.processGroups,
		CancelGrace:   s.cancelGrace,
		CancelWaiter:  s.cancelWaiter,
		LeaseDuration: s.leaseDuration,
		GCInterval:    s.gcInterval,
	})
	if err != nil {
		return nil, err
	}
	return s.rememberStoreLocked(store), nil
}

func (s *Server) storeForJob(jobID string) *engine.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jobStores[jobID]
}

func (s *Server) addActiveJob(job *activeJob) {
	s.mu.Lock()
	s.activeJobs[job.jobID] = job
	s.mu.Unlock()
	s.touchActivity()
}

func (s *Server) removeActiveJob(jobID string) {
	s.mu.Lock()
	delete(s.activeJobs, jobID)
	delete(s.jobLiveness, jobID)
	s.mu.Unlock()
	s.touchActivity()
}

func (s *Server) withAdmissionJobEffect(jobID string, fn func() requestOutcome) requestOutcome {
	lock := s.admissionEffectLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func (s *Server) withAdmissionJobEffectErr(jobID string, fn func() error) error {
	lock := s.admissionEffectLock(jobID)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func (s *Server) admissionEffectLock(jobID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.admissionEffectMu == nil {
		s.admissionEffectMu = make(map[string]*sync.Mutex)
	}
	lock := s.admissionEffectMu[jobID]
	if lock == nil {
		lock = &sync.Mutex{}
		s.admissionEffectMu[jobID] = lock
	}
	return lock
}

func (s *Server) lookupActiveJob(jobID string) *activeJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeJobs[jobID]
}

func (s *Server) listJobStatuses() ([]protocol.JobStatus, *protocol.ErrorObject) {
	statuses, errObj := s.listAuthorityStatuses()
	if errObj != nil {
		return nil, errObj
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].JobID < statuses[j].JobID })
	return statuses, nil
}

func (s *Server) rememberStoreLocked(store *engine.Store) *engine.Store {
	key := store.Layout().Key
	if existing := s.storesByKey[key]; existing != nil {
		if workspace := store.Layout().Workspace; workspace != "" {
			s.stores[workspace] = existing
		}
		return existing
	}
	s.storesByKey[key] = store
	if workspace := store.Layout().Workspace; workspace != "" {
		s.stores[workspace] = store
	}
	return store
}

func (s *Server) currentProcessRef() engine.ProcessRef {
	pid := os.Getpid()
	info, alive, _ := s.processes.Lookup(pid)
	ref := engine.ProcessRef{PID: pid}
	if alive {
		ref.StartTime = info.StartTime
	}
	return ref
}

func (s *Server) nextID(prefix string) string {
	n := s.id.Add(1)
	return fmt.Sprintf("%s_%s_%06d", prefix, s.clock.Now().UTC().Format("20060102T150405000000000Z"), n)
}

func (s *Server) backendNames() []string {
	backends := s.backendSnapshot()
	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Server) backendFor(name string) (engine.Backend, bool) {
	s.backendMapMu.RLock()
	defer s.backendMapMu.RUnlock()
	backend, ok := s.backends[name]
	return backend, ok
}

func (s *Server) backendSnapshot() map[string]engine.Backend {
	s.backendMapMu.RLock()
	defer s.backendMapMu.RUnlock()
	backends := make(map[string]engine.Backend, len(s.backends))
	for name, backend := range s.backends {
		backends[name] = backend
	}
	return backends
}

func (s *Server) replaceBackend(name string, backend engine.Backend) {
	s.backendMapMu.Lock()
	defer s.backendMapMu.Unlock()
	if s.backends == nil {
		s.backends = make(map[string]engine.Backend)
	}
	s.backends[name] = backend
}

func (s *Server) removeBackend(name string) {
	s.backendMapMu.Lock()
	defer s.backendMapMu.Unlock()
	delete(s.backends, name)
}

func ensureToken(path, configured string) (string, error) {
	token, err := readExistingToken(path)
	if err == nil {
		return token, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	token = configured
	if token == "" {
		token, err = randomToken()
		if err != nil {
			return "", err
		}
	}
	created, err := createTokenExclusive(path, token)
	if err == nil {
		return created, nil
	}
	if errors.Is(err, os.ErrExist) {
		return waitForExistingToken(path)
	}
	return "", err
}

func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func readExistingToken(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", errors.New("agentbus token file is empty")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return "", err
	}
	return token, nil
}

func waitForExistingToken(path string) (string, error) {
	deadline := time.Now().Add(time.Second)
	var last error
	for {
		token, err := readExistingToken(path)
		if err == nil {
			return token, nil
		}
		last = err
		if !os.IsNotExist(err) && !strings.Contains(err.Error(), "token file is empty") {
			return "", err
		}
		if time.Now().After(deadline) {
			return "", last
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func createTokenExclusive(path, token string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if _, err := file.Write([]byte(token + "\n")); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	return token, nil
}

func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
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
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	return nil
}

func atomicWriteDurable(path string, data []byte, mode os.FileMode) error {
	if err := atomicWrite(path, data, mode); err != nil {
		return err
	}
	return syncFileAndParent(path)
}

func ensureLogFiles(store *engine.Store, jobID string) (engine.LogPaths, error) {
	layout := store.Layout()
	paths, err := engine.LogPathsForLayout(layout, jobID)
	if err != nil {
		return engine.LogPaths{}, err
	}
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return engine.LogPaths{}, err
		}
		if err := f.Chmod(0o600); err != nil {
			_ = f.Close()
			return engine.LogPaths{}, err
		}
		if err := f.Close(); err != nil {
			return engine.LogPaths{}, err
		}
	}
	return paths, nil
}

func decodeStrict(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		return errors.New("params must contain one JSON object")
	}
	return nil
}

func invalidParams(err error) requestOutcome {
	return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
}

func unknownAuthorityJobError(jobID string) *protocol.ErrorObject {
	return protocol.NewError(protocol.ErrorUnknownJob, "job is not known", protocol.ErrorData{JobID: jobID})
}

func backendError(err error) *protocol.ErrorObject {
	if err == nil {
		return nil
	}
	return protocol.NewError(protocol.ErrorBackendUnavailable, err.Error(), protocol.ErrorData{})
}

func timeoutFromMillis(ms *int64) (time.Duration, *engine.TimeoutResolution, *protocol.ErrorObject) {
	if ms == nil {
		return protocol.DefaultTimeout, &engine.TimeoutResolution{
			Effective: protocol.DefaultTimeout.Milliseconds(),
			Source:    engine.TimeoutSourceDaemonDefault,
		}, nil
	}
	if *ms < 0 {
		return 0, nil, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs cannot be negative", protocol.ErrorData{})
	}
	if *ms == 0 {
		return 0, &engine.TimeoutResolution{
			Requested: ms,
			Effective: 0,
			Source:    engine.TimeoutSourceClient,
		}, nil
	}
	if *ms > protocol.MaxTimeout.Milliseconds() {
		return 0, nil, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs exceeds maximum", protocol.ErrorData{})
	}
	d := time.Duration(*ms) * time.Millisecond
	return d, &engine.TimeoutResolution{
		Requested: ms,
		Effective: d.Milliseconds(),
		Source:    engine.TimeoutSourceClient,
	}, nil
}

func validateTaskSpecEnvelope(raw json.RawMessage) *protocol.ErrorObject {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
	specRaw, ok := envelope["taskSpec"]
	if !ok {
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec is required", protocol.ErrorData{})
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(specRaw, &fields); err != nil {
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec must be an object", protocol.ErrorData{})
	}
	for _, required := range []string{"backend", "cwd", "write", "prompt"} {
		if _, ok := fields[required]; !ok {
			return protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec missing required field "+required, protocol.ErrorData{})
		}
	}
	return nil
}

func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func tagsMatch(actual, want map[string]string) bool {
	for key, value := range want {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func applyPrologue(policy *engine.TurnPolicy, prompt string) string {
	if policy == nil || policy.Prologue == "" {
		return prompt
	}
	return policy.Prologue + "\n\n" + prompt
}

func authoritativeText(event engine.Event) string {
	if event.RawText != "" {
		return event.RawText
	}
	return event.Text
}

func stateForContext(err error) engine.JobState {
	if errors.Is(err, context.DeadlineExceeded) {
		return engine.StateTimedOut
	}
	if errors.Is(err, context.Canceled) {
		return engine.StateInterrupted
	}
	return engine.StateFailed
}

func skippedReasonForState(state engine.JobState) engine.SkippedReason {
	switch state {
	case engine.StateTimedOut:
		return engine.SkipTimeout
	case engine.StateInterrupted, engine.StateCanceled:
		return engine.SkipInterrupt
	default:
		return engine.SkipBackendError
	}
}

func skippedStamp(policy *engine.TurnPolicy, registry *engine.PolicyRegistry, reason engine.SkippedReason) *engine.ContractStamp {
	if policy == nil || policy.Contract == nil {
		return nil
	}
	_, name, hash, err := engine.ResolveContract(*policy.Contract, registry)
	if err != nil {
		return nil
	}
	stamp := engine.SkippedContractStamp(reason, 1, false, name, hash)
	now := time.Now().UTC()
	stamp.ValidatedAt = now
	return &stamp
}

func skippedStampForRun(run jobRun, registry *engine.PolicyRegistry, reason engine.SkippedReason) *engine.ContractStamp {
	if run.policy == nil || run.policy.Contract == nil {
		return nil
	}
	name := run.contractName
	hash := run.contractHash
	if hash == "" && run.contract != nil {
		computed, err := engine.ContractSHA256(*run.contract)
		if err != nil {
			return nil
		}
		hash = computed
	}
	if hash == "" {
		return skippedStamp(run.policy, registry, reason)
	}
	stamp := engine.SkippedContractStamp(reason, 1, false, name, hash)
	now := time.Now().UTC()
	stamp.ValidatedAt = now
	return &stamp
}
