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
)

const duplicateAuthorityJSONWarning = "duplicate-job-id: authority record also has legacy JSON record; authority selected"

var ErrDaemonAlreadyListening = errors.New("agentbus daemon already listening")

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
	if _, err := os.Lstat(socketPath); err != nil {
		return false
	}
	conn, err := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
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
	StateRoot           string
	CWD                 string
	SocketPath          string
	Token               string
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
	ReapInterval        time.Duration
	GCInterval          time.Duration
	ReapTickInterval    time.Duration
	ReadyHook           func(ServeReadyInfo) error
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

type tickerSource struct {
	c    <-chan time.Time
	stop func()
}

func newTickerSource(interval time.Duration) tickerSource {
	ticker := time.NewTicker(interval)
	return tickerSource{c: ticker.C, stop: ticker.Stop}
}

type admissionOwnedWorkChecker interface {
	HasOwnedWork(context.Context) (bool, error)
}

// Server serves the protocol v1 socket API over engine backends.
type Server struct {
	stateRoot              string
	cwd                    string
	socketPath             string
	tokenPath              string
	token                  string
	backends               map[string]engine.Backend
	registry               *engine.PolicyRegistry
	clock                  engine.Clock
	processes              engine.ProcessTable
	processGroups          engine.ProcessGroupSignaler
	cancelGrace            time.Duration
	cancelWaiter           engine.Waiter
	id                     atomic.Uint64
	clients                atomic.Int64
	accepting              atomic.Int64
	idleTimeout            time.Duration
	idleCheckInterval      time.Duration
	binaryIdentityProbe    BinaryIdentityProbe
	beforeStaleCloseHook   func()
	staleListenerHook      func()
	staleSocketRemovedHook func()
	inlineResultCap        int
	leaseDuration          time.Duration
	heartbeatInterval      time.Duration
	reapInterval           time.Duration
	gcInterval             time.Duration
	reapTickInterval       time.Duration
	reapTickFactory        func(time.Duration) tickerSource
	readyHook              func(ServeReadyInfo) error
	afterReapTickHook      func(error)
	listenerFactory        func() (net.Listener, socketFileIdentity, error)
	beforeListenBindHook   func()
	safetyLatch            *SafetyLatch
	safetyDrainTimeout     time.Duration
	jobsRequestIDEnabled   bool
	admissionSubmitMu      sync.Mutex
	admissionCloseEpoch    atomic.Uint64
	admissionOpenEpoch     atomic.Uint64
	admissionStateMu       sync.RWMutex
	resultPublications     atomic.Int64

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

	mu                 sync.Mutex
	stores             map[string]*engine.Store
	storesByKey        map[string]*engine.Store
	jobStores          map[string]*engine.Store
	admissionJobs      map[string]struct{}
	admissionEffectMu  map[string]*sync.Mutex
	activeJobs         map[string]*activeJob
	lastActivity       time.Time
	executablePath     string
	executableIdentity BinaryIdentity
	binaryStale        bool
}

type activeJob struct {
	jobID     string
	sessionID string
	session   engine.Session
	cancel    context.CancelFunc

	mu                sync.Mutex
	terminal          engine.JobState
	admissionCommand  command.RunningCommand
	containmentIntent *launch.ContainmentIntent
}

func (j *activeJob) requestTerminal(state engine.JobState) {
	j.mu.Lock()
	if j.terminal == "" {
		j.terminal = state
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
	reapInterval := cfg.ReapInterval
	if reapInterval < 0 {
		return nil, errors.New("reap interval cannot be negative")
	}
	if reapInterval == 0 {
		reapInterval = engine.DefaultReapInterval
	}
	gcInterval := cfg.GCInterval
	if gcInterval < 0 {
		return nil, errors.New("gc interval cannot be negative")
	}
	if gcInterval == 0 {
		gcInterval = engine.DefaultGCInterval
	}
	reapTickInterval := cfg.ReapTickInterval
	if reapTickInterval < 0 {
		return nil, errors.New("reap tick interval cannot be negative")
	}
	if reapTickInterval == 0 {
		reapTickInterval = reapInterval
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
		stateRoot:              root,
		cwd:                    cwd,
		socketPath:             socketPath,
		tokenPath:              tokenPath,
		token:                  token,
		backends:               backends,
		registry:               registry,
		clock:                  clock,
		processes:              processes,
		processGroups:          cfg.ProcessGroups,
		cancelGrace:            cfg.CancelGrace,
		cancelWaiter:           cfg.CancelWaiter,
		idleTimeout:            idleTimeout,
		idleCheckInterval:      idleCheck,
		binaryIdentityProbe:    binaryIdentityProbe,
		inlineResultCap:        inlineResultCap,
		leaseDuration:          leaseDuration,
		heartbeatInterval:      heartbeatInterval,
		reapInterval:           reapInterval,
		gcInterval:             gcInterval,
		reapTickInterval:       reapTickInterval,
		reapTickFactory:        newTickerSource,
		readyHook:              cfg.ReadyHook,
		safetyLatch:            NewSafetyLatch(),
		safetyDrainTimeout:     defaultSafetyDrain,
		stores:                 make(map[string]*engine.Store),
		storesByKey:            make(map[string]*engine.Store),
		jobStores:              make(map[string]*engine.Store),
		admissionJobs:          make(map[string]struct{}),
		admissionEffectMu:      make(map[string]*sync.Mutex),
		admissionRuntimeConfig: cfg.Runtime,
		admissionProbeRunner:   probeRunner,
		activeJobs:             make(map[string]*activeJob),
		lastActivity:           clock.Now().UTC(),
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
	defer func() {
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
	if s.readyHook != nil {
		if err := s.readyHook(ServeReadyInfo{StateRoot: s.stateRoot, SocketPath: s.socketPath}); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
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
		if err := os.Remove(s.socketPath); err != nil {
			return nil, socketFileIdentity{}, err
		}
	} else if !os.IsNotExist(err) {
		return nil, socketFileIdentity{}, err
	}
	if s.beforeListenBindHook != nil {
		s.beforeListenBindHook()
	}
	ln, err := net.Listen("unix", s.socketPath)
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
	identity, err := statSocketFileIdentity(s.socketPath)
	if err != nil {
		_ = ln.Close()
		return nil, socketFileIdentity{}, fmt.Errorf("stat daemon socket %q: %w", s.socketPath, err)
	}
	if err := os.Chmod(s.socketPath, 0o600); err != nil {
		_ = ln.Close()
		s.removeOwnedSocket(identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	return ln, identity, nil
}

func (s *Server) idleLoop(ctx context.Context, cancel context.CancelFunc, ln net.Listener, socketIdentity socketFileIdentity, acceptSettled <-chan struct{}) {
	ticker := time.NewTicker(s.idleCheckInterval)
	defer ticker.Stop()
	reapTicker := s.reapTickFactory(s.reapTickInterval)
	defer reapTicker.stop()
	staleDraining := false
	drainLogged := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-reapTicker.c:
			err := s.reapKnownStores()
			if s.afterReapTickHook != nil {
				s.afterReapTickHook(err)
			}
			if err != nil {
				log.Printf("agentbus daemon: periodic reap: %v", err)
			}
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
	info, err := os.Stat(path)
	if err != nil {
		return socketFileIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return socketFileIdentity{}, fmt.Errorf("unexpected socket stat type %T", info.Sys())
	}
	return socketFileIdentity{dev: uint64(stat.Dev), ino: uint64(stat.Ino)}, nil
}

func (s *Server) removeOwnedSocket(owned socketFileIdentity, phase string) {
	actual, err := statSocketFileIdentity(s.socketPath)
	if err != nil {
		log.Printf("agentbus daemon: skipping socket removal during %s: cannot stat %s (%v); a replacement daemon may own the path", phase, s.socketPath, err)
		return
	}
	if actual != owned {
		log.Printf("agentbus daemon: skipping socket removal during %s: replacement daemon owns %s", phase, s.socketPath)
		return
	}
	// dev+inode alone cannot prove ownership: tmpfs (Linux /tmp) reuses inodes
	// immediately, so a replacement daemon's socket can inherit the dead
	// socket's exact identity. Removal only ever targets a socket whose own
	// listener is already closed, so a successful dial proves a LIVE listener —
	// necessarily a replacement — and skipping is always fail-safe (daemon
	// startup dials and clears genuinely stale files itself).
	if conn, dialErr := net.DialTimeout("unix", s.socketPath, 250*time.Millisecond); dialErr == nil {
		_ = conn.Close()
		log.Printf("agentbus daemon: skipping socket removal during %s: %s accepts connections; a replacement daemon owns it", phase, s.socketPath)
		return
	}
	if err := os.Remove(s.socketPath); err != nil {
		log.Printf("agentbus daemon: remove owned socket during %s: %v", phase, err)
		return
	}
	log.Printf("agentbus daemon: removed owned socket during %s", phase)
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
		instance.policy.AcceptIdentified &&
		instance.policy.CrashDurableContainment {
		capabilities[protocol.CapabilityAdmissionStrictContainment] = true
	}
	s.admissionStateMu.RUnlock()
	return capabilities
}

func (s *Server) backendMetadata() []protocol.BackendInfo {
	names := s.backendNames()
	result := make([]protocol.BackendInfo, 0, len(names))
	for _, name := range names {
		info := protocol.BackendInfo{Backend: name, Models: []string{}, Efforts: []string{}}
		if provider, ok := s.backends[name].(engine.BackendMetadataProvider); ok {
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
	if record, ok, errObj := s.jsonJobRecord(jobID); ok || errObj != nil {
		if errObj != nil {
			return requestOutcome{err: errObj}
		}
		// During authority degradation, a fenced job with stale JSON can read the
		// stale JSON fallback. S4E SafetyLatch handles that fault-state case.
		return requestOutcome{result: protocol.JobStatusResult{Jobs: []protocol.JobStatus{statusFromRecord(*record)}}}
	}
	if authorityErr != nil {
		return requestOutcome{err: authorityErr}
	}
	return requestOutcome{result: protocol.JobStatusResult{Jobs: []protocol.JobStatus{}}}
}

func (s *Server) handleExactJobResult(jobID string) requestOutcome {
	result, ok, authorityErr := s.authorityResult(jobID)
	if ok {
		return requestOutcome{result: result}
	}
	if record, ok, errObj := s.jsonJobRecord(jobID); ok || errObj != nil {
		if errObj != nil {
			return requestOutcome{err: errObj}
		}
		// During authority degradation, a fenced job with stale JSON can read the
		// stale JSON fallback. S4E SafetyLatch handles that fault-state case.
		return requestOutcome{result: resultFromRecord(*record)}
	}
	if authorityErr != nil {
		return requestOutcome{err: authorityErr}
	}
	return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: jobID})}
}

func (s *Server) jsonJobRecord(jobID string) (*engine.JobRecord, bool, *protocol.ErrorObject) {
	store := s.storeForJob(jobID)
	if store == nil {
		return nil, false, nil
	}
	record, err := store.Load(jobID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})
	}
	if record == nil {
		return nil, false, nil
	}
	return record, true, nil
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
		return requestOutcome{err: authorityMutationIndeterminateError("job.cancel", protocol.ErrorData{JobID: params.JobID}, authorityErr)}
	} else if ok {
		return s.withAdmissionJobEffect(params.JobID, func() requestOutcome {
			return s.handleAuthorityJobCancelLocked(params.JobID)
		})
	}
	return s.handleJobCancelLocked(params.JobID)
}

func authorityMutationIndeterminateError(operation string, data protocol.ErrorData, authorityErr *protocol.ErrorObject) *protocol.ErrorObject {
	message := fmt.Sprintf("%s refused because admission authority ownership is indeterminate", operation)
	if authorityErr != nil && authorityErr.Message != "" {
		message = fmt.Sprintf("%s: %s", message, authorityErr.Message)
	}
	return protocol.NewError(protocol.ErrorBackendUnavailable, message, data)
}

func (s *Server) handleAuthorityJobCancelLocked(jobID string) requestOutcome {
	active := s.lookupActiveJob(jobID)
	if active != nil {
		active.requestTerminal(engine.StateCanceled)
		// Admission cancel is intentional containment. Mark the active launch
		// before coordinator containment so a killed process is the cancel
		// terminal path, not an unprovable safety event.
		interruptCtx, interruptCancel := context.WithTimeout(context.Background(), admissionDetachedCleanupTimeout)
		err := active.interruptAdmissionCommand(interruptCtx)
		interruptCancel()
		if err != nil {
			return requestOutcome{err: admissionProtocolError(err)}
		}
	}
	record, projection, ok, errObj := s.authorityJobProjection(jobID)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	if !ok {
		return s.handleJobCancelLocked(jobID)
	}
	if record.Terminal == nil {
		err := s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
			return coord.Cancel(context.Background(), model.JobID(jobID), nil)
		})
		if err != nil {
			return requestOutcome{err: admissionProtocolError(err)}
		}
		var reloadErr *protocol.ErrorObject
		_, projection, ok, reloadErr = s.authorityJobProjection(jobID)
		if reloadErr != nil {
			return requestOutcome{err: reloadErr}
		}
		if !ok {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: jobID})}
		}
	}
	return requestOutcome{result: protocol.JobCancelResult{JobID: projection.JobID.String(), State: admissionState(projection.Public)}}
}

func (s *Server) handleJobCancelLocked(jobID string) requestOutcome {
	active := s.lookupActiveJob(jobID)
	if active != nil {
		active.requestTerminal(engine.StateCanceled)
	}
	store := s.storeForJob(jobID)
	if store == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: jobID})}
	}
	record, err := store.Load(jobID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	if s.isAdmissionJob(jobID) {
		err := s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
			snapshot, err := coord.Snapshot(context.Background(), model.JobID(jobID))
			if err != nil {
				return err
			}
			if snapshot.Record.Terminal == nil {
				if err := coord.Cancel(context.Background(), model.JobID(jobID), nil); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			if !errors.Is(err, authority.ErrNotReady) && !errors.Is(err, coordinator.ErrCoordinatorNotReady) {
				return requestOutcome{err: admissionProtocolError(err)}
			}
		}
	}
	record, err = store.Cancel(jobID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	if active != nil && active.cancel != nil {
		active.cancel()
	}
	return requestOutcome{result: protocol.JobCancelResult{JobID: record.JobID, State: record.State}}
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
	active                  *activeJob
	onDone                  func()
	authoritativeCompletion bool
	admissionControlled     bool
	admissionMode           model.Mode
	admissionAccepted       authority.AcceptResult
	admissionLaunch         admissionLaunchBinding
}

func (s *Server) runJob(ctx context.Context, run jobRun) {
	run.authoritativeCompletion = true
	defer s.removeActiveJob(run.jobID)
	defer func() {
		if run.onDone != nil {
			run.onDone()
		}
	}()
	defer run.active.cancel()
	if run.active.requestedTerminal() != "" {
		s.finalizeRequestedTerminal(run)
		return
	}
	attemptPrompt := applyPrologue(run.policy, run.prompt)
	text, state, err := s.runAttempt(ctx, run, attemptPrompt, run.write, model.LaunchOrdinalOne)
	if requested := run.active.requestedTerminal(); requested != "" {
		s.finalizeTerminal(run, requested, text, nil)
		return
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.finalizeTerminal(run, engine.StateTimedOut, text, skippedStampForRun(run, s.registry, engine.SkipTimeout))
			return
		}
		if errors.Is(err, context.Canceled) {
			state = engine.StateInterrupted
		}
		s.finalizeTerminal(run, state, text, skippedStampForRun(run, s.registry, skippedReasonForState(state)))
		return
	}

	validation, retryPrompt, compliantState, err := s.validateAttempt(text, run, 1, false)
	if err != nil {
		s.finalizeTerminal(run, engine.StateFailed, text, skippedStampForRun(run, s.registry, engine.SkipBackendError))
		return
	}
	if retryPrompt != "" {
		retryText, retryState, retryErr := s.runAttempt(ctx, run, retryPrompt, false, model.LaunchOrdinalTwo)
		if requested := run.active.requestedTerminal(); requested != "" {
			s.finalizeTerminal(run, requested, retryText, nil)
			return
		}
		if retryErr != nil {
			s.finalizeTerminal(run, retryState, retryText, skippedStampForRun(run, s.registry, skippedReasonForState(retryState)))
			return
		}
		retryValidation, _, retryCompliantState, err := s.validateAttempt(retryText, run, 2, true)
		if err != nil {
			s.finalizeTerminal(run, engine.StateFailed, retryText, skippedStampForRun(run, s.registry, engine.SkipBackendError))
			return
		}
		s.finalizeTerminal(run, retryCompliantState, retryText, retryValidation.Stamp)
		return
	}
	s.finalizeTerminal(run, compliantState, text, validation.Stamp)
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
		LogPaths: engine.LogPaths{},
	}
	events, err := s.admissionTurnEvents(attemptCtx, run, input, ordinal)
	if err != nil {
		return "", engine.StateFailed, err
	}
	var assistantText strings.Builder
	var resultText string
	hasResultMessage := false
	for {
		select {
		case <-attemptCtx.Done():
			if shouldInterruptSessionOnAttemptCancel(run, attemptCtx.Err()) {
				_ = run.session.Interrupt(context.Background())
			}
			return attemptFinalText(hasResultMessage, resultText, assistantText.String()), stateForContext(attemptCtx.Err()), attemptCtx.Err()
		case event, ok := <-events:
			if !ok {
				if attemptCtx.Err() != nil {
					return attemptFinalText(hasResultMessage, resultText, assistantText.String()), stateForContext(attemptCtx.Err()), attemptCtx.Err()
				}
				return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateCompleted, nil
			}
			rawText := authoritativeText(event)
			switch event.Type {
			case engine.EventAgentText:
				assistantText.WriteString(rawText)
			case engine.EventResultMessage:
				resultText = rawText
				hasResultMessage = true
			case engine.EventTerminalError:
				if rawText == "" {
					rawText = "backend failed"
				}
				return attemptFinalText(hasResultMessage, resultText, assistantText.String()), engine.StateFailed, errors.New(rawText)
			}
		}
	}
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
		normalized := strings.ToLower(retry.Template)
		if !strings.Contains(normalized, "emit the corrected report only") ||
			!strings.Contains(normalized, "make no further changes") {
			return errors.New("retry.template must instruct the backend to emit the corrected report only and make no further changes when retry.max is 1")
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
	_ = s.finalizeTerminal(run, engine.StateFailed, "", nil)
}

func (s *Server) finalizeRequestedTerminal(run jobRun) {
	state := run.active.requestedTerminal()
	if state == "" {
		state = engine.StateCanceled
	}
	_ = s.finalizeTerminal(run, state, "", nil)
}

func (s *Server) finalizeTerminal(run jobRun, state engine.JobState, text string, stamp *engine.ContractStamp) error {
	if run.admissionControlled {
		return s.completeAdmissionRun(run, state, text)
	}
	backendSessionID := ""
	if run.session != nil {
		backendSessionID = run.session.ID()
	}
	_, err := run.store.Update(run.jobID, func(record *engine.JobRecord) (bool, error) {
		salvageReaped := run.authoritativeCompletion && record.State == engine.StateReaped && (state == engine.StateCompleted || state == engine.StateCompletedNoncompliant)
		if engine.IsTerminal(record.State) && !salvageReaped {
			return false, nil
		}
		lateFinalization := run.authoritativeCompletion && record.State == engine.StateOrphaned && (state == engine.StateCompleted || state == engine.StateCompletedNoncompliant || state == engine.StateFailed) || salvageReaped
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
		if salvageReaped {
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
		_, _ = run.store.Cancel(run.jobID)
	default:
		_ = s.finalizeTerminal(run, state, "", nil)
	}
	s.removeActiveJob(run.jobID)
	if run.onDone != nil {
		run.onDone()
	}
}

func (s *Server) createQueuedRecord(store *engine.Store, jobID, sessionID, backend string, tags map[string]string, policy *engine.TurnPolicy, resolved *engine.ContractSpec, foreground bool) error {
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
		Foreground:       foreground,
		State:            engine.StateQueued,
		Tags:             cloneTags(tags),
		StartedAt:        now,
		UpdatedAt:        now,
		HeartbeatAt:      now,
		Lease:            engine.Lease{ExpiresAt: now.Add(s.leaseDuration)},
		Supervisor:       ref,
		LogPaths:         logPaths,
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
		ReapInterval:  s.reapInterval,
		GCInterval:    s.gcInterval,
	})
	if err != nil {
		return nil, err
	}
	return s.rememberStoreLocked(store), nil
}

func (s *Server) storeForJob(jobID string) *engine.Store {
	s.mu.Lock()
	if store := s.jobStores[jobID]; store != nil {
		s.mu.Unlock()
		return store
	}
	s.mu.Unlock()

	stores, err := s.knownStores()
	if err != nil {
		return nil
	}
	if len(stores) == 0 {
		s.mu.Lock()
		store, err := s.storeForCWDLocked(s.cwd)
		s.mu.Unlock()
		if err != nil {
			return nil
		}
		stores = []*engine.Store{store}
	}
	for _, store := range stores {
		ok, err := store.HasJob(jobID)
		if err != nil {
			return store
		}
		if !ok {
			continue
		}
		s.mu.Lock()
		if existing := s.jobStores[jobID]; existing != nil {
			s.mu.Unlock()
			return existing
		}
		s.jobStores[jobID] = store
		s.mu.Unlock()
		return store
	}
	return nil
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

func (s *Server) listKnownRecords() []engine.JobRecord {
	stores, err := s.knownStores()
	if err != nil {
		return nil
	}
	var records []engine.JobRecord
	for _, store := range stores {
		list, err := store.List()
		if err == nil {
			records = append(records, list...)
		}
	}
	return records
}

func (s *Server) listJobStatuses() ([]protocol.JobStatus, *protocol.ErrorObject) {
	statusesByID := make(map[string]protocol.JobStatus)
	authorityStatuses, errObj := s.listAuthorityStatuses()
	if errObj != nil {
		return nil, errObj
	}
	for _, status := range authorityStatuses {
		statusesByID[status.JobID] = status
	}
	for _, record := range s.listKnownRecords() {
		status := statusFromRecord(record)
		if existing, ok := statusesByID[status.JobID]; ok {
			existing.Warnings = appendStatusWarning(existing.Warnings, duplicateAuthorityJSONWarning)
			statusesByID[status.JobID] = existing
			continue
		}
		statusesByID[status.JobID] = status
	}
	statuses := make([]protocol.JobStatus, 0, len(statusesByID))
	for _, status := range statusesByID {
		statuses = append(statuses, status)
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].JobID < statuses[j].JobID })
	return statuses, nil
}

func (s *Server) reapKnownStores() error {
	stores, err := s.knownStores()
	if err != nil {
		return err
	}
	for _, store := range stores {
		if err := store.Reap(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) knownStores() ([]*engine.Store, error) {
	discovered, err := engine.OpenWorkspaceStores(s.storeConfig())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, store := range discovered {
		s.rememberStoreLocked(store)
	}
	return s.storeSnapshotLocked(), nil
}

func (s *Server) storeConfig() engine.StoreConfig {
	return engine.StoreConfig{
		Root:          s.stateRoot,
		Clock:         s.clock,
		Processes:     s.processes,
		ProcessGroups: s.processGroups,
		CancelGrace:   s.cancelGrace,
		CancelWaiter:  s.cancelWaiter,
		LeaseDuration: s.leaseDuration,
		ReapInterval:  s.reapInterval,
		GCInterval:    s.gcInterval,
	}
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

func (s *Server) storeSnapshotLocked() []*engine.Store {
	stores := make([]*engine.Store, 0, len(s.storesByKey))
	for _, store := range s.storesByKey {
		stores = append(stores, store)
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].Layout().Key < stores[j].Layout().Key
	})
	return stores
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
	names := make([]string, 0, len(s.backends))
	for name := range s.backends {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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
	paths := engine.LogPaths{
		Stdout: filepath.Join(layout.Logs, jobID+".stdout.log"),
		Stderr: filepath.Join(layout.Logs, jobID+".stderr.log"),
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

func backendError(err error) *protocol.ErrorObject {
	if err == nil {
		return nil
	}
	return protocol.NewError(protocol.ErrorBackendUnavailable, err.Error(), protocol.ErrorData{})
}

func timeoutFromMillis(ms *int64) (time.Duration, *protocol.ErrorObject) {
	if ms == nil {
		return protocol.DefaultTimeout, nil
	}
	if *ms < 0 {
		return 0, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs cannot be negative", protocol.ErrorData{})
	}
	if *ms == 0 {
		return 0, nil
	}
	d := time.Duration(*ms) * time.Millisecond
	if d > protocol.MaxTimeout {
		return 0, protocol.NewError(protocol.ErrorInvalidTaskSpec, "timeoutMs exceeds maximum", protocol.ErrorData{})
	}
	return d, nil
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

func statusFromRecord(record engine.JobRecord) protocol.JobStatus {
	return protocol.JobStatus{
		JobID:                 record.JobID,
		SessionID:             record.SessionID,
		Backend:               record.Backend,
		State:                 record.State,
		LateFinalization:      record.LateFinalization,
		Tags:                  cloneTags(record.Tags),
		StartedAt:             timePtr(record.StartedAt),
		UpdatedAt:             timePtr(record.UpdatedAt),
		HeartbeatAt:           timePtr(record.HeartbeatAt),
		Lease:                 leasePtr(record.Lease),
		WorkerPID:             record.Worker.PID,
		WorkerStartTime:       record.Worker.StartTime,
		BackendChildPID:       record.BackendChildPID,
		BackendChildStartTime: record.BackendChildStartTime,
		StatePath:             record.StatePath,
		LogPaths:              record.LogPaths,
		ModelReported:         record.ModelReported,
		Warnings:              append([]string(nil), record.Warnings...),
	}
}

func appendStatusWarning(warnings []string, warning string) []string {
	for _, existing := range warnings {
		if existing == warning {
			return warnings
		}
	}
	return append(warnings, warning)
}

func resultFromRecord(record engine.JobRecord) protocol.JobResult {
	return protocol.JobResult{
		JobID:            record.JobID,
		SessionID:        record.SessionID,
		State:            record.State,
		LateFinalization: record.LateFinalization,
		Result:           record.Result,
		ModelReported:    record.ModelReported,
		Contract:         record.Contract,
	}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func leasePtr(lease engine.Lease) *engine.Lease {
	if lease.ExpiresAt.IsZero() {
		return nil
	}
	return &lease
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
