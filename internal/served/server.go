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
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	defaultLeaseDuration = time.Minute
	defaultHeartbeat     = 15 * time.Second
)

// Config configures the local JSON-RPC daemon.
type Config struct {
	StateRoot         string
	CWD               string
	SocketPath        string
	Token             string
	Backends          []engine.Backend
	Registry          *engine.PolicyRegistry
	Clock             engine.Clock
	ProcessTable      engine.ProcessTable
	ProcessGroups     engine.ProcessGroupSignaler
	CancelGrace       time.Duration
	CancelWaiter      engine.Waiter
	IdleTimeout       time.Duration
	IdleCheckInterval time.Duration
	InlineResultCap   int
}

// Server serves the protocol v1 socket API over engine backends.
type Server struct {
	stateRoot         string
	cwd               string
	socketPath        string
	tokenPath         string
	token             string
	backends          map[string]engine.Backend
	registry          *engine.PolicyRegistry
	clock             engine.Clock
	processes         engine.ProcessTable
	processGroups     engine.ProcessGroupSignaler
	cancelGrace       time.Duration
	cancelWaiter      engine.Waiter
	id                atomic.Uint64
	clients           atomic.Int64
	idleTimeout       time.Duration
	idleCheckInterval time.Duration
	inlineResultCap   int

	mu           sync.Mutex
	sessions     map[string]*sessionState
	stores       map[string]*engine.Store
	jobStores    map[string]*engine.Store
	activeJobs   map[string]*activeJob
	lastActivity time.Time
}

type sessionState struct {
	id           string
	backend      string
	cwd          string
	writeDefault bool
	model        string
	effort       string
	tags         map[string]string
	session      engine.Session
	activeTurnID string
}

type activeJob struct {
	jobID      string
	sessionID  string
	foreground bool
	session    engine.Session
	cancel     context.CancelFunc

	mu       sync.Mutex
	terminal engine.JobState
}

func (j *activeJob) requestTerminal(state engine.JobState) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.terminal == "" {
		j.terminal = state
	}
}

func (j *activeJob) requestedTerminal() engine.JobState {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.terminal
}

type requestOutcome struct {
	result any
	err    *protocol.ErrorObject
	after  func()
}

type resolvedPolicy struct {
	policy   *engine.TurnPolicy
	contract *engine.ContractSpec
	name     string
	hash     string
}

// New creates a daemon server and ensures state root and token file exist.
func New(cfg Config) (*Server, error) {
	root := cfg.StateRoot
	var err error
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
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
	return &Server{
		stateRoot:         root,
		cwd:               cwd,
		socketPath:        socketPath,
		tokenPath:         tokenPath,
		token:             token,
		backends:          backends,
		registry:          registry,
		clock:             clock,
		processes:         processes,
		processGroups:     cfg.ProcessGroups,
		cancelGrace:       cfg.CancelGrace,
		cancelWaiter:      cfg.CancelWaiter,
		idleTimeout:       idleTimeout,
		idleCheckInterval: idleCheck,
		inlineResultCap:   cfg.InlineResultCap,
		sessions:          make(map[string]*sessionState),
		stores:            make(map[string]*engine.Store),
		jobStores:         make(map[string]*engine.Store),
		activeJobs:        make(map[string]*activeJob),
		lastActivity:      clock.Now().UTC(),
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
	if err := s.reapKnownStores(); err != nil {
		return err
	}
	ln, err := s.listen()
	if err != nil {
		return err
	}
	defer func() {
		_ = ln.Close()
		_ = os.Remove(s.socketPath)
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	go s.idleLoop(ctx, cancel, ln)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.clients.Add(1)
		s.touchActivity()
		c := &connection{server: s, conn: conn}
		go func() {
			defer s.clients.Add(-1)
			defer s.touchActivity()
			c.serve(ctx)
		}()
	}
}

func (s *Server) listen() (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Dir(s.socketPath), 0o700); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(s.socketPath); err == nil {
		if conn, dialErr := net.DialTimeout("unix", s.socketPath, 100*time.Millisecond); dialErr == nil {
			_ = conn.Close()
			return nil, fmt.Errorf("agentbus daemon already listening at %s", s.socketPath)
		}
		if err := os.Remove(s.socketPath); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(s.socketPath, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

func (s *Server) idleLoop(ctx context.Context, cancel context.CancelFunc, ln net.Listener) {
	if s.idleTimeout < 0 {
		return
	}
	ticker := time.NewTicker(s.idleCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.clients.Load() != 0 || s.activeWork() {
				s.touchActivity()
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

func (s *Server) activeWork() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeJobs) > 0
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
			_ = c.writeResponse(resp)
			if out.after != nil && out.err == nil {
				out.after()
			}
		}
	}
}

func requiresRequestID(method string) bool {
	switch method {
	case protocol.MethodHello,
		protocol.MethodSessionStart,
		protocol.MethodSessionResume,
		protocol.MethodSessionList,
		protocol.MethodTurnStart,
		protocol.MethodTurnInterrupt,
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

func (c *connection) notify(method string, params any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeFrame(c.conn, protocol.Notification{JSONRPC: "2.0", Method: method, Params: params})
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
	switch req.Method {
	case protocol.MethodHello:
		return s.handleHello(c, req.Params)
	case protocol.MethodSessionStart:
		return s.handleSessionStart(ctx, req.Params)
	case protocol.MethodSessionResume:
		return s.handleSessionResume(ctx, req.Params)
	case protocol.MethodSessionList:
		return s.handleSessionList(req.Params)
	case protocol.MethodTurnStart:
		return s.handleTurnStart(ctx, c, req.Params)
	case protocol.MethodTurnInterrupt:
		return s.handleTurnInterrupt(req.Params)
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
		return requestOutcome{err: protocol.NewError(protocol.ErrorCapabilityMissing, "unknown method", protocol.ErrorData{})}
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
		Capabilities:    protocol.DefaultCapabilities(),
	}}
}

func (s *Server) handleSessionStart(ctx context.Context, raw json.RawMessage) requestOutcome {
	var params protocol.SessionStartParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.Backend == "" || params.CWD == "" || !filepath.IsAbs(params.CWD) {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "session.start requires backend and absolute cwd", protocol.ErrorData{})}
	}
	backend, ok := s.backends[params.Backend]
	if !ok {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "backend is unavailable", protocol.ErrorData{})}
	}
	session, err := backend.Start(ctx, engine.SessionOpts{CWD: params.CWD, Write: params.Write, Model: params.Model, Effort: params.Effort, Timeout: protocol.DefaultTimeout})
	if err != nil {
		return requestOutcome{err: backendError(err)}
	}
	sessionID := session.ID()
	if sessionID == "" {
		sessionID = s.nextID("ses")
	}
	state := &sessionState{
		id:           sessionID,
		backend:      params.Backend,
		cwd:          params.CWD,
		writeDefault: params.Write,
		model:        params.Model,
		effort:       params.Effort,
		tags:         cloneTags(params.Tags),
		session:      session,
	}
	s.mu.Lock()
	s.sessions[sessionID] = state
	s.mu.Unlock()
	s.touchActivity()
	return requestOutcome{result: protocol.SessionStartResult{SessionID: sessionID, Backend: params.Backend}}
}

func (s *Server) handleSessionResume(ctx context.Context, raw json.RawMessage) requestOutcome {
	var params protocol.SessionResumeParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.SessionID == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "sessionId is required", protocol.ErrorData{})}
	}
	s.mu.Lock()
	existing := s.sessions[params.SessionID]
	s.mu.Unlock()
	if existing != nil {
		return requestOutcome{result: protocol.SessionStartResult{SessionID: existing.id, Backend: existing.backend}}
	}
	return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "session is not known to this daemon", protocol.ErrorData{SessionID: params.SessionID})}
}

func (s *Server) handleSessionList(raw json.RawMessage) requestOutcome {
	var params protocol.SessionListParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	s.mu.Lock()
	sessions := make([]protocol.SessionInfo, 0, len(s.sessions))
	for _, state := range s.sessions {
		if !tagsMatch(state.tags, params.Tags) {
			continue
		}
		var active *string
		if state.activeTurnID != "" {
			id := state.activeTurnID
			active = &id
		}
		sessions = append(sessions, protocol.SessionInfo{
			SessionID:    state.id,
			Backend:      state.backend,
			CWD:          state.cwd,
			Write:        state.writeDefault,
			Tags:         cloneTags(state.tags),
			ActiveTurnID: active,
		})
	}
	s.mu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].SessionID < sessions[j].SessionID })
	return requestOutcome{result: protocol.SessionListResult{Sessions: sessions}}
}

func (s *Server) handleTurnStart(ctx context.Context, c *connection, raw json.RawMessage) requestOutcome {
	var params protocol.TurnStartParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.SessionID == "" || params.Prompt == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "turn.start requires sessionId and prompt", protocol.ErrorData{SessionID: params.SessionID})}
	}
	timeout, errObj := timeoutFromMillis(params.TimeoutMs)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	policy, err := s.resolvePolicy(params.Policy)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{SessionID: params.SessionID})}
	}
	s.mu.Lock()
	session := s.sessions[params.SessionID]
	if session == nil {
		s.mu.Unlock()
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "session is not known", protocol.ErrorData{SessionID: params.SessionID})}
	}
	if session.activeTurnID != "" {
		active := session.activeTurnID
		s.mu.Unlock()
		return requestOutcome{err: protocol.NewError(protocol.ErrorSessionBusy, "session already has an active turn", protocol.ErrorData{SessionID: params.SessionID, TurnID: active, JobID: active})}
	}
	write := session.writeDefault
	if params.Write != nil {
		write = *params.Write
	}
	jobID := s.nextID("job")
	session.activeTurnID = jobID
	store, err := s.storeForCWDLocked(session.cwd)
	if err != nil {
		session.activeTurnID = ""
		s.mu.Unlock()
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{SessionID: params.SessionID})}
	}
	s.jobStores[jobID] = store
	s.mu.Unlock()
	if err := s.createQueuedRecord(store, jobID, session.id, session.backend, session.tags, policy.policy, policy.contract, true); err != nil {
		s.clearActiveTurn(session.id, jobID)
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{SessionID: params.SessionID, JobID: jobID, TurnID: jobID})}
	}
	runCtx, cancel := context.WithCancel(ctx)
	active := &activeJob{jobID: jobID, sessionID: session.id, foreground: true, session: session.session, cancel: cancel}
	s.addActiveJob(active)
	run := jobRun{
		jobID:        jobID,
		sessionID:    session.id,
		backend:      session.backend,
		store:        store,
		session:      session.session,
		prompt:       params.Prompt,
		write:        write,
		policy:       policy.policy,
		contract:     policy.contract,
		contractName: policy.name,
		contractHash: policy.hash,
		timeout:      timeout,
		foreground:   true,
		conn:         c,
		active:       active,
		onDone: func() {
			s.clearActiveTurn(session.id, jobID)
		},
	}
	return requestOutcome{
		result: protocol.TurnStartResult{TurnID: jobID, JobID: jobID, SessionID: session.id},
		after:  func() { go s.runJob(runCtx, run) },
	}
}

func (s *Server) handleTurnInterrupt(raw json.RawMessage) requestOutcome {
	var params protocol.TurnInterruptParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.TurnID == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "turnId is required", protocol.ErrorData{})}
	}
	active := s.lookupActiveJob(params.TurnID)
	if active != nil {
		if !active.foreground {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "turn.interrupt only applies to foreground turns", protocol.ErrorData{JobID: params.TurnID, TurnID: params.TurnID})}
		}
		active.requestTerminal(engine.StateInterrupted)
	}
	store := s.storeForJob(params.TurnID)
	if store == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "turn is not known", protocol.ErrorData{JobID: params.TurnID, TurnID: params.TurnID})}
	}
	if active == nil {
		record, err := store.Load(params.TurnID)
		if err != nil {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.TurnID, TurnID: params.TurnID})}
		}
		if !record.Foreground {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "turn.interrupt only applies to foreground turns", protocol.ErrorData{JobID: params.TurnID, TurnID: params.TurnID})}
		}
	}
	record, err := store.Interrupt(params.TurnID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.TurnID, TurnID: params.TurnID})}
	}
	if active != nil && active.cancel != nil {
		active.cancel()
	}
	return requestOutcome{result: protocol.TurnInterruptResult{TurnID: params.TurnID, JobID: params.TurnID, State: record.State}}
}

func (s *Server) handleJobSubmit(ctx context.Context, raw json.RawMessage) requestOutcome {
	if errObj := validateTaskSpecEnvelope(raw); errObj != nil {
		return requestOutcome{err: errObj}
	}
	var params protocol.JobSubmitParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	spec := params.TaskSpec
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec requires backend, absolute cwd, write, and prompt", protocol.ErrorData{})}
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMs)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	policy, err := s.resolvePolicy(spec.Policy)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	backend, ok := s.backends[spec.Backend]
	if !ok {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "backend is unavailable", protocol.ErrorData{})}
	}
	session, err := backend.Start(ctx, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout})
	if err != nil {
		return requestOutcome{err: backendError(err)}
	}
	sessionID := session.ID()
	if sessionID == "" {
		sessionID = s.nextID("ses")
	}
	s.mu.Lock()
	store, err := s.storeForCWDLocked(spec.CWD)
	if err != nil {
		s.mu.Unlock()
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	jobID := s.nextID("job")
	s.jobStores[jobID] = store
	s.mu.Unlock()
	if err := s.createQueuedRecord(store, jobID, sessionID, spec.Backend, spec.Tags, policy.policy, policy.contract, false); err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	runCtx, cancel := context.WithCancel(ctx)
	active := &activeJob{jobID: jobID, sessionID: sessionID, session: session, cancel: cancel}
	s.addActiveJob(active)
	run := jobRun{
		jobID:        jobID,
		sessionID:    sessionID,
		backend:      spec.Backend,
		store:        store,
		session:      session,
		prompt:       spec.Prompt,
		write:        spec.Write,
		policy:       policy.policy,
		contract:     policy.contract,
		contractName: policy.name,
		contractHash: policy.hash,
		timeout:      timeout,
		active:       active,
	}
	return requestOutcome{
		result: protocol.JobSubmitResult{JobID: jobID, State: engine.StateQueued},
		after:  func() { go s.runJob(runCtx, run) },
	}
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
		store := s.storeForJob(params.JobID)
		if store == nil {
			return requestOutcome{result: protocol.JobStatusResult{Jobs: []protocol.JobStatus{}}}
		}
		record, err := store.Load(params.JobID)
		if err != nil {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.JobID})}
		}
		return requestOutcome{result: protocol.JobStatusResult{Jobs: []protocol.JobStatus{statusFromRecord(*record)}}}
	}
	records := s.listKnownRecords()
	statuses := make([]protocol.JobStatus, 0, len(records))
	for _, record := range records {
		statuses = append(statuses, statusFromRecord(record))
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].JobID < statuses[j].JobID })
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
	store := s.storeForJob(params.JobID)
	if store == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: params.JobID})}
	}
	record, err := store.Load(params.JobID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.JobID})}
	}
	return requestOutcome{result: resultFromRecord(*record)}
}

func (s *Server) handleJobCancel(raw json.RawMessage) requestOutcome {
	var params protocol.JobCancelParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.JobID == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "jobId is required", protocol.ErrorData{})}
	}
	active := s.lookupActiveJob(params.JobID)
	if active != nil {
		if active.foreground {
			return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job.cancel only applies to background jobs", protocol.ErrorData{JobID: params.JobID, TurnID: params.JobID})}
		}
		active.requestTerminal(engine.StateCanceled)
	}
	store := s.storeForJob(params.JobID)
	if store == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: params.JobID})}
	}
	record, err := store.Load(params.JobID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.JobID})}
	}
	if record.Foreground {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "job.cancel only applies to background jobs", protocol.ErrorData{JobID: params.JobID, TurnID: params.JobID})}
	}
	record, err = store.Cancel(params.JobID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: params.JobID})}
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
	jobID        string
	sessionID    string
	backend      string
	store        *engine.Store
	session      engine.Session
	prompt       string
	write        bool
	policy       *engine.TurnPolicy
	contract     *engine.ContractSpec
	contractName string
	contractHash string
	timeout      time.Duration
	foreground   bool
	conn         *connection
	active       *activeJob
	onDone       func()
}

func (s *Server) runJob(ctx context.Context, run jobRun) {
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
	if err := s.transitionRecord(run.store, run.jobID, engine.StateStarting); err != nil {
		s.finalizeFailure(run, err)
		return
	}
	if err := s.transitionRecord(run.store, run.jobID, engine.StateRunning); err != nil {
		s.finalizeFailure(run, err)
		return
	}
	heartbeatDone := make(chan struct{})
	go s.heartbeat(run.store, run.jobID, heartbeatDone)
	defer close(heartbeatDone)

	attemptPrompt := applyPrologue(run.policy, run.prompt)
	text, state, err := s.runAttempt(ctx, run, attemptPrompt, run.write)
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
		if err := s.transitionRecord(run.store, run.jobID, engine.StateRetrying); err != nil {
			s.finalizeFailure(run, err)
			return
		}
		retryText, retryState, retryErr := s.runAttempt(ctx, run, retryPrompt, false)
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

func (s *Server) runAttempt(ctx context.Context, run jobRun, prompt string, write bool) (string, engine.JobState, error) {
	attemptCtx := ctx
	var cancel context.CancelFunc
	if run.timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, run.timeout)
	} else {
		attemptCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	record, _ := run.store.Load(run.jobID)
	input := engine.TurnInput{
		Prompt:   prompt,
		Write:    write,
		Timeout:  run.timeout,
		LogPaths: engine.LogPaths{},
		OnProcessStart: func(ref engine.ProcessRef, backendChildPID int) {
			_ = s.updateBackendProcess(run.store, run.jobID, ref, backendChildPID)
		},
	}
	if record != nil {
		input.LogPaths = record.LogPaths
	}
	events, err := run.session.Turn(attemptCtx, input)
	if err != nil {
		if strings.Contains(err.Error(), protocol.ErrorSessionBusy) {
			return "", engine.StateFailed, err
		}
		return "", engine.StateFailed, err
	}
	var final strings.Builder
	sequence := 0
	for {
		select {
		case <-attemptCtx.Done():
			_ = run.session.Interrupt(context.Background())
			return final.String(), stateForContext(attemptCtx.Err()), attemptCtx.Err()
		case event, ok := <-events:
			if !ok {
				if attemptCtx.Err() != nil {
					return final.String(), stateForContext(attemptCtx.Err()), attemptCtx.Err()
				}
				return final.String(), engine.StateCompleted, nil
			}
			rawText := authoritativeText(event)
			if event.Type == engine.EventAgentText {
				final.WriteString(rawText)
			}
			if run.foreground && run.conn != nil {
				sequence++
				wireEvent := prepareWireEvent(event)
				_ = run.conn.notify(protocol.NotificationTurnEvent, protocol.TurnEventParams{
					SessionID: run.sessionID,
					TurnID:    run.jobID,
					JobID:     run.jobID,
					Sequence:  sequence,
					Event:     wireEvent,
				})
			}
		}
	}
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
	record, err := run.store.Load(run.jobID)
	if err != nil {
		return err
	}
	if engine.IsTerminal(record.State) {
		if run.foreground && run.conn != nil {
			_ = run.conn.notify(protocol.NotificationTurnResult, turnResultFromRecord(*record))
		}
		return nil
	}
	var result *engine.ResultInfo
	if state == engine.StateCompleted || state == engine.StateCompletedNoncompliant {
		if text == "" && run.policy != nil && run.policy.Contract != nil && stamp == nil {
			stamp = skippedStampForRun(run, s.registry, engine.SkipNoFinalMessage)
		}
		info, err := run.store.WriteResult(run.jobID, []byte(text), s.inlineResultCap)
		if err != nil {
			return err
		}
		result = &info
	}
	if err := transitionOrSet(record, state, s.clock.Now().UTC()); err != nil {
		return err
	}
	record.Result = result
	if stamp != nil {
		record.Contract = stamp
	}
	if run.contract != nil {
		resolved := *run.contract
		record.ResolvedContract = &resolved
	}
	if err := run.store.Save(record); err != nil {
		return err
	}
	if run.foreground && run.conn != nil {
		_ = run.conn.notify(protocol.NotificationTurnResult, turnResultFromRecord(*record))
	}
	return nil
}

func (s *Server) heartbeat(store *engine.Store, jobID string, done <-chan struct{}) {
	ticker := time.NewTicker(defaultHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			record, err := store.Load(jobID)
			if err != nil || engine.IsTerminal(record.State) {
				return
			}
			now := s.clock.Now().UTC()
			record.HeartbeatAt = now
			record.Lease = engine.Lease{ExpiresAt: now.Add(defaultLeaseDuration)}
			_ = store.Save(record)
		}
	}
}

func (s *Server) updateBackendProcess(store *engine.Store, jobID string, ref engine.ProcessRef, backendChildPID int) error {
	record, err := store.Load(jobID)
	if err != nil {
		return err
	}
	if engine.IsTerminal(record.State) {
		return nil
	}
	record.BackendChildPID = backendChildPID
	if ref.PID > 0 || ref.PGID > 0 || ref.StartTime != "" {
		record.Worker = ref
	}
	return store.Save(record)
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
		Lease:            engine.Lease{ExpiresAt: now.Add(defaultLeaseDuration)},
		Supervisor:       ref,
		LogPaths:         logPaths,
		Policy:           policy,
		ResolvedContract: resolvedCopy,
	}
	return store.Save(record)
}

func (s *Server) transitionRecord(store *engine.Store, jobID string, state engine.JobState) error {
	record, err := store.Load(jobID)
	if err != nil {
		return err
	}
	if engine.IsTerminal(record.State) {
		return nil
	}
	if err := transitionOrSet(record, state, s.clock.Now().UTC()); err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	record.HeartbeatAt = now
	record.Lease = engine.Lease{ExpiresAt: now.Add(defaultLeaseDuration)}
	return store.Save(record)
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
	store, err = engine.NewStore(engine.StoreConfig{
		Root:          s.stateRoot,
		CWD:           canon,
		Clock:         s.clock,
		Processes:     s.processes,
		ProcessGroups: s.processGroups,
		CancelGrace:   s.cancelGrace,
		CancelWaiter:  s.cancelWaiter,
	})
	if err != nil {
		return nil, err
	}
	s.stores[canon] = store
	return store, nil
}

func (s *Server) storeForJob(jobID string) *engine.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	if store := s.jobStores[jobID]; store != nil {
		return store
	}
	if store, err := s.storeForCWDLocked(s.cwd); err == nil {
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

func (s *Server) lookupActiveJob(jobID string) *activeJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeJobs[jobID]
}

func (s *Server) clearActiveTurn(sessionID, jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if session := s.sessions[sessionID]; session != nil && session.activeTurnID == jobID {
		session.activeTurnID = ""
	}
}

func (s *Server) listKnownRecords() []engine.JobRecord {
	s.mu.Lock()
	stores := make([]*engine.Store, 0, len(s.stores))
	seen := make(map[*engine.Store]struct{}, len(s.stores))
	for _, store := range s.stores {
		if _, ok := seen[store]; !ok {
			seen[store] = struct{}{}
			stores = append(stores, store)
		}
	}
	if len(stores) == 0 {
		if store, err := s.storeForCWDLocked(s.cwd); err == nil {
			stores = append(stores, store)
		}
	}
	s.mu.Unlock()
	var records []engine.JobRecord
	for _, store := range stores {
		list, err := store.List()
		if err == nil {
			records = append(records, list...)
		}
	}
	return records
}

func (s *Server) reapKnownStores() error {
	s.mu.Lock()
	store, err := s.storeForCWDLocked(s.cwd)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return store.Reap()
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
	if configured != "" {
		if err := atomicWrite(path, []byte(configured+"\n"), 0o600); err != nil {
			return "", err
		}
		return configured, nil
	}
	raw, err := os.ReadFile(path)
	if err == nil {
		if err := os.Chmod(path, 0o600); err != nil {
			return "", err
		}
		token := strings.TrimSpace(string(raw))
		if token != "" {
			return token, nil
		}
	}
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	token, err := randomToken()
	if err != nil {
		return "", err
	}
	if err := atomicWrite(path, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	return token, nil
}

func randomToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
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
	if strings.Contains(err.Error(), protocol.ErrorSessionBusy) {
		return protocol.NewError(protocol.ErrorSessionBusy, "session already has an active turn", protocol.ErrorData{})
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
		JobID:           record.JobID,
		SessionID:       record.SessionID,
		Backend:         record.Backend,
		State:           record.State,
		Tags:            cloneTags(record.Tags),
		StartedAt:       timePtr(record.StartedAt),
		UpdatedAt:       timePtr(record.UpdatedAt),
		HeartbeatAt:     timePtr(record.HeartbeatAt),
		Lease:           leasePtr(record.Lease),
		WorkerPID:       record.Worker.PID,
		BackendChildPID: record.BackendChildPID,
		StatePath:       record.StatePath,
		LogPaths:        record.LogPaths,
	}
}

func resultFromRecord(record engine.JobRecord) protocol.JobResult {
	return protocol.JobResult{
		JobID:     record.JobID,
		SessionID: record.SessionID,
		State:     record.State,
		Result:    record.Result,
		Contract:  record.Contract,
	}
}

func turnResultFromRecord(record engine.JobRecord) protocol.TurnResultParams {
	return protocol.TurnResultParams{
		SessionID: record.SessionID,
		TurnID:    record.JobID,
		JobID:     record.JobID,
		State:     record.State,
		Result:    record.Result,
		Contract:  record.Contract,
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

func prepareWireEvent(event engine.Event) engine.Event {
	wireEvent := event
	truncated := engine.TruncateEventText([]byte(wireEvent.Text), engine.DefaultEventTextCap)
	wireEvent.Text = truncated.Text
	wireEvent.Truncated = wireEvent.Truncated || truncated.Truncated
	wireEvent.RawText = ""
	wireEvent.Metadata = engine.SanitizeEventMetadata(wireEvent.Metadata)
	return wireEvent
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
