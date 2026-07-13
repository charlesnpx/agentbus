package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const defaultStartTimeout = 10 * time.Second

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
}

// DaemonStarter starts an agentbus foreground daemon process.
type DaemonStarter interface {
	StartDaemon(context.Context, StartOptions) (int, error)
}

// StartFunc adapts a function to DaemonStarter.
type StartFunc func(context.Context, StartOptions) (int, error)

func (f StartFunc) StartDaemon(ctx context.Context, opts StartOptions) (int, error) {
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
	subs    map[string]chan TurnNotification
	backlog map[string][]TurnNotification
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
		if opts.DisableAutoStart {
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
		subs:       make(map[string]chan TurnNotification),
		backlog:    make(map[string][]TurnNotification),
	}, nil
}

func (c *Client) connect(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("agentbus client is closed")
	}
	c.mu.Unlock()
	token, err := c.readToken()
	if err != nil {
		return err
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
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
		return HelloResult{}, err
	}
	line, err := readLineContext(ctx, conn, reader)
	if err != nil {
		return HelloResult{}, err
	}
	var resp protocol.Response
	if err := json.Unmarshal(bytes.TrimSpace(line), &resp); err != nil {
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
	if err := os.MkdirAll(c.stateRoot, 0o700); err != nil {
		return err
	}
	lockPath := filepath.Join(c.stateRoot, "agentbus.start.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

	if err := c.connect(ctx); err == nil {
		return nil
	}
	starter := c.opts.Starter
	if starter == nil {
		starter = defaultStarter{}
	}
	startCtx, cancel := context.WithTimeout(ctx, c.startTimeout())
	defer cancel()
	pid, err := starter.StartDaemon(startCtx, StartOptions{
		StateRoot:   c.stateRoot,
		SocketPath:  c.socketPath,
		TokenPath:   c.tokenPath,
		CommandPath: c.opts.CommandPath,
	})
	if err != nil {
		return err
	}
	if pid > 0 {
		if err := atomicWrite(filepath.Join(c.stateRoot, "agentbus.pid"), []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
			return err
		}
	}
	deadline := time.Now().Add(c.startTimeout())
	var last error
	for time.Now().Before(deadline) {
		if err := c.connect(ctx); err == nil {
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	if last == nil {
		last = errors.New("daemon did not become ready")
	}
	return last
}

func (c *Client) startTimeout() time.Duration {
	if c.opts.StartTimeout > 0 {
		return c.opts.StartTimeout
	}
	return defaultStartTimeout
}

type defaultStarter struct{}

func (defaultStarter) StartDaemon(ctx context.Context, opts StartOptions) (int, error) {
	command := opts.CommandPath
	if command == "" {
		var err error
		command, err = exec.LookPath("agentbus")
		if err != nil {
			if exe, exeErr := os.Executable(); exeErr == nil && filepath.Base(exe) == "agentbus" {
				command = exe
			} else {
				return 0, fmt.Errorf("agentbus binary not found for autostart: %w", err)
			}
		}
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	cmd := exec.Command(command, "serve", "--foreground")
	// The client often runs under a process-group-scoped tool invocation. Give
	// the daemon its own session so ending that invocation cannot terminate the
	// daemon along with its launcher.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(), "AGENTBUS_STATE_ROOT="+opts.StateRoot)
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	if err := cmd.Start(); err != nil {
		_ = devNull.Close()
		return 0, err
	}
	_ = devNull.Close()
	pid := cmd.Process.Pid
	// Reap the daemon if it ever exits. Waiting asynchronously preserves
	// autostart's non-blocking behavior while preventing a long-lived client
	// process from retaining a zombie child.
	go func() { _ = cmd.Wait() }()
	return pid, nil
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
			c.dispatchNotification(head.Method, line)
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
	for jobID, ch := range c.subs {
		delete(c.subs, jobID)
		close(ch)
	}
}

func (c *Client) dispatchNotification(method string, line []byte) {
	var env struct {
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(line), &env); err != nil {
		return
	}
	n := TurnNotification{Method: method}
	var jobID string
	switch method {
	case protocol.NotificationTurnEvent:
		var params TurnEventParams
		if err := json.Unmarshal(env.Params, &params); err != nil {
			return
		}
		jobID = params.JobID
		n.Event = &params
	case protocol.NotificationTurnResult:
		var params TurnResultParams
		if err := json.Unmarshal(env.Params, &params); err != nil {
			return
		}
		jobID = params.JobID
		n.Result = &params
	default:
		return
	}
	c.mu.Lock()
	ch := c.subs[jobID]
	if ch == nil {
		c.backlog[jobID] = append(c.backlog[jobID], n)
		if len(c.backlog[jobID]) > 128 {
			c.backlog[jobID] = c.backlog[jobID][len(c.backlog[jobID])-128:]
		}
		c.mu.Unlock()
		return
	}
	if n.Result != nil {
		delete(c.subs, jobID)
	}
	ch <- n
	if n.Result != nil {
		close(ch)
	}
	c.mu.Unlock()
}

func (c *Client) subscribe(jobID string) <-chan TurnNotification {
	ch := make(chan TurnNotification, 256)
	c.mu.Lock()
	c.subs[jobID] = ch
	backlog := c.backlog[jobID]
	delete(c.backlog, jobID)
	for _, n := range backlog {
		ch <- n
		if n.Result != nil {
			close(ch)
			delete(c.subs, jobID)
			break
		}
	}
	c.mu.Unlock()
	return ch
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
	}
	return c.autostart(ctx)
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

func (c *Client) SessionStart(ctx context.Context, params SessionStartParams) (SessionStartResult, error) {
	var out SessionStartResult
	err := c.do(ctx, protocol.MethodSessionStart, params, &out)
	return out, err
}

func (c *Client) SessionResume(ctx context.Context, params SessionResumeParams) (SessionStartResult, error) {
	var out SessionStartResult
	err := c.do(ctx, protocol.MethodSessionResume, params, &out)
	return out, err
}

func (c *Client) SessionList(ctx context.Context, params SessionListParams) (SessionListResult, error) {
	var out SessionListResult
	err := c.do(ctx, protocol.MethodSessionList, params, &out)
	return out, err
}

func (c *Client) TurnStart(ctx context.Context, params TurnStartParams) (TurnStartResult, <-chan TurnNotification, error) {
	var out TurnStartResult
	if err := c.do(ctx, protocol.MethodTurnStart, params, &out); err != nil {
		return TurnStartResult{}, nil, err
	}
	return out, c.subscribe(out.JobID), nil
}

func (c *Client) TurnInterrupt(ctx context.Context, params TurnInterruptParams) (TurnInterruptResult, error) {
	var out TurnInterruptResult
	err := c.do(ctx, protocol.MethodTurnInterrupt, params, &out)
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
