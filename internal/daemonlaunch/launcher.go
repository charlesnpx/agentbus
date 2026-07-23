package daemonlaunch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	ReadyFDEnv                = "AGENTBUS_READY_FD"
	ReadinessProtocolVersion  = 1
	DefaultTimeout            = 10 * time.Second
	DefaultStderrTailBytes    = 64 * 1024
	CodeAlreadyListening      = "agentbus daemon already listening"
	readinessFDChildNumber    = 3
	existingVerifyRetryPeriod = 50 * time.Millisecond
	failedExitGrace           = 500 * time.Millisecond
	killWaitTimeout           = 5 * time.Second
)

var (
	ErrStartupFailed              = errors.New("daemon startup failed")
	ErrReadinessEOF               = errors.New("daemon exited before readiness")
	ErrReadinessTimeout           = errors.New("daemon readiness timed out")
	ErrReadinessCanceled          = errors.New("daemon readiness canceled")
	ErrReadinessProtocol          = errors.New("daemon readiness protocol error")
	ErrCanonicalStateRootMismatch = errors.New("daemon canonical state root mismatch")
	ErrExistingDaemonVerification = errors.New("existing daemon verification failed")
)

type readyRecord struct {
	ProtocolVersion    int    `json:"protocolVersion"`
	PID                int    `json:"pid"`
	CanonicalStateRoot string `json:"canonicalStateRoot"`
	SocketPath         string `json:"socketPath"`
}

type failedRecord struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type readinessFrame struct {
	Ready  *readyRecord  `json:"ready,omitempty"`
	Failed *failedRecord `json:"failed,omitempty"`
}

// Reporter writes the child side of the readiness handshake.
type Reporter struct {
	mu   sync.Mutex
	file *os.File
	sent bool
}

// InheritedReporterFromEnv returns a reporter for AGENTBUS_READY_FD when the
// process was launched by this package.
func InheritedReporterFromEnv() (*Reporter, bool, error) {
	raw := strings.TrimSpace(os.Getenv(ReadyFDEnv))
	if raw == "" {
		return nil, false, nil
	}
	fd, err := strconv.Atoi(raw)
	if err != nil || fd < readinessFDChildNumber {
		return nil, true, fmt.Errorf("%s must name an inherited fd >= %d: %q", ReadyFDEnv, readinessFDChildNumber, raw)
	}
	return &Reporter{file: os.NewFile(uintptr(fd), "agentbus-ready")}, true, nil
}

func (reporter *Reporter) Sent() bool {
	if reporter == nil {
		return false
	}
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	return reporter.sent
}

func (reporter *Reporter) Ready(canonicalStateRoot, socketPath string) error {
	return reporter.write(readinessFrame{Ready: &readyRecord{
		ProtocolVersion:    ReadinessProtocolVersion,
		PID:                os.Getpid(),
		CanonicalStateRoot: canonicalStateRoot,
		SocketPath:         socketPath,
	}})
}

func (reporter *Reporter) Failed(code, message string) error {
	if strings.TrimSpace(code) == "" {
		code = "error"
	}
	return reporter.write(readinessFrame{Failed: &failedRecord{
		Code:    code,
		Message: message,
	}})
}

func (reporter *Reporter) Close() error {
	if reporter == nil {
		return nil
	}
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if reporter.file == nil {
		return nil
	}
	err := reporter.file.Close()
	reporter.file = nil
	return err
}

func (reporter *Reporter) write(frame readinessFrame) error {
	if reporter == nil {
		return nil
	}
	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if reporter.sent {
		return ErrReadinessProtocol
	}
	reporter.sent = true
	file := reporter.file
	reporter.file = nil
	if file == nil {
		return ErrReadinessProtocol
	}
	err := json.NewEncoder(file).Encode(frame)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

type Options struct {
	CommandPath string
	Args        []string
	Env         []string
	StateRoot   string
	SocketPath  string
	TokenPath   string
	Timeout     time.Duration
	Starter     ProcessStarter
}

type ProcessStarter func(ProcessConfig) (Process, error)

type ProcessConfig struct {
	CommandPath string
	Args        []string
	Env         []string
	ExtraFiles  []*os.File
	Stdin       *os.File
	Stdout      *os.File
	Stderr      *os.File
	Setsid      bool
}

type Process interface {
	PID() int
	Kill() error
	Wait() error
}

type Result struct {
	PID                int
	CanonicalStateRoot string
	SocketPath         string
	ExistingDaemon     bool

	handle *Handle
}

func (result Result) KillAndWait() error {
	if result.handle == nil {
		return nil
	}
	return result.handle.KillAndWait()
}

type Handle struct {
	pid     int
	process Process
	done    chan error
}

func newHandle(process Process) *Handle {
	handle := &Handle{
		pid:     process.PID(),
		process: process,
		done:    make(chan error, 1),
	}
	go func() {
		handle.done <- process.Wait()
	}()
	return handle
}

func (handle *Handle) KillAndWait() error {
	if handle == nil || handle.process == nil {
		return nil
	}
	killErr := handle.process.Kill()
	if killErr != nil && errors.Is(killErr, os.ErrProcessDone) {
		killErr = nil
	}
	waitErr := handle.waitTimeout(killWaitTimeout)
	if waitErr != nil {
		if strings.Contains(waitErr.Error(), "timed out") {
			return errors.Join(killErr, waitErr)
		}
		if killErr != nil {
			return errors.Join(killErr, waitErr)
		}
		return nil
	}
	return killErr
}

func (handle *Handle) waitGraceOrKill(grace time.Duration) error {
	if handle == nil {
		return nil
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case err := <-handle.done:
		return err
	case <-timer.C:
		return handle.KillAndWait()
	}
}

func (handle *Handle) waitTimeout(timeout time.Duration) error {
	if handle == nil {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-handle.done:
		return err
	case <-timer.C:
		return fmt.Errorf("wait for daemon process %d timed out", handle.pid)
	}
}

type StartupError struct {
	Kind                       error
	Code                       string
	Message                    string
	StderrTail                 string
	ExpectedCanonicalStateRoot string
	ActualCanonicalStateRoot   string
	Cause                      error
}

func (err *StartupError) Error() string {
	if err == nil {
		return ErrStartupFailed.Error()
	}
	kind := ErrStartupFailed
	if err.Kind != nil {
		kind = err.Kind
	}
	var b strings.Builder
	b.WriteString(kind.Error())
	if err.Code != "" {
		b.WriteString(": ")
		b.WriteString(err.Code)
	}
	if err.Message != "" {
		if err.Code == "" {
			b.WriteString(": ")
		} else {
			b.WriteString(": ")
		}
		b.WriteString(err.Message)
	}
	if err.ExpectedCanonicalStateRoot != "" || err.ActualCanonicalStateRoot != "" {
		fmt.Fprintf(&b, ": expected canonicalStateRoot %q received %q", err.ExpectedCanonicalStateRoot, err.ActualCanonicalStateRoot)
	}
	if err.Cause != nil {
		b.WriteString(": ")
		b.WriteString(err.Cause.Error())
	}
	if strings.TrimSpace(err.StderrTail) != "" {
		b.WriteString("\nstderr tail:\n")
		b.WriteString(strings.TrimRight(err.StderrTail, "\n"))
	}
	return b.String()
}

func (err *StartupError) Is(target error) bool {
	return err != nil && err.Kind != nil && errors.Is(err.Kind, target)
}

func (err *StartupError) Unwrap() error {
	if err == nil {
		return nil
	}
	if err.Kind == nil {
		return err.Cause
	}
	if err.Cause == nil {
		return err.Kind
	}
	return errors.Join(err.Kind, err.Cause)
}

func Launch(ctx context.Context, opts Options) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	launchCtx, cancelLaunch := context.WithTimeout(ctx, timeout)
	defer cancelLaunch()
	root, canonicalRoot, err := prepareStateRoot(opts.StateRoot)
	if err != nil {
		return Result{}, err
	}
	socketPath := opts.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(root, protocol.SocketName)
	}
	tokenPath := opts.TokenPath
	if tokenPath == "" {
		tokenPath = filepath.Join(root, protocol.TokenFileName)
	}
	args := opts.Args
	if len(args) == 0 {
		args = []string{"serve", "--foreground"}
	}
	if strings.TrimSpace(opts.CommandPath) == "" {
		return Result{}, errors.New("daemon command path is required")
	}
	if opts.Starter == nil {
		return Result{}, errors.New("daemon process starter is required")
	}

	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		return Result{}, err
	}
	defer readyRead.Close()

	stderrCapture, err := newStderrCapture(DefaultStderrTailBytes)
	if err != nil {
		_ = readyWrite.Close()
		return Result{}, err
	}
	defer stderrCapture.cleanup()

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		_ = readyWrite.Close()
		return Result{}, err
	}
	defer devNull.Close()

	process, err := opts.Starter(ProcessConfig{
		CommandPath: opts.CommandPath,
		Args:        args,
		Env:         launchEnv(opts.Env, root),
		ExtraFiles:  []*os.File{readyWrite},
		Stdin:       devNull,
		Stdout:      devNull,
		Stderr:      stderrCapture.writer,
		Setsid:      true,
	})
	if err != nil {
		_ = readyWrite.Close()
		return Result{}, err
	}
	_ = readyWrite.Close()
	_ = stderrCapture.closeWriter()
	stderrCapture.start()
	handle := newHandle(process)

	frameCh := make(chan handshakeResult, 1)
	go func() {
		frame, err := readFrame(readyRead)
		frameCh <- handshakeResult{frame: frame, err: err}
	}()

	select {
	case got := <-frameCh:
		return handleFrame(launchCtx, handle, got, canonicalRoot, socketPath, tokenPath, stderrCapture)
	case <-launchCtx.Done():
		tail := stderrCapture.stopAndTail()
		killErr := handle.KillAndWait()
		kind := ErrReadinessCanceled
		if errors.Is(launchCtx.Err(), context.DeadlineExceeded) {
			kind = ErrReadinessTimeout
		}
		return Result{}, &StartupError{Kind: kind, StderrTail: tail, Cause: errors.Join(launchCtx.Err(), killErr)}
	}
}

type handshakeResult struct {
	frame readinessFrame
	err   error
}

func handleFrame(ctx context.Context, handle *Handle, got handshakeResult, expectedRoot, socketPath, tokenPath string, stderrCapture *stderrCapture) (Result, error) {
	if got.err != nil {
		tail := stderrCapture.stopAndTail()
		killErr := handle.KillAndWait()
		kind := ErrReadinessProtocol
		if errors.Is(got.err, io.EOF) {
			kind = ErrReadinessEOF
		}
		return Result{}, &StartupError{Kind: kind, StderrTail: tail, Cause: errors.Join(got.err, killErr)}
	}
	frame := got.frame
	switch {
	case frame.Ready != nil && frame.Failed == nil:
		ready := frame.Ready
		if ready.ProtocolVersion != ReadinessProtocolVersion {
			tail := stderrCapture.stopAndTail()
			killErr := handle.KillAndWait()
			return Result{}, &StartupError{Kind: ErrReadinessProtocol, Message: fmt.Sprintf("protocolVersion=%d", ready.ProtocolVersion), StderrTail: tail, Cause: killErr}
		}
		if ready.PID != handle.pid {
			tail := stderrCapture.stopAndTail()
			killErr := handle.KillAndWait()
			return Result{}, &StartupError{Kind: ErrReadinessProtocol, Message: fmt.Sprintf("pid=%d want %d", ready.PID, handle.pid), StderrTail: tail, Cause: killErr}
		}
		if ready.CanonicalStateRoot != expectedRoot {
			tail := stderrCapture.stopAndTail()
			killErr := handle.KillAndWait()
			return Result{}, &StartupError{
				Kind:                       ErrCanonicalStateRootMismatch,
				ExpectedCanonicalStateRoot: expectedRoot,
				ActualCanonicalStateRoot:   ready.CanonicalStateRoot,
				StderrTail:                 tail,
				Cause:                      killErr,
			}
		}
		stderrCapture.stopAndTail()
		return Result{
			PID:                ready.PID,
			CanonicalStateRoot: ready.CanonicalStateRoot,
			SocketPath:         ready.SocketPath,
			handle:             handle,
		}, nil
	case frame.Failed != nil && frame.Ready == nil:
		failed := frame.Failed
		if failed.Code == CodeAlreadyListening {
			if verifyErr := verifyExistingDaemonWithRetry(ctx, socketPath, tokenPath); verifyErr == nil {
				_ = handle.waitGraceOrKill(failedExitGrace)
				stderrCapture.stopAndTail()
				return Result{CanonicalStateRoot: expectedRoot, SocketPath: socketPath, ExistingDaemon: true}, nil
			} else {
				tail := stderrCapture.stopAndTail()
				_ = handle.waitGraceOrKill(failedExitGrace)
				return Result{}, &StartupError{
					Kind:       ErrExistingDaemonVerification,
					Code:       failed.Code,
					Message:    failed.Message,
					StderrTail: tail,
					Cause:      verifyErr,
				}
			}
		}
		tail := stderrCapture.stopAndTail()
		_ = handle.waitGraceOrKill(failedExitGrace)
		return Result{}, &StartupError{
			Kind:       ErrStartupFailed,
			Code:       failed.Code,
			Message:    failed.Message,
			StderrTail: tail,
		}
	default:
		tail := stderrCapture.stopAndTail()
		killErr := handle.KillAndWait()
		return Result{}, &StartupError{Kind: ErrReadinessProtocol, Message: "frame must contain exactly one of ready or failed", StderrTail: tail, Cause: killErr}
	}
}

func readFrame(file *os.File) (readinessFrame, error) {
	defer file.Close()
	var frame readinessFrame
	dec := json.NewDecoder(file)
	if err := dec.Decode(&frame); err != nil {
		return readinessFrame{}, err
	}
	return frame, nil
}

func prepareStateRoot(root string) (string, string, error) {
	resolved, err := resolveStateRoot(root)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(resolved, 0o700); err != nil {
		return "", "", err
	}
	if err := os.Chmod(resolved, 0o700); err != nil {
		return "", "", err
	}
	canonical, err := CanonicalStateRoot(resolved)
	if err != nil {
		return "", "", err
	}
	return resolved, canonical, nil
}

func resolveStateRoot(root string) (string, error) {
	if root != "" {
		return root, nil
	}
	return engine.ResolveStateRoot()
}

func CanonicalStateRoot(root string) (string, error) {
	resolved, err := resolveStateRoot(root)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func launchEnv(base []string, root string) []string {
	if base == nil {
		base = os.Environ()
	}
	env := append([]string(nil), base...)
	env = upsertEnv(env, "AGENTBUS_STATE_ROOT="+root)
	env = upsertEnv(env, ReadyFDEnv+"="+strconv.Itoa(readinessFDChildNumber))
	return env
}

func upsertEnv(env []string, kv string) []string {
	name, _, ok := strings.Cut(kv, "=")
	if !ok || name == "" {
		return env
	}
	for i, existing := range env {
		existingName, _, _ := strings.Cut(existing, "=")
		if existingName == name {
			env[i] = kv
			return env
		}
	}
	return append(env, kv)
}

func verifyExistingDaemonWithRetry(ctx context.Context, socketPath, tokenPath string) error {
	var last error
	for {
		err := verifyExistingDaemon(ctx, socketPath, tokenPath)
		if err == nil {
			return nil
		}
		if !retryableVerifyError(err) {
			return err
		}
		last = err
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-time.After(existingVerifyRetryPeriod):
		}
	}
}

func retryableVerifyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func verifyExistingDaemon(ctx context.Context, socketPath, tokenPath string) error {
	tokenRaw, err := os.ReadFile(tokenPath)
	if err != nil {
		return err
	}
	token := strings.TrimSpace(string(tokenRaw))
	if token == "" {
		return errors.New("agentbus token file is empty")
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	req := protocol.Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`"hello"`),
		Method:  protocol.MethodHello,
		Params:  mustMarshal(protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token}),
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	var resp protocol.Response
	if err := json.Unmarshal(bytes.TrimSpace(line), &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return &protocol.RPCError{Object: *resp.Error}
	}
	var hello protocol.HelloResult
	raw, err = json.Marshal(resp.Result)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &hello); err != nil {
		return err
	}
	if hello.ProtocolVersion != protocol.Version {
		return fmt.Errorf("protocol version mismatch: expected %d received %d", protocol.Version, hello.ProtocolVersion)
	}
	return nil
}

func mustMarshal(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

type stderrCapture struct {
	writer  *os.File
	reader  *os.File
	path    string
	tail    *tailBuffer
	stop    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func newStderrCapture(limit int) (*stderrCapture, error) {
	writer, err := os.CreateTemp("", "agentbus-daemon-stderr-*")
	if err != nil {
		return nil, err
	}
	reader, err := os.Open(writer.Name())
	if err != nil {
		_ = writer.Close()
		_ = os.Remove(writer.Name())
		return nil, err
	}
	return &stderrCapture{
		writer:  writer,
		reader:  reader,
		path:    writer.Name(),
		tail:    newTailBuffer(limit),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}, nil
}

func (capture *stderrCapture) closeWriter() error {
	if capture == nil || capture.writer == nil {
		return nil
	}
	err := capture.writer.Close()
	capture.writer = nil
	if capture.path != "" {
		_ = os.Remove(capture.path)
		capture.path = ""
	}
	return err
}

func (capture *stderrCapture) start() {
	if capture == nil || capture.reader == nil {
		return
	}
	go func() {
		defer close(capture.stopped)
		buf := make([]byte, 4096)
		for {
			n, err := capture.reader.Read(buf)
			if n > 0 {
				capture.tail.Write(buf[:n])
			}
			if err == nil {
				continue
			}
			if !errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-capture.stop:
				for {
					n, err := capture.reader.Read(buf)
					if n > 0 {
						capture.tail.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
}

func (capture *stderrCapture) stopAndTail() string {
	if capture == nil {
		return ""
	}
	capture.once.Do(func() {
		close(capture.stop)
		<-capture.stopped
	})
	return capture.tail.String()
}

func (capture *stderrCapture) cleanup() {
	if capture == nil {
		return
	}
	_ = capture.closeWriter()
	if capture.reader != nil {
		_ = capture.reader.Close()
		capture.reader = nil
	}
	if capture.path != "" {
		_ = os.Remove(capture.path)
		capture.path = ""
	}
}

type tailBuffer struct {
	limit int
	buf   []byte
}

func newTailBuffer(limit int) *tailBuffer {
	if limit <= 0 {
		limit = DefaultStderrTailBytes
	}
	return &tailBuffer{limit: limit}
}

func (tail *tailBuffer) Write(p []byte) {
	if tail == nil || len(p) == 0 {
		return
	}
	if len(p) >= tail.limit {
		tail.buf = append(tail.buf[:0], p[len(p)-tail.limit:]...)
		return
	}
	tail.buf = append(tail.buf, p...)
	if over := len(tail.buf) - tail.limit; over > 0 {
		copy(tail.buf, tail.buf[over:])
		tail.buf = tail.buf[:tail.limit]
	}
}

func (tail *tailBuffer) String() string {
	if tail == nil {
		return ""
	}
	return string(tail.buf)
}
