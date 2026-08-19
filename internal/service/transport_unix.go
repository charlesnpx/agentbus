//go:build darwin || linux

package service

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
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type connection struct {
	server *Server
	conn   net.Conn
	hello  bool
}

func (c *connection) serve(_ context.Context) {
	defer c.conn.Close()
	scanner := bufio.NewScanner(c.conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var request protocol.Request
		if err := json.Unmarshal(line, &request); err != nil {
			_ = c.writeResponse(protocol.Response{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "malformed JSON-RPC frame", protocol.ErrorData{}),
			})
			continue
		}
		if len(request.ID) == 0 && requiresRequestID(request.Method) {
			_ = c.writeResponse(protocol.Response{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   protocol.NewError(protocol.ErrorInvalidTaskSpecV3, request.Method+" requires a JSON-RPC id", protocol.ErrorData{}),
			})
			continue
		}
		outcome := c.server.handle(c, request)
		if len(request.ID) != 0 {
			response := protocol.Response{JSONRPC: "2.0", ID: request.ID, Result: outcome.result, Error: outcome.err}
			_ = c.writeResponse(response)
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
			Error:   protocol.NewError(protocol.ErrorInvalidTaskSpecV3, message, protocol.ErrorData{}),
		})
	}
}

func (c *connection) writeOversizedRequestResponse(response protocol.Response) error {
	if c == nil || c.conn == nil {
		return net.ErrClosed
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(oversizedWriteTimeout)); err != nil {
		_ = c.conn.Close()
		return err
	}
	defer c.conn.SetWriteDeadline(time.Time{})
	return c.writeResponse(response)
}

func requiresRequestID(method string) bool {
	return method == protocol.MethodHello
}

func (c *connection) writeResponse(response protocol.Response) error {
	return writeFrame(c.conn, response)
}

func writeFrame(writer io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = writer.Write(encoded)
	return err
}

type requestOutcome struct {
	result any
	err    *protocol.ErrorObject
}

func (s *Server) handle(connection *connection, request protocol.Request) requestOutcome {
	if request.JSONRPC != "2.0" || request.Method == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "invalid JSON-RPC request", protocol.ErrorData{})}
	}
	if !connection.hello && request.Method != protocol.MethodHello {
		return requestOutcome{err: protocol.NewError(protocol.ErrorUnauthorizedV3, "protocol.hello is required before other methods", protocol.ErrorData{})}
	}
	if connection.hello && request.Method == protocol.MethodHello {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, "protocol.hello has already been completed on this connection", protocol.ErrorData{})}
	}
	switch request.Method {
	case protocol.MethodHello:
		return s.handleHello(connection, request.Params)
	default:
		return requestOutcome{err: protocol.NewError(protocol.ErrorMethodNotFoundV3, "method not found", protocol.ErrorData{})}
	}
}

func (s *Server) handleHello(connection *connection, raw json.RawMessage) requestOutcome {
	var params protocol.HelloParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if params.Token == "" || params.Token != s.token {
		return requestOutcome{err: protocol.NewError(protocol.ErrorUnauthorizedV3, "missing or invalid hello token", protocol.ErrorData{})}
	}
	if params.ClientProtocolVersion != ProtocolVersion {
		return requestOutcome{err: protocol.NewError(protocol.ErrorVersionMismatchV3, "protocol major version mismatch", protocol.ErrorData{ServerProtocolVersion: ProtocolVersion})}
	}
	connection.hello = true
	return requestOutcome{result: protocol.HelloResultV3{
		ProtocolVersion: ProtocolVersion,
		BackendMetadata: s.backendMetadata(),
	}}
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

func ensureToken(path, configured string) (string, error) {
	token, err := readExistingToken(path)
	if err == nil {
		return token, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
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
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
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
		if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "token file is empty") {
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

func listenUnixSocketPrivate(path string) (net.Listener, socketFileIdentity, error) {
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
	listener, err := net.FileListener(file)
	if err != nil {
		_ = file.Close()
		removeSocketPathIfIdentity(path, identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	if err := file.Close(); err != nil {
		_ = listener.Close()
		removeSocketPathIfIdentity(path, identity, "listener setup failure")
		return nil, socketFileIdentity{}, err
	}
	return listener, identity, nil
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

func (s *Server) removeOwnedPIDFile(ctx context.Context, phase string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pidPath := filepath.Join(s.stateRoot, "agentbus.pid")
	raw, owned, err := readPIDFileNoFollow(pidPath)
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
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: invalid pid file %s", phase, pidPath)
		return nil
	}
	if pid != os.Getpid() {
		log.Printf("agentbus daemon: skipping pid removal during %s: %s belongs to pid %d", phase, pidPath, pid)
		return nil
	}
	quarantineDir, quarantinePath, err := createPIDFileQuarantine(s.stateRoot)
	if err != nil {
		log.Printf("agentbus daemon: skipping pid removal during %s: create pid quarantine: %v", phase, err)
		return fmt.Errorf("%w: create pid quarantine: %w", ErrShutdownPIDTeardownFailed, err)
	}
	cleanupQuarantineDir := true
	defer func() {
		if !cleanupQuarantineDir || ctx.Err() != nil {
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
	quarantinedRaw, quarantined, err := readPIDFileNoFollow(quarantinePath)
	if err != nil {
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
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
	if err != nil || quarantined != owned || quarantinedPID != os.Getpid() {
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		cleanupQuarantineDir = cleanup
		return wrapPIDRestoreError(restoreErr)
	}
	if err := abortQuarantinedPIDFileIfContextDone(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir); err != nil {
		return err
	}
	if err := os.Remove(quarantinePath); err != nil {
		cleanup, restoreErr := restoreQuarantinedPIDFileContext(ctx, pidPath, quarantinePath, phase, &cleanupQuarantineDir)
		cleanupQuarantineDir = cleanup
		return errors.Join(
			fmt.Errorf("%w: remove owned pid during %s: %w", ErrShutdownPIDTeardownFailed, phase, err),
			wrapPIDRestoreError(restoreErr),
		)
	}
	if err := ctx.Err(); err != nil {
		cleanupQuarantineDir = false
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

func decodeStrict(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("params must contain one JSON object")
	}
	return nil
}

func invalidParams(err error) requestOutcome {
	return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpecV3, err.Error(), protocol.ErrorData{})}
}
