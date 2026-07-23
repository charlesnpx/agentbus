package daemonlaunch

import (
	"bufio"
	"context"
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
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/procgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	launchHelperEnv     = "AGENTBUS_DAEMONLAUNCH_HELPER"
	launchHelperModeEnv = "AGENTBUS_DAEMONLAUNCH_HELPER_MODE"
	launchHelperPIDEnv  = "AGENTBUS_DAEMONLAUNCH_HELPER_PID"
)

func TestMain(m *testing.M) {
	if os.Getenv(launchHelperEnv) == "1" {
		os.Exit(runLaunchHelper())
	}
	os.Exit(m.Run())
}

func TestLaunchFailedRecordIncludesDiagnosticAndStderr(t *testing.T) {
	root := shortLaunchTempDir(t)
	pidPath := filepath.Join(root, "helper.pid")
	_, err := Launch(context.Background(), helperLaunchOptions(t, root, "failed", pidPath, 2*time.Second))
	if err == nil {
		t.Fatal("Launch succeeded, want failed startup")
	}
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want ErrStartupFailed", err, err)
	}
	if startup.Code != "strict admission support unavailable" || !strings.Contains(startup.Message, "strict diagnostic from helper") {
		t.Fatalf("startup error = %+v, want strict diagnostic code and message", startup)
	}
	if !strings.Contains(startup.StderrTail, "helper failed stderr") || !strings.Contains(err.Error(), "helper failed stderr") {
		t.Fatalf("stderr tail not surfaced: %+v / %v", startup, err)
	}
	assertPIDGone(t, readPIDFile(t, pidPath))
}

func TestLaunchCrashBeforeRecordIncludesStderrAndReaps(t *testing.T) {
	root := shortLaunchTempDir(t)
	pidPath := filepath.Join(root, "helper.pid")
	_, err := Launch(context.Background(), helperLaunchOptions(t, root, "crash", pidPath, 2*time.Second))
	if err == nil {
		t.Fatal("Launch succeeded, want pre-record crash")
	}
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrReadinessEOF) {
		t.Fatalf("Launch error = %T %v, want ErrReadinessEOF", err, err)
	}
	if !strings.Contains(startup.StderrTail, "helper crash stderr") {
		t.Fatalf("stderr tail = %q, want crash stderr", startup.StderrTail)
	}
	assertPIDGone(t, readPIDFile(t, pidPath))
}

func TestLaunchTimeoutKillsAndReaps(t *testing.T) {
	root := shortLaunchTempDir(t)
	pidPath := filepath.Join(root, "helper.pid")
	_, err := Launch(context.Background(), helperLaunchOptions(t, root, "hang", pidPath, 100*time.Millisecond))
	if err == nil {
		t.Fatal("Launch succeeded, want timeout")
	}
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("Launch error = %T %v, want ErrReadinessTimeout", err, err)
	}
	if !strings.Contains(startup.StderrTail, "helper hang stderr") {
		t.Fatalf("stderr tail = %q, want hang stderr", startup.StderrTail)
	}
	assertPIDGone(t, readPIDFile(t, pidPath))
}

func TestLaunchTimeoutKillsChattyChildAndBoundsStderr(t *testing.T) {
	root := shortLaunchTempDir(t)
	pidPath := filepath.Join(root, "helper.pid")
	start := time.Now()
	_, err := Launch(context.Background(), helperLaunchOptions(t, root, "chatty-hang", pidPath, 100*time.Millisecond))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Launch succeeded, want timeout")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Launch timeout took %s, want bounded cleanup", elapsed)
	}
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrReadinessTimeout) {
		t.Fatalf("Launch error = %T %v, want ErrReadinessTimeout", err, err)
	}
	if len(startup.StderrTail) > DefaultStderrTailBytes {
		t.Fatalf("stderr tail length = %d, want <= %d", len(startup.StderrTail), DefaultStderrTailBytes)
	}
	if !strings.Contains(startup.StderrTail, "helper chatty stderr") {
		t.Fatalf("stderr tail = %q, want chatty stderr", startup.StderrTail)
	}
	assertPIDGone(t, readPIDFile(t, pidPath))
}

func TestLaunchRootMismatchKillsAndReaps(t *testing.T) {
	root := shortLaunchTempDir(t)
	pidPath := filepath.Join(root, "helper.pid")
	_, err := Launch(context.Background(), helperLaunchOptions(t, root, "mismatch", pidPath, 2*time.Second))
	if err == nil {
		t.Fatal("Launch succeeded, want root mismatch")
	}
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrCanonicalStateRootMismatch) {
		t.Fatalf("Launch error = %T %v, want ErrCanonicalStateRootMismatch", err, err)
	}
	assertPIDGone(t, readPIDFile(t, pidPath))
}

func TestLaunchAlreadyListeningVerifiesWinner(t *testing.T) {
	root := shortLaunchTempDir(t)
	token := "winner-token"
	if err := os.WriteFile(filepath.Join(root, protocol.TokenFileName), []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, protocol.SocketName)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	helloSeen := make(chan struct{}, 1)
	go serveOneVerifiedHello(listener, token, helloSeen)

	result, err := Launch(context.Background(), helperLaunchOptions(t, root, "already-listening", "", 2*time.Second))
	if err != nil {
		t.Fatalf("Launch already-listening error: %v", err)
	}
	if !result.ExistingDaemon || result.PID != 0 {
		t.Fatalf("result = %+v, want existing daemon without child pid", result)
	}
	select {
	case <-helloSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("winner did not receive authenticated hello")
	}
}

func TestLaunchFailedRecordSurfacesReapError(t *testing.T) {
	reapErr := errors.New("wait failed for test")
	capture, err := newStderrCapture(DefaultStderrTailBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.closeWriter(); err != nil {
		t.Fatal(err)
	}
	capture.start()
	handle := newHandle(stubProcess{pid: 123, waitErr: reapErr})
	_, err = handleFrame(context.Background(), handle, handshakeResult{
		frame: readinessFrame{Failed: &failedRecord{Code: "strict admission support unavailable", Message: "boom"}},
	}, "/tmp/root", "/tmp/root/agentbus.sock", "/tmp/root/token", capture)
	var startup *StartupError
	if !errors.As(err, &startup) || !errors.Is(err, ErrStartupFailed) {
		t.Fatalf("handleFrame error = %T %v, want ErrStartupFailed", err, err)
	}
	if !errors.Is(err, reapErr) {
		t.Fatalf("handleFrame error = %v, want reap error in chain", err)
	}
}

func TestInheritedReporterClearsEnvAndMarksCloseOnExec(t *testing.T) {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readEnd.Close()
	defer writeEnd.Close()
	t.Setenv(ReadyFDEnv, strconv.Itoa(int(writeEnd.Fd())))

	reporter, hasReporter, err := InheritedReporterFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !hasReporter || reporter == nil {
		t.Fatal("InheritedReporterFromEnv did not return reporter")
	}
	defer reporter.Close()
	if got := os.Getenv(ReadyFDEnv); got != "" {
		t.Fatalf("%s = %q, want unset", ReadyFDEnv, got)
	}
	closeOnExec, err := readyFDCloseOnExec(int(writeEnd.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if !closeOnExec {
		t.Fatal("readiness fd is not marked close-on-exec")
	}
}

func TestLaunchSetsid(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process group assertion is Unix-specific")
	}
	root := shortLaunchTempDir(t)
	result, err := Launch(context.Background(), helperLaunchOptions(t, root, "ready", "", 2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = result.KillAndWait() })
	pgid, err := procgroup.ReadProcessPGID(result.PID)
	if err != nil {
		t.Fatal(err)
	}
	if pgid != result.PID {
		t.Fatalf("child pgid = %d, want pid %d", pgid, result.PID)
	}
}

func runLaunchHelper() int {
	mode := os.Getenv(launchHelperModeEnv)
	root := os.Getenv("AGENTBUS_STATE_ROOT")
	pidPath := os.Getenv(launchHelperPIDEnv)
	if pidPath != "" {
		_ = os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600)
	}
	reporter, hasReporter, err := InheritedReporterFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if !hasReporter {
		fmt.Fprintln(os.Stderr, "missing readiness reporter")
		return 2
	}
	defer reporter.Close()
	socketPath := filepath.Join(root, protocol.SocketName)
	switch mode {
	case "failed":
		fmt.Fprintln(os.Stderr, "helper failed stderr")
		_ = reporter.Failed("strict admission support unavailable", "strict diagnostic from helper")
		return 1
	case "crash":
		fmt.Fprintln(os.Stderr, "helper crash stderr")
		return 42
	case "hang":
		fmt.Fprintln(os.Stderr, "helper hang stderr")
		sleepForever()
	case "chatty-hang":
		for i := 0; ; i++ {
			fmt.Fprintf(os.Stderr, "helper chatty stderr %06d %s\n", i, strings.Repeat("x", 1024))
		}
	case "mismatch":
		fmt.Fprintln(os.Stderr, "helper mismatch stderr")
		_ = reporter.Ready("/definitely/not-the-expected-root", socketPath)
		sleepForever()
	case "already-listening":
		fmt.Fprintln(os.Stderr, "helper already-listening stderr")
		_ = reporter.Failed(CodeAlreadyListening, CodeAlreadyListening+" at "+socketPath)
		return 1
	case "ready":
		canonicalRoot, err := CanonicalStateRoot(root)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if err := reporter.Ready(canonicalRoot, socketPath); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		sleepForever()
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q\n", mode)
		return 2
	}
	return 0
}

func sleepForever() {
	for {
		time.Sleep(time.Hour)
	}
}

func helperLaunchOptions(t *testing.T, root, mode, pidPath string, timeout time.Duration) Options {
	t.Helper()
	return Options{
		CommandPath: os.Args[0],
		Args:        []string{"serve", "--foreground"},
		StateRoot:   root,
		Timeout:     timeout,
		Starter:     testProcessStarter,
		Env: append(os.Environ(),
			launchHelperEnv+"=1",
			launchHelperModeEnv+"="+mode,
			launchHelperPIDEnv+"="+pidPath,
		),
	}
}

type testProcess struct {
	cmd *exec.Cmd
}

type stubProcess struct {
	pid     int
	waitErr error
}

func (process stubProcess) PID() int {
	return process.pid
}

func (process stubProcess) Kill() error {
	return nil
}

func (process stubProcess) Wait() error {
	return process.waitErr
}

func testProcessStarter(config ProcessConfig) (Process, error) {
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
	return testProcess{cmd: cmd}, nil
}

func (process testProcess) PID() int {
	return process.cmd.Process.Pid
}

func (process testProcess) Kill() error {
	return process.cmd.Process.Kill()
}

func (process testProcess) Wait() error {
	return process.cmd.Wait()
}

func serveOneVerifiedHello(listener net.Listener, token string, helloSeen chan<- struct{}) {
	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	var req protocol.Request
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(line))), &req); err != nil {
		return
	}
	resp := protocol.Response{JSONRPC: "2.0", ID: req.ID}
	var params protocol.HelloParams
	if req.Method != protocol.MethodHello || json.Unmarshal(req.Params, &params) != nil || params.Token != token {
		resp.Error = protocol.NewError(protocol.ErrorUnauthorized, "unauthorized", protocol.ErrorData{})
		_ = json.NewEncoder(conn).Encode(resp)
		return
	}
	resp.Result = protocol.HelloResult{
		ProtocolVersion: protocol.Version,
		Backends:        []string{},
		Capabilities:    protocol.DefaultCapabilities(),
	}
	_ = json.NewEncoder(conn).Encode(resp)
	helloSeen <- struct{}{}
}

func readPIDFile(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse pid %q: %v", raw, err)
	}
	return pid
}

func assertPIDGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d still exists after launcher returned: %v", pid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func shortLaunchTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), "ab-launch-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
