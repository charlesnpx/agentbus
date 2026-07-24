package agentbusserve

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	busclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/served"
)

const (
	agentbusServeHelperEnv     = "AGENTBUS_AGENTBUSSERVE_HELPER"
	agentbusServeHelperCWDEnv  = "AGENTBUS_AGENTBUSSERVE_HELPER_CWD"
	agentbusServeHelperModeEnv = "AGENTBUS_AGENTBUSSERVE_HELPER_MODE"
)

func TestMain(m *testing.M) {
	if os.Getenv(agentbusServeHelperEnv) == "1" {
		os.Exit(runAgentbusServeHelper())
	}
	os.Exit(m.Run())
}

func TestProductionServedConfigSelectsNativeStrictRuntime(t *testing.T) {
	cfg, err := productionServedConfig(Config{})
	support := cfg.Runtime.Support()
	if runtime.GOOS == "darwin" {
		if err == nil {
			t.Fatal("productionServedConfig() error = nil, want native runtime unsupported")
		}
		if support.Reason == nil || !errors.Is(support.Reason, custodian.ErrNativeRuntimeUnsupported) {
			t.Fatalf("runtime support = %+v, want native runtime unsupported diagnostic", support)
		}
		return
	}
	if err != nil {
		t.Fatalf("productionServedConfig() error = %v", err)
	}
	if support.Reason != nil && errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %+v, want native strict runtime rather than generic unavailable runtime", support)
	}
}

func TestProductionStrictServeFailsTypedOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform startup diagnostic")
	}
	err := Serve(context.Background(), Config{
		StateRoot:   t.TempDir(),
		CWD:         t.TempDir(),
		IdleTimeout: -1,
	})
	var diagnostic served.AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, served.ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("Serve error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if diagnostic.Assessment.Class == custodian.SupportAvailable {
		t.Fatalf("diagnostic assessment = %+v, want unavailable strict support", diagnostic.Assessment)
	}
}

func TestProductionServeLauncherUnsupportedLeavesFreshRootAbsentOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform startup diagnostic")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "state")
	_, err := daemonlaunch.Launch(context.Background(), agentbusServeLaunchOptions(t, root, t.TempDir()))
	assertDarwinUnsupportedLaunchError(t, err)
	if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("state root stat error = %v, want not exist", statErr)
	}
}

func TestProductionServeLauncherUnsupportedLeavesExistingRootPermissionsOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform startup diagnostic")
	}
	parent := t.TempDir()
	root := filepath.Join(parent, "state")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = daemonlaunch.Launch(context.Background(), agentbusServeLaunchOptions(t, root, t.TempDir()))
	assertDarwinUnsupportedLaunchError(t, err)
	after, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("state root mode = %o, want unchanged %o", after.Mode().Perm(), before.Mode().Perm())
	}
	for _, name := range []string{protocol.TokenFileName, protocol.SocketName, "agentbus.pid"} {
		if _, statErr := os.Stat(filepath.Join(root, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, statErr)
		}
	}
}

func TestProductionRecoverCLIUnsupportedLeavesExistingRootEmptyOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform recovery diagnostic")
	}
	root := t.TempDir()
	bin := buildAgentbusServeRealBinary(t)
	cmd := exec.Command(bin, "admission", "recover", "--state-root", root)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("recover succeeded, want unsupported diagnostic; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("recover error = %T %v, want non-zero exit", err, err)
	}
	if !strings.Contains(stderr.String(), served.ErrAdmissionStrictSupportUnavailable.Error()) {
		t.Fatalf("recover stderr = %q, want strict support diagnostic", stderr.String())
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("state root entries after recover = %v, want empty root", names)
	}
}

func TestRecoverAdmissionRootClosesRuntimeOnEarlyRootErrors(t *testing.T) {
	tests := []struct {
		name string
		root func(t *testing.T) string
	}{
		{
			name: "missing root",
			root: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing")
			},
		},
		{
			name: "root is file",
			root: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "state")
				if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime, closes := closeCountingRecoveryRuntime()
			stubRecoveryStrictRuntime(t, runtime, nil)

			_, err := RecoverAdmissionRoot(context.Background(), Config{
				StateRoot: tt.root(t),
				CWD:       t.TempDir(),
			})
			if !errors.Is(err, served.ErrAdmissionRootMissing) {
				t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionRootMissing", err)
			}
			if got := closes.Load(); got != 1 {
				t.Fatalf("runtime closes = %d, want 1", got)
			}
		})
	}
}

func TestRecoverAdmissionRootConfigErrorReturnsBeforeRootValidationAndClosesRuntime(t *testing.T) {
	runtime, closes := closeCountingRecoveryRuntime()
	configErr := errors.New("strict runtime compose failed")
	stubRecoveryStrictRuntime(t, runtime, configErr)

	_, err := RecoverAdmissionRoot(context.Background(), Config{
		StateRoot: filepath.Join(t.TempDir(), "missing"),
		CWD:       t.TempDir(),
	})
	if !errors.Is(err, configErr) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want config error", err)
	}
	if !errors.Is(err, served.ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if errors.Is(err, served.ErrAdmissionRootMissing) {
		t.Fatalf("RecoverAdmissionRoot error = %v, recovery ran root validation before returning config error", err)
	}
	if got := closes.Load(); got != 1 {
		t.Fatalf("runtime closes = %d, want 1", got)
	}
}

func TestServeLauncherReportsAdmissionRootBusyCode(t *testing.T) {
	root := t.TempDir()
	_, err := daemonlaunch.Launch(context.Background(), agentbusServeLaunchOptionsWithMode(t, root, t.TempDir(), "root-busy-report"))
	if err == nil {
		t.Fatal("Launch succeeded, want root-busy startup failure")
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(startup, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want startup failure", err, err)
	}
	if startup.Code != daemonlaunch.CodeAdmissionRootBusy {
		t.Fatalf("startup code = %q, want %q", startup.Code, daemonlaunch.CodeAdmissionRootBusy)
	}
	if !strings.Contains(startup.Message, served.ErrAdmissionRootBusy.Error()) {
		t.Fatalf("startup message = %q, want root busy diagnostic", startup.Message)
	}
}

func TestServeCancellationBeforeRegistrationReturnsCleanWithoutReadyFrame(t *testing.T) {
	root := t.TempDir()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	t.Setenv(daemonlaunch.ReadyFDEnv, strconv.Itoa(int(writer.Fd())))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveStarted := make(chan struct{})
	allowServeReturn := make(chan struct{})
	shutdownCalled := make(chan struct{})
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(_ context.Context, startupCtx context.Context) error {
			close(serveStarted)
			<-startupCtx.Done()
			<-allowServeReturn
			return context.Canceled
		},
		shutdown: func(context.Context) error {
			close(shutdownCalled)
			close(allowServeReturn)
			return served.ErrShutdownNotServing
		},
	}
	stubProductionServe(t, fake)

	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{StateRoot: root, CWD: t.TempDir(), IdleTimeout: -1})
	}()
	select {
	case <-serveStarted:
	case <-time.After(time.Second):
		t.Fatal("fake serve did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error = %v, want clean cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
	select {
	case <-shutdownCalled:
	default:
		t.Fatal("Shutdown was not called after cancellation")
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 0 {
		t.Fatalf("readiness frame = %q, want none", raw)
	}
	for _, name := range []string{protocol.SocketName, "agentbus.pid"} {
		if _, err := os.Stat(filepath.Join(root, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want absent", name, err)
		}
	}
}

func TestServeCancellationAfterRegistrationRunsGracefulShutdown(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readyCalled := make(chan struct{})
	shutdownCalled := make(chan struct{})
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(serviceCtx context.Context, _ context.Context) error {
			close(readyCalled)
			<-serviceCtx.Done()
			return nil
		},
		shutdown: func(context.Context) error {
			close(shutdownCalled)
			return nil
		},
	}
	stubProductionServe(t, fake)

	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{StateRoot: root, CWD: t.TempDir(), IdleTimeout: -1})
	}()
	select {
	case <-readyCalled:
	case <-time.After(time.Second):
		t.Fatal("fake serve did not reach registered lifetime")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error = %v, want graceful shutdown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
	select {
	case <-shutdownCalled:
	default:
		t.Fatal("Shutdown was not called for registered serve")
	}
}

func closeCountingRecoveryRuntime() (custodian.Runtime, *atomic.Int64) {
	closes := &atomic.Int64{}
	runtime := custodian.NewUnavailableRuntimeForTest(custodian.ErrSupervisorUnavailable, func() error {
		closes.Add(1)
		return nil
	})
	return runtime, closes
}

type fakeServedServer struct {
	shutdownTimeout time.Duration
	serve           func(context.Context, context.Context) error
	shutdown        func(context.Context) error
}

func (server *fakeServedServer) ServeWithStartupContext(ctx, startupCtx context.Context) error {
	if server.serve == nil {
		return nil
	}
	return server.serve(ctx, startupCtx)
}

func (server *fakeServedServer) Shutdown(ctx context.Context) error {
	if server.shutdown == nil {
		return nil
	}
	return server.shutdown(ctx)
}

func (server *fakeServedServer) ShutdownTimeout() time.Duration {
	return server.shutdownTimeout
}

func stubProductionServe(t *testing.T, server servedServer) {
	t.Helper()
	previousConfig := productionServedConfigFunc
	previousNewServer := newProductionServerAfterStrictAdmissionSupportPreflight
	productionServedConfigFunc = func(cfg Config) (served.Config, error) {
		return cfg, nil
	}
	newProductionServerAfterStrictAdmissionSupportPreflight = func(context.Context, served.Config) (servedServer, error) {
		return server, nil
	}
	t.Cleanup(func() {
		productionServedConfigFunc = previousConfig
		newProductionServerAfterStrictAdmissionSupportPreflight = previousNewServer
	})
}

func stubRecoveryStrictRuntime(t *testing.T, runtime custodian.Runtime, err error) {
	t.Helper()
	previous := newRecoveryStrictAdmissionRuntime
	newRecoveryStrictAdmissionRuntime = func(StrictAdmissionOptions) (custodian.Runtime, error) {
		return runtime, err
	}
	t.Cleanup(func() {
		newRecoveryStrictAdmissionRuntime = previous
	})
}

func TestServeLauncherDaemonSurvivesStartupDeadline(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real production daemon deadline variant runs on linux; split-context coverage runs in internal/served")
	}
	requireAgentbusServeStrictCgroup(t)
	root := t.TempDir()
	const timeout = 3 * time.Second
	result, err := daemonlaunch.Launch(context.Background(), daemonlaunch.Options{
		CommandPath: buildAgentbusServeRealBinary(t),
		Args:        []string{"serve", "--foreground"},
		StateRoot:   root,
		Timeout:     timeout,
		Starter:     agentbusServeProcessStarter,
		Env:         os.Environ(),
	})
	if err != nil {
		if strings.Contains(err.Error(), "bind: operation not permitted") {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("Launch error = %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = result.KillAndWait()
		}
	})

	time.Sleep(2 * timeout)
	assertAgentbusServeProcessAlive(t, result.PID, "after startup deadline")
	clientCtx, cancelClient := context.WithTimeout(context.Background(), time.Second)
	defer cancelClient()
	client, err := busclient.Connect(clientCtx, busclient.Options{
		StateRoot:        root,
		DisableAutoStart: true,
	})
	if err != nil {
		t.Fatalf("connect after startup deadline: %v", err)
	}
	defer client.Close()
	hello, err := client.Hello(clientCtx)
	if err != nil {
		t.Fatalf("hello after startup deadline: %v", err)
	}
	if hello.ProtocolVersion != protocol.Version {
		t.Fatalf("hello protocolVersion = %d, want %d", hello.ProtocolVersion, protocol.Version)
	}
	assertAgentbusServeProcessAlive(t, result.PID, "after hello past startup deadline")

	_ = result.KillAndWait()
	stopped = true
	assertAgentbusServeProcessGone(t, result.PID)
}

func requireAgentbusServeStrictCgroup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	manager, err := cgroup.New("")
	if err != nil {
		t.Skipf("cgroup strict support unavailable: %v", err)
	}
	support := manager.Probe(ctx)
	if closeErr := manager.Close(); closeErr != nil {
		t.Fatalf("close cgroup probe manager: %v", closeErr)
	}
	if !support.Strict() {
		t.Skipf("strict cgroup-v2 support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}
}

func buildAgentbusServeRealBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agentbus")
	cmd := exec.Command("go", "build", "-o", path, "./cmd/agentbus")
	cmd.Dir = agentbusServeRepoRootFromCaller(t)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build ./cmd/agentbus: %v\n%s", err, output)
	}
	return path
}

func agentbusServeRepoRootFromCaller(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func runAgentbusServeHelper() int {
	cwd := os.Getenv(agentbusServeHelperCWDEnv)
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return 2
		}
	}
	if os.Getenv(agentbusServeHelperModeEnv) == "root-busy-report" {
		reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
		if err != nil {
			return 2
		}
		if !hasReporter {
			return 2
		}
		defer reporter.Close()
		reportStartupFailure(reporter, served.AdmissionRootBusyError{
			Path:       filepath.Join(os.Getenv("AGENTBUS_STATE_ROOT"), "admission.bbolt"),
			SocketPath: filepath.Join(os.Getenv("AGENTBUS_STATE_ROOT"), protocol.SocketName),
		})
		return 1
	}
	if err := Serve(context.Background(), Config{
		StateRoot:   os.Getenv("AGENTBUS_STATE_ROOT"),
		CWD:         cwd,
		IdleTimeout: -1,
	}); err != nil {
		return 1
	}
	return 0
}

func agentbusServeLaunchOptions(t *testing.T, root, cwd string) daemonlaunch.Options {
	t.Helper()
	return agentbusServeLaunchOptionsWithMode(t, root, cwd, "")
}

func agentbusServeLaunchOptionsWithMode(t *testing.T, root, cwd, mode string) daemonlaunch.Options {
	t.Helper()
	return agentbusServeLaunchOptionsWithModeAndTimeout(t, root, cwd, mode, 2*time.Second)
}

func agentbusServeLaunchOptionsWithModeAndTimeout(t *testing.T, root, cwd, mode string, timeout time.Duration) daemonlaunch.Options {
	t.Helper()
	env := append(os.Environ(),
		agentbusServeHelperEnv+"=1",
		agentbusServeHelperCWDEnv+"="+cwd,
	)
	if mode != "" {
		env = append(env, agentbusServeHelperModeEnv+"="+mode)
	}
	return daemonlaunch.Options{
		CommandPath: os.Args[0],
		Args:        []string{"serve", "--foreground"},
		StateRoot:   root,
		Timeout:     timeout,
		Starter:     agentbusServeProcessStarter,
		Env:         env,
	}
}

func assertDarwinUnsupportedLaunchError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Launch succeeded, want unsupported diagnostic")
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(err, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want startup failure", err, err)
	}
	if startup.Code != served.ErrAdmissionStrictSupportUnavailable.Error() {
		t.Fatalf("startup code = %q, want strict support unavailable", startup.Code)
	}
	if !strings.Contains(startup.Message, served.ErrAdmissionStrictSupportUnavailable.Error()) {
		t.Fatalf("startup message = %q, want strict support diagnostic", startup.Message)
	}
}

type agentbusServeProcess struct {
	cmd *exec.Cmd
}

func agentbusServeProcessStarter(config daemonlaunch.ProcessConfig) (daemonlaunch.Process, error) {
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
	return agentbusServeProcess{cmd: cmd}, nil
}

func (process agentbusServeProcess) PID() int {
	return process.cmd.Process.Pid
}

func (process agentbusServeProcess) Kill() error {
	return process.cmd.Process.Kill()
}

func (process agentbusServeProcess) Wait() error {
	return process.cmd.Wait()
}

func assertAgentbusServeProcessAlive(t *testing.T, pid int, reason string) {
	t.Helper()
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("daemon process %d is not alive %s: %v", pid, reason, err)
	}
}

func assertAgentbusServeProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon process %d still exists after shutdown", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
