package agentbusserve

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	busclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/served"
)

const (
	agentbusServeHelperEnv     = "AGENTBUS_AGENTBUSSERVE_HELPER"
	agentbusServeHelperCWDEnv  = "AGENTBUS_AGENTBUSSERVE_HELPER_CWD"
	agentbusServeHelperModeEnv = "AGENTBUS_AGENTBUSSERVE_HELPER_MODE"
	agentbusServeHelperMarkEnv = "AGENTBUS_AGENTBUSSERVE_HELPER_MARK"
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
	if err != nil {
		t.Fatalf("productionServedConfig() error = %v", err)
	}
	defer func() {
		if closeErr := cfg.Runtime.Close(); closeErr != nil {
			t.Fatalf("runtime Close() error = %v", closeErr)
		}
	}()
	if support.Reason != nil && errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %+v, want native strict runtime rather than generic unavailable runtime", support)
	}
	if runtime.GOOS == "darwin" {
		if !errors.Is(support.Reason, custodian.ErrNativeRuntimeSelfTestRequired) || support.RuntimeProbePassed {
			t.Fatalf("darwin runtime support = %+v, want self-test required before serving preflight", support)
		}
		if _, ok := cfg.Runtime.Process().(*custodian.NativeCustodian); !ok {
			t.Fatalf("darwin runtime process = %T, want *NativeCustodian", cfg.Runtime.Process())
		}
	}
}

func TestProductionStrictServePreflightPassesOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin strict runtime startup qualification")
	}
	cfg, err := strictAdmissionServedConfig(Config{
		StateRoot:   t.TempDir(),
		CWD:         t.TempDir(),
		IdleTimeout: -1,
	}, StrictAdmissionOptions{AgentbusPath: buildAgentbusServeRealBinary(t)})
	if err != nil {
		t.Fatalf("strictAdmissionServedConfig() error = %v", err)
	}
	defer func() {
		if closeErr := cfg.Runtime.Close(); closeErr != nil {
			t.Fatalf("runtime Close() error = %v", closeErr)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := served.StrictAdmissionSupportPreflight(ctx, cfg); err != nil {
		t.Fatalf("StrictAdmissionSupportPreflight() error = %v", err)
	}
	support := cfg.Runtime.Support()
	if support.Assessment.Class != custodian.SupportAvailable || !support.ParkedExec || !support.VerifiedContainment {
		t.Fatalf("darwin runtime support after preflight = %+v, want available parked exec and verified containment", support)
	}
}

func TestProductionServeLauncherServesOnDarwinFreshRoot(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin strict runtime startup qualification")
	}
	parent := shortAgentbusServeTempDir(t)
	root := filepath.Join(parent, "state")
	result, err := daemonlaunch.Launch(context.Background(), realAgentbusServeLaunchOptions(t, root))
	if err != nil {
		failOrSkipAgentbusServeBindDenied(t, "production serve launcher fresh root", err)
		t.Fatalf("Launch error = %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = result.KillAndWait()
		}
	})
	assertAgentbusServeReady(t, root, result)
	_ = result.KillAndWait()
	stopped = true
	assertAgentbusServeProcessGone(t, result.PID)
}

func TestProductionServeLauncherServesFromExistingRootOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin strict runtime startup qualification")
	}
	parent := shortAgentbusServeTempDir(t)
	root := filepath.Join(parent, "state")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := daemonlaunch.Launch(context.Background(), realAgentbusServeLaunchOptions(t, root))
	if err != nil {
		failOrSkipAgentbusServeBindDenied(t, "production serve launcher existing root", err)
		t.Fatalf("Launch error = %v", err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = result.KillAndWait()
		}
	})
	assertAgentbusServeReady(t, root, result)
	after, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	// The daemon deliberately tightens the state root to owner-only (0700) during
	// serve (served/server.go socket-security hardening): the root holds the
	// socket, token, and repository. An existing looser mode is tightened, never
	// loosened.
	if after.Mode().Perm() != 0o700 {
		t.Fatalf("state root mode = %o, want daemon-tightened 0700 (was %o before serve)", after.Mode().Perm(), before.Mode().Perm())
	}
	_ = result.KillAndWait()
	stopped = true
	assertAgentbusServeProcessGone(t, result.PID)
}

func TestProductionRecoverCLIReportsRootMissingOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin strict runtime recovery qualification")
	}
	root := t.TempDir()
	bin := buildAgentbusServeRealBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "admission", "recover", "--state-root", root)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("recover timed out: %v; stdout=%s stderr=%s", ctx.Err(), stdout.String(), stderr.String())
	}
	if err == nil {
		t.Fatalf("recover succeeded, want missing admission root diagnostic; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("recover error = %T %v, want non-zero exit", err, err)
	}
	if strings.Contains(stderr.String(), served.ErrAdmissionStrictSupportUnavailable.Error()) {
		t.Fatalf("recover stderr = %q, did not expect strict support diagnostic on darwin", stderr.String())
	}
	if !strings.Contains(stderr.String(), served.ErrAdmissionRootMissing.Error()) {
		t.Fatalf("recover stderr = %q, want missing admission root diagnostic", stderr.String())
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

func TestServeSIGTERMDuringSynchronousPreflightReturnsClean(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "state")
	marker := filepath.Join(parent, "preflight-started")
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		agentbusServeHelperEnv+"=1",
		agentbusServeHelperCWDEnv+"="+t.TempDir(),
		agentbusServeHelperModeEnv+"=preflight-stall-signal",
		agentbusServeHelperMarkEnv+"="+marker,
		"AGENTBUS_STATE_ROOT="+root,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("preflight marker stat error = %v", err)
		}
		select {
		case err := <-done:
			t.Fatalf("helper exited before synchronous preflight stalled: %v stderr=%s", err, stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("helper did not enter synchronous preflight; stderr=%s", stderr.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("SIGTERM helper: %v stderr=%s", err, stderr.String())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("helper exit after SIGTERM = %v, want clean exit; stderr=%s", err, stderr.String())
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("helper did not exit after SIGTERM; stderr=%s", stderr.String())
	}
	for _, path := range []string{root, filepath.Join(root, protocol.SocketName), filepath.Join(root, "agentbus.pid")} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want absent", path, err)
		}
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

func TestServeCancellationJoinedSafetyFailStopReturnsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failStopErr := errors.Join(served.SafetyFailStopError{Reason: context.Canceled}, context.Canceled)
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(context.Context, context.Context) error {
			return failStopErr
		},
		shutdown: func(context.Context) error {
			return served.ErrShutdownNotServing
		},
	}
	stubProductionServe(t, fake)

	err := Serve(ctx, Config{StateRoot: t.TempDir(), CWD: t.TempDir(), IdleTimeout: -1})
	if err == nil {
		t.Fatal("Serve error = nil, want joined safety fail-stop failure")
	}
	var safety served.SafetyFailStopError
	if !errors.As(err, &safety) {
		t.Fatalf("Serve error = %T %v, want SafetyFailStopError", err, err)
	}
	if !errors.Is(err, served.ErrSafetyFailStopped) {
		t.Fatalf("Serve error = %v, want ErrSafetyFailStopped", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want joined context.Canceled preserved", err)
	}
}

func TestServeCancellationDefinitelyNotCommittedContextCanceledReturnsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(context.Context, context.Context) error {
			return errors.Join(repository.ErrDefinitelyNotCommitted, context.Canceled)
		},
		shutdown: func(context.Context) error {
			return served.ErrShutdownNotServing
		},
	}
	stubProductionServe(t, fake)

	if err := Serve(ctx, Config{StateRoot: t.TempDir(), CWD: t.TempDir(), IdleTimeout: -1}); err != nil {
		t.Fatalf("Serve error = %v, want clean cancellation", err)
	}
}

func TestServeCancellationAmbiguousCommitContextCanceledReturnsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(context.Context, context.Context) error {
			return errors.Join(repository.ErrAmbiguousCommit, context.Canceled)
		},
		shutdown: func(context.Context) error {
			return served.ErrShutdownNotServing
		},
	}
	stubProductionServe(t, fake)

	err := Serve(ctx, Config{StateRoot: t.TempDir(), CWD: t.TempDir(), IdleTimeout: -1})
	if !errors.Is(err, repository.ErrAmbiguousCommit) {
		t.Fatalf("Serve error = %v, want ambiguous commit failure", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want joined context.Canceled preserved", err)
	}
}

func TestServeLauncherReportsJoinedSafetyFailStopDuringCancellation(t *testing.T) {
	root := t.TempDir()
	_, err := daemonlaunch.Launch(context.Background(), agentbusServeLaunchOptionsWithMode(t, root, t.TempDir(), "joined-fail-stop-canceled-report"))
	if err == nil {
		t.Fatal("Launch succeeded, want reported safety fail-stop startup failure")
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(startup, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want startup failure", err, err)
	}
	if startup.Code != served.ErrSafetyFailStopped.Error() {
		t.Fatalf("startup code = %q, want %q", startup.Code, served.ErrSafetyFailStopped)
	}
	if !strings.Contains(startup.Message, served.ErrSafetyFailStopped.Error()) {
		t.Fatalf("startup message = %q, want safety fail-stop", startup.Message)
	}
}

func TestServeLauncherReportsAuthorityFailStopDuringCancellation(t *testing.T) {
	root := t.TempDir()
	_, err := daemonlaunch.Launch(context.Background(), agentbusServeLaunchOptionsWithMode(t, root, t.TempDir(), "authority-fail-stop-canceled-report"))
	if err == nil {
		t.Fatal("Launch succeeded, want reported authority fail-stop startup failure")
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(startup, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want startup failure", err, err)
	}
	if startup.Code != served.ErrSafetyFailStopped.Error() {
		t.Fatalf("startup code = %q, want %q", startup.Code, served.ErrSafetyFailStopped)
	}
	if !strings.Contains(startup.Message, authority.ErrFailStopped.Error()) {
		t.Fatalf("startup message = %q, want authority fail-stop", startup.Message)
	}
}

func TestServeCancellationImmediateContextCanceledReturnsClean(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveStarted := make(chan struct{})
	fake := &fakeServedServer{
		shutdownTimeout: time.Second,
		serve: func(context.Context, context.Context) error {
			close(serveStarted)
			<-ctx.Done()
			return fmt.Errorf("serve stopped: %w", context.Canceled)
		},
		shutdown: func(context.Context) error {
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
}

func TestServeReadinessAfterCancellationDoesNotEmitReadyFrame(t *testing.T) {
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
	canonicalStarted := make(chan struct{})
	releaseCanonical := make(chan struct{})
	previousCanonical := canonicalStateRootFunc
	canonicalStateRootFunc = func(path string) (string, error) {
		close(canonicalStarted)
		<-releaseCanonical
		return filepath.Clean(path), nil
	}
	t.Cleanup(func() {
		canonicalStateRootFunc = previousCanonical
	})

	var readyHook func(served.ServeReadyInfo) error
	previousConfig := productionServedConfigFunc
	previousNewServer := newProductionServerAfterStrictAdmissionSupportPreflight
	productionServedConfigFunc = func(cfg Config) (served.Config, error) {
		return cfg, nil
	}
	newProductionServerAfterStrictAdmissionSupportPreflight = func(_ context.Context, cfg served.Config) (servedServer, error) {
		readyHook = cfg.ReadyHook
		return &fakeServedServer{
			shutdownTimeout: time.Second,
			serve: func(context.Context, context.Context) error {
				if readyHook == nil {
					return errors.New("ready hook was not installed")
				}
				return readyHook(served.ServeReadyInfo{StateRoot: root, SocketPath: filepath.Join(root, protocol.SocketName)})
			},
			shutdown: func(context.Context) error {
				return served.ErrShutdownNotServing
			},
		}, nil
	}
	t.Cleanup(func() {
		productionServedConfigFunc = previousConfig
		newProductionServerAfterStrictAdmissionSupportPreflight = previousNewServer
	})

	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{StateRoot: root, CWD: t.TempDir(), IdleTimeout: -1})
	}()
	select {
	case <-canonicalStarted:
	case err := <-done:
		t.Fatalf("Serve returned before canonical root resolution stalled: %v", err)
	case <-time.After(time.Second):
		t.Fatal("canonical root resolution did not start")
	}
	cancel()
	close(releaseCanonical)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve error = %v, want clean cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after cancellation")
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 0 {
		t.Fatalf("readiness frame = %q, want none", raw)
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
		failOrSkipAgentbusServeBindDenied(t, "startup deadline launcher", err)
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

func shortAgentbusServeTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "agentbus-serve-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
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
	switch os.Getenv(agentbusServeHelperModeEnv) {
	case "preflight-stall-signal":
		return runAgentbusServePreflightStallSignalHelper(cwd)
	case "joined-fail-stop-canceled-report":
		return runAgentbusServeJoinedFailStopCanceledReportHelper(cwd)
	case "authority-fail-stop-canceled-report":
		return runAgentbusServeAuthorityFailStopCanceledReportHelper(cwd)
	case "root-busy-report":
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

func runAgentbusServePreflightStallSignalHelper(cwd string) int {
	productionServedConfigFunc = func(cfg Config) (served.Config, error) {
		cfg.Runtime = custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)
		return cfg, nil
	}
	newProductionServerAfterStrictAdmissionSupportPreflight = func(ctx context.Context, _ served.Config) (servedServer, error) {
		if marker := os.Getenv(agentbusServeHelperMarkEnv); marker != "" {
			if err := os.WriteFile(marker, []byte("started\n"), 0o600); err != nil {
				return nil, err
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	if err := Serve(ctx, Config{
		StateRoot:   os.Getenv("AGENTBUS_STATE_ROOT"),
		CWD:         cwd,
		IdleTimeout: -1,
	}); err != nil {
		return 1
	}
	return 0
}

func runAgentbusServeJoinedFailStopCanceledReportHelper(cwd string) int {
	productionServedConfigFunc = func(cfg Config) (served.Config, error) {
		cfg.Runtime = custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)
		return cfg, nil
	}
	newProductionServerAfterStrictAdmissionSupportPreflight = func(context.Context, served.Config) (servedServer, error) {
		return &fakeServedServer{
			shutdownTimeout: time.Second,
			serve: func(context.Context, context.Context) error {
				return errors.Join(served.SafetyFailStopError{Reason: context.Canceled}, context.Canceled)
			},
			shutdown: func(context.Context) error {
				return served.ErrShutdownNotServing
			},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, Config{
		StateRoot:   os.Getenv("AGENTBUS_STATE_ROOT"),
		CWD:         cwd,
		IdleTimeout: -1,
	}); err != nil {
		return 1
	}
	return 0
}

func runAgentbusServeAuthorityFailStopCanceledReportHelper(cwd string) int {
	productionServedConfigFunc = func(cfg Config) (served.Config, error) {
		cfg.Runtime = custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)
		return cfg, nil
	}
	newProductionServerAfterStrictAdmissionSupportPreflight = func(context.Context, served.Config) (servedServer, error) {
		return &fakeServedServer{
			shutdownTimeout: time.Second,
			serve: func(context.Context, context.Context) error {
				failStopped := errors.Join(authority.FailStoppedError{Reason: "anchor advance: context canceled"}, context.Canceled)
				return errors.Join(served.SafetyFailStopError{Reason: failStopped}, context.Canceled)
			},
			shutdown: func(context.Context) error {
				return served.ErrShutdownNotServing
			},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Serve(ctx, Config{
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

func realAgentbusServeLaunchOptions(t *testing.T, root string) daemonlaunch.Options {
	t.Helper()
	return daemonlaunch.Options{
		CommandPath: buildAgentbusServeRealBinary(t),
		Args:        []string{"serve", "--foreground"},
		StateRoot:   root,
		Timeout:     20 * time.Second,
		Starter:     agentbusServeProcessStarter,
		Env:         os.Environ(),
	}
}

func failOrSkipAgentbusServeBindDenied(t *testing.T, context string, err error) {
	t.Helper()
	if err == nil || !agentbusServeBindDeniedOutput(err.Error()) {
		return
	}
	if strings.TrimSpace(os.Getenv("AGENTBUS_TEST_SANDBOX_BIND_DENIED")) == "1" {
		t.Skipf("Unix socket bind denied by sandbox in %s (AGENTBUS_TEST_SANDBOX_BIND_DENIED=1): %v", context, err)
	}
	t.Fatalf("Unix socket bind denied in %s without AGENTBUS_TEST_SANDBOX_BIND_DENIED=1; failing to expose daemon bind regressions: %v", context, err)
}

func agentbusServeBindDeniedOutput(output string) bool {
	output = strings.ToLower(output)
	return strings.Contains(output, "bind: operation not permitted") ||
		strings.Contains(output, "bind: permission denied") ||
		strings.Contains(output, "unix socket bind denied by sandbox")
}

func assertAgentbusServeReady(t *testing.T, root string, result daemonlaunch.Result) {
	t.Helper()
	if result.PID <= 0 {
		t.Fatalf("daemon pid = %d, want positive", result.PID)
	}
	assertAgentbusServeProcessAlive(t, result.PID, "after readiness")
	clientCtx, cancelClient := context.WithTimeout(context.Background(), time.Second)
	defer cancelClient()
	client, err := busclient.Connect(clientCtx, busclient.Options{
		StateRoot:        root,
		DisableAutoStart: true,
	})
	if err != nil {
		t.Fatalf("connect after readiness: %v", err)
	}
	defer client.Close()
	// Connect already performed the protocol.hello handshake (and rejects a
	// version mismatch typed, per the ADR-12 client contract); a second explicit
	// Hello on the same connection is a protocol error. Assert the cached result.
	hello := client.HelloResult()
	if hello.ProtocolVersion != protocol.Version {
		t.Fatalf("hello protocolVersion = %d, want %d", hello.ProtocolVersion, protocol.Version)
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
