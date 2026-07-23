package agentbusserve

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
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
		Timeout:     2 * time.Second,
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
