//go:build darwin || linux

package custodian

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const (
	nativeHelperEnv             = "AGENTBUS_NATIVE_CUSTODIAN_HELPER"
	nativeHelperSimple          = "simple"
	nativeHelperTermGrandchild  = "term-grandchild"
	nativeHelperGrandchild      = "grandchild"
	nativeHelperMonitor         = "monitor"
	nativeHelperAgentbusGOCACHE = "GOCACHE=/tmp/abd-gocache"
	nativeHelperAgentbusMOD     = "GOMODCACHE=/tmp/abd-gomodcache"
)

var (
	nativeAgentbusBuildOnce sync.Once
	nativeAgentbusBuildPath string
	nativeAgentbusBuildErr  error
)

func TestNativeHelperSimpleProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperSimple {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeSimpleHelper(args))
}

func TestNativeHelperTermGrandchildProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperTermGrandchild {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeTermGrandchildHelper(args))
}

func TestNativeHelperGrandchildProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperGrandchild {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeGrandchildHelper(args))
}

func TestNativeHelperMonitorProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperMonitor {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeMonitorHelper(args))
}

func TestNativeCustodianLaunchesParkedBackendAndObservesExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)

	stdout, err := io.ReadAll(running.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if !strings.Contains(string(stdout), "native-simple") {
		t.Fatalf("stdout = %q, want native-simple marker", stdout)
	}
	exit, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean exit", exit)
	}
	result := readNativeBackendResult(t, resultPath)
	if result.PID != running.Ref().Leader.PID {
		t.Fatalf("backend pid = %d, want stable leader pid %d", result.PID, running.Ref().Leader.PID)
	}
	if result.PGID != running.Ref().PGID {
		t.Fatalf("backend pgid = %d, want group %d", result.PGID, running.Ref().PGID)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeContainAndVerifyKillsTermIgnoringGrandchild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}

	outcome := running.ContainAndVerify(ctx)
	if !outcome.Absent() {
		onlyLeader, onlyLeaderErr := groupHasNoMembersExceptLeader(running.Ref())
		t.Fatalf("ContainAndVerify() = %+v, want Absent; unreaped=%t onlyLeader=%t onlyLeaderErr=%v members=%s", outcome, running.leader.unreapedFor(running.Ref()), onlyLeader, onlyLeaderErr, debugGroupMembers(running.Ref()))
	}
	if outcome.Method != model.QuiescenceTermKill {
		t.Fatalf("physical method = %s, want term_kill", outcome.Method)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeZombieOnlyGroupIsUnprovableUntilOwnerReaps(t *testing.T) {
	params := defaultNativeTestParams()
	params.PollTimeout = 100 * time.Millisecond
	params.PollInterval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, params)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	if running.handle.LeaderReaped() {
		t.Fatal("leader was reaped before owner finalization")
	}

	outcome := containPhysical(ctx, running.Ref(), params, running.leader)
	if outcome.Absent() {
		t.Fatalf("containPhysical zombie-only outcome = %+v, want Unprovable before owner reap", outcome)
	}
	final := running.ContainAndVerify(ctx)
	if !final.Absent() {
		t.Fatalf("ContainAndVerify() after owner reap = %+v, want Absent", final)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeWaitAndContainShareSerializedFinalization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}

	waitDone := make(chan command.ExitObservation, 1)
	waitErr := make(chan error, 1)
	go func() {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		exit, err := running.Wait(waitCtx)
		waitDone <- exit
		waitErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if running.handle.LeaderReaped() {
		t.Fatal("Wait reaped leader while residual group members remained")
	}
	outcome := running.ContainAndVerify(ctx)
	if !outcome.Absent() {
		t.Fatalf("ContainAndVerify() = %+v, want Absent", outcome)
	}
	exit := <-waitDone
	err = <-waitErr
	if exit.Signal == "" && !exit.Exited {
		t.Fatalf("Wait exit observation = %+v, want cached leader exit", exit)
	}
	if err != nil {
		t.Fatalf("Wait() error = %v, want nil for TERM-handled helper", err)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeContainAndVerifyUnprovableWhenLeaderReapedOutOfBand(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}

	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	state, err := running.handle.WaitState()
	if err != nil {
		t.Fatalf("reap leader WaitState() error = %v", err)
	}
	exit := exitObservationForState(state)
	if !exit.Exited {
		t.Fatalf("leader exit observation = %+v, want exited", exit)
	}
	outcome := running.ContainAndVerify(ctx)
	if !outcome.Unprovable() {
		t.Fatalf("ContainAndVerify() = %+v, want Unprovable after reaping leader", outcome)
	}
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d was killed after unprovable outcome: %v", result.GrandchildPID, err)
	}
}

func TestNativeMonitorDaemonEOFContainsTermIgnoringGrandchild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}

	if err := running.handle.Monitor.DaemonControlWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	running.handle.Monitor.DaemonControlWrite = nil
	select {
	case <-running.handle.Monitor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not exit after daemon EOF")
	}
	_ = running.handle.Monitor.Wait()
	waitPIDAbsent(t, result.GrandchildPID, 5*time.Second)
	final := running.ContainAndVerify(ctx)
	if !final.Absent() {
		t.Fatalf("final ContainAndVerify() = %+v, want Absent after monitor containment", final)
	}
}

func TestNativePreReleaseHandleFailureAbortsUnreleasedWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	native.options.newLeaderRetention = func(model.GroupRef) (*leaderRetention, error) {
		return nil, errors.New("injected leader handle failure")
	}
	spec, resultPath := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err == nil {
		cleanupNativeRunning(t, running)
		t.Fatal("Launch() succeeded, want pre-release handle failure")
	}
	if !strings.Contains(err.Error(), "injected leader handle failure") {
		t.Fatalf("Launch() error = %v, want injected handle failure", err)
	}
	if _, statErr := os.Stat(resultPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("backend result stat = %v, want not exist because release never happened", statErr)
	}
}

func TestNativeCanceledWaitsDoNotLeakReapers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	_ = readNativeBackendResult(t, resultPath)
	before := runtime.NumGoroutine()
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	for i := 0; i < 20; i++ {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _ = running.Wait(waitCtx)
		waitCancel()
	}
	time.Sleep(100 * time.Millisecond)
	if running.handle.LeaderReaped() {
		t.Fatal("canceled Wait reaped leader later")
	}
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("goroutines before=%d after=%d, want no repeated wait leak", before, after)
	}
	outcome := running.ContainAndVerify(ctx)
	if !outcome.Absent() {
		t.Fatalf("ContainAndVerify() = %+v, want Absent after canceled waits", outcome)
	}
}

func TestNativeContainAndVerifyUnprovableWhenBoundsExpire(t *testing.T) {
	params := defaultNativeTestParams()
	params.GracePeriod = 200 * time.Millisecond
	native := newNativeCustodianForTest(t, params)
	spec, _ := nativeTermGrandchildLaunchSpec(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")

	containCtx, containCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer containCancel()
	outcome := running.ContainAndVerify(containCtx)
	if !outcome.Unprovable() {
		t.Fatalf("ContainAndVerify() = %+v, want Unprovable when bounds expire", outcome)
	}
}

func TestNativeCustodianDoesNotMintProofAndProductionUnavailable(t *testing.T) {
	nativeType := fmt.Sprintf("%T", PhysicalOutcome{})
	if strings.Contains(nativeType, "VerifiedQuiescence") {
		t.Fatalf("physical outcome type = %s, must not be VerifiedQuiescence", nativeType)
	}
	assertNativeSourceDoesNotMintProof(t)

	runtime := NewUnavailableRuntime(ErrSupervisorUnavailable)
	if _, ok := runtime.Process().(UnavailableCustodian); !ok {
		t.Fatalf("production runtime process = %T, want UnavailableCustodian", runtime.Process())
	}
	if runtime.Support().VerifiedContainment || runtime.Support().AdvertisedAvailable() {
		t.Fatalf("production runtime support = %+v, want unavailable", runtime.Support())
	}
}

func TestNewNativeRuntimeProbeExercisesContainmentButDoesNotAdvertise(t *testing.T) {
	exe := nativeTestBinaryPath(t)
	native, support, err := NewNativeRuntime(NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    nativeMonitorCommand(t),
		ContainmentParams: defaultNativeTestParams(),
		WorkerEnv:         append(os.Environ(), nativeHelperAgentbusGOCACHE, nativeHelperAgentbusMOD),
		WorkerDir:         filepath.Dir(exe),
	})
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}
	if native == nil {
		t.Fatal("NewNativeRuntime() native custodian is nil")
	}
	if !support.RuntimeProbePassed || !support.VerifiedContainment || support.RuntimeProbeResult != nil {
		t.Fatalf("native runtime support = %+v, want passed containment probe", support)
	}
	if support.FeatureConfigured || support.FeatureAdvertised || support.AdvertisedAvailable() {
		t.Fatalf("native runtime support = %+v, want capability off/not advertised", support)
	}
}

type nativeBackendResult struct {
	PID            int `json:"pid"`
	PGID           int `json:"pgid"`
	GrandchildPID  int `json:"grandchildPid,omitempty"`
	GrandchildPGID int `json:"grandchildPgid,omitempty"`
}

func newNativeCustodianForTest(t *testing.T, params containment.Params) *NativeCustodian {
	t.Helper()
	exe := nativeTestBinaryPath(t)
	native, err := NewNativeCustodian(NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    nativeMonitorCommand(t),
		ContainmentParams: params,
		WorkerEnv:         append(os.Environ(), nativeHelperAgentbusGOCACHE, nativeHelperAgentbusMOD),
		WorkerDir:         filepath.Dir(exe),
	})
	if err != nil {
		t.Fatalf("NewNativeCustodian() error = %v", err)
	}
	return native
}

func defaultNativeTestParams() containment.Params {
	return containment.Params{
		GracePeriod:                100 * time.Millisecond,
		PollInterval:               20 * time.Millisecond,
		PollTimeout:                3 * time.Second,
		TrustedMonitorWait:         100 * time.Millisecond,
		TrustedMonitorPollInterval: 20 * time.Millisecond,
	}
}

func nativeSimpleLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "simple-result.json")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperSimpleProcess$",
			"--",
			"--result", resultPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperSimple),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeTermGrandchildLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "term-grandchild-result.json")
	readyPath := filepath.Join(dir, "grandchild-ready")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperTermGrandchildProcess$",
			"--",
			"--result", resultPath,
			"--grandchild-ready", readyPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperTermGrandchild),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeLaunchSpec(t *testing.T, exec command.ExecSpec) NativeLaunchSpec {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	attempt := model.AttemptRef{JobID: model.JobID("job-native-" + suffix), AttemptID: model.AttemptID("attempt-native-" + suffix), Epoch: 1}
	launchKey := model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne}
	return NativeLaunchSpec{
		Exec:      exec,
		CustodyID: model.CustodyID("custody-native-" + suffix),
		LaunchKey: launchKey,
		LogicalGrant: model.LaunchGrant{
			Attempt:   attempt,
			Ordinal:   model.LaunchOrdinalOne,
			Nonce:     model.LaunchNonce("nonce-native-" + suffix),
			GrantedBy: model.BootRef{BootID: model.BootID("boot-native-" + suffix), OwnerID: model.OwnerID("owner-native-" + suffix)},
		},
		ReleaseSecret: model.ReleaseSecret("release-native-" + suffix),
	}
}

func nativeMonitorCommand(t *testing.T) parklaunch.CommandSpec {
	t.Helper()
	exe := nativeTestBinaryPath(t)
	return parklaunch.CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"-test.run=^TestNativeHelperMonitorProcess$",
			"--",
			"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
			"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
			"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperMonitor),
		Dir: filepath.Dir(exe),
	}
}

func builtNativeAgentbusPath(t *testing.T) string {
	t.Helper()
	nativeAgentbusBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "agentbus-native-custodian-bin-")
		if err != nil {
			nativeAgentbusBuildErr = err
			return
		}
		nativeAgentbusBuildPath = filepath.Join(dir, "agentbus")
		cmd := exec.Command("go", "build", "-o", nativeAgentbusBuildPath, "./cmd/agentbus")
		cmd.Dir = nativeRepoRootFromCaller()
		cmd.Env = append(os.Environ(), nativeHelperAgentbusGOCACHE, nativeHelperAgentbusMOD)
		output, err := cmd.CombinedOutput()
		if err != nil {
			nativeAgentbusBuildErr = fmt.Errorf("go build ./cmd/agentbus: %w\n%s", err, output)
		}
	})
	if nativeAgentbusBuildErr != nil {
		t.Fatal(nativeAgentbusBuildErr)
	}
	return nativeAgentbusBuildPath
}

func nativeRepoRootFromCaller() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func nativeTestBinaryPath(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

func waitNativeReadyLine(t *testing.T, reader io.Reader, want string) {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(reader).ReadString('\n')
		done <- line
	}()
	select {
	case line := <-done:
		if !strings.Contains(line, want) {
			t.Fatalf("ready line = %q, want %q", line, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", want)
	}
}

func readNativeBackendResult(t *testing.T, path string) nativeBackendResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var result nativeBackendResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for result %s: %v", path, lastErr)
	return nativeBackendResult{}
}

func cleanupNativeRunning(t *testing.T, running *NativeRunningProcess) {
	t.Helper()
	if running == nil {
		return
	}
	group := running.Ref()
	if group.PGID > 1 {
		err := unix.Kill(-group.PGID, unix.SIGKILL)
		if err != nil && !errors.Is(err, unix.ESRCH) {
			t.Fatalf("cleanup kill group %d: %v", group.PGID, err)
		}
	}
	if running.handle != nil && !running.handle.LeaderReaped() {
		_, _ = running.handle.WaitState()
	}
	waitGroupAbsent(t, group, 5*time.Second)
	if running.handle != nil && running.handle.Monitor != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.handle.Monitor.Stop(ctx); err != nil && !strings.Contains(err.Error(), "signal: killed") {
			t.Fatalf("cleanup monitor stop: %v", err)
		}
	}
	if running.leader != nil {
		_ = running.leader.close()
	}
}

func waitGroupAbsent(t *testing.T, group model.GroupRef, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last model.GroupExistenceObservation
	for {
		claim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
		if err != nil {
			t.Fatal(err)
		}
		last = procgroup.ClassifyGroup(claim)
		if last == model.GroupAbsent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %d observation = %s after %s, want absent", group.PGID, last, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitPIDAbsent(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := procgroup.ReadProcessClaim(pid)
		if errors.Is(err, procgroup.ErrProcessMissing) {
			return
		}
		if err != nil && time.Now().After(deadline) {
			t.Fatalf("pid %d read error after %s: %v", pid, timeout, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still exists after %s", pid, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertNativeSourceDoesNotMintProof(t *testing.T) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "native") || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, forbidden := range []string{"AttestQuiescence(", "VerifiedQuiescence", "QuiescenceCertificate"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains proof minting token %q", name, forbidden)
			}
		}
	}
}

func nativeHelperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" && i+1 <= len(os.Args) {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func runNativeSimpleHelper(args []string) int {
	fs := flag.NewFlagSet("native-simple", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" {
		return 2
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	fmt.Printf("native-simple pid=%d pgid=%d\n", os.Getpid(), pgid)
	return writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid})
}

func runNativeTermGrandchildHelper(args []string) int {
	fs := flag.NewFlagSet("native-term-grandchild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	grandchildReady := fs.String("grandchild-ready", "", "grandchild ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" || *grandchildReady == "" {
		return 2
	}
	exe, err := os.Executable()
	if err != nil {
		return 3
	}
	cmd := exec.Command(exe,
		"-test.run=^TestNativeHelperGrandchildProcess$",
		"--",
		"--ready", *grandchildReady,
	)
	cmd.Env = append(os.Environ(), nativeHelperEnv+"="+nativeHelperGrandchild)
	cmd.Dir = filepath.Dir(exe)
	if err := cmd.Start(); err != nil {
		return 4
	}
	if err := waitForFile(*grandchildReady, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return 5
	}
	childPGID, err := readGrandchildPGID(*grandchildReady)
	if err != nil {
		_ = cmd.Process.Kill()
		return 6
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		_ = cmd.Process.Kill()
		return 7
	}
	if code := writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid, GrandchildPID: cmd.Process.Pid, GrandchildPGID: childPGID}); code != 0 {
		_ = cmd.Process.Kill()
		return code
	}
	fmt.Printf("term-grandchild-ready pid=%d pgid=%d child=%d\n", os.Getpid(), pgid, cmd.Process.Pid)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	return 0
}

func runNativeGrandchildHelper(args []string) int {
	fs := flag.NewFlagSet("native-grandchild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	readyPath := fs.String("ready", "", "ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *readyPath == "" {
		return 2
	}
	signal.Ignore(syscall.SIGTERM)
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	if err := os.WriteFile(*readyPath, []byte(strconv.Itoa(pgid)+"\n"), 0o600); err != nil {
		return 3
	}
	for {
		time.Sleep(time.Hour)
	}
}

func runNativeMonitorHelper(args []string) int {
	fs := flag.NewFlagSet("native-monitor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	daemonFD := fs.Int("daemon-fd", -1, "daemon fd")
	targetFD := fs.Int("target-fd", -1, "target fd")
	readyFD := fs.Int("ready-fd", -1, "ready fd")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *daemonFD < 3 || *targetFD < 3 || *readyFD < 3 {
		return 2
	}
	err := parklaunch.RunMonitorFromFDs(context.Background(), *daemonFD, *targetFD, *readyFD, RealContainment{Params: defaultNativeTestParams()})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0
}

func writeNativeBackendResult(path string, result nativeBackendResult) int {
	raw, err := json.Marshal(result)
	if err != nil {
		return 7
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return 8
	}
	return 0
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", path)
}

func readGrandchildPGID(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pgid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, err
	}
	if pgid <= 0 {
		return 0, fmt.Errorf("grandchild pgid must be positive")
	}
	return pgid, nil
}
