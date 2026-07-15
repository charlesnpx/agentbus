//go:build darwin || linux

package parklaunch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const (
	parklaunchHelperEnv     = "AGENTBUS_PARKLAUNCH_HELPER"
	parklaunchBackendMode   = "backend"
	parklaunchMonitorMode   = "monitor"
	parklaunchMonitorNoAck  = "monitor-no-ack"
	parklaunchFDScanMode    = "fd-scan"
	parklaunchFDHoldMode    = "fd-hold"
	parklaunchMonitorFDMode = "monitor-fd"
)

var (
	agentbusBuildOnce sync.Once
	agentbusBuildPath string
	agentbusBuildErr  error
)

func TestLaunchHappyPathReleasesBackendWithStablePIDAndEOF(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	handle, err := Launch(fixture.ctx, fixture.spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer stopMonitor(t, handle.Monitor)

	stdout, err := io.ReadAll(handle.Stdout)
	if err != nil {
		t.Fatalf("read backend stdout: %v", err)
	}
	if !strings.Contains(string(stdout), "parklaunch-backend") {
		t.Fatalf("backend stdout = %q", stdout)
	}
	if err := handle.Wait(); err != nil {
		t.Fatalf("backend wait error = %v", err)
	}
	result := readBackendResult(t, fixture.backend.ResultPath)
	if result.PID != handle.GroupRef.Leader.PID {
		t.Fatalf("backend pid = %d, want stable worker pid %d", result.PID, handle.GroupRef.Leader.PID)
	}
	if result.PGID != handle.GroupRef.PGID {
		t.Fatalf("backend pgid = %d, want target group %d", result.PGID, handle.GroupRef.PGID)
	}
	if handle.GroupRef.RetainedID != "" {
		t.Fatalf("retained id = %q, want empty v1 retained id", handle.GroupRef.RetainedID)
	}
	if handle.GroupRef.Monitor.PID == handle.GroupRef.PGID {
		t.Fatalf("monitor pid %d is target group leader", handle.GroupRef.Monitor.PID)
	}
	if len(result.OpenFDs) != 0 {
		t.Fatalf("backend inherited unexpected fds: %v", result.OpenFDs)
	}
	if got := fixture.containment.CallCount(); got != 0 {
		t.Fatalf("parent containment calls = %d, want 0", got)
	}
	assertFileAbsent(t, fixture.monitorMarker)
}

func TestLaunchIdentityMismatchFailsClosed(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	fixture.spec.identity = mismatchClassifyingReader{identityReader: nativeIdentityReader{}}

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err == nil {
		stopMonitor(t, handle.Monitor)
		t.Fatal("Launch() succeeded; want identity mismatch")
	}
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("Launch() error = %v, want ErrIdentityMismatch", err)
	}
	if got := fixture.containment.CallCount(); got != 1 {
		t.Fatalf("containment calls = %d, want 1", got)
	}
	assertFileAbsent(t, fixture.backend.MarkerPath)
	fixture.containment.WaitAbsent(t)
}

func TestLaunchReleaseIsOneUse(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	handle, err := Launch(fixture.ctx, fixture.spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer stopMonitor(t, handle.Monitor)

	if err := handle.Release(fixture.ctx); !errors.Is(err, ErrReleaseAlreadySent) {
		t.Fatalf("second Release() error = %v, want ErrReleaseAlreadySent", err)
	}
	_, _ = io.ReadAll(handle.Stdout)
	if err := handle.Wait(); err != nil {
		t.Fatalf("backend wait error = %v", err)
	}
	result := readBackendResult(t, fixture.backend.ResultPath)
	if result.PID != handle.GroupRef.Leader.PID {
		t.Fatalf("backend pid = %d, want stable worker pid %d", result.PID, handle.GroupRef.Leader.PID)
	}
	if got := fixture.containment.CallCount(); got != 0 {
		t.Fatalf("parent containment calls = %d, want 0", got)
	}
}

func TestLaunchChannelLossBeforeReleaseContainsTarget(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	fixture.spec.hooks.beforeRelease = func(snapshot launchControlSnapshot) error {
		return snapshot.ControlWrite.Close()
	}

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err == nil {
		stopMonitor(t, handle.Monitor)
		t.Fatal("Launch() succeeded; want channel-loss failure")
	}
	if !errors.Is(err, ErrChannelLostBeforeRelease) {
		t.Fatalf("Launch() error = %v, want ErrChannelLostBeforeRelease", err)
	}
	if got := fixture.containment.CallCount(); got != 1 {
		t.Fatalf("containment calls = %d, want 1", got)
	}
	assertFileAbsent(t, fixture.backend.MarkerPath)
	fixture.containment.WaitAbsent(t)
}

func TestLaunchAckReadFailureAfterReleaseLetsArmedMonitorContainWhenParentContainmentFails(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}, Hold: 5 * time.Second})
	fixture.spec.Monitor.Command = monitorCommandWithKill(t, fixture.monitorMarker, true)
	parentContainment := &failingContainment{err: errors.New("synthetic parent containment failure")}
	fixture.spec.Containment = parentContainment
	fixture.spec.hooks.afterMonitorStarted = func(monitor *MonitorProcess) error {
		parentContainment.daemonClosed = func() bool {
			return monitor.DaemonControlWrite == nil
		}
		return nil
	}
	fixture.spec.hooks.afterRelease = func(snapshot launchControlSnapshot) error {
		waitBackendResult(t, fixture.backend.ResultPath)
		return snapshot.ControlRead.Close()
	}

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err == nil {
		cleanupParkedHandle(t, handle)
		t.Fatal("Launch() succeeded; want release-ack failure")
	}
	if !errors.Is(err, ErrReleaseAck) {
		t.Fatalf("Launch() error = %v, want ErrReleaseAck", err)
	}
	if !strings.Contains(err.Error(), "synthetic parent containment failure") {
		t.Fatalf("Launch() error = %v, want parent containment failure joined", err)
	}
	calls := parentContainment.Calls()
	if len(calls) != 1 {
		t.Fatalf("parent containment calls = %d, want 1", len(calls))
	}
	if !calls[0].daemonControlClosed {
		t.Fatal("parent containment ran before daemon-control writer was closed")
	}
	lines := readLines(t, fixture.monitorMarker)
	if len(lines) != 1 {
		t.Fatalf("monitor containment lines = %d (%v), want 1", len(lines), lines)
	}
	wantLine := fmt.Sprintf("contained pgid=%d leader=%d", calls[0].group.PGID, calls[0].group.Leader.PID)
	if lines[0] != wantLine {
		t.Fatalf("monitor containment line = %q, want %q", lines[0], wantLine)
	}
	if err := waitGroupAbsent(context.Background(), calls[0].group); err != nil {
		t.Fatalf("target group still present after armed monitor containment: %v", err)
	}
}

func TestMonitorDaemonEOFContainsExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	marker := filepath.Join(dir, "containment.log")
	monitor := startTestMonitor(t, ctx, marker)
	claim, err := procgroup.ReadProcessClaim(monitor.PID)
	if err != nil {
		t.Fatalf("read monitor claim: %v", err)
	}
	if claim.PGID != claim.PID {
		t.Fatalf("monitor pgid=%d pid=%d, want own process group", claim.PGID, claim.PID)
	}
	target := syntheticGroupRef(424242, 515151)
	if err := monitor.BindTarget(target); err != nil {
		t.Fatalf("BindTarget() error = %v", err)
	}
	if err := monitor.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady() error = %v", err)
	}

	if err := closeMonitorDaemonControl(monitor); err != nil {
		t.Fatalf("close daemon write: %v", err)
	}
	waitMonitorSuccess(t, monitor)
	lines := readLines(t, marker)
	if len(lines) != 1 {
		t.Fatalf("monitor containment lines = %d (%v), want 1", len(lines), lines)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "containment.log" {
		t.Fatalf("monitor wrote unexpected durable files: %v", dirEntries(entries))
	}
}

func TestLaunchFDOwnership(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: fdRange(3, 32)})
	fdScanResult := filepath.Join(t.TempDir(), "fd-scan.json")
	fixture.spec.hooks.afterPipesCreated = func(snapshot launchPipeSnapshot) error {
		open, err := runFDScanHelper([]int{snapshot.ControlWriteFD, snapshot.BootstrapWriteFD}, fdScanResult)
		if err != nil {
			return err
		}
		if len(open) != 0 {
			return fmt.Errorf("unrelated child inherited daemon-side fds: %v", open)
		}
		return nil
	}
	handle, err := Launch(fixture.ctx, fixture.spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer stopMonitor(t, handle.Monitor)
	_, _ = io.ReadAll(handle.Stdout)
	if err := handle.Wait(); err != nil {
		t.Fatalf("backend wait error = %v", err)
	}
	result := readBackendResult(t, fixture.backend.ResultPath)
	if len(result.OpenFDs) != 0 {
		t.Fatalf("backend inherited non-stdio fds: %v", result.OpenFDs)
	}
}

func TestLaunchStartupContextCancelAfterReturnDoesNotKillBackendOrMonitor(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}, Hold: 5 * time.Second})
	handle, err := Launch(fixture.ctx, fixture.spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupParkedHandle(t, handle)

	result := waitBackendResult(t, fixture.backend.ResultPath)
	fixture.cancel()
	time.Sleep(200 * time.Millisecond)
	assertProcessMatches(t, result.PID, handle.GroupRef.Leader.HighResStartToken)
	assertProcessMatches(t, handle.Monitor.PID, handle.GroupRef.Monitor.HighResStartToken)
	if got := fixture.containment.CallCount(); got != 0 {
		t.Fatalf("parent containment calls after startup cancel = %d, want 0", got)
	}

	if err := closeMonitorDaemonControl(handle.Monitor); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	waitMonitorSuccess(t, handle.Monitor)
	lines := readLines(t, fixture.monitorMarker)
	if len(lines) != 1 {
		t.Fatalf("monitor containment lines = %d (%v), want 1", len(lines), lines)
	}
}

func TestLaunchMonitorExitBeforeReadyFailsClosedWithoutRelease(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	fixture.spec.Monitor.Command = monitorNoAckCommand(t)

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err == nil {
		stopMonitor(t, handle.Monitor)
		t.Fatal("Launch() succeeded; want monitor-not-armed failure")
	}
	if !errors.Is(err, ErrMonitorNotArmed) {
		t.Fatalf("Launch() error = %v, want ErrMonitorNotArmed", err)
	}
	if got := fixture.containment.CallCount(); got != 1 {
		t.Fatalf("containment calls = %d, want 1", got)
	}
	assertFileAbsent(t, fixture.backend.MarkerPath)
	fixture.containment.WaitAbsent(t)
}

func TestLaunchDaemonControlWriterNotInheritedByUnrelatedChild(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	duringLaunchScan := filepath.Join(t.TempDir(), "during-launch-fds.json")
	fixture.spec.hooks.afterMonitorStarted = func(monitor *MonitorProcess) error {
		open, err := runFDScanHelper([]int{int(monitor.DaemonControlWrite.Fd())}, duringLaunchScan)
		if err != nil {
			return err
		}
		if len(open) != 0 {
			return fmt.Errorf("unrelated child inherited daemon-control writer during launch: %v", open)
		}
		return nil
	}

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupParkedHandle(t, handle)

	afterLaunchScan := filepath.Join(t.TempDir(), "after-launch-fds.json")
	startFDHoldHelper(t, []int{int(handle.Monitor.DaemonControlWrite.Fd())}, afterLaunchScan, 5*time.Second)
	if open := waitOpenFDResult(t, afterLaunchScan); len(open) != 0 {
		t.Fatalf("unrelated child inherited daemon-control writer after launch: %v", open)
	}
	if err := closeMonitorDaemonControl(handle.Monitor); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	waitMonitorSuccess(t, handle.Monitor)
}

func TestLaunchPreIdentityFailureTerminatesAndReapsWorker(t *testing.T) {
	fixture := newLaunchFixture(t, backendFixtureOptions{ClosedFDs: []int{3, 4, 5}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fixture.ctx = ctx
	workerPID := 0
	fixture.spec.hooks.afterWorkerStarted = func(pid int) error {
		workerPID = pid
		return nil
	}
	fixture.spec.identity = workerOnlyIdentityReader{identityReader: nativeIdentityReader{}, workerPID: &workerPID}

	handle, err := Launch(fixture.ctx, fixture.spec)
	if err == nil {
		stopMonitor(t, handle.Monitor)
		t.Fatal("Launch() succeeded; want pre-identity failure")
	}
	if workerPID == 0 {
		t.Fatal("worker pid was not recorded")
	}
	if got := fixture.containment.CallCount(); got != 0 {
		t.Fatalf("containment calls = %d, want 0 before valid GroupRef", got)
	}
	waitPIDAbsent(t, workerPID)
	assertFileAbsent(t, fixture.backend.MarkerPath)
}

func TestStartMonitorProcessStartFailureDoesNotLeakFDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	missing := filepath.Join(t.TempDir(), "missing-monitor")
	before := openFDSet(fdRange(3, 256))
	for i := 0; i < 25; i++ {
		monitor, err := StartMonitorProcess(ctx, MonitorProcessSpec{
			Command: CommandSpec{Path: missing},
		})
		if err == nil {
			stopMonitor(t, monitor)
			t.Fatal("StartMonitorProcess() succeeded; want start failure")
		}
	}
	after := openFDSet(fdRange(3, 256))
	if extra := fdSetDifference(after, before); len(extra) != 0 {
		t.Fatalf("monitor start failure leaked fds: %v", extra)
	}
}

func TestParklaunchBackendHelperProcess(t *testing.T) {
	if os.Getenv(parklaunchHelperEnv) != parklaunchBackendMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runBackendHelper(args))
}

func TestParklaunchMonitorHelperProcess(t *testing.T) {
	if os.Getenv(parklaunchHelperEnv) != parklaunchMonitorMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runMonitorHelper(args))
}

func TestParklaunchMonitorNoAckHelperProcess(t *testing.T) {
	if os.Getenv(parklaunchHelperEnv) != parklaunchMonitorNoAck {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runMonitorNoAckHelper(args))
}

func TestParklaunchFDScanHelperProcess(t *testing.T) {
	if os.Getenv(parklaunchHelperEnv) != parklaunchFDScanMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runFDScanHelperProcess(args))
}

func TestParklaunchFDHoldHelperProcess(t *testing.T) {
	if os.Getenv(parklaunchHelperEnv) != parklaunchFDHoldMode {
		return
	}
	args, ok := helperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runFDHoldHelperProcess(args))
}

type launchFixture struct {
	ctx           context.Context
	cancel        context.CancelFunc
	spec          Spec
	backend       backendFixtureSpec
	containment   *recordingContainment
	monitorMarker string
}

type backendFixtureOptions struct {
	ClosedFDs []int
	Hold      time.Duration
}

type backendFixtureSpec struct {
	parkproto.ExecSpec
	MarkerPath string
	ResultPath string
}

type backendResult struct {
	PID     int   `json:"pid"`
	PGID    int   `json:"pgid"`
	OpenFDs []int `json:"openFds"`
}

func newLaunchFixture(t *testing.T, backendOpts backendFixtureOptions) launchFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	backend := newBackendFixture(t, dir, backendOpts)
	containment := &recordingContainment{kill: true}
	attempt := model.AttemptRef{JobID: "job-parklaunch", AttemptID: "attempt-1", Epoch: 1}
	launchKey := model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne}
	boot := model.BootRef{BootID: "boot-parklaunch", OwnerID: "owner-parklaunch"}
	monitorMarker := filepath.Join(dir, "monitor-containment.log")
	spec := Spec{
		AgentbusPath:  builtAgentbusPath(t),
		ExecSpec:      backend.ExecSpec,
		CustodyID:     "custody-parklaunch",
		LaunchKey:     launchKey,
		LogicalGrant:  model.LaunchGrant{Attempt: attempt, Ordinal: model.LaunchOrdinalOne, Nonce: "nonce-parklaunch", GrantedBy: boot},
		ReleaseSecret: "release-secret-parklaunch",
		Containment:   containment,
		Monitor: &MonitorProcessSpec{
			Command: monitorCommand(t, monitorMarker),
		},
	}
	return launchFixture{
		ctx:           ctx,
		cancel:        cancel,
		spec:          spec,
		backend:       backend,
		containment:   containment,
		monitorMarker: monitorMarker,
	}
}

func newBackendFixture(t *testing.T, dir string, opts backendFixtureOptions) backendFixtureSpec {
	t.Helper()
	exe := testBinaryPath(t)
	marker := filepath.Join(dir, "backend-started")
	result := filepath.Join(dir, "backend-result.json")
	closed := make([]string, 0, len(opts.ClosedFDs))
	for _, fd := range opts.ClosedFDs {
		closed = append(closed, strconv.Itoa(fd))
	}
	holdMillis := "0"
	if opts.Hold > 0 {
		holdMillis = strconv.FormatInt(opts.Hold.Milliseconds(), 10)
	}
	argv := []string{
		exe,
		"-test.run=TestParklaunchBackendHelperProcess",
		"--",
		"--marker", marker,
		"--result", result,
		"--closed-fds", strings.Join(closed, ","),
		"--hold-ms", holdMillis,
	}
	return backendFixtureSpec{
		ExecSpec: parkproto.ExecSpec{
			Path: exe,
			Argv: argv,
			Env:  []string{parklaunchHelperEnv + "=" + parklaunchBackendMode},
			Dir:  filepath.Dir(exe),
		},
		MarkerPath: marker,
		ResultPath: result,
	}
}

func monitorCommand(t *testing.T, marker string) CommandSpec {
	t.Helper()
	return monitorCommandWithKill(t, marker, false)
}

func monitorCommandWithKill(t *testing.T, marker string, kill bool) CommandSpec {
	t.Helper()
	exe := testBinaryPath(t)
	return CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"-test.run=TestParklaunchMonitorHelperProcess",
			"--",
			"--daemon-fd", strconv.Itoa(MonitorDaemonControlFD),
			"--target-fd", strconv.Itoa(MonitorTargetFD),
			"--ready-fd", strconv.Itoa(MonitorReadyFD),
			"--marker", marker,
			"--kill", strconv.FormatBool(kill),
		},
		Env: append(os.Environ(), parklaunchHelperEnv+"="+parklaunchMonitorMode),
		Dir: filepath.Dir(exe),
	}
}

func monitorNoAckCommand(t *testing.T) CommandSpec {
	t.Helper()
	exe := testBinaryPath(t)
	return CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"-test.run=TestParklaunchMonitorNoAckHelperProcess",
			"--",
			"--target-fd", strconv.Itoa(MonitorTargetFD),
		},
		Env: append(os.Environ(), parklaunchHelperEnv+"="+parklaunchMonitorNoAck),
		Dir: filepath.Dir(exe),
	}
}

func startTestMonitor(t *testing.T, ctx context.Context, marker string) *MonitorProcess {
	t.Helper()
	monitor, err := StartMonitorProcess(ctx, MonitorProcessSpec{
		Command: monitorCommand(t, marker),
	})
	if err != nil {
		t.Fatalf("StartMonitorProcess() error = %v", err)
	}
	t.Cleanup(func() {
		stopMonitor(t, monitor)
	})
	return monitor
}

func builtAgentbusPath(t *testing.T) string {
	t.Helper()
	agentbusBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "agentbus-parklaunch-bin-")
		if err != nil {
			agentbusBuildErr = err
			return
		}
		agentbusBuildPath = filepath.Join(dir, "agentbus")
		if runtime.GOOS == "windows" {
			agentbusBuildPath += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", agentbusBuildPath, "./cmd/agentbus")
		cmd.Dir = repoRootFromCaller()
		cmd.Env = os.Environ()
		output, err := cmd.CombinedOutput()
		if err != nil {
			agentbusBuildErr = fmt.Errorf("go build ./cmd/agentbus: %w\n%s", err, output)
		}
	})
	if agentbusBuildErr != nil {
		t.Fatal(agentbusBuildErr)
	}
	return agentbusBuildPath
}

func repoRootFromCaller() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func testBinaryPath(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

type recordingContainment struct {
	mu     sync.Mutex
	groups []model.GroupRef
	kill   bool
}

func (containment *recordingContainment) Contain(ctx context.Context, group model.GroupRef) error {
	containment.mu.Lock()
	containment.groups = append(containment.groups, group)
	containment.mu.Unlock()
	if !containment.kill {
		return nil
	}
	err := unix.Kill(-group.PGID, unix.SIGKILL)
	if err != nil && !errors.Is(err, unix.ESRCH) {
		return err
	}
	return waitGroupAbsent(ctx, group)
}

func (containment *recordingContainment) CallCount() int {
	containment.mu.Lock()
	defer containment.mu.Unlock()
	return len(containment.groups)
}

func (containment *recordingContainment) WaitAbsent(t *testing.T) {
	t.Helper()
	containment.mu.Lock()
	groups := append([]model.GroupRef(nil), containment.groups...)
	containment.mu.Unlock()
	for _, group := range groups {
		if err := waitGroupAbsent(context.Background(), group); err != nil {
			t.Fatalf("group %d still present after containment: %v", group.PGID, err)
		}
	}
}

type failingContainment struct {
	mu           sync.Mutex
	err          error
	daemonClosed func() bool
	calls        []failingContainmentCall
}

type failingContainmentCall struct {
	group               model.GroupRef
	daemonControlClosed bool
}

func (containment *failingContainment) Contain(_ context.Context, group model.GroupRef) error {
	daemonControlClosed := false
	if containment.daemonClosed != nil {
		daemonControlClosed = containment.daemonClosed()
	}
	containment.mu.Lock()
	containment.calls = append(containment.calls, failingContainmentCall{
		group:               group,
		daemonControlClosed: daemonControlClosed,
	})
	containment.mu.Unlock()
	if containment.err != nil {
		return containment.err
	}
	return errors.New("synthetic parent containment failure")
}

func (containment *failingContainment) Calls() []failingContainmentCall {
	containment.mu.Lock()
	defer containment.mu.Unlock()
	return append([]failingContainmentCall(nil), containment.calls...)
}

type mismatchClassifyingReader struct {
	identityReader
}

func (reader mismatchClassifyingReader) ClassifyProcess(procgroup.ProcessClaim) model.ProcessIdentityObservation {
	return model.ProcessIdentityReused
}

type workerOnlyIdentityReader struct {
	identityReader
	workerPID *int
}

func (reader workerOnlyIdentityReader) ReadProcessClaim(pid int) (procgroup.ProcessClaim, error) {
	if reader.workerPID != nil && pid == *reader.workerPID {
		return reader.identityReader.ReadProcessClaim(pid)
	}
	return procgroup.ProcessClaim{}, fmt.Errorf("synthetic monitor identity failure for pid %d", pid)
}

type markerContainment struct {
	path string
	kill bool
}

func (containment markerContainment) Contain(ctx context.Context, group model.GroupRef) error {
	line := fmt.Sprintf("contained pgid=%d leader=%d\n", group.PGID, group.Leader.PID)
	file, err := os.OpenFile(containment.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(line); err != nil {
		return err
	}
	if !containment.kill {
		return nil
	}
	err = unix.Kill(-group.PGID, unix.SIGKILL)
	if err != nil && !errors.Is(err, unix.ESRCH) {
		return err
	}
	return waitGroupAbsent(ctx, group)
}

func runBackendHelper(args []string) int {
	fs := flag.NewFlagSet("parklaunch-backend", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	marker := fs.String("marker", "", "marker path")
	result := fs.String("result", "", "result path")
	closedFDs := fs.String("closed-fds", "", "closed fds")
	holdMillis := fs.Int("hold-ms", 0, "hold duration in milliseconds")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *marker == "" || *result == "" {
		return 2
	}
	open := openFDs(parseFDList(*closedFDs))
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	fmt.Printf("parklaunch-backend pid=%d pgid=%d\n", os.Getpid(), pgid)
	if err := os.WriteFile(*marker, []byte("started\n"), 0o600); err != nil {
		return 4
	}
	raw, err := json.Marshal(backendResult{PID: os.Getpid(), PGID: pgid, OpenFDs: open})
	if err != nil {
		return 5
	}
	if err := os.WriteFile(*result, append(raw, '\n'), 0o600); err != nil {
		return 6
	}
	if *holdMillis > 0 {
		time.Sleep(time.Duration(*holdMillis) * time.Millisecond)
	}
	return 0
}

func runMonitorHelper(args []string) int {
	fs := flag.NewFlagSet("parklaunch-monitor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	daemonFD := fs.Int("daemon-fd", -1, "daemon fd")
	targetFD := fs.Int("target-fd", -1, "target fd")
	readyFD := fs.Int("ready-fd", -1, "ready fd")
	marker := fs.String("marker", "", "containment marker")
	kill := fs.Bool("kill", false, "kill target group")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *daemonFD < 3 || *targetFD < 3 || *readyFD < 3 || *marker == "" {
		return 2
	}
	if err := RunMonitorFromFDs(context.Background(), *daemonFD, *targetFD, *readyFD, markerContainment{path: *marker, kill: *kill}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0
}

func runMonitorNoAckHelper(args []string) int {
	fs := flag.NewFlagSet("parklaunch-monitor-no-ack", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	targetFD := fs.Int("target-fd", -1, "target fd")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *targetFD < 3 {
		return 2
	}
	targetFile := os.NewFile(uintptr(*targetFD), "agentbus-parklaunch-monitor-target-no-ack")
	if targetFile == nil {
		return 3
	}
	defer targetFile.Close()
	if _, err := io.ReadAll(targetFile); err != nil {
		return 4
	}
	return 0
}

func runFDScanHelperProcess(args []string) int {
	fs := flag.NewFlagSet("parklaunch-fd-scan", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fdsRaw := fs.String("fds", "", "fds")
	result := fs.String("result", "", "result")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *result == "" {
		return 2
	}
	open := openFDs(parseFDList(*fdsRaw))
	raw, err := json.Marshal(open)
	if err != nil {
		return 3
	}
	if err := os.WriteFile(*result, append(raw, '\n'), 0o600); err != nil {
		return 4
	}
	return 0
}

func runFDHoldHelperProcess(args []string) int {
	fs := flag.NewFlagSet("parklaunch-fd-hold", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fdsRaw := fs.String("fds", "", "fds")
	result := fs.String("result", "", "result")
	holdMillis := fs.Int("hold-ms", 5000, "hold duration in milliseconds")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *result == "" {
		return 2
	}
	open := openFDs(parseFDList(*fdsRaw))
	raw, err := json.Marshal(open)
	if err != nil {
		return 3
	}
	if err := os.WriteFile(*result, append(raw, '\n'), 0o600); err != nil {
		return 4
	}
	if *holdMillis > 0 {
		time.Sleep(time.Duration(*holdMillis) * time.Millisecond)
	}
	return 0
}

func runFDScanHelper(fds []int, result string) ([]int, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	parts := make([]string, 0, len(fds))
	for _, fd := range fds {
		parts = append(parts, strconv.Itoa(fd))
	}
	cmd := exec.Command(exe,
		"-test.run=TestParklaunchFDScanHelperProcess",
		"--",
		"--fds", strings.Join(parts, ","),
		"--result", result,
	)
	cmd.Env = append(os.Environ(), parklaunchHelperEnv+"="+parklaunchFDScanMode)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("fd scan helper: %w\n%s", err, output)
	}
	raw, err := os.ReadFile(result)
	if err != nil {
		return nil, err
	}
	var open []int
	if err := json.Unmarshal(raw, &open); err != nil {
		return nil, err
	}
	return open, nil
}

func startFDHoldHelper(t *testing.T, fds []int, result string, hold time.Duration) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parts := make([]string, 0, len(fds))
	for _, fd := range fds {
		parts = append(parts, strconv.Itoa(fd))
	}
	cmd := exec.Command(exe,
		"-test.run=TestParklaunchFDHoldHelperProcess",
		"--",
		"--fds", strings.Join(parts, ","),
		"--result", result,
		"--hold-ms", strconv.FormatInt(hold.Milliseconds(), 10),
	)
	cmd.Env = append(os.Environ(), parklaunchHelperEnv+"="+parklaunchFDHoldMode)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start fd hold helper: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})
	return cmd
}

func readBackendResult(t *testing.T, path string) backendResult {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result backendResult
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func waitBackendResult(t *testing.T, path string) backendResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			var result backendResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for backend result %s: %v", path, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func cleanupParkedHandle(t *testing.T, handle *ParkedHandle) {
	t.Helper()
	if handle == nil {
		return
	}
	_ = closeMonitorDaemonControl(handle.Monitor)
	stopMonitor(t, handle.Monitor)
	if handle.Stdin != nil {
		_ = handle.Stdin.Close()
	}
	if handle.Stdout != nil {
		_ = handle.Stdout.Close()
	}
	if handle.Stderr != nil {
		_ = handle.Stderr.Close()
	}
	if handle.GroupRef.PGID > 0 {
		err := unix.Kill(-handle.GroupRef.PGID, unix.SIGKILL)
		if err != nil && !errors.Is(err, unix.ESRCH) {
			t.Fatalf("kill backend group %d: %v", handle.GroupRef.PGID, err)
		}
	}
	select {
	case <-handle.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("backend pid %d did not exit", handle.GroupRef.Leader.PID)
	}
}

func stopMonitor(t *testing.T, monitor *MonitorProcess) {
	t.Helper()
	if monitor == nil || monitor.cmd == nil || monitor.cmd.Process == nil {
		return
	}
	if monitor.cmd.ProcessState != nil && monitor.cmd.ProcessState.Exited() {
		return
	}
	select {
	case <-monitor.done:
		return
	default:
	}
	_ = closeMonitorTarget(monitor)
	_ = closeMonitorReady(monitor)
	_ = closeMonitorDaemonControl(monitor)
	_ = monitor.cmd.Process.Kill()
	select {
	case <-monitor.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("monitor pid %d did not exit", monitor.PID)
	}
}

func assertProcessMatches(t *testing.T, pid int, startToken string) {
	t.Helper()
	claim, err := procgroup.ReadProcessClaim(pid)
	if err != nil {
		t.Fatalf("read process %d claim: %v", pid, err)
	}
	if claim.StartToken.String() != startToken {
		t.Fatalf("process %d start token = %s, want %s", pid, claim.StartToken, startToken)
	}
}

func waitPIDAbsent(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := unix.Kill(pid, 0); errors.Is(err, unix.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still exists", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitMonitorSuccess(t *testing.T, monitor *MonitorProcess) {
	t.Helper()
	select {
	case err := <-monitor.done:
		if err != nil {
			t.Fatalf("monitor wait error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("monitor pid %d timed out", monitor.PID)
	}
}

func waitGroupAbsent(ctx context.Context, group model.GroupRef) error {
	claim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
	if err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if observed := procgroup.ClassifyGroup(claim); observed == model.GroupAbsent {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for group %d absence", group.PGID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func syntheticGroupRef(pgid, monitorPID int) model.GroupRef {
	attempt := model.AttemptRef{JobID: "job-monitor", AttemptID: "attempt-monitor", Epoch: 1}
	return model.GroupRef{
		Version:           1,
		CustodyID:         "custody-monitor",
		Launch:            model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		HostBootID:        "boot-monitor",
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader:            model.ProcessIdentity{PID: pgid, HighResStartToken: "start-monitor-leader"},
		Monitor:           model.ProcessIdentity{PID: monitorPID, HighResStartToken: "start-monitor-helper"},
	}
}

func helperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func parseFDList(raw string) []int {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	fds := make([]int, 0, len(parts))
	for _, part := range parts {
		fd, err := strconv.Atoi(part)
		if err == nil {
			fds = append(fds, fd)
		}
	}
	return fds
}

func openFDs(fds []int) []int {
	var open []int
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err == nil {
			open = append(open, fd)
		} else if !errors.Is(err, unix.EBADF) {
			open = append(open, fd)
		}
	}
	return open
}

func openFDSet(fds []int) map[int]struct{} {
	out := make(map[int]struct{})
	for _, fd := range openFDs(fds) {
		out[fd] = struct{}{}
	}
	return out
}

func fdSetDifference(a, b map[int]struct{}) []int {
	var out []int
	for fd := range a {
		if _, ok := b[fd]; !ok {
			out = append(out, fd)
		}
	}
	return out
}

func waitOpenFDResult(t *testing.T, path string) []int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			var open []int
			if err := json.Unmarshal(raw, &open); err != nil {
				t.Fatal(err)
			}
			return open
		}
		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for fd helper result %s: %v", path, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func fdRange(first, last int) []int {
	out := make([]int, 0, last-first+1)
	for fd := first; fd <= last; fd++ {
		out = append(out, fd)
	}
	return out
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func dirEntries(entries []os.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func assertFileAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("file exists unexpectedly: %s", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", path, err)
	}
}
