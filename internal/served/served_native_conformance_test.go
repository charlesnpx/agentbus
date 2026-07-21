//go:build darwin || linux

package served

import (
	"bufio"
	"bytes"
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
	goruntime "runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	servedNativeBackendName             = "codex"
	servedNativeFixtureEnv              = "AGENTBUS_SERVED_NATIVE_FIXTURE"
	servedNativeGrandchildEnv           = "AGENTBUS_SERVED_NATIVE_GRANDCHILD"
	servedNativeMonitorEnv              = "AGENTBUS_SERVED_NATIVE_MONITOR"
	servedNativeDaemonEnv               = "AGENTBUS_SERVED_NATIVE_DAEMON"
	servedNativeDaemonStartedPathEnv    = "AGENTBUS_SERVED_NATIVE_STARTED_PATH"
	servedNativeCgroupConformanceEnv    = "AGENTBUS_CGROUP_CONFORMANCE"
	servedNativeOfflineModcacheEnv      = "AGENTBUS_OFFLINE_MODCACHE"
	servedNativeFixtureModeClean        = "clean"
	servedNativeFixtureModeGrandchild   = "grandchild"
	servedNativeFixtureModeHold         = "hold"
	servedNativeAgentbusGOFLAGS         = "GOFLAGS=-mod=mod"
	servedNativeAgentbusGOPROXY         = "GOPROXY=off"
	servedNativeResultText              = "PASS\n\n## Findings\nNone.\n"
	servedNativeConformancePollInterval = 20 * time.Millisecond
)

var (
	servedNativeAgentbusBuildOnce sync.Once
	servedNativeAgentbusBuildPath string
	servedNativeAgentbusBuildErr  error
)

func TestServedNativeConformanceFixtureProcess(t *testing.T) {
	if os.Getenv(servedNativeFixtureEnv) != "1" {
		return
	}
	args, ok := servedNativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runServedNativeFixture(args))
}

func TestServedNativeConformanceGrandchildProcess(t *testing.T) {
	if os.Getenv(servedNativeGrandchildEnv) != "1" {
		return
	}
	args, ok := servedNativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runServedNativeGrandchild(args))
}

func TestServedNativeConformanceMonitorProcess(t *testing.T) {
	if os.Getenv(servedNativeMonitorEnv) != "1" {
		return
	}
	args, ok := servedNativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runServedNativeMonitor(args))
}

func TestServedNativeConformanceDaemonProcess(t *testing.T) {
	if os.Getenv(servedNativeDaemonEnv) != "1" {
		return
	}
	args, ok := servedNativeHelperArgs()
	if !ok {
		t.Fatal("daemon helper args missing")
	}
	fs := flag.NewFlagSet("served-native-daemon", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	root := fs.String("root", "", "state root")
	cwd := fs.String("cwd", "", "daemon cwd")
	startedPath := fs.String("started", "", "post-release marker path")
	jobPath := fs.String("job", "", "submitted job path")
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if *root == "" || *cwd == "" || *startedPath == "" || *jobPath == "" {
		t.Fatalf("root, cwd, started path, and job path are required")
	}

	runtimeBundle := requireServedNativeRuntime(t)
	backend := newServedNativeFixtureBackend(servedNativeFixtureModeHold, filepath.Join(*root, "served-native-fixture"), *startedPath)
	server, err := New(Config{
		StateRoot:   *root,
		CWD:         *cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{backend},
		IdleTimeout: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	enableServedNativeRuntime(server, runtimeBundle)
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-served-native-restart",
		RequestID:    "request-served-native-restart",
		TaskSpec: protocol.TaskSpec{
			Backend: servedNativeBackendName,
			CWD:     *cwd,
			Write:   false,
			Prompt:  servedNativeFixtureModeHold,
		},
	}))
	if outcome.err != nil {
		t.Fatalf("helper job.submit error = %+v", outcome.err)
	}
	submitted, ok := outcome.result.(protocol.JobSubmitResult)
	if !ok || submitted.JobID == "" {
		t.Fatalf("helper job.submit result = %T %+v", outcome.result, outcome.result)
	}
	if err := os.WriteFile(*jobPath, []byte(submitted.JobID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if outcome.after == nil {
		t.Fatal("helper job.submit returned no response-delivery action")
	}
	outcome.after()
	if err := waitServedNativeFile(*startedPath, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestServedNativeConformanceProductionDefaultsUnavailableAndGateOff(t *testing.T) {
	server, _, cwd := newUnstartedTestServer(t, newFakeBackend(servedNativeBackendName))
	if server.jobsRequestIDEnabled {
		t.Fatal("jobsRequestIDEnabled default = true, want false")
	}
	runtime := newServedAdmissionRuntime(server)
	support := runtime.support()
	if support.ParkedExec || support.VerifiedContainment || !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("production runtime support = %+v, want unavailable supervisor", support)
	}
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.jobsRequestIDEnabled {
		t.Fatal("jobsRequestIDEnabled changed to true after default bootstrap")
	}

	outcome := server.handleJobSubmit(context.Background(), mustMarshal(t, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-production-gate-off",
		RequestID:    "request-production-gate-off",
		TaskSpec: protocol.TaskSpec{
			Backend: servedNativeBackendName,
			CWD:     cwd,
			Write:   false,
			Prompt:  servedNativeFixtureModeClean,
		},
	}))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorCapabilityMissing {
		t.Fatalf("identified submit with production gate off = result:%+v err:%+v, want capability_missing", outcome.result, outcome.err)
	}
	if outcome.err.Data.AdmissionCause != protocol.AdmissionRejectUnavailableNativeRuntime {
		t.Fatalf("identified submit admission cause = %q, want %q", outcome.err.Data.AdmissionCause, protocol.AdmissionRejectUnavailableNativeRuntime)
	}
}

func TestServedNativeConformanceIdentifiedFencedHappyPath(t *testing.T) {
	runtimeBundle := requireServedNativeRuntime(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	backend := newServedNativeFixtureBackend(servedNativeFixtureModeClean, filepath.Join(root, "served-native-fixture"), "")
	server := newServedNativeBootstrappedServer(t, root, cwd, backend, runtimeBundle)

	submitted := submitServedNativeScriptedJob(t, server, "happy", cwd, servedNativeFixtureModeClean)
	waitServedNativeBackendStarted(t, backend)
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	if launchProof.Quiescence.Method == model.QuiescenceTermKill {
		t.Fatalf("happy-path quiescence method = %s, want natural/absent completion", launchProof.Quiescence.Method)
	}
	assertServedNativeFixtureMetadata(t, backend, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
}

func TestServedNativeConformanceGrandchildSurvivalContained(t *testing.T) {
	runtimeBundle := requireServedNativeRuntime(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	backend := newServedNativeFixtureBackend(servedNativeFixtureModeGrandchild, filepath.Join(root, "served-native-fixture"), "")
	server := newServedNativeBootstrappedServer(t, root, cwd, backend, runtimeBundle)

	submitted := submitServedNativeScriptedJob(t, server, "grandchild", cwd, servedNativeFixtureModeGrandchild)
	waitServedNativeBackendStarted(t, backend)
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	if launchProof.Quiescence.Method != model.QuiescenceTermKill {
		t.Fatalf("grandchild quiescence method = %s, want term_kill containment", launchProof.Quiescence.Method)
	}
	assertServedNativeFixtureMetadata(t, backend, *launchProof.Group, true)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
}

func TestServedNativeConformanceDaemonKillRestartRecovery(t *testing.T) {
	if goruntime.GOOS == "darwin" {
		t.Skip("macOS leader-retention custody cannot prove recovery containment after daemon SIGKILL; run this scenario in the Linux cgroup-v2 conformance harness")
	}
	requireServedNativeSupport(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	startedPath := filepath.Join(root, "served-native-release-recorded")
	jobPath := filepath.Join(root, "served-native-job-id")
	helper := startServedNativeDaemonHelper(t, root, cwd, startedPath, jobPath)
	if err := waitServedNativeFile(startedPath, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	submittedJobID := readServedNativeJobID(t, jobPath)
	helper.killAndWait(t)

	recoveryRuntime := requireServedNativeRuntime(t)
	backend := newServedNativeFixtureBackend(servedNativeFixtureModeClean, filepath.Join(root, "served-native-fixture-recovery"), "")
	server, h := startServedNativeServer(t, root, cwd, backend, recoveryRuntime)
	recoveryConn := dialRaw(t, h.socketPath)
	defer recoveryConn.Close()
	recoveryReader := bufio.NewReader(recoveryConn)
	helloRaw(t, recoveryConn, recoveryReader, h.token)

	record := waitServedNativeAdmissionTerminal(t, server, submittedJobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeReaped)
	if record.Terminal.Cause != model.CauseDaemonRestartedAfterAuthorization {
		t.Fatalf("recovery terminal cause = %s, want daemon_restart_after_authorization", record.Terminal.Cause)
	}
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)

	var status protocol.JobStatusResult
	decodeResult(t, rpc(t, recoveryConn, recoveryReader, "status-recovered", protocol.MethodJobStatus, protocol.JobStatusParams{JobID: submittedJobID}), &status)
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateReaped {
		t.Fatalf("recovered status = %+v, want one reaped job", status)
	}
}

func requireServedNativeRuntime(t *testing.T) custodian.Runtime {
	t.Helper()
	if goruntime.GOOS == "linux" && os.Getenv(servedNativeCgroupConformanceEnv) != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run served native cgroup-v2 conformance")
	}
	options := servedNativeRuntimeOptions(t)
	runtimeBundle, err := custodian.NewNativeRuntime(options)
	support := runtimeBundle.Support()
	if err != nil || !support.ParkedExec || !support.VerifiedContainment {
		t.Skipf("native runtime support unavailable: support=%+v err=%v", support, err)
	}
	t.Cleanup(func() {
		if closer, ok := runtimeBundle.Process().(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				t.Fatalf("native runtime Close() error = %v", err)
			}
		}
	})
	return runtimeBundle
}

// requireServedNativeSupport gates a test on native-runtime availability WITHOUT
// retaining the exclusive cgroup root lease for the test's lifetime. It creates a
// runtime, records support, and CLOSES it immediately (releasing the root lease) so a
// later runtime in the same test (e.g. a restarted daemon) can acquire it. Using
// requireServedNativeRuntime here instead would hold the lease until test end and make a
// subsequent runtime acquisition fail with EAGAIN.
func requireServedNativeSupport(t *testing.T) {
	t.Helper()
	if goruntime.GOOS == "linux" && os.Getenv(servedNativeCgroupConformanceEnv) != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run served native cgroup-v2 conformance")
	}
	options := servedNativeRuntimeOptions(t)
	runtimeBundle, err := custodian.NewNativeRuntime(options)
	support := runtimeBundle.Support()
	if closer, ok := runtimeBundle.Process().(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	if err != nil || !support.ParkedExec || !support.VerifiedContainment {
		t.Skipf("native runtime support unavailable: support=%+v err=%v", support, err)
	}
}

func servedNativeRuntimeOptions(t *testing.T) custodian.NativeOptions {
	t.Helper()
	exe := servedNativeTestBinaryPath(t)
	return custodian.NativeOptions{
		AgentbusPath:      builtServedNativeAgentbusPath(t),
		MonitorCommand:    servedNativeMonitorCommand(t),
		ContainmentParams: servedNativeContainmentParams(),
		WorkerEnv:         append(os.Environ(), servedNativeAgentbusGoEnv()...),
		WorkerDir:         filepath.Dir(exe),
	}
}

func servedNativeContainmentParams() containment.Params {
	return containment.Params{
		GracePeriod:                100 * time.Millisecond,
		PollInterval:               20 * time.Millisecond,
		PollTimeout:                3 * time.Second,
		TrustedMonitorWait:         100 * time.Millisecond,
		TrustedMonitorPollInterval: 20 * time.Millisecond,
	}
}

func servedNativeMonitorCommand(t *testing.T) parklaunch.CommandSpec {
	t.Helper()
	exe := servedNativeTestBinaryPath(t)
	return parklaunch.CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"-test.run=^TestServedNativeConformanceMonitorProcess$",
			"--",
			"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
			"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
			"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
		},
		Env: append(os.Environ(), servedNativeMonitorEnv+"=1"),
		Dir: filepath.Dir(exe),
	}
}

func builtServedNativeAgentbusPath(t *testing.T) string {
	t.Helper()
	servedNativeAgentbusBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "agentbus-served-native-bin-")
		if err != nil {
			servedNativeAgentbusBuildErr = err
			return
		}
		servedNativeAgentbusBuildPath = filepath.Join(dir, "agentbus")
		cmd := exec.Command("go", "build", "-o", servedNativeAgentbusBuildPath, "./cmd/agentbus")
		cmd.Dir = servedNativeRepoRootFromCaller()
		cmd.Env = append(os.Environ(), servedNativeAgentbusGoEnv()...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			servedNativeAgentbusBuildErr = fmt.Errorf("go build ./cmd/agentbus: %w\n%s", err, output)
		}
	})
	if servedNativeAgentbusBuildErr != nil {
		t.Fatal(servedNativeAgentbusBuildErr)
	}
	return servedNativeAgentbusBuildPath
}

func servedNativeAgentbusGoEnv() []string {
	env := []string{
		servedNativeAgentbusGOFLAGS,
		servedNativeAgentbusGOPROXY,
	}
	if modcache := os.Getenv(servedNativeOfflineModcacheEnv); modcache != "" {
		env = append(env, "GOMODCACHE="+modcache)
	}
	return env
}

func servedNativeRepoRootFromCaller() string {
	_, file, _, ok := goruntime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func servedNativeTestBinaryPath(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

func enableServedNativeRuntime(server *Server, runtimeBundle custodian.Runtime) {
	server.admissionRuntimeFactory = func(*Server) *servedAdmissionRuntime {
		return &servedAdmissionRuntime{
			runtime: runtimeBundle,
		}
	}
}

type servedNativeFixtureBackend struct {
	mode        string
	dir         string
	startedPath string
	started     chan struct{}
	metadata    chan servedNativeFixtureMetadata
	count       atomic.Int64
}

func newServedNativeFixtureBackend(mode, dir, startedPath string) *servedNativeFixtureBackend {
	return &servedNativeFixtureBackend{
		mode:        mode,
		dir:         dir,
		startedPath: startedPath,
		started:     make(chan struct{}, 4),
		metadata:    make(chan servedNativeFixtureMetadata, 4),
	}
}

func (b *servedNativeFixtureBackend) Name() string { return servedNativeBackendName }

func (b *servedNativeFixtureBackend) AdmissionParkable() bool { return true }

func (b *servedNativeFixtureBackend) AdmissionControlledRunner() bool { return true }

func (b *servedNativeFixtureBackend) ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error) {
	return b, nil
}

func (b *servedNativeFixtureBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: b.Name()}, nil
}

func (b *servedNativeFixtureBackend) Start(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
	n := b.count.Add(1)
	return &servedNativeFixtureSession{id: fmt.Sprintf("%s-session-%d", b.Name(), n), backend: b}, nil
}

func (b *servedNativeFixtureBackend) Resume(_ context.Context, id string, _ engine.SessionOpts) (engine.Session, error) {
	if id == "" {
		id = fmt.Sprintf("%s-session-resumed", b.Name())
	}
	return &servedNativeFixtureSession{id: id, backend: b}, nil
}

func (b *servedNativeFixtureBackend) fixturePaths(mode string) (resultPath, grandchildReadyPath string, err error) {
	if err := os.MkdirAll(b.dir, 0o700); err != nil {
		return "", "", err
	}
	n := b.count.Load()
	resultPath = filepath.Join(b.dir, fmt.Sprintf("%s-%d-result.json", mode, n))
	if mode == servedNativeFixtureModeGrandchild {
		grandchildReadyPath = filepath.Join(b.dir, fmt.Sprintf("%s-%d-grandchild-ready", mode, n))
	}
	return resultPath, grandchildReadyPath, nil
}

func (b *servedNativeFixtureBackend) markStarted() {
	select {
	case b.started <- struct{}{}:
	default:
	}
	if b.startedPath != "" {
		_ = os.WriteFile(b.startedPath, []byte("release-recorded\n"), 0o600)
	}
}

type servedNativeFixtureSession struct {
	id      string
	backend *servedNativeFixtureBackend
}

func (s *servedNativeFixtureSession) ID() string { return s.id }

func (s *servedNativeFixtureSession) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	return nil, fmt.Errorf("served native fixture requires ordinal-bound runner: %w", custodian.ErrSupervisorUnavailable)
}

func (s *servedNativeFixtureSession) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	mode := s.backend.mode
	if strings.Contains(input.Prompt, servedNativeFixtureModeGrandchild) {
		mode = servedNativeFixtureModeGrandchild
	} else if strings.Contains(input.Prompt, servedNativeFixtureModeHold) {
		mode = servedNativeFixtureModeHold
	}
	resultPath, grandchildReadyPath, err := s.backend.fixturePaths(mode)
	if err != nil {
		return nil, err
	}
	exe := servedNativeTestBinaryPathForBackend()
	args := []string{
		exe,
		"-test.run=^TestServedNativeConformanceFixtureProcess$",
		"--",
		"--mode", mode,
		"--result", resultPath,
	}
	if grandchildReadyPath != "" {
		args = append(args, "--grandchild-ready", grandchildReadyPath)
	}
	running, err := runner.Start(ctx, command.ExecSpec{
		Argv: args,
		Env:  append(os.Environ(), servedNativeFixtureEnv+"=1"),
		Dir:  filepath.Dir(exe),
	})
	if err != nil {
		return nil, err
	}
	s.backend.markStarted()

	ch := make(chan engine.Event, 4)
	go func() {
		defer close(ch)
		if stdin := running.Stdin(); stdin != nil {
			_ = stdin.Close()
		}
		stdoutDone := discardReadCloser(running.Stdout())
		stderrDone := discardReadCloser(running.Stderr())
		exit, waitErr := running.Wait(ctx)
		<-stdoutDone
		<-stderrDone
		if waitErr != nil {
			ch <- engine.Event{Type: engine.EventTerminalError, Text: waitErr.Error()}
			return
		}
		if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
			ch <- engine.Event{Type: engine.EventTerminalError, Text: fmt.Sprintf("fixture exit = %+v", exit)}
			return
		}
		if metadata, err := readServedNativeFixtureMetadata(resultPath); err == nil {
			select {
			case s.backend.metadata <- metadata:
			default:
			}
		}
		ch <- engine.Event{Type: engine.EventAgentText, Text: servedNativeResultText}
	}()
	return ch, nil
}

func (s *servedNativeFixtureSession) Interrupt(context.Context) error { return nil }

func servedNativeTestBinaryPathForBackend() string {
	exe, err := os.Executable()
	if err != nil {
		return os.Args[0]
	}
	return exe
}

func discardReadCloser(reader io.ReadCloser) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		if reader != nil {
			_, _ = io.Copy(io.Discard, reader)
			_ = reader.Close()
		}
		close(done)
	}()
	return done
}

type servedNativeFixtureMetadata struct {
	PID            int `json:"pid"`
	PGID           int `json:"pgid"`
	GrandchildPID  int `json:"grandchildPid,omitempty"`
	GrandchildPGID int `json:"grandchildPgid,omitempty"`
}

func runServedNativeFixture(args []string) int {
	fs := flag.NewFlagSet("served-native-fixture", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	mode := fs.String("mode", "", "fixture mode")
	resultPath := fs.String("result", "", "result path")
	grandchildReadyPath := fs.String("grandchild-ready", "", "grandchild ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *mode == "" || *resultPath == "" {
		return 2
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	metadata := servedNativeFixtureMetadata{PID: os.Getpid(), PGID: pgid}
	switch *mode {
	case servedNativeFixtureModeClean:
		fmt.Printf("served-native-clean pid=%d pgid=%d\n", os.Getpid(), pgid)
		return writeServedNativeFixtureMetadata(*resultPath, metadata)
	case servedNativeFixtureModeGrandchild:
		if *grandchildReadyPath == "" {
			return 2
		}
		exe, err := os.Executable()
		if err != nil {
			return 4
		}
		cmd := exec.Command(exe,
			"-test.run=^TestServedNativeConformanceGrandchildProcess$",
			"--",
			"--ready", *grandchildReadyPath,
		)
		cmd.Env = append(os.Environ(), servedNativeGrandchildEnv+"=1")
		cmd.Dir = filepath.Dir(exe)
		if err := cmd.Start(); err != nil {
			return 5
		}
		if err := waitServedNativeFile(*grandchildReadyPath, 5*time.Second); err != nil {
			_ = cmd.Process.Kill()
			return 6
		}
		childPGID, err := readServedNativeGrandchildPGID(*grandchildReadyPath)
		if err != nil {
			_ = cmd.Process.Kill()
			return 7
		}
		metadata.GrandchildPID = cmd.Process.Pid
		metadata.GrandchildPGID = childPGID
		if code := writeServedNativeFixtureMetadata(*resultPath, metadata); code != 0 {
			_ = cmd.Process.Kill()
			return code
		}
		fmt.Printf("served-native-grandchild-ready pid=%d pgid=%d child=%d\n", os.Getpid(), pgid, cmd.Process.Pid)
		return 0
	case servedNativeFixtureModeHold:
		signal.Ignore(syscall.SIGTERM)
		if code := writeServedNativeFixtureMetadata(*resultPath, metadata); code != 0 {
			return code
		}
		fmt.Printf("served-native-hold-ready pid=%d pgid=%d\n", os.Getpid(), pgid)
		for {
			time.Sleep(time.Hour)
		}
	default:
		return 2
	}
}

func runServedNativeGrandchild(args []string) int {
	fs := flag.NewFlagSet("served-native-grandchild", flag.ContinueOnError)
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
		return 4
	}
	for {
		time.Sleep(time.Hour)
	}
}

func runServedNativeMonitor(args []string) int {
	fs := flag.NewFlagSet("served-native-monitor", flag.ContinueOnError)
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
	if err := parklaunch.RunMonitorFromFDs(context.Background(), *daemonFD, *targetFD, *readyFD, custodian.RealContainment{Params: servedNativeContainmentParams()}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0
}

func servedNativeHelperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" && i+1 <= len(os.Args) {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func writeServedNativeFixtureMetadata(path string, metadata servedNativeFixtureMetadata) int {
	if path == "" {
		return 2
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return 8
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return 8
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return 8
	}
	return 0
}

func readServedNativeFixtureMetadata(path string) (servedNativeFixtureMetadata, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return servedNativeFixtureMetadata{}, err
	}
	var metadata servedNativeFixtureMetadata
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return servedNativeFixtureMetadata{}, err
	}
	return metadata, nil
}

func readServedNativeGrandchildPGID(path string) (int, error) {
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

func waitServedNativeFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	return fmt.Errorf("timeout waiting for %s: %v", path, lastErr)
}

func startServedNativeServer(t *testing.T, root, cwd string, backend engine.Backend, runtimeBundle custodian.Runtime) (*Server, testServer) {
	t.Helper()
	server, err := New(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{backend},
		IdleTimeout: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	enableServedNativeRuntime(server, runtimeBundle)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	h := testServer{root: root, cwd: cwd, socketPath: filepath.Join(root, protocol.SocketName), token: "test-token", done: done, cancel: cancel}
	waitForSocket(t, h.socketPath, done)
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("server did not stop")
		}
	})
	return server, h
}

func newServedNativeBootstrappedServer(t *testing.T, root, cwd string, backend engine.Backend, runtimeBundle custodian.Runtime) *Server {
	t.Helper()
	server, err := New(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{backend},
		IdleTimeout: -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	enableServedNativeRuntime(server, runtimeBundle)
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if server.admissionClose != nil {
			if err := server.admissionClose.Close(); err != nil {
				t.Fatalf("admission close error = %v", err)
			}
		}
	})
	return server
}

func submitServedNativeScriptedJob(t *testing.T, server *Server, requestSuffix, cwd, prompt string) protocol.JobSubmitResult {
	t.Helper()
	conn := serveScriptedRequest(t, server, protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-served-native-" + requestSuffix,
		RequestID:    "request-served-native-" + requestSuffix,
		TaskSpec: protocol.TaskSpec{
			Backend: servedNativeBackendName,
			CWD:     cwd,
			Write:   false,
			Prompt:  prompt,
		},
	}, nil)
	resp := responseFromScriptedConn(t, conn)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}
	return submitted
}

func waitServedNativeBackendStarted(t *testing.T, backend *servedNativeFixtureBackend) {
	t.Helper()
	select {
	case <-backend.started:
	case <-time.After(10 * time.Second):
		t.Fatal("served native fixture backend did not start")
	}
}

func waitServedNativeAdmissionTerminal(t *testing.T, server *Server, jobID string, timeout time.Duration) model.SafetyRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last model.SafetyRecord
	for time.Now().Before(deadline) {
		record := loadAdmissionSafetyRecord(t, server, jobID)
		last = record
		if record.Terminal != nil {
			return record
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("admission safety record %s did not reach terminal after %s; last = %+v", jobID, timeout, last)
	return model.SafetyRecord{}
}

func assertServedNativeIdentifiedTerminal(t *testing.T, record model.SafetyRecord, outcome model.Outcome) *model.LaunchProof {
	t.Helper()
	if record.Mode != model.ModeIdentifiedFenced {
		t.Fatalf("admission mode = %s, want IdentifiedFenced", record.Mode)
	}
	if record.Terminal == nil || record.Terminal.Outcome != outcome {
		t.Fatalf("terminal = %+v, want outcome %s", record.Terminal, outcome)
	}
	launchProof, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launchProof.Group == nil || launchProof.Grant == nil || launchProof.Released == nil || launchProof.Quiescence == nil {
		t.Fatalf("launch proof incomplete: %+v", launchProof)
	}
	return launchProof
}

func assertServedNativeFixtureMetadata(t *testing.T, backend *servedNativeFixtureBackend, group model.GroupRef, wantGrandchild bool) {
	t.Helper()
	select {
	case metadata := <-backend.metadata:
		if metadata.PID != group.Leader.PID || metadata.PGID != group.PGID {
			t.Fatalf("fixture metadata = %+v, want leader pid %d pgid %d", metadata, group.Leader.PID, group.PGID)
		}
		if wantGrandchild {
			if metadata.GrandchildPID <= 0 || metadata.GrandchildPGID != group.PGID {
				t.Fatalf("fixture metadata = %+v, want grandchild in group %d", metadata, group.PGID)
			}
			return
		}
		if metadata.GrandchildPID != 0 || metadata.GrandchildPGID != 0 {
			t.Fatalf("fixture metadata = %+v, want no grandchild", metadata)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fixture metadata was not recorded")
	}
}

func assertServedNativeIndependentGroupAbsent(t *testing.T, group model.GroupRef, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		absent, detail, err := servedNativeIndependentGroupAbsent(group)
		if err != nil {
			t.Fatalf("independent group oracle error for pgid %d: %v", group.PGID, err)
		}
		last = detail
		if absent {
			time.Sleep(20 * time.Millisecond)
			again, againDetail, err := servedNativeIndependentGroupAbsent(group)
			if err != nil {
				t.Fatalf("independent group oracle second sample error for pgid %d: %v", group.PGID, err)
			}
			if again {
				return
			}
			last = againDetail
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("independent group oracle for pgid %d = %s after %s, want absent", group.PGID, last, timeout)
}

func servedNativeIndependentGroupAbsent(group model.GroupRef) (bool, string, error) {
	if err := group.Validate(); err != nil {
		return false, "", err
	}
	killErr := unix.Kill(-group.PGID, 0)
	killAbsent := errors.Is(killErr, unix.ESRCH)
	if killErr != nil && !killAbsent && !errors.Is(killErr, unix.EPERM) {
		return false, "", killErr
	}
	claim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
	if err != nil {
		return false, "", err
	}
	classified := procgroup.ClassifyGroup(claim)
	detail := fmt.Sprintf("kill0=%v procgroup=%s", killErr, classified)
	return killAbsent && classified == model.GroupAbsent, detail, nil
}

type servedNativeDaemonHelper struct {
	cmd    *exec.Cmd
	done   chan error
	output *bytes.Buffer
	killed atomic.Bool
}

func readServedNativeJobID(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	jobID := strings.TrimSpace(string(raw))
	if jobID == "" {
		t.Fatalf("job id file %s was empty", path)
	}
	return jobID
}

func startServedNativeDaemonHelper(t *testing.T, root, cwd, startedPath, jobPath string) *servedNativeDaemonHelper {
	t.Helper()
	exe := servedNativeTestBinaryPath(t)
	cmd := exec.Command(exe,
		"-test.run=^TestServedNativeConformanceDaemonProcess$",
		"--",
		"--root", root,
		"--cwd", cwd,
		"--started", startedPath,
		"--job", jobPath,
	)
	cmd.Env = append(os.Environ(), servedNativeDaemonEnv+"=1", servedNativeDaemonStartedPathEnv+"="+startedPath)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	helper := &servedNativeDaemonHelper{cmd: cmd, done: make(chan error, 1), output: &output}
	go func() {
		helper.done <- cmd.Wait()
		close(helper.done)
	}()
	waitServedNativeHelperFiles(t, []string{jobPath, startedPath}, helper.done, &output)
	t.Cleanup(func() {
		if helper.killed.Load() {
			return
		}
		_ = helper.cmd.Process.Kill()
		select {
		case <-helper.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("daemon helper cleanup wait timed out; output:\n%s", helper.output.String())
		}
	})
	return helper
}

func waitServedNativeHelperFiles(t *testing.T, paths []string, done <-chan error, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("daemon helper exited before markers were ready: %v\n%s", err, output.String())
		default:
		}
		allReady := true
		for _, path := range paths {
			if _, err := os.Stat(path); err != nil {
				allReady = false
				break
			}
		}
		if allReady {
			return
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("daemon helper markers %v did not become ready; output:\n%s", paths, output.String())
}

func (helper *servedNativeDaemonHelper) killAndWait(t *testing.T) {
	t.Helper()
	helper.killed.Store(true)
	if err := helper.cmd.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("SIGKILL daemon helper: %v", err)
	}
	select {
	case err := <-helper.done:
		if err == nil {
			t.Fatalf("daemon helper exited cleanly, want SIGKILL; output:\n%s", helper.output.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("daemon helper did not exit after SIGKILL; output:\n%s", helper.output.String())
	}
}
