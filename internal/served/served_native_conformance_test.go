//go:build darwin || linux

package served

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	_ "unsafe"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

//go:linkname parklaunchReleaseAfterSendBeforeAckForTest github.com/charlesnpx/agentbus/internal/parklaunch.releaseAfterSendBeforeAckForTest
var parklaunchReleaseAfterSendBeforeAckForTest func(func() error) error

//go:linkname authorityAllocateGrantBeforeCommitForTest github.com/charlesnpx/agentbus/engine/execution/authority.allocateGrantBeforeCommitForTest
var authorityAllocateGrantBeforeCommitForTest func() error

const (
	servedNativeBackendName              = "codex"
	servedNativeCodexFixtureEnv          = "AGENTBUS_SERVED_NATIVE_CODEX_FIXTURE"
	servedNativeCodexExecLogEnv          = "AGENTBUS_SERVED_NATIVE_CODEX_EXEC_LOG"
	servedNativeCodexReadyDirEnv         = "AGENTBUS_SERVED_NATIVE_CODEX_READY_DIR"
	servedNativeGrandchildEnv            = "AGENTBUS_SERVED_NATIVE_GRANDCHILD"
	servedNativeDaemonEnv                = "AGENTBUS_SERVED_NATIVE_DAEMON"
	servedNativeStartedPathTag           = "SERVED_NATIVE_STARTED_PATH"
	servedNativeModeTag                  = "SERVED_NATIVE_MODE"
	servedNativeCgroupConformanceEnv     = "AGENTBUS_CGROUP_CONFORMANCE"
	servedNativePrebuiltBinaryEnv        = "AGENTBUS_E2E_PREBUILT_BINARY"
	servedNativeOfflineModcacheEnv       = "AGENTBUS_OFFLINE_MODCACHE"
	servedNativeFixtureModeClean         = "clean"
	servedNativeFixtureModeGrandchild    = "grandchild"
	servedNativeFixtureModeHold          = "hold"
	servedNativeExecModeEntry            = "entry"
	servedNativeExecModeStdin            = "stdin"
	servedNativeEntryStartedMarker       = "exec-entry\n"
	servedNativeStdinStartedMarker       = "exec-stdin\n"
	servedNativeAgentbusGOFLAGS          = "GOFLAGS=-mod=mod"
	servedNativeAgentbusGOPROXY          = "GOPROXY=off"
	servedNativeResultText               = "PASS\n\n## Findings\nNone.\n"
	servedNativeConformancePollInterval  = 20 * time.Millisecond
	servedNativeDaemonHelperReadyTimeout = 60 * time.Second
)

var (
	servedNativeAgentbusBuildOnce sync.Once
	servedNativeAgentbusBuildPath string
	servedNativeAgentbusBuildErr  error
)

func TestServedNativeConformanceCodexFixtureProcess(t *testing.T) {
	if os.Getenv(servedNativeCodexFixtureEnv) != "1" {
		return
	}
	args, ok := servedNativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runServedNativeCodexFixture(args))
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
	agentbus := fs.String("agentbus", "", "agentbus helper binary")
	startedPath := fs.String("started", "", "post-release marker path")
	jobPath := fs.String("job", "", "submitted job path")
	paramsPath := fs.String("params", "", "submitted params path")
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	if *root == "" || *cwd == "" || *agentbus == "" || *startedPath == "" || *jobPath == "" || *paramsPath == "" {
		t.Fatalf("root, cwd, agentbus, started path, job path, and params path are required")
	}

	codexPath, err := exec.LookPath(servedNativeBackendName)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := StrictAdmissionConfig(Config{
		StateRoot:   *root,
		CWD:         *cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{codexcli.New(codexcli.Options{Binary: codexPath})},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(*agentbus, os.Environ()))
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	socketPath := filepath.Join(*root, protocol.SocketName)
	waitForSocket(t, socketPath, done)
	conn := dialRaw(t, socketPath)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	helloRaw(t, conn, reader, "test-token")
	params := servedNativeSubmitParams("restart", *cwd, servedNativeFixtureModeHold, servedNativeStartedTags(*startedPath))
	rawParams := mustMarshal(t, params)
	if err := os.WriteFile(*paramsPath, rawParams, 0o600); err != nil {
		t.Fatal(err)
	}
	resp := rpcRawParams(t, conn, reader, "submit-restart", protocol.MethodJobSubmit, rawParams)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}
	if err := os.WriteFile(*jobPath, []byte(submitted.JobID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitServedNativeFile(*startedPath, servedNativeDaemonHelperReadyTimeout); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestServedNativeProductionDefaultsUnavailableAndGateOff(t *testing.T) {
	server, root, _ := newUnstartedTestServer(t, newFakeBackend(servedNativeBackendName))
	if server.jobsRequestIDEnabled {
		t.Fatal("jobsRequestIDEnabled default = true, want false")
	}
	runtime := newServedAdmissionRuntime(server)
	support := runtime.support()
	if support.ParkedExec || support.VerifiedContainment || !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("production runtime support = %+v, want unavailable supervisor", support)
	}
	err := server.bootstrapAdmission(context.Background())
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("bootstrapAdmission() error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("bootstrapAdmission() error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if server.jobsRequestIDEnabled {
		t.Fatal("jobsRequestIDEnabled changed to true after default bootstrap")
	}
	if server.admissionInstance != nil {
		t.Fatal("admission instance was constructed for unavailable runtime")
	}
	for _, name := range []string{admissionRepositoryFile, admissionAnchorFile} {
		if _, statErr := os.Stat(filepath.Join(root, name)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("%s stat error = %v, want not exist", name, statErr)
		}
	}
}

func TestServedStrictCompositionDefaultConstructionConformance(t *testing.T) {
	requireServedNativeConformance(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, root)
	cfg, err := StrictAdmissionConfig(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{servedNativeCodexBackend(fixture)},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if err != nil {
		t.Fatalf("StrictAdmissionConfig() error = %v", err)
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.bootstrapAdmission(context.Background()); err != nil {
		t.Fatalf("bootstrapAdmission() error = %v", err)
	}
	defer closeServedNativeAdmission(t, server)
	if server.admissionInstance == nil {
		t.Fatal("admission instance missing")
	}
	policy := server.admissionInstance.policy
	if policy.Mode != AdmissionStrictIdentified || !policy.strictRuntimeAvailable() {
		t.Fatalf("policy = %+v support=%+v, want strict available", policy, policy.runtimeSupport)
	}
	inspection, err := authority.InspectAdmissionRepository(context.Background(), server.admissionRepository)
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.ActivationMetadata.Activated {
		t.Fatal("strict composition did not activate the admission root")
	}

	restrictedCfg, restrictedErr := StrictAdmissionConfig(Config{
		StateRoot:   shortTempDir(t),
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{servedNativeCodexBackend(fixture)},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if goruntime.GOOS == "linux" {
		if !errors.Is(restrictedErr, cgroup.ErrRootLeaseUnavailable) {
			t.Fatalf("restricted composition error = %v, want typed cgroup root lease contention", restrictedErr)
		}
	} else {
		if restrictedErr != nil {
			t.Fatalf("second strict composition error = %v, want darwin process-group runtime construction to remain available", restrictedErr)
		}
		if err := restrictedCfg.Runtime.Close(); err != nil {
			t.Fatalf("second strict composition runtime Close() error = %v", err)
		}
	}
}

func TestServedStrictCompositionIdentifiedSubmitReplayConformance(t *testing.T) {
	server, h, fixture := startServedNativeStrictServer(t, shortTempDir(t), shortTempDir(t), nil)
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()

	params := servedNativeSubmitParams("identified-submit", h.cwd, servedNativeFixtureModeClean, nil)
	resp := rpc(t, conn, reader, "submit-1", protocol.MethodJobSubmit, params)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}
	execution := waitServedNativeCodexExecutions(t, fixture, 1, 10*time.Second)[0]
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)

	resp = rpc(t, conn, reader, "submit-replay", protocol.MethodJobSubmit, params)
	var replay protocol.JobSubmitResult
	decodeResult(t, resp, &replay)
	if replay.JobID != submitted.JobID || !replay.Deduplicated || replay.State != engine.StateCompleted {
		t.Fatalf("replay submit = %+v, want same completed terminal job %s", replay, submitted.JobID)
	}
	assertServedNativeCodexExecutionCount(t, fixture, 1)
}

func TestServedStrictCompositionNoExecBeforeGrantConformance(t *testing.T) {
	grantHoldEntered := make(chan struct{})
	releaseGrant := make(chan struct{})
	var hookCalls atomic.Int32
	setServedNativeAllocateGrantBeforeCommitHook(t, func() error {
		call := hookCalls.Add(1)
		if call != 1 {
			return nil
		}
		close(grantHoldEntered)
		select {
		case <-releaseGrant:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("served native pre-grant hold timed out")
		}
	})
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	startedPath := filepath.Join(root, "no-exec-before-grant-started")
	t.Setenv(servedNativeModeTag, servedNativeFixtureModeHold)
	t.Setenv(servedNativeStartedPathTag, startedPath)
	fixture := installServedNativeCodexFixture(t, root)
	server, h, fixture := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, nil)
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()
	assertServedNativeCodexExecutionCount(t, fixture, 0)
	submitted := submitServedNativeJobOnConn(t, conn, reader, "no-exec-before-grant", h.cwd, servedNativeFixtureModeHold, nil)
	select {
	case <-grantHoldEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("authority grant commit hook did not fire")
	}
	assertServedNativeEntryEvidenceAbsentFor(t, fixture, startedPath, 200*time.Millisecond)
	close(releaseGrant)
	execution := assertServedNativeExecWaitsForGrantRelease(t, server, fixture, submitted.JobID, startedPath, 10*time.Second)
	resp := rpc(t, conn, reader, "cancel-no-exec", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: submitted.JobID})
	var canceled protocol.JobCancelResult
	decodeResult(t, resp, &canceled)
	if canceled.JobID != submitted.JobID {
		t.Fatalf("cancel result = %+v, want job %s", canceled, submitted.JobID)
	}
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCanceled)
	if launchProof.Grant == nil || launchProof.Released == nil {
		t.Fatalf("launch proof = %+v, want grant and release before terminal execution proof", launchProof)
	}
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
	assertServedNativeCodexExecutionCount(t, fixture, 1)
}

func TestServedStrictCompositionReleaseAckLossConformance(t *testing.T) {
	var hookCalls atomic.Int32
	setServedNativeReleaseAfterSendBeforeAckHook(t, func(dropAck func() error) error {
		call := hookCalls.Add(1)
		if call != 1 {
			return nil
		}
		if dropAck == nil {
			return errors.New("served native release ack loss hook missing drop function")
		}
		if err := dropAck(); err != nil {
			return fmt.Errorf("drop release ack: %w", err)
		}
		return errors.New("served native release ack lost after release send")
	})

	server, h, fixture := startServedNativeStrictServer(t, shortTempDir(t), shortTempDir(t), nil)
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()

	params := servedNativeSubmitParams("release-ack-loss", h.cwd, servedNativeFixtureModeClean, nil)
	resp := rpc(t, conn, reader, "submit-release-ack-loss", protocol.MethodJobSubmit, params)
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}

	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeReleaseAckLossTerminal(t, record)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
	executions := readServedNativeCodexExecutions(t, fixture)
	if len(executions) > 1 {
		t.Fatalf("codex execution count = %d (%+v), want at most 1 after release ack loss", len(executions), executions)
	}
	if len(executions) == 1 {
		assertServedNativeExecutionMetadata(t, executions[0], *launchProof.Group, false)
	}
	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("release ack loss hook calls = %d, want 1", got)
	}

	resp = rpc(t, conn, reader, "replay-release-ack-loss", protocol.MethodJobSubmit, params)
	var replay protocol.JobSubmitResult
	decodeResult(t, resp, &replay)
	if replay.JobID != submitted.JobID || !replay.Deduplicated || replay.State != engine.StateReaped {
		t.Fatalf("replay after release ack loss = %+v, want same reaped terminal job %s", replay, submitted.JobID)
	}
	afterReplay := readServedNativeCodexExecutions(t, fixture)
	if len(afterReplay) != len(executions) {
		t.Fatalf("codex execution count after replay = %d (%+v), want unchanged %d", len(afterReplay), afterReplay, len(executions))
	}
	if got := hookCalls.Load(); got != 1 {
		t.Fatalf("release ack loss hook calls after replay = %d, want 1", got)
	}

	newJob := submitServedNativeJobOnConn(t, conn, reader, "post-release-ack-loss", h.cwd, servedNativeFixtureModeClean, nil)
	newRecord := waitServedNativeAdmissionTerminal(t, server, newJob.JobID, 10*time.Second)
	newLaunchProof := assertServedNativeIdentifiedTerminal(t, newRecord, model.OutcomeCompleted)
	assertServedNativeIndependentGroupAbsent(t, *newLaunchProof.Group, 5*time.Second)
}

func TestServedStrictCompositionCancellationConformance(t *testing.T) {
	server, h, fixture := startServedNativeStrictServer(t, shortTempDir(t), shortTempDir(t), nil)
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()
	submitted := submitServedNativeJobOnConn(t, conn, reader, "cancel", h.cwd, servedNativeFixtureModeHold, servedNativeStartedTags(filepath.Join(h.root, "hold-started")))
	execution := waitServedNativeCodexExecutions(t, fixture, 1, 10*time.Second)[0]
	resp := rpc(t, conn, reader, "cancel", protocol.MethodJobCancel, protocol.JobCancelParams{JobID: submitted.JobID})
	var canceled protocol.JobCancelResult
	decodeResult(t, resp, &canceled)
	if canceled.JobID != submitted.JobID {
		t.Fatalf("cancel result = %+v, want job %s", canceled, submitted.JobID)
	}
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCanceled)
	if launchProof.Quiescence.Method != model.QuiescenceTermKill {
		t.Fatalf("cancel quiescence method = %s, want term_kill", launchProof.Quiescence.Method)
	}
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)

	newJob := submitServedNativeJobOnConn(t, conn, reader, "post-cancel", h.cwd, servedNativeFixtureModeClean, nil)
	newRecord := waitServedNativeAdmissionTerminal(t, server, newJob.JobID, 10*time.Second)
	newLaunchProof := assertServedNativeIdentifiedTerminal(t, newRecord, model.OutcomeCompleted)
	assertServedNativeIndependentGroupAbsent(t, *newLaunchProof.Group, 5*time.Second)
}

func TestServedStrictCompositionDescendantContainmentConformance(t *testing.T) {
	server, h, fixture := startServedNativeStrictServer(t, shortTempDir(t), shortTempDir(t), nil)
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()
	submitted := submitServedNativeJobOnConn(t, conn, reader, "grandchild", h.cwd, servedNativeFixtureModeGrandchild, nil)
	execution := waitServedNativeCodexExecutions(t, fixture, 1, 10*time.Second)[0]
	record := waitServedNativeAdmissionTerminal(t, server, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	if launchProof.Quiescence.Method != model.QuiescenceTermKill {
		t.Fatalf("grandchild quiescence method = %s, want term_kill residual containment", launchProof.Quiescence.Method)
	}
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, true)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
	if execution.GrandchildPID <= 0 {
		t.Fatalf("execution metadata = %+v, want grandchild pid", execution)
	}
	assertServedNativePIDAbsent(t, execution.GrandchildPID, 5*time.Second)
}

func TestServedStrictCompositionDaemonSIGKILLRestartRecoveryConformance(t *testing.T) {
	requireServedNativeConformance(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, root)
	startedPath := filepath.Join(root, "served-native-release-recorded")
	jobPath := filepath.Join(root, "served-native-job-id")
	paramsPath := filepath.Join(root, "served-native-submit-params.json")
	helper := startServedNativeDaemonHelper(t, root, cwd, builtServedNativeAgentbusPath(t), fixture.env, startedPath, jobPath, paramsPath)
	if err := waitServedNativeFile(startedPath, servedNativeDaemonHelperReadyTimeout); err != nil {
		t.Fatalf("daemon helper release marker did not become ready: %v\n%s\noutput:\n%s", err, helper.diagnostics([]string{startedPath}, nil), helper.output.String())
	}
	submittedJobID := readServedNativeJobID(t, jobPath)
	submitParams := readServedNativeSubmitParams(t, paramsPath)
	firstExecution := waitServedNativeCodexExecutions(t, fixture, 1, 10*time.Second)[0]
	helper.killAndWait(t)

	recoveryServer, h, _ := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, nil)
	recoveryConn, recoveryReader := servedNativeRPC(t, h)
	defer recoveryConn.Close()
	record := waitServedNativeAdmissionTerminal(t, recoveryServer, submittedJobID, 10*time.Second)
	launchProof := assertServedNativeRestartRecoveryTerminal(t, record)
	if launchProof.Group != nil {
		assertServedNativeExecutionMetadata(t, firstExecution, *launchProof.Group, false)
		if record.Terminal.Outcome == model.OutcomeReaped {
			assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
		}
	}

	resp := rpcRawParams(t, recoveryConn, recoveryReader, "replay", protocol.MethodJobSubmit, submitParams)
	var replay protocol.JobSubmitResult
	decodeResult(t, resp, &replay)
	wantReplayState := engine.StateReaped
	if record.Terminal.Outcome == model.OutcomeOrphaned {
		wantReplayState = engine.StateOrphaned
	}
	if replay.JobID != submittedJobID || !replay.Deduplicated || replay.State != wantReplayState {
		t.Fatalf("replay after recovery = %+v, want %s job %s", replay, wantReplayState, submittedJobID)
	}

	newJob := submitServedNativeJobOnConn(t, recoveryConn, recoveryReader, "post-recovery", cwd, servedNativeFixtureModeClean, nil)
	secondExecution := waitServedNativeCodexExecutions(t, fixture, 2, 10*time.Second)[1]
	newRecord := waitServedNativeAdmissionTerminal(t, recoveryServer, newJob.JobID, 10*time.Second)
	newLaunchProof := assertServedNativeIdentifiedTerminal(t, newRecord, model.OutcomeCompleted)
	assertServedNativeExecutionMetadata(t, secondExecution, *newLaunchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *newLaunchProof.Group, 5*time.Second)
}

func TestServedStrictCompositionActivatedRootSupportLossConformance(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("retained cgroup root lease support-loss is Linux-only")
	}
	requireServedNativeConformance(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, root)
	server, h, _ := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, nil)
	stopServedNativeServer(t, h)
	closeServedNativeAdmission(t, server)
	inspection, err := authority.InspectAdmissionRoot(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.ActivationMetadata.Activated {
		t.Fatal("root was not activated before support-loss scenario")
	}

	blockerCfg, err := StrictAdmissionConfig(Config{
		StateRoot:   shortTempDir(t),
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{servedNativeCodexBackend(fixture)},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if err != nil {
		t.Fatalf("blocker StrictAdmissionConfig() error = %v", err)
	}
	blockerServer, err := New(blockerCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := blockerServer.bootstrapAdmission(context.Background()); err != nil {
		t.Fatalf("blocker bootstrapAdmission() error = %v", err)
	}
	defer closeServedNativeAdmission(t, blockerServer)

	lossCfg, lossErr := StrictAdmissionConfig(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{servedNativeCodexBackend(fixture)},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if !errors.Is(lossErr, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("support-loss construction error = %v, want ErrRootLeaseUnavailable", lossErr)
	}
	lossServer, err := New(lossCfg)
	if err != nil {
		t.Fatal(err)
	}
	err = lossServer.Serve(context.Background())
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve support-loss error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	after, err := authority.InspectAdmissionRoot(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ActivationMetadata.Activated {
		t.Fatal("support-loss startup cleared activation")
	}
	if _, statErr := os.Stat(filepath.Join(root, protocol.SocketName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("support-loss socket stat = %v, want not exist", statErr)
	}
}

func TestServedStrictCompositionRuntimeLeaseReuseConformance(t *testing.T) {
	requireServedNativeConformance(t)
	root := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, root)
	server, h, _ := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, nil)

	concurrentCfg, concurrentErr := StrictAdmissionConfig(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    []engine.Backend{servedNativeCodexBackend(fixture)},
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if goruntime.GOOS == "linux" {
		if !errors.Is(concurrentErr, cgroup.ErrRootLeaseUnavailable) {
			t.Fatalf("concurrent strict composition error = %v, want ErrRootLeaseUnavailable", concurrentErr)
		}
		concurrentServer, err := New(concurrentCfg)
		if err != nil {
			t.Fatal(err)
		}
		err = concurrentServer.Serve(context.Background())
		var diagnostic AdmissionSupportDiagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("concurrent Serve error = %T %v, want AdmissionSupportDiagnostic", err, err)
		}
	} else {
		if concurrentErr != nil {
			t.Fatalf("concurrent strict composition error = %v, want darwin process-group runtime construction to remain available", concurrentErr)
		}
		if err := concurrentCfg.Runtime.Close(); err != nil {
			t.Fatalf("concurrent runtime Close() error = %v", err)
		}
	}

	stopServedNativeServer(t, h)
	err := server.Serve(context.Background())
	if !errors.Is(err, ErrRuntimeConsumed) {
		t.Fatalf("same server second Serve error = %v, want ErrRuntimeConsumed", err)
	}
	closeServedNativeAdmission(t, server)

	freshServer, freshH, _ := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, nil)
	conn, reader := servedNativeRPC(t, freshH)
	defer conn.Close()
	submitted := submitServedNativeJobOnConn(t, conn, reader, "fresh-second-serve", cwd, servedNativeFixtureModeClean, nil)
	record := waitServedNativeAdmissionTerminal(t, freshServer, submitted.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
}

func TestServedStrictCompositionRejectionsConformance(t *testing.T) {
	unfenceable := newFakeBackend("fake")
	unfenceable.controlled = false
	server, h, fixture := startServedNativeStrictServer(t, shortTempDir(t), shortTempDir(t), []engine.Backend{unfenceable})
	conn, reader := servedNativeRPC(t, h)
	defer conn.Close()

	missingIdentityRaw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"","requestId":"","taskSpec":{"backend":"%s","cwd":%q,"write":false,"prompt":"%s"}}`, servedNativeBackendName, h.cwd, servedNativeFixtureModeClean))
	resp := rpcRawParams(t, conn, reader, "missing-identity", protocol.MethodJobSubmit, missingIdentityRaw)
	assertRPCCode(t, resp, protocol.ErrorInvalidTaskSpec)
	if resp.Error.Data.AdmissionCause != protocol.AdmissionRejectMissingIdentity {
		t.Fatalf("missing identity cause = %q, want %q", resp.Error.Data.AdmissionCause, protocol.AdmissionRejectMissingIdentity)
	}
	assertServedNativeNoAuthorityJobs(t, server)
	assertServedNativeCodexExecutionCount(t, fixture, 0)

	resp = rpc(t, conn, reader, "unfenceable", protocol.MethodJobSubmit, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-unfenceable",
		RequestID:    "request-unfenceable",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     h.cwd,
			Write:   false,
			Prompt:  "unfenceable",
		},
	})
	assertRPCCode(t, resp, protocol.ErrorCapabilityMissing)
	if resp.Error.Data.AdmissionCause != protocol.AdmissionRejectUnfenceableBackend {
		t.Fatalf("unfenceable cause = %q, want %q", resp.Error.Data.AdmissionCause, protocol.AdmissionRejectUnfenceableBackend)
	}
	assertServedNativeNoAuthorityJobs(t, server)
	assertNoEngineJobRecordsForCWD(t, server, h.cwd)
	assertServedNativeCodexExecutionCount(t, fixture, 0)

	ok := submitServedNativeJobOnConn(t, conn, reader, "healthy-after-reject", h.cwd, servedNativeFixtureModeClean, nil)
	record := waitServedNativeAdmissionTerminal(t, server, ok.JobID, 10*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
}

func requireServedNativeConformance(t *testing.T) {
	t.Helper()
	if goruntime.GOOS == "darwin" {
		return
	}
	if goruntime.GOOS != "linux" {
		t.Skip("served native strict conformance requires darwin process groups or linux cgroup-v2")
	}
	if os.Getenv(servedNativeCgroupConformanceEnv) != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run served native cgroup-v2 conformance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	manager, err := cgroup.New("")
	if err != nil {
		t.Skipf("cgroup.New(\"\") unavailable: %v", err)
	}
	support := manager.Probe(ctx)
	if closeErr := manager.Close(); closeErr != nil {
		t.Fatalf("close cgroup conformance probe manager: %v", closeErr)
	}
	if !support.Strict() {
		t.Skipf("strict cgroup-v2 support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}
}

func servedNativeStrictOptions(agentbusPath string, env []string) StrictAdmissionOptions {
	return StrictAdmissionOptions{
		AgentbusPath: agentbusPath,
		WorkerEnv:    append([]string(nil), env...),
		MonitorEnv:   append([]string(nil), env...),
		ContainmentParams: containment.Params{
			GracePeriod:                100 * time.Millisecond,
			PollInterval:               20 * time.Millisecond,
			PollTimeout:                3 * time.Second,
			TrustedMonitorWait:         100 * time.Millisecond,
			TrustedMonitorPollInterval: 20 * time.Millisecond,
		},
	}
}

type servedNativeCodexFixture struct {
	binDir    string
	codexPath string
	execLog   string
	readyDir  string
	env       []string
}

type servedNativeStrictHandle struct {
	root       string
	cwd        string
	socketPath string
	token      string
	done       chan error
	cancel     context.CancelFunc
	stopOnce   sync.Once
}

func installServedNativeCodexFixture(t *testing.T, root string) servedNativeCodexFixture {
	t.Helper()
	binDir := filepath.Join(root, "fixture-bin")
	readyDir := filepath.Join(root, "fixture-ready")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(readyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	exe := servedNativeTestBinaryPath(t)
	script := fmt.Sprintf("#!/bin/sh\nexec %s -test.run=^TestServedNativeConformanceCodexFixtureProcess$ -- \"$@\"\n", shellQuote(exe))
	codexPath := filepath.Join(binDir, "codex")
	if err := os.WriteFile(codexPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	execLog := filepath.Join(root, "codex-executions.jsonl")
	env := append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		servedNativeCodexFixtureEnv+"=1",
		servedNativeCodexExecLogEnv+"="+execLog,
		servedNativeCodexReadyDirEnv+"="+readyDir,
	)
	for _, kv := range servedNativeAgentbusGoEnv() {
		env = upsertEnv(env, kv)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv(servedNativeCodexFixtureEnv, "1")
	t.Setenv(servedNativeCodexExecLogEnv, execLog)
	t.Setenv(servedNativeCodexReadyDirEnv, readyDir)
	return servedNativeCodexFixture{binDir: binDir, codexPath: codexPath, execLog: execLog, readyDir: readyDir, env: env}
}

func servedNativeCodexBackend(fixture servedNativeCodexFixture) engine.Backend {
	return codexcli.New(codexcli.Options{Binary: fixture.codexPath})
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func upsertEnv(env []string, kv string) []string {
	name, _, ok := strings.Cut(kv, "=")
	if !ok {
		return env
	}
	prefix := name + "="
	for i, existing := range env {
		if strings.HasPrefix(existing, prefix) {
			env[i] = kv
			return env
		}
	}
	return append(env, kv)
}

func startServedNativeStrictServer(t *testing.T, root, cwd string, extraBackends []engine.Backend) (*Server, *servedNativeStrictHandle, servedNativeCodexFixture) {
	t.Helper()
	fixture := installServedNativeCodexFixture(t, root)
	server, h, fixture := startServedNativeStrictServerWithFixture(t, root, cwd, fixture, extraBackends)
	return server, h, fixture
}

func startServedNativeStrictServerWithFixture(t *testing.T, root, cwd string, fixture servedNativeCodexFixture, extraBackends []engine.Backend) (*Server, *servedNativeStrictHandle, servedNativeCodexFixture) {
	t.Helper()
	requireServedNativeConformance(t)
	backends := append([]engine.Backend{servedNativeCodexBackend(fixture)}, extraBackends...)
	cfg, err := StrictAdmissionConfig(Config{
		StateRoot:   root,
		CWD:         cwd,
		Token:       "test-token",
		Backends:    backends,
		IdleTimeout: -1,
	}, servedNativeStrictOptions(builtServedNativeAgentbusPath(t), fixture.env))
	if err != nil {
		t.Fatalf("StrictAdmissionConfig() error = %v", err)
	}
	server, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
		close(done)
	}()
	h := &servedNativeStrictHandle{root: root, cwd: cwd, socketPath: filepath.Join(root, protocol.SocketName), token: "test-token", done: done, cancel: cancel}
	waitForSocket(t, h.socketPath, done)
	t.Cleanup(func() {
		stopServedNativeServer(t, h)
	})
	return server, h, fixture
}

func stopServedNativeServer(t *testing.T, h *servedNativeStrictHandle) {
	t.Helper()
	if h == nil || h.cancel == nil || h.done == nil {
		return
	}
	h.stopOnce.Do(func() {
		h.cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Fatalf("server shutdown error = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("server did not stop")
		}
	})
}

func closeServedNativeAdmission(t *testing.T, server *Server) {
	t.Helper()
	if server == nil {
		return
	}
	if server.admissionClose != nil || server.admissionRuntime != nil || server.admissionInstance != nil {
		if err := server.closeServeAdmission(); err != nil {
			t.Fatalf("admission close error = %v", err)
		}
	}
}

func servedNativeRPC(t *testing.T, h *servedNativeStrictHandle) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn := dialRaw(t, h.socketPath)
	reader := bufio.NewReader(conn)
	helloRaw(t, conn, reader, h.token)
	return conn, reader
}

func setServedNativeReleaseAfterSendBeforeAckHook(t *testing.T, hook func(func() error) error) {
	t.Helper()
	previous := parklaunchReleaseAfterSendBeforeAckForTest
	parklaunchReleaseAfterSendBeforeAckForTest = hook
	t.Cleanup(func() {
		parklaunchReleaseAfterSendBeforeAckForTest = previous
	})
}

func setServedNativeAllocateGrantBeforeCommitHook(t *testing.T, hook func() error) {
	t.Helper()
	previous := authorityAllocateGrantBeforeCommitForTest
	authorityAllocateGrantBeforeCommitForTest = hook
	t.Cleanup(func() {
		authorityAllocateGrantBeforeCommitForTest = previous
	})
}

func submitServedNativeJobOnConn(t *testing.T, conn net.Conn, reader *bufio.Reader, requestSuffix, cwd, prompt string, tags map[string]string) protocol.JobSubmitResult {
	t.Helper()
	resp := rpc(t, conn, reader, "submit-"+requestSuffix, protocol.MethodJobSubmit, servedNativeSubmitParams(requestSuffix, cwd, prompt, tags))
	var submitted protocol.JobSubmitResult
	decodeResult(t, resp, &submitted)
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}
	return submitted
}

func servedNativeSubmitParams(requestSuffix, cwd, prompt string, tags map[string]string) protocol.JobSubmitParams {
	return protocol.JobSubmitParams{
		WorkspaceKey: "workspace-served-native-" + requestSuffix,
		RequestID:    "request-served-native-" + requestSuffix,
		TaskSpec: protocol.TaskSpec{
			Backend: servedNativeBackendName,
			CWD:     cwd,
			Write:   false,
			Prompt:  servedNativePromptWithTags(prompt, tags),
			Tags:    tags,
		},
	}
}

func servedNativeStartedTags(path string) map[string]string {
	return map[string]string{servedNativeStartedPathTag: path}
}

func servedNativeFixtureEntryEnv(env []string, mode string, tags map[string]string) []string {
	if mode != "" {
		env = upsertEnv(env, servedNativeModeTag+"="+mode)
	}
	for key, value := range tags {
		if strings.HasPrefix(key, "SERVED_NATIVE_") && strings.TrimSpace(value) != "" {
			env = upsertEnv(env, key+"="+value)
		}
	}
	return env
}

func servedNativePromptWithTags(prompt string, tags map[string]string) string {
	if len(tags) == 0 {
		return prompt
	}
	keys := make([]string, 0, len(tags))
	for key, value := range tags {
		if strings.HasPrefix(key, "SERVED_NATIVE_") && strings.TrimSpace(value) != "" {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return prompt
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(prompt)
	if !strings.HasSuffix(prompt, "\n") {
		b.WriteByte('\n')
	}
	for _, key := range keys {
		fmt.Fprintf(&b, "%s=%s\n", key, tags[key])
	}
	return b.String()
}

func rpcRawParams(t *testing.T, conn net.Conn, r *bufio.Reader, id, method string, params json.RawMessage) protocol.Response {
	t.Helper()
	req := protocol.Request{JSONRPC: "2.0", ID: json.RawMessage(strconvQuote(id)), Method: method, Params: params}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(raw, '\n')); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		var head struct {
			ID     json.RawMessage `json:"id,omitempty"`
			Method string          `json:"method,omitempty"`
		}
		mustUnmarshal(t, line, &head)
		if head.Method != "" {
			continue
		}
		var resp protocol.Response
		mustUnmarshal(t, line, &resp)
		return resp
	}
}

func builtServedNativeAgentbusPath(t *testing.T) string {
	t.Helper()
	if override := strings.TrimSpace(os.Getenv(servedNativePrebuiltBinaryEnv)); override != "" {
		if !filepath.IsAbs(override) {
			abs, err := filepath.Abs(override)
			if err != nil {
				t.Fatalf("%s=%q cannot be made absolute: %v", servedNativePrebuiltBinaryEnv, override, err)
			}
			override = abs
		}
		info, err := os.Stat(override)
		if err != nil {
			t.Fatalf("%s=%q stat: %v", servedNativePrebuiltBinaryEnv, override, err)
		}
		if info.IsDir() || info.Mode()&0o111 == 0 {
			t.Fatalf("%s=%q must be an executable file, mode=%s", servedNativePrebuiltBinaryEnv, override, info.Mode())
		}
		return override
	}
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

type servedNativeCodexExecution struct {
	PID            int               `json:"pid"`
	PGID           int               `json:"pgid"`
	Mode           string            `json:"mode"`
	Prompt         string            `json:"prompt,omitempty"`
	Args           []string          `json:"args,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	GrandchildPID  int               `json:"grandchildPid,omitempty"`
	GrandchildPGID int               `json:"grandchildPgid,omitempty"`
}

type servedNativeCodexFixtureEntry struct {
	envTags       map[string]string
	entryRecorded bool
	entryPGID     int
}

type servedNativeAppServerFrame struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
}

func runServedNativeCodexFixture(args []string) int {
	if len(args) > 0 {
		switch args[0] {
		case "--version":
			fmt.Println("codex-cli " + codexcli.MinimumKnownGoodVersion)
			return 0
		case "--help", "help":
			fmt.Println("codex fixture")
			return 0
		case "app-server":
			return runServedNativeCodexAppServerFixture(args)
		}
	}
	envTags := servedNativeEnvTags()
	entryRecorded := false
	entryPGID := 0
	if path := envTags[servedNativeStartedPathTag]; path != "" {
		pgid, err := unix.Getpgid(0)
		if err != nil {
			return 3
		}
		entryPGID = pgid
		execution := servedNativeCodexExecution{
			PID:  os.Getpid(),
			PGID: pgid,
			Mode: servedNativeExecModeEntry,
			Args: append([]string(nil), args...),
			Tags: envTags,
		}
		if err := appendServedNativeCodexExecution(execution); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 4
		}
		if err := writeServedNativeStartedMarker(path, servedNativeEntryStartedMarker); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 4
		}
		entryRecorded = true
	}
	promptRaw, err := io.ReadAll(os.Stdin)
	if err != nil {
		return 2
	}
	prompt := string(promptRaw)
	mode := servedNativeFixtureModeClean
	switch {
	case strings.Contains(prompt, servedNativeFixtureModeGrandchild):
		mode = servedNativeFixtureModeGrandchild
	case strings.Contains(prompt, servedNativeFixtureModeHold):
		mode = servedNativeFixtureModeHold
	}
	pgid := entryPGID
	if pgid == 0 {
		var err error
		pgid, err = unix.Getpgid(0)
		if err != nil {
			return 3
		}
	}
	tags := mergeServedNativeTags(envTags, parseServedNativePromptTags(prompt))
	execution := servedNativeCodexExecution{
		PID:    os.Getpid(),
		PGID:   pgid,
		Mode:   mode,
		Prompt: prompt,
		Args:   append([]string(nil), args...),
		Tags:   tags,
	}
	if !entryRecorded && tags[servedNativeStartedPathTag] != "" {
		execution.Mode = servedNativeExecModeStdin
	}
	switch mode {
	case servedNativeFixtureModeClean:
		if !entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		writeServedNativeCodexResult()
		return 0
	case servedNativeFixtureModeHold:
		signal.Ignore(syscall.SIGTERM)
		if !entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		if path := execution.Tags[servedNativeStartedPathTag]; path != "" && !entryRecorded {
			_ = writeServedNativeStartedMarker(path, servedNativeStdinStartedMarker)
		}
		for {
			time.Sleep(time.Hour)
		}
	case servedNativeFixtureModeGrandchild:
		readyPath := filepath.Join(os.Getenv(servedNativeCodexReadyDirEnv), fmt.Sprintf("grandchild-%d-ready", os.Getpid()))
		exe, err := os.Executable()
		if err != nil {
			return 5
		}
		cmd := exec.Command(exe,
			"-test.run=^TestServedNativeConformanceGrandchildProcess$",
			"--",
			"--ready", readyPath,
		)
		cmd.Env = append(os.Environ(), servedNativeGrandchildEnv+"=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return 6
		}
		if err := waitServedNativeFile(readyPath, 5*time.Second); err != nil {
			_ = cmd.Process.Kill()
			return 7
		}
		childPGID, err := readServedNativeGrandchildPGID(readyPath)
		if err != nil {
			_ = cmd.Process.Kill()
			return 8
		}
		execution.GrandchildPID = cmd.Process.Pid
		execution.GrandchildPGID = childPGID
		if !entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				_ = cmd.Process.Kill()
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		writeServedNativeCodexResult()
		return 0
	default:
		return 2
	}
}

func runServedNativeCodexAppServerFixture(args []string) int {
	entry, code := startServedNativeCodexAppServerEntry(args)
	if code != 0 {
		return code
	}
	dec := json.NewDecoder(os.Stdin)
	enc := json.NewEncoder(os.Stdout)
	threadID := "served-native-session"
	for {
		var frame servedNativeAppServerFrame
		if err := dec.Decode(&frame); err != nil {
			if errors.Is(err, io.EOF) {
				return 0
			}
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		params := servedNativeAppServerParams(frame.Params)
		switch frame.Method {
		case "":
			continue
		case "initialize":
			if !servedNativeAppServerHasID(frame.ID) {
				continue
			}
			if err := servedNativeAppServerRespond(enc, frame.ID, servedNativeAppServerInitializeResult()); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		case "initialized":
			continue
		case "thread/start", "thread/resume":
			if id := servedNativeStringFromMap(params, "threadId", "thread_id", "id"); id != "" {
				threadID = id
			}
			if !servedNativeAppServerHasID(frame.ID) {
				continue
			}
			if err := servedNativeAppServerRespond(enc, frame.ID, servedNativeAppServerThreadResult(threadID)); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		case "turn/start":
			if id := servedNativeStringFromMap(params, "threadId", "thread_id"); id != "" {
				threadID = id
			}
			turnID := fmt.Sprintf("served-native-turn-%d", os.Getpid())
			if servedNativeAppServerHasID(frame.ID) {
				if err := servedNativeAppServerRespond(enc, frame.ID, servedNativeAppServerTurnResult(turnID)); err != nil {
					fmt.Fprintln(os.Stderr, err)
					return 4
				}
			}
			prompt := servedNativeAppServerPrompt(params)
			return runServedNativeCodexAppServerTurn(args, entry, prompt, func() error {
				return writeServedNativeCodexAppServerResult(enc, threadID, turnID)
			})
		case "model/list":
			if !servedNativeAppServerHasID(frame.ID) {
				continue
			}
			if err := servedNativeAppServerRespond(enc, frame.ID, servedNativeAppServerModelListResult()); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		default:
			if servedNativeAppServerHasID(frame.ID) {
				_ = servedNativeAppServerRespondError(enc, frame.ID, -32601, "unsupported fixture method: "+frame.Method)
			}
		}
	}
}

func startServedNativeCodexAppServerEntry(args []string) (servedNativeCodexFixtureEntry, int) {
	envTags := servedNativeEnvTags()
	entry := servedNativeCodexFixtureEntry{envTags: envTags}
	if path := envTags[servedNativeStartedPathTag]; path != "" {
		pgid, err := unix.Getpgid(0)
		if err != nil {
			return entry, 3
		}
		entry.entryPGID = pgid
		execution := servedNativeCodexExecution{
			PID:  os.Getpid(),
			PGID: pgid,
			Mode: servedNativeExecModeEntry,
			Args: append([]string(nil), args...),
			Tags: envTags,
		}
		if err := appendServedNativeCodexExecution(execution); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return entry, 4
		}
		if err := writeServedNativeStartedMarker(path, servedNativeEntryStartedMarker); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return entry, 4
		}
		entry.entryRecorded = true
	}
	return entry, 0
}

func runServedNativeCodexAppServerTurn(args []string, entry servedNativeCodexFixtureEntry, prompt string, complete func() error) int {
	mode := servedNativeFixtureModeClean
	switch {
	case strings.Contains(prompt, servedNativeFixtureModeGrandchild):
		mode = servedNativeFixtureModeGrandchild
	case strings.Contains(prompt, servedNativeFixtureModeHold):
		mode = servedNativeFixtureModeHold
	}
	pgid := entry.entryPGID
	if pgid == 0 {
		var err error
		pgid, err = unix.Getpgid(0)
		if err != nil {
			return 3
		}
	}
	tags := mergeServedNativeTags(entry.envTags, parseServedNativePromptTags(prompt))
	execution := servedNativeCodexExecution{
		PID:    os.Getpid(),
		PGID:   pgid,
		Mode:   mode,
		Prompt: prompt,
		Args:   append([]string(nil), args...),
		Tags:   tags,
	}
	if !entry.entryRecorded && tags[servedNativeStartedPathTag] != "" {
		execution.Mode = servedNativeExecModeStdin
	}
	switch mode {
	case servedNativeFixtureModeClean:
		if !entry.entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		if complete != nil {
			if err := complete(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		return 0
	case servedNativeFixtureModeHold:
		signal.Ignore(syscall.SIGTERM)
		if !entry.entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		if path := execution.Tags[servedNativeStartedPathTag]; path != "" && !entry.entryRecorded {
			_ = writeServedNativeStartedMarker(path, servedNativeStdinStartedMarker)
		}
		for {
			time.Sleep(time.Hour)
		}
	case servedNativeFixtureModeGrandchild:
		readyPath := filepath.Join(os.Getenv(servedNativeCodexReadyDirEnv), fmt.Sprintf("grandchild-%d-ready", os.Getpid()))
		exe, err := os.Executable()
		if err != nil {
			return 5
		}
		cmd := exec.Command(exe,
			"-test.run=^TestServedNativeConformanceGrandchildProcess$",
			"--",
			"--ready", readyPath,
		)
		cmd.Env = append(os.Environ(), servedNativeGrandchildEnv+"=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return 6
		}
		if err := waitServedNativeFile(readyPath, 5*time.Second); err != nil {
			_ = cmd.Process.Kill()
			return 7
		}
		childPGID, err := readServedNativeGrandchildPGID(readyPath)
		if err != nil {
			_ = cmd.Process.Kill()
			return 8
		}
		execution.GrandchildPID = cmd.Process.Pid
		execution.GrandchildPGID = childPGID
		if !entry.entryRecorded {
			if err := appendServedNativeCodexExecution(execution); err != nil {
				_ = cmd.Process.Kill()
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		if complete != nil {
			if err := complete(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 4
			}
		}
		return 0
	default:
		return 2
	}
}

func servedNativeAppServerHasID(id json.RawMessage) bool {
	return len(bytes.TrimSpace(id)) != 0
}

func servedNativeAppServerParams(raw json.RawMessage) map[string]any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return map[string]any{}
	}
	return params
}

func servedNativeAppServerRespond(enc *json.Encoder, id json.RawMessage, result any) error {
	return enc.Encode(map[string]any{"id": id, "result": result})
}

func servedNativeAppServerRespondError(enc *json.Encoder, id json.RawMessage, code int, message string) error {
	return enc.Encode(map[string]any{
		"id": id,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func servedNativeAppServerInitializeResult() map[string]any {
	return map[string]any{
		"codexHome":      os.TempDir(),
		"platformFamily": "unix",
		"platformOs":     goruntime.GOOS,
		"userAgent":      "codex-cli/" + codexcli.MinimumKnownGoodVersion,
	}
}

func servedNativeAppServerThreadResult(threadID string) map[string]any {
	return map[string]any{
		"threadId": threadID,
		"thread": map[string]any{
			"id":        threadID,
			"sessionId": threadID,
			"status":    "running",
			"source":    "app-server",
			"turns":     []any{},
		},
	}
}

func servedNativeAppServerTurnResult(turnID string) map[string]any {
	return map[string]any{
		"turnId": turnID,
		"turn": map[string]any{
			"id":     turnID,
			"items":  []any{},
			"status": "inProgress",
		},
	}
}

func servedNativeAppServerModelListResult() map[string]any {
	return map[string]any{
		"data": []any{
			map[string]any{
				"model": "gpt-5.5",
				"supportedReasoningEfforts": []any{
					"none",
					"minimal",
					"low",
					"medium",
					"high",
					"xhigh",
				},
			},
		},
	}
}

func servedNativeAppServerPrompt(params map[string]any) string {
	if text := servedNativeStringFromMap(params, "prompt", "text"); text != "" {
		return text
	}
	if text := servedNativeStringValue(params["input"]); text != "" {
		return text
	}
	return ""
}

func servedNativeStringFromMap(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if text := servedNativeStringValue(obj[key]); text != "" {
			return text
		}
	}
	return ""
}

func servedNativeStringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := servedNativeStringValue(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	case map[string]any:
		return servedNativeStringFromMap(v, "text", "content", "message", "prompt")
	default:
		return ""
	}
}

func writeServedNativeCodexAppServerResult(enc *json.Encoder, threadID, turnID string) error {
	itemID := "served-native-result"
	item := map[string]any{
		"id":   itemID,
		"type": "agentMessage",
		"text": servedNativeResultText,
	}
	itemParams := map[string]any{
		"threadId": threadID,
		"turnId":   turnID,
		"item":     item,
	}
	if err := enc.Encode(map[string]any{"method": "item/started", "params": itemParams}); err != nil {
		return err
	}
	if err := enc.Encode(map[string]any{
		"method": "item/agentMessage/delta",
		"params": map[string]any{
			"threadId": threadID,
			"turnId":   turnID,
			"itemId":   itemID,
			"delta":    servedNativeResultText,
		},
	}); err != nil {
		return err
	}
	if err := enc.Encode(map[string]any{"method": "item/completed", "params": itemParams}); err != nil {
		return err
	}
	return enc.Encode(map[string]any{
		"method": "turn/completed",
		"params": map[string]any{
			"threadId": threadID,
			"turnId":   turnID,
			"status":   "completed",
			"turn": map[string]any{
				"id":     turnID,
				"items":  []any{item},
				"status": "completed",
			},
		},
	})
}

func servedNativeEnvTags() map[string]string {
	tags := make(map[string]string)
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if strings.HasPrefix(key, "SERVED_NATIVE_") && value != "" {
			tags[key] = value
		}
	}
	return tags
}

func mergeServedNativeTags(tags ...map[string]string) map[string]string {
	merged := make(map[string]string)
	for _, tagSet := range tags {
		for key, value := range tagSet {
			if strings.HasPrefix(key, "SERVED_NATIVE_") && strings.TrimSpace(value) != "" {
				merged[key] = value
			}
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func writeServedNativeStartedMarker(path, value string) error {
	if path == "" {
		return nil
	}
	return os.WriteFile(path, []byte(value), 0o600)
}

func parseServedNativePromptTags(prompt string) map[string]string {
	tags := make(map[string]string)
	for _, line := range strings.Split(prompt, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if strings.HasPrefix(key, "SERVED_NATIVE_") && value != "" {
			tags[key] = value
		}
	}
	return tags
}

func writeServedNativeCodexResult() {
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(map[string]any{"type": "agent_message", "text": servedNativeResultText, "thread_id": "served-native-session"})
	_ = enc.Encode(map[string]any{"type": "turn.completed", "last_agent_message": servedNativeResultText, "thread_id": "served-native-session"})
}

func appendServedNativeCodexExecution(execution servedNativeCodexExecution) error {
	path := os.Getenv(servedNativeCodexExecLogEnv)
	if path == "" {
		return errors.New("codex execution log path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(execution)
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

func servedNativeHelperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" && i+1 <= len(os.Args) {
			return os.Args[i+1:], true
		}
	}
	return nil, false
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

func readServedNativeCodexExecutions(t *testing.T, fixture servedNativeCodexFixture) []servedNativeCodexExecution {
	t.Helper()
	raw, err := os.ReadFile(fixture.execLog)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var out []servedNativeCodexExecution
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var execution servedNativeCodexExecution
		if err := json.Unmarshal(line, &execution); err != nil {
			t.Fatalf("decode execution log %s: %v", fixture.execLog, err)
		}
		out = append(out, execution)
	}
	return out
}

func waitServedNativeCodexExecutions(t *testing.T, fixture servedNativeCodexFixture, want int, timeout time.Duration) []servedNativeCodexExecution {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []servedNativeCodexExecution
	for time.Now().Before(deadline) {
		got = readServedNativeCodexExecutions(t, fixture)
		if len(got) >= want {
			return got
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("codex executions = %d, want at least %d", len(got), want)
	return nil
}

func assertServedNativeCodexExecutionCount(t *testing.T, fixture servedNativeCodexFixture, want int) {
	t.Helper()
	got := readServedNativeCodexExecutions(t, fixture)
	if len(got) != want {
		t.Fatalf("codex execution count = %d (%+v), want %d", len(got), got, want)
	}
}

func assertServedNativeEntryEvidenceAbsentFor(t *testing.T, fixture servedNativeCodexFixture, markerPath string, interval time.Duration) {
	t.Helper()
	deadline := time.Now().Add(interval)
	for time.Now().Before(deadline) {
		if raw, err := os.ReadFile(markerPath); err == nil {
			t.Fatalf("backend marker %s exists while pre-grant commit is held: %q", markerPath, string(raw))
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read backend marker %s: %v", markerPath, err)
		}
		executions := readServedNativeCodexExecutions(t, fixture)
		if execution, ok := servedNativeEntryExecution(executions, markerPath); ok {
			t.Fatalf("entry-mode execution exists while pre-grant commit is held: %+v", execution)
		}
		if len(executions) > 0 {
			t.Fatalf("backend executions exist while pre-grant commit is held: %+v", executions)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func servedNativeEntryExecution(executions []servedNativeCodexExecution, markerPath string) (servedNativeCodexExecution, bool) {
	for _, execution := range executions {
		if execution.Mode == servedNativeExecModeEntry && execution.Tags[servedNativeStartedPathTag] == markerPath {
			return execution, true
		}
	}
	return servedNativeCodexExecution{}, false
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

func assertServedNativeExecWaitsForGrantRelease(t *testing.T, server *Server, fixture servedNativeCodexFixture, jobID, markerPath string, timeout time.Duration) servedNativeCodexExecution {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last model.SafetyRecord
	var lastExecutions []servedNativeCodexExecution
	lastMarker := "<missing>"
	for time.Now().Before(deadline) {
		record := loadAdmissionSafetyRecord(t, server, jobID)
		last = record
		launchProof, launchBound := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
		grantedAndReleased := launchBound && launchProof.Grant != nil && launchProof.Released != nil
		rawMarker, markerErr := os.ReadFile(markerPath)
		markerSeen := markerErr == nil
		if markerSeen {
			lastMarker = strconv.Quote(string(rawMarker))
		} else if !errors.Is(markerErr, os.ErrNotExist) {
			t.Fatalf("read backend marker %s: %v", markerPath, markerErr)
		}
		executions := readServedNativeCodexExecutions(t, fixture)
		lastExecutions = executions
		entryExecution, entrySeen := servedNativeEntryExecution(executions, markerPath)
		if len(executions) > 0 && !entrySeen {
			t.Fatalf("backend executions reached log without entry-mode evidence for %s; marker=%s executions=%+v record=%+v launch=%+v", markerPath, lastMarker, executions, record, launchProof)
		}
		if markerSeen || entrySeen {
			if !grantedAndReleased {
				t.Fatalf("entry evidence for %s exists before durable grant/release; marker=%s executions=%+v record=%+v launch=%+v", markerPath, lastMarker, executions, record, launchProof)
			}
		}
		if markerSeen {
			if got := string(rawMarker); got != servedNativeEntryStartedMarker {
				t.Fatalf("backend marker %s = %q, want %q; executions=%+v", markerPath, got, servedNativeEntryStartedMarker, executions)
			}
			if !entrySeen {
				t.Fatalf("backend marker %s is entry content but entry-mode execution log is absent; executions=%+v", markerPath, executions)
			}
			return entryExecution
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("entry-mode backend marker %s did not appear after %s; last marker=%s last executions=%+v last record=%+v", markerPath, timeout, lastMarker, lastExecutions, last)
	return servedNativeCodexExecution{}
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

func assertServedNativeRestartRecoveryTerminal(t *testing.T, record model.SafetyRecord) *model.LaunchProof {
	t.Helper()
	if record.Mode != model.ModeIdentifiedFenced {
		t.Fatalf("admission mode = %s, want IdentifiedFenced", record.Mode)
	}
	if record.Terminal == nil {
		t.Fatal("terminal is nil, want restart recovery terminal")
	}
	if record.Terminal.Cause != model.CauseDaemonRestartedAfterAuthorization {
		t.Fatalf("recovery terminal cause = %s, want daemon_restart_after_authorization", record.Terminal.Cause)
	}
	switch record.Terminal.Outcome {
	case model.OutcomeReaped:
		if record.Terminal.Proof != model.ProofContained {
			t.Fatalf("reaped recovery proof = %s, want contained", record.Terminal.Proof)
		}
		if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionVerifiedAbsent {
			t.Fatalf("reaped recovery cleanup disposition = %s, want %s", got, model.CleanupDispositionVerifiedAbsent)
		}
	case model.OutcomeOrphaned:
		if goruntime.GOOS != "darwin" {
			t.Fatalf("recovery terminal outcome = %s on %s, want reaped", record.Terminal.Outcome, goruntime.GOOS)
		}
		if record.Terminal.Proof != model.ProofUnresolvedAbsence {
			t.Fatalf("orphaned recovery proof = %s, want unresolved_absence", record.Terminal.Proof)
		}
		if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionUnresolved {
			t.Fatalf("orphaned recovery cleanup disposition = %s, want %s", got, model.CleanupDispositionUnresolved)
		}
	default:
		want := "reaped"
		if goruntime.GOOS == "darwin" {
			want = "reaped or orphaned"
		}
		t.Fatalf("recovery terminal outcome = %s, want %s", record.Terminal.Outcome, want)
	}
	launchProof, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launchProof.Group == nil || launchProof.Grant == nil {
		t.Fatalf("restart recovery launch proof incomplete: %+v", launchProof)
	}
	return launchProof
}

func assertServedNativeReleaseAckLossTerminal(t *testing.T, record model.SafetyRecord) *model.LaunchProof {
	t.Helper()
	if record.Mode != model.ModeIdentifiedFenced {
		t.Fatalf("admission mode = %s, want IdentifiedFenced", record.Mode)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeReaped {
		t.Fatalf("terminal = %+v, want reaped release-ack-loss terminal", record.Terminal)
	}
	if record.Terminal.Cause != model.CauseReleaseOutcomeUnknown {
		t.Fatalf("terminal cause = %s, want release_outcome_unknown", record.Terminal.Cause)
	}
	if record.Terminal.Proof != model.ProofContained {
		t.Fatalf("terminal proof = %s, want contained", record.Terminal.Proof)
	}
	if got := model.DeriveCleanupDisposition(record); got != model.CleanupDispositionVerifiedAbsent {
		t.Fatalf("cleanup disposition = %s, want %s", got, model.CleanupDispositionVerifiedAbsent)
	}
	launchProof, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launchProof.Group == nil || launchProof.Grant == nil || launchProof.Quiescence == nil {
		t.Fatalf("release-ack-loss launch proof incomplete: %+v", launchProof)
	}
	if launchProof.Released != nil {
		t.Fatalf("release-ack-loss launch release = %+v, want nil release record", launchProof.Released)
	}
	return launchProof
}

func assertServedNativeExecutionMetadata(t *testing.T, execution servedNativeCodexExecution, group model.GroupRef, wantGrandchild bool) {
	t.Helper()
	if execution.PID != group.Leader.PID || execution.PGID != group.PGID {
		t.Fatalf("execution metadata = %+v, want leader pid %d pgid %d", execution, group.Leader.PID, group.PGID)
	}
	if wantGrandchild {
		if execution.GrandchildPID <= 0 || execution.GrandchildPGID != group.PGID {
			t.Fatalf("execution metadata = %+v, want grandchild in group %d", execution, group.PGID)
		}
		return
	}
	if execution.GrandchildPID != 0 || execution.GrandchildPGID != 0 {
		t.Fatalf("execution metadata = %+v, want no grandchild", execution)
	}
}

func assertServedNativeNoAuthorityJobs(t *testing.T, server *Server) {
	t.Helper()
	if server == nil || server.admissionRepository == nil {
		t.Fatal("admission repository is not ready")
	}
	var count int
	if err := server.admissionRepository.View(context.Background(), func(tx repository.ReadTx) error {
		jobs, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		count = len(jobs)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("authority jobs = %d, want 0", count)
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
	leaderAbsent, leaderDetail, err := servedNativeExactLeaderAbsent(group)
	if err != nil {
		return false, "", err
	}
	cgroupPIDs, cgroupDetail, err := servedNativeRetainedCgroupPIDs(group.RetainedID)
	if err != nil {
		return false, "", err
	}
	detail := fmt.Sprintf("kill0=%v leader=%s cgroup=%s", killErr, leaderDetail, cgroupDetail)
	return killAbsent && leaderAbsent && len(cgroupPIDs) == 0, detail, nil
}

func servedNativeExactLeaderAbsent(group model.GroupRef) (bool, string, error) {
	identity := group.Leader
	if identity.PID <= 0 {
		return true, "leader=invalid", nil
	}
	if goruntime.GOOS == "darwin" {
		claim, err := procgroup.ReadProcessClaim(identity.PID)
		if errors.Is(err, procgroup.ErrProcessMissing) {
			return true, "leader=missing", nil
		}
		if err != nil {
			return false, "", err
		}
		matches := claim.PID == identity.PID && claim.PGID == group.PGID && claim.StartToken.String() == identity.HighResStartToken
		return !matches, fmt.Sprintf("leader_pid=%d pgid=%d starttoken=%s matches=%t", claim.PID, claim.PGID, claim.StartToken, matches), nil
	}
	snapshot, err := readServedNativeProcStat(identity.PID)
	if errors.Is(err, os.ErrNotExist) {
		return true, "leader_stat=missing", nil
	}
	if err != nil {
		return false, "", err
	}
	matches := snapshot.PID == identity.PID && snapshot.PGID == group.PGID && snapshot.StartTime == identity.HighResStartToken
	return !matches, fmt.Sprintf("leader_pid=%d pgid=%d starttime=%s matches=%t", snapshot.PID, snapshot.PGID, snapshot.StartTime, matches), nil
}

type servedNativeProcStat struct {
	PID       int
	PGID      int
	StartTime string
}

func readServedNativeProcStat(pid int) (servedNativeProcStat, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return servedNativeProcStat{}, err
	}
	return parseServedNativeProcStat(string(raw))
}

func parseServedNativeProcStat(stat string) (servedNativeProcStat, error) {
	stat = strings.TrimSpace(stat)
	rightParen := strings.LastIndex(stat, ")")
	if rightParen < 0 {
		return servedNativeProcStat{}, errors.New("proc stat missing command terminator")
	}
	leftParen := strings.Index(stat[:rightParen+1], "(")
	if leftParen < 0 {
		return servedNativeProcStat{}, errors.New("proc stat missing command start")
	}
	pidField := strings.TrimSpace(stat[:leftParen])
	pid, err := strconv.Atoi(pidField)
	if err != nil || pid <= 0 {
		return servedNativeProcStat{}, fmt.Errorf("proc stat invalid pid %q", pidField)
	}
	fields := strings.Fields(stat[rightParen+1:])
	if len(fields) < 20 {
		return servedNativeProcStat{}, fmt.Errorf("proc stat too short: got %d fields after command", len(fields))
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil || pgid <= 0 {
		return servedNativeProcStat{}, fmt.Errorf("proc stat invalid pgid %q", fields[2])
	}
	startTime := fields[19]
	if _, err := strconv.ParseUint(startTime, 10, 64); err != nil {
		return servedNativeProcStat{}, fmt.Errorf("proc stat invalid starttime %q", startTime)
	}
	return servedNativeProcStat{PID: pid, PGID: pgid, StartTime: startTime}, nil
}

func servedNativeRetainedCgroupPIDs(retainedID string) ([]int, string, error) {
	if retainedID == "" {
		return nil, "retained_id=empty", nil
	}
	leaf, err := servedNativeRetainedLeafName(retainedID)
	if err != nil {
		return nil, "", err
	}
	path := filepath.Join("/sys/fs/cgroup", leaf, "cgroup.procs")
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "cgroup.procs=missing", nil
	}
	if err != nil {
		return nil, "", err
	}
	var pids []int
	for _, field := range strings.Fields(string(raw)) {
		pid, err := strconv.Atoi(field)
		if err != nil {
			return nil, "", err
		}
		pids = append(pids, pid)
	}
	return pids, fmt.Sprintf("%s pids=%v", path, pids), nil
}

func servedNativeRetainedLeafName(retainedID string) (string, error) {
	parts := strings.Split(retainedID, ".")
	if len(parts) != 4 || parts[0] != "cg2a" {
		return "", fmt.Errorf("retained id %q does not carry cgroup identity", retainedID)
	}
	leaf, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	name := string(leaf)
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return "", fmt.Errorf("invalid retained leaf name %q", name)
	}
	return name, nil
}

func assertServedNativePIDAbsent(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := unix.Kill(pid, 0); errors.Is(err, unix.ESRCH) {
			return
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("pid %d still exists after %s", pid, timeout)
}

type servedNativeDaemonHelper struct {
	cmd          *exec.Cmd
	done         chan error
	output       *servedNativeLockedBuffer
	killed       atomic.Bool
	root         string
	cwd          string
	agentbusPath string
	startedPath  string
	jobPath      string
	paramsPath   string
}

type servedNativeLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *servedNativeLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *servedNativeLockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
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

func readServedNativeSubmitParams(t *testing.T, path string) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		t.Fatalf("submit params file %s was empty", path)
	}
	if !json.Valid(raw) {
		t.Fatalf("submit params file %s was not valid JSON: %s", path, string(raw))
	}
	return json.RawMessage(raw)
}

func startServedNativeDaemonHelper(t *testing.T, root, cwd, agentbusPath string, env []string, startedPath, jobPath, paramsPath string) *servedNativeDaemonHelper {
	t.Helper()
	assertServedNativeDaemonHelperEnv(t, env)
	exe := servedNativeTestBinaryPath(t)
	cmd := exec.Command(exe,
		"-test.run=^TestServedNativeConformanceDaemonProcess$",
		"-test.v",
		"--",
		"--root", root,
		"--cwd", cwd,
		"--agentbus", agentbusPath,
		"--started", startedPath,
		"--job", jobPath,
		"--params", paramsPath,
	)
	cmd.Env = append(env, servedNativeDaemonEnv+"=1")
	output := &servedNativeLockedBuffer{}
	cmd.Stdout = output
	cmd.Stderr = output
	helper := &servedNativeDaemonHelper{
		cmd:          cmd,
		done:         make(chan error, 1),
		output:       output,
		root:         root,
		cwd:          cwd,
		agentbusPath: agentbusPath,
		startedPath:  startedPath,
		jobPath:      jobPath,
		paramsPath:   paramsPath,
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start daemon helper: %v\n%s\noutput:\n%s", err, helper.diagnostics([]string{jobPath, startedPath, paramsPath}, err), output.String())
	}
	go func() {
		helper.done <- cmd.Wait()
		close(helper.done)
	}()
	t.Cleanup(func() {
		if helper.killed.Load() {
			return
		}
		_ = helper.cmd.Process.Kill()
		select {
		case <-helper.done:
		case <-time.After(2 * time.Second):
			t.Fatalf("daemon helper cleanup wait timed out\n%s\noutput:\n%s", helper.diagnostics([]string{jobPath, startedPath, paramsPath}, nil), helper.output.String())
		}
	})
	waitServedNativeHelperFiles(t, []string{jobPath, startedPath, paramsPath}, helper)
	return helper
}

func assertServedNativeDaemonHelperEnv(t *testing.T, env []string) {
	t.Helper()
	required := []string{
		"PATH",
		servedNativeCodexFixtureEnv,
		servedNativeCodexExecLogEnv,
		servedNativeCodexReadyDirEnv,
	}
	if os.Getenv(servedNativeCgroupConformanceEnv) != "" {
		required = append(required, servedNativeCgroupConformanceEnv)
	}
	for _, name := range required {
		if envValue(env, name) == "" {
			t.Fatalf("daemon helper env missing %s", name)
		}
	}
	if envValue(env, servedNativeCodexFixtureEnv) != "1" {
		t.Fatalf("daemon helper env %s = %q, want 1", servedNativeCodexFixtureEnv, envValue(env, servedNativeCodexFixtureEnv))
	}
}

func envValue(env []string, name string) string {
	prefix := name + "="
	for _, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			return strings.TrimPrefix(kv, prefix)
		}
	}
	return ""
}

func waitServedNativeHelperFiles(t *testing.T, paths []string, helper *servedNativeDaemonHelper) {
	t.Helper()
	deadline := time.Now().Add(servedNativeDaemonHelperReadyTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-helper.done:
			if servedNativeHelperBindDenied(helper.output.String()) {
				servedTestSkipOrFailBindDeniedf(t, "daemon helper", "%v\n%s\noutput:\n%s", err, helper.diagnostics(paths, err), helper.output.String())
			}
			t.Fatalf("daemon helper exited before markers were ready: %v\n%s\noutput:\n%s", err, helper.diagnostics(paths, err), helper.output.String())
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
	t.Fatalf("daemon helper markers %v did not become ready after %s\n%s\noutput:\n%s", paths, servedNativeDaemonHelperReadyTimeout, helper.diagnostics(paths, nil), helper.output.String())
}

func servedNativeHelperBindDenied(output string) bool {
	return servedTestBindDeniedOutput(output)
}

func (helper *servedNativeDaemonHelper) diagnostics(paths []string, waitErr error) string {
	if helper == nil || helper.cmd == nil {
		return "daemon helper: <nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "daemon helper argv: %q\n", helper.cmd.Args)
	fmt.Fprintf(&b, "daemon helper env: %s\n", servedNativeEnvSummary(helper.cmd.Env))
	fmt.Fprintf(&b, "daemon helper process: %s\n", servedNativeDaemonProcessState(helper.cmd, waitErr))
	fmt.Fprintf(&b, "daemon helper root: %s (%s)\n", helper.root, servedNativePathStatus(helper.root))
	fmt.Fprintf(&b, "daemon helper cwd: %s (%s)\n", helper.cwd, servedNativePathStatus(helper.cwd))
	exe := ""
	if len(helper.cmd.Args) > 0 {
		exe = helper.cmd.Args[0]
	}
	fmt.Fprintf(&b, "daemon helper test binary: %s (%s)\n", exe, servedNativePathStatus(exe))
	fmt.Fprintf(&b, "daemon helper agentbus binary: %s (%s)\n", helper.agentbusPath, servedNativePathStatus(helper.agentbusPath))
	if len(paths) == 0 {
		paths = []string{helper.jobPath, helper.startedPath, helper.paramsPath}
	}
	for _, path := range paths {
		fmt.Fprintf(&b, "daemon helper marker: %s (%s)\n", path, servedNativePathStatus(path))
	}
	return strings.TrimRight(b.String(), "\n")
}

func servedNativeEnvSummary(env []string) string {
	names := []string{
		"PATH",
		servedNativeCodexFixtureEnv,
		servedNativeCodexExecLogEnv,
		servedNativeCodexReadyDirEnv,
		servedNativeCgroupConformanceEnv,
		servedNativeOfflineModcacheEnv,
		servedNativeDaemonEnv,
		"GOFLAGS",
		"GOPROXY",
		"GOMODCACHE",
		"GOCACHE",
	}
	parts := make([]string, 0, len(names))
	for _, name := range names {
		value := envValue(env, name)
		if value == "" {
			value = "<unset>"
		}
		parts = append(parts, fmt.Sprintf("%s=%q", name, value))
	}
	return strings.Join(parts, " ")
}

func servedNativeDaemonProcessState(cmd *exec.Cmd, waitErr error) string {
	if cmd == nil {
		return "<nil command>"
	}
	if cmd.ProcessState == nil {
		if cmd.Process != nil {
			return fmt.Sprintf("running pid=%d waitErr=%v", cmd.Process.Pid, waitErr)
		}
		return fmt.Sprintf("not-started waitErr=%v", waitErr)
	}
	state := cmd.ProcessState
	parts := []string{
		fmt.Sprintf("pid=%d", state.Pid()),
		fmt.Sprintf("exited=%t", state.Exited()),
		fmt.Sprintf("success=%t", state.Success()),
		fmt.Sprintf("exitCode=%d", state.ExitCode()),
	}
	if status, ok := state.Sys().(syscall.WaitStatus); ok {
		parts = append(parts, fmt.Sprintf("waitStatus=%d", int(status)))
		if status.Signaled() {
			parts = append(parts, fmt.Sprintf("signal=%s", status.Signal()))
		} else {
			parts = append(parts, fmt.Sprintf("exitStatus=%d", status.ExitStatus()))
		}
	}
	if waitErr != nil {
		parts = append(parts, fmt.Sprintf("waitErr=%v", waitErr))
	}
	return strings.Join(parts, " ")
}

func servedNativePathStatus(path string) string {
	if path == "" {
		return "<empty>"
	}
	info, err := os.Stat(path)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("mode=%s size=%d", info.Mode(), info.Size())
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
