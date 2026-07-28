//go:build abd_strict_e2e

package served

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestProductionStrictCLIRecoverE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)

	first := launchProductionStrictDaemonE2B(t, agentbusPath, stateRoot, fixture.env)
	firstStopped := false
	t.Cleanup(func() {
		if !firstStopped {
			_ = first.KillAndWait()
		}
	})
	client := connectProductionStrictClient(t, stateRoot, make(chan error))
	params := servedNativeSubmitParams("recover-cli-e2b", cwd, servedNativeFixtureModeClean, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	submitted, err := client.JobSubmit(ctx, agentclient.JobSubmitParams(params))
	if err != nil {
		t.Fatalf("strict identified submit: %v", err)
	}
	status := waitProductionStrictClientTerminal(t, client, submitted.JobID, 15*time.Second, productionStrictDiagnostics{stateRoot: stateRoot, fixture: fixture})
	if status.State != engine.StateCompleted {
		t.Fatalf("job status = %+v, want completed", status)
	}
	result, err := client.JobResult(ctx, agentclient.JobResultParams{JobID: submitted.JobID})
	if err != nil {
		t.Fatalf("job.result: %v", err)
	}
	if result.State != engine.StateCompleted || result.Result == nil || result.Result.Text != servedNativeResultText {
		t.Fatalf("job.result = %+v, want completed result text", result)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	killProductionStrictDaemonE2B(t, first, "first daemon before recover")
	firstStopped = true

	report := runProductionStrictRecoverCLI(t, agentbusPath, stateRoot)
	if report.Mode != AdmissionRecoveryOnly.String() || report.WorkItems != 0 || report.RecoveryPasses == 0 {
		t.Fatalf("recovery report = %+v, want recovery-only clean pass", report)
	}

	second := launchProductionStrictDaemonE2B(t, agentbusPath, stateRoot, fixture.env)
	secondStopped := false
	t.Cleanup(func() {
		if !secondStopped {
			_ = second.KillAndWait()
		}
	})
	secondClient := connectProductionStrictClient(t, stateRoot, make(chan error))
	if secondClient.HelloResult().ProtocolVersion != protocol.Version {
		t.Fatalf("hello = %+v", secondClient.HelloResult())
	}
	if err := secondClient.Close(); err != nil {
		t.Fatalf("second client close: %v", err)
	}
	killProductionStrictDaemonE2B(t, second, "second daemon")
	secondStopped = true
	inspection, err := authority.InspectAdmissionRoot(context.Background(), stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Counts.RecoveryObligations != 0 {
		t.Fatalf("recovery obligations after clean restart = %d, want 0", inspection.Counts.RecoveryObligations)
	}
}

func TestProductionStrictCLIRecoverMissingRootE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	agentbusPath := builtServedNativeAgentbusPath(t)
	missingRoot := filepath.Join(t.TempDir(), "missing-root")
	cmd := exec.Command(agentbusPath, "admission", "recover", "--state-root", missingRoot)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatalf("recover missing root succeeded; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("recover missing root error = %T %v, want non-zero exit", err, err)
	}
	if !strings.Contains(stderr.String(), ErrAdmissionRootMissing.Error()) {
		t.Fatalf("recover missing root stderr = %q, want %q", stderr.String(), ErrAdmissionRootMissing)
	}
	if _, statErr := os.Stat(missingRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("missing root stat = %v, want not exist", statErr)
	}
}

func TestProductionStrictAutostartRestoresAfterDaemonExitE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)
	first := launchProductionStrictDaemonE2B(t, agentbusPath, stateRoot, fixture.env)
	killProductionStrictDaemonE2B(t, first, "first daemon before autostart")
	assertProductionStrictPIDAbsentE2B(t, first.PID, 5*time.Second)

	starter := newProductionStrictClientStarterE2B(agentbusPath, fixture.env)
	clientCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := agentclient.Connect(clientCtx, agentclient.Options{
		StateRoot:    stateRoot,
		CommandPath:  agentbusPath,
		StartTimeout: 10 * time.Second,
		Starter:      starter,
	})
	if err != nil {
		t.Fatalf("autostart connect after daemon exit: %v", err)
	}
	defer client.Close()
	started := starter.singleResult(t)
	t.Cleanup(func() {
		_ = started.KillAndWait()
		assertProductionStrictDaemonCountE2B(t, agentbusPath, 0)
	})
	if got := starter.starts.Load(); got != 1 {
		t.Fatalf("autostart starts = %d, want 1", got)
	}
	if started.PID == first.PID || started.PID <= 0 || started.ExistingDaemon {
		t.Fatalf("autostart result = %+v, first pid=%d; want one new daemon", started, first.PID)
	}
	if client.HelloResult().ProtocolVersion != protocol.Version {
		t.Fatalf("hello = %+v", client.HelloResult())
	}
	submitCtx, cancelSubmit := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelSubmit()
	submitted, err := client.JobSubmit(submitCtx, agentclient.JobSubmitParams(servedNativeSubmitParams("autostart-restart-e2b", cwd, servedNativeFixtureModeClean, nil)))
	if err != nil {
		t.Fatalf("submit after autostart: %v", err)
	}
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit after autostart = %+v, want accepted new job", submitted)
	}
	status := waitProductionStrictClientTerminal(t, client, submitted.JobID, 15*time.Second, productionStrictDiagnostics{stateRoot: stateRoot, fixture: fixture})
	if status.State != engine.StateCompleted {
		t.Fatalf("autostart job status = %+v, want completed", status)
	}
	assertProductionStrictDaemonCountE2B(t, agentbusPath, 1)
}

func TestProductionStrictJobCLIStatusResultCancelE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)

	first := launchProductionStrictDaemonE2B(t, agentbusPath, stateRoot, fixture.env)
	firstStopped := false
	t.Cleanup(func() {
		if !firstStopped {
			_ = first.KillAndWait()
		}
	})
	client := connectProductionStrictClient(t, stateRoot, make(chan error))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	completed, err := client.JobSubmit(ctx, agentclient.JobSubmitParams(servedNativeSubmitParams("cli-status-result-cancel-e2b", cwd, servedNativeFixtureModeClean, nil)))
	if err != nil {
		t.Fatalf("submit completed job: %v", err)
	}
	status := waitProductionStrictClientTerminal(t, client, completed.JobID, 15*time.Second, productionStrictDiagnostics{stateRoot: stateRoot, fixture: fixture})
	if status.State != engine.StateCompleted {
		t.Fatalf("completed job status = %+v", status)
	}

	statusOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 0, "status", "--job", completed.JobID, "--json")
	var cliStatus protocol.JobStatusResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(statusOut.stdout)), &cliStatus)
	if len(cliStatus.Jobs) != 1 || cliStatus.Jobs[0].JobID != completed.JobID || cliStatus.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("cli status = %+v", cliStatus)
	}
	resultOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 0, "result", "--job", completed.JobID, "--json")
	var cliResult protocol.JobResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(resultOut.stdout)), &cliResult)
	if cliResult.JobID != completed.JobID || cliResult.State != engine.StateCompleted || cliResult.Result == nil || cliResult.Result.Text != servedNativeResultText {
		t.Fatalf("cli result = %+v", cliResult)
	}
	cancelOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 0, "cancel", "--job", completed.JobID, "--json")
	var terminalCancel protocol.JobCancelResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(cancelOut.stdout)), &terminalCancel)
	if terminalCancel.JobID != completed.JobID || terminalCancel.State != engine.StateCompleted {
		t.Fatalf("terminal cli cancel = %+v", terminalCancel)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	killProductionStrictDaemonE2B(t, first, "first daemon before cli autostart")
	firstStopped = true
	assertProductionStrictPIDAbsentE2B(t, first.PID, 5*time.Second)

	statusOut = runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 0, "status", "--job", completed.JobID, "--json")
	mustUnmarshal(t, bytes.TrimSpace([]byte(statusOut.stdout)), &cliStatus)
	if len(cliStatus.Jobs) != 1 || cliStatus.Jobs[0].State != engine.StateCompleted {
		t.Fatalf("cli status after autostart = %+v", cliStatus)
	}
	autostartPID := readProductionStrictPIDFileE2B(t, stateRoot)
	autostartStopped := false
	t.Cleanup(func() {
		if autostartStopped {
			return
		}
		_ = syscall.Kill(autostartPID, syscall.SIGKILL)
		assertProductionStrictPIDAbsentE2B(t, autostartPID, 5*time.Second)
	})
	resultOut = runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 0, "result", "--job", completed.JobID, "--json")
	mustUnmarshal(t, bytes.TrimSpace([]byte(resultOut.stdout)), &cliResult)
	if cliResult.JobID != completed.JobID || cliResult.State != engine.StateCompleted || cliResult.Result == nil || cliResult.Result.Text != servedNativeResultText {
		t.Fatalf("cli result after autostart = %+v", cliResult)
	}

	secondClient := connectProductionStrictClient(t, stateRoot, make(chan error))
	defer secondClient.Close()
	startedPath := filepath.Join(stateRoot, "cli-cancel-started")
	running, err := secondClient.JobSubmit(ctx, agentclient.JobSubmitParams(servedNativeSubmitParams("cli-running-cancel-e2b", cwd, servedNativeFixtureModeHold, servedNativeStartedTags(startedPath))))
	if err != nil {
		t.Fatalf("submit running job: %v", err)
	}
	if err := waitServedNativeFile(startedPath, 10*time.Second); err != nil {
		t.Fatalf("running job did not start: %v", err)
	}
	runningStatusOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 2, "status", "--job", running.JobID, "--json")
	var runningStatus protocol.JobStatusResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(runningStatusOut.stdout)), &runningStatus)
	if len(runningStatus.Jobs) != 1 || runningStatus.Jobs[0].JobID != running.JobID || runningStatus.Jobs[0].State != engine.StateRunning {
		t.Fatalf("running cli status = %+v", runningStatus)
	}
	runningCancelOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 7, "cancel", "--job", running.JobID, "--json")
	var runningCancel protocol.JobCancelResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(runningCancelOut.stdout)), &runningCancel)
	if runningCancel.JobID != running.JobID || runningCancel.State != engine.StateCanceled {
		t.Fatalf("running cli cancel = %+v", runningCancel)
	}
	canceledResultOut := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 7, "result", "--job", running.JobID, "--json")
	var canceledResult protocol.JobResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(canceledResultOut.stdout)), &canceledResult)
	if canceledResult.JobID != running.JobID || canceledResult.State != engine.StateCanceled || canceledResult.Result != nil {
		t.Fatalf("canceled cli result = %+v", canceledResult)
	}
	executions := waitServedNativeCodexExecutions(t, fixture, 2, 10*time.Second)
	if err := secondClient.Close(); err != nil {
		t.Fatalf("second client close before repository read: %v", err)
	}
	if err := syscall.Kill(autostartPID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("kill autostart daemon before repository read: %v", err)
	}
	assertProductionStrictPIDAbsentE2B(t, autostartPID, 5*time.Second)
	autostartStopped = true

	canceledRecord := waitProductionStrictAdmissionTerminalFromRepository(t, stateRoot, running.JobID, 5*time.Second)
	canceledProof := assertServedNativeIdentifiedTerminal(t, canceledRecord, model.OutcomeCanceled)
	assertServedNativeExecutionMetadata(t, executions[1], *canceledProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *canceledProof.Group, 5*time.Second)
}

func TestProductionStrictSIGTERMMidJobGracefulShutdownE2B(t *testing.T) {
	requireProductionStrictSIGTERMGracefulShutdownE2BGate(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)
	daemon := startProductionStrictForegroundCommandE2B(t, agentbusPath, stateRoot, cwd, fixture.env)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stopProductionStrictCommand(t, daemon.cmd, daemon.done, &daemon.stdout, &daemon.stderr)
		}
	})
	client := connectProductionStrictForegroundClientE2B(t, stateRoot, daemon)
	defer client.Close()
	writeProductionStrictOwnerPIDFileE2B(t, stateRoot, daemon.cmd.Process.Pid)

	startedPath := filepath.Join(stateRoot, "sigterm-hold-started")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	submitted, err := client.JobSubmit(ctx, agentclient.JobSubmitParams(servedNativeSubmitParams("sigterm-shutdown-e2b", cwd, servedNativeFixtureModeHold, servedNativeStartedTags(startedPath))))
	if err != nil {
		t.Fatalf("submit hold job: %v", err)
	}
	if err := waitServedNativeFile(startedPath, 10*time.Second); err != nil {
		t.Fatalf("hold job did not start: %v", err)
	}
	execution := waitServedNativeCodexExecutions(t, fixture, 1, time.Second)[0]
	if err := syscall.Kill(daemon.cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM daemon: %v", err)
	}
	waitForSocketRemoved(t, filepath.Join(stateRoot, protocol.SocketName), daemon.done)
	_, err = client.JobSubmit(ctx, agentclient.JobSubmitParams(servedNativeSubmitParams("sigterm-reject-e2b", cwd, servedNativeFixtureModeClean, nil)))
	assertProductionStrictAdmissionClosingE2B(t, err)
	waitProductionStrictForegroundExitE2B(t, daemon)
	stopped = true
	assertProductionStrictSocketPIDRemovedE2B(t, stateRoot)
	assertProductionStrictPIDAbsentE2B(t, daemon.cmd.Process.Pid, 5*time.Second)

	report := runProductionStrictRecoverCLI(t, agentbusPath, stateRoot)
	record := waitProductionStrictAdmissionTerminalFromRepository(t, stateRoot, submitted.JobID, 10*time.Second)
	assertProductionStrictSIGTERMGracefulShutdownTerminalE2B(t, record, execution, report)
}

func TestProductionStrictSIGINTIdleGracefulShutdownE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)
	daemon := startProductionStrictForegroundCommandE2B(t, agentbusPath, stateRoot, cwd, fixture.env)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stopProductionStrictCommand(t, daemon.cmd, daemon.done, &daemon.stdout, &daemon.stderr)
		}
	})
	client := connectProductionStrictClient(t, stateRoot, daemon.done)
	writeProductionStrictOwnerPIDFileE2B(t, stateRoot, daemon.cmd.Process.Pid)
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	if err := syscall.Kill(daemon.cmd.Process.Pid, syscall.SIGINT); err != nil {
		t.Fatalf("SIGINT daemon: %v", err)
	}
	waitProductionStrictForegroundExitE2B(t, daemon)
	stopped = true
	assertProductionStrictSocketPIDRemovedE2B(t, stateRoot)
	assertProductionStrictPIDAbsentE2B(t, daemon.cmd.Process.Pid, 5*time.Second)
	report := runProductionStrictRecoverCLI(t, agentbusPath, stateRoot)
	if report.WorkItems != 0 {
		t.Fatalf("recover after graceful SIGINT = %+v, want zero work items", report)
	}
}

func TestProductionStrictCLINoDaemonAutostartsDarwinE2B(t *testing.T) {
	if strings.TrimSpace(os.Getenv(strictE2ERunEnv)) != "1" {
		t.Skipf("set %s=1 to run strict cli e2e", strictE2ERunEnv)
	}
	if testing.Short() {
		t.Skip("strict cli e2e is not run in short mode")
	}
	if runtime.GOOS != "darwin" {
		t.Skip("darwin autostart check is macOS-only")
	}
	agentbusPath := builtServedNativeAgentbusPath(t)
	root := shortTempDir(t)
	env := upsertEnv(os.Environ(), "HOME="+shortTempDir(t))
	result := runProductionStrictJobCLI(t, agentbusPath, root, env, 10, "status", "--job", "job_darwin_serves")
	if strings.Contains(result.stderr, "daemon startup failed") {
		t.Fatalf("agentbus status stderr=%q, did not expect daemon startup failure", result.stderr)
	}
	pid := readProductionStrictPIDFileE2B(t, root)
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		assertProductionStrictPIDAbsentE2B(t, pid, 5*time.Second)
	})
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("autostarted daemon pid %d not alive: %v", pid, err)
	}
}

func TestProductionStrictCLIOrphanedExitCodeE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)
	jobID := createProductionStrictOrphanedRootE2B(t, stateRoot)
	env := productionStrictSandboxedEnvE2B(t, fixture.env)

	result := runProductionStrictJobCLI(t, agentbusPath, stateRoot, env, 14, "status", "--job", jobID, "--json")
	var status protocol.JobStatusResult
	mustUnmarshal(t, bytes.TrimSpace([]byte(result.stdout)), &status)
	if len(status.Jobs) != 1 ||
		status.Jobs[0].JobID != jobID ||
		status.Jobs[0].State != engine.StateOrphaned ||
		status.Jobs[0].CleanupDisposition != model.CleanupDispositionUnresolved.String() {
		t.Fatalf("orphaned cli status = %+v, want terminal orphaned unresolved job %s", status, jobID)
	}
	pid := readProductionStrictPIDFileE2B(t, stateRoot)
	t.Cleanup(func() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		assertProductionStrictPIDAbsentE2B(t, pid, 5*time.Second)
	})
}

func TestProductionStrictCLIStatusFailStopExitE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	agentbusPath := builtServedNativeAgentbusPath(t)
	stateRoot := shortTempDir(t)
	done := startFailStoppedProtocolDaemonE2B(t, stateRoot)
	result := runProductionStrictJobCLI(t, agentbusPath, stateRoot, os.Environ(), 12, "status", "--job", "job_failstop", "--json")
	if !strings.Contains(result.stderr, "code=backend_unavailable") || !strings.Contains(result.stderr, "admissionCause=root_fail_stopped") {
		t.Fatalf("fail-stop stderr = %q, want typed code and admission cause", result.stderr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fail-stop protocol daemon did not finish")
	}
}

func TestProductionStrictCLIStatusPersistedFailStopAutostartExitE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	agentbusPath := builtServedNativeAgentbusPath(t)
	stateRoot := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)

	first := launchProductionStrictDaemonE2B(t, agentbusPath, stateRoot, fixture.env)
	killProductionStrictDaemonE2B(t, first, "daemon before persisted fail-stop autostart")
	assertProductionStrictPIDAbsentE2B(t, first.PID, 5*time.Second)

	persistProductionStrictFailStopE2B(t, stateRoot, "production strict E2B persisted fail-stop")
	result := runProductionStrictJobCLI(t, agentbusPath, stateRoot, fixture.env, 12, "status", "--job", "job_persisted_failstop", "--json")
	if result.stdout != "" {
		t.Fatalf("persisted fail-stop stdout = %q, want empty", result.stdout)
	}
	for _, want := range []string{
		"code=backend_unavailable",
		"admissionCause=root_fail_stopped",
		ErrSafetyFailStopped.Error(),
		authority.ErrFailStopped.Error(),
	} {
		if !strings.Contains(result.stderr, want) {
			t.Fatalf("persisted fail-stop stderr = %q, want %q", result.stderr, want)
		}
	}
	assertProductionStrictDaemonCountE2B(t, agentbusPath, 0)
}

func TestProductionStrictAutostartRaceConvergesOneDaemonE2B(t *testing.T) {
	requireProductionStrictE2BGate(t)
	stateRoot := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)
	starter := newProductionStrictClientStarterE2B(agentbusPath, fixture.env)
	start := make(chan struct{})
	errs := make(chan error, 3)
	clients := make(chan *agentclient.Client, 2)
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			client, err := agentclient.Connect(ctx, agentclient.Options{
				StateRoot:    stateRoot,
				CommandPath:  agentbusPath,
				StartTimeout: 10 * time.Second,
				Starter:      starter,
			})
			if err != nil {
				errs <- fmt.Errorf("client autostart %d: %w", i, err)
				return
			}
			clients <- client
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		cmd := exec.Command(agentbusPath, "serve")
		cmd.Env = upsertEnv(fixture.env, "AGENTBUS_STATE_ROOT="+stateRoot)
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			errs <- fmt.Errorf("cli background serve: %w stdout=%s stderr=%s", err, stdout.String(), stderr.String())
		}
	}()

	close(start)
	wg.Wait()
	close(errs)
	close(clients)
	for err := range errs {
		t.Fatal(err)
	}
	var opened []*agentclient.Client
	for client := range clients {
		opened = append(opened, client)
	}
	if len(opened) != 2 {
		t.Fatalf("connected clients = %d, want 2", len(opened))
	}
	for _, client := range opened {
		defer client.Close()
		if client.HelloResult().ProtocolVersion != protocol.Version {
			t.Fatalf("client hello = %+v", client.HelloResult())
		}
	}
	if raw, err := os.ReadFile(filepath.Join(stateRoot, protocol.TokenFileName)); err != nil || strings.TrimSpace(string(raw)) == "" {
		t.Fatalf("token read = %q, %v; want one non-empty token", raw, err)
	}
	pidRaw, err := os.ReadFile(filepath.Join(stateRoot, "agentbus.pid"))
	if err != nil {
		t.Fatalf("pid file: %v", err)
	}
	pidText := strings.TrimSpace(string(pidRaw))
	if pidText == "" || strings.Contains(pidText, "\n") {
		t.Fatalf("pid file = %q, want one pid", string(pidRaw))
	}
	pid := parsePositivePIDE2B(t, pidText)
	var launched []daemonlaunch.Result
	t.Cleanup(func() {
		reaped := false
		for _, result := range launched {
			if result.PID == pid {
				_ = result.KillAndWait()
				reaped = true
			}
		}
		if !reaped {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		assertProductionStrictPIDAbsentE2B(t, pid, 5*time.Second)
		assertProductionStrictDaemonCountE2B(t, agentbusPath, 0)
	})
	launched = starter.resultsForStarts(t)
	assertProductionStrictDaemonCountE2B(t, agentbusPath, 1)
}

func requireProductionStrictE2BGate(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(strictE2ERunEnv)) != "1" {
		t.Skipf("set %s=1 to run E2B strict e2e", strictE2ERunEnv)
	}
	if testing.Short() {
		t.Skip("strict E2B e2e is not run in short mode")
	}
	if runtime.GOOS != "linux" {
		if runtime.GOOS == "darwin" {
			requireServedNativeConformance(t)
			return
		}
		t.Skip("strict E2B e2e gate runs on darwin or linux")
	}
	requireProductionStrictCgroup(t)
}

func requireProductionStrictSIGTERMGracefulShutdownE2BGate(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(strictE2ERunEnv)) != "1" {
		t.Skipf("set %s=1 to run E2B strict e2e", strictE2ERunEnv)
	}
	if testing.Short() {
		t.Skip("strict E2B e2e is not run in short mode")
	}
	switch runtime.GOOS {
	case "linux":
		requireProductionStrictCgroup(t)
	case "darwin":
		requireServedNativeConformance(t)
	default:
		t.Skip("strict E2B SIGTERM graceful shutdown requires darwin process groups or linux cgroup-v2")
	}
}

func launchProductionStrictDaemonE2B(t *testing.T, agentbusPath, stateRoot string, env []string) daemonlaunch.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result, err := daemonlaunch.Launch(ctx, daemonlaunch.Options{
		CommandPath: agentbusPath,
		Args:        []string{"serve", "--foreground"},
		StateRoot:   stateRoot,
		Timeout:     15 * time.Second,
		Starter:     servedLaunchProcessStarter,
		Env:         env,
	})
	if err != nil {
		if productionStrictBindDeniedE2B(err.Error()) {
			servedTestSkipOrFailBindDenied(t, "production strict daemon", err)
		}
		t.Fatalf("launch production strict daemon: %v", err)
	}
	if result.PID <= 0 || result.ExistingDaemon {
		t.Fatalf("launch result = %+v, want new daemon pid", result)
	}
	return result
}

type productionStrictForegroundCommandE2B struct {
	cmd    *exec.Cmd
	done   chan error
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func startProductionStrictForegroundCommandE2B(t *testing.T, agentbusPath, stateRoot, cwd string, env []string) *productionStrictForegroundCommandE2B {
	t.Helper()
	daemon := &productionStrictForegroundCommandE2B{}
	daemon.cmd = exec.Command(agentbusPath, "serve", "--foreground")
	daemon.cmd.Dir = cwd
	daemon.cmd.Env = upsertEnv(env, "AGENTBUS_STATE_ROOT="+stateRoot)
	daemon.cmd.Stdout = &daemon.stdout
	daemon.cmd.Stderr = &daemon.stderr
	if err := daemon.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	daemon.done = make(chan error, 1)
	go func() {
		daemon.done <- daemon.cmd.Wait()
		close(daemon.done)
	}()
	return daemon
}

func connectProductionStrictForegroundClientE2B(t *testing.T, stateRoot string, daemon *productionStrictForegroundCommandE2B) *agentclient.Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-daemon.done:
			if productionStrictBindDeniedE2B(daemon.stderr.String()) {
				servedTestSkipOrFailBindDeniedf(t, "foreground daemon", "%v\nstdout=%s\nstderr=%s", err, daemon.stdout.String(), daemon.stderr.String())
			}
			t.Fatalf("Serve exited before client connection: %v\nstdout=%s\nstderr=%s", err, daemon.stdout.String(), daemon.stderr.String())
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		client, err := agentclient.Connect(ctx, agentclient.Options{
			StateRoot:        stateRoot,
			DisableAutoStart: true,
		})
		cancel()
		if err == nil {
			return client
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("client did not connect to production strict server: %v\nstdout=%s\nstderr=%s", lastErr, daemon.stdout.String(), daemon.stderr.String())
	return nil
}

func productionStrictBindDeniedE2B(output string) bool {
	return servedTestBindDeniedOutput(output)
}

func writeProductionStrictOwnerPIDFileE2B(t *testing.T, stateRoot string, pid int) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(stateRoot, "agentbus.pid"), []byte(fmt.Sprintf("%d\n", pid)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitProductionStrictForegroundExitE2B(t *testing.T, daemon *productionStrictForegroundCommandE2B) {
	t.Helper()
	select {
	case err := <-daemon.done:
		if err != nil {
			t.Fatalf("foreground daemon exited with error: %v\nstdout=%s\nstderr=%s", err, daemon.stdout.String(), daemon.stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("foreground daemon did not exit\nstdout=%s\nstderr=%s", daemon.stdout.String(), daemon.stderr.String())
	}
}

func assertProductionStrictAdmissionClosingE2B(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("submit during shutdown succeeded, want admission_closing")
	}
	var rpcErr *protocol.RPCError
	if !errors.As(err, &rpcErr) {
		t.Fatalf("submit during shutdown error = %T %v, want RPC admission_closing", err, err)
	}
	if rpcErr.Object.Data.AdmissionCause != protocol.AdmissionRejectAdmissionClosing {
		t.Fatalf("submit during shutdown error = %+v, want admission_closing", rpcErr.Object)
	}
}

func assertProductionStrictSocketPIDRemovedE2B(t *testing.T, stateRoot string) {
	t.Helper()
	for _, path := range []string{filepath.Join(stateRoot, protocol.SocketName), filepath.Join(stateRoot, "agentbus.pid")} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s stat = %v, want not exist", path, err)
		}
	}
}

func assertProductionStrictSIGTERMGracefulShutdownTerminalE2B(t *testing.T, record model.SafetyRecord, execution servedNativeCodexExecution, report AdmissionRecoveryReport) {
	t.Helper()
	if record.Terminal == nil {
		t.Fatal("shutdown terminal is nil")
	}
	launchProof, ok := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
	if !ok || launchProof.Group == nil || launchProof.Grant == nil {
		t.Fatalf("shutdown launch proof incomplete: %+v", launchProof)
	}
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
	if err := validateProductionStrictSIGTERMGracefulShutdownTerminalE2B(record, *launchProof, report); err != nil {
		t.Fatal(err)
	}
}

func validateProductionStrictSIGTERMGracefulShutdownTerminalE2B(record model.SafetyRecord, launchProof model.LaunchProof, report AdmissionRecoveryReport) error {
	if record.Terminal == nil {
		return fmt.Errorf("shutdown terminal is nil")
	}
	if report.WorkItems != 0 || report.QuiescedLaunches != 0 || report.FinalizedJobs != 0 || report.OrphanedJobs != 0 || report.UnresolvedLaunches != 0 {
		return fmt.Errorf("recover after graceful SIGTERM = %+v, want no post-shutdown recovery obligation", report)
	}
	if record.Terminal.Outcome != model.OutcomeCanceled {
		return fmt.Errorf("shutdown terminal outcome = %s, want %s", record.Terminal.Outcome, model.OutcomeCanceled)
	}
	if record.Terminal.Cause != model.CauseCanceledAfterAuthorization {
		return fmt.Errorf("shutdown terminal cause = %s, want %s", record.Terminal.Cause, model.CauseCanceledAfterAuthorization)
	}
	switch got := model.DeriveCleanupDisposition(record); got {
	case model.CleanupDispositionVerifiedAbsent:
		if launchProof.Quiescence == nil || launchProof.Quiescence.Method != model.QuiescenceTermKill {
			return fmt.Errorf("verified shutdown quiescence = %+v, want term_kill", launchProof.Quiescence)
		}
	case model.CleanupDispositionUnresolved:
		if record.Terminal.Proof != model.ProofUnresolvedAbsence {
			return fmt.Errorf("unresolved shutdown proof = %s, want %s", record.Terminal.Proof, model.ProofUnresolvedAbsence)
		}
	default:
		return fmt.Errorf("shutdown cleanup disposition = %s, want %s or %s", got, model.CleanupDispositionVerifiedAbsent, model.CleanupDispositionUnresolved)
	}
	return nil
}

func TestProductionStrictSIGTERMGracefulShutdownValidationRejectsRecoveryOrphanedE2B(t *testing.T) {
	record := model.SafetyRecord{
		Terminal: &model.TerminalCertificate{
			Outcome: model.OutcomeOrphaned,
			Proof:   model.ProofUnresolvedAbsence,
			Cause:   model.CauseSupervisorLostAfterAuthorization,
		},
	}
	if err := validateProductionStrictSIGTERMGracefulShutdownTerminalE2B(record, model.LaunchProof{}, AdmissionRecoveryReport{
		WorkItems:    1,
		OrphanedJobs: 1,
	}); err == nil || !strings.Contains(err.Error(), "no post-shutdown recovery obligation") {
		t.Fatalf("validation error = %v, want recovery obligation rejection", err)
	}
	if err := validateProductionStrictSIGTERMGracefulShutdownTerminalE2B(record, model.LaunchProof{}, AdmissionRecoveryReport{}); err == nil || !strings.Contains(err.Error(), "want canceled") {
		t.Fatalf("validation error = %v, want orphaned terminal rejection", err)
	}
}

func killProductionStrictDaemonE2B(t *testing.T, result daemonlaunch.Result, description string) {
	t.Helper()
	if err := result.KillAndWait(); err != nil && !processExitSignalE2B(err, syscall.SIGKILL) {
		t.Fatalf("kill %s: %v", description, err)
	}
}

func processExitSignalE2B(err error, signal syscall.Signal) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ProcessState == nil {
		return false
	}
	status, ok := exitErr.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == signal
}

func runProductionStrictRecoverCLI(t *testing.T, agentbusPath, stateRoot string) AdmissionRecoveryReport {
	t.Helper()
	cmd := exec.Command(agentbusPath, "admission", "recover", "--state-root", stateRoot, "--json")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("agentbus admission recover failed: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
	}
	var report AdmissionRecoveryReport
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &report); err != nil {
		t.Fatalf("decode recovery report %q: %v", stdout.String(), err)
	}
	return report
}

type productionStrictCLIResultE2B struct {
	code   int
	stdout string
	stderr string
}

func runProductionStrictJobCLI(t *testing.T, agentbusPath, stateRoot string, env []string, wantCode int, args ...string) productionStrictCLIResultE2B {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, agentbusPath, args...)
	cmd.Env = upsertEnv(env, "AGENTBUS_STATE_ROOT="+stateRoot)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		} else {
			t.Fatalf("agentbus %s failed to run: %v\nstdout=%s\nstderr=%s", strings.Join(args, " "), err, stdout.String(), stderr.String())
		}
	}
	if ctx.Err() != nil {
		t.Fatalf("agentbus %s timed out: %v\nstdout=%s\nstderr=%s", strings.Join(args, " "), ctx.Err(), stdout.String(), stderr.String())
	}
	result := productionStrictCLIResultE2B{code: code, stdout: stdout.String(), stderr: stderr.String()}
	if result.code != wantCode {
		if productionStrictBindDeniedE2B(result.stderr) {
			servedTestSkipOrFailBindDeniedf(t, "agentbus "+strings.Join(args, " "), "stderr=%s", result.stderr)
		}
		t.Fatalf("agentbus %s exit=%d want=%d\nstdout=%s\nstderr=%s", strings.Join(args, " "), result.code, wantCode, result.stdout, result.stderr)
	}
	return result
}

func productionStrictSandboxedEnvE2B(t *testing.T, env []string) []string {
	t.Helper()
	home := shortTempDir(t)
	env = upsertEnv(env, "HOME="+home)
	env = upsertEnv(env, "XDG_CACHE_HOME="+filepath.Join(home, "cache"))
	return env
}

func readProductionStrictPIDFileE2B(t *testing.T, stateRoot string) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(stateRoot, "agentbus.pid"))
	if err != nil {
		t.Fatalf("read agentbus.pid: %v", err)
	}
	return parsePositivePIDE2B(t, strings.TrimSpace(string(raw)))
}

func startFailStoppedProtocolDaemonE2B(t *testing.T, stateRoot string) <-chan error {
	t.Helper()
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateRoot, protocol.TokenFileName), []byte("test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(stateRoot, protocol.SocketName)
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.Remove(socketPath)
	})
	done := make(chan error, 1)
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		dec := json.NewDecoder(conn)
		enc := json.NewEncoder(conn)
		var hello protocol.Request
		if err := dec.Decode(&hello); err != nil {
			done <- err
			return
		}
		if hello.Method != protocol.MethodHello {
			done <- fmt.Errorf("first method = %s, want hello", hello.Method)
			return
		}
		if err := enc.Encode(protocol.Response{
			JSONRPC: "2.0",
			ID:      hello.ID,
			Result: protocol.HelloResult{
				ProtocolVersion: protocol.Version,
				Backends:        []string{"codex"},
				Capabilities:    protocol.DefaultCapabilities(),
			},
		}); err != nil {
			done <- err
			return
		}
		var status protocol.Request
		if err := dec.Decode(&status); err != nil {
			done <- err
			return
		}
		if status.Method != protocol.MethodJobStatus {
			done <- fmt.Errorf("second method = %s, want job.status", status.Method)
			return
		}
		errObj := protocol.NewError(protocol.ErrorBackendUnavailable, "authority fail-stopped", protocol.ErrorData{AdmissionCause: protocol.AdmissionRejectRootFailStopped})
		if err := enc.Encode(protocol.Response{JSONRPC: "2.0", ID: status.ID, Error: errObj}); err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	return done
}

func persistProductionStrictFailStopE2B(t *testing.T, stateRoot, reason string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo, err := openReadOnlyAdmissionRepositoryWithContentionRetry(ctx, filepath.Join(stateRoot, admissionRepositoryFile), filepath.Join(stateRoot, protocol.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	dbUUID, schemaMajor, identityErr := repo.AnchorIdentity()
	closeErr := repo.Close()
	if identityErr != nil {
		t.Fatal(identityErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	boot, err := model.NewBootRef("boot-production-strict-fail-stop-e2b", "owner-production-strict-fail-stop-e2b")
	if err != nil {
		t.Fatal(err)
	}
	anchor := authority.NewFileAnchor(filepath.Join(stateRoot, admissionAnchorFile), dbUUID, schemaMajor)
	if err := anchor.FailStop(ctx, boot, reason); err != nil {
		t.Fatal(err)
	}
	inspection, err := authority.InspectAdmissionRoot(ctx, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.FailStopped || !strings.Contains(inspection.FailStopReason, reason) {
		t.Fatalf("inspection after fail-stop = %+v, want reason %q", inspection, reason)
	}
}

type productionStrictClientStarterE2B struct {
	agentbusPath string
	env          []string
	starts       atomic.Int64
	results      chan daemonlaunch.Result
}

func newProductionStrictClientStarterE2B(agentbusPath string, env []string) *productionStrictClientStarterE2B {
	return &productionStrictClientStarterE2B{
		agentbusPath: agentbusPath,
		env:          append([]string(nil), env...),
		results:      make(chan daemonlaunch.Result, 3),
	}
}

func (s *productionStrictClientStarterE2B) StartDaemon(ctx context.Context, opts agentclient.StartOptions) (agentclient.StartResult, error) {
	s.starts.Add(1)
	result, err := daemonlaunch.Launch(ctx, daemonlaunch.Options{
		CommandPath: s.agentbusPath,
		Args:        []string{"serve", "--foreground"},
		StateRoot:   opts.StateRoot,
		SocketPath:  opts.SocketPath,
		TokenPath:   opts.TokenPath,
		Timeout:     opts.Timeout,
		Starter:     servedLaunchProcessStarter,
		Env:         s.env,
	})
	if err != nil {
		return agentclient.StartResult{}, err
	}
	s.results <- result
	return agentclient.StartResult{PID: result.PID, ExistingDaemon: result.ExistingDaemon}, nil
}

func (s *productionStrictClientStarterE2B) singleResult(t *testing.T) daemonlaunch.Result {
	t.Helper()
	select {
	case result := <-s.results:
		return result
	case <-time.After(time.Second):
		t.Fatal("starter did not record a launch result")
		return daemonlaunch.Result{}
	}
}

func (s *productionStrictClientStarterE2B) resultsForStarts(t *testing.T) []daemonlaunch.Result {
	t.Helper()
	starts := int(s.starts.Load())
	results := make([]daemonlaunch.Result, 0, starts)
	for i := 0; i < starts; i++ {
		select {
		case result := <-s.results:
			results = append(results, result)
		case <-time.After(time.Second):
			t.Fatalf("starter recorded %d starts but only %d results", starts, len(results))
		}
	}
	return results
}

func parsePositivePIDE2B(t *testing.T, raw string) int {
	t.Helper()
	var pid int
	if _, err := fmt.Sscanf(raw, "%d", &pid); err != nil || pid <= 0 {
		t.Fatalf("pid = %q, parse err=%v", raw, err)
	}
	return pid
}

func assertProductionStrictPIDAbsentE2B(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d is still present", pid)
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
}

func assertProductionStrictDaemonCountE2B(t *testing.T, agentbusPath string, want int) {
	t.Helper()
	got, err := productionStrictDaemonPIDsE2B(agentbusPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != want {
		t.Fatalf("agentbus daemon pids for %s = %v, want %d", agentbusPath, got, want)
	}
}

func createProductionStrictOrphanedRootE2B(t *testing.T, stateRoot string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := authority.ResetEmptyAdmissionRoot(ctx, stateRoot); err != nil {
		t.Fatal(err)
	}
	repo, err := bboltrepo.OpenExisting(filepath.Join(stateRoot, admissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := repo.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_, verifier := custodian.NewAttestationChannel()
	bootstrapper, err := authority.NewBootstrapper(
		repo,
		authority.WithAnchor(authority.NewFileAnchor(filepath.Join(stateRoot, admissionAnchorFile), dbUUID, schemaMajor)),
		authority.WithQuiescenceVerifier(verifier),
	)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := model.NewBootRef("boot-production-strict-orphaned-e2b", "owner-production-strict-orphaned-e2b")
	if err != nil {
		t.Fatal(err)
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := session.ActivateRoot(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := ready.Accept(ctx, authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: "workspace-production-strict-orphaned-e2b",
			RequestID:    "request-production-strict-orphaned-e2b",
		},
		WorkspaceLayoutKey: model.WorkspaceKey(strings.Repeat("b", 64)),
		TaskIdentity:       model.NewSHA256TaskIdentity([]byte("production-strict-orphaned-e2b")),
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          "session-production-strict-orphaned-e2b",
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := accepted.Record.Attempt.Ref
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne})
	if _, err := ready.BindGroup(ctx, accepted.Record.JobID, ref, model.LaunchOrdinalOne, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ready.AllocateGrant(ctx, ref, model.LaunchOrdinalOne); err != nil {
		t.Fatal(err)
	}
	finalized, err := ready.Finalize(ctx, accepted.Record.JobID, ref, model.TerminalIntent{
		Outcome: model.OutcomeOrphaned,
		Cause:   model.CauseDaemonRestartedAfterAuthorization,
	})
	if err != nil {
		t.Fatal(err)
	}
	if finalized.Record.Terminal == nil || finalized.Record.Terminal.Outcome != model.OutcomeOrphaned {
		t.Fatalf("finalized = %+v, want orphaned terminal", finalized.Record.Terminal)
	}
	return string(accepted.Record.JobID)
}

func productionStrictDaemonPIDsE2B(agentbusPath string) ([]int, error) {
	cmd := exec.Command("ps", "-axo", "pid=,command=")
	raw, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, agentbusPath) || !strings.Contains(line, "serve --foreground") {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(line, "%d", &pid); err == nil && pid > 0 {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}
