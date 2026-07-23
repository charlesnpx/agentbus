//go:build abd_strict_e2e

package served

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		t.Skip("strict E2B e2e gate runs on linux")
	}
	requireProductionStrictCgroup(t)
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
		t.Fatalf("launch production strict daemon: %v", err)
	}
	if result.PID <= 0 || result.ExistingDaemon {
		t.Fatalf("launch result = %+v, want new daemon pid", result)
	}
	return result
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
