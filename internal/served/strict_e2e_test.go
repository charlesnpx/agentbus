//go:build abd_strict_e2e

package served

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const strictE2ERunEnv = "AGENTBUS_RUN_STRICT_E2E"

func TestProductionStrictServe(t *testing.T) {
	if strings.TrimSpace(os.Getenv(strictE2ERunEnv)) != "1" {
		t.Skipf("set %s=1 to run strict e2e", strictE2ERunEnv)
	}
	if testing.Short() {
		t.Skip("strict e2e is not run in short mode")
	}
	if runtime.GOOS != "linux" {
		// The authoritative strict native gate runs under Linux cgroup-v2 Docker.
		// Non-Linux hosts skip instead of asserting platform-specific runtime details.
		t.Skip("strict native e2e gate runs on linux")
	}
	requireProductionStrictCgroup(t)
	stateRoot := shortTempDir(t)
	cwd := shortTempDir(t)
	fixture := installServedNativeCodexFixture(t, stateRoot)
	agentbusPath := builtServedNativeAgentbusPath(t)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(agentbusPath, "serve", "--foreground")
	cmd.Dir = cwd
	cmd.Env = upsertEnv(fixture.env, "AGENTBUS_STATE_ROOT="+stateRoot)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- cmd.Wait()
		close(serveDone)
	}()
	stopped := false
	t.Cleanup(func() {
		if stopped {
			return
		}
		stopProductionStrictCommand(t, cmd, serveDone, &stdout, &stderr)
	})

	client := connectProductionStrictClient(t, stateRoot, serveDone)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("client close: %v", err)
		}
	})

	hello := client.HelloResult()
	// The default composition really does register the configured backend, so the
	// rejection below cannot be an incidental "unknown backend" error masquerading
	// as the admission gate.
	if !slices.Contains(hello.Backends, "codex") {
		t.Fatalf("default production Serve did not advertise the codex backend: %v", hello.Backends)
	}
	if !hello.Capabilities[protocol.CapabilityAdmissionStrictContainment] {
		t.Fatalf("strict containment capability absent in strict production hello: %+v", hello.Capabilities)
	}
	// jobs.requestId stays unadvertised in the default composition; strict
	// identified admission is no longer gated by this legacy capability flag.
	if hello.Capabilities["jobs.requestId"] {
		t.Fatal("jobs.requestId capability is unexpectedly advertised by the default composition")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	params := servedNativeSubmitParams("production-strict-e2e", cwd, servedNativeFixtureModeClean, nil)
	submitted, err := client.JobSubmit(ctx, agentclient.JobSubmitParams(params))
	if err != nil {
		t.Fatalf("strict identified submit: %v", err)
	}
	if submitted.JobID == "" || submitted.Deduplicated {
		t.Fatalf("submit result = %+v, want new job", submitted)
	}
	status := waitProductionStrictClientTerminal(t, client, submitted.JobID, 15*time.Second, productionStrictDiagnostics{
		stateRoot: stateRoot,
		fixture:   fixture,
		stdout:    &stdout,
		stderr:    &stderr,
	})
	if status.State != engine.StateCompleted {
		t.Fatalf("job status = %+v, want completed", status)
	}
	result, err := client.JobResult(ctx, agentclient.JobResultParams{JobID: submitted.JobID})
	if err != nil {
		t.Fatalf("job.result: %v", err)
	}
	if result.JobID != submitted.JobID || result.State != engine.StateCompleted || result.Result == nil || result.Result.Text != servedNativeResultText {
		t.Fatalf("job.result = %+v, want completed result text %q for %s", result, servedNativeResultText, submitted.JobID)
	}
	resultRaw, err := os.ReadFile(result.Result.ResultPath)
	if err != nil {
		t.Fatalf("read strict result artifact: %v", err)
	}
	if string(resultRaw) != servedNativeResultText {
		t.Fatalf("strict result artifact = %q, want %q", string(resultRaw), servedNativeResultText)
	}
	replay, err := client.JobSubmit(ctx, agentclient.JobSubmitParams(params))
	if err != nil {
		t.Fatalf("strict identified replay: %v", err)
	}
	if replay.JobID != submitted.JobID || !replay.Deduplicated || replay.State != engine.StateCompleted {
		t.Fatalf("replay submit = %+v, want same completed terminal job %s", replay, submitted.JobID)
	}
	assertServedNativeCodexExecutionCount(t, fixture, 1)

	stopped = true
	stopProductionStrictCommand(t, cmd, serveDone, &stdout, &stderr)
	execution := waitServedNativeCodexExecutions(t, fixture, 1, time.Second)[0]
	record := waitProductionStrictAdmissionTerminalFromRepository(t, stateRoot, submitted.JobID, 5*time.Second)
	launchProof := assertServedNativeIdentifiedTerminal(t, record, model.OutcomeCompleted)
	assertServedNativeExecutionMetadata(t, execution, *launchProof.Group, false)
	assertServedNativeIndependentGroupAbsent(t, *launchProof.Group, 5*time.Second)
	t.Log("strict_admission_real_job_end_to_end")
}

func connectProductionStrictClient(t *testing.T, stateRoot string, serveDone <-chan error) *agentclient.Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-serveDone:
			t.Fatalf("Serve exited before client connection: %v", err)
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
	t.Fatalf("client did not connect to production strict server: %v", lastErr)
	return nil
}

func requireProductionStrictCgroup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	manager, err := cgroup.New("")
	if err != nil {
		t.Skipf("cgroup.New(\"\") unavailable: %v", err)
	}
	support := manager.Probe(ctx)
	if closeErr := manager.Close(); closeErr != nil {
		t.Fatalf("close cgroup e2e probe manager: %v", closeErr)
	}
	if !support.Strict() {
		t.Skipf("strict cgroup-v2 support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}
}

type productionStrictDiagnostics struct {
	stateRoot string
	fixture   servedNativeCodexFixture
	stdout    *bytes.Buffer
	stderr    *bytes.Buffer
}

func waitProductionStrictClientTerminal(t *testing.T, client *agentclient.Client, jobID string, timeout time.Duration, diagnostics productionStrictDiagnostics) protocol.JobStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last protocol.JobStatus
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		statuses, err := client.JobStatus(ctx, agentclient.JobStatusParams{JobID: jobID})
		cancel()
		if err == nil && len(statuses.Jobs) == 1 {
			last = statuses.Jobs[0]
			if engine.IsTerminal(last.State) {
				return last
			}
		} else if err != nil {
			lastErr = err
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("job %s did not reach terminal after %s; stage=%s last=%+v err=%v", jobID, timeout, diagnoseProductionStrictStage(t, diagnostics, jobID), last, lastErr)
	return protocol.JobStatus{}
}

func diagnoseProductionStrictStage(t *testing.T, diagnostics productionStrictDiagnostics, jobID string) string {
	t.Helper()
	var parts []string
	fixtureExists := true
	if diagnostics.fixture.codexPath == "" {
		fixtureExists = false
		parts = append(parts, "fixture=missing-path")
	} else if info, err := os.Stat(diagnostics.fixture.codexPath); err != nil {
		fixtureExists = false
		parts = append(parts, "fixture=missing:"+err.Error())
	} else {
		parts = append(parts, "fixture="+diagnostics.fixture.codexPath)
		parts = append(parts, "fixtureMode="+info.Mode().String())
	}

	executions := readServedNativeCodexExecutions(t, diagnostics.fixture)
	parts = append(parts, fmt.Sprintf("fixtureExecutions=%d", len(executions)))

	record, recordOK, recordErr := productionStrictSafetyRecord(diagnostics.stateRoot, jobID)
	if recordErr != nil {
		parts = append(parts, "authorityRecordErr="+recordErr.Error())
	}
	if recordOK {
		launch, launchOK := record.Attempt.Launches.Get(model.LaunchOrdinalOne)
		switch {
		case record.Terminal != nil:
			parts = append(parts, "stage=terminal-record-present")
		case launchOK && (launch.Group != nil || launch.Grant != nil || launch.Released != nil || launch.Quiescence != nil):
			parts = append(parts, "stage=launched-but-not-terminal")
		case len(executions) > 0:
			parts = append(parts, "stage=launched-but-not-terminal")
		case !fixtureExists:
			parts = append(parts, "stage=fixture-missing")
		default:
			parts = append(parts, "stage=accepted-but-not-launched")
		}
		parts = append(parts, fmt.Sprintf("terminal=%+v", record.Terminal))
		parts = append(parts, fmt.Sprintf("launchOne=%+v", launch))
	} else if !fixtureExists {
		parts = append(parts, "stage=fixture-missing")
	} else {
		parts = append(parts, "stage=accepted-but-authority-record-unreadable")
	}
	parts = append(parts, "serveStdoutTail="+quoteTail(diagnostics.stdout))
	parts = append(parts, "serveStderrTail="+quoteTail(diagnostics.stderr))
	return strings.Join(parts, " ")
}

func productionStrictSafetyRecord(stateRoot, jobID string) (model.SafetyRecord, bool, error) {
	if stateRoot == "" || jobID == "" {
		return model.SafetyRecord{}, false, nil
	}
	repo, err := bboltrepo.OpenReadOnly(filepath.Join(stateRoot, admissionRepositoryFile))
	if err != nil {
		return model.SafetyRecord{}, false, err
	}
	defer repo.Close()
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		return model.SafetyRecord{}, false, err
	}
	var record model.SafetyRecord
	var ok bool
	err = repo.View(context.Background(), func(tx repository.ReadTx) error {
		image := tx.LoadJob(modelJobID)
		if image.Safety.State != repository.RecordValid {
			return nil
		}
		record = image.Safety.Value
		ok = true
		return nil
	})
	return record, ok, err
}

func quoteTail(buf *bytes.Buffer) string {
	if buf == nil {
		return `""`
	}
	const limit = 4096
	text := buf.String()
	if len(text) > limit {
		text = text[len(text)-limit:]
	}
	return strconv.Quote(text)
}

func waitProductionStrictAdmissionTerminalFromRepository(t *testing.T, stateRoot, jobID string, timeout time.Duration) model.SafetyRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last model.SafetyRecord
	for time.Now().Before(deadline) {
		repo, err := bboltrepo.OpenReadOnly(filepath.Join(stateRoot, admissionRepositoryFile))
		if err == nil {
			record := loadAuthoritySafetyRecordFromRepository(t, repo, jobID)
			if closeErr := repo.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			last = record
			if record.Terminal != nil {
				return record
			}
		}
		time.Sleep(servedNativeConformancePollInterval)
	}
	t.Fatalf("admission safety record %s did not reach terminal after %s; last = %+v", jobID, timeout, last)
	return model.SafetyRecord{}
}

func stopProductionStrictCommand(t *testing.T, cmd *exec.Cmd, done <-chan error, stdout, stderr *bytes.Buffer) {
	t.Helper()
	if cmd.Process != nil && cmd.ProcessState == nil {
		_ = cmd.Process.Kill()
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("agentbus serve did not stop after kill; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}
