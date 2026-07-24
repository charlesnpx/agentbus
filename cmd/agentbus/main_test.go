package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type fakeProbe struct {
	probe engine.BackendSetupProbe
	err   error
}

func (p fakeProbe) SetupProbe(context.Context) (engine.BackendSetupProbe, error) {
	return p.probe, p.err
}

type fakeBackend struct {
	name   string
	health engine.Health
	err    error
}

func (b fakeBackend) Name() string { return b.name }

func (b fakeBackend) Preflight(context.Context) (engine.Health, error) {
	if b.err != nil {
		return engine.Health{}, b.err
	}
	return b.health, nil
}

func (b fakeBackend) Start(context.Context, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used")
}

func (b fakeBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("not used")
}

func TestVersionAndServeCommands(t *testing.T) {
	t.Parallel()
	a := testApp(t)

	code, stdout, stderr := runTestCLI(t, a, []string{"version", "--json"}, "")
	if code != 0 {
		t.Fatalf("version exit = %d stderr=%s", code, stderr)
	}
	var version versionOutput
	decodeJSON(t, stdout, &version)
	if version.Version != "test" || version.ProtocolVersion != protocol.Version || version.Schema != cliJSONSchema {
		t.Fatalf("version output = %+v", version)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"serve", "--help"}, "")
	if code != 0 {
		t.Fatalf("serve help exit = %d stderr=%s", code, stderr)
	}
	help := stdout + stderr
	removedServeFlag := "--" + "admission"
	if strings.Contains(help, removedServeFlag) {
		t.Fatalf("serve help still mentions admission flag: stdout=%s stderr=%s", stdout, stderr)
	}

	code, _, stderr = runTestCLI(t, a, []string{"serve", "--foreground", removedServeFlag + "=strict"}, "")
	if code == 0 || !strings.Contains(stderr, "flag provided but not defined") {
		t.Fatalf("removed admission flag exit=%d stderr=%s", code, stderr)
	}
}

func TestStartBackgroundDaemonWritesPIDAfterLauncherReady(t *testing.T) {
	a := testApp(t)
	launched := make(chan struct{})
	releaseReady := make(chan struct{})
	a.daemonLauncher = func(ctx context.Context, opts daemonlaunch.Options) (daemonlaunch.Result, error) {
		if opts.StateRoot != a.stateRoot {
			t.Errorf("launcher state root = %q, want %q", opts.StateRoot, a.stateRoot)
		}
		close(launched)
		select {
		case <-releaseReady:
		case <-ctx.Done():
			return daemonlaunch.Result{}, ctx.Err()
		}
		return daemonlaunch.Result{PID: 4242, CanonicalStateRoot: a.stateRoot}, nil
	}

	done := make(chan error, 1)
	go func() { done <- a.startBackgroundDaemon(context.Background()) }()
	select {
	case <-launched:
	case <-time.After(time.Second):
		t.Fatal("launcher was not invoked")
	}
	pidPath := filepath.Join(a.stateRoot, "agentbus.pid")
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pid file before readiness stat error = %v, want not exist", err)
	}
	close(releaseReady)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("startBackgroundDaemon did not return after readiness")
	}
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(raw)) != "4242" {
		t.Fatalf("pid file = %q, want 4242", raw)
	}
}

func TestSetupCachesProbeAndReportsJSON(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	probe := engine.BackendSetupProbe{
		Backend:           "codex",
		BinaryPath:        "/tmp/bin/codex",
		Version:           "0.143.0",
		StreamSchema:      "codex-json-v1",
		ConfigMode:        engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:      []string{"workspace-write", "read-only"},
		JSONEventsProbed:  true,
		DiscoveredModels:  []string{"gpt-5.4"},
		DiscoveredEfforts: []string{"high"},
	}
	a.backends = []backendSpec{{
		name: "codex",
		backend: fakeBackend{
			name: "codex",
			health: engine.Health{
				Backend:      "codex",
				BinaryPath:   probe.BinaryPath,
				Version:      probe.Version,
				StreamSchema: probe.StreamSchema,
			},
		},
		probe: fakeProbe{probe: probe},
	}}

	code, stdout, stderr := runTestCLI(t, a, []string{"setup", "--json"}, "")
	if code != 0 {
		t.Fatalf("setup exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var output setupOutput
	decodeJSON(t, stdout, &output)
	if len(output.Backends) != 1 {
		t.Fatalf("backends = %d, want 1", len(output.Backends))
	}
	got := output.Backends[0]
	if got.Backend != "codex" || got.BinaryPath != probe.BinaryPath || got.Version != probe.Version || !got.JSONEventsProbe.Ran || got.JSONEventsProbe.StreamSchema != probe.StreamSchema {
		t.Fatalf("setup backend = %+v", got)
	}
	if strings.Join(got.DiscoveredModels, ",") != "gpt-5.4" || strings.Join(got.DiscoveredEfforts, ",") != "high" {
		t.Fatalf("discovery report=%+v", got)
	}
	cachePath, err := engine.SetupProbeCachePath(a.stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := engine.ReadSetupProbeCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if cache.Version != engine.SetupProbeCacheVersion || len(cache.Backends) != 1 || cache.Backends[0].Version != probe.Version || cache.Backends[0].StreamSchema != probe.StreamSchema || len(cache.Backends[0].DiscoveredModels) != 1 {
		t.Fatalf("cache = %+v", cache)
	}
}

func TestSetupReportsDiscoveryWarnings(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	probe := engine.BackendSetupProbe{Backend: "claude", BinaryPath: "/tmp/bin/claude", Version: "2.1.205", StreamSchema: "claude-stream-json-v1", JSONEventsProbed: true, DiscoveryWarnings: []string{"claude model discovery failed: claude --help model discovery parser found no model or effort listings"}}
	a.backends = []backendSpec{{name: "claude", backend: fakeBackend{name: "claude", health: engine.Health{Backend: "claude", BinaryPath: probe.BinaryPath, Version: probe.Version, StreamSchema: probe.StreamSchema}}, probe: fakeProbe{probe: probe}}}
	code, stdout, stderr := runTestCLI(t, a, []string{"setup", "--json"}, "")
	if code != 0 {
		t.Fatalf("setup exit=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var output setupOutput
	decodeJSON(t, stdout, &output)
	if len(output.Backends) != 1 || len(output.Backends[0].Warnings) != 1 || !strings.Contains(output.Backends[0].Warnings[0], "claude --help") {
		t.Fatalf("output=%+v", output)
	}
}

func TestSetupDriftDetectionFailsLoudly(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	drift := "backend version changed since setup; re-run agentbus setup"
	probe := engine.BackendSetupProbe{
		Backend:          "claude",
		BinaryPath:       "/tmp/bin/claude",
		Version:          "2.1.205",
		StreamSchema:     "claude-stream-json-v1",
		JSONEventsProbed: true,
	}
	a.backends = []backendSpec{{
		name:    "claude",
		backend: fakeBackend{name: "claude", err: errors.New(drift)},
		probe:   fakeProbe{probe: probe},
	}}

	code, stdout, stderr := runTestCLI(t, a, []string{"setup", "--json"}, "")
	if code != 1 {
		t.Fatalf("setup exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var output setupOutput
	decodeJSON(t, stdout, &output)
	if output.Error != "setup preflight failed" {
		t.Fatalf("setup error = %q", output.Error)
	}
	if len(output.Backends) != 1 || !strings.Contains(output.Backends[0].Error, drift) {
		t.Fatalf("backend error = %+v", output.Backends)
	}
	if !strings.Contains(stdout, drift) {
		t.Fatalf("drift was not loud in JSON output: %s", stdout)
	}
}

func TestAdmissionCLIInspectResetAndSealFlags(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	root := filepath.Join(t.TempDir(), "admission-root")

	code, stdout, stderr := runTestCLI(t, a, []string{"admission", "reset-empty-root", "--state-root", root, "--json"}, "")
	if code != 0 {
		t.Fatalf("reset-empty-root exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var reset authority.RootInspection
	decodeJSON(t, stdout, &reset)
	if reset.DomainUUID == "" || reset.Sealed || !reset.Counts.Empty() || reset.ActivationMetadata.Activated {
		t.Fatalf("reset inspection = %+v", reset)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"admission", "inspect", "--state-root", root, "--json"}, "")
	if code != 0 {
		t.Fatalf("inspect exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var inspected authority.RootInspection
	decodeJSON(t, stdout, &inspected)
	if inspected.DomainUUID != reset.DomainUUID || !inspected.Counts.Empty() {
		t.Fatalf("inspect = %+v, reset = %+v", inspected, reset)
	}

	code, _, stderr = runTestCLI(t, a, []string{"admission", "seal", "--state-root", root}, "")
	if code != 1 || !strings.Contains(stderr, authority.ErrSealConfirmationRequired.Error()) {
		t.Fatalf("seal without flags exit=%d stderr=%s", code, stderr)
	}
	newRoot := filepath.Join(t.TempDir(), "new-admission-root")
	code, stdout, stderr = runTestCLI(t, a, []string{"admission", "seal", "--state-root", root, "--new-state-root", newRoot, "--start-new-authority-domain", "--acknowledge-replay-history-reset", "--json"}, "")
	if code != 0 {
		t.Fatalf("seal exit=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var sealed authority.SealReport
	decodeJSON(t, stdout, &sealed)
	if !sealed.OldRootSealed || sealed.NewRoot != newRoot || sealed.NewDomainUUID == "" {
		t.Fatalf("seal report = %+v", sealed)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"admission", "--help"}, "")
	if code != 0 {
		t.Fatalf("admission help exit=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "Multi-root read/cancel/result routing is out of scope in this first release.") {
		t.Fatalf("admission help missing limitation sentence: %s", stdout)
	}
}

func TestStatusResultAndCancelExitCodes(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	client := &fakeProtocolClient{
		statuses: map[string]agentclient.JobStatus{
			"job_done":    {JobID: "job_done", SessionID: "ses_done", State: engine.StateCompleted},
			"job_running": {JobID: "job_running", SessionID: "ses_running", State: engine.StateRunning},
		},
		results: map[string]agentclient.JobResult{
			"job_done":    {JobID: "job_done", SessionID: "ses_done", State: engine.StateCompleted, Result: &engine.ResultInfo{Text: "done", Bytes: 4}},
			"job_running": {JobID: "job_running", SessionID: "ses_running", State: engine.StateRunning},
		},
		cancels: map[string]agentclient.JobCancelResult{
			"job_cancel_me": {JobID: "job_cancel_me", State: engine.StateCanceled},
		},
	}
	a.clientConnect = func(ctx context.Context, opts agentclient.Options) (protocolClient, error) {
		if opts.StateRoot != a.stateRoot {
			t.Errorf("client state root = %q, want %q", opts.StateRoot, a.stateRoot)
		}
		if opts.CommandPath == "" {
			t.Error("client command path is empty")
		}
		return client, nil
	}

	tests := []struct {
		name      string
		args      []string
		wantCode  int
		wantState engine.JobState
	}{
		{name: "status completed", args: []string{"status", "--job", "job_done", "--json"}, wantCode: 0, wantState: engine.StateCompleted},
		{name: "status nonterminal", args: []string{"status", "--job", "job_running", "--json"}, wantCode: 2, wantState: engine.StateRunning},
		{name: "result completed", args: []string{"result", "--job", "job_done", "--json"}, wantCode: 0, wantState: engine.StateCompleted},
		{name: "result nonterminal", args: []string{"result", "--job", "job_running", "--json"}, wantCode: 2, wantState: engine.StateRunning},
		{name: "cancel queued", args: []string{"cancel", "--job", "job_cancel_me", "--json"}, wantCode: 7, wantState: engine.StateCanceled},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runTestCLI(t, a, tt.args, "")
			if code != tt.wantCode {
				t.Fatalf("exit = %d want %d stderr=%s stdout=%s", code, tt.wantCode, stderr, stdout)
			}
			if tt.args[0] == "status" {
				var output protocol.JobStatusResult
				decodeJSON(t, stdout, &output)
				if len(output.Jobs) != 1 || output.Jobs[0].State != tt.wantState {
					t.Fatalf("status output = %+v", output)
				}
				return
			}
			if tt.args[0] == "result" {
				var output protocol.JobResult
				decodeJSON(t, stdout, &output)
				if output.State != tt.wantState {
					t.Fatalf("result output = %+v", output)
				}
				if tt.wantState == engine.StateCompleted && (output.Result == nil || output.Result.Text != "done") {
					t.Fatalf("result text = %+v", output.Result)
				}
				return
			}
			var output protocol.JobCancelResult
			decodeJSON(t, stdout, &output)
			if output.State != tt.wantState {
				t.Fatalf("cancel output = %+v", output)
			}
		})
	}
}

func TestStatusListsViaProtocolClient(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
		return &fakeProtocolClient{
			list: []agentclient.JobStatus{
				{JobID: "job_a", State: engine.StateRunning},
				{JobID: "job_b", State: engine.StateCompleted},
			},
		}, nil
	}
	code, stdout, stderr := runTestCLI(t, a, []string{"status", "--json"}, "")
	if code != 0 {
		t.Fatalf("status list exit=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var output protocol.JobStatusResult
	decodeJSON(t, stdout, &output)
	if len(output.Jobs) != 2 || output.Jobs[0].JobID != "job_a" || output.Jobs[1].JobID != "job_b" {
		t.Fatalf("status list output = %+v", output)
	}
}

func TestStatusResultCancelProtocolErrorExitCodes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		args       []string
		err        error
		wantCode   int
		wantStderr []string
	}{
		{
			name:       "unknown job",
			args:       []string{"status", "--job", "job_missing"},
			err:        &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: "job_missing"})},
			wantCode:   cliExitUnknownJob,
			wantStderr: []string{"code=invalid_task_spec", "jobId=job_missing"},
		},
		{
			name:       "fail stop",
			args:       []string{"status", "--job", "job_failstop"},
			err:        &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorBackendUnavailable, "authority fail-stopped", protocol.ErrorData{AdmissionCause: protocol.AdmissionRejectRootFailStopped})},
			wantCode:   cliExitAuthorityFailStop,
			wantStderr: []string{"code=backend_unavailable", "admissionCause=root_fail_stopped"},
		},
		{
			name:       "daemon startup failure",
			args:       []string{"result", "--job", "job_any"},
			err:        &daemonlaunch.StartupError{Kind: daemonlaunch.ErrStartupFailed, Code: "strict admission support unavailable", Message: "unsupported host"},
			wantCode:   cliExitDaemonStartupFailure,
			wantStderr: []string{"daemon startup failed", "unsupported host"},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
				return &fakeProtocolClient{err: tt.err}, nil
			}
			code, stdout, stderr := runTestCLI(t, a, tt.args, "")
			if code != tt.wantCode {
				t.Fatalf("exit=%d want=%d stdout=%s stderr=%s", code, tt.wantCode, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout=%q, want empty", stdout)
			}
			for _, want := range tt.wantStderr {
				if !strings.Contains(stderr, want) {
					t.Fatalf("stderr=%q, want %q", stderr, want)
				}
			}
		})
	}
}

func TestStatusResultCancelDaemonStartupFailureLeavesRootEmpty(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"status", "--job", "job_any"},
		{"result", "--job", "job_any"},
		{"cancel", "--job", "job_any"},
	} {
		args := args
		t.Run(args[0], func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
				return nil, &daemonlaunch.StartupError{Kind: daemonlaunch.ErrStartupFailed, Code: "strict admission support unavailable", Message: "unsupported host"}
			}
			code, stdout, stderr := runTestCLI(t, a, args, "")
			if code != cliExitDaemonStartupFailure {
				t.Fatalf("exit=%d want=%d stdout=%s stderr=%s", code, cliExitDaemonStartupFailure, stdout, stderr)
			}
			if _, err := os.Stat(a.stateRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("state root stat = %v, want not exist", err)
			}
		})
	}
}

func TestSessionsCommandIsUnknown(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	code, _, stderr := runTestCLI(t, a, []string{"sessions"}, "")
	if code != 2 || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("sessions exit=%d stderr=%s", code, stderr)
	}
}

func TestValidateContractFilesAndRegisteredNames(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	spec := engine.ContractSpec{Shape: &engine.ShapeSpec{
		FirstLineEnum:    []string{"PASS"},
		RequiredSections: []string{"Findings"},
	}}
	contractPath := writeJSONFile(t, t.TempDir(), "contract.json", spec)
	textPath := filepath.Join(t.TempDir(), "result.txt")
	if err := os.WriteFile(textPath, []byte("PASS\n\n## Findings\nNone.\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runTestCLI(t, a, []string{"validate", "--contract", contractPath, "--text-file", textPath, "--json"}, "")
	if code != 0 {
		t.Fatalf("validate file exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var fileResult engine.ValidationResult
	decodeJSON(t, stdout, &fileResult)
	if !fileResult.Valid || fileResult.ContractSHA256 == "" {
		t.Fatalf("file validation = %+v", fileResult)
	}

	if _, err := a.registry.Register("delegate/test@1", spec); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = runTestCLI(t, a, []string{"validate", "--contract", "delegate/test@1", "--text-file", textPath, "--json"}, "")
	if code != 0 {
		t.Fatalf("validate name exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var namedResult engine.ValidationResult
	decodeJSON(t, stdout, &namedResult)
	if !namedResult.Valid || namedResult.ContractName != "delegate/test@1" {
		t.Fatalf("named validation = %+v", namedResult)
	}

	badPath := filepath.Join(t.TempDir(), "bad.txt")
	if err := os.WriteFile(badPath, []byte("FAIL\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = runTestCLI(t, a, []string{"validate", "--contract", contractPath, "--text-file", badPath, "--json"}, "")
	if code != 3 {
		t.Fatalf("invalid validate exit = %d stderr=%s stdout=%s", code, stderr, stdout)
	}
	var badResult engine.ValidationResult
	decodeJSON(t, stdout, &badResult)
	if badResult.Valid || len(badResult.Missing) == 0 {
		t.Fatalf("bad validation = %+v", badResult)
	}
}

func testApp(t *testing.T) *app {
	t.Helper()
	root := filepath.Join(t.TempDir(), "state")
	cwd := t.TempDir()
	return &app{
		version:   "test",
		stateRoot: root,
		cwd:       cwd,
		registry:  engine.NewPolicyRegistry(),
		clock:     testClock{now: time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)},
	}
}

func runTestCLI(t *testing.T, a *app, args []string, stdin string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := a.run(context.Background(), args, strings.NewReader(stdin), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

type fakeProtocolClient struct {
	list     []agentclient.JobStatus
	statuses map[string]agentclient.JobStatus
	results  map[string]agentclient.JobResult
	cancels  map[string]agentclient.JobCancelResult
	err      error
}

func (c *fakeProtocolClient) JobStatus(_ context.Context, params agentclient.JobStatusParams) (agentclient.JobStatusResult, error) {
	if c.err != nil {
		return agentclient.JobStatusResult{}, c.err
	}
	if params.JobID != "" {
		status, ok := c.statuses[params.JobID]
		if !ok {
			return agentclient.JobStatusResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: params.JobID})}
		}
		return agentclient.JobStatusResult{Jobs: []agentclient.JobStatus{status}}, nil
	}
	if len(c.list) > 0 {
		return agentclient.JobStatusResult{Jobs: append([]agentclient.JobStatus(nil), c.list...)}, nil
	}
	return agentclient.JobStatusResult{Jobs: mapValues(c.statuses)}, nil
}

func (c *fakeProtocolClient) JobResult(_ context.Context, params agentclient.JobResultParams) (agentclient.JobResult, error) {
	if c.err != nil {
		return agentclient.JobResult{}, c.err
	}
	result, ok := c.results[params.JobID]
	if !ok {
		return agentclient.JobResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: params.JobID})}
	}
	return result, nil
}

func (c *fakeProtocolClient) JobCancel(_ context.Context, params agentclient.JobCancelParams) (agentclient.JobCancelResult, error) {
	if c.err != nil {
		return agentclient.JobCancelResult{}, c.err
	}
	result, ok := c.cancels[params.JobID]
	if !ok {
		return agentclient.JobCancelResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorInvalidTaskSpec, "job is not known", protocol.ErrorData{JobID: params.JobID})}
	}
	return result, nil
}

func (c *fakeProtocolClient) Close() error {
	return nil
}

func mapValues(in map[string]agentclient.JobStatus) []agentclient.JobStatus {
	out := make([]agentclient.JobStatus, 0, len(in))
	for _, value := range in {
		out = append(out, value)
	}
	return out
}

func decodeJSON(t *testing.T, raw string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		t.Fatalf("decode JSON %q: %v", raw, err)
	}
}

func writeJSONFile(t *testing.T, dir, name string, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
