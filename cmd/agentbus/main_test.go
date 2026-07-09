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

	"github.com/charlesnpx/agentbus/engine"
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
	if version.Version != "test" || version.ProtocolVersion != protocolMajor || version.Schema != cliJSONSchema {
		t.Fatalf("version output = %+v", version)
	}

	code, _, stderr = runTestCLI(t, a, []string{"serve", "--foreground"}, "")
	if code != 1 {
		t.Fatalf("serve exit = %d, want 1", code)
	}
	if !strings.Contains(stderr, serveStubError) {
		t.Fatalf("serve stderr = %q", stderr)
	}
}

func TestSetupCachesProbeAndReportsJSON(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	probe := engine.BackendSetupProbe{
		Backend:          "codex",
		BinaryPath:       "/tmp/bin/codex",
		Version:          "0.143.0",
		StreamSchema:     "codex-json-v1",
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
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
	cachePath, err := engine.SetupProbeCachePath(a.stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := engine.ReadSetupProbeCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(cache.Backends) != 1 || cache.Backends[0].Version != probe.Version || cache.Backends[0].StreamSchema != probe.StreamSchema {
		t.Fatalf("cache = %+v", cache)
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

func TestSessionsFiltersRecordedState(t *testing.T) {
	t.Parallel()
	a, store := testAppAndStore(t)
	if err := store.Save(&engine.JobRecord{
		JobID:     "job_delegate_active",
		SessionID: "ses_delegate",
		Backend:   "codex",
		State:     engine.StateRunning,
		Tags:      map[string]string{"client": "delegate", "slot": "codex-a"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&engine.JobRecord{
		JobID:     "job_other_active",
		SessionID: "ses_other",
		Backend:   "claude",
		State:     engine.StateRunning,
		Tags:      map[string]string{"client": "other"},
	}); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runTestCLI(t, a, []string{"sessions", "--tags", "client=delegate", "--json"}, "")
	if code != 0 {
		t.Fatalf("sessions exit = %d stderr=%s", code, stderr)
	}
	var output sessionsOutput
	decodeJSON(t, stdout, &output)
	if len(output.Sessions) != 1 {
		t.Fatalf("sessions = %+v", output.Sessions)
	}
	session := output.Sessions[0]
	if session.SessionID != "ses_delegate" || session.Backend != "codex" || session.ActiveTurnID == nil || *session.ActiveTurnID != "job_delegate_active" {
		t.Fatalf("session = %+v", session)
	}
}

func TestStatusResultAndCancelExitCodes(t *testing.T) {
	t.Parallel()
	a, store := testAppAndStore(t)
	info, err := store.WriteResult("job_done", []byte("done"), engine.DefaultInlineResultCap)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&engine.JobRecord{
		JobID:     "job_done",
		SessionID: "ses_done",
		Backend:   "codex",
		State:     engine.StateCompleted,
		Result:    &info,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&engine.JobRecord{
		JobID:     "job_running",
		SessionID: "ses_running",
		Backend:   "claude",
		State:     engine.StateRunning,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(&engine.JobRecord{
		JobID: "job_cancel_me",
		State: engine.StateQueued,
	}); err != nil {
		t.Fatal(err)
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
				var output statusOutput
				decodeJSON(t, stdout, &output)
				if len(output.Jobs) != 1 || output.Jobs[0].State != tt.wantState {
					t.Fatalf("status output = %+v", output)
				}
				return
			}
			if tt.args[0] == "result" {
				var output jobResult
				decodeJSON(t, stdout, &output)
				if output.State != tt.wantState {
					t.Fatalf("result output = %+v", output)
				}
				if tt.wantState == engine.StateCompleted && (output.Result == nil || output.Result.Text != "done") {
					t.Fatalf("result text = %+v", output.Result)
				}
				return
			}
			var output cancelOutput
			decodeJSON(t, stdout, &output)
			if output.State != tt.wantState {
				t.Fatalf("cancel output = %+v", output)
			}
			loaded, err := store.Load("job_cancel_me")
			if err != nil {
				t.Fatal(err)
			}
			if loaded.State != engine.StateCanceled {
				t.Fatalf("persisted cancel state = %s", loaded.State)
			}
		})
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

func testAppAndStore(t *testing.T) (*app, *engine.Store) {
	t.Helper()
	a := testApp(t)
	store, err := engine.NewStore(engine.StoreConfig{
		Root:  a.stateRoot,
		CWD:   a.cwd,
		Clock: a.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return a, store
}

func runTestCLI(t *testing.T, a *app, args []string, stdin string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := a.run(context.Background(), args, strings.NewReader(stdin), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
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
