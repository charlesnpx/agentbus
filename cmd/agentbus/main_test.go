package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestVersionReportsBothVersions(t *testing.T) {
	a := testApp(t)
	code, stdout, stderr := runTestCLI(t, a, []string{"version"})
	if code != 0 || stderr != "" {
		t.Fatalf("version exit=%d stderr=%q", code, stderr)
	}
	if stdout != "agentbus test protocol 3\n" {
		t.Fatalf("human version output = %q", stdout)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"version", "--json"})
	if code != 0 || stderr != "" {
		t.Fatalf("JSON version exit=%d stderr=%q", code, stderr)
	}
	var got versionOutput
	decodeJSON(t, stdout, &got)
	if got.Version != "test" || got.Schema != cliJSONSchema || got.ProtocolVersion != protocol.Version {
		t.Fatalf("version output = %+v", got)
	}
}

func TestDeletedCommandIsUnknown(t *testing.T) {
	a := testApp(t)
	code, _, stderr := runTestCLI(t, a, []string{"setup"})
	if code != 2 || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("exit=%d stderr=%q, want unknown-command usage error", code, stderr)
	}
}

func TestStatusAndResultJSONAreByteIdentical(t *testing.T) {
	digest := strings.Repeat("a", sha256.Size*2)
	record := detailedRecord(protocol.PublicStateCompleted)
	record.Result = &protocol.ResultInfoWire{
		Text:       "the result",
		ResultPath: "/state/results/job-1",
		SHA256:     digest,
		Bytes:      10,
	}
	a := testApp(t)
	a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
		return &fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}}, nil
	}

	statusCode, statusJSON, statusErr := runTestCLI(t, a, []string{"status", "--job", "job-1", "--json"})
	resultCode, resultJSON, resultErr := runTestCLI(t, a, []string{"result", "--job", "job-1", "--json"})
	if statusCode != 0 || resultCode != 0 || statusErr != "" || resultErr != "" {
		t.Fatalf("status=(%d,%q) result=(%d,%q)", statusCode, statusErr, resultCode, resultErr)
	}
	if statusJSON != resultJSON {
		t.Fatalf("status JSON = %q\nresult JSON = %q\nwant byte-identical job.get records", statusJSON, resultJSON)
	}
	if !strings.Contains(statusJSON, `"result":{"text":"the result","resultPath":"/state/results/job-1","sha256":"`+digest+`","bytes":10}`) {
		t.Fatalf("result JSON shape = %q, want bare lowercase SHA-256 value", statusJSON)
	}
}

func TestStatusHumanProjectionIncludesOperatorFieldsAndNeverResultText(t *testing.T) {
	record := detailedRecord(protocol.PublicStateFailed)
	started := record.CreatedAt.Add(time.Minute)
	finished := started.Add(time.Minute)
	record.StartedAt = &started
	record.FinishedAt = &finished
	record.Tags = map[string]string{"z": "last", "a": "first"}
	record.ModelReported = "gpt-5"
	record.Result = &protocol.ResultInfoWire{
		Text:       "secret result text",
		ResultPath: "/state/results/job-1",
		SHA256:     strings.Repeat("a", sha256.Size*2),
		Bytes:      18,
	}
	record.Failure = &protocol.JobFailureWire{Class: protocol.FailureClassBackendError, Reason: "provider stopped"}
	record.Contract = &protocol.ContractResult{Evaluated: true, Compliant: false}
	record.LogPaths = &protocol.LogPathsWire{Stdout: "/logs/out", Stderr: "/logs/err"}
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}})

	code, stdout, stderr := runTestCLI(t, a, []string{"status", "--job", "job-1"})
	if code != 4 || stderr != "" {
		t.Fatalf("status exit=%d stderr=%q", code, stderr)
	}
	for _, want := range []string{
		"jobId=job-1 state=failed backend=codex cleanup=clean age=",
		"createdAt=2026-01-02T03:04:05Z",
		"startedAt=2026-01-02T03:05:05Z",
		"finishedAt=2026-01-02T03:06:05Z",
		"timeout.effective=1800000 timeout.source=client",
		"model=gpt-5 tags=a=first,z=last",
		"result.bytes=18 result.sha256=sha256:aaaaaaaaaaaa",
		"failure.class=backend_error failure.reason=provider stopped",
		"contract.evaluated=true contract.compliant=false",
		"logPaths.stdout=/logs/out logPaths.stderr=/logs/err",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("status output missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "secret result text") || strings.Contains(stdout, "/state/results/job-1") {
		t.Fatalf("status exposed result text or path: %q", stdout)
	}
}

func TestStatusListHumanProjectionIncludesFailureClassAndContractVerdict(t *testing.T) {
	created := time.Now().UTC().Add(-2 * time.Minute)
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{list: agentclient.JobGetListResult{Jobs: []agentclient.JobSummaryWire{{
		JobID:        "job-1",
		Backend:      "codex",
		State:        protocol.PublicStateRunning,
		Cleanup:      protocol.CleanupUncertain,
		CreatedAt:    created,
		FailureClass: protocol.FailureClassBackendError,
		Contract:     &protocol.ContractVerdict{Evaluated: true, Compliant: false},
	}}}})
	code, stdout, stderr := runTestCLI(t, a, []string{"status"})
	if code != 0 || stderr != "" {
		t.Fatalf("status list exit=%d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "jobId=job-1 state=running backend=codex cleanup=uncertain age=") ||
		!strings.Contains(stdout, "failure.class=backend_error") ||
		!strings.Contains(stdout, "contract.evaluated=true contract.compliant=false") {
		t.Fatalf("status list = %q", stdout)
	}
}

func TestResultHumanProjection(t *testing.T) {
	tests := []struct {
		name       string
		record     agentclient.JobGetResult
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{
			name:     "text is pipeable and newline terminated",
			record:   withResult(detailedRecord(protocol.PublicStateCompleted), &protocol.ResultInfoWire{Text: "pipe me", SHA256: strings.Repeat("a", sha256.Size*2), Bytes: 7}),
			wantCode: 0, wantStdout: "pipe me\n",
		},
		{
			name:       "completed without result is unavailable",
			record:     detailedRecord(protocol.PublicStateCompleted),
			wantCode:   cliExitResultUnavailable,
			wantStderr: "no authoritative result",
		},
		{
			name:     "failed writes diagnostics only",
			record:   withFailure(detailedRecord(protocol.PublicStateFailed), protocol.FailureClassTimeout, "deadline exceeded"),
			wantCode: 5, wantStderr: "failure.class=timeout failure.reason=deadline exceeded",
		},
		{
			name:       "nonterminal writes state only to stderr",
			record:     detailedRecord(protocol.PublicStateRunning),
			wantCode:   2,
			wantStderr: "jobId=job-1 state=running",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": tt.record}})
			code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1"})
			if code != tt.wantCode || stdout != tt.wantStdout || !strings.Contains(stderr, tt.wantStderr) {
				t.Fatalf("result=(code=%d stdout=%q stderr=%q), want code=%d stdout=%q stderr containing %q", code, stdout, stderr, tt.wantCode, tt.wantStdout, tt.wantStderr)
			}
		})
	}
}

func TestResultStreamsVerifiedArtifact(t *testing.T) {
	artifactBytes := []byte(strings.Repeat("a", engine.DefaultInlineResultCap+1))
	artifactPath := filepath.Join(t.TempDir(), "result.txt")
	if err := os.WriteFile(artifactPath, artifactBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(artifactBytes)
	record := withResult(detailedRecord(protocol.PublicStateCompleted), &protocol.ResultInfoWire{
		ResultPath: artifactPath,
		SHA256:     hex.EncodeToString(sum[:]),
		Bytes:      int64(len(artifactBytes)),
	})
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}})

	code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1"})
	if code != 0 || stdout != string(artifactBytes) || stderr != "" {
		t.Fatalf("result=(code=%d stdout=%d bytes stderr=%q), want verified artifact bytes and success", code, len(stdout), stderr)
	}
}

func TestResultRejectsPrefixedDigest(t *testing.T) {
	artifactBytes := []byte("authoritative result")
	artifactPath := filepath.Join(t.TempDir(), "result.txt")
	if err := os.WriteFile(artifactPath, artifactBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(artifactBytes)
	record := withResult(detailedRecord(protocol.PublicStateCompleted), &protocol.ResultInfoWire{
		ResultPath: artifactPath,
		SHA256:     "sha256:" + hex.EncodeToString(sum[:]),
		Bytes:      int64(len(artifactBytes)),
	})
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}})

	code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1"})
	if code != cliExitResultUnavailable || stdout != "" || !strings.Contains(stderr, "SHA-256 check failed") {
		t.Fatalf("result=(code=%d stdout=%q stderr=%q), want prefixed digest rejection", code, stdout, stderr)
	}
}

func TestResultMissingArtifactReturnsExit15WithoutStdout(t *testing.T) {
	artifactPath := filepath.Join(t.TempDir(), "missing-result.txt")
	record := withResult(detailedRecord(protocol.PublicStateCompleted), &protocol.ResultInfoWire{
		ResultPath: artifactPath,
		SHA256:     strings.Repeat("0", sha256.Size*2),
		Bytes:      1,
	})
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}})

	code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1"})
	if code != 15 || stdout != "" || !strings.Contains(stderr, artifactPath) || !strings.Contains(stderr, "missing") {
		t.Fatalf("result=(code=%d stdout=%q stderr=%q), want exit 15, empty stdout, and missing artifact diagnostic", code, stdout, stderr)
	}
}

func TestResultJSONRetainsRecordButUsesExit15ForUnusableArtifact(t *testing.T) {
	artifactPath := filepath.Join(t.TempDir(), "result.txt")
	artifactBytes := []byte("authoritative result")
	if err := os.WriteFile(artifactPath, artifactBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(artifactBytes)

	tests := []struct {
		name   string
		result protocol.ResultInfoWire
	}{
		{
			name: "missing",
			result: protocol.ResultInfoWire{
				ResultPath: filepath.Join(t.TempDir(), "missing-result.txt"),
				SHA256:     hex.EncodeToString(sum[:]),
				Bytes:      int64(len(artifactBytes)),
			},
		},
		{
			name: "unreadable directory",
			result: protocol.ResultInfoWire{
				ResultPath: t.TempDir(),
				SHA256:     hex.EncodeToString(sum[:]),
				Bytes:      int64(len(artifactBytes)),
			},
		},
		{
			name: "byte mismatch",
			result: protocol.ResultInfoWire{
				ResultPath: artifactPath,
				SHA256:     hex.EncodeToString(sum[:]),
				Bytes:      int64(len(artifactBytes) - 1),
			},
		},
		{
			name: "digest mismatch",
			result: protocol.ResultInfoWire{
				ResultPath: artifactPath,
				SHA256:     strings.Repeat("0", sha256.Size*2),
				Bytes:      int64(len(artifactBytes)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := withResult(detailedRecord(protocol.PublicStateCompleted), &tt.result)
			a := testApp(t)
			a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": record}})

			statusCode, statusJSON, statusErr := runTestCLI(t, a, []string{"status", "--job", "job-1", "--json"})
			resultCode, resultJSON, resultErr := runTestCLI(t, a, []string{"result", "--job", "job-1", "--json"})
			if statusCode != 0 || statusErr != "" || resultCode != cliExitResultUnavailable || resultErr != "" {
				t.Fatalf("status=(%d,%q) result=(%d,%q), want status success and result exit %d", statusCode, statusErr, resultCode, resultErr, cliExitResultUnavailable)
			}
			if resultJSON != statusJSON {
				t.Fatalf("status JSON = %q\nresult JSON = %q\nwant byte-identical records", statusJSON, resultJSON)
			}
		})
	}
}

func TestExitCodesUseStateFailureAndContract(t *testing.T) {
	completedNoncompliant := detailedRecord(protocol.PublicStateCompleted)
	completedNoncompliant.Contract = &protocol.ContractResult{Evaluated: true, Compliant: false}
	tests := []struct {
		name   string
		record agentclient.JobGetResult
		want   int
	}{
		{"queued", detailedRecord(protocol.PublicStateQueued), 2},
		{"running", detailedRecord(protocol.PublicStateRunning), 2},
		{"completed", detailedRecord(protocol.PublicStateCompleted), 0},
		{"completed noncompliant", completedNoncompliant, 3},
		{"failed default", detailedRecord(protocol.PublicStateFailed), 4},
		{"failed timeout", withFailure(detailedRecord(protocol.PublicStateFailed), protocol.FailureClassTimeout, "timeout"), 5},
		{"failed interrupted", withFailure(detailedRecord(protocol.PublicStateFailed), protocol.FailureClassInterrupted, "interrupted"), 6},
		{"canceled", detailedRecord(protocol.PublicStateCanceled), 7},
		{"unknown", detailedRecord(protocol.PublicStateUnknown), 14},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = fakeConnector(&fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": tt.record}})
			code, _, _ := runTestCLI(t, a, []string{"status", "--job", "job-1", "--json"})
			if code != tt.want {
				t.Fatalf("status exit=%d, want %d", code, tt.want)
			}
		})
	}
}

func TestCancelFetchesRecordForContractExitCode(t *testing.T) {
	record := detailedRecord(protocol.PublicStateCompleted)
	record.Contract = &protocol.ContractResult{Evaluated: true, Compliant: false}
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{
		records: map[string]agentclient.JobGetResult{"job-1": record},
		cancels: map[string]agentclient.JobCancelResult{"job-1": {JobID: "job-1", State: protocol.PublicStateCompleted}},
	})
	code, stdout, stderr := runTestCLI(t, a, []string{"cancel", "--job", "job-1", "--json"})
	if code != 3 || stderr != "" {
		t.Fatalf("cancel exit=%d stderr=%q", code, stderr)
	}
	if stdout != "{\"jobId\":\"job-1\",\"state\":\"completed\"}\n" {
		t.Fatalf("cancel JSON = %q", stdout)
	}
}

func TestProtocolErrorsAndStartupFailuresKeepTypedExitCodes(t *testing.T) {
	unknown := &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: "missing"})}
	startup := &daemonlaunch.StartupError{Kind: daemonlaunch.ErrReadinessEOF}
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{
		{"unknown job", unknown, cliExitUnknownJob},
		{"startup", startup, cliExitDaemonStartupFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) { return nil, tt.err }
			code, stdout, stderr := runTestCLI(t, a, []string{"status", "--job", "missing"})
			if code != tt.want || stdout != "" || stderr == "" {
				t.Fatalf("status=(%d,%q,%q), want code %d and diagnostics", code, stdout, stderr, tt.want)
			}
		})
	}
}

func TestServeBackgroundStartupFailureUsesExit11(t *testing.T) {
	a := testApp(t)
	a.daemonLauncher = func(context.Context, daemonlaunch.Options) (daemonlaunch.Result, error) {
		return daemonlaunch.Result{}, &daemonlaunch.StartupError{Kind: daemonlaunch.ErrReadinessEOF}
	}

	code, stdout, stderr := runTestCLI(t, a, []string{"serve"})
	if code != cliExitDaemonStartupFailure || stdout != "" || !strings.Contains(stderr, "serve:") {
		t.Fatalf("serve=(code=%d stdout=%q stderr=%q), want startup exit %d", code, stdout, stderr, cliExitDaemonStartupFailure)
	}
}

func TestServeForegroundStartupFailureUsesExit11(t *testing.T) {
	a := testApp(t)
	rootFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(rootFile, []byte("not a state root"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.stateRoot = rootFile

	code, stdout, stderr := runTestCLI(t, a, []string{"serve", "--foreground"})
	if code != cliExitDaemonStartupFailure || stdout != "" || !strings.Contains(stderr, "agentbus:") {
		t.Fatalf("serve foreground=(code=%d stdout=%q stderr=%q), want startup exit %d", code, stdout, stderr, cliExitDaemonStartupFailure)
	}
}

func TestStartBackgroundDaemonWritesPIDAfterReady(t *testing.T) {
	a := testApp(t)
	a.daemonLauncher = func(_ context.Context, opts daemonlaunch.Options) (daemonlaunch.Result, error) {
		if got, want := opts.Args, []string{"serve", "--foreground"}; !slices.Equal(got, want) {
			t.Fatalf("launch args=%v want=%v", got, want)
		}
		return daemonlaunch.Result{PID: 4242, CanonicalStateRoot: a.stateRoot}, nil
	}
	if err := a.startBackgroundDaemon(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(a.stateRoot, "agentbus.pid"))
	if err != nil || string(raw) != "4242\n" {
		t.Fatalf("pid file=(%q,%v)", raw, err)
	}
}

func TestCodexHomeSettingsPreferInheritance(t *testing.T) {
	t.Setenv("AGENTBUS_CODEX_HOME", "/override")
	t.Setenv("AGENTBUS_CODEX_HOME_INHERIT", "1")
	t.Setenv("CODEX_HOME", "/auth")
	override, inherit, authHome := codexHomeSettings()
	if override != "/override" || !inherit || authHome != "/auth" {
		t.Fatalf("settings=(%q,%t,%q)", override, inherit, authHome)
	}
}

func detailedRecord(state protocol.PublicState) agentclient.JobGetResult {
	created := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	return agentclient.JobGetResult{
		JobID:        "job-1",
		WorkspaceKey: "workspace-1",
		RequestID:    "request-1",
		Backend:      "codex",
		State:        state,
		CreatedAt:    created,
		Cleanup:      protocol.CleanupClean,
		Timeout: &engine.TimeoutResolution{
			Effective: 1800000,
			Source:    engine.TimeoutSourceClient,
		},
	}
}

func withResult(record agentclient.JobGetResult, result *protocol.ResultInfoWire) agentclient.JobGetResult {
	record.Result = result
	return record
}

func withFailure(record agentclient.JobGetResult, class protocol.FailureClass, reason string) agentclient.JobGetResult {
	record.Failure = &protocol.JobFailureWire{Class: class, Reason: reason}
	return record
}

func testApp(t *testing.T) *app {
	t.Helper()
	return &app{version: "test", stateRoot: filepath.Join(t.TempDir(), "state")}
}

func fakeConnector(client protocolClient) func(context.Context, agentclient.Options) (protocolClient, error) {
	return func(context.Context, agentclient.Options) (protocolClient, error) { return client, nil }
}

func runTestCLI(t *testing.T, a *app, args []string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := a.run(context.Background(), args, strings.NewReader(""), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

type fakeProtocolClient struct {
	records map[string]agentclient.JobGetResult
	list    agentclient.JobGetListResult
	cancels map[string]agentclient.JobCancelResult
	err     error
}

func (c *fakeProtocolClient) JobGet(_ context.Context, params agentclient.JobGetParams) (agentclient.JobGetResult, error) {
	if c.err != nil {
		return agentclient.JobGetResult{}, c.err
	}
	record, ok := c.records[params.JobID]
	if !ok {
		return agentclient.JobGetResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: params.JobID})}
	}
	return record, nil
}

func (c *fakeProtocolClient) JobGetList(context.Context) (agentclient.JobGetListResult, error) {
	if c.err != nil {
		return agentclient.JobGetListResult{}, c.err
	}
	return c.list, nil
}

func (c *fakeProtocolClient) JobCancel(_ context.Context, params agentclient.JobCancelParams) (agentclient.JobCancelResult, error) {
	if c.err != nil {
		return agentclient.JobCancelResult{}, c.err
	}
	result, ok := c.cancels[params.JobID]
	if !ok {
		return agentclient.JobCancelResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: params.JobID})}
	}
	return result, nil
}

func (c *fakeProtocolClient) Close() error { return nil }

func decodeJSON(t *testing.T, raw string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		t.Fatalf("decode JSON %q: %v", raw, err)
	}
}
