package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

func TestHelpExitsSuccessfullyWithoutConnecting(t *testing.T) {
	for _, args := range [][]string{
		{"--help"}, {"-h"}, {"help"}, {"status", "--help"}, {"result", "-h"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			a := testApp(t)
			a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
				t.Fatal("help attempted to connect to the daemon")
				return nil, nil
			}
			code, stdout, stderr := runTestCLI(t, a, args)
			if code != 0 || stderr != "" || !strings.Contains(stdout, "Usage:") {
				t.Fatalf("help=(%d,%q,%q), want usage on stdout and exit 0", code, stdout, stderr)
			}
		})
	}
}

func TestBareWaitDurationRejectedWithUnitSuggestions(t *testing.T) {
	a := testApp(t)
	a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
		t.Fatal("invalid duration attempted to connect to the daemon")
		return nil, nil
	}
	code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1", "--wait", "--wait-timeout=2400"})
	if code != 2 || stdout != "" || !strings.Contains(stderr, "2400 is ambiguous") || !strings.Contains(stderr, "2400s") || !strings.Contains(stderr, "40m") {
		t.Fatalf("bare duration=(%d,%q,%q), want actionable usage error", code, stdout, stderr)
	}
}

func TestResultWaitExpiryReportsRecordWithoutCanceling(t *testing.T) {
	client := &fakeProtocolClient{records: map[string]agentclient.JobGetResult{"job-1": detailedRecord(protocol.PublicStateRunning)}}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	code, stdout, stderr := runTestCLI(t, a, []string{"result", "--job", "job-1", "--wait", "--wait-timeout", "1ns", "--json"})
	if code != cliExitObservationTimeout || stderr != "" {
		t.Fatalf("wait expiry=(%d,%q,%q), want observation timeout and record", code, stdout, stderr)
	}
	var record agentclient.JobGetResult
	decodeJSON(t, stdout, &record)
	if record.JobID != "job-1" || record.State != protocol.PublicStateRunning || len(client.cancelParams) != 0 {
		t.Fatalf("wait result=%#v cancel calls=%d, want running record and no cancellation", record, len(client.cancelParams))
	}
}

func TestResultWaitExpiryDuringFirstLookupReportsRecord(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &fakeProtocolClient{jobGetFunc: func(ctx context.Context, params agentclient.JobGetParams) (agentclient.JobGetResult, error) {
		close(started)
		guard := time.NewTimer(5 * time.Second)
		defer guard.Stop()
		select {
		case <-ctx.Done():
			return agentclient.JobGetResult{}, ctx.Err()
		case <-release:
			return detailedRecord(protocol.PublicStateRunning), nil
		case <-guard.C:
			return agentclient.JobGetResult{}, errors.New("fake JobGet response timed out")
		}
	}}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	type result struct {
		code   int
		stdout string
		stderr string
	}
	resultCh := make(chan result, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		code := a.run(context.Background(), []string{"result", "--job", "job-1", "--wait", "--wait-timeout", "1ms", "--json"}, strings.NewReader(""), &stdout, &stderr)
		resultCh <- result{code: code, stdout: stdout.String(), stderr: stderr.String()}
	}()
	startGuard := time.NewTimer(5 * time.Second)
	select {
	case <-started:
	case <-startGuard.C:
		t.Fatal("timed out waiting for the first JobGet call")
	}
	startGuard.Stop()
	wait := time.NewTimer(20 * time.Millisecond)
	waitGuard := time.NewTimer(5 * time.Second)
	select {
	case <-wait.C:
	case <-waitGuard.C:
		t.Fatal("timed out waiting for the observation deadline")
	}
	wait.Stop()
	waitGuard.Stop()
	close(release)
	resultGuard := time.NewTimer(5 * time.Second)
	var got result
	select {
	case got = <-resultCh:
	case <-resultGuard.C:
		t.Fatal("timed out waiting for the result command")
	}
	resultGuard.Stop()
	if got.code != cliExitObservationTimeout || got.stderr != "" {
		t.Fatalf("wait expiry=(%d,%q,%q), want observation timeout and first record", got.code, got.stdout, got.stderr)
	}
	var record agentclient.JobGetResult
	decodeJSON(t, got.stdout, &record)
	if record.JobID != "job-1" || record.State != protocol.PublicStateRunning {
		t.Fatalf("wait result=%#v, want the observed running record", record)
	}
}

func TestTranscriptFollowDrainsFullPageToTerminalAndReportsGap(t *testing.T) {
	items := make([]agentclient.TranscriptItem, cliTranscriptFollowPageSize+1)
	for i := range items {
		items[i] = agentclient.TranscriptItem{Ordinal: i + 1, Kind: "message", Text: "item"}
	}
	client := &fakeProtocolClient{
		records: map[string]agentclient.JobGetResult{"job-1": detailedRecord(protocol.PublicStateCompleted)},
		transcriptFunc: func(params agentclient.JobTranscriptParams, call int) (agentclient.JobTranscriptResult, error) {
			if call == 1 {
				return agentclient.JobTranscriptResult{State: protocol.PublicStateCompleted, ItemCount: len(items), Items: items[:cliTranscriptFollowPageSize]}, nil
			}
			return agentclient.JobTranscriptResult{State: protocol.PublicStateCompleted, ItemCount: len(items), Items: items[cliTranscriptFollowPageSize:], Gap: true}, nil
		},
	}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	code, stdout, stderr := runTestCLI(t, a, []string{"transcript", "--job", "job-1", "--follow", "--kind", "message", "--json"})
	if code != 0 || stderr != "" {
		t.Fatalf("follow=(%d,%q), want completed with no error", code, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != len(items)+1 {
		t.Fatalf("follow emitted %d JSON lines, want %d items plus summary", len(lines), len(items)+1)
	}
	var summary transcriptFollowSummary
	decodeJSON(t, lines[len(lines)-1], &summary)
	if summary.Type != "summary" || summary.State != protocol.PublicStateCompleted || !summary.Gap {
		t.Fatalf("follow summary=%#v, want terminal gap report", summary)
	}
	if len(client.transcriptParams) != 2 || client.transcriptParams[0].Limit == nil || *client.transcriptParams[0].Limit != cliTranscriptFollowPageSize ||
		client.transcriptParams[1].SinceOrdinal == nil || *client.transcriptParams[1].SinceOrdinal != cliTranscriptFollowPageSize {
		t.Fatalf("follow page requests=%#v, want a drained second ordinal page", client.transcriptParams)
	}
}

func TestTranscriptFollowSummaryWriteErrorExitsNonzero(t *testing.T) {
	client := &fakeProtocolClient{
		records:     map[string]agentclient.JobGetResult{"job-1": detailedRecord(protocol.PublicStateCompleted)},
		transcripts: map[string]agentclient.JobTranscriptResult{"job-1": {State: protocol.PublicStateCompleted}},
	}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	var stderr bytes.Buffer
	code := a.run(context.Background(), []string{"transcript", "--job", "job-1", "--follow"}, strings.NewReader(""), failingWriter{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "write failed") {
		t.Fatalf("follow summary write=(%d,%q), want output error", code, stderr.String())
	}
}

func TestStatusListTruncationAndContinuation(t *testing.T) {
	client := &fakeProtocolClient{list: agentclient.JobListResult{
		Jobs:      []agentclient.JobSummaryWire{{JobID: "newest", State: protocol.PublicStateCompleted}, {JobID: "older", State: protocol.PublicStateFailed}},
		Truncated: true, NextCursor: "cursor-2",
	}}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	code, stdout, stderr := runTestCLI(t, a, []string{"status", "--limit", "2", "--json"})
	if code != 0 || stderr != "" {
		t.Fatalf("JSON list=(%d,%q), want success without a text notice", code, stderr)
	}
	var page agentclient.JobListResult
	decodeJSON(t, stdout, &page)
	if !page.Truncated || page.NextCursor != "cursor-2" || len(page.Jobs) != 2 || client.listParams[0].Limit != 2 {
		t.Fatalf("first page=%#v params=%#v, want truncated limit-2 page", page, client.listParams)
	}
	code, _, stderr = runTestCLI(t, a, []string{"status", "--limit", "2", "--before", page.NextCursor})
	if code != 0 || !strings.Contains(stderr, "status list truncated") || !strings.Contains(stderr, "nextCursor=cursor-2") {
		t.Fatalf("text list=(%d,%q), want visible truncation notice", code, stderr)
	}
	if len(client.listParams) != 2 || client.listParams[1].Cursor != page.NextCursor || client.listParams[1].Limit != 2 {
		t.Fatalf("continuation params=%#v, want before cursor and limit 2", client.listParams)
	}
}

func TestDuplicateSingletonFlagIsUsageError(t *testing.T) {
	a := testApp(t)
	a.clientConnect = func(context.Context, agentclient.Options) (protocolClient, error) {
		t.Fatal("duplicate flag attempted to connect to the daemon")
		return nil, nil
	}
	code, stdout, stderr := runTestCLI(t, a, []string{"status", "--job", "job-a", "--job", "job-b"})
	if code != 2 || stdout != "" || !strings.Contains(stderr, "--job specified more than once") {
		t.Fatalf("duplicate singleton=(%d,%q,%q), want usage error", code, stdout, stderr)
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
	record.Execution = &protocol.ExecutionPosture{
		Intent: "inspect", Network: "denied", GitMetadata: "read-only", Enforcement: "os-sandbox",
	}
	record.Steers = []protocol.SteerSummaryWire{{
		MessageID: "message-1", Status: protocol.SteerStatusDelivered,
		Mechanism: "native", Time: finished.Add(time.Minute),
	}}
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
		"inputMode=fixed model=gpt-5 tags=a=first,z=last",
		"result.bytes=18 result.sha256=sha256:aaaaaaaaaaaa",
		"failure.class=backend_error failure.reason=provider stopped",
		"contract.evaluated=true contract.compliant=false",
		"logPaths.stdout=/logs/out logPaths.stderr=/logs/err",
		`execution={"intent":"inspect","sourceWritable":false,"network":"denied","gitMetadata":"read-only","enforcement":"os-sandbox"}`,
		"steer.messageId=message-1 status=delivered mechanism=native time=2026-01-02T03:07:05Z",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("status output missing %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "secret result text") || strings.Contains(stdout, "/state/results/job-1") {
		t.Fatalf("status exposed result text or path: %q", stdout)
	}
}

func TestSteerCommandReceiptAndExitCodes(t *testing.T) {
	for _, test := range []struct {
		name   string
		status protocol.SteerStatus
		want   int
	}{
		{name: "admitted", status: protocol.SteerStatusAdmitted, want: 0},
		{name: "delivered", status: protocol.SteerStatusDelivered, want: 0},
		{name: "rejected", status: protocol.SteerStatusRejected, want: 17},
		{name: "unknown", status: protocol.SteerStatusUnknown, want: 18},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeProtocolClient{steer: agentclient.JobSteerResult{
				JobID: "job-1", MessageID: "message-1", Status: test.status,
				Reason: protocol.SteerReasonDeliveryUnconfirmed,
			}}
			a := testApp(t)
			a.clientConnect = fakeConnector(client)
			var stdout, stderr bytes.Buffer
			code := a.run(context.Background(), []string{
				"steer", "--job", "job-1", "--request-id", "message-1", "--message-file", "-", "--json",
			}, strings.NewReader("follow-up"), &stdout, &stderr)
			if code != test.want || stderr.Len() != 0 {
				t.Fatalf("steer exit=%d stderr=%q, want exit=%d", code, stderr.String(), test.want)
			}
			var receipt agentclient.JobSteerResult
			decodeJSON(t, stdout.String(), &receipt)
			if receipt.Status != test.status || client.steerParams.JobID != "job-1" || client.steerParams.MessageID != "message-1" || client.steerParams.Text != "follow-up" {
				t.Fatalf("steer receipt=%#v params=%#v", receipt, client.steerParams)
			}
		})
	}
}

func TestStatusListHumanProjectionIncludesFailureClassAndContractVerdict(t *testing.T) {
	created := time.Now().UTC().Add(-2 * time.Minute)
	lastItemAt := created.Add(time.Minute)
	lastActivityAt := lastItemAt.Add(time.Second)
	itemCount := 3
	a := testApp(t)
	a.clientConnect = fakeConnector(&fakeProtocolClient{list: agentclient.JobListResult{Jobs: []agentclient.JobSummaryWire{{
		JobID:          "job-1",
		Backend:        "codex",
		State:          protocol.PublicStateRunning,
		Tags:           map[string]string{"team": "core"},
		Cleanup:        protocol.CleanupUncertain,
		CreatedAt:      created,
		FailureClass:   protocol.FailureClassBackendError,
		Contract:       &protocol.ContractVerdict{Evaluated: true, Compliant: false},
		ItemCount:      &itemCount,
		LastItemAt:     &lastItemAt,
		LastActivityAt: &lastActivityAt,
		Liveness:       protocol.LivenessAlive,
	}}}})
	code, stdout, stderr := runTestCLI(t, a, []string{"status"})
	if code != 0 || stderr != "" {
		t.Fatalf("status list exit=%d stderr=%q", code, stderr)
	}
	if !strings.Contains(stdout, "jobId=job-1 state=running backend=codex cleanup=uncertain age=") ||
		!strings.Contains(stdout, "failure.class=backend_error") ||
		!strings.Contains(stdout, "contract.evaluated=true contract.compliant=false") ||
		!strings.Contains(stdout, "tags=team=core itemCount=3") ||
		!strings.Contains(stdout, "lastItemAt="+lastItemAt.Format(time.RFC3339Nano)) ||
		!strings.Contains(stdout, "lastActivityAt="+lastActivityAt.Format(time.RFC3339Nano)) ||
		!strings.Contains(stdout, "liveness=alive") {
		t.Fatalf("status list = %q", stdout)
	}

	noActivity := testApp(t)
	noActivity.clientConnect = fakeConnector(&fakeProtocolClient{list: agentclient.JobListResult{Jobs: []agentclient.JobSummaryWire{{
		JobID: "job-2", Backend: "codex", State: protocol.PublicStateRunning, Cleanup: protocol.CleanupClean, CreatedAt: created,
	}}}})
	code, stdout, stderr = runTestCLI(t, noActivity, []string{"status"})
	if code != 0 || stderr != "" || strings.Contains(stdout, "lastActivityAt=") {
		t.Fatalf("status without activity = (%d,%q,%q), want unchanged human projection", code, stdout, stderr)
	}
}

func TestStatusListDefaultsAllWorkspacesAndFiltersExplicitWorkspace(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantCode      int
		wantWorkspace string
		wantTags      map[string]string
		wantStates    []protocol.PublicState
	}{
		{
			name:     "default lists every workspace",
			args:     []string{"status"},
			wantCode: 0,
		},
		{
			name:          "explicit workspace key filters the list",
			args:          []string{"status", "--workspace-key", "delegate-v1-workspace", "--tag", "team=core", "--state", "running"},
			wantCode:      0,
			wantWorkspace: "delegate-v1-workspace",
			wantTags:      map[string]string{"team": "core"},
			wantStates:    []protocol.PublicState{protocol.PublicStateRunning},
		},
		{
			name:     "all workspaces flag is removed",
			args:     []string{"status", "--all-workspaces"},
			wantCode: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &fakeProtocolClient{list: agentclient.JobListResult{}}
			a := testApp(t)
			a.clientConnect = fakeConnector(client)

			code, _, stderr := runTestCLI(t, a, tt.args)
			if code != tt.wantCode {
				t.Fatalf("status = (%d,%q), want %d", code, stderr, tt.wantCode)
			}
			if tt.wantCode != 0 {
				if len(client.listParams) != 0 {
					t.Fatalf("removed flag made %d job.list calls, want 0", len(client.listParams))
				}
				return
			}
			if len(client.listParams) != 1 {
				t.Fatalf("job.list calls = %d, want 1", len(client.listParams))
			}
			if got := client.listParams[0]; got.WorkspaceKey != tt.wantWorkspace ||
				!slices.Equal(got.States, tt.wantStates) ||
				len(got.Tags) != len(tt.wantTags) {
				t.Fatalf("job.list params = %#v", got)
			} else {
				for key, want := range tt.wantTags {
					if got.Tags[key] != want {
						t.Fatalf("job.list tags = %#v, want %s=%s", got.Tags, key, want)
					}
				}
			}
		})
	}
}

func TestTranscriptForwardsSelectorsAndPrintsDigest(t *testing.T) {
	since := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	transcript := agentclient.JobTranscriptResult{
		State:     protocol.PublicStateRunning,
		Liveness:  protocol.LivenessAlive,
		ItemCount: 5,
		Counts: map[string]int{
			"message": 1, "reasoning": 1, "tool": 1, "toolResult": 1, "fileChange": 0, "warning": 0, "error": 1,
		},
		Items: []agentclient.TranscriptItem{
			{Ordinal: 7, At: since.Add(time.Second), Kind: "message", Name: "tool\nname", Text: "hello\nworld"},
		},
	}
	client := &fakeProtocolClient{
		records:     map[string]agentclient.JobGetResult{"job-1": detailedRecord(protocol.PublicStateRunning)},
		transcripts: map[string]agentclient.JobTranscriptResult{"job-1": transcript},
	}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)

	code, stdout, stderr := runTestCLI(t, a, []string{
		"transcript", "--job", "job-1", "--kind", "message", "--kind", "error",
		"--since", since.Format(time.RFC3339Nano), "--since-ordinal", "6", "--last", "3", "--limit", "2",
	})
	if code != 2 || stderr != "" {
		t.Fatalf("selected transcript = (%d,%q)", code, stderr)
	}
	if len(client.transcriptParams) != 1 {
		t.Fatalf("job.transcript calls = %d, want 1", len(client.transcriptParams))
	}
	params := client.transcriptParams[0]
	if params.JobID != "job-1" || !slices.Equal(params.Kinds, []string{"message", "error"}) || params.Since == nil || !params.Since.Equal(since) || params.SinceOrdinal == nil || *params.SinceOrdinal != 6 || params.Last == nil || *params.Last != 3 || params.Limit == nil || *params.Limit != 2 {
		t.Fatalf("transcript params = %#v", params)
	}
	if !strings.Contains(stdout, "state=running itemCount=5 liveness=alive gap=false") || !strings.Contains(stdout, "7 kind=message") || !strings.Contains(stdout, `name="tool\nname"`) || !strings.Contains(stdout, `text="hello\nworld"`) {
		t.Fatalf("selected transcript output = %q", stdout)
	}
	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[1], "7 ") {
		t.Fatalf("selected transcript item must remain on one ordinal-prefixed line: %q", stdout)
	}
	if strings.Contains(stdout, "counts message=") {
		t.Fatalf("selected transcript unexpectedly printed digest counts: %q", stdout)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"transcript", "--job", "job-1"})
	if code != 2 || stderr != "" || !strings.Contains(stdout, "counts message=1 reasoning=1 tool=1 toolResult=1 fileChange=0 warning=0 error=1") {
		t.Fatalf("digest transcript = (%d,%q,%q)", code, stdout, stderr)
	}

	code, stdout, stderr = runTestCLI(t, a, []string{"transcript", "--job", "job-1", "--json"})
	if code != 2 || stderr != "" {
		t.Fatalf("JSON transcript = (%d,%q)", code, stderr)
	}
	var decoded agentclient.JobTranscriptResult
	decodeJSON(t, stdout, &decoded)
	if decoded.ItemCount != transcript.ItemCount || len(decoded.Items) != 1 || decoded.Items[0].Ordinal != 7 {
		t.Fatalf("JSON transcript = %#v", decoded)
	}
}

func TestTranscriptShowsPrunedIndication(t *testing.T) {
	record := detailedRecord(protocol.PublicStateCompleted)
	client := &fakeProtocolClient{
		records:     map[string]agentclient.JobGetResult{"job-1": record},
		transcripts: map[string]agentclient.JobTranscriptResult{"job-1": {State: protocol.PublicStateCompleted, Gap: true, Pruned: true}},
	}
	a := testApp(t)
	a.clientConnect = fakeConnector(client)
	code, stdout, stderr := runTestCLI(t, a, []string{"transcript", "--job", "job-1"})
	if code != 0 || stderr != "" || !strings.Contains(stdout, "pruned=true") {
		t.Fatalf("pruned transcript=(%d,%q,%q)", code, stdout, stderr)
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
			client := &fakeProtocolClient{
				records:     map[string]agentclient.JobGetResult{"job-1": tt.record},
				transcripts: map[string]agentclient.JobTranscriptResult{"job-1": {State: tt.record.State}},
				cancels:     map[string]agentclient.JobCancelResult{"job-1": {JobID: "job-1", State: tt.record.State}},
			}
			a := testApp(t)
			a.clientConnect = fakeConnector(client)
			statusCode, _, _ := runTestCLI(t, a, []string{"status", "--job", "job-1", "--json"})
			resultCode, _, _ := runTestCLI(t, a, []string{"result", "--job", "job-1", "--json"})
			transcriptCode, _, _ := runTestCLI(t, a, []string{"transcript", "--job", "job-1", "--json"})
			cancelCode, _, _ := runTestCLI(t, a, []string{"cancel", "--job", "job-1", "--json"})
			if statusCode != tt.want || resultCode != tt.want || transcriptCode != tt.want || cancelCode != tt.want {
				t.Fatalf("selected-job exits: status=%d result=%d transcript=%d cancel=%d, want %d", statusCode, resultCode, transcriptCode, cancelCode, tt.want)
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
		if got, want := opts.Args, []string{"serve", "--foreground", "--prune-interval", "24h0m0s"}; !slices.Equal(got, want) {
			t.Fatalf("launch args=%v want=%v", got, want)
		}
		return daemonlaunch.Result{PID: 4242, CanonicalStateRoot: a.stateRoot}, nil
	}
	if err := a.startBackgroundDaemon(context.Background(), 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(a.stateRoot, "agentbus.pid"))
	if err != nil || string(raw) != "4242\n" {
		t.Fatalf("pid file=(%q,%v)", raw, err)
	}
}

func TestPruneDefaultsToDryRunAndSupportsApply(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		apply bool
	}{
		{name: "default dry-run", args: []string{"prune", "--json"}},
		{name: "apply", args: []string{"prune", "--apply"}, apply: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dryRun := !test.apply
			client := &fakeProtocolClient{prune: agentclient.ArtifactsPruneResult{
				DryRun:              dryRun,
				SuccessfulOlderThan: "168h",
				FailedOlderThan:     "720h",
				WouldRemove:         []agentclient.PruneItem{{JobID: "job-1", Kind: "log", Path: "/state/artifacts/logs/job-1.stdout.log", Bytes: 3}},
				Removed:             []agentclient.PruneItem{},
				Skipped:             []agentclient.PruneSkip{{JobID: "job-2", Reason: "session is retained"}},
				WouldRemoveBytes:    3,
			}}
			a := testApp(t)
			a.clientConnect = fakeConnector(client)
			code, stdout, stderr := runTestCLI(t, a, test.args)
			if code != 0 || stderr != "" {
				t.Fatalf("prune=(%d,%q,%q)", code, stdout, stderr)
			}
			if len(client.pruneParams) != 1 {
				t.Fatalf("prune params = %+v", client.pruneParams)
			}
			params := client.pruneParams[0]
			if params.DryRun == nil || *params.DryRun != !test.apply || params.SuccessfulOlderThan != "168h0m0s" || params.FailedOlderThan != "720h0m0s" {
				t.Fatalf("prune params = %+v", params)
			}
			if test.apply {
				if !strings.Contains(stdout, "mode=apply") || !strings.Contains(stdout, "skipped jobId=job-2") {
					t.Fatalf("apply output = %q", stdout)
				}
			} else if !strings.Contains(stdout, `"wouldRemoveBytes":3`) || !strings.Contains(stdout, `"dryRun":true`) {
				t.Fatalf("dry-run JSON = %q", stdout)
			}
		})
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
	records          map[string]agentclient.JobGetResult
	jobGetFunc       func(context.Context, agentclient.JobGetParams) (agentclient.JobGetResult, error)
	list             agentclient.JobListResult
	listParams       []agentclient.JobListParams
	transcripts      map[string]agentclient.JobTranscriptResult
	transcriptParams []agentclient.JobTranscriptParams
	transcriptFunc   func(agentclient.JobTranscriptParams, int) (agentclient.JobTranscriptResult, error)
	cancels          map[string]agentclient.JobCancelResult
	prune            agentclient.ArtifactsPruneResult
	pruneParams      []agentclient.ArtifactsPruneParams
	steer            agentclient.JobSteerResult
	steerParams      agentclient.JobSteerParams
	cancelParams     []agentclient.JobCancelParams
	err              error
}

func (c *fakeProtocolClient) JobGet(ctx context.Context, params agentclient.JobGetParams) (agentclient.JobGetResult, error) {
	if c.err != nil {
		return agentclient.JobGetResult{}, c.err
	}
	if c.jobGetFunc != nil {
		return c.jobGetFunc(ctx, params)
	}
	record, ok := c.records[params.JobID]
	if !ok {
		return agentclient.JobGetResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: params.JobID})}
	}
	return record, nil
}

func (c *fakeProtocolClient) JobList(_ context.Context, params agentclient.JobListParams) (agentclient.JobListResult, error) {
	if c.err != nil {
		return agentclient.JobListResult{}, c.err
	}
	c.listParams = append(c.listParams, params)
	return c.list, nil
}

func (c *fakeProtocolClient) JobTranscript(_ context.Context, params agentclient.JobTranscriptParams) (agentclient.JobTranscriptResult, error) {
	if c.err != nil {
		return agentclient.JobTranscriptResult{}, c.err
	}
	c.transcriptParams = append(c.transcriptParams, params)
	if c.transcriptFunc != nil {
		return c.transcriptFunc(params, len(c.transcriptParams))
	}
	result, ok := c.transcripts[params.JobID]
	if !ok {
		return agentclient.JobTranscriptResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: params.JobID})}
	}
	return result, nil
}

func (c *fakeProtocolClient) JobCancel(_ context.Context, params agentclient.JobCancelParams) (agentclient.JobCancelResult, error) {
	if c.err != nil {
		return agentclient.JobCancelResult{}, c.err
	}
	c.cancelParams = append(c.cancelParams, params)
	result, ok := c.cancels[params.JobID]
	if !ok {
		return agentclient.JobCancelResult{}, &protocol.RPCError{Object: *protocol.NewError(protocol.ErrorUnknownJob, "not found", protocol.ErrorData{JobID: params.JobID})}
	}
	return result, nil
}

func (c *fakeProtocolClient) ArtifactsPrune(_ context.Context, params agentclient.ArtifactsPruneParams) (agentclient.ArtifactsPruneResult, error) {
	if c.err != nil {
		return agentclient.ArtifactsPruneResult{}, c.err
	}
	c.pruneParams = append(c.pruneParams, params)
	return c.prune, nil
}

func (c *fakeProtocolClient) Steer(_ context.Context, params agentclient.JobSteerParams) (agentclient.JobSteerResult, error) {
	if c.err != nil {
		return agentclient.JobSteerResult{}, c.err
	}
	c.steerParams = params
	return c.steer, nil
}

func (c *fakeProtocolClient) Close() error { return nil }

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func decodeJSON(t *testing.T, raw string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		t.Fatalf("decode JSON %q: %v", raw, err)
	}
}
