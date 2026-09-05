//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

func TestJobGetRequiresIDAndReturnsBareRecord(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	tags := map[string]string{"unit": "u9"}
	timeout := int64(250)
	params := submissionParams("get-workspace", "get-request", "fake", t.TempDir(), "get task")
	params.TaskSpec.Tags = &tags
	params.TaskSpec.TimeoutMS = &timeout
	submitted := submitResultForTest(t, submitForTest(t, server, params))

	empty := server.handleJobGet(json.RawMessage(`{}`))
	if empty.err == nil || empty.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("empty job.get = %#v, want typed invalid params", empty.err)
	}

	single := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted.JobID}))
	if single.err != nil {
		t.Fatalf("job.get single error = %#v", single.err)
	}
	record, ok := single.result.(protocol.JobRecordWire)
	if !ok {
		t.Fatalf("job.get single result = %T, want bare protocol.JobRecordWire", single.result)
	}
	if record.JobID != submitted.JobID || record.Tags["unit"] != "u9" || record.Timeout == nil || record.Timeout.Effective != timeout {
		t.Fatalf("job.get single record = %#v", record)
	}

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.Get(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	info, err := spillAuthoritativeResult(terminal, bytes.Repeat([]byte("x"), engine.DefaultInlineResultCap+1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(terminal.JobID, jobstore.TerminalUpdate{
		State:        protocol.PublicStateCompleted,
		Cleanup:      protocol.CleanupClean,
		ResultText:   info.Text,
		ResultPath:   info.ResultPath,
		ResultSHA256: info.SHA256,
		ResultBytes:  info.Bytes,
	}); err != nil {
		t.Fatal(err)
	}
	durable, err := store.Get(terminal.JobID)
	if err != nil {
		t.Fatal(err)
	}
	detailed := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: terminal.JobID}))
	if detailed.err != nil {
		t.Fatalf("job.get detailed result error = %#v", detailed.err)
	}
	detailedRecord, ok := detailed.result.(protocol.JobRecordWire)
	if !ok || detailedRecord.Result == nil {
		t.Fatalf("job.get detailed result = %#v, want record with result metadata", detailed.result)
	}
	if result := detailedRecord.Result; result.Text != durable.ResultText || result.ResultPath != durable.ResultPath || result.SHA256 != durable.ResultSHA256 || result.Bytes != durable.ResultBytes {
		t.Fatalf("job.get detailed result = %#v, want durable path:%q sha256:%q bytes:%d", result, durable.ResultPath, durable.ResultSHA256, durable.ResultBytes)
	}
}

func TestJobListFiltersCombineWithAND(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	submit := func(workspace, requestID string, tags map[string]string) protocol.JobSubmitResult {
		params := submissionParams(workspace, requestID, "fake", t.TempDir(), requestID)
		params.TaskSpec.Tags = &tags
		return submitResultForTest(t, submitForTest(t, server, params))
	}
	queued := submit("workspace-a", "queued", map[string]string{"project": "one", "team": "core"})
	failed := submit("workspace-b", "failed", map[string]string{"project": "one", "team": "ops"})
	completed := submit("workspace-a", "completed", map[string]string{"project": "two", "team": "core"})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(failed.JobID, jobstore.TerminalUpdate{
		State:        protocol.PublicStateFailed,
		Cleanup:      protocol.CleanupClean,
		FailureClass: protocol.FailureClassBackendError,
		FinishedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(completed.JobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCompleted,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		params protocol.JobListParams
		want   []string
	}{
		{name: "no filters", want: []string{queued.JobID, failed.JobID, completed.JobID}},
		{name: "workspace", params: protocol.JobListParams{WorkspaceKey: "workspace-a"}, want: []string{queued.JobID, completed.JobID}},
		{name: "tags", params: protocol.JobListParams{Tags: map[string]string{"project": "one"}}, want: []string{queued.JobID, failed.JobID}},
		{name: "states", params: protocol.JobListParams{States: []protocol.PublicState{protocol.PublicStateCompleted}}, want: []string{completed.JobID}},
		{name: "all filters", params: protocol.JobListParams{
			WorkspaceKey: "workspace-a",
			Tags:         map[string]string{"project": "one", "team": "core"},
			States:       []protocol.PublicState{protocol.PublicStateQueued},
		}, want: []string{queued.JobID}},
	} {
		outcome := server.handleJobList(mustJSON(t, test.params))
		if outcome.err != nil {
			t.Fatalf("%s job.list error = %#v", test.name, outcome.err)
		}
		result, ok := outcome.result.(protocol.JobListResult)
		if !ok {
			t.Fatalf("%s job.list result = %T, want protocol.JobListResult", test.name, outcome.result)
		}
		got := make(map[string]protocol.JobSummaryWire, len(result.Jobs))
		for _, summary := range result.Jobs {
			got[summary.JobID] = summary
		}
		if len(got) != len(test.want) {
			t.Fatalf("%s job.list = %#v, want IDs %#v", test.name, result.Jobs, test.want)
		}
		for _, jobID := range test.want {
			if _, ok := got[jobID]; !ok {
				t.Fatalf("%s job.list missing %q: %#v", test.name, jobID, result.Jobs)
			}
		}
		if test.name == "all filters" && got[queued.JobID].Tags["project"] != "one" {
			t.Fatalf("job.list summary tags = %#v, want projected tags", got[queued.JobID].Tags)
		}
	}
}

func TestJobListProjectsActiveActivityAndLiveness(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	params := submissionParams("activity-workspace", "activity-request", "fake", t.TempDir(), "activity")
	tags := map[string]string{"watch": "true"}
	params.TaskSpec.Tags = &tags
	submitted := submitResultForTest(t, submitForTest(t, server, params))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(submitted.JobID); err != nil {
		t.Fatal(err)
	}
	claim := jobstore.ProcessClaim{PID: 7191, PGID: 7191, StartToken: "activity-token"}
	record, err := store.RecordProcessClaim(submitted.JobID, claim)
	if err != nil {
		t.Fatal(err)
	}
	run := newActiveExecution(record.JobID, server.backends[record.Backend])
	lastItemAt := time.Now().UTC().Round(0)
	run.noteTranscriptItem(lastItemAt)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()

	for _, test := range []struct {
		name     string
		table    summaryProcessTable
		liveness protocol.Liveness
	}{
		{name: "matching claim", table: summaryProcessTable{info: engine.ProcessInfo{PID: claim.PID, StartTime: claim.StartToken}, alive: true}, liveness: protocol.LivenessAlive},
		{name: "token mismatch", table: summaryProcessTable{info: engine.ProcessInfo{PID: claim.PID, StartTime: "recycled-token"}, alive: true}, liveness: protocol.LivenessGone},
		{name: "unreadable claim", table: summaryProcessTable{err: errors.New("permission denied")}, liveness: protocol.LivenessUnknown},
	} {
		server.processTable = test.table
		outcome := server.handleJobList(mustJSON(t, protocol.JobListParams{}))
		if outcome.err != nil {
			t.Fatalf("%s job.list error = %#v", test.name, outcome.err)
		}
		result := outcome.result.(protocol.JobListResult)
		if len(result.Jobs) != 1 {
			t.Fatalf("%s job.list jobs = %#v", test.name, result.Jobs)
		}
		summary := result.Jobs[0]
		if summary.ItemCount == nil || *summary.ItemCount != 1 ||
			summary.LastItemAt == nil || !summary.LastItemAt.Equal(lastItemAt) ||
			summary.Liveness != test.liveness {
			t.Fatalf("%s active summary = %#v", test.name, summary)
		}
		encoded, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range [][]byte{[]byte(`"pid"`), []byte(`"pgid"`), []byte(`"startToken"`), []byte(`"processClaim"`)} {
			if bytes.Contains(encoded, forbidden) {
				t.Fatalf("%s summary JSON = %s, unexpectedly contains %s", test.name, encoded, forbidden)
			}
		}
	}

	if _, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCompleted,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	terminal := server.handleJobList(mustJSON(t, protocol.JobListParams{}))
	if terminal.err != nil {
		t.Fatalf("terminal job.list error = %#v", terminal.err)
	}
	summary := terminal.result.(protocol.JobListResult).Jobs[0]
	if summary.ItemCount != nil || summary.LastItemAt != nil || summary.Liveness != "" {
		t.Fatalf("terminal summary activity = %#v, want omitted fields", summary)
	}
}

func TestJobGetStartingSerializesRunning(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "fake"}}})
	submitted := submitResultForTest(t, submitForTest(t, server, submissionParams("starting-workspace", "starting-request", "fake", t.TempDir(), "task")))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(submitted.JobID); err != nil {
		t.Fatal(err)
	}

	outcome := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted.JobID}))
	if outcome.err != nil {
		t.Fatalf("job.get starting error = %#v", outcome.err)
	}
	wire, err := json.Marshal(outcome.result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"state":"running"`)) {
		t.Fatalf("serialized starting job = %s, want state running on the wire", wire)
	}
	if bytes.Contains(wire, []byte(`"state":"starting"`)) {
		t.Fatalf("serialized starting job leaked private state: %s", wire)
	}
}

func TestJobGetUnknownIDReturnsTypedError(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	outcome := server.handleJobGet(json.RawMessage(`{"jobId":"job_missing"}`))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorUnknownJob {
		t.Fatalf("unknown job.get outcome = %#v, want typed unknown-job error", outcome.err)
	}
}

func TestJobGetStoreReadFailureIsBackendUnavailable(t *testing.T) {
	root := t.TempDir()
	first := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "get-store-error"}}})
	submitted := submitResultForTest(t, submitForTest(t, first, submissionParams("get-store-error-workspace", "get-store-error-request", "get-store-error", t.TempDir(), "task")))
	first.closeJobStore()

	func() {
		db, err := bolt.Open(root+"/jobs.db", 0o600, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.Update(func(tx *bolt.Tx) error {
			jobs := tx.Bucket([]byte("jobs"))
			if jobs == nil {
				return errors.New("jobs bucket is missing")
			}
			return jobs.Put([]byte(submitted.JobID), []byte("not JSON"))
		}); err != nil {
			t.Fatal(err)
		}
	}()

	restarted := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "get-store-error"}}})
	outcome := restarted.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted.JobID}))
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorBackendUnavailable {
		t.Fatalf("job.get store-read error = %#v, want backend-unavailable error", outcome.err)
	}
}

func TestProjectLogPathsReportsTruncation(t *testing.T) {
	logs := t.TempDir()
	record := jobstore.Record{
		JobID: "job_log_projection",
		Artifacts: jobstore.ArtifactPaths{
			Log: filepath.Join(logs, "job_log_projection.log"),
		},
	}
	paths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: logs}, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.Stdout, []byte("backend stream"+engine.TruncationMarker()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.Stderr, []byte("backend stream completed\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	wire := projectLogPaths(record)
	if wire == nil {
		t.Fatal("projectLogPaths returned nil")
	}
	if wire.Stdout != paths.Stdout || wire.Stderr != paths.Stderr {
		t.Fatalf("log paths = %#v, want stdout %q and stderr %q", wire, paths.Stdout, paths.Stderr)
	}
	if wire.StdoutTruncated == nil || !*wire.StdoutTruncated {
		t.Fatalf("stdout truncated = %v, want true", wire.StdoutTruncated)
	}
	if wire.StderrTruncated == nil || *wire.StderrTruncated {
		t.Fatalf("stderr truncated = %v, want false", wire.StderrTruncated)
	}

	t.Cleanup(func() { _ = os.Chmod(paths.Stdout, 0o600) })
	if err := os.Chmod(paths.Stdout, 0o000); err != nil {
		t.Fatal(err)
	}
	if file, err := os.Open(paths.Stdout); err == nil {
		_ = file.Close()
		t.Fatal("stdout log remained readable after removing permissions")
	}
	wire = projectLogPaths(record)
	if wire == nil || wire.StdoutTruncated != nil {
		t.Fatalf("unreadable stdout projection = %#v, want nil truncation state", wire)
	}
}

func TestJobCancelBeforeSpawnIsDurable(t *testing.T) {
	var starts atomic.Int64
	backend := &executionFakeBackend{name: "cancel-before-spawn"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return claimedResultSession("late completion"), nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	submitted := submitResultForTest(t, submitForTest(t, server, submissionParams("cancel-queued-workspace", "cancel-queued-request", backend.Name(), t.TempDir(), "task")))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}

	// enqueueQueuedJob installs this same run before its goroutine can launch.
	// Installing it directly lets this test choose the cancel-before-spawn
	// partition without timing a scheduler race.
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	canceled := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
	if canceled.err != nil {
		t.Fatalf("job.cancel error = %#v", canceled.err)
	}
	server.runJob(context.Background(), record, run)
	if got := starts.Load(); got != 0 {
		t.Fatalf("backend Start calls = %d, want no spawn after durable cancellation", got)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateCanceled || stored.Cleanup != protocol.CleanupClean {
		t.Fatalf("record after cancel-before-spawn = %#v", stored)
	}
}

func TestJobCancelAfterSpawnRecordsCleanupOutcome(t *testing.T) {
	for _, tt := range []struct {
		name        string
		argv        []string
		waitForExit bool
		wantCleanup protocol.Cleanup
	}{
		{name: "clean inside grace", argv: []string{"sleep", "30"}, waitForExit: true, wantCleanup: protocol.CleanupClean},
		{name: "uncertain outside grace", argv: []string{"sh", "-c", "trap '' TERM; while :; do sleep 1; done"}, wantCleanup: protocol.CleanupUncertain},
	} {
		t.Run(tt.name, func(t *testing.T) {
			interrupted := false
			backend := &executionFakeBackend{name: "cancel-after-spawn-" + tt.name}
			server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
			server.processGroupGrace = 100 * time.Millisecond
			cmd, claim := startProcessGroup(t, tt.argv...)
			waitDone := make(chan error, 1)
			go func() {
				waitDone <- cmd.Wait()
				close(waitDone)
			}()
			t.Cleanup(func() { stopWaitedProcessGroup(claim.PGID, waitDone) })
			record := queuedExecutionRecord(t, server, backend.Name(), "task", nil)
			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.MarkStarting(record.JobID); err != nil {
				t.Fatal(err)
			}
			if _, err := store.RecordProcessClaim(record.JobID, claim); err != nil {
				t.Fatal(err)
			}
			run := newActiveExecution(record.JobID, backend)
			run.setSession(&executionFakeSession{interrupt: func(context.Context) error {
				interrupted = true
				if err := syscall.Kill(-claim.PGID, syscall.SIGTERM); err != nil {
					return err
				}
				if !tt.waitForExit {
					return nil
				}
				if err := <-waitDone; err != nil {
					if _, ok := err.(*exec.ExitError); !ok {
						return err
					}
				}
				return nil
			}})
			server.executionMu.Lock()
			server.executions = map[string]*activeExecution{record.JobID: run}
			server.executionMu.Unlock()

			outcome := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
			if outcome.err != nil {
				t.Fatalf("job.cancel error = %#v", outcome.err)
			}
			if !interrupted {
				t.Fatal("job.cancel did not invoke the active session's fenced interruption")
			}
			stored, err := store.Get(record.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != protocol.PublicStateCanceled || stored.Cleanup != tt.wantCleanup {
				t.Fatalf("canceled record = %#v, want state canceled cleanup %q", stored, tt.wantCleanup)
			}
		})
	}
}

func TestRestartQueuedBecomesFailedWithoutStart(t *testing.T) {
	root := t.TempDir()
	var starts atomic.Int64
	backend := &executionFakeBackend{name: "restart-queued"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return nil, errors.New("must not start recovered queued work")
	}
	first := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	submitted := submitResultForTest(t, submitForTest(t, first, submissionParams("restart-queued-workspace", "restart-queued-request", backend.Name(), t.TempDir(), "task")))
	first.closeJobStore()

	restarted := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	store, err := restarted.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateFailed || stored.FailureClass != protocol.FailureClassInternal {
		t.Fatalf("reconciled queued record = %#v", stored)
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("recovered queued backend Start calls = %d, want 0", got)
	}
}

func TestRestartRunningBecomesUnknownWithUncertainCleanup(t *testing.T) {
	root := t.TempDir()
	first := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "restart-running"}}})
	submitted := submitResultForTest(t, submitForTest(t, first, submissionParams("restart-running-workspace", "restart-running-request", "restart-running", t.TempDir(), "task")))
	store, err := first.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(submitted.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordProcessClaim(submitted.JobID, jobstore.ProcessClaim{PID: 2147483647, PGID: 2147483647, StartToken: "missing-running-token"}); err != nil {
		t.Fatal(err)
	}
	first.closeJobStore()

	restarted := newTestServer(t, root, Config{Backends: []engine.Backend{helloBackend{name: "restart-running"}}})
	store, err = restarted.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateUnknown || stored.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("reconciled running record = %#v", stored)
	}
}

func TestRestartPreservesUnknownTerminalRecordBytes(t *testing.T) {
	root := t.TempDir()
	var starts atomic.Int64
	backend := &executionFakeBackend{name: "restart-unknown"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		starts.Add(1)
		return nil, errors.New("must not launch a recovered unknown job")
	}
	first := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	submitted := submitResultForTest(t, submitForTest(t, first, submissionParams("restart-unknown-workspace", "restart-unknown-request", backend.Name(), t.TempDir(), "task")))
	store, err := first.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(submitted.JobID, jobstore.TerminalUpdate{State: protocol.PublicStateUnknown, Cleanup: protocol.CleanupUncertain}); err != nil {
		t.Fatal(err)
	}
	first.closeJobStore()
	before := rawJobRecord(t, root, submitted.JobID)

	restarted := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	store, err = restarted.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	restarted.closeJobStore()
	after := rawJobRecord(t, root, submitted.JobID)
	if !bytes.Equal(before, after) {
		t.Fatalf("terminal record bytes changed across restart\nbefore=%s\nafter=%s", before, after)
	}
	if got := starts.Load(); got != 0 {
		t.Fatalf("recovered terminal backend Start calls = %d, want 0", got)
	}
}

func TestOrphanReaperSignalsExactTokenMatch(t *testing.T) {
	for _, tt := range []struct {
		name              string
		secondLookupToken string
		wantCleanup       protocol.Cleanup
		wantEvents        []string
	}{
		{
			name:        "matching token signals in order",
			wantCleanup: protocol.CleanupClean,
			wantEvents:  []string{"lookup", "group-present", "lookup", "TERM", "group-gone"},
		},
		{
			name:              "changed second token does not signal",
			secondLookupToken: "recycled-pid-token",
			wantCleanup:       protocol.CleanupUncertain,
			wantEvents:        []string{"lookup", "group-present", "lookup"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "reaper-exact"}}})
			fixture := newOrphanReaperFixture()
			if tt.secondLookupToken != "" {
				fixture.lookupTokens = []string{fixture.startToken, tt.secondLookupToken}
			}
			server.processTable = fixture
			server.processGroups = fixture
			server.processGroupGoneFn = fixture.processGroupGone
			claim := fixture.claim()
			record := persistRunningClaim(t, server, "reaper-exact", claim)
			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			if err := server.reconcileRecoveredJobs(store); err != nil {
				t.Fatal(err)
			}
			stored, err := store.Get(record.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != protocol.PublicStateUnknown || stored.Cleanup != tt.wantCleanup {
				t.Fatalf("exact-token reaped record = %#v, want cleanup %s", stored, tt.wantCleanup)
			}
			if !slices.Equal(fixture.events, tt.wantEvents) {
				t.Fatalf("exact-token events = %#v, want %#v", fixture.events, tt.wantEvents)
			}
		})
	}
}

func TestOrphanReaperDoesNotSignalTokenMismatch(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "reaper-mismatch"}}})
	fixture := newOrphanReaperFixture()
	server.processTable = fixture
	server.processGroups = fixture
	server.processGroupGoneFn = fixture.processGroupGone
	claim := fixture.claim()
	claim.StartToken = "recycled-pid-token"
	record := persistRunningClaim(t, server, "reaper-mismatch", claim)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	if fixture.lookups != 1 || len(fixture.signals) != 0 || len(fixture.groupGone) != 0 {
		t.Fatalf("mismatched-token fixture activity = lookups:%d signals:%#v group-gone:%#v, want one lookup and no group action", fixture.lookups, fixture.signals, fixture.groupGone)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateUnknown || stored.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("mismatched-token reaped record = %#v", stored)
	}
	if len(stored.Diagnostics) != 2 || stored.Diagnostics[1] != "orphan reaper: leader start token mismatch; no signal sent" {
		t.Fatalf("mismatched-token diagnostics = %#v", stored.Diagnostics)
	}
}

type processGroupSignalCall struct {
	pgid   int
	signal syscall.Signal
}

type orphanReaperFixture struct {
	pid        int
	pgid       int
	startToken string
	groupLive  bool

	lookupTokens []string
	lookupIndex  int
	events       []string

	lookups   int
	signals   []processGroupSignalCall
	groupGone []bool
}

type summaryProcessTable struct {
	info  engine.ProcessInfo
	alive bool
	err   error
}

func (table summaryProcessTable) Lookup(int) (engine.ProcessInfo, bool, error) {
	return table.info, table.alive, table.err
}

func newOrphanReaperFixture() *orphanReaperFixture {
	return &orphanReaperFixture{
		pid:        4101,
		pgid:       4101,
		startToken: "exact-token",
		groupLive:  true,
	}
}

func (f *orphanReaperFixture) claim() jobstore.ProcessClaim {
	return jobstore.ProcessClaim{PID: f.pid, PGID: f.pgid, StartToken: f.startToken}
}

func (f *orphanReaperFixture) Lookup(pid int) (engine.ProcessInfo, bool, error) {
	f.events = append(f.events, "lookup")
	f.lookups++
	if pid != f.pid {
		return engine.ProcessInfo{}, false, nil
	}
	startToken := f.startToken
	if f.lookupIndex < len(f.lookupTokens) {
		startToken = f.lookupTokens[f.lookupIndex]
	}
	f.lookupIndex++
	return engine.ProcessInfo{PID: f.pid, StartTime: startToken}, true, nil
}

func (f *orphanReaperFixture) SignalProcessGroup(pgid int, signal syscall.Signal) error {
	f.signals = append(f.signals, processGroupSignalCall{pgid: pgid, signal: signal})
	if pgid != f.pgid {
		return errors.New("unexpected process group")
	}
	if signal != syscall.SIGTERM {
		return errors.New("unexpected process-group signal")
	}
	f.events = append(f.events, "TERM")
	f.groupLive = false
	return nil
}

func (f *orphanReaperFixture) processGroupGone(pgid int) (bool, error) {
	if pgid != f.pgid {
		return false, errors.New("unexpected process group")
	}
	seenGone := !f.groupLive
	f.groupGone = append(f.groupGone, seenGone)
	if seenGone {
		f.events = append(f.events, "group-gone")
	} else {
		f.events = append(f.events, "group-present")
	}
	return seenGone, nil
}

func rawJobRecord(t *testing.T, root, jobID string) []byte {
	t.Helper()
	db, err := bolt.Open(root+"/jobs.db", 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var raw []byte
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("jobs"))
		if bucket == nil {
			return errors.New("jobs bucket is missing")
		}
		raw = append([]byte(nil), bucket.Get([]byte(jobID))...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return raw
}

func persistRunningClaim(t *testing.T, server *Server, backend string, claim jobstore.ProcessClaim) jobstore.Record {
	t.Helper()
	record := queuedExecutionRecord(t, server, backend, "task", nil)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordProcessClaim(record.JobID, claim); err != nil {
		t.Fatal(err)
	}
	return record
}

func startProcessGroup(t *testing.T, argv ...string) (*exec.Cmd, jobstore.ProcessClaim) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	info, alive, err := (engine.NativeProcessTable{}).Lookup(pid)
	if err != nil || !alive || info.StartTime == "" {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = cmd.Wait()
		t.Fatalf("read reaper process identity: alive=%t info=%#v err=%v", alive, info, err)
	}
	return cmd, jobstore.ProcessClaim{PID: pid, PGID: pgid, StartToken: info.StartTime}
}

func stopWaitedProcessGroup(pgid int, waitDone <-chan error) {
	if pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
	select {
	case <-waitDone:
	case <-time.After(time.Second):
	}
}
