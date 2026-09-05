//go:build darwin || linux

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestJobLifecycleProjectsBackendReportedModel(t *testing.T) {
	const requestedModel = "caller-requested-model"
	for _, test := range []struct {
		name      string
		requestID string
		reported  string
	}{
		{name: "reported", requestID: "reported", reported: "backend-resolved-model"},
		{name: "not reported", requestID: "not-reported"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &executionFakeBackend{name: "model-" + test.requestID}
			backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
				return &executionFakeSession{
					turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						input.OnProcessStart(engine.ProcessRef{PID: 4190, PGID: 4190, StartTime: "model-report-token"}, 0)
						events := []engine.Event{}
						if test.reported != "" {
							events = append(events, engine.Event{Type: engine.EventModelReported, ModelReported: test.reported})
						}
						events = append(events, engine.Event{Type: engine.EventResultMessage, Text: "finished"})
						return executionEvents(events...), nil
					},
				}, nil
			}
			server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
			params := submissionParams("model-workspace", "model-"+test.requestID, backend.Name(), t.TempDir(), "report the resolved model")
			requested := requestedModel
			params.TaskSpec.Model = &requested
			submitted := submitResultForTest(t, submitForTest(t, server, params))

			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			record, err := store.Get(submitted.JobID)
			if err != nil {
				t.Fatal(err)
			}
			runExecution(t, server, record)

			get := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted.JobID}))
			if get.err != nil {
				t.Fatalf("job.get error = %#v", get.err)
			}
			detail, ok := get.result.(protocol.JobRecordWire)
			if !ok {
				t.Fatalf("job.get result = %T, want protocol.JobRecordWire", get.result)
			}
			if detail.ModelReported != test.reported {
				t.Fatalf("job.get modelReported = %q, want %q", detail.ModelReported, test.reported)
			}

			list := server.handleJobList(mustJSON(t, protocol.JobListParams{}))
			if list.err != nil {
				t.Fatalf("job.list error = %#v", list.err)
			}
			jobs := list.result.(protocol.JobListResult).Jobs
			if len(jobs) != 1 || jobs[0].JobID != submitted.JobID || jobs[0].ModelReported != test.reported {
				t.Fatalf("job.list jobs = %#v, want %q with modelReported %q", jobs, submitted.JobID, test.reported)
			}
		})
	}

	t.Run("running", func(t *testing.T) {
		const reportedModel = "backend-resolved-model"
		events := make(chan engine.Event)
		modelSent := make(chan struct{})
		releaseEvents := make(chan struct{})
		released := false
		release := func() {
			if !released {
				close(releaseEvents)
				released = true
			}
		}
		t.Cleanup(release)

		backend := &executionFakeBackend{name: "model-running"}
		backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
			return &executionFakeSession{
				turn: func(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
					go func() {
						events <- engine.Event{Type: engine.EventModelReported, ModelReported: reportedModel}
						close(modelSent)
						<-releaseEvents
						events <- engine.Event{Type: engine.EventResultMessage, Text: "finished"}
						close(events)
					}()
					return events, nil
				},
			}, nil
		}
		server := newExecutionServer(t, backend)
		record := queuedExecutionRecord(t, server, backend.Name(), "report the resolved model", nil)
		run := newActiveExecution(record.JobID, backend)
		server.executionMu.Lock()
		server.executions = map[string]*activeExecution{record.JobID: run}
		server.executionMu.Unlock()
		done := make(chan struct{})
		go func() {
			server.runJob(context.Background(), record, run)
			close(done)
		}()

		select {
		case <-modelSent:
		case <-time.After(time.Second):
			t.Fatal("model-report event did not arrive")
		}

		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			get := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: record.JobID}))
			if get.err != nil {
				t.Fatalf("running job.get error = %#v", get.err)
			}
			detail, ok := get.result.(protocol.JobRecordWire)
			if !ok {
				t.Fatalf("running job.get result = %T, want protocol.JobRecordWire", get.result)
			}
			list := server.handleJobList(mustJSON(t, protocol.JobListParams{}))
			if list.err != nil {
				t.Fatalf("running job.list error = %#v", list.err)
			}
			jobs := list.result.(protocol.JobListResult).Jobs
			if detail.ModelReported == reportedModel && len(jobs) == 1 && jobs[0].JobID == record.JobID && jobs[0].ModelReported == reportedModel &&
				jobs[0].ItemCount != nil && *jobs[0].ItemCount == 0 && jobs[0].LastItemAt == nil && jobs[0].LastActivityAt != nil {
				break
			}
			select {
			case <-deadline.C:
				t.Fatalf("running model projection: job.get=%q job.list=%#v, want model %q with activity but no item", detail.ModelReported, jobs, reportedModel)
			case <-ticker.C:
			}
		}

		release()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("running model turn did not finish")
		}
	})
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
	lastItemAt := time.Now().UTC().Add(-time.Second).Round(0)
	run.noteTranscriptItem(lastItemAt)
	newItemAssembler(run, nil).absorb(engine.Event{Type: engine.EventProgress}, "")
	activity := run.itemActivity()
	if activity.ItemCount != 1 || !activity.LastItemAt.Equal(lastItemAt) || !activity.LastActivityAt.After(lastItemAt) {
		t.Fatalf("contentless progress activity = %#v, want unchanged item timestamp and later activity timestamp", activity)
	}
	lastActivityAt := activity.LastActivityAt
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
			summary.LastActivityAt == nil || !summary.LastActivityAt.Equal(lastActivityAt) ||
			summary.LastItemAt.Equal(*summary.LastActivityAt) ||
			summary.Liveness != test.liveness {
			t.Fatalf("%s active summary = %#v", test.name, summary)
		}
		encoded, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		var wireSummary struct {
			LastItemAt     *time.Time `json:"lastItemAt"`
			LastActivityAt *time.Time `json:"lastActivityAt"`
		}
		if err := json.Unmarshal(encoded, &wireSummary); err != nil {
			t.Fatal(err)
		}
		if wireSummary.LastItemAt == nil || !wireSummary.LastItemAt.Equal(lastItemAt) ||
			wireSummary.LastActivityAt == nil || !wireSummary.LastActivityAt.Equal(lastActivityAt) ||
			wireSummary.LastItemAt.Equal(*wireSummary.LastActivityAt) {
			t.Fatalf("%s summary JSON activity = %s, want distinct item and activity timestamps", test.name, encoded)
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
	if summary.ItemCount != nil || summary.LastItemAt != nil || summary.LastActivityAt != nil || summary.Liveness != "" {
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
	if stored.State != protocol.PublicStateCanceled || stored.Cleanup != protocol.CleanupClean || stored.BackendSessionID != "" {
		t.Fatalf("record after cancel-before-spawn = %#v", stored)
	}
}

func TestJobCancelBetweenProcessStartAndClaimPersistenceKeepsClaim(t *testing.T) {
	backend := &executionFakeBackend{name: "cancel-between-claim-phases"}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "cancel between process start and claim persistence", nil)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatal(err)
	}

	ref := engine.ProcessRef{PID: 4117, PGID: 4117, StartTime: "claim-persistence-window-token"}
	observedLivePGID := make(chan int)
	server.processGroupGoneFn = func(pgid int) (bool, error) {
		observedLivePGID <- pgid
		return true, nil
	}
	run := newActiveExecution(record.JobID, backend)
	run.setSession(&executionFakeSession{})
	run.beginTurn()
	// This is the reachable state after OnProcessStart notes the identity and
	// releases the launch gate, but before its separate claim transaction.
	run.noteProcessClaim(ref)
	t.Cleanup(func() {
		run.retireTurn(store, turnOutcome{err: context.Canceled, cleanup: protocol.CleanupClean})
	})
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()

	beforePersistence, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if beforePersistence.ProcessClaim != nil {
		t.Fatalf("record before claim persistence = %#v, want no durable claim", beforePersistence)
	}

	cancelDone := make(chan requestOutcome, 1)
	go func() {
		cancelDone <- server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
	}()
	select {
	case pgid := <-observedLivePGID:
		if pgid != ref.PGID {
			t.Fatalf("job.cancel observed process group %d, want live claim group %d", pgid, ref.PGID)
		}
	case <-time.After(time.Second):
		t.Fatal("job.cancel did not use the live in-memory process claim")
	}

	// The turn owner persists after containment has selected the live claim,
	// before it retires and lets cancellation commit the terminal record.
	run.persistProcessClaim(store)
	run.retireTurn(store, turnOutcome{err: context.Canceled, cleanup: protocol.CleanupClean})

	select {
	case outcome := <-cancelDone:
		if outcome.err != nil {
			t.Fatalf("job.cancel error = %#v", outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("job.cancel did not return after the active turn retired")
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateCanceled || stored.ProcessClaim == nil || stored.ProcessClaim.PID != ref.PID || stored.ProcessClaim.PGID != ref.PGID || stored.ProcessClaim.StartToken != ref.StartTime {
		t.Fatalf("terminal record after the claim-persistence window = %#v, want canceled record retaining %+v", stored, ref)
	}
}

func TestJobCancelDuringCorrectionPreservesRetiredTurnOutcome(t *testing.T) {
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	correctionEvents := make(chan engine.Event)
	correctionStarted := make(chan struct{})
	turns := 0
	backend := &executionFakeBackend{name: "cancel-during-correction"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				switch turns {
				case 1:
					input.OnProcessStart(engine.ProcessRef{PID: 4113, PGID: 4113, StartTime: "initial-correction-token"}, 0)
					return executionEvents(
						engine.Event{Type: engine.EventResultMessage, Text: `{"wrong":true}`},
						engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{BackendSessionID: "thread-initial", CleanupFailed: true}},
					), nil
				case 2:
					if input.Write {
						t.Fatal("correction turn was write-enabled")
					}
					input.OnProcessStart(engine.ProcessRef{PID: 4114, PGID: 4114, StartTime: "correction-cancel-token"}, 0)
					close(correctionStarted)
					return correctionEvents, nil
				default:
					t.Fatalf("turns = %d, want initial and correction turns", turns)
					return nil, nil
				}
			},
			interrupt: func(context.Context) error {
				close(correctionEvents)
				return nil
			},
		}, nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	server.processGroupGoneFn = func(int) (bool, error) { return true, nil }
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "correct after invalid output", nil, schemaRaw)
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(done)
	}()
	select {
	case <-correctionStarted:
	case <-time.After(time.Second):
		t.Fatal("correction turn did not start")
	}

	canceled := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
	if canceled.err != nil {
		t.Fatalf("job.cancel error = %#v", canceled.err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled correction did not retire")
	}

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateCanceled || stored.Cleanup != protocol.CleanupUncertain || stored.BackendSessionID != "thread-initial" {
		t.Fatalf("canceled correction record = %#v, want canceled uncertain record with initial session", stored)
	}
	initialDiagnosticCount := 0
	for _, diagnostic := range stored.Diagnostics {
		if diagnostic == "backend reported uncertain cleanup" {
			initialDiagnosticCount++
		}
	}
	if initialDiagnosticCount != 1 {
		t.Fatalf("canceled correction diagnostics = %#v, want one initial cleanup diagnostic", stored.Diagnostics)
	}
}

func TestJobCancelRetainsCurrentTurnFinalEvidence(t *testing.T) {
	events := make(chan engine.Event, 1)
	turnStarted := make(chan struct{})
	publishedFinal := false
	backend := &executionFakeBackend{name: "cancel-current-turn-evidence"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4115, PGID: 4115, StartTime: "current-turn-cancel-token"}, 0)
				close(turnStarted)
				return events, nil
			},
			interrupt: func(context.Context) error { return nil },
		}, nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	server.processGroupGoneFn = func(int) (bool, error) {
		if !publishedFinal {
			publishedFinal = true
			events <- engine.Event{
				Type: engine.EventTurnFinal,
				TurnFinal: &engine.TurnFinalObservation{
					BackendSessionID: "thread-current",
					CleanupFailed:    true,
				},
			}
			close(events)
		}
		return true, nil
	}
	record := queuedExecutionRecord(t, server, backend.Name(), "cancel after turn launch", nil)
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(done)
	}()
	select {
	case <-turnStarted:
	case <-time.After(time.Second):
		t.Fatal("turn did not start")
	}

	canceled := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
	if canceled.err != nil {
		t.Fatalf("job.cancel error = %#v", canceled.err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled turn did not retire")
	}

	get := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: record.JobID}))
	if get.err != nil {
		t.Fatalf("job.get error = %#v", get.err)
	}
	detail, ok := get.result.(protocol.JobRecordWire)
	if !ok {
		t.Fatalf("job.get result = %T, want protocol.JobRecordWire", get.result)
	}
	if detail.State != protocol.PublicStateCanceled || detail.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("job.get canceled record = %#v, want canceled with uncertain cleanup", detail)
	}

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateCanceled || stored.Cleanup != protocol.CleanupUncertain || stored.BackendSessionID != "thread-current" {
		t.Fatalf("durable canceled record = %#v, want canceled uncertain record with current session", stored)
	}
	for _, diagnostic := range stored.Diagnostics {
		if diagnostic == "backend reported uncertain cleanup" {
			return
		}
	}
	t.Fatalf("durable canceled diagnostics = %#v, want backend cleanup uncertainty", stored.Diagnostics)
}

func TestJobCancelReturnsAfterUnretiredTurnStreamGrace(t *testing.T) {
	// This fake event stream intentionally never sends and never closes.
	events := make(chan engine.Event)
	turnStarted := make(chan struct{})
	backend := &executionFakeBackend{name: "cancel-unretired-turn-stream"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4116, PGID: 4116, StartTime: "unretired-turn-cancel-token"}, 0)
				close(turnStarted)
				return events, nil
			},
			interrupt: func(context.Context) error {
				return nil
			},
		}, nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
	server.processGroupGoneFn = func(int) (bool, error) { return true, nil }
	server.turnDrainGrace = 100 * time.Millisecond
	record := queuedExecutionRecord(t, server, backend.Name(), "cancel with an unretired turn", nil)
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	runDone := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(runDone)
	}()
	select {
	case <-turnStarted:
	case <-time.After(time.Second):
		t.Fatal("turn did not start")
	}

	cancelRaw := mustJSON(t, protocol.JobCancelParams{JobID: record.JobID})
	cancelDone := make(chan requestOutcome, 1)
	go func() {
		cancelDone <- server.handleJobCancel(cancelRaw)
	}()
	completionBound := time.NewTimer(2 * server.turnDrainGrace)
	defer completionBound.Stop()
	select {
	case canceled := <-cancelDone:
		if canceled.err != nil {
			t.Fatalf("job.cancel error = %#v", canceled.err)
		}
	case <-completionBound.C:
		t.Fatal("job.cancel did not return after the event-stream containment grace")
	}
	<-runDone

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != protocol.PublicStateCanceled || stored.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("canceled record = %#v, want canceled with uncertain cleanup", stored)
	}
	for _, diagnostic := range stored.Diagnostics {
		if diagnostic == "backend event stream did not close after containment grace" {
			return
		}
	}
	t.Fatalf("canceled diagnostics = %#v, want event-stream cleanup uncertainty", stored.Diagnostics)
}

func TestJobCancelAfterSpawnRecordsCleanupOutcome(t *testing.T) {
	for _, tt := range []struct {
		name        string
		argv        []string
		readyLine   string
		waitForExit bool
		wantCleanup protocol.Cleanup
	}{
		{name: "clean inside grace", argv: []string{"sleep", "30"}, waitForExit: true, wantCleanup: protocol.CleanupClean},
		{name: "uncertain outside grace", argv: []string{"sh", "-c", "trap '' TERM; printf 'term-trap-ready\\n'; while :; do sleep 1; done"}, readyLine: "term-trap-ready\n", wantCleanup: protocol.CleanupUncertain},
	} {
		t.Run(tt.name, func(t *testing.T) {
			interrupted := false
			backend := &executionFakeBackend{name: "cancel-after-spawn-" + tt.name}
			server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
			server.processGroupGrace = 100 * time.Millisecond
			cmd, claim := startProcessGroup(t, tt.readyLine, tt.argv...)
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

func TestRestartDuringCorrectionPreservesRetiredSessionForResume(t *testing.T) {
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
	run := newActiveExecution(submitted.JobID, nil)
	run.beginTurn()
	if _, err := store.RecordProcessClaim(submitted.JobID, jobstore.ProcessClaim{PID: 2147483647, PGID: 2147483647, StartToken: "initial-turn-token"}); err != nil {
		t.Fatal(err)
	}
	if retired := run.retireTurn(store, turnOutcome{backendSessionID: "thread-recovered", cleanup: protocol.CleanupClean}); retired.backendSessionID != "thread-recovered" {
		t.Fatalf("retired turn = %#v, want recorded session", retired)
	}
	// The initial turn has retired and its session write has committed before
	// this second turn starts; restart reconciliation must retain that ID.
	run.beginTurn()
	if _, err := store.RecordProcessClaim(submitted.JobID, jobstore.ProcessClaim{PID: 2147483647, PGID: 2147483647, StartToken: "correction-turn-token"}); err != nil {
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
	if stored.State != protocol.PublicStateUnknown || stored.Cleanup != protocol.CleanupUncertain || stored.BackendSessionID != "thread-recovered" {
		t.Fatalf("reconciled running record = %#v", stored)
	}
	params := submissionParams("restart-resume-workspace", "restart-resume-request", "restart-running", t.TempDir(), "continue")
	params.TaskSpec.ResumeJobID = submitted.JobID
	resumed := submitResultForTest(t, submitForTest(t, restarted, params))
	if resumed.State != protocol.PublicStateQueued {
		t.Fatalf("resume after restart reconciliation = %#v, want queued job", resumed)
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
	wantDiagnostics := []string{
		"restart reconciliation: no relaunch",
		"orphan reaper: leader start token mismatch; no signal sent",
	}
	if !slices.Equal(stored.Diagnostics, wantDiagnostics) {
		t.Fatalf("mismatched-token diagnostics = %#v, want %#v", stored.Diagnostics, wantDiagnostics)
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

func startProcessGroup(t *testing.T, readyLine string, argv ...string) (*exec.Cmd, jobstore.ProcessClaim) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout io.ReadCloser
	if readyLine != "" {
		var err error
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
	}
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
	if readyLine != "" {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		_ = stdout.Close()
		if err != nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			_ = cmd.Wait()
			t.Fatalf("read TERM-ignore readiness: %v", err)
		}
		if line != readyLine {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
			_ = cmd.Wait()
			t.Fatalf("TERM-ignore readiness = %q, want %q", line, readyLine)
		}
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
