//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type providerGate struct {
	mu      sync.RWMutex
	started chan string
	release map[string]chan struct{}
	once    map[string]*sync.Once
}

func newProviderGate() *providerGate {
	return &providerGate{
		started: make(chan string, 32),
		release: make(map[string]chan struct{}),
		once:    make(map[string]*sync.Once),
	}
}

func (gate *providerGate) add(id string) {
	gate.mu.Lock()
	gate.release[id] = make(chan struct{})
	gate.once[id] = &sync.Once{}
	gate.mu.Unlock()
}

func (gate *providerGate) releaseOne(id string) {
	gate.mu.RLock()
	ch, once := gate.release[id], gate.once[id]
	gate.mu.RUnlock()
	if ch != nil && once != nil {
		once.Do(func() { close(ch) })
	}
}

func (gate *providerGate) releaseAll() {
	gate.mu.RLock()
	ids := make([]string, 0, len(gate.release))
	for id := range gate.release {
		ids = append(ids, id)
	}
	gate.mu.RUnlock()
	for _, id := range ids {
		gate.releaseOne(id)
	}
}

func (gate *providerGate) backend(name string) *executionFakeBackend {
	return &executionFakeBackend{
		name: name,
		start: func(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
			gate.mu.RLock()
			release := gate.release[opts.Model]
			gate.mu.RUnlock()
			if release == nil {
				return nil, errors.New("missing provider gate")
			}
			gate.started <- opts.Model
			select {
			case <-release:
				return claimedResultSession(opts.Model), nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
}

func concurrencyTestServer(t *testing.T, maxJobs, maxPerBackend, maxQueued int, backends ...engine.Backend) *Server {
	t.Helper()
	server := newTestServer(t, t.TempDir(), Config{
		Backends:          backends,
		MaxJobs:           maxJobs,
		MaxJobsPerBackend: maxPerBackend,
		MaxQueued:         maxQueued,
	})
	ctx, cancel := context.WithCancel(context.Background())
	server.beginExecutions(ctx)
	t.Cleanup(func() {
		cancel()
		server.stopExecutions()
	})
	return server
}

func submitConcurrencyJob(t *testing.T, server *Server, backend, id string) protocol.JobSubmitResult {
	t.Helper()
	params := submissionParams("concurrency-workspace", id, backend, t.TempDir(), id)
	params.TaskSpec.Model = &id
	return submitResultForTest(t, submitForTest(t, server, params))
}

func receiveProviderStart(t *testing.T, gate *providerGate) string {
	t.Helper()
	select {
	case id := <-gate.started:
		return id
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fake provider start")
		return ""
	}
}

func mustGetStoredJob(t *testing.T, server *Server, jobID string) jobstore.Record {
	t.Helper()
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(jobID)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestBackendQueueStartsInFIFOOrderWhenSlotsFree(t *testing.T) {
	gate := newProviderGate()
	backend := gate.backend("alpha")
	server := concurrencyTestServer(t, 4, 2, 8, backend)
	t.Cleanup(gate.releaseAll)
	submitted := make(map[string]protocol.JobSubmitResult)
	for _, id := range []string{"a1", "a2", "a3", "a4"} {
		gate.add(id)
		submitted[id] = submitConcurrencyJob(t, server, "alpha", id)
	}
	first := receiveProviderStart(t, gate)
	second := receiveProviderStart(t, gate)
	if first == second || (first != "a1" && first != "a2") || (second != "a1" && second != "a2") {
		t.Fatalf("initial provider starts = %q, %q; want a1 and a2", first, second)
	}
	if got := mustGetStoredJob(t, server, submitted["a3"].JobID).State; got != protocol.PublicStateQueued {
		t.Fatalf("third same-backend job state = %q, want queued", got)
	}

	gate.releaseOne(first)
	if got := receiveProviderStart(t, gate); got != "a3" {
		t.Fatalf("first freed slot started %q, want FIFO job a3", got)
	}
	gate.releaseOne(second)
	if got := receiveProviderStart(t, gate); got != "a4" {
		t.Fatalf("second freed slot started %q, want FIFO job a4", got)
	}
	gate.releaseAll()
	server.executionWG.Wait()
}

func TestSaturatedBackendDoesNotBlockOtherBackend(t *testing.T) {
	gate := newProviderGate()
	alpha, beta := gate.backend("alpha"), gate.backend("beta")
	server := concurrencyTestServer(t, 3, 2, 2, alpha, beta)
	t.Cleanup(gate.releaseAll)
	submitted := make(map[string]protocol.JobSubmitResult)
	for _, id := range []string{"a1", "a2", "a3", "b1", "b2"} {
		gate.add(id)
	}
	submitted["a1"] = submitConcurrencyJob(t, server, "alpha", "a1")
	submitted["a2"] = submitConcurrencyJob(t, server, "alpha", "a2")
	submitted["a3"] = submitConcurrencyJob(t, server, "alpha", "a3")
	submitted["b1"] = submitConcurrencyJob(t, server, "beta", "b1")
	starts := map[string]bool{
		receiveProviderStart(t, gate): true,
		receiveProviderStart(t, gate): true,
		receiveProviderStart(t, gate): true,
	}
	if !starts["a1"] || !starts["a2"] || !starts["b1"] || starts["a3"] {
		t.Fatalf("provider starts = %#v; want a1, a2, and b1 while a3 remains queued", starts)
	}
	if got := mustGetStoredJob(t, server, submitted["a3"].JobID).State; got != protocol.PublicStateQueued {
		t.Fatalf("blocked alpha job state = %q, want queued", got)
	}
	submitted["b2"] = submitConcurrencyJob(t, server, "beta", "b2")
	if got := mustGetStoredJob(t, server, submitted["b2"].JobID).State; got != protocol.PublicStateQueued {
		t.Fatalf("job beyond global limit state = %q, want queued", got)
	}

	gate.releaseOne("a1")
	if got := receiveProviderStart(t, gate); got != "a3" {
		t.Fatalf("freed alpha slot started %q, want queued alpha job a3", got)
	}
	if got := mustGetStoredJob(t, server, submitted["b2"].JobID).State; got != protocol.PublicStateQueued {
		t.Fatalf("job beyond global limit state after alpha dispatch = %q, want queued", got)
	}
	gate.releaseOne("b1")
	if got := receiveProviderStart(t, gate); got != "b2" {
		t.Fatalf("freed global slot started %q, want queued beta job b2", got)
	}
}

func TestQueueBoundReturnsTypedErrorAndDeduplicatedRetryAddsNoEntry(t *testing.T) {
	gate := newProviderGate()
	backend := gate.backend("alpha")
	server := concurrencyTestServer(t, 1, 1, 1, backend)
	t.Cleanup(gate.releaseAll)
	for _, id := range []string{"a1", "a2", "a3"} {
		gate.add(id)
	}
	first := submitConcurrencyJob(t, server, "alpha", "a1")
	if got := receiveProviderStart(t, gate); got != "a1" {
		t.Fatalf("provider start = %q, want a1", got)
	}
	params := submissionParams("concurrency-workspace", "a2", "alpha", t.TempDir(), "a2")
	params.TaskSpec.Model = ptrString("a2")
	waiting := submitResultForTest(t, submitForTest(t, server, params))
	replayed := submitForTest(t, server, params)
	if replayed.err != nil {
		t.Fatalf("deduplicated retry with full queue error = %#v", replayed.err)
	}
	if result := submitResultForTest(t, replayed); !result.Deduplicated || result.JobID != mustGetStoredJob(t, server, waiting.JobID).JobID {
		t.Fatalf("deduplicated result = %+v, want existing a2 job", result)
	}
	if got := mustGetStoredJob(t, server, first.JobID).State; got != protocol.PublicStateRunning {
		t.Fatalf("active first job state = %q, want running", got)
	}

	third := submissionParams("concurrency-workspace", "a3", "alpha", t.TempDir(), "a3")
	third.TaskSpec.Model = ptrString("a3")
	outcome := submitForTest(t, server, third)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorQueueFull {
		t.Fatalf("submission beyond queue bound error = %#v, want queue_full", outcome.err)
	}
	if got := mustGetStoredJob(t, server, waiting.JobID).State; got != protocol.PublicStateQueued {
		t.Fatalf("waiting job state = %q, want queued", got)
	}
}

func TestQueuedCancelAndControlReadsRespondAtCapacity(t *testing.T) {
	gate := newProviderGate()
	backend := gate.backend("alpha")
	server := concurrencyTestServer(t, 2, 2, 4, backend)
	t.Cleanup(gate.releaseAll)
	for _, id := range []string{"a1", "a2", "a3"} {
		gate.add(id)
	}
	submitted := map[string]protocol.JobSubmitResult{
		"a1": submitConcurrencyJob(t, server, "alpha", "a1"),
		"a2": submitConcurrencyJob(t, server, "alpha", "a2"),
	}
	started := map[string]bool{receiveProviderStart(t, gate): true, receiveProviderStart(t, gate): true}
	if !started["a1"] || !started["a2"] {
		t.Fatalf("active starts = %#v, want a1 and a2", started)
	}
	for _, id := range []string{"a1", "a2"} {
		outcome := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted[id].JobID}))
		if outcome.err != nil {
			t.Fatalf("running job.get error = %#v", outcome.err)
		}
		record := outcome.result.(protocol.JobRecordWire)
		if record.State != protocol.PublicStateRunning || record.StartedAt == nil {
			t.Fatalf("running job projection = %+v, want running with startedAt", record)
		}
	}
	queued := submitConcurrencyJob(t, server, "alpha", "a3")
	getRaw := mustJSON(t, protocol.JobGetParams{JobID: queued.JobID})
	getDone := make(chan requestOutcome, 1)
	go func() { getDone <- server.handleJobGet(getRaw) }()
	select {
	case outcome := <-getDone:
		if outcome.err != nil {
			t.Fatalf("queued job.get error = %#v", outcome.err)
		}
		record := outcome.result.(protocol.JobRecordWire)
		if record.State != protocol.PublicStateQueued || record.QueuedAt.IsZero() || record.StartedAt != nil {
			t.Fatalf("queued job projection = %+v, want queuedAt and no startedAt", record)
		}
	case <-time.After(time.Second):
		t.Fatal("job.get waited for an execution slot")
	}
	listOutcome := server.handleJobList(mustJSON(t, protocol.JobListParams{States: []protocol.PublicState{protocol.PublicStateQueued}}))
	if listOutcome.err != nil {
		t.Fatalf("queued job.list error = %#v", listOutcome.err)
	}
	queuedSummaryFound := false
	for _, summary := range listOutcome.result.(protocol.JobListResult).Jobs {
		if summary.JobID == queued.JobID {
			queuedSummaryFound = summary.State == protocol.PublicStateQueued && !summary.QueuedAt.IsZero()
		}
	}
	if !queuedSummaryFound {
		t.Fatal("job.list did not report the waiting job with queuedAt")
	}
	transcriptRaw := mustJSON(t, protocol.JobTranscriptParams{JobID: queued.JobID})
	transcriptDone := make(chan requestOutcome, 1)
	go func() { transcriptDone <- server.handleJobTranscript(transcriptRaw) }()
	select {
	case outcome := <-transcriptDone:
		if outcome.err != nil {
			t.Fatalf("queued job.transcript error = %#v", outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("job.transcript waited for an execution slot")
	}

	cancelRaw := mustJSON(t, protocol.JobCancelParams{JobID: queued.JobID})
	cancelDone := make(chan requestOutcome, 1)
	go func() { cancelDone <- server.handleJobCancel(cancelRaw) }()
	select {
	case outcome := <-cancelDone:
		if outcome.err != nil {
			t.Fatalf("queued job.cancel error = %#v", outcome.err)
		}
		if result := outcome.result.(protocol.JobCancelResult); result.State != protocol.PublicStateCanceled {
			t.Fatalf("queued cancel result = %+v, want canceled", result)
		}
	case <-time.After(time.Second):
		t.Fatal("job.cancel waited for an execution slot")
	}

	gate.releaseAll()
	server.executionWG.Wait()
	select {
	case got := <-gate.started:
		t.Fatalf("unexpected provider start %q after releasing the occupied slots", got)
	default:
	}
	if got := mustGetStoredJob(t, server, queued.JobID).State; got != protocol.PublicStateCanceled {
		t.Fatalf("canceled queued record state = %q, want canceled", got)
	}
}

func TestJobTimeoutDeadlineStartsAtSlotReservation(t *testing.T) {
	timeoutMS := int64(60_000)
	var gotDeadline time.Time
	backend := &executionFakeBackend{name: "timeout-slot"}
	backend.start = func(ctx context.Context, _ engine.SessionOpts) (engine.Session, error) {
		gotDeadline, _ = ctx.Deadline()
		return claimedResultSession("timeout-slot"), nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "timeout slot", &timeoutMS)
	slotAcquiredAt := record.CreatedAt.Add(15 * time.Minute)
	run := newActiveExecution(record.JobID, backend)
	run.slotAcquiredAt = slotAcquiredAt
	server.runJob(context.Background(), record, run)
	wantDeadline := slotAcquiredAt.Add(time.Duration(timeoutMS) * time.Millisecond)
	if !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("provider deadline = %s, want slot time plus timeout %s", gotDeadline, wantDeadline)
	}
}

func ptrString(value string) *string { return &value }
