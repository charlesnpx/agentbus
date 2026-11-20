//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/claudecli"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
)

type executionFakeBackend struct {
	name   string
	start  func(context.Context, engine.SessionOpts) (engine.Session, error)
	resume func(context.Context, string, engine.SessionOpts) (engine.Session, error)
}

func (backend *executionFakeBackend) Name() string { return backend.name }

func (backend *executionFakeBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: backend.name}, nil
}

func (backend *executionFakeBackend) Start(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
	if backend.start == nil {
		return nil, errors.New("fake backend Start was not configured")
	}
	return backend.start(ctx, opts)
}

func (backend *executionFakeBackend) Resume(ctx context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	if backend.resume == nil {
		return nil, errors.New("fake backend Resume was not configured")
	}
	return backend.resume(ctx, id, opts)
}

type executionFakeSession struct {
	turn      func(context.Context, engine.TurnInput) (<-chan engine.Event, error)
	interrupt func(context.Context) error
}

func (*executionFakeSession) ID() string { return "execution-fake-session" }

func (session *executionFakeSession) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	if session.turn == nil {
		return nil, errors.New("fake session Turn was not configured")
	}
	return session.turn(ctx, input)
}

func (session *executionFakeSession) Interrupt(ctx context.Context) error {
	if session.interrupt == nil {
		return nil
	}
	return session.interrupt(ctx)
}

func executionEvents(events ...engine.Event) <-chan engine.Event {
	stream := make(chan engine.Event, len(events))
	for _, event := range events {
		stream <- event
	}
	close(stream)
	return stream
}

func newExecutionServer(t *testing.T, backend engine.Backend) *Server {
	t.Helper()
	return newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
}

func queuedExecutionRecord(t *testing.T, server *Server, backend, prompt string, timeoutMS *int64) jobstore.Record {
	t.Helper()
	return queuedExecutionRecordWithSchema(t, server, backend, prompt, timeoutMS, nil)
}

func queuedExecutionRecordWithSchema(t *testing.T, server *Server, backend, prompt string, timeoutMS *int64, outputSchema json.RawMessage) jobstore.Record {
	return queuedExecutionRecordWithInputMode(t, server, backend, prompt, timeoutMS, "", outputSchema)
}

func queuedExecutionRecordWithInputMode(t *testing.T, server *Server, backend, prompt string, timeoutMS *int64, inputMode protocol.InputMode, outputSchema json.RawMessage) jobstore.Record {
	t.Helper()
	spec := protocol.TaskSpec{
		Backend:      backend,
		CWD:          t.TempDir(),
		Prompt:       prompt,
		Intent:       engine.IntentInspect,
		InputMode:    inputMode,
		TimeoutMS:    timeoutMS,
		OutputSchema: append(json.RawMessage(nil), outputSchema...),
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: "execution-workspace", RequestID: "execution-request"},
		raw,
		func(id string) (jobstore.Record, error) {
			record := jobstore.Record{JobID: id, Backend: backend, CWD: spec.CWD, Intent: spec.Intent}
			if spec.InputMode != "" {
				record.InputMode = spec.InputMode
			}
			return record, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("new execution record was unexpectedly deduplicated")
	}
	return record
}

func runExecution(t *testing.T, server *Server, record jobstore.Record) {
	t.Helper()
	backend := server.backends[record.Backend]
	if backend == nil {
		t.Fatalf("configured backend %q is missing", record.Backend)
	}
	run := newActiveExecution(record.JobID, backend)
	server.runJob(context.Background(), record, run)
}

type deadlineWarningTestSession struct {
	events <-chan engine.Event
	steers chan engine.SteerMessage
	calls  atomic.Int64
}

func (*deadlineWarningTestSession) ID() string { return "deadline-warning-test-session" }

func (session *deadlineWarningTestSession) Turn(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	input.OnProcessStart(engine.ProcessRef{PID: 4201, PGID: 4201, StartTime: "deadline-warning-token"}, 0)
	return session.events, nil
}

func (*deadlineWarningTestSession) Interrupt(context.Context) error { return nil }

func (session *deadlineWarningTestSession) Steer(ctx context.Context, message engine.SteerMessage) (engine.SteerDelivery, error) {
	session.calls.Add(1)
	select {
	case session.steers <- message:
		return engine.SteerDelivery{Mechanism: engine.SteerMechanismNative, Confirmed: true}, nil
	case <-ctx.Done():
		return engine.SteerDelivery{}, ctx.Err()
	}
}

func TestExecutionWarnsSteerableJobAtDeadlineThreshold(t *testing.T) {
	oldWait := waitForDeadlineWarning
	armed := make(chan time.Duration, 1)
	fire := make(chan struct{})
	waitForDeadlineWarning = func(ctx context.Context, delay time.Duration) bool {
		armed <- delay
		select {
		case <-fire:
			return true
		case <-ctx.Done():
			return false
		}
	}
	t.Cleanup(func() { waitForDeadlineWarning = oldWait })

	events := make(chan engine.Event, 1)
	session := &deadlineWarningTestSession{events: events, steers: make(chan engine.SteerMessage, 2)}
	backend := &executionFakeBackend{name: "warning-fake"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) { return session, nil }
	server := newExecutionServer(t, backend)
	timeoutMS := int64(10_000)
	record := queuedExecutionRecordWithInputMode(t, server, backend.Name(), "finish", &timeoutMS, protocol.InputModeSteerable, nil)
	run := newActiveExecution(record.JobID, backend)
	run.slotAcquiredAt = time.Now().UTC()
	server.executionMu.Lock()
	if server.executions == nil {
		server.executions = make(map[string]*activeExecution)
	}
	server.executions[record.JobID] = run
	server.executionMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(done)
	}()

	select {
	case delay := <-armed:
		warningAt := run.slotAcquiredAt.Add(time.Duration(timeoutMS) * time.Millisecond * 9 / 10)
		wantDelay := warningAt.Sub(time.Now())
		if delta := delay - wantDelay; delay <= 0 || delta > time.Second || delta < -time.Second {
			t.Fatalf("warning delay = %s, want about %s from slot acquisition", delay, wantDelay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deadline warning timer did not arm")
	}
	close(fire)
	var warning engine.SteerMessage
	select {
	case warning = <-session.steers:
	case <-time.After(5 * time.Second):
		t.Fatal("deadline warning was not delivered to the session")
	}
	if warning.ID != deadlineWarningMessageID {
		t.Fatalf("warning ID = %q, want %q", warning.ID, deadlineWarningMessageID)
	}
	const wantPrefix = "agentbus: "
	if !strings.HasPrefix(warning.Text, wantPrefix) {
		t.Fatalf("warning text = %q, want prefix %q", warning.Text, wantPrefix)
	}
	events <- engine.Event{Type: engine.EventResultMessage, Text: "done"}
	close(events)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not finish after the warning")
	}
	if got := session.calls.Load(); got != 1 {
		t.Fatalf("session steer calls = %d, want exactly one", got)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Steers) != 1 || stored.Steers[0].MessageID != deadlineWarningMessageID || stored.Steers[0].Status != protocol.SteerStatusDelivered {
		t.Fatalf("stored warning steer = %#v, want one delivered reserved-ID record", stored.Steers)
	}
}

func TestExecutionWarnsFixedModeAfterLateInitialization(t *testing.T) {
	oldWait := waitForDeadlineWarning
	armed := make(chan time.Duration, 1)
	waitForDeadlineWarning = func(_ context.Context, delay time.Duration) bool { armed <- delay; return true }
	t.Cleanup(func() { waitForDeadlineWarning = oldWait })
	session := &testSteerSession{entered: make(chan engine.SteerMessage, 1), delivery: engine.SteerDelivery{Mechanism: engine.SteerMechanismNative, Confirmed: true}}
	server, store, run, jobID := newRunningSteerTestJob(t, protocol.InputModeFixed, session, true)
	server.startDeadlineWarning(jobID, run, session, context.Background(), time.Now().Add(-9500*time.Millisecond), 10*time.Second, protocol.InputModeFixed)
	select {
	case delay := <-armed:
		if delay != 0 {
			t.Fatalf("late warning delay = %s", delay)
		}
	case <-time.After(time.Second):
		t.Fatal("warning not armed")
	}
	select {
	case message := <-session.entered:
		if message.ID != deadlineWarningMessageID {
			t.Fatalf("warning = %#v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("fixed job did not receive deadline notice")
	}
	run.retireTurn(store, turnOutcome{cleanup: protocol.CleanupClean})
}

func claimedResultSession(result string) *executionFakeSession {
	return &executionFakeSession{
		turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4101, PGID: 4101, StartTime: "process-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: result}), nil
		},
	}
}

func TestExecutionNoSchemaDoesNotEvaluateOrCorrect(t *testing.T) {
	turns := 0
	backend := &executionFakeBackend{name: "fake"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				if turns != 1 {
					t.Fatalf("turns = %d, want no correction without a schema", turns)
				}
				input.OnProcessStart(engine.ProcessRef{PID: 4100, PGID: 4100, StartTime: "completion-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventAgentText, Text: "non-authoritative"},
					engine.Event{Type: engine.EventResultMessage, Text: "authoritative final text"},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "finish", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != "authoritative final text" {
		t.Fatalf("terminal record = %+v, want completed authoritative result", got)
	}
	if got.Contract.Evaluated || got.Contract.Attempts != 0 {
		t.Fatalf("contract without schema = %+v, want unevaluated with no correction", got.Contract)
	}
	if turns != 1 {
		t.Fatalf("turn count = %d, want 1 without a schema", turns)
	}
	spilled, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	if string(spilled) != got.ResultText {
		t.Fatalf("spilled result = %q, want %q", spilled, got.ResultText)
	}
}

func TestExecutionCompliantSchemaRecordsContract(t *testing.T) {
	const result = `{"ok":true}`
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	turns := 0
	backend := &executionFakeBackend{name: "compliant-schema"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				if turns != 1 {
					t.Fatalf("turns = %d, want no correction for a compliant result", turns)
				}
				input.OnProcessStart(engine.ProcessRef{PID: 4106, PGID: 4106, StartTime: "compliant-token"}, 0)
				return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: result}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "compliant", nil, schemaRaw)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := schema.Digest(schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != result {
		t.Fatalf("terminal record = %+v, want completed compliant result", got)
	}
	if got.Contract.SchemaSHA256 != digest || !got.Contract.Evaluated || !got.Contract.Compliant || got.Contract.Attempts != 1 || len(got.Contract.Violations) != 0 {
		t.Fatalf("contract = %+v, want evaluated compliant one-attempt contract", got.Contract)
	}
	if turns != 1 {
		t.Fatalf("turn count = %d, want 1", turns)
	}
}

func TestExecutionNoncompliantSchemaUsesOneSuccessfulCorrection(t *testing.T) {
	const initialResult = `{"wrong":true}`
	const correctedResult = `{"ok":true}`
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	initialValidation, err := schema.Validate(initialResult, schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	var server *Server
	var record jobstore.Record
	turns := 0
	var correctionPrompt string
	backend := &executionFakeBackend{name: "successful-correction"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				switch turns {
				case 1:
					input.OnProcessStart(engine.ProcessRef{PID: 4107, PGID: 4107, StartTime: "initial-token"}, 0)
					return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: initialResult}), nil
				case 2:
					if input.Intent != engine.IntentInspect {
						t.Fatal("correction turn was write-enabled")
					}
					correctionPrompt = input.Prompt
					input.OnProcessStart(engine.ProcessRef{PID: 4108, PGID: 4108, StartTime: "correction-token"}, 0)
					return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: correctedResult}), nil
				default:
					t.Fatalf("turns = %d, want exactly one correction", turns)
					return nil, nil
				}
			},
		}, nil
	}
	server = newExecutionServer(t, backend)
	record = queuedExecutionRecordWithSchema(t, server, backend.Name(), "correct", nil, schemaRaw)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := schema.Digest(schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	if correctionPrompt != schema.CorrectionPrompt(string(schemaRaw), initialValidation.Violations) {
		t.Fatalf("correction prompt = %q, want fixed schema correction prompt", correctionPrompt)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != correctedResult {
		t.Fatalf("terminal record = %+v, want corrected authoritative result", got)
	}
	if got.Contract.SchemaSHA256 != digest || !got.Contract.Evaluated || !got.Contract.Compliant || got.Contract.Attempts != 2 || len(got.Contract.Violations) != 0 {
		t.Fatalf("contract = %+v, want evaluated compliant two-attempt contract", got.Contract)
	}
	if got.ProcessClaim == nil || got.ProcessClaim.StartToken != "correction-token" {
		t.Fatalf("terminal process claim = %+v, want correction claim", got.ProcessClaim)
	}
	if turns != 2 {
		t.Fatalf("turn count = %d, want 2", turns)
	}
}

func TestUnrecordedCorrectionCannotReuseEarlierClaimForCleanup(t *testing.T) {
	turns := 0
	backend := &executionFakeBackend{name: "unrecorded-correction"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			turns++
			if turns == 1 {
				input.OnProcessStart(engine.ProcessRef{PID: 4107, PGID: 4107, StartTime: "retired-token"}, 0)
				return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: `{"wrong":true}`}), nil
			}
			return executionEvents(engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{CleanupFailed: true}}), nil
		}}, nil
	}
	server := newExecutionServer(t, backend)
	server.processGroupGoneFn = func(int) (bool, error) { return true, nil }
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "task", nil, json.RawMessage(`{"required":["ok"]}`))
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(record.JobID)
	if err != nil || turns != 2 || before.State != protocol.PublicStateCompleted || before.Cleanup != protocol.CleanupUncertain || before.ResultText != `{"wrong":true}` {
		t.Fatalf("turns=%d record=%#v error=%v", turns, before, err)
	}
	if err := server.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	after, err := store.Get(record.JobID)
	if err != nil || after.Cleanup != protocol.CleanupUncertain || after.ResultText != before.ResultText {
		t.Fatalf("unrecorded correction cleanup after reconciliation = %#v, error %v", after, err)
	}
}

func TestMultipleReportedProcessesCannotBeClearedByOneGroupObservation(t *testing.T) {
	backend := &executionFakeBackend{name: "multiple-process-claims"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4107, PGID: 4107, StartTime: "first-token"}, 0)
			input.OnProcessStart(engine.ProcessRef{PID: 4108, PGID: 4108, StartTime: "second-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: "answer"}), nil
		}}, nil
	}
	server := newExecutionServer(t, backend)
	server.processGroupGoneFn = func(int) (bool, error) { return true, nil }
	record := queuedExecutionRecord(t, server, backend.Name(), "task", nil)
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(record.JobID)
	if err != nil || !before.State.IsTerminal() || before.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("multiple-claim terminal = %#v, error %v", before, err)
	}
	if err := server.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	after, err := store.Get(record.JobID)
	if err != nil || after.Cleanup != protocol.CleanupUncertain || after.State != before.State || after.ResultText != before.ResultText {
		t.Fatalf("multiple-claim reconciliation = %#v, error %v", after, err)
	}
}

func TestExecutionFailedCorrectionPreservesOriginalNoncompliantResult(t *testing.T) {
	const initialResult = `{"wrong":true}`
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	initialValidation, err := schema.Validate(initialResult, schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	turns := 0
	backend := &executionFakeBackend{name: "failed-correction"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				switch turns {
				case 1:
					input.OnProcessStart(engine.ProcessRef{PID: 4109, PGID: 4109, StartTime: "failed-initial-token"}, 0)
					return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: initialResult}), nil
				case 2:
					if input.Intent != engine.IntentInspect {
						t.Fatal("correction turn was write-enabled")
					}
					input.OnProcessStart(engine.ProcessRef{PID: 4110, PGID: 4110, StartTime: "failed-correction-token"}, 0)
					return executionEvents(engine.Event{Type: engine.EventTerminalError, Err: errors.New("correction backend failed")}), nil
				default:
					t.Fatalf("turns = %d, want exactly one correction", turns)
					return nil, nil
				}
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "failed correction", nil, schemaRaw)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := schema.Digest(schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != initialResult {
		t.Fatalf("terminal record = %+v, want original completed result", got)
	}
	if got.Contract.SchemaSHA256 != digest || !got.Contract.Evaluated || got.Contract.Compliant || got.Contract.Attempts != 2 || strings.Join(got.Contract.Violations, "\n") != strings.Join(initialValidation.Violations, "\n") {
		t.Fatalf("contract = %+v, want original noncompliant two-attempt contract", got.Contract)
	}
	spilled, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	if string(spilled) != initialResult {
		t.Fatalf("spilled result = %q, want preserved original result %q", spilled, initialResult)
	}
	if got.ProcessClaim == nil || got.ProcessClaim.StartToken != "failed-correction-token" {
		t.Fatalf("terminal process claim = %+v, want correction claim", got.ProcessClaim)
	}
	if turns != 2 {
		t.Fatalf("turn count = %d, want 2", turns)
	}
}

func TestExecutionCommitsStartingBeforeBackendStart(t *testing.T) {
	var server *Server
	backend := &executionFakeBackend{name: "ordered"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		store, err := server.ensureJobStore()
		if err != nil {
			t.Fatal(err)
		}
		records, err := store.List()
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || !records[0].Starting || records[0].State != protocol.PublicStateRunning {
			t.Fatalf("record at Backend.Start = %+v, want committed private starting", records)
		}
		return claimedResultSession("ordered"), nil
	}
	server = newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "ordered", nil)
	runExecution(t, server, record)
}

func TestExecutionMarkStartingFailureTerminalizesWithoutBackendStart(t *testing.T) {
	startedBackend := false
	backend := &executionFakeBackend{name: "starting-failure"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		startedBackend = true
		return nil, errors.New("backend must not start after MarkStarting fails")
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "starting failure", nil)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatalf("prepare starting record: %v", err)
	}

	runExecution(t, server, record)

	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if startedBackend {
		t.Fatal("backend started after MarkStarting failed")
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassInternal || got.Starting || got.FinishedAt == nil {
		t.Fatalf("terminal record = %+v, want failed internal non-starting terminal record", got)
	}
	if !strings.Contains(got.FailureReason, "job is already starting") {
		t.Fatalf("failure reason = %q, want MarkStarting error", got.FailureReason)
	}
}

func TestExecutionRecordsClaimInSeparateTransaction(t *testing.T) {
	var server *Server
	backend := &executionFakeBackend{name: "claim"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				store, err := server.ensureJobStore()
				if err != nil {
					t.Fatal(err)
				}
				before, err := store.List()
				if err != nil {
					t.Fatal(err)
				}
				if len(before) != 1 || !before[0].Starting || before[0].ProcessClaim != nil {
					t.Fatalf("record before process claim = %+v, want separate committed starting record", before)
				}
				input.OnProcessStart(engine.ProcessRef{PID: 4102, PGID: 4102, StartTime: "claim-token"}, 0)
				after, err := store.List()
				if err != nil {
					t.Fatal(err)
				}
				if len(after) != 1 || after[0].Starting || after[0].ProcessClaim == nil || after[0].ProcessClaim.StartToken != "claim-token" {
					t.Fatalf("record after process claim = %+v, want separately committed claim", after)
				}
				return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: "claimed"}), nil
			},
		}, nil
	}
	server = newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "claim", nil)
	runExecution(t, server, record)
}

func TestExecutionTimeoutFailsAndInterruptsProcessGroup(t *testing.T) {
	timeoutMS := int64(25)
	stream := make(chan engine.Event)
	var closeOnce sync.Once
	var actionsMu sync.Mutex
	var actions []string
	backend := &executionFakeBackend{name: "timeout"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4103, PGID: 4103, StartTime: "timeout-token"}, 0)
				return stream, nil
			},
			interrupt: func(context.Context) error {
				actionsMu.Lock()
				actions = append(actions, "TERM", "KILL")
				actionsMu.Unlock()
				closeOnce.Do(func() { close(stream) })
				return nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "timeout", &timeoutMS)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassTimeout {
		t.Fatalf("timeout terminal = %+v, want failed timeout", got)
	}
	actionsMu.Lock()
	defer actionsMu.Unlock()
	if strings.Join(actions, ",") != "TERM,KILL" {
		t.Fatalf("cleanup actions = %v, want TERM then KILL", actions)
	}
}

func TestExecutionSpillsLargeTerminalResultWithDigestAndElision(t *testing.T) {
	text := strings.Repeat("x", engine.DefaultInlineResultCap+1)
	backend := &executionFakeBackend{name: "spill"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return claimedResultSession(text), nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "spill", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	spilled, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(text))
	wantSHA256 := hex.EncodeToString(sum[:])
	if got.State != protocol.PublicStateCompleted || got.ResultText != "" || got.ResultPath != got.Artifacts.Result || got.ResultSHA256 != wantSHA256 || got.ResultBytes != int64(len(text)) || len(spilled) != len(text) || wantSHA256 != hex.EncodeToString(sha256Sum(spilled)) {
		t.Fatalf("spilled terminal = state=%s inline=%d bytes=%d, want complete elided matching result", got.State, len(got.ResultText), len(spilled))
	}
}

func TestExecutionCaptureWarningIsRecordedAndGapsTranscript(t *testing.T) {
	const warningText = "capture warning: raw stdout log stopped after 128 bytes: no space left on device"
	backend := &executionFakeBackend{name: "capture-warning"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4210, PGID: 4210, StartTime: "capture-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventWarning, Text: warningText, Metadata: map[string]any{"agentbusCaptureWarning": true, "bytesCaptured": 128}},
					engine.Event{Type: engine.EventResultMessage, Text: "finished"},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "capture warning", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(got.Diagnostics, "\n"), warningText) {
		t.Fatalf("job diagnostics = %#v, want capture warning", got.Diagnostics)
	}
	wire, err := server.jobRecordWire(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(wire.Diagnostics, "\n"), warningText) {
		t.Fatalf("job.get diagnostics = %#v, want capture warning", wire.Diagnostics)
	}
	limit := 10
	transcript, err := server.jobTranscript(got, protocol.JobTranscriptParams{JobID: got.JobID, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if !transcript.Gap || len(transcript.Items) == 0 || transcript.Items[0].Kind != "warning" || transcript.Items[0].Text != warningText {
		t.Fatalf("transcript = %#v, want warning item and a capture gap", transcript)
	}
}

func TestExecutionResultArtifactFailureTerminalizesAsStorageFailure(t *testing.T) {
	const backendSessionID = "storage-failure-session"
	backend := &executionFakeBackend{name: "storage-failure"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4211, PGID: 4211, StartTime: "storage-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventResultMessage, Text: "final output that cannot be persisted"},
					engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{BackendSessionID: backendSessionID}},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "storage failure", nil)
	if err := os.MkdirAll(filepath.Dir(record.Artifacts.Result), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(record.Artifacts.Result, 0o700); err != nil {
		t.Fatal(err)
	}
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassStorage || got.FailureStage != "finalize" {
		t.Fatalf("terminal record = state %q class %q stage %q, want failed storage finalize", got.State, got.FailureClass, got.FailureStage)
	}
	if !strings.Contains(got.FailureReason, "persist final result artifact") ||
		!(strings.Contains(got.FailureReason, "exist") || strings.Contains(got.FailureReason, "directory")) {
		t.Fatalf("failure reason = %q, want storage stage and OS error", got.FailureReason)
	}
	if got.ResultPath != "" || got.ResultText != "" {
		t.Fatalf("failed record contains an authoritative result: path=%q text=%q", got.ResultPath, got.ResultText)
	}
	if got.BackendSessionID != backendSessionID {
		t.Fatalf("backend session ID = %q, want %q", got.BackendSessionID, backendSessionID)
	}
	limit := 10
	transcript, err := server.jobTranscript(got, protocol.JobTranscriptParams{JobID: got.JobID, Limit: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if transcript.ItemCount != 1 || len(transcript.Items) != 1 || transcript.Items[0].Text != "final output that cannot be persisted" {
		t.Fatalf("transcript = %#v, want retained partial output", transcript)
	}
}

func TestManagedCodexHomeSurvivesTerminalStorageFailure(t *testing.T) {
	server := newExecutionServer(t, &executionFakeBackend{name: "codex"})
	record := queuedExecutionRecord(t, server, "codex", "terminal storage failure", nil)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatal(err)
	}
	layout := engine.WorkspaceLayout{Root: server.stateRoot, Namespace: filepath.Join(server.stateRoot, "sessions"), Codex: filepath.Join(server.stateRoot, "sessions", "codex")}
	home, err := createManagedCodexHome(layout, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	server.rememberManagedCodexHome(record.JobID, home)

	// A completed record cannot carry a failure reason. This makes the first
	// terminal write fail validation and exercises the storage-failure fallback.
	server.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:            protocol.PublicStateCompleted,
		Cleanup:          protocol.CleanupClean,
		BackendSessionID: "terminal-storage-session",
		FailureReason:    "invalid on a completed record",
	})

	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassStorage || got.FailureStage != "finalize" {
		t.Fatalf("terminal record = state %q class %q stage %q, want failed storage finalize fallback", got.State, got.FailureClass, got.FailureStage)
	}
	if got.BackendSessionID != "terminal-storage-session" {
		t.Fatalf("backend session ID = %q, want session retained by failed record", got.BackendSessionID)
	}
	if _, err := os.Stat(home.path); err != nil {
		t.Fatalf("managed home stat = %v, want home to survive failed terminal persistence", err)
	}
}

func sha256Sum(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func TestExecutionProviderOverloadedUsesTypedFailureClass(t *testing.T) {
	backend := &executionFakeBackend{name: "overloaded"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4105, PGID: 4105, StartTime: "overload-token"}, 0)
				return executionEvents(engine.Event{Type: engine.EventTerminalError, Text: "provider capacity text must not control classification", Err: engine.ErrProviderOverloaded}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "overloaded", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassProviderOverloaded {
		t.Fatalf("overload terminal = %+v, want provider_overloaded", got)
	}
}

func TestExecutionUsageLimitPreservesProviderEvidence(t *testing.T) {
	resetAt := time.Date(2026, time.October, 28, 23, 15, 0, 0, time.UTC)
	backend := &executionFakeBackend{name: "usage-limit"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4199, PGID: 4199, StartTime: "usage-limit-token"}, 0)
				failure := &engine.ProviderFailureError{
					Evidence: engine.ProviderFailureEvidence{
						Class: "usage_limit", Code: "rate_limit_error", Message: "weekly usage limit reached", RetryAfter: &resetAt,
					},
					Cause: errors.New("provider rate limit reached"),
				}
				return executionEvents(engine.Event{Type: engine.EventTerminalError, Text: failure.Error(), Err: failure}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "quota retry metadata", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassUsageLimit || got.FailureStage != "turn" {
		t.Fatalf("failure record = %+v, want usage_limit at turn stage", got)
	}
	if got.FailureProviderCode != "rate_limit_error" || got.FailureProviderMessage != "weekly usage limit reached" || got.FailureRetryAfter == nil || !got.FailureRetryAfter.Equal(resetAt) {
		t.Fatalf("provider evidence = code %q message %q retryAfter %v", got.FailureProviderCode, got.FailureProviderMessage, got.FailureRetryAfter)
	}
	if got.FailureReason != "provider rate limit reached" {
		t.Fatalf("failure reason = %q, want original adapter reason", got.FailureReason)
	}
}

func TestCancellationAndTimeoutReasonsPersistWithoutStateChanges(t *testing.T) {
	tests := []struct {
		name      string
		state     protocol.PublicState
		reason    protocol.CancellationReason
		configure func(*testing.T, *Server, *executionFakeBackend, jobstore.Record)
	}{
		{
			name: "operator cancel of queued job", state: protocol.PublicStateCanceled, reason: protocol.CancellationReasonOperator,
			configure: func(t *testing.T, server *Server, _ *executionFakeBackend, record jobstore.Record) {
				outcome := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
				if outcome.err != nil {
					t.Fatalf("job.cancel error = %#v", outcome.err)
				}
			},
		},
		{
			name: "deadline", state: protocol.PublicStateFailed, reason: protocol.CancellationReasonDeadline,
			configure: func(t *testing.T, server *Server, backend *executionFakeBackend, record jobstore.Record) {
				backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
					return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						input.OnProcessStart(engine.ProcessRef{PID: 4200, PGID: 4200, StartTime: "deadline-token"}, 0)
						return executionEvents(engine.Event{Type: engine.EventTerminalError, Text: context.DeadlineExceeded.Error(), Err: context.DeadlineExceeded}), nil
					}}, nil
				}
				runExecution(t, server, record)
			},
		},
		{
			name: "daemon shutdown", state: protocol.PublicStateFailed, reason: protocol.CancellationReasonDaemonShutdown,
			configure: func(t *testing.T, server *Server, backend *executionFakeBackend, record jobstore.Record) {
				backend.start = func(ctx context.Context, _ engine.SessionOpts) (engine.Session, error) { return nil, ctx.Err() }
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				server.runJob(ctx, record, newActiveExecution(record.JobID, backend))
			},
		},
		{
			name: "provider interruption", state: protocol.PublicStateFailed, reason: protocol.CancellationReasonProviderInterruption,
			configure: func(t *testing.T, server *Server, backend *executionFakeBackend, record jobstore.Record) {
				backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
					return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						input.OnProcessStart(engine.ProcessRef{PID: 4201, PGID: 4201, StartTime: "interrupt-token"}, 0)
						return executionEvents(engine.Event{Type: engine.EventTerminalError, Text: engine.ErrTurnInterrupted.Error(), Err: engine.ErrTurnInterrupted}), nil
					}}, nil
				}
				server.runJob(context.Background(), record, newActiveExecution(record.JobID, backend))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &executionFakeBackend{name: "cancel-reason"}
			server := newExecutionServer(t, backend)
			record := queuedExecutionRecord(t, server, backend.Name(), test.name, nil)
			test.configure(t, server, backend, record)
			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Get(record.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != test.state || got.CancellationReason != test.reason {
				t.Fatalf("terminal record = state %q cancellationReason %q, want %q / %q", got.State, got.CancellationReason, test.state, test.reason)
			}
		})
	}
}

func TestLatestObservedModelAndEffortReplaceEarlierValues(t *testing.T) {
	backend := &executionFakeBackend{name: "model-observation"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4202, PGID: 4202, StartTime: "model-observation-token"}, 0)
			return executionEvents(
				engine.Event{Type: engine.EventModelReported, ModelReported: "first-model", EffortReported: "low"},
				engine.Event{Type: engine.EventModelReported, ModelReported: "latest-model", EffortReported: "high"},
				engine.Event{Type: engine.EventResultMessage, Text: "done"},
			), nil
		}}, nil
	}
	server := newExecutionServer(t, backend)
	requestedModel, requestedEffort := "requested-model", "minimal"
	params := submissionParams("model-observation-ws", "model-observation-request", backend.Name(), t.TempDir(), "model provenance")
	params.TaskSpec.Model, params.TaskSpec.Effort = &requestedModel, &requestedEffort
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
	got, err := store.Get(submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Selection == nil || got.Selection.Model != requestedModel || got.Selection.Effort != requestedEffort || got.ModelReported != "latest-model" || got.EffortReported != "high" {
		t.Fatalf("selected/observed values = selection %#v observed %q/%q", got.Selection, got.ModelReported, got.EffortReported)
	}
	wireOutcome := server.handleJobGet(mustJSON(t, protocol.JobGetParams{JobID: submitted.JobID}))
	wire := wireOutcome.result.(protocol.JobRecordWire)
	if wire.ModelRequested == nil || *wire.ModelRequested != requestedModel || wire.EffortRequested == nil || *wire.EffortRequested != requestedEffort {
		t.Fatalf("requested wire selection = model %v effort %v", wire.ModelRequested, wire.EffortRequested)
	}
}

func TestExecutionMissingBinaryIsBackendUnavailableWithCleanCleanup(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "missing-claude")
	if _, err := os.Stat(binary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing binary stat = %v, want not exist", err)
	}
	backend := claudecli.New(claudecli.Options{Binary: binary})
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "missing binary", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendUnavailable || got.Cleanup != protocol.CleanupClean || got.ProcessClaim != nil {
		t.Fatalf("missing-binary terminal record = %+v, want failed backend_unavailable with clean cleanup and no process claim", got)
	}
}

func TestExecutionFailureAfterRecordedClaimPreservesUncertainCleanup(t *testing.T) {
	backend := &executionFakeBackend{name: "uncertain-after-spawn"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4106, PGID: 4106, StartTime: "uncertain-after-spawn-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventTerminalError, Err: errors.New("backend turn failed after launch")},
					engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{CleanupFailed: true}},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "uncertain after spawn", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendError || got.Cleanup != protocol.CleanupUncertain || got.ProcessClaim == nil || got.ProcessClaim.StartToken != "uncertain-after-spawn-token" {
		t.Fatalf("post-launch terminal record = %+v, want failed backend_error with uncertain cleanup and recorded claim", got)
	}
}

func TestExecutionFailureWithoutRecordedClaimPreservesUncertainCleanup(t *testing.T) {
	backend := &executionFakeBackend{name: "uncertain-without-claim"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, _ engine.TurnInput) (<-chan engine.Event, error) {
				return executionEvents(
					engine.Event{Type: engine.EventTerminalError, Err: errors.New("backend turn failed before process claim")},
					engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{CleanupFailed: true}},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "uncertain without claim", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassBackendError || got.Cleanup != protocol.CleanupUncertain || got.ProcessClaim != nil {
		t.Fatalf("no-claim terminal record = %+v, want failed backend_error with uncertain cleanup and no process claim", got)
	}
}

func TestRetireTurnMetadataFailuresDoNotChangeCleanup(t *testing.T) {
	backend := &executionFakeBackend{name: "retire-metadata-failure"}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "retire metadata failure", nil)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(record.JobID); err != nil {
		t.Fatal(err)
	}
	run := newActiveExecution(record.JobID, backend)
	run.beginTurn()
	// Closing the real store makes both metadata writes fail without a test
	// store seam. retireTurn must preserve cleanup while retaining why each
	// metadata write could not be recorded.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	outcome := run.retireTurn(store, turnOutcome{
		cleanup:          protocol.CleanupClean,
		modelReported:    "resolved-model",
		backendSessionID: "backend-session",
	})
	if outcome.cleanup != protocol.CleanupClean {
		t.Fatalf("retired cleanup = %q, want clean after metadata-only failures", outcome.cleanup)
	}
	diagnostics := strings.Join(outcome.diagnostics, "\n")
	if !strings.Contains(diagnostics, "record reported model:") || !strings.Contains(diagnostics, "record backend session:") {
		t.Fatalf("retired diagnostics = %#v, want both metadata failure diagnostics", outcome.diagnostics)
	}
}

func TestExecutionDoesNotPersistSessionIDOnCompletedRecords(t *testing.T) {
	for _, tt := range []struct {
		name    string
		start   func(context.Context, engine.SessionOpts) (engine.Session, error)
		wantID  string
		wantRun protocol.PublicState
	}{
		{
			name: "turn final session id",
			start: func(context.Context, engine.SessionOpts) (engine.Session, error) {
				return &executionFakeSession{
					turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						input.OnProcessStart(engine.ProcessRef{PID: 4110, PGID: 4110, StartTime: "session-id-token"}, 0)
						return executionEvents(
							engine.Event{Type: engine.EventResultMessage, Text: "done"},
							engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{BackendSessionID: "thread-finished"}},
						), nil
					},
				}, nil
			},
			wantID:  "",
			wantRun: protocol.PublicStateCompleted,
		},
		{
			name: "start failure before first turn",
			start: func(context.Context, engine.SessionOpts) (engine.Session, error) {
				return nil, errors.New("start failed before first turn")
			},
			wantRun: protocol.PublicStateFailed,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backend := &executionFakeBackend{name: "session-id-" + tt.name, start: tt.start}
			server := newExecutionServer(t, backend)
			record := queuedExecutionRecord(t, server, backend.Name(), "session id", nil)
			runExecution(t, server, record)

			store, err := server.ensureJobStore()
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Get(record.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tt.wantRun || got.BackendSessionID != tt.wantID {
				t.Fatalf("terminal record = state=%s backendSessionID=%q, want state=%s backendSessionID=%q", got.State, got.BackendSessionID, tt.wantRun, tt.wantID)
			}
		})
	}
}

func TestExecutionResumeFollowsManagedHomeLineage(t *testing.T) {
	var startCalled bool
	var resumedIDs []string
	var resumedHomes []string
	backend := &executionFakeBackend{name: "codex"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		startCalled = true
		return nil, errors.New("Start must not run for a resumed job")
	}
	backend.resume = func(_ context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
		resumedIDs = append(resumedIDs, id)
		resumedHomes = append(resumedHomes, opts.EnvOverlay["CODEX_HOME"])
		switch id {
		case "thread-source":
			return &executionFakeSession{
				turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
					input.OnProcessStart(engine.ProcessRef{PID: 4111, PGID: 4111, StartTime: "first-resume-token"}, 0)
					return executionEvents(
						engine.Event{Type: engine.EventTerminalError, Err: errors.New("first resumed turn failed")},
						engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{BackendSessionID: "thread-first-resumed"}},
					), nil
				},
			}, nil
		case "thread-first-resumed":
			return &executionFakeSession{
				turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
					input.OnProcessStart(engine.ProcessRef{PID: 4112, PGID: 4112, StartTime: "second-resume-token"}, 0)
					return executionEvents(
						engine.Event{Type: engine.EventResultMessage, Text: "second resumed result"},
						engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{BackendSessionID: "thread-second-resumed"}},
					), nil
				},
			}, nil
		default:
			return nil, fmt.Errorf("unexpected resume session %q", id)
		}
	}
	server := newExecutionServer(t, backend)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	sourceResult := submitResultForTest(t, submitForTest(t, server, submissionParams("resume-source-workspace", "resume-source-request", backend.Name(), t.TempDir(), "source")))
	source, err := store.Get(sourceResult.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(source.JobID, jobstore.TerminalUpdate{
		State:            protocol.PublicStateFailed,
		Cleanup:          protocol.CleanupClean,
		BackendSessionID: "thread-source",
		FailureClass:     protocol.FailureClassBackendError,
		FailureReason:    "timed out after a turn",
	}); err != nil {
		t.Fatal(err)
	}
	sourceHome := filepath.Join(server.stateRoot, "sessions", "codex", source.JobID)
	if err := os.MkdirAll(sourceHome, 0o700); err != nil {
		t.Fatal(err)
	}
	persistedSource, err := store.Get(source.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedSource.State != protocol.PublicStateFailed || persistedSource.BackendSessionID != "thread-source" {
		t.Fatalf("source record = %+v, want failed source with recorded session", persistedSource)
	}

	newResumed := func(workspace, requestID, resumeJobID string) jobstore.Record {
		t.Helper()
		spec := protocol.TaskSpec{
			Backend:     backend.Name(),
			CWD:         t.TempDir(),
			Prompt:      "continue from the source thread",
			Intent:      engine.IntentInspect,
			ResumeJobID: resumeJobID,
		}
		raw, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		resumed, deduplicated, err := store.SubmitTx(
			jobstore.RequestKey{WorkspaceKey: workspace, RequestID: requestID},
			raw,
			func(id string) (jobstore.Record, error) {
				return jobstore.Record{JobID: id, Backend: spec.Backend, CWD: spec.CWD, Intent: spec.Intent}, nil
			},
		)
		if err != nil || deduplicated {
			t.Fatalf("create resumed job = (%+v, deduplicated=%t, %v), want new record", resumed, deduplicated, err)
		}
		return resumed
	}

	firstResumed := newResumed("first-resume-workspace", "first-resume-request", source.JobID)
	runExecution(t, server, firstResumed)
	firstTerminal, err := store.Get(firstResumed.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if firstTerminal.State != protocol.PublicStateFailed || firstTerminal.BackendSessionID != "thread-first-resumed" {
		t.Fatalf("first resumed terminal = %+v, want failed with its new recorded session", firstTerminal)
	}
	if _, err := os.Stat(filepath.Join(server.stateRoot, "sessions", "codex", firstResumed.JobID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect first resumed home stat = %v, want no new managed thread home", err)
	}

	secondResumed := newResumed("second-resume-workspace", "second-resume-request", firstResumed.JobID)
	runExecution(t, server, secondResumed)
	secondTerminal, err := store.Get(secondResumed.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if secondTerminal.State != protocol.PublicStateCompleted || secondTerminal.BackendSessionID != "" {
		t.Fatalf("second resumed terminal = %+v, want completed without a retained session", secondTerminal)
	}
	if startCalled || len(resumedIDs) != 2 || resumedIDs[0] != "thread-source" || resumedIDs[1] != "thread-first-resumed" || resumedHomes[0] != sourceHome || resumedHomes[1] != sourceHome {
		t.Fatalf("session launches = Start:%t Resume:%#v CODEX_HOME:%#v, want Start:false IDs:[thread-source thread-first-resumed] homes:[%q %q]", startCalled, resumedIDs, resumedHomes, sourceHome, sourceHome)
	}
	if secondTerminal.JobID == source.JobID || secondTerminal.Artifacts == source.Artifacts || secondTerminal.Artifacts == firstTerminal.Artifacts {
		t.Fatalf("second resumed terminal = %+v, want a distinct record with its own artifacts", secondTerminal)
	}
}

func TestExecutionPersistsExplicitEmptyResultAfterAssistantText(t *testing.T) {
	backend := &executionFakeBackend{name: "empty-result"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4212, PGID: 4212, StartTime: "empty-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventAgentText, Text: "earlier status"}, engine.Event{Type: engine.EventResultMessage, Text: ""}), nil
		}}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "empty result", nil)
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(nil)
	if got.State != protocol.PublicStateCompleted || len(artifact) != 0 || got.ResultBytes != 0 || got.ResultSHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("empty result record = %#v, artifact length = %d", got, len(artifact))
	}
}

func TestDeadlineDuringRetirementPreservesAcceptedResult(t *testing.T) {
	backend := &executionFakeBackend{name: "accepted-before-deadline"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4214, PGID: 4214, StartTime: "completed-token"}, 0)
			events := make(chan engine.Event, 2)
			events <- engine.Event{Type: engine.EventResultMessage, Text: "accepted result"}
			go func() {
				<-ctx.Done()
				events <- engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{}}
				close(events)
			}()
			return events, nil
		}, interrupt: func(context.Context) error { return nil }}, nil
	}
	server := newExecutionServer(t, backend)
	timeout := int64(100)
	record := queuedExecutionRecord(t, server, backend.Name(), "deadline after completion", &timeout)
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != "accepted result" {
		t.Fatalf("deadline changed accepted result: %#v", got)
	}
}
