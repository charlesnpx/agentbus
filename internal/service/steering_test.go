//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type testSteerSession struct {
	calls    atomic.Int64
	entered  chan engine.SteerMessage
	release  <-chan struct{}
	delivery engine.SteerDelivery
	err      error
	onCall   func()
}

type plainTestSession struct{}

func (plainTestSession) ID() string { return "plain-test-session" }

func (plainTestSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	return nil, nil
}

func (plainTestSession) Interrupt(context.Context) error { return nil }

func (*testSteerSession) ID() string { return "test-session" }

func (*testSteerSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	return nil, nil
}

func (*testSteerSession) Interrupt(context.Context) error { return nil }

func (session *testSteerSession) Steer(ctx context.Context, message engine.SteerMessage) (engine.SteerDelivery, error) {
	session.calls.Add(1)
	if session.onCall != nil {
		session.onCall()
	}
	if session.entered != nil {
		session.entered <- message
	}
	if session.release != nil {
		select {
		case <-session.release:
		case <-ctx.Done():
			return engine.SteerDelivery{}, ctx.Err()
		}
	}
	return session.delivery, session.err
}

func newRunningSteerTestJob(t *testing.T, mode protocol.InputMode, session engine.Session, worker bool) (*Server, *jobstore.Store, *activeExecution, string) {
	t.Helper()
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("workspace-steering", "request-steering", "codex", t.TempDir(), "original task")
	params.TaskSpec.InputMode = mode
	submitted := submitResultForTest(t, submitForTest(t, server, params))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarting(submitted.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordProcessClaim(submitted.JobID, jobstore.ProcessClaim{PID: 42, PGID: 42, StartToken: "test-start-token"}); err != nil {
		t.Fatal(err)
	}
	run := newActiveExecution(submitted.JobID, nil)
	run.setSession(session)
	run.beginTurn()
	run.controlMu.Lock()
	run.turnActive = true
	run.controlMu.Unlock()
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{submitted.JobID: run}
	server.executionMu.Unlock()
	if worker {
		server.startSteeringWorker(store, run, context.Background())
	}
	t.Cleanup(func() { server.finishSteering(store, run) })
	return server, store, run, submitted.JobID
}

func TestSessionWithoutSteererIsRejected(t *testing.T) {
	server, _, _, jobID := newRunningSteerTestJob(t, protocol.InputModeSteerable, plainTestSession{}, false)
	receipt := steerForTest(t, server, protocol.JobSteerParams{JobID: jobID, MessageID: "unsupported-message", Text: "continue"})
	if receipt.Status != protocol.SteerStatusRejected || receipt.Reason != protocol.SteerReasonBackendUnsupported {
		t.Fatalf("unsupported session receipt = %#v, want rejected/backend_unsupported", receipt)
	}
}

func steerForTest(t *testing.T, server *Server, params protocol.JobSteerParams) protocol.JobSteerResult {
	t.Helper()
	outcome := server.handleJobSteer(mustJSON(t, params))
	if outcome.err != nil {
		t.Fatalf("job.steer error = %#v", outcome.err)
	}
	receipt, ok := outcome.result.(protocol.JobSteerResult)
	if !ok {
		t.Fatalf("job.steer result = %T %#v", outcome.result, outcome.result)
	}
	return receipt
}

func TestFixedJobSteerRejectedBeforeSessionSteer(t *testing.T) {
	session := &testSteerSession{onCall: func() { t.Errorf("fixed job invoked Session.Steer") }}
	server, store, _, jobID := newRunningSteerTestJob(t, protocol.InputModeFixed, session, false)
	params := protocol.JobSteerParams{JobID: jobID, MessageID: "fixed-message", Text: "private follow-up"}
	receipt := steerForTest(t, server, params)
	if receipt.Status != protocol.SteerStatusRejected || receipt.Reason != protocol.SteerReasonNotSteerable {
		t.Fatalf("fixed job receipt = %#v, want rejected/not_steerable", receipt)
	}
	if got := session.calls.Load(); got != 0 {
		t.Fatalf("Session.Steer calls = %d, want 0", got)
	}
	record, err := store.Get(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Steers) != 1 || record.Steers[0].Status != protocol.SteerStatusRejected {
		t.Fatalf("stored steers = %#v, want one rejected metadata record", record.Steers)
	}
	digest := sha256.Sum256([]byte(params.Text))
	if got := record.Steers[0].TextSHA256; got != hex.EncodeToString(digest[:]) {
		t.Fatalf("stored steer digest = %q, want sha256", got)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), params.Text) {
		t.Fatalf("stored job contains steering text: %s", encoded)
	}
}

func TestSteerDeduplicatesAndConflictsByMessageID(t *testing.T) {
	session := &testSteerSession{
		entered:  make(chan engine.SteerMessage, 1),
		delivery: engine.SteerDelivery{Mechanism: engine.SteerMechanismNative, Confirmed: true},
	}
	server, store, run, jobID := newRunningSteerTestJob(t, protocol.InputModeSteerable, session, true)
	params := protocol.JobSteerParams{JobID: jobID, MessageID: "message-1", Text: "continue"}
	first := steerForTest(t, server, params)
	if first.Status != protocol.SteerStatusAdmitted {
		t.Fatalf("first receipt = %#v, want admitted", first)
	}
	select {
	case message := <-session.entered:
		if message.ID != params.MessageID || message.Text != params.Text {
			t.Fatalf("engine message = %#v, want %#v", message, engine.SteerMessage{ID: params.MessageID, Text: params.Text})
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for steering worker")
	}
	run.retireTurn(store, turnOutcome{cleanup: protocol.CleanupClean})
	record, err := store.Get(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Steers) != 1 || record.Steers[0].Status != protocol.SteerStatusDelivered || record.Steers[0].Mechanism != engine.SteerMechanismNative {
		t.Fatalf("delivered steer record = %#v", record.Steers)
	}
	wire, err := server.jobRecordWire(record)
	if err != nil {
		t.Fatal(err)
	}
	if wire.InputMode != protocol.InputModeSteerable || len(wire.Steers) != 1 || wire.Steers[0].MessageID != params.MessageID || wire.Steers[0].Mechanism != engine.SteerMechanismNative {
		t.Fatalf("job.get steer projection = %#v, want text-free delivered summary", wire)
	}
	projected, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(projected), params.Text) {
		t.Fatalf("job.get exposed steering text: %s", projected)
	}
	deduplicated := steerForTest(t, server, params)
	if !deduplicated.Deduplicated || deduplicated.Status != protocol.SteerStatusDelivered {
		t.Fatalf("same-text retry receipt = %#v, want deduplicated original delivery", deduplicated)
	}
	conflict := steerForTest(t, server, protocol.JobSteerParams{JobID: jobID, MessageID: params.MessageID, Text: "different"})
	if conflict.Status != protocol.SteerStatusRejected || conflict.Reason != protocol.SteerReasonDuplicateConflict || conflict.Deduplicated {
		t.Fatalf("different-text retry receipt = %#v, want duplicate_conflict", conflict)
	}
	if got := session.calls.Load(); got != 1 {
		t.Fatalf("Session.Steer calls = %d, want one", got)
	}
}

func TestSteerPendingQueueIsBounded(t *testing.T) {
	session := &testSteerSession{onCall: func() { t.Errorf("queued steer reached Session.Steer without a worker") }}
	server, store, run, jobID := newRunningSteerTestJob(t, protocol.InputModeSteerable, session, false)
	for index := 0; index < maxPendingSteers; index++ {
		params := protocol.JobSteerParams{JobID: jobID, MessageID: fmt.Sprintf("message-%d", index), Text: fmt.Sprintf("follow-up %d", index)}
		if receipt := steerForTest(t, server, params); receipt.Status != protocol.SteerStatusAdmitted {
			t.Fatalf("queue item %d receipt = %#v, want admitted", index, receipt)
		}
	}
	full := steerForTest(t, server, protocol.JobSteerParams{JobID: jobID, MessageID: "overflow", Text: "overflow"})
	if full.Status != protocol.SteerStatusRejected || full.Reason != protocol.SteerReasonQueueFull {
		t.Fatalf("overflow receipt = %#v, want rejected/queue_full", full)
	}
	if !server.cancelSteering(store, run) {
		t.Fatal("cancel reservation unexpectedly lost")
	}
	record, err := store.Get(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Steers) != maxPendingSteers+1 || record.Steers[maxPendingSteers].Reason != protocol.SteerReasonQueueFull {
		t.Fatalf("steer records after queue overflow = %#v", record.Steers)
	}
}

func TestUnconfirmedSteerAndRestartRemainUnknown(t *testing.T) {
	t.Run("unconfirmed adapter", func(t *testing.T) {
		session := &testSteerSession{
			entered:  make(chan engine.SteerMessage, 1),
			delivery: engine.SteerDelivery{Mechanism: engine.SteerMechanismNative, Confirmed: false},
		}
		server, store, run, jobID := newRunningSteerTestJob(t, protocol.InputModeSteerable, session, true)
		params := protocol.JobSteerParams{JobID: jobID, MessageID: "uncertain-message", Text: "maybe sent"}
		if receipt := steerForTest(t, server, params); receipt.Status != protocol.SteerStatusAdmitted {
			t.Fatalf("initial receipt = %#v, want admitted", receipt)
		}
		select {
		case <-session.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for steering worker")
		}
		run.retireTurn(store, turnOutcome{cleanup: protocol.CleanupClean})
		record, err := store.Get(jobID)
		if err != nil {
			t.Fatal(err)
		}
		steer := record.Steers[0]
		if steer.Status != protocol.SteerStatusUnknown || steer.Reason != protocol.SteerReasonDeliveryUnconfirmed || steer.Mechanism != "" {
			t.Fatalf("unconfirmed steer record = %#v, want unknown/delivery_unconfirmed", steer)
		}
	})

	t.Run("restart reconciliation", func(t *testing.T) {
		server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
		params := submissionParams("workspace-steering-restart", "request-steering-restart", "codex", t.TempDir(), "original task")
		params.TaskSpec.InputMode = protocol.InputModeSteerable
		submitted := submitResultForTest(t, submitForTest(t, server, params))
		store, err := server.ensureJobStore()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkStarting(submitted.JobID); err != nil {
			t.Fatal(err)
		}
		text := "test"
		digest := sha256.Sum256([]byte(text))
		steer := newSteerRecord("restart-message", hex.EncodeToString(digest[:]), len(text), protocol.SteerStatusAdmitted, "")
		if _, existed, err := store.RecordSteer(submitted.JobID, steer); err != nil || existed {
			t.Fatalf("record admitted steer: existed=%t err=%v", existed, err)
		}
		if err := server.reconcileRecoveredJobs(store); err != nil {
			t.Fatal(err)
		}
		record, err := store.Get(submitted.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State != protocol.PublicStateUnknown || len(record.Steers) != 1 || record.Steers[0].Status != protocol.SteerStatusUnknown || record.Steers[0].Reason != protocol.SteerReasonDeliveryUnconfirmed {
			t.Fatalf("recovered job = state:%s steers:%#v, want unknown without redelivery", record.State, record.Steers)
		}
	})
}

func TestSteerAfterCompletionOrCancelIsNotRunning(t *testing.T) {
	for _, terminal := range []struct {
		name  string
		state protocol.PublicState
	}{
		{name: "completion", state: protocol.PublicStateCompleted},
		{name: "cancel", state: protocol.PublicStateCanceled},
	} {
		t.Run(terminal.name, func(t *testing.T) {
			session := &testSteerSession{onCall: func() { t.Errorf("terminal job invoked Session.Steer") }}
			server, store, run, jobID := newRunningSteerTestJob(t, protocol.InputModeSteerable, session, false)
			pending := steerForTest(t, server, protocol.JobSteerParams{JobID: jobID, MessageID: "pending-message", Text: "queued before terminal"})
			if pending.Status != protocol.SteerStatusAdmitted {
				t.Fatalf("pending receipt = %#v, want admitted", pending)
			}
			if terminal.state == protocol.PublicStateCanceled {
				if !server.cancelSteering(store, run) {
					t.Fatal("cancel reservation unexpectedly lost")
				}
				if _, err := store.MarkTerminal(jobID, jobstore.TerminalUpdate{State: terminal.state, Cleanup: protocol.CleanupClean}); err != nil {
					t.Fatal(err)
				}
			} else {
				server.markTerminal(store, jobID, jobstore.TerminalUpdate{State: terminal.state, Cleanup: protocol.CleanupClean})
			}
			receipt := steerForTest(t, server, protocol.JobSteerParams{JobID: jobID, MessageID: "late-message", Text: "too late"})
			if receipt.Status != protocol.SteerStatusRejected || receipt.Reason != protocol.SteerReasonNotRunning {
				t.Fatalf("terminal steer receipt = %#v, want rejected/not_running", receipt)
			}
			if got := session.calls.Load(); got != 0 {
				t.Fatalf("Session.Steer calls = %d, want 0", got)
			}
			record, err := store.Get(jobID)
			if err != nil {
				t.Fatal(err)
			}
			if len(record.Steers) != 2 || record.Steers[0].Status != protocol.SteerStatusRejected || record.Steers[0].Reason != protocol.SteerReasonNotRunning {
				t.Fatalf("terminal job steering records = %#v, want pending steer rejected as not_running", record.Steers)
			}
		})
	}
}
