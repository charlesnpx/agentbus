//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	// maxSteerMessageIDBytes bounds the client-controlled id stored with a job.
	maxSteerMessageIDBytes = 256
	// maxSteerMessageBytes caps steering text at 64 KiB.
	maxSteerMessageBytes = 64 * 1024
	// maxPendingSteers bounds each job's in-memory ordered delivery queue.
	maxPendingSteers = 8
)

type queuedSteer struct {
	message engine.SteerMessage
}

// handleJobSteer validates and records a steering request before scheduling
// adapter I/O. The per-execution control lock is the ordering point shared with
// turn completion, cancel, and output-schema correction.
func (s *Server) handleJobSteer(raw json.RawMessage) requestOutcome {
	var params protocol.JobSteerParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if strings.TrimSpace(params.JobID) == "" {
		return invalidParams(errors.New("jobId is required"))
	}
	if strings.TrimSpace(params.MessageID) == "" || len(params.MessageID) > maxSteerMessageIDBytes {
		return invalidParams(fmt.Errorf("messageId must be non-empty and at most %d bytes", maxSteerMessageIDBytes))
	}
	if strings.TrimSpace(params.Text) == "" || len(params.Text) > maxSteerMessageBytes {
		return invalidParams(fmt.Errorf("text must be non-empty and at most %d bytes", maxSteerMessageBytes))
	}

	store, err := s.ensureJobStore()
	if err != nil {
		return jobStoreUnavailable("open job store", err)
	}
	record, err := store.Get(params.JobID)
	if err != nil {
		if errors.Is(err, jobstore.ErrNotFound) {
			return requestOutcome{err: unknownJobError(params.JobID)}
		}
		return jobStoreUnavailable("get job for steering", err)
	}
	run := s.activeExecution(params.JobID)
	if run != nil {
		run.controlMu.Lock()
		defer run.controlMu.Unlock()
		// Refresh after taking the ordering lock; it may have raced a terminal write.
		record, err = store.Get(params.JobID)
		if err != nil {
			return jobStoreUnavailable("refresh job for steering", err)
		}
	}

	digest := sha256.Sum256([]byte(params.Text))
	textHash := hex.EncodeToString(digest[:])
	if existing, ok := findSteerRecord(record, params.MessageID); ok {
		return steerDuplicateReceipt(params, textHash, existing)
	}

	spec, err := taskSpecFromRecord(record)
	if err != nil {
		return jobStoreUnavailable("read task mode for steering", err)
	}
	mode := protocol.EffectiveInputMode(spec.InputMode)
	if record.InputMode != "" {
		mode = record.InputMode
	}
	status := protocol.SteerStatusRejected
	reason := protocol.SteerReasonNotRunning
	switch {
	case record.State != protocol.PublicStateRunning || record.Starting:
		reason = protocol.SteerReasonNotRunning
	case mode != protocol.InputModeSteerable:
		reason = protocol.SteerReasonNotSteerable
	case run != nil && run.schemaCorrectionActive:
		reason = protocol.SteerReasonSchemaCorrectionActive
	case run == nil || run.steerCanceled || run.steerClosed || !run.turnActive:
		reason = protocol.SteerReasonNotRunning
	default:
		run.mu.Lock()
		session := run.session
		run.mu.Unlock()
		if _, ok := session.(engine.Steerer); !ok {
			reason = protocol.SteerReasonBackendUnsupported
		} else if run.steerPending >= maxPendingSteers {
			reason = protocol.SteerReasonQueueFull
		} else {
			status = protocol.SteerStatusAdmitted
			reason = ""
		}
	}

	steer := newSteerRecord(params.MessageID, textHash, len(params.Text), status, reason)
	stored, existed, err := store.RecordSteer(params.JobID, steer)
	if err != nil {
		return jobStoreUnavailable("record steering request", err)
	}
	if existed {
		return steerDuplicateReceipt(params, textHash, stored)
	}
	if status == protocol.SteerStatusAdmitted {
		select {
		case run.steerQueue <- queuedSteer{message: engine.SteerMessage{ID: params.MessageID, Text: params.Text}}:
			run.steerPending++
			run.steerQueued[params.MessageID] = struct{}{}
		default:
			updated, updateErr := store.UpdateSteer(protocol.JobSteerRecordUpdate{
				JobID: params.JobID, MessageID: params.MessageID,
				Status: protocol.SteerStatusRejected, Reason: protocol.SteerReasonQueueFull,
			})
			if updateErr != nil {
				return jobStoreUnavailable("record full steering queue", updateErr)
			}
			stored = updated
		}
		return requestOutcome{result: steerReceipt(params.JobID, stored, false)}
	}
	return requestOutcome{result: steerReceipt(params.JobID, stored, false)}
}

func newSteerRecord(messageID, textHash string, textBytes int, status protocol.SteerStatus, reason protocol.SteerReason) protocol.SteerRecord {
	now := time.Now().UTC()
	return protocol.SteerRecord{
		MessageID:  messageID,
		TextSHA256: textHash,
		TextBytes:  textBytes,
		Status:     status,
		Reason:     reason,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func findSteerRecord(record jobstore.Record, messageID string) (protocol.SteerRecord, bool) {
	for _, steer := range record.Steers {
		if steer.MessageID == messageID {
			return steer, true
		}
	}
	return protocol.SteerRecord{}, false
}

func steerDuplicateReceipt(params protocol.JobSteerParams, textHash string, existing protocol.SteerRecord) requestOutcome {
	if existing.TextSHA256 != textHash || existing.TextBytes != len(params.Text) {
		return requestOutcome{result: protocol.JobSteerResult{
			JobID: params.JobID, MessageID: params.MessageID,
			Status: protocol.SteerStatusRejected, Reason: protocol.SteerReasonDuplicateConflict,
		}}
	}
	return requestOutcome{result: steerReceipt(params.JobID, existing, true)}
}

func steerReceipt(jobID string, steer protocol.SteerRecord, deduplicated bool) protocol.JobSteerResult {
	return protocol.JobSteerResult{
		JobID: jobID, MessageID: steer.MessageID, Status: steer.Status,
		Mechanism: steer.Mechanism, Reason: steer.Reason, Deduplicated: deduplicated,
	}
}

func (s *Server) startSteeringWorker(store *jobstore.Store, run *activeExecution, ctx context.Context) {
	run.controlMu.Lock()
	if run.steerClosed || run.steerWorkerStarted {
		run.controlMu.Unlock()
		return
	}
	run.steerWorkerStarted = true
	run.controlMu.Unlock()
	go s.steeringLoop(store, run, ctx)
}

func (s *Server) steeringLoop(store *jobstore.Store, run *activeExecution, ctx context.Context) {
	defer close(run.steerDone)
	for {
		select {
		case queued := <-run.steerQueue:
			s.deliverSteer(store, run, ctx, queued)
		case <-run.steerStop:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) deliverSteer(store *jobstore.Store, run *activeExecution, ctx context.Context, queued queuedSteer) {
	run.controlMu.Lock()
	if _, pending := run.steerQueued[queued.message.ID]; pending {
		delete(run.steerQueued, queued.message.ID)
		if run.steerPending > 0 {
			run.steerPending--
		}
	}
	status := protocol.SteerStatusRejected
	reason := protocol.SteerReasonNotRunning
	var session engine.Session
	switch {
	case run.schemaCorrectionActive:
		reason = protocol.SteerReasonSchemaCorrectionActive
	case run.steerCanceled || run.steerClosed || !run.turnActive:
		reason = protocol.SteerReasonNotRunning
	default:
		run.mu.Lock()
		session = run.session
		run.mu.Unlock()
		steerer, ok := session.(engine.Steerer)
		if !ok {
			reason = protocol.SteerReasonBackendUnsupported
		} else {
			deliveryDone := make(chan struct{})
			run.steerInFlightDone = deliveryDone
			run.controlMu.Unlock()

			delivery, err := steerer.Steer(ctx, queued.message)
			mechanism := ""
			switch {
			case errors.Is(err, engine.ErrSteerNotActive):
				reason = protocol.SteerReasonNotRunning
			case errors.Is(err, engine.ErrSteerUnsupported):
				reason = protocol.SteerReasonBackendUnsupported
			case err != nil || !delivery.Confirmed || validSteerMechanism(delivery.Mechanism) == "":
				status = protocol.SteerStatusUnknown
				reason = protocol.SteerReasonDeliveryUnconfirmed
			default:
				status = protocol.SteerStatusDelivered
				reason = ""
				mechanism = validSteerMechanism(delivery.Mechanism)
			}
			_, _ = store.UpdateSteer(protocol.JobSteerRecordUpdate{
				JobID: run.jobID, MessageID: queued.message.ID,
				Status: status, Mechanism: mechanism, Reason: reason,
			})
			close(deliveryDone)

			run.controlMu.Lock()
			if run.steerInFlightDone == deliveryDone {
				run.steerInFlightDone = nil
			}
			run.controlMu.Unlock()
			return
		}
	}
	_, _ = store.UpdateSteer(protocol.JobSteerRecordUpdate{
		JobID: run.jobID, MessageID: queued.message.ID,
		Status: status, Reason: reason,
	})
	run.controlMu.Unlock()
}

func validSteerMechanism(value string) string {
	switch value {
	case engine.SteerMechanismNative, engine.SteerMechanismInterruptContinue:
		return value
	default:
		return ""
	}
}

// cancelSteering reserves cancellation at the job ordering point and rejects
// pending instructions before the process interruption begins.
func (s *Server) cancelSteering(store *jobstore.Store, run *activeExecution) bool {
	if run == nil {
		return true
	}
	run.controlMu.Lock()
	if run.steerClosed {
		run.controlMu.Unlock()
		return false
	}
	run.steerCanceled = true
	run.turnActive = false
	run.rejectQueuedSteersLocked(store, protocol.SteerReasonNotRunning)
	run.controlMu.Unlock()
	return true
}

// rejectQueuedSteersLocked discards all queued, not-yet-delivered messages.
func (run *activeExecution) rejectQueuedSteersLocked(store *jobstore.Store, reason protocol.SteerReason) {
	for messageID := range run.steerQueued {
		_, _ = store.UpdateSteer(protocol.JobSteerRecordUpdate{
			JobID: run.jobID, MessageID: messageID,
			Status: protocol.SteerStatusRejected, Reason: reason,
		})
		delete(run.steerQueued, messageID)
	}
	run.steerPending = 0
	for {
		select {
		case <-run.steerQueue:
		default:
			return
		}
	}
}

func (run *activeExecution) closeSteeringLocked(store *jobstore.Store) {
	run.steerClosed = true
	run.turnActive = false
	run.rejectQueuedSteersLocked(store, protocol.SteerReasonNotRunning)
	if run.steerInFlightDone != nil {
		<-run.steerInFlightDone
		run.steerInFlightDone = nil
	}
	run.steerStopOnce.Do(func() { close(run.steerStop) })
}

func (run *activeExecution) waitSteeringWorker() {
	if run == nil {
		return
	}
	run.controlMu.Lock()
	started := run.steerWorkerStarted
	done := run.steerDone
	run.controlMu.Unlock()
	if !started {
		return
	}
	<-done
}

func (s *Server) finishSteering(store *jobstore.Store, run *activeExecution) {
	if run == nil {
		return
	}
	run.controlMu.Lock()
	run.closeSteeringLocked(store)
	run.controlMu.Unlock()
	run.waitSteeringWorker()
}

func (run *activeExecution) setSchemaCorrectionActive(active bool) {
	run.controlMu.Lock()
	run.schemaCorrectionActive = active
	run.controlMu.Unlock()
}
