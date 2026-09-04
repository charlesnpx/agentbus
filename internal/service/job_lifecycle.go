//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const processGroupPollInterval = 10 * time.Millisecond

// handleJobGet returns a bare detailed record for an identified lookup.
func (s *Server) handleJobGet(raw json.RawMessage) requestOutcome {
	var params protocol.JobGetParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if strings.TrimSpace(params.JobID) == "" {
		return invalidParams(errors.New("jobId is required"))
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
		return jobStoreUnavailable("get job", err)
	}
	wire, err := jobRecordWire(record)
	if err != nil {
		return jobStoreUnavailable("project job record", err)
	}
	return requestOutcome{result: wire}
}

// handleJobList returns compact summaries after applying every supplied
// workspace, tag, and public-state filter.
func (s *Server) handleJobList(raw json.RawMessage) requestOutcome {
	var params protocol.JobListParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if err := validateJobListParams(params); err != nil {
		return invalidParams(err)
	}
	store, err := s.ensureJobStore()
	if err != nil {
		return jobStoreUnavailable("open job store", err)
	}
	records, err := store.List()
	if err != nil {
		return jobStoreUnavailable("list jobs", err)
	}
	jobs := make([]protocol.JobSummaryWire, 0, len(records))
	for _, record := range records {
		spec, _, err := taskSpecForProjection(record)
		if err != nil {
			return jobStoreUnavailable("project job summary", err)
		}
		if !jobListMatches(record, spec, params) {
			continue
		}
		jobs = append(jobs, s.jobSummaryWireFromSpec(record, spec))
	}
	return requestOutcome{result: protocol.JobListResult{Jobs: jobs}}
}

func validateJobListParams(params protocol.JobListParams) error {
	for _, state := range params.States {
		if !state.Valid() {
			return fmt.Errorf("states contains invalid public state %q", state)
		}
	}
	return nil
}

func jobListMatches(record jobstore.Record, spec protocol.TaskSpec, params protocol.JobListParams) bool {
	if params.WorkspaceKey != "" && record.WorkspaceKey != params.WorkspaceKey {
		return false
	}
	if len(params.Tags) > 0 {
		var tags map[string]string
		if spec.Tags != nil {
			tags = *spec.Tags
		}
		for key, want := range params.Tags {
			if got, ok := tags[key]; !ok || got != want {
				return false
			}
		}
	}
	if len(params.States) > 0 {
		state := projectedState(record)
		for _, allowed := range params.States {
			if state == allowed {
				return true
			}
		}
		return false
	}
	return true
}

// handleJobCancel first prevents the locally owned execution from crossing a
// launch boundary, then commits the same first-terminal-wins record transition
// used by ordinary execution. The in-memory interruption is containment only;
// MarkTerminal is the durable cancellation fact.
func (s *Server) handleJobCancel(raw json.RawMessage) requestOutcome {
	var params protocol.JobCancelParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if strings.TrimSpace(params.JobID) == "" {
		return invalidParams(errors.New("jobId is required"))
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
		return invalidParams(fmt.Errorf("get job for cancellation: %w", err))
	}
	if record.State.IsTerminal() {
		return requestOutcome{result: protocol.JobCancelResult{JobID: record.JobID, State: projectedState(record)}}
	}

	run := s.activeExecution(record.JobID)
	cleanup, diagnostics := s.cancelActiveOrRecordedProcess(store, record)
	if run != nil {
		diagnostics = append(diagnostics, run.itemSidecarDiagnostics()...)
	}
	terminal, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:       protocol.PublicStateCanceled,
		Cleanup:     cleanup,
		Diagnostics: diagnostics,
		FinishedAt:  time.Now().UTC(),
	})
	if err != nil {
		if errors.Is(err, jobstore.ErrTerminal) {
			// Completion or another cancellation committed first. First terminal
			// wins, so return that durable fact without overwriting it.
			current, getErr := store.Get(record.JobID)
			if getErr == nil {
				return requestOutcome{result: protocol.JobCancelResult{JobID: current.JobID, State: projectedState(current)}}
			}
			return jobStoreUnavailable("read concurrent terminal cancellation", getErr)
		}
		return jobStoreUnavailable("persist cancellation", err)
	}
	return requestOutcome{result: protocol.JobCancelResult{JobID: terminal.JobID, State: projectedState(terminal)}}
}

func jobStoreUnavailable(action string, err error) requestOutcome {
	return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, action+": "+err.Error(), protocol.ErrorData{})}
}

func unknownJobError(jobID string) *protocol.ErrorObject {
	return protocol.NewError(protocol.ErrorUnknownJob, "unknown job", protocol.ErrorData{JobID: jobID})
}

func projectedState(record jobstore.Record) protocol.PublicState {
	if record.Starting {
		return protocol.PublicStateRunning
	}
	return record.State
}

func jobRecordWire(record jobstore.Record) (protocol.JobRecordWire, error) {
	spec, timeout, err := taskSpecForProjection(record)
	if err != nil {
		return protocol.JobRecordWire{}, err
	}
	var tags map[string]string
	if spec.Tags != nil {
		tags = *spec.Tags
	}
	return protocol.JobRecordWire{
		JobID:        record.JobID,
		WorkspaceKey: record.WorkspaceKey,
		RequestID:    record.RequestID,
		Backend:      record.Backend,
		State:        projectedState(record),
		Tags:         tags,
		CreatedAt:    record.CreatedAt,
		StartedAt:    record.StartedAt,
		FinishedAt:   record.FinishedAt,
		Timeout:      timeout,
		Result:       projectResult(record),
		Contract:     projectContract(record, spec),
		Failure:      projectFailure(record),
		Cleanup:      record.Cleanup,
		LogPaths:     projectLogPaths(record),
	}, nil
}

func (s *Server) jobSummaryWire(record jobstore.Record) (protocol.JobSummaryWire, error) {
	spec, _, err := taskSpecForProjection(record)
	if err != nil {
		return protocol.JobSummaryWire{}, err
	}
	return s.jobSummaryWireFromSpec(record, spec), nil
}

func (s *Server) jobSummaryWireFromSpec(record jobstore.Record, spec protocol.TaskSpec) protocol.JobSummaryWire {
	var tags map[string]string
	if spec.Tags != nil {
		tags = *spec.Tags
	}
	wire := protocol.JobSummaryWire{
		JobID:        record.JobID,
		Backend:      record.Backend,
		State:        projectedState(record),
		Tags:         tags,
		Cleanup:      record.Cleanup,
		CreatedAt:    record.CreatedAt,
		UpdatedAt:    record.UpdatedAt,
		FailureClass: projectFailureClass(record),
		Contract:     projectContractVerdict(record, spec),
	}
	if record.State.IsTerminal() {
		return wire
	}
	activity, active := s.ItemActivity(record.JobID)
	if !active {
		return wire
	}
	itemCount := activity.ItemCount
	wire.ItemCount = &itemCount
	if !activity.LastItemAt.IsZero() {
		lastItemAt := activity.LastItemAt
		wire.LastItemAt = &lastItemAt
	}
	wire.Liveness = s.recordedClaimLiveness(record.ProcessClaim)
	return wire
}

func taskSpecForProjection(record jobstore.Record) (protocol.TaskSpec, *engine.TimeoutResolution, error) {
	spec, err := decodeTaskSpec(record.TaskSpec)
	if err != nil {
		return protocol.TaskSpec{}, nil, fmt.Errorf("decode durable task spec: %w", err)
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMS)
	if errObj != nil {
		return protocol.TaskSpec{}, nil, errors.New(errObj.Message)
	}
	return spec, timeout, nil
}

func projectContract(record jobstore.Record, spec protocol.TaskSpec) *protocol.ContractResult {
	if len(spec.OutputSchema) == 0 {
		return nil
	}
	return &record.Contract
}

func projectContractVerdict(record jobstore.Record, spec protocol.TaskSpec) *protocol.ContractVerdict {
	if len(spec.OutputSchema) == 0 {
		return nil
	}
	return &protocol.ContractVerdict{Evaluated: record.Contract.Evaluated, Compliant: record.Contract.Compliant}
}

func projectFailure(record jobstore.Record) *protocol.JobFailureWire {
	if record.State != protocol.PublicStateFailed {
		return nil
	}
	return &protocol.JobFailureWire{Class: record.FailureClass, Reason: record.FailureReason}
}

func projectFailureClass(record jobstore.Record) protocol.FailureClass {
	if record.State != protocol.PublicStateFailed {
		return ""
	}
	return record.FailureClass
}

func projectResult(record jobstore.Record) *protocol.ResultInfoWire {
	if record.State != protocol.PublicStateCompleted || record.ResultPath == "" {
		return nil
	}
	return &protocol.ResultInfoWire{
		Text:       record.ResultText,
		ResultPath: record.ResultPath,
		SHA256:     record.ResultSHA256,
		Bytes:      record.ResultBytes,
	}
}

func projectLogPaths(record jobstore.Record) *protocol.LogPathsWire {
	if record.Artifacts.Log == "" {
		return nil
	}
	paths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		return nil
	}
	stdoutExists := fileExists(paths.Stdout)
	stderrExists := fileExists(paths.Stderr)
	if !stdoutExists && !stderrExists {
		return nil
	}
	wire := &protocol.LogPathsWire{}
	if stdoutExists {
		wire.Stdout = paths.Stdout
		wire.StdoutTruncated = logEndsWithTruncationMarker(paths.Stdout)
	}
	if stderrExists {
		wire.Stderr = paths.Stderr
		wire.StderrTruncated = logEndsWithTruncationMarker(paths.Stderr)
	}
	return wire
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// logEndsWithTruncationMarker reads only the marker-sized tail so projection
// stays bounded even when a log reaches its maximum size. A nil result means
// the file could not be inspected.
func logEndsWithTruncationMarker(path string) *bool {
	file, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil
	}
	marker := engine.TruncationMarker()
	truncated := false
	if info.Size() < int64(len(marker)) {
		return &truncated
	}
	if _, err := file.Seek(-int64(len(marker)), io.SeekEnd); err != nil {
		return nil
	}
	tail := make([]byte, len(marker))
	if _, err := io.ReadFull(file, tail); err != nil {
		return nil
	}
	truncated = string(tail) == marker
	return &truncated
}

// reconcileRecoveredJobs is intentionally called before the listener exists.
// It snapshots all records once, never enqueues recovered work, and writes
// only records that were nonterminal in that snapshot.
func (s *Server) reconcileRecoveredJobs(store *jobstore.Store) error {
	if store == nil {
		return errors.New("restart reconciliation requires a job store")
	}
	records, err := store.List()
	if err != nil {
		return err
	}
	for _, record := range records {
		switch {
		case record.State == protocol.PublicStateQueued:
			_, err = store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
				State:         protocol.PublicStateFailed,
				Cleanup:       protocol.CleanupClean,
				FailureClass:  protocol.FailureClassInternal,
				FailureReason: "daemon restarted before job launch",
				Diagnostics:   []string{"restart reconciliation: queued job was never relaunched"},
				FinishedAt:    time.Now().UTC(),
			})
		case record.Starting || record.State == protocol.PublicStateRunning:
			cleanup, diagnostics := s.terminateRecordedProcessClaim(record.ProcessClaim)
			_, err = store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
				State:       protocol.PublicStateUnknown,
				Cleanup:     cleanup,
				Diagnostics: append([]string{"restart reconciliation: no relaunch"}, diagnostics...),
				FinishedAt:  time.Now().UTC(),
			})
		default:
			// A terminal record is deliberately untouched. In particular, do
			// not use MarkTerminal here: even a logically equivalent rewrite
			// would violate the byte-for-byte preservation requirement.
			continue
		}
		if err != nil && !errors.Is(err, jobstore.ErrTerminal) {
			return fmt.Errorf("reconcile recovered job %s: %w", record.JobID, err)
		}
	}
	return nil
}

// cancelActiveOrRecordedProcess gives queued work a clean cancellation without
// a signal. An active local session uses the retained command terminator;
// recovery-like records without such a session use the same exact-token fence
// as the orphan reaper.
func (s *Server) cancelActiveOrRecordedProcess(store *jobstore.Store, record jobstore.Record) (protocol.Cleanup, []string) {
	if run := s.activeExecution(record.JobID); run != nil {
		sessionPresent, interruptErr := run.requestCancellation()
		if interruptErr != nil {
			return protocol.CleanupUncertain, []string{"cancel live process group: " + interruptErr.Error()}
		}
		current, err := store.Get(record.JobID)
		if err != nil {
			return protocol.CleanupUncertain, []string{"read process claim after live cancellation: " + err.Error()}
		}
		// The initial Get may have observed queued just before runJob committed
		// MarkStarting. Re-read after the launch gate is held so that a cancel
		// that actually raced past that commit is handled as an active process,
		// never as a clean no-spawn cancellation.
		if current.State == protocol.PublicStateQueued && !current.Starting {
			return protocol.CleanupClean, nil
		}
		if current.ProcessClaim == nil {
			return protocol.CleanupUncertain, []string{"cancel live process group: process claim is missing"}
		}
		if !sessionPresent {
			return s.terminateRecordedProcessClaim(current.ProcessClaim)
		}
		gone, err := s.waitForProcessGroupGone(current.ProcessClaim.PGID, s.processGroupCancellationGrace())
		if err != nil {
			return protocol.CleanupUncertain, []string{"verify canceled process group: " + err.Error()}
		}
		if !gone {
			return protocol.CleanupUncertain, []string{"canceled process group did not exit within grace"}
		}
		return protocol.CleanupClean, nil
	}
	if record.State == protocol.PublicStateQueued && !record.Starting {
		return protocol.CleanupClean, nil
	}

	return s.terminateRecordedProcessClaim(record.ProcessClaim)
}

func (s *Server) activeExecution(jobID string) *activeExecution {
	if s == nil || jobID == "" {
		return nil
	}
	s.executionMu.Lock()
	run := s.executions[jobID]
	s.executionMu.Unlock()
	return run
}

func (s *Server) cancellationPending(jobID string) bool {
	if run := s.activeExecution(jobID); run != nil {
		return run.cancellationRequested()
	}
	return false
}

type processClaimDiagnosticKind uint8

const (
	processClaimExact processClaimDiagnosticKind = iota
	processClaimMissing
	processClaimIncomplete
	processClaimUnreadable
	processClaimUnavailable
	processClaimStartTokenUnavailable
	processClaimStartTokenMismatch
)

// processClaimDiagnostic preserves the identity-check result for both the
// reaper and the public liveness projection. The public projection deliberately
// maps this detailed private result to a small verdict enum.
type processClaimDiagnostic struct {
	kind processClaimDiagnosticKind
	err  error
}

func (diagnostic processClaimDiagnostic) exact() bool {
	return diagnostic.kind == processClaimExact
}

func (diagnostic processClaimDiagnostic) message() string {
	switch diagnostic.kind {
	case processClaimMissing:
		return "orphan reaper: process claim is missing; no signal sent"
	case processClaimIncomplete:
		return "orphan reaper: process claim is incomplete; no signal sent"
	case processClaimUnreadable:
		return "orphan reaper: leader start token is unreadable; no signal sent: " + diagnostic.err.Error()
	case processClaimUnavailable, processClaimStartTokenUnavailable:
		return "orphan reaper: leader start token is unavailable; no signal sent"
	case processClaimStartTokenMismatch:
		return "orphan reaper: leader start token mismatch; no signal sent"
	default:
		return ""
	}
}

func (diagnostic processClaimDiagnostic) liveness() protocol.Liveness {
	switch diagnostic.kind {
	case processClaimExact:
		return protocol.LivenessAlive
	case processClaimUnavailable, processClaimStartTokenMismatch:
		return protocol.LivenessGone
	default:
		return protocol.LivenessUnknown
	}
}

// terminateRecordedProcessClaim is the reaper's only signaling path. It
// proves exact live-leader token equality immediately before TERM and again
// before KILL. Any missing, unreadable, or changed identity is an uncertainty
// result and deliberately sends no group signal.
func (s *Server) terminateRecordedProcessClaim(claim *jobstore.ProcessClaim) (protocol.Cleanup, []string) {
	if diagnostic := s.exactClaimDiagnostic(claim); !diagnostic.exact() {
		return protocol.CleanupUncertain, []string{diagnostic.message()}
	}
	if gone, err := s.processGroupGone(claim.PGID); err != nil {
		return protocol.CleanupUncertain, []string{"orphan reaper: inspect process group: " + err.Error()}
	} else if gone {
		return protocol.CleanupClean, nil
	}
	// Inspecting group existence is not an identity proof. Repeat the exact
	// leader-token comparison immediately before the first signal so a PID
	// recycled in that small observation window is never targeted.
	if diagnostic := s.exactClaimDiagnostic(claim); !diagnostic.exact() {
		return protocol.CleanupUncertain, []string{diagnostic.message()}
	}

	if err := s.processGroupSignaler().SignalProcessGroup(claim.PGID, syscall.SIGTERM); err != nil {
		return protocol.CleanupUncertain, []string{"orphan reaper: send SIGTERM to exact claimed group: " + err.Error()}
	}
	if gone, err := s.waitForProcessGroupGone(claim.PGID, s.processGroupCancellationGrace()); err != nil {
		return protocol.CleanupUncertain, []string{"orphan reaper: verify SIGTERM cleanup: " + err.Error()}
	} else if gone {
		return protocol.CleanupClean, nil
	}

	if diagnostic := s.exactClaimDiagnostic(claim); !diagnostic.exact() {
		return protocol.CleanupUncertain, []string{diagnostic.message()}
	}
	if err := s.processGroupSignaler().SignalProcessGroup(claim.PGID, syscall.SIGKILL); err != nil {
		return protocol.CleanupUncertain, []string{"orphan reaper: send SIGKILL to exact claimed group: " + err.Error()}
	}
	// The grace elapsed while waiting after TERM. A KILL sent afterward lowers
	// the orphan risk but cannot establish clean cleanup inside that grace.
	return protocol.CleanupUncertain, []string{"orphan reaper: process group did not exit within cancellation grace"}
}

// recordedClaimLiveness derives the public verdict from the same exact
// identity comparison used by the reaper; it never exposes the claim itself.
func (s *Server) recordedClaimLiveness(claim *jobstore.ProcessClaim) protocol.Liveness {
	return s.exactClaimDiagnostic(claim).liveness()
}

func (s *Server) exactClaimDiagnostic(claim *jobstore.ProcessClaim) processClaimDiagnostic {
	if claim == nil {
		return processClaimDiagnostic{kind: processClaimMissing}
	}
	if claim.PID <= 0 || claim.PGID <= 0 || claim.StartToken == "" {
		return processClaimDiagnostic{kind: processClaimIncomplete}
	}
	table := s.processTable
	if table == nil {
		table = engine.NativeProcessTable{}
	}
	info, alive, err := table.Lookup(claim.PID)
	if err != nil {
		return processClaimDiagnostic{kind: processClaimUnreadable, err: err}
	}
	if !alive {
		return processClaimDiagnostic{kind: processClaimUnavailable}
	}
	if info.StartTime == "" {
		return processClaimDiagnostic{kind: processClaimStartTokenUnavailable}
	}
	if info.StartTime != claim.StartToken {
		return processClaimDiagnostic{kind: processClaimStartTokenMismatch}
	}
	return processClaimDiagnostic{kind: processClaimExact}
}

func (s *Server) processGroupSignaler() engine.ProcessGroupSignaler {
	if s != nil && s.processGroups != nil {
		return s.processGroups
	}
	return engine.NativeProcessGroupSignaler{}
}

func (s *Server) processGroupCancellationGrace() time.Duration {
	if s != nil && s.processGroupGrace > 0 {
		return s.processGroupGrace
	}
	return engine.DefaultCancelGrace
}

func (s *Server) processGroupGone(pgid int) (bool, error) {
	if pgid <= 0 {
		return false, errors.New("process group id is invalid")
	}
	if s != nil && s.processGroupGoneFn != nil {
		return s.processGroupGoneFn(pgid)
	}
	err := syscall.Kill(-pgid, 0)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	return false, err
}

func (s *Server) waitForProcessGroupGone(pgid int, grace time.Duration) (bool, error) {
	if grace <= 0 {
		grace = engine.DefaultCancelGrace
	}
	deadline := time.Now().Add(grace)
	for {
		gone, err := s.processGroupGone(pgid)
		if err != nil || gone {
			return gone, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		if remaining > processGroupPollInterval {
			remaining = processGroupPollInterval
		}
		time.Sleep(remaining)
	}
}
