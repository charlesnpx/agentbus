//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
)

// activeExecution is deliberately small. The retained adapter and
// command.DirectCommandRunner own process-group creation, TERM, bounded wait,
// and KILL; service owns only one job context, its deadline, and the durable
// job lifecycle around that turn.
type activeExecution struct {
	jobID   string
	backend engine.Backend

	mu             sync.Mutex
	cancel         context.CancelFunc
	session        engine.Session
	claimAttempted bool
	claimRecorded  bool
	claimErr       error
	interruptOnce  sync.Once
	interruptErr   error
}

func newActiveExecution(jobID string, backend engine.Backend) *activeExecution {
	return &activeExecution{jobID: jobID, backend: backend}
}

func (run *activeExecution) setCancel(cancel context.CancelFunc) {
	run.mu.Lock()
	run.cancel = cancel
	run.mu.Unlock()
}

func (run *activeExecution) setSession(session engine.Session) {
	run.mu.Lock()
	run.session = session
	run.mu.Unlock()
}

// beginTurn resets process-claim tracking only after the preceding turn has
// retired. Each retained adapter turn owns exactly one process and therefore
// has exactly one separate durable claim transaction.
func (run *activeExecution) beginTurn() {
	run.mu.Lock()
	run.claimAttempted = false
	run.claimRecorded = false
	run.claimErr = nil
	run.mu.Unlock()
}

func (run *activeExecution) interrupt() error {
	run.interruptOnce.Do(func() {
		run.mu.Lock()
		session := run.session
		run.mu.Unlock()
		if session == nil {
			return
		}
		// A canceled job context cannot carry the containment request. The
		// session's retained direct runner applies its own TERM/KILL grace.
		interruptCtx, cancel := context.WithTimeout(context.Background(), 2*engine.DefaultCancelGrace)
		defer cancel()
		err := session.Interrupt(interruptCtx)
		run.mu.Lock()
		run.interruptErr = err
		run.mu.Unlock()
	})
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.interruptErr
}

func (run *activeExecution) recordProcessClaim(store *jobstore.Store, ref engine.ProcessRef) {
	run.mu.Lock()
	if run.claimAttempted {
		run.claimErr = errors.New("backend reported more than one process claim for one turn")
		run.mu.Unlock()
		run.containAfterClaimFailure()
		return
	}
	run.claimAttempted = true
	run.mu.Unlock()

	claim := jobstore.ProcessClaim{PID: ref.PID, PGID: ref.PGID, StartToken: ref.StartTime}
	_, err := store.RecordProcessClaim(run.jobID, claim)

	run.mu.Lock()
	if err == nil {
		run.claimRecorded = true
	} else {
		run.claimErr = err
	}
	run.mu.Unlock()
	if err != nil {
		run.containAfterClaimFailure()
	}
}

func (run *activeExecution) containAfterClaimFailure() {
	run.mu.Lock()
	cancel := run.cancel
	session := run.session
	run.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if session != nil {
		go func() { _ = run.interrupt() }()
	}
}

func (run *activeExecution) claimStatus() (attempted, recorded bool, err error) {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.claimAttempted, run.claimRecorded, run.claimErr
}

// beginExecutions establishes the daemon-owned parent context for jobs newly
// admitted during this Serve lifetime. It intentionally does not enumerate
// queued records: restart reconciliation belongs to U9 and ADR-14 forbids
// relaunching recovered work.
func (s *Server) beginExecutions(parent context.Context) {
	if s == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	s.executionMu.Lock()
	if s.executionCtx != nil {
		s.executionMu.Unlock()
		cancel()
		return
	}
	s.executionCtx = ctx
	s.executionCancel = cancel
	if s.executions == nil {
		s.executions = make(map[string]*activeExecution)
	}
	s.executionMu.Unlock()
}

// stopExecutions prevents new launches, cancels each per-job context through
// the shared parent, and waits for the retained adapter to finish cleanup
// before the jobstore may close.
func (s *Server) stopExecutions() {
	if s == nil {
		return
	}
	s.executionMu.Lock()
	cancel := s.executionCancel
	s.executionCtx = nil
	s.executionCancel = nil
	s.executionMu.Unlock()
	if cancel != nil {
		cancel()
	}
	s.executionWG.Wait()
}

// enqueueQueuedJob starts exactly one newly admitted queued record while the
// daemon is serving. A recovered queued record is intentionally never passed
// here.
func (s *Server) enqueueQueuedJob(record jobstore.Record, backend engine.Backend) {
	if s == nil || record.State != protocol.PublicStateQueued || record.JobID == "" {
		return
	}
	s.executionMu.Lock()
	if s.executionCtx == nil || s.executionCtx.Err() != nil {
		s.executionMu.Unlock()
		return
	}
	if _, exists := s.executions[record.JobID]; exists {
		s.executionMu.Unlock()
		return
	}
	run := newActiveExecution(record.JobID, backend)
	s.executions[record.JobID] = run
	parent := s.executionCtx
	s.executionWG.Add(1)
	s.activeJobs.Add(1)
	s.executionMu.Unlock()

	go func() {
		defer s.executionWG.Done()
		defer s.activeJobs.Add(-1)
		defer s.touchActivity()
		s.runJob(parent, record, run)
		s.executionMu.Lock()
		if s.executions[record.JobID] == run {
			delete(s.executions, record.JobID)
		}
		s.executionMu.Unlock()
	}()
}

// runJob establishes the initial turn. Its first store mutation is
// MarkStarting, which commits before Backend.Start and Session.Turn can spawn
// a provider process. The process-claim callback runs only after the retained
// adapter has forked and execed into its own process group.
func (s *Server) runJob(parent context.Context, record jobstore.Record, run *activeExecution) {
	if parent == nil {
		parent = context.Background()
	}
	store, err := s.ensureJobStore()
	if err != nil {
		log.Printf("agentbus service: job %s cannot open store for execution: %v", record.JobID, err)
		return
	}

	spec, err := taskSpecFromRecord(record)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, err, nil)
		return
	}
	resolution, timeoutErr := timeoutFromMillis(spec.TimeoutMS)
	if timeoutErr != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New(timeoutErr.Message), nil)
		return
	}
	// Derived from the resolution rather than returned alongside it, so the
	// effective timeout has exactly one source of truth.
	timeout := time.Duration(resolution.Effective) * time.Millisecond

	jobCtx, cancel := context.WithCancel(parent)
	if timeout > 0 {
		jobCtx, cancel = context.WithTimeout(parent, timeout)
	}
	run.setCancel(cancel)
	defer cancel()

	// This durable transaction is intentionally separate from the later claim
	// transaction in activeExecution.recordProcessClaim.
	started, err := store.MarkStarting(record.JobID)
	if err != nil {
		// A failed or ambiguous starting commit must leave the record alone and
		// must never license a spawn.
		log.Printf("agentbus service: job %s starting commit failed; backend was not launched: %v", record.JobID, err)
		return
	}
	record = started

	logPaths, err := ensureJobLogFiles(record)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare backend logs: %w", err), nil)
		return
	}

	if run.backend == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable, fmt.Errorf("backend %q is unavailable", record.Backend), nil)
		return
	}
	session, err := run.backend.Start(jobCtx, engine.SessionOpts{
		CWD:     record.CWD,
		Write:   record.Write,
		Model:   record.Model,
		Effort:  record.Effort,
		Timeout: timeout,
	})
	if err != nil {
		s.finishStartError(store, record, jobCtx, err)
		return
	}
	if session == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New("backend returned a nil session"), nil)
		return
	}
	run.setSession(session)

	if err := jobCtx.Err(); err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, err)
		s.finishContextStop(store, record, err, cleanup, diagnostics)
		return
	}

	run.beginTurn()
	events, err := session.Turn(jobCtx, engine.TurnInput{
		Prompt:   spec.Prompt,
		Write:    spec.Write,
		Timeout:  timeout,
		LogPaths: logPaths,
		OnProcessStart: func(ref engine.ProcessRef, _ int) {
			run.recordProcessClaim(store, ref)
		},
	})
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, jobCtx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		s.finishTurnError(store, record, jobCtx, err, cleanup, diagnostics)
		return
	}

	outcome := collectTurn(jobCtx, run, events)
	if outcome.streamClosed {
		if err := discardEmptyBackendLogs(logPaths); err != nil {
			outcome.cleanup = protocol.CleanupUncertain
			outcome.diagnostics = append(outcome.diagnostics, "discard empty backend logs: "+err.Error())
		}
	}
	outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)

	if errors.Is(outcome.err, context.DeadlineExceeded) || outcome.timedOut {
		s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, outcome.diagnostics)
		return
	}
	if errors.Is(outcome.err, context.Canceled) || outcome.interrupted {
		_, _, claimErr := run.claimStatus()
		if claimErr != nil {
			s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassInternal, claimErr, outcome.diagnostics)
			return
		}
		s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassInterrupted, outcome.err, outcome.diagnostics)
		return
	}
	if outcome.err != nil {
		s.recordExecutionFailure(store, record, outcome.cleanup, classifyExecutionFailure(outcome.err), outcome.err, outcome.diagnostics)
		return
	}
	text, contract, cleanup, diagnostics := s.evaluateOutputSchema(store, record, run, session, jobCtx, timeout, spec, logPaths, outcome.text, outcome.cleanup, outcome.diagnostics)
	s.recordExecutionCompletion(store, record, text, cleanup, diagnostics, contract)
}

func taskSpecFromRecord(record jobstore.Record) (protocol.TaskSpecV3, error) {
	var spec protocol.TaskSpecV3
	if len(record.TaskSpec) == 0 {
		return spec, errors.New("durable task spec is missing")
	}
	if err := json.Unmarshal(record.TaskSpec, &spec); err != nil {
		return spec, fmt.Errorf("decode durable task spec: %w", err)
	}
	if spec.Backend != record.Backend || spec.CWD == "" || spec.Prompt == "" {
		return spec, errors.New("durable task spec disagrees with job record")
	}
	return spec, nil
}

// evaluateOutputSchema runs only after the initial turn has successfully
// retired and before the job's sole terminal write. A correction failure is a
// contract failure, not a reason to discard the already authoritative result.
func (s *Server) evaluateOutputSchema(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, spec protocol.TaskSpecV3, logPaths engine.LogPaths, original string, cleanup protocol.Cleanup, diagnostics []string) (string, protocol.ContractResult, protocol.Cleanup, []string) {
	if len(spec.OutputSchema) == 0 {
		return original, protocol.ContractResult{}, cleanup, diagnostics
	}

	digest, err := schema.Digest(spec.OutputSchema)
	if err != nil {
		return original, protocol.ContractResult{
			Evaluated:  true,
			Compliant:  false,
			Attempts:   1,
			Violations: []string{"schema digest: " + executionFailureReason(err)},
		}, cleanup, append(diagnostics, "evaluate output schema: "+executionFailureReason(err))
	}
	initial, err := schema.Validate(original, spec.OutputSchema)
	contract := protocol.ContractResult{
		SchemaSHA256: digest,
		Evaluated:    initial.Evaluated,
		Compliant:    initial.Compliant,
		Attempts:     1,
		Violations:   append([]string(nil), initial.Violations...),
	}
	if err != nil {
		contract.Evaluated = true
		contract.Compliant = false
		contract.Violations = []string{"schema validation: " + executionFailureReason(err)}
		return original, contract, cleanup, append(diagnostics, "evaluate output schema: "+executionFailureReason(err))
	}
	if contract.Compliant {
		return original, contract, cleanup, diagnostics
	}
	if err := ctx.Err(); err != nil {
		return original, contract, cleanup, diagnostics
	}
	initialClaimAttempted, initialClaimRecorded, initialClaimErr := run.claimStatus()
	if initialClaimErr != nil || !initialClaimAttempted || !initialClaimRecorded {
		return original, contract, cleanup, diagnostics
	}

	// collectTurn returns only after the initial event stream closes. The
	// retained adapter clears that turn's active process before closing its
	// stream, so the correction's claim is recorded after the initial process
	// has retired.
	correction := s.runCorrectionTurn(store, record, run, session, ctx, timeout, logPaths, schema.CorrectionPrompt(string(spec.OutputSchema), initial.Violations))
	if correction.cleanup == protocol.CleanupUncertain {
		cleanup = protocol.CleanupUncertain
	}
	diagnostics = append(diagnostics, correction.diagnostics...)
	contract.Attempts = 2
	if correction.err != nil || correction.timedOut || correction.interrupted {
		return original, contract, cleanup, diagnostics
	}
	if _, _, claimErr := run.claimStatus(); claimErr != nil {
		return original, contract, cleanup, diagnostics
	}

	corrected, err := schema.Validate(correction.text, spec.OutputSchema)
	if err != nil || !corrected.Compliant {
		return original, contract, cleanup, diagnostics
	}
	contract.Evaluated = corrected.Evaluated
	contract.Compliant = corrected.Compliant
	contract.Violations = append([]string(nil), corrected.Violations...)
	return correction.text, contract, cleanup, diagnostics
}

// runCorrectionTurn starts the sole read-only correction turn after the
// initial turn has retired.
func (s *Server) runCorrectionTurn(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, logPaths engine.LogPaths, prompt string) turnOutcome {
	run.beginTurn()
	events, err := session.Turn(ctx, engine.TurnInput{
		Prompt:   prompt,
		Write:    false,
		Timeout:  timeout,
		LogPaths: logPaths,
		OnProcessStart: func(ref engine.ProcessRef, _ int) {
			run.recordProcessClaim(store, ref)
		},
	})
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, ctx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		return turnOutcome{err: err, cleanup: cleanup, diagnostics: diagnostics}
	}

	outcome := collectTurn(ctx, run, events)
	if outcome.streamClosed {
		if err := discardEmptyBackendLogs(logPaths); err != nil {
			outcome.cleanup = protocol.CleanupUncertain
			outcome.diagnostics = append(outcome.diagnostics, "discard empty backend logs: "+err.Error())
		}
	}
	outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
	return outcome
}

func (s *Server) finishStartError(store *jobstore.Store, record jobstore.Record, ctx context.Context, err error) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassTimeout, context.DeadlineExceeded, nil)
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInterrupted, ctx.Err(), nil)
		return
	}
	s.recordExecutionFailure(store, record, protocol.CleanupClean, classifyStartFailure(err), err, nil)
}

func (s *Server) finishTurnError(store *jobstore.Store, record jobstore.Record, ctx context.Context, err error, cleanup protocol.Cleanup, diagnostics []string) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, diagnostics)
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassInterrupted, ctx.Err(), diagnostics)
		return
	}
	s.recordExecutionFailure(store, record, cleanup, classifyExecutionFailure(err), err, diagnostics)
}

func (s *Server) finishContextStop(store *jobstore.Store, record jobstore.Record, err error, cleanup protocol.Cleanup, diagnostics []string) {
	if errors.Is(err, context.DeadlineExceeded) {
		s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, diagnostics)
		return
	}
	s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassInterrupted, err, diagnostics)
}

func classifyStartFailure(err error) protocol.FailureClass {
	if errors.Is(err, engine.ErrProviderOverloaded) {
		return protocol.FailureClassProviderOverloaded
	}
	return protocol.FailureClassBackendUnavailable
}

func classifyExecutionFailure(err error) protocol.FailureClass {
	switch {
	case errors.Is(err, engine.ErrProviderOverloaded):
		return protocol.FailureClassProviderOverloaded
	case errors.Is(err, context.DeadlineExceeded):
		return protocol.FailureClassTimeout
	case errors.Is(err, context.Canceled), errors.Is(err, engine.ErrTurnInterrupted):
		return protocol.FailureClassInterrupted
	default:
		return protocol.FailureClassBackendError
	}
}

type turnOutcome struct {
	text         string
	err          error
	timedOut     bool
	interrupted  bool
	cleanup      protocol.Cleanup
	diagnostics  []string
	streamClosed bool
}

func collectTurn(ctx context.Context, run *activeExecution, events <-chan engine.Event) turnOutcome {
	outcome := turnOutcome{cleanup: protocol.CleanupClean}
	if events == nil {
		outcome.err = errors.New("backend returned a nil event stream")
		return outcome
	}
	var assistant strings.Builder
	var result string
	hasResult := false
	for {
		select {
		case <-ctx.Done():
			if err := run.interrupt(); err != nil {
				outcome.cleanup = protocol.CleanupUncertain
				outcome.diagnostics = append(outcome.diagnostics, "backend cleanup: "+err.Error())
			}
			if drainTurnEvents(events, &outcome, &assistant, &result, &hasResult) {
				outcome.text = attemptFinalText(hasResult, result, assistant.String())
				outcome.streamClosed = true
			} else {
				outcome.cleanup = protocol.CleanupUncertain
				outcome.diagnostics = append(outcome.diagnostics, "backend event stream did not close after containment grace")
				discardTurnEvents(events)
			}
			outcome.err = ctx.Err()
			return outcome
		case event, ok := <-events:
			if !ok {
				outcome.text = attemptFinalText(hasResult, result, assistant.String())
				outcome.streamClosed = true
				return outcome
			}
			absorbTurnEvent(&outcome, &assistant, &result, &hasResult, event)
		}
	}
}

func drainTurnEvents(events <-chan engine.Event, outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool) bool {
	timer := time.NewTimer(engine.DefaultCancelGrace)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return true
			}
			absorbTurnEvent(outcome, assistant, result, hasResult, event)
		case <-timer.C:
			return false
		}
	}
}

func absorbTurnEvent(outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool, event engine.Event) {
	rawText := authoritativeText(event)
	switch event.Type {
	case engine.EventAgentText:
		assistant.WriteString(rawText)
	case engine.EventResultMessage:
		*result = rawText
		*hasResult = true
	case engine.EventTerminalError:
		if event.Err != nil {
			outcome.err = event.Err
		} else if strings.TrimSpace(rawText) != "" {
			outcome.err = errors.New(rawText)
		} else {
			outcome.err = errors.New("backend failed")
		}
	case engine.EventTurnFinal:
		if event.TurnFinal == nil {
			return
		}
		if event.TurnFinal.CleanupFailed {
			outcome.cleanup = protocol.CleanupUncertain
			outcome.diagnostics = append(outcome.diagnostics, "backend reported uncertain cleanup")
		}
		if event.TurnFinal.TimedOut {
			outcome.timedOut = true
		}
		if event.TurnFinal.Canceled {
			outcome.interrupted = true
		}
		if event.TurnFinal.ExecutionFailed && outcome.err == nil {
			outcome.err = errors.New("backend execution failed")
		}
	}
}

func discardTurnEvents(events <-chan engine.Event) {
	if events == nil {
		return
	}
	go func() {
		for range events {
		}
	}()
}

func cleanupAfterContextStop(run *activeExecution, contextErr error) (protocol.Cleanup, []string) {
	if contextErr == nil {
		return protocol.CleanupClean, nil
	}
	if err := run.interrupt(); err != nil {
		return protocol.CleanupUncertain, []string{"backend cleanup: " + err.Error()}
	}
	return protocol.CleanupClean, nil
}

func cleanupForRun(run *activeExecution, cleanup protocol.Cleanup, diagnostics []string) (protocol.Cleanup, []string) {
	attempted, recorded, claimErr := run.claimStatus()
	if claimErr != nil {
		cleanup = protocol.CleanupUncertain
		diagnostics = append(diagnostics, "process claim: "+claimErr.Error())
		return cleanup, diagnostics
	}
	if !attempted || !recorded {
		cleanup = protocol.CleanupUncertain
		diagnostics = append(diagnostics, "process claim was not recorded")
	}
	return cleanup, diagnostics
}

func (s *Server) recordExecutionCompletion(store *jobstore.Store, record jobstore.Record, text string, cleanup protocol.Cleanup, diagnostics []string, contract protocol.ContractResult) {
	info, err := spillAuthoritativeResult(record, []byte(text))
	if err != nil {
		s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassInternal, fmt.Errorf("spill authoritative result: %w", err), diagnostics)
		return
	}
	resultText := info.Text
	if info.TextElided {
		resultText = ""
	}
	s.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:       protocol.PublicStateCompleted,
		Cleanup:     cleanup,
		Diagnostics: diagnostics,
		Contract:    contract,
		ResultText:  resultText,
		FinishedAt:  time.Now().UTC(),
	})
}

func (s *Server) recordExecutionFailure(store *jobstore.Store, record jobstore.Record, cleanup protocol.Cleanup, class protocol.FailureClass, cause error, diagnostics []string) {
	if !class.Valid() {
		class = protocol.FailureClassInternal
	}
	s.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:         protocol.PublicStateFailed,
		Cleanup:       cleanup,
		FailureClass:  class,
		FailureReason: executionFailureReason(cause),
		Diagnostics:   diagnostics,
		FinishedAt:    time.Now().UTC(),
	})
}

func (s *Server) markTerminal(store *jobstore.Store, jobID string, terminal jobstore.TerminalUpdate) {
	_, err := store.MarkTerminal(jobID, terminal)
	if err == nil || errors.Is(err, jobstore.ErrTerminal) {
		// ErrTerminal is the store-enforced first-terminal-wins result. A
		// later observer never retries or overwrites that recorded outcome.
		return
	}
	log.Printf("agentbus service: job %s terminal write failed: %v", jobID, err)
}

func executionFailureReason(err error) string {
	if err == nil || strings.TrimSpace(err.Error()) == "" {
		return "backend failed"
	}
	const maxRunes = engine.FailureReasonMaxRunes
	value := strings.TrimSpace(err.Error())
	if len([]rune(value)) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes-3]) + "..."
}

func ensureJobLogFiles(record jobstore.Record) (engine.LogPaths, error) {
	if strings.TrimSpace(record.Artifacts.Log) == "" {
		return engine.LogPaths{}, errors.New("job log artifact path is missing")
	}
	paths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		return engine.LogPaths{}, err
	}
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return engine.LogPaths{}, err
		}
		if err := os.Chmod(filepath.Dir(path), 0o700); err != nil {
			return engine.LogPaths{}, err
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return engine.LogPaths{}, err
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			return engine.LogPaths{}, err
		}
		if err := file.Close(); err != nil {
			return engine.LogPaths{}, err
		}
	}
	return paths, nil
}

func spillAuthoritativeResult(record jobstore.Record, raw []byte) (engine.ResultInfo, error) {
	if strings.TrimSpace(record.Artifacts.Result) == "" {
		return engine.ResultInfo{}, errors.New("job result artifact path is missing")
	}
	layout := engine.WorkspaceLayout{Results: filepath.Dir(record.Artifacts.Result)}
	info, err := engine.WriteResultForLayout(layout, record.JobID, raw, engine.DefaultInlineResultCap)
	if err != nil {
		return engine.ResultInfo{}, err
	}
	if filepath.Clean(info.ResultPath) != filepath.Clean(record.Artifacts.Result) {
		return engine.ResultInfo{}, errors.New("result helper resolved a path outside the durable job artifact")
	}
	return info, nil
}

func discardEmptyBackendLogs(paths engine.LogPaths) error {
	for _, path := range []string{paths.Stdout, paths.Stderr} {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backend log %q is not a regular file", path)
		}
		if info.Size() == 0 {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func attemptFinalText(hasResultMessage bool, resultText, assistantText string) string {
	if hasResultMessage {
		return resultText
	}
	return assistantText
}

func authoritativeText(event engine.Event) string {
	if event.RawText != "" {
		return event.RawText
	}
	return event.Text
}
