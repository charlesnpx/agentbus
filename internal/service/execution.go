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
)

// activeExecution is deliberately small. The retained adapter and
// command.DirectCommandRunner own process-group creation, TERM, bounded wait,
// and KILL; service owns only one job context, its deadline, and the durable
// job lifecycle around that turn.
type activeExecution struct {
	jobID string
	done  chan struct{}

	mu              sync.Mutex
	cancel          context.CancelFunc
	session         engine.Session
	cancelRequested bool
	claimAttempted  bool
	claimRecorded   bool
	claimErr        error
	interruptOnce   sync.Once
	interruptErr    error
}

func newActiveExecution(jobID string) *activeExecution {
	return &activeExecution{jobID: jobID, done: make(chan struct{})}
}

func (run *activeExecution) setCancel(cancel context.CancelFunc) {
	if run == nil {
		return
	}
	run.mu.Lock()
	run.cancel = cancel
	requested := run.cancelRequested
	run.mu.Unlock()
	if requested && cancel != nil {
		cancel()
	}
}

func (run *activeExecution) setSession(session engine.Session) {
	if run == nil {
		return
	}
	run.mu.Lock()
	run.session = session
	requested := run.cancelRequested
	run.mu.Unlock()
	if requested && session != nil {
		go func() { _ = run.interrupt() }()
	}
}

func (run *activeExecution) requestCancel() {
	if run == nil {
		return
	}
	run.mu.Lock()
	run.cancelRequested = true
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

func (run *activeExecution) cancellationRequested() bool {
	if run == nil {
		return false
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.cancelRequested
}

func (run *activeExecution) interrupt() error {
	if run == nil {
		return nil
	}
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
	if run == nil {
		return
	}
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
	if run == nil {
		return
	}
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
	if run == nil {
		return false, false, errors.New("missing active execution")
	}
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
func (s *Server) enqueueQueuedJob(record jobstore.Record) {
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
	run := newActiveExecution(record.JobID)
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

// cancelJob is the lifecycle half of job.cancel. U9 supplies the RPC surface;
// this method contains no transport behavior. For an active job it requests
// cancellation and waits for the owning goroutine to commit its one terminal
// record. For an unowned queued record, it records cancellation directly
// before any launch can occur.
func (s *Server) cancelJob(ctx context.Context, jobID string) (jobstore.Record, error) {
	if s == nil {
		return jobstore.Record{}, errors.New("nil service server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	store, err := s.ensureJobStore()
	if err != nil {
		return jobstore.Record{}, err
	}

	s.executionMu.Lock()
	run := s.executions[jobID]
	s.executionMu.Unlock()
	if run != nil {
		run.requestCancel()
		select {
		case <-run.done:
		case <-ctx.Done():
			return store.Get(jobID)
		}
		return store.Get(jobID)
	}

	record, err := store.Get(jobID)
	if err != nil || record.State.IsTerminal() {
		return record, err
	}
	if record.State != protocol.PublicStateQueued {
		// A nonterminal record without a local owner is a restart/reaper case.
		// Do not guess whether a process still exists or signal it here.
		return record, nil
	}
	terminal, err := store.MarkTerminal(jobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCanceled,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: time.Now().UTC(),
	})
	if errors.Is(err, jobstore.ErrTerminal) {
		return store.Get(jobID)
	}
	return terminal, err
}

// runJob is the only initial-turn launch path. Its first store mutation is
// MarkStarting, which commits before Backend.Start and Session.Turn can spawn
// a provider process. The process-claim callback runs only after the retained
// adapter has forked and execed into its own process group.
func (s *Server) runJob(parent context.Context, record jobstore.Record, run *activeExecution) {
	if run == nil {
		return
	}
	defer close(run.done)
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
	timeout, _, timeoutErr := timeoutFromMillis(spec.TimeoutMS)
	if timeoutErr != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New(timeoutErr.Message), nil)
		return
	}

	jobCtx, cancel := context.WithCancel(parent)
	if timeout > 0 {
		jobCtx, cancel = context.WithTimeout(parent, timeout)
	}
	run.setCancel(cancel)
	defer cancel()

	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, protocol.CleanupClean, nil)
		return
	}

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

	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, protocol.CleanupClean, nil)
		return
	}

	logPaths, err := ensureJobLogFiles(record)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare backend logs: %w", err), nil)
		return
	}

	backend, ok := s.backends[record.Backend]
	if !ok || backend == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable, fmt.Errorf("backend %q is unavailable", record.Backend), nil)
		return
	}
	session, err := backend.Start(jobCtx, engine.SessionOpts{
		CWD:     record.CWD,
		Write:   record.Write,
		Model:   record.Model,
		Effort:  record.Effort,
		Timeout: timeout,
	})
	if err != nil {
		s.finishStartError(store, record, run, jobCtx, err)
		return
	}
	if session == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New("backend returned a nil session"), nil)
		return
	}
	run.setSession(session)

	if err := jobCtx.Err(); err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, err)
		s.finishContextStop(store, record, run, err, cleanup, diagnostics)
		return
	}

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
		s.finishTurnError(store, record, run, jobCtx, err, cleanup, diagnostics)
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

	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, outcome.cleanup, outcome.diagnostics)
		return
	}
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
	s.recordExecutionCompletion(store, record, outcome.text, outcome.cleanup, outcome.diagnostics)
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

func (s *Server) finishStartError(store *jobstore.Store, record jobstore.Record, run *activeExecution, ctx context.Context, err error) {
	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, protocol.CleanupClean, nil)
		return
	}
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

func (s *Server) finishTurnError(store *jobstore.Store, record jobstore.Record, run *activeExecution, ctx context.Context, err error, cleanup protocol.Cleanup, diagnostics []string) {
	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, cleanup, diagnostics)
		return
	}
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

func (s *Server) finishContextStop(store *jobstore.Store, record jobstore.Record, run *activeExecution, err error, cleanup protocol.Cleanup, diagnostics []string) {
	if run.cancellationRequested() {
		s.recordExecutionCanceled(store, record, cleanup, diagnostics)
		return
	}
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

func (s *Server) recordExecutionCompletion(store *jobstore.Store, record jobstore.Record, text string, cleanup protocol.Cleanup, diagnostics []string) {
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

func (s *Server) recordExecutionCanceled(store *jobstore.Store, record jobstore.Record, cleanup protocol.Cleanup, diagnostics []string) {
	s.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:       protocol.PublicStateCanceled,
		Cleanup:     cleanup,
		Diagnostics: diagnostics,
		FinishedAt:  time.Now().UTC(),
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

func discardBackendLogs(paths engine.LogPaths) error {
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
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
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
