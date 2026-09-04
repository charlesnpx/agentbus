//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	// launchMu linearizes a cancellation request with the two calls that may
	// cross a process-launch boundary. A request that acquires it first keeps a
	// queued/starting job from subsequently reaching Backend.Start or
	// Session.Turn before its durable cancellation is committed.
	launchMu        sync.Mutex
	mu              sync.Mutex
	cancel          context.CancelFunc
	session         engine.Session
	cancelRequested bool
	claimAttempted  bool
	claimRecorded   bool
	claimErr        error
	interruptOnce   sync.Once
	interruptErr    error
	itemCount       int
	lastItemAt      time.Time
	lastActivityAt  time.Time
	itemSidecarDiag string
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

func (run *activeExecution) cancellationRequested() bool {
	if run == nil {
		return false
	}
	run.mu.Lock()
	requested := run.cancelRequested
	run.mu.Unlock()
	return requested
}

// requestCancellation prevents a future launch before returning. If a session
// already exists, it also invokes its retained, identity-fenced interrupt
// path. The caller still has to persist MarkTerminal: this flag is only the
// short in-process gate that closes the launch/cancel race.
func (run *activeExecution) requestCancellation() (sessionPresent bool, err error) {
	if run == nil {
		return false, nil
	}
	run.launchMu.Lock()
	run.mu.Lock()
	run.cancelRequested = true
	cancel := run.cancel
	sessionPresent = run.session != nil
	run.mu.Unlock()
	run.launchMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if !sessionPresent {
		return false, nil
	}
	return true, run.interrupt()
}

// startSession serializes Backend.Start with requestCancellation. It reports a
// pending cancellation separately so runJob never converts that request into
// a competing interrupted/failed terminal record.
func (run *activeExecution) startSession(ctx context.Context, opts engine.SessionOpts) (engine.Session, error, bool) {
	run.launchMu.Lock()
	if run.cancellationRequested() {
		run.launchMu.Unlock()
		return nil, context.Canceled, true
	}
	session, err := run.backend.Start(ctx, opts)
	if session != nil {
		run.setSession(session)
	}
	canceled := run.cancellationRequested()
	run.launchMu.Unlock()
	if canceled && session != nil {
		_ = run.interrupt()
	}
	return session, err, canceled
}

// startTurn applies the same launch gate to Session.Turn, which is where the
// retained adapters fork and exec their provider process groups.
func (run *activeExecution) startTurn(session engine.Session, ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error, bool) {
	run.launchMu.Lock()
	if run.cancellationRequested() {
		run.launchMu.Unlock()
		return nil, context.Canceled, true
	}
	events, err := session.Turn(ctx, input)
	canceled := run.cancellationRequested()
	run.launchMu.Unlock()
	if canceled {
		_ = run.interrupt()
	}
	return events, err, canceled
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
	run.mu.Lock()
	session := run.session
	run.mu.Unlock()
	if session == nil {
		// Do not consume interruptOnce before Start publishes a session. A
		// cancel may arrive in that short interval, and the later session must
		// still receive the fenced interruption.
		return nil
	}
	run.interruptOnce.Do(func() {
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
	defer s.closeManagedCodexHome(record.JobID)
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
	if run.cancellationRequested() {
		return
	}

	// This durable transaction is intentionally separate from the later claim
	// transaction in activeExecution.recordProcessClaim.
	started, err := store.MarkStarting(record.JobID)
	if err != nil {
		// A failed or ambiguous starting commit must never license a spawn. Try
		// to make that failure durable so the job remains visible; markTerminal
		// logs and leaves it alone if that write also fails.
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("commit starting transition: %w", err), nil)
		log.Printf("agentbus service: job %s starting commit failed; backend was not launched: %v", record.JobID, err)
		return
	}
	record = started
	if run.cancellationRequested() {
		return
	}

	if strings.TrimSpace(record.Artifacts.Log) == "" {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New("job log artifact path is missing"), nil)
		return
	}
	logPaths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("resolve backend logs: %w", err), nil)
		return
	}

	if run.backend == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable, fmt.Errorf("backend %q is unavailable", record.Backend), nil)
		return
	}
	opts := engine.SessionOpts{
		CWD:     record.CWD,
		Write:   record.Write,
		Model:   record.Model,
		Effort:  record.Effort,
		Timeout: timeout,
	}
	if record.Backend == "codex" {
		layout, err := engine.LayoutForWorkspace(s.stateRoot, record.CWD)
		if err != nil {
			s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare Codex workspace layout: %w", err), nil)
			return
		}
		codexHome, managed, err := s.prepareCodexHome(layout, record.JobID)
		if managed != nil {
			s.rememberManagedCodexHome(record.JobID, managed)
		}
		if err != nil {
			s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare Codex home: %w", err), nil)
			return
		}
		if codexHome != "" {
			opts.EnvOverlay = map[string]string{"CODEX_HOME": codexHome}
		}
		if record.Write {
			cache, managed, cacheErr := prepareCodexJobCache(layout, record.JobID, managed)
			if managed != nil {
				s.rememberManagedCodexHome(record.JobID, managed)
			}
			if cacheErr != nil {
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare Codex job cache: %w", cacheErr), nil)
				return
			}
			opts.WriteSandboxRoot = cache.root
			// Override unconditionally: inherited host cache and temporary paths are
			// outside the write sandbox, which is the condition this cache fixes.
			opts.WriteEnvOverlay = map[string]string{
				"GOCACHE":    cache.goBuild,
				"GOMODCACHE": cache.goMod,
				"TMPDIR":     cache.tmp,
			}
		}
	}
	session, err, canceled := run.startSession(jobCtx, opts)
	if canceled {
		return
	}
	if err != nil {
		s.finishStartError(store, record, jobCtx, err)
		return
	}
	if session == nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, errors.New("backend returned a nil session"), nil)
		return
	}
	if err := jobCtx.Err(); err != nil {
		if run.cancellationRequested() {
			return
		}
		cleanup, diagnostics := cleanupAfterContextStop(run, err)
		s.finishContextStop(store, record, err, cleanup, diagnostics)
		return
	}
	itemPath, err := itemSidecarPath(logPaths.Stdout)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("derive item sidecar path: %w", err), nil)
		return
	}
	itemWriter := newItemSidecarWriter(itemPath, transcriptItemTextCap, transcriptItemFileCap)
	itemWriter.setFailureSink(run.noteItemSidecarDiagnostic)
	defer itemWriter.close()
	items := newItemAssembler(run, itemWriter)

	run.beginTurn()
	events, err, canceled := run.startTurn(session, jobCtx, engine.TurnInput{
		Prompt:   spec.Prompt,
		Write:    spec.Write,
		Timeout:  timeout,
		LogPaths: logPaths,
		OnProcessStart: func(ref engine.ProcessRef, _ int) {
			run.recordProcessClaim(store, ref)
		},
	})
	if canceled {
		return
	}
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, jobCtx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		diagnostics = appendItemSidecarDiagnostics(diagnostics, itemWriter)
		s.finishTurnError(store, record, jobCtx, err, cleanup, diagnostics)
		return
	}

	outcome := collectTurn(jobCtx, run, events, items)
	outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
	if run.cancellationRequested() {
		return
	}

	if errors.Is(outcome.err, context.DeadlineExceeded) || outcome.timedOut {
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, outcome.diagnostics)
		return
	}
	if errors.Is(outcome.err, context.Canceled) || outcome.interrupted {
		_, _, claimErr := run.claimStatus()
		if claimErr != nil {
			outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
			s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassInternal, claimErr, outcome.diagnostics)
			return
		}
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailure(store, record, outcome.cleanup, protocol.FailureClassInterrupted, outcome.err, outcome.diagnostics)
		return
	}
	if outcome.err != nil {
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailure(store, record, outcome.cleanup, classifyExecutionFailure(outcome.err), outcome.err, outcome.diagnostics)
		return
	}
	text, contract, cleanup, diagnostics := s.evaluateOutputSchema(store, record, run, session, jobCtx, timeout, spec, logPaths, items, outcome.text, outcome.cleanup, outcome.diagnostics)
	diagnostics = appendItemSidecarDiagnostics(diagnostics, itemWriter)
	s.recordExecutionCompletion(store, record, text, cleanup, diagnostics, contract)
}

func taskSpecFromRecord(record jobstore.Record) (protocol.TaskSpec, error) {
	var spec protocol.TaskSpec
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
func (s *Server) evaluateOutputSchema(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, spec protocol.TaskSpec, logPaths engine.LogPaths, items *itemAssembler, original string, cleanup protocol.Cleanup, diagnostics []string) (string, protocol.ContractResult, protocol.Cleanup, []string) {
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
	correction := s.runCorrectionTurn(store, record, run, session, ctx, timeout, logPaths, items, schema.CorrectionPrompt(string(spec.OutputSchema), initial.Violations))
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
func (s *Server) runCorrectionTurn(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, logPaths engine.LogPaths, items *itemAssembler, prompt string) turnOutcome {
	run.beginTurn()
	events, err, canceled := run.startTurn(session, ctx, engine.TurnInput{
		Prompt:   prompt,
		Write:    false,
		Timeout:  timeout,
		LogPaths: logPaths,
		OnProcessStart: func(ref engine.ProcessRef, _ int) {
			run.recordProcessClaim(store, ref)
		},
	})
	if canceled {
		return turnOutcome{err: context.Canceled, cleanup: protocol.CleanupClean}
	}
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, ctx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		return turnOutcome{err: err, cleanup: cleanup, diagnostics: diagnostics}
	}

	outcome := collectTurn(ctx, run, events, items)
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
	if errors.Is(err, context.DeadlineExceeded) {
		return protocol.FailureClassTimeout
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrTurnInterrupted) {
		return protocol.FailureClassInterrupted
	}
	if errors.Is(err, engine.ErrProviderOverloaded) {
		return protocol.FailureClassProviderOverloaded
	}
	return classifyBackendFailureText(err)
}

type turnOutcome struct {
	text        string
	err         error
	timedOut    bool
	interrupted bool
	cleanup     protocol.Cleanup
	diagnostics []string
}

func collectTurn(ctx context.Context, run *activeExecution, events <-chan engine.Event, items *itemAssembler) turnOutcome {
	outcome := turnOutcome{cleanup: protocol.CleanupClean}
	defer items.finishTurn()
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
			if drainTurnEvents(events, &outcome, &assistant, &result, &hasResult, items) {
				outcome.text = attemptFinalText(hasResult, result, assistant.String())
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
				return outcome
			}
			absorbTurnEvent(&outcome, &assistant, &result, &hasResult, items, event)
		}
	}
}

func drainTurnEvents(events <-chan engine.Event, outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool, items *itemAssembler) bool {
	timer := time.NewTimer(engine.DefaultCancelGrace)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return true
			}
			absorbTurnEvent(outcome, assistant, result, hasResult, items, event)
		case <-timer.C:
			return false
		}
	}
}

func absorbTurnEvent(outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool, items *itemAssembler, event engine.Event) {
	rawText := authoritativeText(event)
	items.absorb(event, rawText)
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
	_, _, claimErr := run.claimStatus()
	if claimErr != nil {
		cleanup = protocol.CleanupUncertain
		diagnostics = append(diagnostics, "process claim: "+claimErr.Error())
		return cleanup, diagnostics
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
		State:        protocol.PublicStateCompleted,
		Cleanup:      cleanup,
		Diagnostics:  diagnostics,
		Contract:     contract,
		ResultText:   resultText,
		ResultPath:   info.ResultPath,
		ResultSHA256: info.SHA256,
		ResultBytes:  info.Bytes,
		FinishedAt:   time.Now().UTC(),
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
	// job.cancel has already reserved this active execution and will commit a
	// canceled first terminal itself. Do not let an asynchronously drained
	// backend stream race it with an interrupted, failed, or completed record.
	if s.cancellationPending(jobID) {
		return
	}
	if home := s.takeManagedCodexHome(jobID); home != nil {
		current, getErr := store.Get(jobID)
		if getErr != nil {
			home.close()
			terminal.Cleanup = protocol.CleanupUncertain
			terminal.Diagnostics = append(terminal.Diagnostics, "inspect managed Codex home terminal record: "+getErr.Error())
		} else if current.State.IsTerminal() {
			home.close()
			return
		} else {
			terminal.Cleanup, terminal.Diagnostics = finalizeManagedCodexHome(home, terminal.State, terminal.Cleanup, terminal.Diagnostics)
		}
	}
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
