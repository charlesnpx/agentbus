//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"github.com/charlesnpx/agentbus/internal/schema"
	"golang.org/x/sys/unix"
)

const lowStateRootSpaceThresholdBytes uint64 = 256 * 1024 * 1024

// activeExecution is deliberately small. The retained adapter and
// command.DirectCommandRunner own process-group creation, TERM, bounded wait,
// and KILL; service owns only one job context, its deadline, and the durable
// job lifecycle around that turn.
type activeExecution struct {
	jobID   string
	backend engine.Backend
	// slotAcquiredAt starts the execution timeout and is fixed when the
	// scheduler reserves a concurrency slot.
	slotAcquiredAt time.Time

	// launchMu linearizes a cancellation request with the two calls that may
	// cross a process-launch boundary. A request that acquires it first keeps a
	// queued/starting job from subsequently reaching Backend.Start or
	// Session.Turn before its durable cancellation is committed.
	launchMu sync.Mutex
	// controlMu is the per-job ordering point for steering, cancellation,
	// schema-correction turns, and the first terminal transition.
	controlMu              sync.Mutex
	mu                     sync.Mutex
	cancel                 context.CancelFunc
	session                engine.Session
	cancelRequested        bool
	claimAttempted         bool
	claimRecorded          bool
	claimErr               error
	processClaim           *jobstore.ProcessClaim
	interruptOnce          sync.Once
	interruptErr           error
	itemCount              int
	lastItemAt             time.Time
	lastActivityAt         time.Time
	itemSidecarDiag        string
	steerDiagnostics       []string
	modelReported          string
	effortReported         string
	turn                   *activeTurn
	latestSessionID        string
	retiredCleanup         protocol.Cleanup
	retiredDiagnostics     []string
	turnActive             bool
	schemaCorrectionActive bool
	steerCanceled          bool
	steerClosed            bool
	steerPending           int
	steerQueue             chan queuedSteer
	steerQueued            map[string]struct{}
	steerInFlightDone      chan struct{}
	steerStop              chan struct{}
	steerDone              chan struct{}
	steerStopOnce          sync.Once
	steerWorkerStarted     bool
}

// activeTurn owns the one retirement receipt for a turn. A cancellation can
// wait on that receipt without racing a later correction turn that replaces
// activeExecution.turn.
type activeTurn struct {
	done    chan struct{}
	once    sync.Once
	outcome turnOutcome
}

func newActiveExecution(jobID string, backend engine.Backend) *activeExecution {
	return &activeExecution{
		jobID:       jobID,
		backend:     backend,
		steerQueue:  make(chan queuedSteer, maxPendingSteers),
		steerQueued: make(map[string]struct{}),
		steerStop:   make(chan struct{}),
		steerDone:   make(chan struct{}),
	}
}

func (run *activeExecution) setCancel(cancel context.CancelFunc) {
	run.mu.Lock()
	run.cancel = cancel
	run.mu.Unlock()
}

func (run *activeExecution) noteSteerDiagnostic(diagnostic string) {
	if run == nil || diagnostic == "" {
		return
	}
	run.mu.Lock()
	run.steerDiagnostics = append(run.steerDiagnostics, diagnostic)
	run.mu.Unlock()
}

func (run *activeExecution) takeSteerDiagnostics() []string {
	if run == nil {
		return nil
	}
	run.mu.Lock()
	diagnostics := append([]string(nil), run.steerDiagnostics...)
	run.steerDiagnostics = nil
	run.mu.Unlock()
	return diagnostics
}

func (run *activeExecution) setSession(session engine.Session) {
	run.mu.Lock()
	run.session = session
	run.mu.Unlock()
}

func (run *activeExecution) noteModelReported(model, effort string) {
	if run == nil {
		return
	}
	model = strings.TrimSpace(model)
	effort = strings.TrimSpace(effort)
	if model == "" && effort == "" {
		return
	}
	run.mu.Lock()
	if model != "" {
		run.modelReported = model
	}
	if effort != "" {
		run.effortReported = effort
	}
	run.mu.Unlock()
}

func (run *activeExecution) reportedSelection() (string, string) {
	if run == nil {
		return "", ""
	}
	run.mu.Lock()
	model := run.modelReported
	effort := run.effortReported
	run.mu.Unlock()
	return model, effort
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

// startSession serializes Backend.Start or Backend.Resume with
// requestCancellation. It reports a pending cancellation separately so runJob
// never converts that request into a competing interrupted/failed terminal
// record.
func (run *activeExecution) startSession(ctx context.Context, opts engine.SessionOpts, resumeID string) (engine.Session, error, bool) {
	run.launchMu.Lock()
	if run.cancellationRequested() {
		run.launchMu.Unlock()
		return nil, context.Canceled, true
	}
	if err := ctx.Err(); err != nil {
		run.launchMu.Unlock()
		return nil, err, false
	}
	var (
		session engine.Session
		err     error
	)
	if resumeID == "" {
		session, err = run.backend.Start(ctx, opts)
	} else {
		session, err = run.backend.Resume(ctx, resumeID, opts)
	}
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
// retained adapters fork and exec their provider process groups. Once an
// adapter reports that its process exists, the gate is released before the
// separate durable claim transaction; a cancellation then uses that in-memory
// claim and waits for this active turn to retire before terminalizing.
func (run *activeExecution) startTurn(store *jobstore.Store, session engine.Session, ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error, bool) {
	run.launchMu.Lock()
	launchLocked := true
	releaseLaunch := func() {
		if launchLocked {
			run.launchMu.Unlock()
			launchLocked = false
		}
	}
	if run.cancellationRequested() {
		releaseLaunch()
		return nil, context.Canceled, true
	}
	input.OnProcessStart = func(ref engine.ProcessRef, _ int) {
		run.noteProcessClaim(ref)
		releaseLaunch()
		run.persistProcessClaim(store)
	}
	events, err := session.Turn(ctx, input)
	canceled := run.cancellationRequested()
	releaseLaunch()
	if err == nil {
		run.controlMu.Lock()
		if !run.steerCanceled && !run.steerClosed {
			run.turnActive = true
		}
		run.controlMu.Unlock()
	}
	if canceled {
		_ = run.interrupt()
	}
	return events, err, canceled
}

// beginTurn resets process-claim tracking only after the preceding turn has
// retired. Each retained adapter turn owns exactly one process and therefore
// has exactly one separate durable claim transaction.
func (run *activeExecution) beginTurn() {
	run.controlMu.Lock()
	run.turnActive = false
	run.controlMu.Unlock()
	run.mu.Lock()
	run.claimAttempted = false
	run.claimRecorded = false
	run.claimErr = nil
	run.processClaim = nil
	run.turn = &activeTurn{done: make(chan struct{})}
	run.mu.Unlock()
}

// retireTurn durably records a non-empty session ID, then publishes the full
// observed result of the current turn before a cancellation commits its
// terminal record. The aggregate keeps facts from earlier turns available after
// a later correction turn replaces the per-turn receipt.
func (run *activeExecution) retireTurn(store *jobstore.Store, outcome turnOutcome) turnOutcome {
	if run == nil {
		return outcome
	}
	run.mu.Lock()
	turn := run.turn
	run.mu.Unlock()
	if turn == nil {
		return outcome
	}
	turn.once.Do(func() {
		run.controlMu.Lock()
		run.turnActive = false
		run.rejectQueuedSteersLocked(store, protocol.SteerReasonNotRunning)
		steerInFlightDone := run.steerInFlightDone
		run.controlMu.Unlock()
		if steerInFlightDone != nil {
			<-steerInFlightDone
		}
		run.controlMu.Lock()
		if run.steerInFlightDone == steerInFlightDone {
			run.steerInFlightDone = nil
		}
		run.controlMu.Unlock()
		outcome.diagnostics = append(outcome.diagnostics, run.takeSteerDiagnostics()...)
		if strings.TrimSpace(outcome.modelReported) != "" || strings.TrimSpace(outcome.effortReported) != "" {
			if err := store.RecordObservedSelection(run.jobID, outcome.modelReported, outcome.effortReported); err != nil && !errors.Is(err, jobstore.ErrTerminal) {
				outcome.diagnostics = append(outcome.diagnostics, "record reported model: "+err.Error())
			}
		}
		if strings.TrimSpace(outcome.backendSessionID) != "" {
			if _, err := store.RecordBackendSessionID(run.jobID, outcome.backendSessionID); err != nil && !errors.Is(err, jobstore.ErrTerminal) {
				outcome.diagnostics = append(outcome.diagnostics, "record backend session: "+err.Error())
			}
		}
		outcome.diagnostics = append([]string(nil), outcome.diagnostics...)
		turn.outcome = outcome
		run.mu.Lock()
		if strings.TrimSpace(outcome.backendSessionID) != "" {
			run.latestSessionID = outcome.backendSessionID
		}
		if outcome.cleanup == protocol.CleanupUncertain {
			run.retiredCleanup = protocol.CleanupUncertain
		}
		run.retiredDiagnostics = append(run.retiredDiagnostics, outcome.diagnostics...)
		run.mu.Unlock()
		close(turn.done)
	})
	<-turn.done
	return turn.outcome
}

// retiredTurnOutcome waits only when a turn has been started. Its result is
// the aggregate cancellation receipt; a job canceled before its first turn
// therefore keeps the intentionally empty session ID and clean cleanup state.
func (run *activeExecution) retiredTurnOutcome() (turnOutcome, bool) {
	if run == nil {
		return turnOutcome{}, false
	}
	run.mu.Lock()
	turn := run.turn
	run.mu.Unlock()
	if turn == nil {
		return turnOutcome{}, false
	}
	<-turn.done
	outcome := turn.outcome
	run.mu.Lock()
	if strings.TrimSpace(outcome.backendSessionID) == "" {
		outcome.backendSessionID = run.latestSessionID
	}
	if run.retiredCleanup == protocol.CleanupUncertain {
		outcome.cleanup = protocol.CleanupUncertain
	}
	if len(run.retiredDiagnostics) > 0 {
		outcome.diagnostics = append([]string(nil), run.retiredDiagnostics...)
	}
	run.mu.Unlock()
	return outcome, true
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

// noteProcessClaim retains the one process identity reported synchronously by
// an adapter while startTurn holds launchMu. The owner persists it immediately
// after startTurn releases that lock, before the active turn can retire.
func (run *activeExecution) noteProcessClaim(ref engine.ProcessRef) {
	claim := jobstore.ProcessClaim{PID: ref.PID, PGID: ref.PGID, StartToken: ref.StartTime}
	run.mu.Lock()
	if run.claimAttempted {
		run.claimErr = errors.New("backend reported more than one process claim for one turn")
		run.mu.Unlock()
		run.containAfterClaimFailure()
		return
	}
	run.claimAttempted = true
	run.processClaim = &claim
	run.mu.Unlock()
}

// persistProcessClaim performs the separate durable claim transaction after
// launchMu is released. If cancellation wins that small interval, it uses the
// in-memory claim for containment and waits for this active turn to retire;
// the owner invokes this method before that retirement can publish.
func (run *activeExecution) persistProcessClaim(store *jobstore.Store) {
	run.mu.Lock()
	if run.claimRecorded || run.processClaim == nil {
		run.mu.Unlock()
		return
	}
	claim := *run.processClaim
	run.mu.Unlock()

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

// currentProcessClaim returns the active turn's identity, including the short
// interval after an adapter reports it and before its durable transaction
// commits. Cancellation prefers it to a prior turn's durable claim.
func (run *activeExecution) currentProcessClaim() *jobstore.ProcessClaim {
	if run == nil {
		return nil
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.processClaim == nil {
		return nil
	}
	claim := *run.processClaim
	return &claim
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
	s.queuedJobs = nil
	s.runningByBackend = make(map[string]int)
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
	queued := len(s.queuedJobs)
	s.queuedJobs = nil
	s.executionMu.Unlock()
	if queued > 0 {
		s.activeJobs.Add(-int64(queued))
	}
	if cancel != nil {
		cancel()
	}
	s.executionWG.Wait()
}

type queuedJob struct {
	record  jobstore.Record
	backend engine.Backend
}

// queueWouldExceed reports whether a new submission would wait beyond the
// configured queue bound. A submission that can claim an immediately free
// slot is not a queue entry, even when the waiting queue is full.
func (s *Server) queueWouldExceed(backend string) bool {
	s.executionMu.Lock()
	defer s.executionMu.Unlock()
	if s.executionCtx == nil || s.executionCtx.Err() != nil {
		return false
	}
	if len(s.queuedJobs) < s.maxQueued {
		return false
	}
	if len(s.executions) >= s.maxJobs || s.runningByBackend[backend] >= s.maxJobsPerBackend {
		return true
	}
	for _, queued := range s.queuedJobs {
		if queued.record.Backend == backend {
			return true
		}
	}
	return false
}

// enqueueQueuedJob records one newly admitted queued job and fills every
// currently available slot without allowing a saturated backend to block a
// later eligible backend. Recovered jobs are intentionally never passed here.
func (s *Server) enqueueQueuedJob(record jobstore.Record, backend engine.Backend) {
	if s == nil || record.State != protocol.PublicStateQueued || record.JobID == "" {
		return
	}
	s.executionMu.Lock()
	if s.executionCtx == nil || s.executionCtx.Err() != nil {
		s.executionMu.Unlock()
		return
	}
	store, err := s.ensureJobStore()
	if err != nil {
		s.executionMu.Unlock()
		return
	}
	current, err := store.Get(record.JobID)
	if err != nil || current.State != protocol.PublicStateQueued || current.Starting {
		s.executionMu.Unlock()
		return
	}
	record = current
	for _, queued := range s.queuedJobs {
		if queued.record.JobID == record.JobID {
			s.executionMu.Unlock()
			return
		}
	}
	if _, exists := s.executions[record.JobID]; exists {
		s.executionMu.Unlock()
		return
	}
	s.queuedJobs = append(s.queuedJobs, queuedJob{record: record, backend: backend})
	s.activeJobs.Add(1)
	s.dispatchQueuedLocked()
	s.executionMu.Unlock()
}

// dispatchQueuedLocked starts as many eligible jobs as the daemon-owned
// global and per-backend limits permit. The first eligible item wins; an
// earlier item for a saturated backend does not block a different backend.
// The caller holds executionMu.
func (s *Server) dispatchQueuedLocked() {
	for s.executionCtx != nil && s.executionCtx.Err() == nil && len(s.executions) < s.maxJobs {
		selected := -1
		for index, queued := range s.queuedJobs {
			if s.runningByBackend[queued.record.Backend] < s.maxJobsPerBackend {
				selected = index
				break
			}
		}
		if selected < 0 {
			return
		}
		queued := s.queuedJobs[selected]
		copy(s.queuedJobs[selected:], s.queuedJobs[selected+1:])
		s.queuedJobs[len(s.queuedJobs)-1] = queuedJob{}
		s.queuedJobs = s.queuedJobs[:len(s.queuedJobs)-1]
		s.startQueuedJobLocked(queued)
	}
}

// startQueuedJobLocked reserves a slot before the goroutine can cross the
// durable starting transition. The caller holds executionMu.
func (s *Server) startQueuedJobLocked(queued queuedJob) {
	if _, exists := s.executions[queued.record.JobID]; exists {
		return
	}
	run := newActiveExecution(queued.record.JobID, queued.backend)
	run.slotAcquiredAt = time.Now().UTC()
	s.executions[queued.record.JobID] = run
	s.runningByBackend[queued.record.Backend]++
	parent := s.executionCtx
	s.executionWG.Add(1)

	go func() {
		defer s.executionWG.Done()
		defer s.touchActivity()
		s.runJob(parent, queued.record, run)
		s.executionMu.Lock()
		if s.executions[queued.record.JobID] == run {
			delete(s.executions, queued.record.JobID)
		}
		s.runningByBackend[queued.record.Backend]--
		if s.runningByBackend[queued.record.Backend] <= 0 {
			delete(s.runningByBackend, queued.record.Backend)
		}
		s.dispatchQueuedLocked()
		s.executionMu.Unlock()
		s.activeJobs.Add(-1)
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

	var jobCtx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		acquiredAt := run.slotAcquiredAt
		if acquiredAt.IsZero() {
			acquiredAt = time.Now().UTC()
		}
		jobCtx, cancel = context.WithDeadline(parent, acquiredAt.Add(timeout))
	} else {
		jobCtx, cancel = context.WithCancel(parent)
	}
	run.setCancel(cancel)
	defer cancel()
	if run.cancellationRequested() {
		return
	}
	if err := jobCtx.Err(); err != nil {
		s.finishStartError(store, record, jobCtx, err)
		return
	}

	resumeTarget, err := resumeTargetForExecution(store, spec)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("resolve resume target: %w", err), nil)
		return
	}
	if diagnostic := stateRootSpaceDiagnostic(s.stateRoot); diagnostic != "" {
		if updated, appendErr := store.AppendDiagnostic(record.JobID, diagnostic); appendErr != nil {
			log.Printf("agentbus service: job %s cannot record state-root space diagnostic: %v", record.JobID, appendErr)
		} else {
			record = updated
		}
	}
	if err := jobCtx.Err(); err != nil {
		s.finishStartError(store, record, jobCtx, err)
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
	codexSandboxed := true
	policy, hasSandboxPolicy := run.backend.(interface {
		CodexSandboxRequired(write, localIPC bool) bool
	})
	if spec.Verify && !hasSandboxPolicy {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable, errors.New("verify requires Codex OS sandbox support"), nil)
		return
	}
	if hasSandboxPolicy {
		codexSandboxed = policy.CodexSandboxRequired(record.Write, spec.LocalIPC) || spec.Verify
		if codexSandboxed {
			if err := s.ensureCodexSandbox(jobCtx, run.backend); err != nil {
				if jobCtx.Err() != nil {
					s.finishStartError(store, record, jobCtx, jobCtx.Err())
					return
				}
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable,
					fmt.Errorf("Codex OS sandbox cannot start on this host: %w", err), nil)
				return
			}
		}
	}
	requestedModel := record.Model
	requestedEffort := record.Effort
	if defaults := s.backendDefaults[record.Backend]; defaults.Model != "" && requestedModel == "" {
		requestedModel = defaults.Model
	}
	if defaults := s.backendDefaults[record.Backend]; defaults.Effort != "" && requestedEffort == "" {
		requestedEffort = defaults.Effort
	}
	opts := engine.SessionOpts{
		CWD:      record.CWD,
		Write:    record.Write,
		LocalIPC: spec.LocalIPC,
		Verify:   spec.Verify,
		Model:    requestedModel,
		Effort:   requestedEffort,
		Timeout:  timeout,
	}
	var (
		layout         engine.WorkspaceLayout
		managed        *managedCodexHome
		cache          jobCachePaths
		caches         map[string]string
		frozenCopyPath string
		frozenSource   *protocol.FrozenSource
	)
	if record.Backend == "codex" {
		layout, err = engine.LayoutForWorkspace(s.stateRoot, record.CWD)
		if err != nil {
			s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare Codex workspace layout: %w", err), nil)
			return
		}
		if resumeTarget != nil {
			codexHome, err := s.resumeCodexHome(store, *resumeTarget)
			if err != nil {
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare resumed Codex home: %w", err), nil)
				return
			}
			if codexHome != "" {
				opts.EnvOverlay = map[string]string{"CODEX_HOME": codexHome}
			}
		} else {
			codexHome, prepared, err := s.prepareCodexHome(layout, record.JobID)
			if prepared != nil {
				s.rememberManagedCodexHome(record.JobID, prepared)
			}
			if err != nil {
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare Codex home: %w", err), nil)
				return
			}
			managed = prepared
			if codexHome != "" {
				opts.EnvOverlay = map[string]string{"CODEX_HOME": codexHome}
			}
		}
		if opts.EnvOverlay == nil {
			opts.EnvOverlay = make(map[string]string)
		}
		// The offline Codex sandbox may read the daemon's populated module
		// cache, but does not get a private empty replacement or network access.
		if s.moduleCache != "" {
			opts.EnvOverlay["GOMODCACHE"] = s.moduleCache
		}
		opts.EnvOverlay["GOPROXY"] = "off"
	} else if record.Write {
		layout, err = engine.LayoutForWorkspace(s.stateRoot, record.CWD)
		if err != nil {
			s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare job workspace layout: %w", err), nil)
			return
		}
	}
	if record.Write || spec.Verify {
		cache, managed, err = prepareJobCache(layout, record.JobID, managed)
		if managed != nil {
			s.rememberManagedCodexHome(record.JobID, managed)
		}
		if err != nil {
			s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare job cache: %w", err), nil)
			return
		}
		if record.Backend == "codex" {
			opts.WriteSandboxRoot = cache.root
		}
		if spec.Verify {
			frozenCopyPath = filepath.Join(cache.root, "src")
			identity, copyErr := createFrozenCopyContext(jobCtx, record.CWD, frozenCopyPath)
			if copyErr != nil {
				if jobCtx.Err() != nil {
					s.finishStartError(store, record, jobCtx, jobCtx.Err())
					return
				}
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("prepare frozen verification copy: %w", copyErr), nil)
				return
			}
			frozenSource = &identity
			opts.CWD = frozenCopyPath
		}
		if spec.LocalIPC {
			ipcRoot, rootErr := createLocalIPCRoot()
			if rootErr != nil {
				s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassBackendUnavailable, rootErr, nil)
				return
			}
			opts.LocalIPCRoot = ipcRoot
			s.rememberManagedLocalIPCRoot(record.JobID, ipcRoot)
		}
		opts.WriteEnvOverlay = map[string]string{
			"GOCACHE":              cache.goBuild,
			"TMPDIR":               cache.tmp,
			"npm_config_cache":     cache.npm,
			"COREPACK_HOME":        cache.corepack,
			"XDG_CACHE_HOME":       cache.xdg,
			"npm_config_store_dir": cache.pnpm,
			"YARN_CACHE_FOLDER":    cache.yarn,
		}
		if spec.LocalIPC {
			opts.WriteEnvOverlay["TMPDIR"] = opts.LocalIPCRoot
		}
		caches = make(map[string]string, len(opts.WriteEnvOverlay)+1)
		for key, value := range opts.WriteEnvOverlay {
			caches[key] = value
		}
	}
	if record.Backend == "codex" && s.moduleCache != "" {
		if caches == nil {
			caches = make(map[string]string, 1)
		}
		caches["GOMODCACHE"] = s.moduleCache
	}
	if len(caches) == 0 {
		caches = nil
	}
	scratch := ""
	if record.Write || spec.Verify {
		scratch = cache.root
	}
	posture := executionPosture(record.Backend, record.Write, spec.Verify, spec.LocalIPC, codexSandboxed, scratch, caches)
	if spec.Verify {
		posture.FrozenCopy = frozenCopyPath
		posture.FrozenSource = frozenSource
	}
	if run.cancellationRequested() {
		s.discardManagedCodexHome(record.JobID)
		return
	}

	// Persist the execution description in the same transaction as the
	// starting marker, before a backend process can be spawned.
	started, err := store.MarkStartingWithExecution(record.JobID, &posture)
	if err != nil {
		s.discardManagedCodexHome(record.JobID)
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
	resumeSessionID := ""
	if resumeTarget != nil {
		resumeSessionID = resumeTarget.BackendSessionID
	}
	session, err, canceled := run.startSession(jobCtx, opts, resumeSessionID)
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
	s.startSteeringWorker(store, run, jobCtx)
	itemPath, err := itemSidecarPath(logPaths.Stdout)
	if err != nil {
		s.recordExecutionFailure(store, record, protocol.CleanupClean, protocol.FailureClassInternal, fmt.Errorf("derive item sidecar path: %w", err), nil)
		return
	}
	itemWriter := newItemSidecarWriter(itemPath, transcriptItemTextCap, transcriptItemFileCap, run.noteItemSidecarDiagnostic)
	defer itemWriter.close()
	items := newItemAssembler(run, itemWriter)

	run.beginTurn()
	events, err, canceled := run.startTurn(store, session, jobCtx, engine.TurnInput{
		Prompt:   spec.Prompt,
		Write:    spec.Write,
		Timeout:  timeout,
		LogPaths: logPaths,
	})
	if canceled {
		if events == nil {
			run.retireTurn(store, turnOutcome{err: context.Canceled, cleanup: protocol.CleanupClean})
			return
		}
		outcome := collectTurn(s, jobCtx, run, events, items)
		outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
		run.retireTurn(store, outcome)
		return
	}
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, jobCtx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		diagnostics = appendItemSidecarDiagnostics(diagnostics, itemWriter)
		outcome := run.retireTurn(store, turnOutcome{err: err, cleanup: cleanup, diagnostics: diagnostics})
		s.finishTurnError(store, record, jobCtx, outcome.err, outcome.cleanup, outcome.diagnostics, outcome.backendSessionID)
		return
	}

	outcome := collectTurn(s, jobCtx, run, events, items)
	outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
	outcome = run.retireTurn(store, outcome)
	if run.cancellationRequested() {
		return
	}

	if errors.Is(outcome.err, context.DeadlineExceeded) || outcome.timedOut {
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailureWithDetails(store, record, outcome.cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, outcome.diagnostics, outcome.backendSessionID, "turn")
		return
	}
	if errors.Is(outcome.err, context.Canceled) || outcome.interrupted {
		_, _, claimErr := run.claimStatus()
		if claimErr != nil {
			outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
			s.recordExecutionFailureWithDetails(store, record, outcome.cleanup, protocol.FailureClassInternal, claimErr, outcome.diagnostics, outcome.backendSessionID, "turn")
			return
		}
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailureWithDetails(store, record, outcome.cleanup, protocol.FailureClassInterrupted, outcome.err, outcome.diagnostics, outcome.backendSessionID, "turn")
		return
	}
	if outcome.err != nil {
		outcome.diagnostics = appendItemSidecarDiagnostics(outcome.diagnostics, itemWriter)
		s.recordExecutionFailureWithDetails(store, record, outcome.cleanup, classifyExecutionFailure(outcome.err), outcome.err, outcome.diagnostics, outcome.backendSessionID, "turn")
		return
	}
	text, backendSessionID, contract, cleanup, diagnostics := s.evaluateOutputSchema(store, record, run, session, jobCtx, timeout, spec, logPaths, items, outcome.text, outcome.backendSessionID, outcome.cleanup, outcome.diagnostics)
	diagnostics = appendItemSidecarDiagnostics(diagnostics, itemWriter)
	s.recordExecutionCompletion(store, record, text, cleanup, diagnostics, contract, backendSessionID)
}

func taskSpecFromRecord(record jobstore.Record) (protocol.TaskSpec, error) {
	var spec protocol.TaskSpec
	if len(record.TaskSpec) == 0 {
		return spec, errors.New("durable task spec is missing")
	}
	if err := json.Unmarshal(record.TaskSpec, &spec); err != nil {
		return spec, fmt.Errorf("decode durable task spec: %w", err)
	}
	if spec.InputMode != "" && spec.InputMode != protocol.InputModeFixed && spec.InputMode != protocol.InputModeSteerable {
		return spec, fmt.Errorf("durable task spec has invalid inputMode %q", spec.InputMode)
	}
	if err := validateVerifyTaskSpec(spec); err != nil {
		return spec, fmt.Errorf("durable task spec has invalid verify: %w", err)
	}
	if err := validateLocalIPCSpec(spec, localIPCGOOS); err != nil {
		return spec, fmt.Errorf("durable task spec has invalid localIPC: %w", err)
	}
	if record.InputMode != "" && record.InputMode != protocol.EffectiveInputMode(spec.InputMode) {
		return spec, errors.New("durable task spec inputMode disagrees with job record")
	}
	if spec.Backend != record.Backend || spec.CWD == "" || spec.Prompt == "" || spec.Write != record.Write {
		return spec, errors.New("durable task spec disagrees with job record")
	}
	return spec, nil
}

// resumeTargetForExecution loads the source selected by admission. Admission
// already validated it transactionally before the new record was persisted;
// terminal records are immutable, so execution needs only the durable lookup.
func resumeTargetForExecution(store *jobstore.Store, spec protocol.TaskSpec) (*jobstore.Record, error) {
	if spec.ResumeJobID == "" {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("job store is unavailable")
	}
	target, err := store.Get(spec.ResumeJobID)
	if err != nil {
		return nil, err
	}
	return &target, nil
}

// validateCodexResumeHome rejects compatibility home modes because the source
// record does not retain their original path. A resumed thread must use the
// durable per-job home that actually owns it.
func (s *Server) validateCodexResumeHome() error {
	if s == nil {
		return errors.New("nil service server")
	}
	if s.codexHomeInherit {
		return errors.New("cannot resume Codex job with inherited CODEX_HOME: the source job home was not recorded")
	}
	if strings.TrimSpace(s.codexHomeOverride) != "" {
		return errors.New("cannot resume Codex job with AGENTBUS_CODEX_HOME override: the source job home was not recorded")
	}
	return nil
}

// resumeCodexHome returns the exact CODEX_HOME at the root of a resume
// lineage: that is where the backend thread actually lives. It intentionally
// never creates a replacement, because a missing thread home must fail the new
// job rather than making a fresh thread look like a resume.
func (s *Server) resumeCodexHome(store *jobstore.Store, source jobstore.Record) (string, error) {
	if err := s.validateCodexResumeHome(); err != nil {
		return "", err
	}
	threadSource, err := resumeThreadSource(store, source)
	if err != nil {
		return "", err
	}
	if threadSource.JobID == "" || filepath.Base(threadSource.JobID) != threadSource.JobID || threadSource.CWD == "" {
		return "", errors.New("resume target has no valid managed Codex home identity")
	}
	path := filepath.Join(s.stateRoot, "workspaces", engine.WorkspaceKey(threadSource.CWD), "codex", threadSource.JobID)
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect source Codex home: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("source Codex home is not a directory")
	}
	return path, nil
}

// resumeThreadSource follows persisted resumeJobId links to the job that
// created the managed thread home. Each link is immutable after admission; the
// cycle check makes a corrupt record fail closed before a backend launch.
func resumeThreadSource(store *jobstore.Store, source jobstore.Record) (jobstore.Record, error) {
	if store == nil {
		return jobstore.Record{}, errors.New("job store is unavailable")
	}
	seen := make(map[string]struct{})
	for {
		if source.JobID == "" {
			return jobstore.Record{}, errors.New("resume target has no job ID")
		}
		if _, duplicate := seen[source.JobID]; duplicate {
			return jobstore.Record{}, errors.New("resume lineage contains a cycle")
		}
		seen[source.JobID] = struct{}{}
		spec, err := taskSpecFromRecord(source)
		if err != nil {
			return jobstore.Record{}, fmt.Errorf("decode resume lineage job %q: %w", source.JobID, err)
		}
		if spec.ResumeJobID == "" {
			return source, nil
		}
		source, err = store.Get(spec.ResumeJobID)
		if err != nil {
			return jobstore.Record{}, fmt.Errorf("load resume lineage job %q: %w", spec.ResumeJobID, err)
		}
	}
}

// evaluateOutputSchema runs only after the initial turn has successfully
// retired and before the job's sole terminal write. A correction failure is a
// contract failure, not a reason to discard the already authoritative result.
func (s *Server) evaluateOutputSchema(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, spec protocol.TaskSpec, logPaths engine.LogPaths, items *itemAssembler, original string, backendSessionID string, cleanup protocol.Cleanup, diagnostics []string) (string, string, protocol.ContractResult, protocol.Cleanup, []string) {
	if len(spec.OutputSchema) == 0 {
		return original, backendSessionID, protocol.ContractResult{}, cleanup, diagnostics
	}

	digest, err := schema.Digest(spec.OutputSchema)
	if err != nil {
		return original, backendSessionID, protocol.ContractResult{
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
		return original, backendSessionID, contract, cleanup, append(diagnostics, "evaluate output schema: "+executionFailureReason(err))
	}
	if contract.Compliant {
		return original, backendSessionID, contract, cleanup, diagnostics
	}
	if err := ctx.Err(); err != nil {
		return original, backendSessionID, contract, cleanup, diagnostics
	}
	initialClaimAttempted, initialClaimRecorded, initialClaimErr := run.claimStatus()
	if initialClaimErr != nil || !initialClaimAttempted || !initialClaimRecorded {
		return original, backendSessionID, contract, cleanup, diagnostics
	}

	// collectTurn returns only after the initial event stream closes. The
	// retained adapter clears that turn's active process before closing its
	// stream, so the correction's claim is recorded after the initial process
	// has retired.
	run.setSchemaCorrectionActive(true)
	correction := func() turnOutcome {
		defer run.setSchemaCorrectionActive(false)
		return s.runCorrectionTurn(store, record, run, session, ctx, timeout, logPaths, items, schema.CorrectionPrompt(string(spec.OutputSchema), initial.Violations))
	}()
	if correction.backendSessionID != "" {
		backendSessionID = correction.backendSessionID
	}
	if correction.cleanup == protocol.CleanupUncertain {
		cleanup = protocol.CleanupUncertain
	}
	diagnostics = append(diagnostics, correction.diagnostics...)
	contract.Attempts = 2
	if correction.err != nil || correction.timedOut || correction.interrupted {
		return original, backendSessionID, contract, cleanup, diagnostics
	}
	if _, _, claimErr := run.claimStatus(); claimErr != nil {
		return original, backendSessionID, contract, cleanup, diagnostics
	}

	corrected, err := schema.Validate(correction.text, spec.OutputSchema)
	if err != nil || !corrected.Compliant {
		return original, backendSessionID, contract, cleanup, diagnostics
	}
	contract.Evaluated = corrected.Evaluated
	contract.Compliant = corrected.Compliant
	contract.Violations = append([]string(nil), corrected.Violations...)
	return correction.text, backendSessionID, contract, cleanup, diagnostics
}

// runCorrectionTurn starts the sole read-only correction turn after the
// initial turn has retired.
func (s *Server) runCorrectionTurn(store *jobstore.Store, record jobstore.Record, run *activeExecution, session engine.Session, ctx context.Context, timeout time.Duration, logPaths engine.LogPaths, items *itemAssembler, prompt string) turnOutcome {
	run.beginTurn()
	events, err, canceled := run.startTurn(store, session, ctx, engine.TurnInput{
		Prompt:   prompt,
		Write:    false,
		Timeout:  timeout,
		LogPaths: logPaths,
	})
	if canceled {
		if events == nil {
			outcome := turnOutcome{err: context.Canceled, cleanup: protocol.CleanupClean}
			outcome = run.retireTurn(store, outcome)
			return outcome
		}
		outcome := collectTurn(s, ctx, run, events, items)
		outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
		outcome = run.retireTurn(store, outcome)
		return outcome
	}
	if err != nil {
		cleanup, diagnostics := cleanupAfterContextStop(run, ctx.Err())
		if cleanup == protocol.CleanupClean {
			cleanup, diagnostics = cleanupForRun(run, cleanup, diagnostics)
		}
		outcome := turnOutcome{err: err, cleanup: cleanup, diagnostics: diagnostics}
		outcome = run.retireTurn(store, outcome)
		return outcome
	}

	outcome := collectTurn(s, ctx, run, events, items)
	outcome.cleanup, outcome.diagnostics = cleanupForRun(run, outcome.cleanup, outcome.diagnostics)
	outcome = run.retireTurn(store, outcome)
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

func (s *Server) finishTurnError(store *jobstore.Store, record jobstore.Record, ctx context.Context, err error, cleanup protocol.Cleanup, diagnostics []string, backendSessionID string) {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		s.recordExecutionFailureWithDetails(store, record, cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, diagnostics, backendSessionID, "turn")
		return
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		s.recordExecutionFailureWithDetails(store, record, cleanup, protocol.FailureClassInterrupted, ctx.Err(), diagnostics, backendSessionID, "turn")
		return
	}
	s.recordExecutionFailureWithDetails(store, record, cleanup, classifyExecutionFailure(err), err, diagnostics, backendSessionID, "turn")
}

func (s *Server) finishContextStop(store *jobstore.Store, record jobstore.Record, err error, cleanup protocol.Cleanup, diagnostics []string) {
	if errors.Is(err, context.DeadlineExceeded) {
		s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassTimeout, context.DeadlineExceeded, diagnostics)
		return
	}
	s.recordExecutionFailure(store, record, cleanup, protocol.FailureClassInterrupted, err, diagnostics)
}

func classifyStartFailure(err error) protocol.FailureClass {
	if class, ok := providerFailureClass(err); ok {
		return class
	}
	if errors.Is(err, engine.ErrProviderOverloaded) {
		return protocol.FailureClassProviderOverloaded
	}
	return protocol.FailureClassBackendUnavailable
}

func providerFailureClass(err error) (protocol.FailureClass, bool) {
	var providerFailure *engine.ProviderFailureError
	if !errors.As(err, &providerFailure) || providerFailure == nil {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(providerFailure.Evidence.Class)) {
	case "usage_limit":
		return protocol.FailureClassUsageLimit, true
	case "authentication":
		return protocol.FailureClassAuthentication, true
	case "provider_overloaded":
		return protocol.FailureClassProviderOverloaded, true
	case "model_unavailable":
		return protocol.FailureClassModelUnavailable, true
	case "content_policy":
		return protocol.FailureClassContentPolicy, true
	default:
		return "", false
	}
}

func classifyExecutionFailure(err error) protocol.FailureClass {
	if class, ok := providerFailureClass(err); ok {
		return class
	}
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
	text             string
	backendSessionID string
	modelReported    string
	effortReported   string
	err              error
	timedOut         bool
	interrupted      bool
	cleanup          protocol.Cleanup
	diagnostics      []string
}

func collectTurn(s *Server, ctx context.Context, run *activeExecution, events <-chan engine.Event, items *itemAssembler) turnOutcome {
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
			if drainTurnEvents(s, run, events, &outcome, &assistant, &result, &hasResult, items) {
				outcome.text = attemptFinalText(hasResult, result, assistant.String())
			} else {
				outcome.cleanup = protocol.CleanupUncertain
				outcome.diagnostics = append(outcome.diagnostics, "backend event stream did not close after containment grace")
				items.writer.markIncomplete()
				discardTurnEvents(events)
			}
			outcome.err = ctx.Err()
			return outcome
		case event, ok := <-events:
			if !ok {
				outcome.text = attemptFinalText(hasResult, result, assistant.String())
				return outcome
			}
			absorbTurnEvent(run, &outcome, &assistant, &result, &hasResult, items, event)
		}
	}
}

func drainTurnEvents(s *Server, run *activeExecution, events <-chan engine.Event, outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool, items *itemAssembler) bool {
	timer := time.NewTimer(s.turnDrainGrace)
	defer timer.Stop()
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return true
			}
			absorbTurnEvent(run, outcome, assistant, result, hasResult, items, event)
		case <-timer.C:
			return false
		}
	}
}

func absorbTurnEvent(run *activeExecution, outcome *turnOutcome, assistant *strings.Builder, result *string, hasResult *bool, items *itemAssembler, event engine.Event) {
	rawText := authoritativeText(event)
	items.absorb(event, rawText)
	if captureWarning, _ := event.Metadata["agentbusCaptureWarning"].(bool); captureWarning {
		outcome.diagnostics = append(outcome.diagnostics, rawText)
	}
	switch event.Type {
	case engine.EventModelReported:
		model := strings.TrimSpace(event.ModelReported)
		if model != "" {
			outcome.modelReported = model
		}
		effort := strings.TrimSpace(event.EffortReported)
		if effort != "" {
			outcome.effortReported = effort
		}
		run.noteModelReported(model, effort)
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
		if strings.TrimSpace(event.TurnFinal.BackendSessionID) != "" {
			outcome.backendSessionID = event.TurnFinal.BackendSessionID
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

func (s *Server) recordExecutionCompletion(store *jobstore.Store, record jobstore.Record, text string, cleanup protocol.Cleanup, diagnostics []string, contract protocol.ContractResult, backendSessionID string) {
	info, err := spillAuthoritativeResult(record, []byte(text))
	if err != nil {
		// Result persistence is mandatory for an authoritative completion. Keep
		// the provider session and partial transcript available as a failed job.
		const prefix = "persist final result artifact: "
		cause := errors.New(prefix + boundedFailureSuffix(err.Error(), engine.FailureReasonMaxRunes-len([]rune(prefix))))
		s.recordExecutionFailureWithDetails(store, record, cleanup, protocol.FailureClassStorage, cause, diagnostics, backendSessionID, "finalize")
		return
	}
	resultText := info.Text
	if info.TextElided {
		resultText = ""
	}
	// Completed work remains terminal even when cleanup retains its home. The
	// store keeps the session ID for retained completed records so they can be
	// resumed as a new job, and clears it for other completed records.
	s.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:            protocol.PublicStateCompleted,
		Cleanup:          cleanup,
		BackendSessionID: backendSessionID,
		Diagnostics:      appendRecordDiagnostics(record.Diagnostics, diagnostics),
		Contract:         contract,
		ResultText:       resultText,
		ResultPath:       info.ResultPath,
		ResultSHA256:     info.SHA256,
		ResultBytes:      info.Bytes,
		FinishedAt:       time.Now().UTC(),
	})
}

func (s *Server) recordExecutionFailure(store *jobstore.Store, record jobstore.Record, cleanup protocol.Cleanup, class protocol.FailureClass, cause error, diagnostics []string) {
	s.recordExecutionFailureWithDetails(store, record, cleanup, class, cause, diagnostics, "", "startup")
}

func (s *Server) recordExecutionFailureWithDetails(store *jobstore.Store, record jobstore.Record, cleanup protocol.Cleanup, class protocol.FailureClass, cause error, diagnostics []string, backendSessionID, stage string) {
	if !class.Valid() {
		class = protocol.FailureClassInternal
	}
	var evidence *engine.ProviderFailureEvidence
	var providerFailure *engine.ProviderFailureError
	if errors.As(cause, &providerFailure) && providerFailure != nil {
		copy := providerFailure.Evidence
		copy.Code = boundProviderValue(copy.Code, 128)
		copy.Message = boundProviderValue(copy.Message, 512)
		if copy.RetryAfter != nil {
			retryAfter := copy.RetryAfter.UTC()
			copy.RetryAfter = &retryAfter
		}
		evidence = &copy
	}
	s.markTerminal(store, record.JobID, jobstore.TerminalUpdate{
		State:            protocol.PublicStateFailed,
		Cleanup:          cleanup,
		BackendSessionID: backendSessionID,
		FailureClass:     class,
		FailureReason:    executionFailureReason(cause),
		FailureProviderCode: func() string {
			if evidence == nil {
				return ""
			}
			return evidence.Code
		}(),
		FailureProviderMessage: func() string {
			if evidence == nil {
				return ""
			}
			return evidence.Message
		}(),
		FailureStage: stage,
		FailureRetryAfter: func() *time.Time {
			if evidence == nil {
				return nil
			}
			return evidence.RetryAfter
		}(),
		CancellationReason: cancellationReasonFor(class, cause),
		Diagnostics:        appendRecordDiagnostics(record.Diagnostics, diagnostics),
		FinishedAt:         time.Now().UTC(),
	})
}

func cancellationReasonFor(class protocol.FailureClass, cause error) protocol.CancellationReason {
	if class == protocol.FailureClassTimeout || errors.Is(cause, context.DeadlineExceeded) {
		return protocol.CancellationReasonDeadline
	}
	if errors.Is(cause, engine.ErrTurnInterrupted) {
		return protocol.CancellationReasonProviderInterruption
	}
	if errors.Is(cause, context.Canceled) {
		return protocol.CancellationReasonDaemonShutdown
	}
	return ""
}

func boundProviderValue(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes-3]) + "..."
	}
	return value
}

func (s *Server) markTerminal(store *jobstore.Store, jobID string, terminal jobstore.TerminalUpdate) {
	run := s.activeExecution(jobID)
	if run != nil {
		run.controlMu.Lock()
		if run.steerCanceled {
			run.controlMu.Unlock()
			return
		}
		inFlightDone := run.closeSteeringLocked(store)
		run.controlMu.Unlock()
		if inFlightDone != nil {
			<-inFlightDone
		}
		run.waitSteeringWorker()
		run.controlMu.Lock()
		if run.steerCanceled {
			run.controlMu.Unlock()
			return
		}
		defer run.controlMu.Unlock()
		terminal.Diagnostics = append(terminal.Diagnostics, run.takeSteerDiagnostics()...)
	}
	// job.cancel has already reserved this active execution and will commit a
	// canceled first terminal itself. Do not let an asynchronously drained
	// backend stream race it with an interrupted, failed, or completed record.
	if s.cancellationPending(jobID) {
		return
	}
	var (
		home          *managedCodexHome
		cleanupRecord jobstore.Record
	)
	if home = s.takeManagedCodexHome(jobID); home != nil {
		current, getErr := store.Get(jobID)
		if getErr != nil {
			home.close()
			home = nil
			terminal.Cleanup = protocol.CleanupUncertain
			terminal.Diagnostics = append(terminal.Diagnostics, "inspect managed Codex home terminal record: "+getErr.Error())
		} else if current.State.IsTerminal() {
			if recordUsesVerify(current) {
				if copyErr := removeFrozenCopyFromManagedHome(home); copyErr != nil {
					log.Printf("agentbus service: job %s frozen verification copy cleanup after terminal race: %v", jobID, copyErr)
				}
			}
			home.close()
			s.discardRecordedLocalIPCRoot(current)
			return
		} else {
			cleanupRecord = current
			cleanupRecord.State = terminal.State
			if recordUsesVerify(cleanupRecord) {
				if copyErr := removeFrozenCopyFromManagedHome(home); copyErr != nil {
					terminal.Cleanup = protocol.CleanupUncertain
					terminal.Diagnostics = append(terminal.Diagnostics, "frozen verification copy cleanup: "+copyErr.Error())
				}
			}
		}
	}
	_, err := store.MarkTerminal(jobID, terminal)
	if err == nil {
		s.discardManagedLocalIPCRoot(jobID, "")
		if home != nil {
			cleanup, diagnostics := finalizeManagedCodexHome(home, cleanupRecord, terminal.Cleanup, terminal.Diagnostics)
			if cleanup != terminal.Cleanup && len(diagnostics) > len(terminal.Diagnostics) {
				log.Printf("agentbus service: job %s managed Codex home cleanup after terminal write: %s", jobID, diagnostics[len(diagnostics)-1])
			}
		}
		return
	}
	if errors.Is(err, jobstore.ErrTerminal) {
		if home != nil {
			home.close()
		}
		s.discardManagedLocalIPCRoot(jobID, "")
		// ErrTerminal is the store-enforced first-terminal-wins result. A
		// later observer never retries or overwrites that recorded outcome.
		return
	}
	if terminal.State == protocol.PublicStateCompleted {
		storageCause := fmt.Errorf("persist terminal job record: %w", err)
		storageDiagnostic := "storage failure: " + executionFailureReason(storageCause)
		failed := jobstore.TerminalUpdate{
			State:            protocol.PublicStateFailed,
			Cleanup:          terminal.Cleanup,
			BackendSessionID: terminal.BackendSessionID,
			FailureClass:     protocol.FailureClassStorage,
			FailureReason:    executionFailureReason(storageCause),
			Diagnostics:      appendRecordDiagnostics(terminal.Diagnostics, []string{storageDiagnostic}),
			FinishedAt:       time.Now().UTC(),
		}
		if _, failureErr := store.MarkTerminal(jobID, failed); failureErr == nil {
			s.discardManagedLocalIPCRoot(jobID, "")
			if home != nil {
				home.close()
			}
			return
		} else if !errors.Is(failureErr, jobstore.ErrTerminal) {
			err = errors.Join(err, fmt.Errorf("persist storage failure terminal record: %w", failureErr))
		} else {
			s.discardManagedLocalIPCRoot(jobID, "")
		}
	}
	if home != nil {
		home.close()
	}
	log.Printf("agentbus service: job %s terminal write failed: %v", jobID, err)
}

func appendRecordDiagnostics(recordDiagnostics, additional []string) []string {
	result := append([]string(nil), recordDiagnostics...)
	return append(result, additional...)
}

func stateRootSpaceDiagnostic(root string) string {
	var stats unix.Statfs_t
	if err := unix.Statfs(root, &stats); err != nil {
		return ""
	}
	if stats.Bavail <= 0 || stats.Bsize <= 0 {
		return fmt.Sprintf("low state-root free space: less than %d bytes available; warning threshold is %d bytes", uint64(0), lowStateRootSpaceThresholdBytes)
	}
	blocks, blockSize := uint64(stats.Bavail), uint64(stats.Bsize)
	available := ^uint64(0)
	if blocks <= math.MaxUint64/blockSize {
		available = blocks * blockSize
	}
	if available >= lowStateRootSpaceThresholdBytes {
		return ""
	}
	return fmt.Sprintf("low state-root free space: %d bytes available; warning threshold is %d bytes", available, lowStateRootSpaceThresholdBytes)
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

func boundedFailureSuffix(value string, maxRunes int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= maxRunes {
		return string(runes)
	}
	return "..." + string(runes[len(runes)-(maxRunes-3):])
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
