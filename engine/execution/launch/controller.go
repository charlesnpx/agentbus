package launch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type DurabilityOutcome = authority.DurabilityOutcome

const (
	DefinitelyNotCommitted DurabilityOutcome = authority.DefinitelyNotCommitted
	CommittedAndAnchored   DurabilityOutcome = authority.CommittedAndAnchored
	CommitOutcomeUnknown   DurabilityOutcome = authority.CommitOutcomeUnknown
)

var (
	ErrAuthorityRequired        = errors.New("launch authority is required")
	ErrCustodianRequired        = errors.New("launch custodian is required")
	ErrInvalidLaunchRequest     = errors.New("invalid launch request")
	ErrDurabilityNotCommitted   = errors.New("durable mutation was not committed")
	ErrDurabilityUnknown        = errors.New("durable mutation outcome is unknown")
	ErrReleaseUncertain         = errors.New("release outcome is uncertain")
	ErrReleaseRecordUnavailable = errors.New("release record is unavailable")
	ErrFailClosed               = errors.New("launch failed closed")
)

type AuthorityPort interface {
	BindGroup(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.GroupRef) (DurabilityOutcome, error)
	AllocateGrant(context.Context, model.AttemptRef, model.LaunchOrdinal) (model.LaunchGrant, DurabilityOutcome, error)
	RecordRelease(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.ChildIdentity, model.Evidence) (DurabilityOutcome, error)
	RecordQuiescence(context.Context, model.JobID, model.LaunchOrdinal, custodian.VerifiedQuiescence) (DurabilityOutcome, error)
	FailStop(context.Context, error) error
}

type CustodianPort interface {
	Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error)
	ContainAndVerify(context.Context, model.GroupRef, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
}

type PreparedProcess interface {
	Ref() model.GroupRef
	Release(context.Context) (RunningProcess, custodian.ReleaseOutcome, error)
	AbortAndVerify(context.Context) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
}

type RunningProcess interface {
	Ref() model.GroupRef
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	WaitAndVerify(context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
	ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
}

type waitContainmentReporter interface {
	WaitContained() bool
}

type LaunchController struct {
	authority AuthorityPort
	custodian CustodianPort
}

func New(authority AuthorityPort, custodian CustodianPort) (*LaunchController, error) {
	if authority == nil {
		return nil, ErrAuthorityRequired
	}
	if custodian == nil {
		return nil, ErrCustodianRequired
	}
	return &LaunchController{authority: authority, custodian: custodian}, nil
}

type LaunchContext struct {
	JobID   model.JobID
	Attempt model.AttemptRef
	Ordinal model.LaunchOrdinal
}

func (launch LaunchContext) Validate() error {
	if err := launch.JobID.Validate(); err != nil {
		return fmt.Errorf("%w: job_id: %v", ErrInvalidLaunchRequest, err)
	}
	if err := launch.Attempt.Validate(); err != nil {
		return fmt.Errorf("%w: attempt: %v", ErrInvalidLaunchRequest, err)
	}
	if launch.Attempt.JobID != launch.JobID {
		return fmt.Errorf("%w: attempt job_id mismatch", ErrInvalidLaunchRequest)
	}
	if err := launch.Ordinal.Validate(); err != nil {
		return fmt.Errorf("%w: ordinal: %v", ErrInvalidLaunchRequest, err)
	}
	return nil
}

func (launch LaunchContext) Key() model.LaunchKey {
	return model.LaunchKey{Attempt: launch.Attempt, Ordinal: launch.Ordinal}
}

type LaunchRequest struct {
	Context  LaunchContext
	Exec     command.ExecSpec
	Failures *FailureInjector
}

func (request LaunchRequest) Validate() error {
	if err := request.Context.Validate(); err != nil {
		return err
	}
	if len(request.Exec.Argv) == 0 || request.Exec.Argv[0] == "" {
		return fmt.Errorf("%w: exec argv is required", ErrInvalidLaunchRequest)
	}
	return nil
}

type RunnerBinding struct {
	Context  LaunchContext
	Failures *FailureInjector
}

func (binding RunnerBinding) Validate() error {
	if err := binding.Context.Validate(); err != nil {
		return err
	}
	return nil
}

func (controller *LaunchController) Runner(binding RunnerBinding) (command.Runner, error) {
	if err := controller.ready(); err != nil {
		return nil, err
	}
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	return boundRunner{controller: controller, binding: binding}, nil
}

type boundRunner struct {
	controller *LaunchController
	binding    RunnerBinding
}

func (runner boundRunner) Start(ctx context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	return runner.controller.Start(ctx, LaunchRequest{
		Context:  runner.binding.Context,
		Exec:     spec,
		Failures: runner.binding.Failures,
	})
}

func (controller *LaunchController) Start(ctx context.Context, request LaunchRequest) (*Process, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := controller.ready(); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}

	prepared, err := controller.Prepare(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := inject(request.Failures, FailAfterPrepare); err != nil {
		return nil, errors.Join(err, controller.abortPrepared(containmentContext(ctx), prepared, false, request.Context))
	}

	group := prepared.Ref()
	bindOutcome, bindErr := controller.BindGroup(ctx, request.Context, group)
	if err := controller.handlePreGrantDurability(containmentContext(ctx), "bind_group", bindOutcome, bindErr, prepared, false, request.Context); err != nil {
		return nil, err
	}
	if err := inject(request.Failures, FailAfterBindGroup); err != nil {
		return nil, errors.Join(err, controller.abortPrepared(containmentContext(ctx), prepared, true, request.Context))
	}

	grant, grantOutcome, grantErr := controller.AllocateGrant(ctx, request.Context)
	if err := controller.handleGrantDurability(containmentContext(ctx), "allocate_grant", grantOutcome, grantErr, prepared, request.Context); err != nil {
		return nil, err
	}
	if err := validateGrant(request.Context, grant); err != nil {
		return nil, controller.containGroupAndFailStop(containmentContext(ctx), group, err)
	}
	if err := inject(request.Failures, FailAfterAllocateGrant); err != nil {
		return nil, controller.containGroupAndFailStop(containmentContext(ctx), group, err)
	}

	running, physicalReleaseOutcome, err := controller.Release(ctx, prepared)
	if releaseErr := controller.handleReleaseOutcome(containmentContext(ctx), prepared, request.Context, group, physicalReleaseOutcome, err); releaseErr != nil {
		return nil, releaseErr
	}
	if running == nil {
		reason := fmt.Errorf("%w: release returned nil running process", ErrReleaseUncertain)
		return nil, controller.containGroupAndFailStop(containmentContext(ctx), group, reason)
	}
	if !running.Ref().Equal(group) {
		reason := fmt.Errorf("%w: released group mismatch", ErrReleaseUncertain)
		return nil, controller.containGroupAndFailStop(containmentContext(ctx), group, reason)
	}

	process := newProcess(controller, containmentContext(ctx), request.Context, group, grant, running, request.Failures)
	if err := inject(request.Failures, FailAfterRelease); err != nil {
		return nil, process.failClosedBeforeReleaseRecord(containmentContext(ctx), err)
	}

	releaseOutcome, releaseErr := controller.RecordRelease(ctx, request.Context, group)
	if err := process.handlePostReleaseDurability(containmentContext(ctx), "record_release", releaseOutcome, releaseErr); err != nil {
		return nil, err
	}
	if err := inject(request.Failures, FailAfterRecordRelease); err != nil {
		return nil, process.failClosedAfterReleaseRecord(containmentContext(ctx), err)
	}
	process.enableReleaseRecord()
	return process, nil
}

func (controller *LaunchController) Run(ctx context.Context, request LaunchRequest) (Result, error) {
	process, err := controller.Start(ctx, request)
	if err != nil {
		return Result{}, err
	}
	_, waitErr := process.Wait(ctx)
	result, resultErr := process.Result(containmentContext(ctx))
	return result, errors.Join(waitErr, resultErr)
}

func (controller *LaunchController) Prepare(ctx context.Context, request LaunchRequest) (PreparedProcess, error) {
	prepared, err := controller.custodian.Prepare(ctx, request.Exec, request.Context.Key())
	if err != nil {
		return nil, err
	}
	if prepared == nil {
		return nil, fmt.Errorf("%w: custodian returned nil prepared process", ErrInvalidLaunchRequest)
	}
	if err := validatePreparedGroup(request.Context, prepared.Ref()); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (controller *LaunchController) BindGroup(ctx context.Context, launch LaunchContext, group model.GroupRef) (DurabilityOutcome, error) {
	return controller.authority.BindGroup(ctx, launch.JobID, launch.Attempt, launch.Ordinal, group)
}

func (controller *LaunchController) AllocateGrant(ctx context.Context, launch LaunchContext) (model.LaunchGrant, DurabilityOutcome, error) {
	return controller.authority.AllocateGrant(ctx, launch.Attempt, launch.Ordinal)
}

func (controller *LaunchController) Release(ctx context.Context, prepared PreparedProcess) (RunningProcess, custodian.ReleaseOutcome, error) {
	if prepared == nil {
		return nil, custodian.ReleaseDefinitelyNotSent, fmt.Errorf("%w: prepared process is nil", ErrInvalidLaunchRequest)
	}
	return prepared.Release(ctx)
}

func (controller *LaunchController) handleReleaseOutcome(ctx context.Context, prepared PreparedProcess, launch LaunchContext, group model.GroupRef, outcome custodian.ReleaseOutcome, releaseErr error) error {
	switch outcome {
	case custodian.ReleaseAccepted:
		if releaseErr != nil {
			reason := fmt.Errorf("%w: release accepted with error: %v", ErrReleaseUncertain, releaseErr)
			return controller.containGroupAndFailStop(ctx, group, reason)
		}
		return nil
	case custodian.ReleaseDefinitelyNotSent:
		reason := ErrReleaseUncertain
		if releaseErr != nil {
			reason = fmt.Errorf("%w: release definitely not sent: %v", ErrReleaseUncertain, releaseErr)
		}
		return errors.Join(reason, controller.abortPrepared(ctx, prepared, true, launch))
	case custodian.ReleaseOutcomeUnknown:
		reason := ErrReleaseUncertain
		if releaseErr != nil {
			reason = fmt.Errorf("%w: release outcome unknown: %v", ErrReleaseUncertain, releaseErr)
		}
		return controller.containRecordQuiescenceOrFailStop(ctx, launch, group, reason)
	default:
		reason := fmt.Errorf("%w: invalid release outcome %d", ErrReleaseUncertain, outcome)
		if releaseErr != nil {
			reason = errors.Join(reason, releaseErr)
		}
		return controller.containGroupAndFailStop(ctx, group, reason)
	}
}

func (controller *LaunchController) RecordRelease(ctx context.Context, launch LaunchContext, group model.GroupRef) (DurabilityOutcome, error) {
	child, evidence, err := releaseObservation(group)
	if err != nil {
		return DefinitelyNotCommitted, err
	}
	return controller.authority.RecordRelease(ctx, launch.JobID, launch.Attempt, launch.Ordinal, child, evidence)
}

func (controller *LaunchController) RecordQuiescence(ctx context.Context, launch LaunchContext, verified custodian.VerifiedQuiescence) (DurabilityOutcome, error) {
	return controller.authority.RecordQuiescence(ctx, launch.JobID, launch.Ordinal, verified)
}

func (controller *LaunchController) ContainAndVerify(ctx context.Context, launch LaunchContext, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, error) {
	verified, cleanup, err := controller.ContainAndVerifyWithCleanup(ctx, launch, group, cause)
	return verified, errors.Join(err, cleanup.Err)
}

func (controller *LaunchController) ContainAndVerifyWithCleanup(ctx context.Context, launch LaunchContext, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := controller.ready(); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	if err := launch.Validate(); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	if err := validatePreparedGroup(launch, group); err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return controller.custodian.ContainAndVerify(ctx, group, cause)
}

func (controller *LaunchController) ready() error {
	if controller == nil || controller.authority == nil {
		return ErrAuthorityRequired
	}
	if controller.custodian == nil {
		return ErrCustodianRequired
	}
	return nil
}

func (controller *LaunchController) abortPrepared(ctx context.Context, prepared PreparedProcess, groupDurable bool, launch LaunchContext) error {
	verified, cleanup, err := prepared.AbortAndVerify(ctx)
	abortErr := cleanup.Err
	if err != nil {
		abortErr = fmt.Errorf("abort prepared process: %w", err)
		verified, cleanup, err = controller.custodian.ContainAndVerify(ctx, prepared.Ref(), custodian.QuiescenceCauseContain)
		if err != nil {
			reason := errors.Join(abortErr, fmt.Errorf("contain prepared group: %w", err))
			return errors.Join(reason, controller.failStop(ctx, reason), ErrFailClosed)
		}
		abortErr = errors.Join(abortErr, cleanup.Err)
	}
	if !groupDurable {
		return abortErr
	}
	outcome, err := controller.RecordQuiescence(ctx, launch, verified)
	if mapped := durableMutationError("record_quiescence", outcome, err); mapped != nil {
		reason := errors.Join(abortErr, mapped)
		return errors.Join(reason, controller.failStop(ctx, reason), ErrFailClosed)
	}
	return abortErr
}

func (controller *LaunchController) handlePreGrantDurability(ctx context.Context, step string, outcome DurabilityOutcome, stepErr error, prepared PreparedProcess, groupDurable bool, launch LaunchContext) error {
	recordDurable := groupDurable || durabilityDecision(outcome) == durabilityCommitted
	switch durabilityDecision(outcome) {
	case durabilityCommitted:
		if stepErr == nil {
			return nil
		}
		return errors.Join(durableMutationError(step, outcome, stepErr), controller.abortPrepared(ctx, prepared, recordDurable, launch))
	case durabilityNotCommitted:
		return errors.Join(durableMutationError(step, outcome, stepErr), controller.abortPrepared(ctx, prepared, groupDurable, launch))
	case durabilityUnknown:
		return controller.containGroupAndFailStop(ctx, prepared.Ref(), durableMutationError(step, outcome, stepErr))
	default:
		return controller.containGroupAndFailStop(ctx, prepared.Ref(), durableMutationError(step, outcome, stepErr))
	}
}

func (controller *LaunchController) handleGrantDurability(ctx context.Context, step string, outcome DurabilityOutcome, stepErr error, prepared PreparedProcess, launch LaunchContext) error {
	group := prepared.Ref()
	switch durabilityDecision(outcome) {
	case durabilityCommitted:
		if stepErr == nil {
			return nil
		}
		return controller.containGroupAndFailStop(ctx, group, durableMutationError(step, outcome, stepErr))
	case durabilityNotCommitted:
		return errors.Join(durableMutationError(step, outcome, stepErr), controller.abortPrepared(ctx, prepared, true, launch))
	case durabilityUnknown:
		return controller.containGroupAndFailStop(ctx, group, durableMutationError(step, outcome, stepErr))
	default:
		return controller.containGroupAndFailStop(ctx, group, durableMutationError(step, outcome, stepErr))
	}
}

func (controller *LaunchController) containGroupAndFailStop(ctx context.Context, group model.GroupRef, reason error) error {
	_, cleanup, containErr := controller.custodian.ContainAndVerify(ctx, group, custodian.QuiescenceCauseContain)
	containErr = errors.Join(containErr, cleanup.Err)
	return errors.Join(reason, containErr, controller.failStop(ctx, errors.Join(reason, containErr)), ErrFailClosed)
}

func (controller *LaunchController) containRecordQuiescenceOrFailStop(ctx context.Context, launch LaunchContext, group model.GroupRef, reason error) error {
	verified, cleanup, containErr := controller.custodian.ContainAndVerify(ctx, group, custodian.QuiescenceCauseContain)
	if containErr != nil {
		failReason := errors.Join(reason, containErr)
		return errors.Join(failReason, controller.failStop(ctx, failReason), ErrFailClosed)
	}
	// C4 closes the release-unknown gap by contain-and-verify: once quiescence
	// is durably recorded, execution is possible but absent, so fail-stop is
	// reserved for unprovable containment or unknown quiescence durability.
	outcome, err := controller.RecordQuiescence(ctx, launch, verified)
	if mapped := durableMutationError("record_quiescence", outcome, err); mapped != nil {
		failReason := errors.Join(reason, mapped)
		return errors.Join(failReason, controller.failStop(ctx, failReason), ErrFailClosed)
	}
	return errors.Join(reason, cleanup.Err)
}

func (controller *LaunchController) failStop(ctx context.Context, reason error) error {
	if reason == nil {
		reason = ErrFailClosed
	}
	return controller.authority.FailStop(ctx, reason)
}

type durabilityAction uint8

const (
	durabilityCommitted durabilityAction = iota + 1
	durabilityNotCommitted
	durabilityUnknown
)

func durabilityDecision(outcome DurabilityOutcome) durabilityAction {
	switch outcome {
	case CommittedAndAnchored:
		return durabilityCommitted
	case DefinitelyNotCommitted:
		return durabilityNotCommitted
	case CommitOutcomeUnknown:
		return durabilityUnknown
	default:
		return durabilityUnknown
	}
}

func durableMutationError(step string, outcome DurabilityOutcome, err error) error {
	switch durabilityDecision(outcome) {
	case durabilityCommitted:
		if err == nil {
			return nil
		}
		return fmt.Errorf("%s committed with error: %w", step, err)
	case durabilityNotCommitted:
		if err == nil {
			return fmt.Errorf("%w: %s", ErrDurabilityNotCommitted, step)
		}
		return fmt.Errorf("%w: %s: %v", ErrDurabilityNotCommitted, step, err)
	case durabilityUnknown:
		if err == nil {
			return fmt.Errorf("%w: %s", ErrDurabilityUnknown, step)
		}
		return fmt.Errorf("%w: %s: %v", ErrDurabilityUnknown, step, err)
	default:
		if err == nil {
			return fmt.Errorf("%w: %s returned invalid durability outcome %d", ErrDurabilityUnknown, step, outcome)
		}
		return fmt.Errorf("%w: %s returned invalid durability outcome %d: %v", ErrDurabilityUnknown, step, outcome, err)
	}
}

func validatePreparedGroup(launch LaunchContext, group model.GroupRef) error {
	if err := group.Validate(); err != nil {
		return fmt.Errorf("%w: group_ref: %v", ErrInvalidLaunchRequest, err)
	}
	if !group.Launch.Attempt.Equal(launch.Attempt) {
		return fmt.Errorf("%w: group launch attempt mismatch", ErrInvalidLaunchRequest)
	}
	if group.Launch.Ordinal != launch.Ordinal {
		return fmt.Errorf("%w: group launch ordinal mismatch", ErrInvalidLaunchRequest)
	}
	return nil
}

func validateGrant(launch LaunchContext, grant model.LaunchGrant) error {
	if err := grant.Validate(); err != nil {
		return fmt.Errorf("%w: grant: %v", ErrInvalidLaunchRequest, err)
	}
	if !grant.Attempt.Equal(launch.Attempt) {
		return fmt.Errorf("%w: grant attempt mismatch", ErrInvalidLaunchRequest)
	}
	if grant.Ordinal != launch.Ordinal {
		return fmt.Errorf("%w: grant ordinal mismatch", ErrInvalidLaunchRequest)
	}
	return nil
}

func releaseObservation(group model.GroupRef) (model.ChildIdentity, model.Evidence, error) {
	child := model.ChildIdentity{
		PID:               group.Leader.PID,
		HighResStartToken: group.Leader.HighResStartToken,
	}
	if err := child.Validate(); err != nil {
		return model.ChildIdentity{}, model.Evidence{}, err
	}
	evidence, err := model.NewEvidence("custodian_release", fmt.Sprintf("release acknowledged for custody %s ordinal %s", group.CustodyID, group.Launch.Ordinal))
	if err != nil {
		return model.ChildIdentity{}, model.Evidence{}, err
	}
	return child, evidence, nil
}

func containmentContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

type releaseRecordState struct {
	record bool
	err    error
}

type Result struct {
	Context         LaunchContext
	Group           model.GroupRef
	Grant           model.LaunchGrant
	Exit            command.ExitObservation
	Verified        custodian.VerifiedQuiescence
	Contained       bool
	ReleaseRecorded bool
}

type Process struct {
	controller *LaunchController
	controlCtx context.Context
	context    LaunchContext
	group      model.GroupRef
	grant      model.LaunchGrant
	running    RunningProcess
	failures   *FailureInjector

	releaseOnce  sync.Once
	releaseState chan releaseRecordState

	waitCancel   context.CancelFunc
	waitReturned chan struct{}

	finalOnce sync.Once
	done      chan struct{}

	failClosedMu  sync.Mutex
	failClosedErr error
	failStopOnce  sync.Once
	failStopErr   error
	containmentMu sync.Mutex
	containing    bool

	mu       sync.Mutex
	result   Result
	finalErr error
}

func newProcess(controller *LaunchController, controlCtx context.Context, launch LaunchContext, group model.GroupRef, grant model.LaunchGrant, running RunningProcess, failures *FailureInjector) *Process {
	waitCtx, waitCancel := context.WithCancel(controlCtx)
	process := &Process{
		controller:   controller,
		controlCtx:   controlCtx,
		context:      launch,
		group:        group,
		grant:        grant,
		running:      running,
		failures:     failures,
		releaseState: make(chan releaseRecordState, 1),
		waitCancel:   waitCancel,
		waitReturned: make(chan struct{}),
		done:         make(chan struct{}),
	}
	go process.eagerWait(waitCtx)
	return process
}

func (process *Process) Stdin() io.WriteCloser {
	return process.running.Stdin()
}

func (process *Process) Stdout() io.ReadCloser {
	return process.running.Stdout()
}

func (process *Process) Stderr() io.ReadCloser {
	return process.running.Stderr()
}

func (process *Process) Done() <-chan struct{} {
	return process.done
}

func (process *Process) Wait(ctx context.Context) (command.ExitObservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-process.done:
		result, err := process.final()
		return result.Exit, err
	case <-ctx.Done():
		process.markContaining()
		process.finalizeFromContain(containmentContext(ctx), custodian.QuiescenceCauseContain, command.ExitObservation{}, nil, false)
		result, finalErr := process.final()
		if !result.Contained {
			reason := errors.Join(ctx.Err(), finalErr)
			return result.Exit, errors.Join(reason, process.failStop(containmentContext(ctx), reason), ErrFailClosed)
		}
		return result.Exit, errors.Join(ctx.Err(), finalErr)
	}
}

func (process *Process) Interrupt(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	process.markContaining()
	process.finalizeFromContain(ctx, custodian.QuiescenceCauseContain, command.ExitObservation{}, nil, false)
	_, err := process.Result(ctx)
	return err
}

func (process *Process) FinalResult(ctx context.Context) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-process.done:
		return process.final()
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (process *Process) Result(ctx context.Context) (Result, error) {
	return process.FinalResult(ctx)
}

func (process *Process) ContainAndFailStop(ctx context.Context, reason error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	process.requestFailClosed(reason)
	return process.containAndFailStop(ctx, reason)
}

func (process *Process) containAndFailStop(ctx context.Context, reason error) error {
	process.markContaining()
	process.finalizeFromContain(ctx, custodian.QuiescenceCauseContain, command.ExitObservation{}, reason, false)
	_, finalErr := process.FinalResult(ctx)
	if !errors.Is(finalErr, ErrFailClosed) {
		finalErr = errors.Join(finalErr, reason, process.failStop(ctx, errors.Join(reason, finalErr)), ErrFailClosed)
	}
	return finalErr
}

func (process *Process) enableReleaseRecord() {
	process.releaseOnce.Do(func() {
		process.releaseState <- releaseRecordState{record: true}
	})
}

func (process *Process) disableReleaseRecord(err error) {
	process.releaseOnce.Do(func() {
		process.releaseState <- releaseRecordState{err: err}
	})
}

func (process *Process) failClosedBeforeReleaseRecord(ctx context.Context, reason error) error {
	process.requestFailClosed(reason)
	process.disableReleaseRecord(reason)
	return process.containAndFailStop(ctx, reason)
}

func (process *Process) failClosedAfterReleaseRecord(ctx context.Context, reason error) error {
	process.requestFailClosed(reason)
	process.enableReleaseRecord()
	return process.containAndFailStop(ctx, reason)
}

func (process *Process) handlePostReleaseDurability(ctx context.Context, step string, outcome DurabilityOutcome, stepErr error) error {
	mapped := durableMutationError(step, outcome, stepErr)
	switch durabilityDecision(outcome) {
	case durabilityCommitted:
		if mapped == nil {
			return nil
		}
		return process.failClosedBeforeReleaseRecord(ctx, mapped)
	case durabilityNotCommitted:
		return process.failClosedBeforeReleaseRecord(ctx, mapped)
	case durabilityUnknown:
		return process.failClosedBeforeReleaseRecord(ctx, mapped)
	default:
		return process.failClosedBeforeReleaseRecord(ctx, mapped)
	}
}

func (process *Process) eagerWait(ctx context.Context) {
	exit, verified, cleanup, err := process.running.WaitAndVerify(ctx)
	close(process.waitReturned)
	if err != nil {
		cause := custodian.QuiescenceCauseWait
		priorErr := err
		if ctx.Err() != nil || process.containmentRequested() {
			cause = custodian.QuiescenceCauseContain
			priorErr = nil
		} else {
			process.requestFailClosed(err)
		}
		process.finalizeFromContain(process.controlCtx, cause, exit, priorErr, true)
		return
	}
	process.finalizeWithVerified(process.controlCtx, exit, verified, waitReportedContainment(process.running), true, cleanup.Err)
}

func waitReportedContainment(running RunningProcess) bool {
	reporter, ok := running.(waitContainmentReporter)
	return ok && reporter.WaitContained()
}

func (process *Process) finalizeFromContain(ctx context.Context, cause custodian.QuiescenceCause, priorExit command.ExitObservation, priorErr error, waitReturned bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	process.markContaining()
	if !waitReturned {
		process.waitCancel()
		select {
		case <-process.waitReturned:
		case <-ctx.Done():
		}
	}
	process.finalOnce.Do(func() {
		verified, cleanup, containErr := process.running.ContainAndVerify(ctx, cause)
		process.finish(ctx, priorExit, verified, containErr == nil, containErr == nil, errors.Join(priorErr, containErr, cleanup.Err))
	})
}

func (process *Process) markContaining() {
	process.containmentMu.Lock()
	process.containing = true
	process.containmentMu.Unlock()
}

func (process *Process) containmentRequested() bool {
	process.containmentMu.Lock()
	defer process.containmentMu.Unlock()
	return process.containing
}

func (process *Process) finalizeWithVerified(ctx context.Context, exit command.ExitObservation, verified custodian.VerifiedQuiescence, contained bool, verifiedOK bool, reconcileErr error) {
	process.finalOnce.Do(func() {
		process.finish(ctx, exit, verified, contained, verifiedOK, reconcileErr)
	})
}

func (process *Process) finish(ctx context.Context, exit command.ExitObservation, verified custodian.VerifiedQuiescence, contained bool, verifiedOK bool, reconcileErr error) {
	result := Result{
		Context:   process.context,
		Group:     process.group,
		Grant:     process.grant,
		Exit:      exit,
		Verified:  verified,
		Contained: contained,
	}
	finalErr := reconcileErr
	if verifiedOK {
		state := <-process.releaseState
		result.ReleaseRecorded = state.record
		if err := inject(process.failures, FailAfterWait); err != nil {
			process.requestFailClosed(err)
		}
		if process.failClosedReason() != nil && !result.Contained {
			verifiedOK = process.containFinalResult(ctx, &result, &finalErr)
		}
		if !state.record {
			finalErr = errors.Join(finalErr, ErrReleaseRecordUnavailable, state.err)
		} else if verifiedOK {
			outcome, err := process.controller.RecordQuiescence(ctx, process.context, result.Verified)
			if mapped := durableMutationError("record_quiescence", outcome, err); mapped != nil {
				finalErr = errors.Join(finalErr, mapped)
				process.containFinalResult(ctx, &result, &finalErr)
				process.requestFailClosed(mapped)
			} else if err := inject(process.failures, FailAfterRecordQuiescence); err != nil {
				finalErr = errors.Join(finalErr, err)
				process.containFinalResult(ctx, &result, &finalErr)
				process.requestFailClosed(err)
			}
		}
	}
	if reason := process.failClosedReason(); reason != nil {
		finalErr = errors.Join(finalErr, reason, process.failStop(ctx, errors.Join(reason, finalErr)), ErrFailClosed)
	}
	process.setFinal(result, finalErr)
}

func (process *Process) containFinalResult(ctx context.Context, result *Result, finalErr *error) bool {
	verified, cleanup, containErr := process.running.ContainAndVerify(ctx, custodian.QuiescenceCauseContain)
	if containErr != nil {
		result.Contained = false
		*finalErr = errors.Join(*finalErr, containErr)
		return false
	}
	result.Verified = verified
	result.Contained = true
	*finalErr = errors.Join(*finalErr, cleanup.Err)
	return true
}

func (process *Process) requestFailClosed(reason error) {
	if reason == nil {
		reason = ErrFailClosed
	}
	process.failClosedMu.Lock()
	defer process.failClosedMu.Unlock()
	process.failClosedErr = errors.Join(process.failClosedErr, reason)
}

func (process *Process) failClosedReason() error {
	process.failClosedMu.Lock()
	defer process.failClosedMu.Unlock()
	return process.failClosedErr
}

func (process *Process) failStop(ctx context.Context, reason error) error {
	process.failStopOnce.Do(func() {
		process.failStopErr = process.controller.failStop(ctx, reason)
	})
	return process.failStopErr
}

func (process *Process) setFinal(result Result, err error) {
	process.mu.Lock()
	process.result = result
	process.finalErr = err
	process.mu.Unlock()
	close(process.done)
}

func (process *Process) final() (Result, error) {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.result, process.finalErr
}
