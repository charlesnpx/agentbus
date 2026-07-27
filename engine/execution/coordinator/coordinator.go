package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrCoordinatorNotReady       = errors.New("coordinator not ready")
	ErrAuthorityRequired         = errors.New("coordinator authority is required")
	ErrLaunchContainmentRequired = errors.New("coordinator launch containment is required")
	ErrFatalRecovery             = errors.New("coordinator fatal recovery plan")
	ErrAlreadyFinalized          = errors.New("coordinator job already finalized")
)

type AlreadyFinalizedError struct {
	JobID model.JobID
	Cause error
}

func (e AlreadyFinalizedError) Error() string {
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", ErrAlreadyFinalized, e.JobID)
	}
	return fmt.Sprintf("%s: %s: %v", ErrAlreadyFinalized, e.JobID, e.Cause)
}

func (e AlreadyFinalizedError) Unwrap() error {
	return e.Cause
}

func (e AlreadyFinalizedError) Is(target error) bool {
	return target == ErrAlreadyFinalized
}

type AdmissionAuthority interface {
	RecordQuiescence(context.Context, model.JobID, model.LaunchOrdinal, custodian.VerifiedQuiescence) (StepResult, error)
	RequestCancel(context.Context, model.JobID) (StepResult, error)
	RecordOutcome(context.Context, model.JobID, model.AttemptRef, model.Outcome) (StepResult, error)
	RecordResult(context.Context, model.JobID, model.AttemptRef, model.ResultReceipt) (StepResult, error)
	Finalize(context.Context, model.JobID, model.AttemptRef, model.TerminalIntent) (StepResult, error)
	Snapshot(context.Context, model.JobID) (JobSnapshot, error)
	RecoveryPlan(context.Context, model.JobID, model.RecoveryTrigger) (model.RecoveryPlan, error)
	HasOwnedWork(context.Context) (bool, error)
	FailStop(context.Context, error) error
}

type StepResult struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
	Changed    bool
}

type JobSnapshot struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
}

type Coordinator struct {
	authority         AdmissionAuthority
	launchContainment LaunchContainment
	results           ResultPublisher
	shutdownPoll      time.Duration
}

type LaunchContainment interface {
	ContainAndVerify(context.Context, launch.LaunchContext, model.GroupRef, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error)
}

func New(authority AdmissionAuthority, launchContainment LaunchContainment, results ResultPublisher, owner model.OwnerID) (*Coordinator, error) {
	if authority == nil {
		return nil, ErrAuthorityRequired
	}
	if launchContainment == nil {
		return nil, ErrLaunchContainmentRequired
	}
	if err := owner.Validate(); err != nil {
		return nil, fmt.Errorf("owner: %w", err)
	}
	return &Coordinator{
		authority:         authority,
		launchContainment: launchContainment,
		results:           results,
		shutdownPoll:      10 * time.Millisecond,
	}, nil
}

func (c *Coordinator) Snapshot(ctx context.Context, jobID model.JobID) (JobSnapshot, error) {
	if err := c.ready(); err != nil {
		return JobSnapshot{}, err
	}
	return c.authority.Snapshot(ctx, jobID)
}

func (c *Coordinator) Complete(ctx context.Context, jobID model.JobID, outcome model.Outcome, result []byte, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	if !terminalOutcome(outcome) {
		return fmt.Errorf("terminal outcome required: %s", outcome)
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	if _, err := c.authority.RecordOutcome(ctx, jobID, snapshot.Record.Attempt.Ref, outcome); err != nil {
		return c.alreadyFinalizedError(ctx, jobID, err)
	}
	if completionOutcome(outcome) {
		if c.results == nil {
			return errors.New("result publisher is required for completed outcomes")
		}
		if err := c.publishResult(ctx, jobID, result, injector); err != nil {
			return err
		}
	}
	snapshot, err = c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	_, err = c.authority.Finalize(ctx, jobID, snapshot.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: outcome,
		Cause:   model.CauseCompletedNormally,
	})
	if err != nil {
		return c.alreadyFinalizedError(ctx, jobID, err)
	}
	return nil
}

func (c *Coordinator) Finalize(ctx context.Context, jobID model.JobID, intent model.TerminalIntent) error {
	if err := c.ready(); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	_, err = c.authority.Finalize(ctx, jobID, snapshot.Record.Attempt.Ref, intent)
	if err != nil {
		return c.alreadyFinalizedError(ctx, jobID, err)
	}
	return nil
}

func (c *Coordinator) Cancel(ctx context.Context, jobID model.JobID, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	if _, err := c.authority.RequestCancel(ctx, jobID); err != nil {
		return c.alreadyFinalizedError(ctx, jobID, err)
	}
	return c.recover(ctx, jobID, model.RecoveryCancelAfterGrant, nil, injector)
}

func (c *Coordinator) Recover(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	return c.recover(ctx, jobID, trigger, nil, injector)
}

func (c *Coordinator) HasOwnedWork(ctx context.Context) (bool, error) {
	if err := c.ready(); err != nil {
		return false, err
	}
	return c.authority.HasOwnedWork(ctx)
}

func (c *Coordinator) Shutdown(ctx context.Context) error {
	if err := c.ready(); err != nil {
		return err
	}
	poll := c.shutdownPoll
	if poll <= 0 {
		poll = 10 * time.Millisecond
	}
	for {
		owned, err := c.authority.HasOwnedWork(ctx)
		if err != nil {
			return err
		}
		if !owned {
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Coordinator) publishResult(ctx context.Context, jobID model.JobID, payload []byte, injector *FailureInjector) error {
	if err := inject(injector, FailResultTempWrite); err != nil {
		return err
	}
	published, err := c.results.Publish(ctx, jobID, payload)
	if err != nil {
		return err
	}
	if err := inject(injector, FailResultFsync); err != nil {
		return err
	}
	verified, err := c.results.Verify(ctx, published.Result)
	if err != nil {
		return err
	}
	if err := inject(injector, FailResultRename); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	_, err = c.authority.RecordResult(ctx, jobID, snapshot.Record.Attempt.Ref, verified)
	if err != nil {
		return c.alreadyFinalizedError(ctx, jobID, err)
	}
	return nil
}

func (c *Coordinator) recover(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger, cause error, injector *FailureInjector) error {
	for i := 0; i < 8; i++ {
		plan, err := c.authority.RecoveryPlan(ctx, jobID, trigger)
		if err != nil {
			if cause != nil {
				return fmt.Errorf("%w; recovery plan: %v", cause, err)
			}
			return err
		}
		switch plan.Next.Kind {
		case model.RecoveryFinalizeCertified:
			if plan.Next.Finalize == nil {
				return cause
			}
			if _, err := c.authority.Finalize(ctx, jobID, plan.Next.Finalize.Ref, plan.Next.Finalize.Intent); err != nil {
				err = c.alreadyFinalizedError(ctx, jobID, err)
				if cause != nil {
					return errors.Join(cause, fmt.Errorf("finalize recovery: %w", err))
				}
				return err
			}
			return cause
		case model.RecoveryRetireThenFinalize:
			snapshot, err := c.authority.Snapshot(ctx, jobID)
			if err != nil {
				return err
			}
			if err := c.retire(ctx, snapshot.Record, injector); err != nil {
				if recoveryAborted(err) {
					return err
				}
				if physicalCleanupUnresolved(err) {
					return c.finalizeUnresolved(ctx, jobID, trigger, cause, err)
				}
				return c.failStop(ctx, err)
			}
		case model.RecoveryContainThenFinalize:
			snapshot, err := c.authority.Snapshot(ctx, jobID)
			if err != nil {
				return err
			}
			if err := c.contain(ctx, snapshot.Record, injector); err != nil {
				if recoveryAborted(err) {
					return err
				}
				if physicalCleanupUnresolved(err) {
					return c.finalizeUnresolved(ctx, jobID, trigger, cause, err)
				}
				return c.failStop(ctx, err)
			}
		case model.RecoveryAwaitResultCertificate:
			if err := c.awaitResultCertificateProgress(ctx, jobID, plan.BasedOnRevision, trigger); err != nil {
				return err
			}
		case model.RecoveryFatalUnprovable:
			err := fmt.Errorf("%w: %s trigger %d", ErrFatalRecovery, jobID, trigger)
			if cause != nil {
				err = fmt.Errorf("%w; %v", cause, err)
			}
			return c.failStop(ctx, err)
		default:
			return c.failStop(ctx, fmt.Errorf("%w: unknown recovery action %d", ErrFatalRecovery, plan.Next.Kind))
		}
	}
	return c.failStop(ctx, fmt.Errorf("%w: recovery did not converge for %s", ErrFatalRecovery, jobID))
}

func (c *Coordinator) awaitResultCertificateProgress(ctx context.Context, jobID model.JobID, basedOn uint64, trigger model.RecoveryTrigger) error {
	poll := c.shutdownPoll
	if poll <= 0 {
		poll = 10 * time.Millisecond
	}
	for {
		snapshot, err := c.authority.Snapshot(ctx, jobID)
		if err != nil {
			return err
		}
		if snapshot.Record.Terminal != nil ||
			snapshot.Record.Result != nil ||
			snapshot.Record.Revision != basedOn ||
			!awaitingCompletionResult(snapshot.Record, trigger) {
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Coordinator) finalizeUnresolved(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger, cause error, unresolved error) error {
	if err := recoveryAbortedBeforeOperation(ctx); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return c.failStopUnlessRecoveryAborted(ctx, errors.Join(unresolved, err))
	}
	intent, err := model.RecoveryTerminalIntent(snapshot.Record, trigger, false)
	if err != nil {
		return c.failStopUnlessRecoveryAborted(ctx, errors.Join(unresolved, err))
	}
	if err := recoveryAbortedBeforeOperation(ctx); err != nil {
		return err
	}
	if _, err := c.authority.Finalize(ctx, jobID, snapshot.Record.Attempt.Ref, intent); err != nil {
		err = c.alreadyFinalizedError(ctx, jobID, err)
		if errors.Is(err, ErrAlreadyFinalized) {
			return err
		}
		return c.failStopUnlessRecoveryAborted(ctx, errors.Join(unresolved, err))
	}
	return cause
}

func (c *Coordinator) alreadyFinalizedError(ctx context.Context, jobID model.JobID, err error) error {
	if err == nil {
		return nil
	}
	var absorbed model.TerminalAbsorbedError
	if !errors.As(err, &absorbed) {
		return err
	}
	if absorbed.Terminal.JobID != jobID {
		return errors.Join(err, fmt.Errorf("absorbed terminal job mismatch: got %s want %s", absorbed.Terminal.JobID, jobID))
	}
	if validErr := absorbed.Terminal.Validate(); validErr != nil {
		return errors.Join(err, fmt.Errorf("absorbing terminal certificate is invalid: %w", validErr))
	}
	return AlreadyFinalizedError{JobID: jobID, Cause: err}
}

func awaitingCompletionResult(record model.SafetyRecord, trigger model.RecoveryTrigger) bool {
	return trigger == model.RecoveryCancelAfterGrant &&
		record.Cancel != nil &&
		record.Outcome != nil &&
		completionOutcome(record.Outcome.Outcome) &&
		record.Result == nil
}

func (c *Coordinator) contain(ctx context.Context, record model.SafetyRecord, injector *FailureInjector) error {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Quiescence != nil {
			continue
		}
		group, err := groupFromRecord(record, ordinal)
		if err != nil {
			return err
		}
		if err := inject(injector, FailContainSignal); err != nil {
			return err
		}
		verified, cleanup, err := c.launchContainment.ContainAndVerify(ctx, launchContext(record, ordinal), group, custodian.QuiescenceCauseContain)
		if err != nil {
			return err
		}
		if err := inject(injector, FailContainVerified); err != nil {
			return err
		}
		applied, err := c.authority.RecordQuiescence(ctx, record.JobID, ordinal, verified)
		if err != nil {
			return fmt.Errorf("record containment quiescence: %w", err)
		}
		record = applied.Record
		reportCleanupWarning(record.JobID, ordinal, custodian.QuiescenceCauseContain, cleanup.Err)
	}
	return nil
}

func (c *Coordinator) retire(ctx context.Context, record model.SafetyRecord, injector *FailureInjector) error {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Quiescence != nil {
			continue
		}
		group, err := groupFromRecord(record, ordinal)
		if err != nil {
			return err
		}
		if err := inject(injector, FailRetireClose); err != nil {
			return err
		}
		verified, cleanup, err := c.launchContainment.ContainAndVerify(ctx, launchContext(record, ordinal), group, custodian.QuiescenceCauseRecovery)
		if err != nil {
			return err
		}
		if err := inject(injector, FailRetireFsync); err != nil {
			return err
		}
		applied, err := c.authority.RecordQuiescence(ctx, record.JobID, ordinal, verified)
		if err != nil {
			return fmt.Errorf("record retirement quiescence: %w", err)
		}
		record = applied.Record
		reportCleanupWarning(record.JobID, ordinal, custodian.QuiescenceCauseRecovery, cleanup.Err)
	}
	return nil
}

func reportCleanupWarning(jobID model.JobID, ordinal model.LaunchOrdinal, cause custodian.QuiescenceCause, err error) {
	if err == nil {
		return
	}
	log.Printf("agentbus execution: cleanup warning: job=%s ordinal=%s phase=%s: %v", jobID, ordinal, cause, err)
}

func recoveryAborted(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func recoveryAbortedBeforeOperation(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); recoveryAborted(err) {
		return err
	}
	return nil
}

func physicalCleanupUnresolved(err error) bool {
	return custodian.IsCleanupUnresolved(err) || errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved)
}

func (c *Coordinator) failStopUnlessRecoveryAborted(ctx context.Context, err error) error {
	if recoveryAborted(err) {
		return err
	}
	return c.failStop(ctx, err)
}

func (c *Coordinator) ready() error {
	if c == nil || c.authority == nil || c.launchContainment == nil {
		return ErrCoordinatorNotReady
	}
	return nil
}

func (c *Coordinator) failStop(ctx context.Context, err error) error {
	if c != nil && c.authority != nil && err != nil {
		_ = c.authority.FailStop(ctx, err)
	}
	return err
}

func groupFromRecord(record model.SafetyRecord, ordinal model.LaunchOrdinal) (model.GroupRef, error) {
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil {
		return model.GroupRef{}, fmt.Errorf("group reference is not bound for %s ordinal %s", record.JobID, ordinal)
	}
	group := *launch.Group
	if err := group.Validate(); err != nil {
		return model.GroupRef{}, fmt.Errorf("group reference is invalid for %s ordinal %s: %w", record.JobID, ordinal, err)
	}
	if !group.Launch.Attempt.Equal(record.Attempt.Ref) {
		return model.GroupRef{}, fmt.Errorf("group reference attempt mismatch for %s ordinal %s", record.JobID, ordinal)
	}
	if group.Launch.Ordinal != ordinal {
		return model.GroupRef{}, fmt.Errorf("group reference ordinal mismatch for %s ordinal %s", record.JobID, ordinal)
	}
	return group, nil
}

func launchContext(record model.SafetyRecord, ordinal model.LaunchOrdinal) launch.LaunchContext {
	return launch.LaunchContext{
		JobID:   record.JobID,
		Attempt: record.Attempt.Ref,
		Ordinal: ordinal,
	}
}

func terminalOutcome(outcome model.Outcome) bool {
	switch outcome {
	case model.OutcomeCompleted, model.OutcomeCompletedNoncompliant, model.OutcomeFailed, model.OutcomeTimedOut, model.OutcomeCanceled, model.OutcomeReaped, model.OutcomeInterrupted, model.OutcomeQuarantined, model.OutcomeOrphaned:
		return true
	default:
		return false
	}
}

func completionOutcome(outcome model.Outcome) bool {
	return outcome == model.OutcomeCompleted || outcome == model.OutcomeCompletedNoncompliant
}
