package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrCoordinatorNotReady = errors.New("coordinator not ready")
	ErrSupervisorRequired  = errors.New("coordinator supervisor is required")
	ErrAuthorityRequired   = errors.New("coordinator authority is required")
	ErrFatalRecovery       = errors.New("coordinator fatal recovery plan")
)

type AdmissionAuthority interface {
	Accept(context.Context, AdmissionRequest) (AdmissionResult, error)
	BindGroup(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.GroupRef) (StepResult, error)
	CommitGrant(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.PermitNonce) (StepResult, error)
	RecordRelease(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.ChildIdentity, model.Evidence) (StepResult, error)
	RecordQuiescence(context.Context, model.JobID, model.LaunchOrdinal, custodian.VerifiedQuiescence) (StepResult, error)
	RequestCancel(context.Context, model.JobID) (StepResult, error)
	RecordOutcome(context.Context, model.JobID, model.AttemptRef, model.Outcome) (StepResult, error)
	RecordResult(context.Context, model.JobID, model.AttemptRef, model.ResultReceipt) (StepResult, error)
	Finalize(context.Context, model.JobID, model.AttemptRef, model.TerminalIntent) (StepResult, error)
	Snapshot(context.Context, model.JobID) (JobSnapshot, error)
	RecoveryPlan(context.Context, model.JobID, model.RecoveryTrigger) (model.RecoveryPlan, error)
	ClaimPending(context.Context, model.AttemptRef, model.OwnerID) error
	HasOwnedWork(context.Context) (bool, error)
	FailStop(context.Context, error) error
}

type AdmissionRequest struct {
	RequestKey   model.RequestKey
	TaskIdentity model.TaskIdentity
	Mode         model.Mode
	SessionID    string
}

type AdmissionResult struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
	Replayed   bool
}

type StepResult struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
	Durability authority.DurabilityOutcome
	Changed    bool
}

type JobSnapshot struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
}

type Coordinator struct {
	authority    AdmissionAuthority
	supervisor   Supervisor
	results      ResultPublisher
	owner        model.OwnerID
	shutdownPoll time.Duration
}

func New(authority AdmissionAuthority, supervisor Supervisor, results ResultPublisher, owner model.OwnerID) (*Coordinator, error) {
	if authority == nil {
		return nil, ErrAuthorityRequired
	}
	if supervisor == nil {
		return nil, ErrSupervisorRequired
	}
	if err := owner.Validate(); err != nil {
		return nil, fmt.Errorf("owner: %w", err)
	}
	return &Coordinator{
		authority:    authority,
		supervisor:   supervisor,
		results:      results,
		owner:        owner,
		shutdownPoll: 10 * time.Millisecond,
	}, nil
}

func (c *Coordinator) Submit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error) {
	if err := c.ready(); err != nil {
		return AdmissionResult{}, err
	}
	accepted, err := c.authority.Accept(ctx, request)
	if err != nil {
		return AdmissionResult{}, err
	}
	if accepted.Record.Terminal != nil {
		return accepted, nil
	}
	if err := c.authority.ClaimPending(ctx, accepted.Record.Attempt.Ref, c.owner); err != nil {
		return accepted, c.failStop(ctx, fmt.Errorf("claim accepted attempt %s: %w", accepted.Record.JobID, err))
	}
	return accepted, nil
}

func (c *Coordinator) Snapshot(ctx context.Context, jobID model.JobID) (JobSnapshot, error) {
	if err := c.ready(); err != nil {
		return JobSnapshot{}, err
	}
	return c.authority.Snapshot(ctx, jobID)
}

func (c *Coordinator) PrepareSupervisor(ctx context.Context, jobID model.JobID, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	if snapshot.Record.Terminal != nil {
		return nil
	}
	if launch, ok := snapshot.Record.Attempt.Launches.Get(model.LaunchOrdinalOne); ok && launch.Group != nil {
		return nil
	}
	if err := inject(injector, FailSupervisorPrepareBefore); err != nil {
		return err
	}
	prepared, err := c.supervisor.Prepare(ctx, LaunchPlan{
		JobID:        snapshot.Record.JobID,
		Ref:          snapshot.Record.Attempt.Ref,
		Ordinal:      model.LaunchOrdinalOne,
		RequestKey:   snapshot.Record.RequestKey,
		TaskIdentity: snapshot.Record.TaskIdentity,
		SessionID:    snapshot.Projection.SessionID,
	})
	if err != nil {
		return err
	}
	if err := prepared.ValidateFor(snapshot.Record.Attempt.Ref); err != nil {
		return c.failStop(ctx, err)
	}
	if err := inject(injector, FailSupervisorPrepareAfter); err != nil {
		return c.failStop(ctx, err)
	}
	applied, err := c.authority.BindGroup(ctx, jobID, snapshot.Record.Attempt.Ref, model.LaunchOrdinalOne, prepared.Group)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "bind prepared supervisor", applied, err); handled {
		return failErr
	}
	if err != nil {
		return c.failStop(ctx, fmt.Errorf("bind prepared supervisor: %w", err))
	}
	return nil
}

func (c *Coordinator) GrantPermit(ctx context.Context, jobID model.JobID, launchOrdinal uint8, nonce model.PermitNonce, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	ordinal, err := model.NewLaunchOrdinal(launchOrdinal)
	if err != nil {
		return err
	}
	if err := model.LaunchNonce(nonce).Validate(); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	if launch, ok := snapshot.Record.Attempt.Launches.Get(ordinal); !ok || launch.Group == nil {
		prepared, err := c.supervisor.Prepare(ctx, LaunchPlan{
			JobID:        snapshot.Record.JobID,
			Ref:          snapshot.Record.Attempt.Ref,
			Ordinal:      ordinal,
			RequestKey:   snapshot.Record.RequestKey,
			TaskIdentity: snapshot.Record.TaskIdentity,
			SessionID:    snapshot.Projection.SessionID,
		})
		if err != nil {
			return err
		}
		if err := prepared.ValidateFor(snapshot.Record.Attempt.Ref); err != nil {
			return c.failStop(ctx, err)
		}
		applied, err := c.authority.BindGroup(ctx, jobID, snapshot.Record.Attempt.Ref, ordinal, prepared.Group)
		if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "bind grant supervisor", applied, err); handled {
			return failErr
		}
		if err != nil {
			return err
		}
		snapshot.Record = applied.Record
	}
	if err := inject(injector, FailGrantBeforeCommit); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	applied, err := c.authority.CommitGrant(ctx, jobID, snapshot.Record.Attempt.Ref, ordinal, nonce)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "commit launch grant", applied, err); handled {
		return failErr
	}
	if err != nil {
		return err
	}
	launch, ok := applied.Record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Grant == nil {
		return c.failStop(ctx, fmt.Errorf("authority did not return launch grant %s for %s", ordinal, jobID))
	}
	prepared, err := preparedFromRecord(applied.Record, ordinal)
	if err != nil {
		return c.failStop(ctx, err)
	}
	if err := inject(injector, FailGrantAfterCommit); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	if err := inject(injector, FailPermitSendBefore); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	if err := c.supervisor.SendPermit(ctx, prepared, *launch.Grant); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	if err := inject(injector, FailPermitSendAfter); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	return nil
}

func (c *Coordinator) Start(ctx context.Context, jobID model.JobID, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	snapshot, err := c.authority.Snapshot(ctx, jobID)
	if err != nil {
		return err
	}
	grant, err := nextUnconsumedGrant(snapshot.Record)
	if err != nil {
		return err
	}
	prepared, err := preparedFromRecord(snapshot.Record, grant.Ordinal)
	if err != nil {
		return err
	}
	if err := inject(injector, FailLaunchForked); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	if err := inject(injector, FailLaunchExeced); err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	observation, err := c.supervisor.ObserveLaunch(ctx, prepared, grant)
	if err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	if observation.Ordinal == 0 {
		observation.Ordinal = grant.Ordinal
	}
	if err := observation.ValidateFor(grant); err != nil {
		return c.failStop(ctx, err)
	}
	applied, err := c.authority.RecordRelease(ctx, jobID, snapshot.Record.Attempt.Ref, observation.Ordinal, observation.Child, observation.Evidence)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record launch release", applied, err); handled {
		return failErr
	}
	if err != nil {
		return c.recover(ctx, jobID, model.RecoveryPostGrantFailure, err, injector)
	}
	return nil
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
	applied, err := c.authority.RecordOutcome(ctx, jobID, snapshot.Record.Attempt.Ref, outcome)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record outcome", applied, err); handled {
		return failErr
	}
	if err != nil {
		return err
	}
	record := applied.Record
	if err := c.certifyQuiescence(ctx, &record, injector); err != nil {
		return err
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
	applied, err = c.authority.Finalize(ctx, jobID, snapshot.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: outcome,
		Cause:   model.CauseCompletedNormally,
	})
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "finalize completion", applied, err); handled {
		return failErr
	}
	return err
}

func (c *Coordinator) Cancel(ctx context.Context, jobID model.JobID, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	applied, err := c.authority.RequestCancel(ctx, jobID)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "request cancel", applied, err); handled {
		return failErr
	}
	if err != nil {
		return err
	}
	return c.recover(ctx, jobID, model.RecoveryCancelAfterGrant, nil, injector)
}

func (c *Coordinator) LiveSupervisorLoss(ctx context.Context, jobID model.JobID, injector *FailureInjector) error {
	if err := c.ready(); err != nil {
		return err
	}
	return c.recover(ctx, jobID, model.RecoveryLiveLoss, nil, injector)
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

func (c *Coordinator) certifyQuiescence(ctx context.Context, record *model.SafetyRecord, injector *FailureInjector) error {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Released == nil || launch.Quiescence != nil {
			continue
		}
		prepared, err := preparedFromRecord(*record, ordinal)
		if err != nil {
			return err
		}
		verified, err := c.supervisor.VerifyQuiescence(ctx, prepared, *launch.Released)
		if err != nil {
			return err
		}
		if err := inject(injector, FailLaunchQuiescent); err != nil {
			return err
		}
		applied, err := c.authority.RecordQuiescence(ctx, record.JobID, ordinal, verified)
		if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record verified quiescence", applied, err); handled {
			return failErr
		}
		if err != nil {
			return err
		}
		*record = applied.Record
	}
	return nil
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
	applied, err := c.authority.RecordResult(ctx, jobID, snapshot.Record.Attempt.Ref, verified)
	if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record result", applied, err); handled {
		return failErr
	}
	return err
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
			applied, err := c.authority.Finalize(ctx, jobID, plan.Next.Finalize.Ref, plan.Next.Finalize.Intent)
			if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "finalize recovery", applied, err); handled {
				return failErr
			}
			if err != nil {
				if cause != nil {
					return fmt.Errorf("%w; finalize recovery: %v", cause, err)
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
				return c.failStop(ctx, err)
			}
		case model.RecoveryContainThenFinalize:
			snapshot, err := c.authority.Snapshot(ctx, jobID)
			if err != nil {
				return err
			}
			if err := c.contain(ctx, snapshot.Record, injector); err != nil {
				return c.failStop(ctx, err)
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

func (c *Coordinator) contain(ctx context.Context, record model.SafetyRecord, injector *FailureInjector) error {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Quiescence != nil {
			continue
		}
		prepared, err := preparedFromRecord(record, ordinal)
		if err != nil {
			return err
		}
		if err := inject(injector, FailContainSignal); err != nil {
			return err
		}
		verified, err := c.supervisor.Contain(ctx, prepared)
		if err != nil {
			return err
		}
		if err := inject(injector, FailContainVerified); err != nil {
			return err
		}
		applied, err := c.authority.RecordQuiescence(ctx, record.JobID, ordinal, verified)
		if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record containment quiescence", applied, err); handled {
			return failErr
		}
		if err != nil {
			return c.failStop(ctx, fmt.Errorf("record containment quiescence: %w", err))
		}
		record = applied.Record
	}
	return nil
}

func (c *Coordinator) retire(ctx context.Context, record model.SafetyRecord, injector *FailureInjector) error {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Quiescence != nil {
			continue
		}
		prepared, err := preparedFromRecord(record, ordinal)
		if err != nil {
			return err
		}
		if err := inject(injector, FailRetireClose); err != nil {
			return err
		}
		verified, err := c.supervisor.Retire(ctx, prepared)
		if err != nil {
			return err
		}
		if err := inject(injector, FailRetireFsync); err != nil {
			return err
		}
		applied, err := c.authority.RecordQuiescence(ctx, record.JobID, ordinal, verified)
		if handled, failErr := c.failStopOnAmbiguousDurability(ctx, "record retirement quiescence", applied, err); handled {
			return failErr
		}
		if err != nil {
			return c.failStop(ctx, fmt.Errorf("record retirement quiescence: %w", err))
		}
		record = applied.Record
	}
	return nil
}

func (c *Coordinator) ready() error {
	if c == nil || c.authority == nil {
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

func (c *Coordinator) failStopOnAmbiguousDurability(ctx context.Context, action string, result StepResult, err error) (bool, error) {
	if authority.SafeActionForGrantDurability(result.Durability) != authority.ContainFailStop {
		return false, nil
	}
	containErr := c.containAmbiguousDurability(ctx, result.Record)
	var reason error
	if err != nil {
		reason = fmt.Errorf("%s durability outcome %d: %w", action, result.Durability, err)
	} else {
		reason = fmt.Errorf("%s durability outcome %d", action, result.Durability)
	}
	if containErr != nil {
		reason = errors.Join(reason, fmt.Errorf("contain ambiguous durability: %w", containErr))
	}
	return true, c.failStop(ctx, reason)
}

func (c *Coordinator) containAmbiguousDurability(ctx context.Context, record model.SafetyRecord) error {
	if c == nil || c.supervisor == nil {
		return ErrSupervisorRequired
	}
	var containErr error
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Group == nil || launch.Quiescence != nil {
			continue
		}
		prepared, err := preparedFromRecord(record, ordinal)
		if err != nil {
			containErr = errors.Join(containErr, err)
			continue
		}
		if _, err := c.supervisor.Contain(ctx, prepared); err != nil {
			containErr = errors.Join(containErr, err)
		}
	}
	return containErr
}

func preparedFromRecord(record model.SafetyRecord, ordinal model.LaunchOrdinal) (PreparedSupervisor, error) {
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil {
		return PreparedSupervisor{}, fmt.Errorf("group reference is not bound for %s ordinal %s", record.JobID, ordinal)
	}
	prepared := PreparedSupervisor{Ref: record.Attempt.Ref, Ordinal: ordinal, Group: *launch.Group}
	if err := prepared.ValidateFor(record.Attempt.Ref); err != nil {
		return PreparedSupervisor{}, err
	}
	return prepared, nil
}

func nextUnconsumedGrant(record model.SafetyRecord) (model.LaunchGrant, error) {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Grant == nil {
			continue
		}
		if launch.Released == nil {
			return *launch.Grant, nil
		}
	}
	return model.LaunchGrant{}, fmt.Errorf("no unconsumed launch grant for %s", record.JobID)
}

func terminalOutcome(outcome model.Outcome) bool {
	switch outcome {
	case model.OutcomeCompleted, model.OutcomeCompletedNoncompliant, model.OutcomeFailed, model.OutcomeTimedOut, model.OutcomeCanceled, model.OutcomeReaped, model.OutcomeInterrupted, model.OutcomeQuarantined:
		return true
	default:
		return false
	}
}

func completionOutcome(outcome model.Outcome) bool {
	return outcome == model.OutcomeCompleted || outcome == model.OutcomeCompletedNoncompliant
}
