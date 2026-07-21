package served

import (
	"context"
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

const admissionRecoveryMaxSteps = 1024

type admissionRecoveryExecutor struct {
	session *authority.RecoverySession
	launch  launch.CustodianPort
	latch   *SafetyLatch
}

func newAdmissionRecoveryExecutor(session *authority.RecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch) *admissionRecoveryExecutor {
	return &admissionRecoveryExecutor{session: session, launch: launchPort, latch: latch}
}

func (e *admissionRecoveryExecutor) Recover(ctx context.Context) error {
	if e == nil || e.session == nil {
		return authority.ErrNotReady
	}
	if e.launch == nil {
		return e.failClosed(custodian.ErrSupervisorUnavailable)
	}
	for step := 0; step < admissionRecoveryMaxSteps; step++ {
		items, err := e.session.WorkItems(ctx)
		if err != nil {
			return e.failClosed(err)
		}
		if len(items) == 0 {
			return nil
		}
		progressed := false
		for _, item := range items {
			if err := e.recoverItem(ctx, item); err != nil {
				return e.failClosed(err)
			}
			progressed = true
		}
		if !progressed {
			return e.failClosed(fmt.Errorf("%w: startup recovery made no progress", authority.ErrRecoveryNeeded))
		}
	}
	return e.failClosed(fmt.Errorf("%w: startup recovery did not converge", authority.ErrRecoveryNeeded))
}

func (e *admissionRecoveryExecutor) recoverItem(ctx context.Context, item model.RecoveryWorkItem) error {
	current := item
	for step := 0; step < admissionRecoveryMaxSteps; step++ {
		if err := current.Validate(); err != nil {
			return fmt.Errorf("%w: invalid recovery work item: %v", authority.ErrRecoveryNeeded, err)
		}
		if len(current.Launches) == 0 {
			if err := e.session.FinalizePlanned(ctx, current.Token); err != nil {
				return fmt.Errorf("%w: finalize planned recovery for %s: %v", authority.ErrRecoveryNeeded, current.JobID, err)
			}
			return nil
		}

		next, err := e.recoverLaunch(ctx, current, current.Launches[0])
		if err != nil {
			return err
		}
		current = next
	}
	return fmt.Errorf("%w: recovery item %s did not converge", authority.ErrRecoveryNeeded, item.JobID)
}

func (e *admissionRecoveryExecutor) recoverLaunch(ctx context.Context, item model.RecoveryWorkItem, recoveryLaunch model.RecoveryLaunch) (model.RecoveryWorkItem, error) {
	verified, cleanup, err := containLaunchPortWithCleanup(ctx, e.launch, recoveryLaunch.Group, custodian.QuiescenceCauseRecovery)
	if err != nil {
		return model.RecoveryWorkItem{}, fmt.Errorf("%w: contain recovery launch %s ordinal %s: %v", authority.ErrRecoveryNeeded, item.JobID, recoveryLaunch.Ordinal, err)
	}
	if err := item.Validate(); err != nil {
		return model.RecoveryWorkItem{}, fmt.Errorf("%w: invalid recovery work item: %v", authority.ErrRecoveryNeeded, err)
	}
	if err := e.session.RecordQuiescence(ctx, item.Token, recoveryLaunch.Ordinal, verified); err != nil {
		return model.RecoveryWorkItem{}, fmt.Errorf("%w: record recovery quiescence for %s ordinal %s: %v", authority.ErrRecoveryNeeded, item.JobID, recoveryLaunch.Ordinal, err)
	}
	if cleanup.Err != nil {
		return model.RecoveryWorkItem{}, fmt.Errorf("%w: cleanup recovery launch %s ordinal %s: %v", authority.ErrRecoveryNeeded, item.JobID, recoveryLaunch.Ordinal, cleanup.Err)
	}
	next, err := e.session.AdvanceRecovery(ctx, item.Token)
	if err != nil {
		return model.RecoveryWorkItem{}, fmt.Errorf("%w: advance recovery for %s: %v", authority.ErrRecoveryNeeded, item.JobID, err)
	}
	return next, nil
}

func (e *admissionRecoveryExecutor) failClosed(reason error) error {
	if reason == nil {
		reason = authority.ErrRecoveryNeeded
	}
	e.latch.Trip(reason)
	return errors.Join(SafetyFailStopError{Reason: reason}, reason)
}
