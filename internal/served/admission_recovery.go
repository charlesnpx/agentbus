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

type AdmissionRecoveryReport struct {
	Mode             string `json:"mode,omitempty"`
	WorkItems        int    `json:"workItems"`
	QuiescedLaunches int    `json:"quiescedLaunches"`
	FinalizedJobs    int    `json:"finalizedJobs"`
	RecoveryPasses   int    `json:"recoveryPasses"`
}

func newAdmissionRecoveryExecutor(session *authority.RecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch) *admissionRecoveryExecutor {
	return &admissionRecoveryExecutor{session: session, launch: launchPort, latch: latch}
}

func (e *admissionRecoveryExecutor) Recover(ctx context.Context) error {
	_, err := e.RecoverReport(ctx)
	return err
}

func (e *admissionRecoveryExecutor) RecoverReport(ctx context.Context) (AdmissionRecoveryReport, error) {
	var report AdmissionRecoveryReport
	if e == nil || e.session == nil {
		return report, authority.ErrNotReady
	}
	if e.launch == nil {
		return report, e.failClosed(custodian.ErrSupervisorUnavailable)
	}
	for step := 0; step < admissionRecoveryMaxSteps; step++ {
		report.RecoveryPasses++
		items, err := e.session.WorkItems(ctx)
		if err != nil {
			return report, e.failClosed(err)
		}
		if len(items) == 0 {
			return report, nil
		}
		progressed := false
		for _, item := range items {
			report.WorkItems++
			itemReport, err := e.recoverItem(ctx, item)
			if err != nil {
				if errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
					return report, err
				}
				return report, e.failClosed(err)
			}
			report.QuiescedLaunches += itemReport.QuiescedLaunches
			report.FinalizedJobs += itemReport.FinalizedJobs
			progressed = true
		}
		if !progressed {
			return report, e.failClosed(fmt.Errorf("%w: startup recovery made no progress", authority.ErrRecoveryNeeded))
		}
	}
	return report, e.failClosed(fmt.Errorf("%w: startup recovery did not converge", authority.ErrRecoveryNeeded))
}

func (e *admissionRecoveryExecutor) recoverItem(ctx context.Context, item model.RecoveryWorkItem) (AdmissionRecoveryReport, error) {
	current := item
	var report AdmissionRecoveryReport
	for step := 0; step < admissionRecoveryMaxSteps; step++ {
		if err := current.Validate(); err != nil {
			return report, fmt.Errorf("%w: invalid recovery work item: %v", authority.ErrRecoveryNeeded, err)
		}
		if len(current.Launches) == 0 {
			if err := e.session.FinalizePlanned(ctx, current.Token); err != nil {
				return report, fmt.Errorf("%w: finalize planned recovery for %s: %v", authority.ErrRecoveryNeeded, current.JobID, err)
			}
			report.FinalizedJobs++
			return report, nil
		}

		next, err := e.recoverLaunch(ctx, current, current.Launches[0])
		if err != nil {
			if errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
				return report, err
			}
			return report, err
		}
		report.QuiescedLaunches++
		current = next
	}
	return report, fmt.Errorf("%w: recovery item %s did not converge", authority.ErrRecoveryNeeded, item.JobID)
}

func (e *admissionRecoveryExecutor) recoverLaunch(ctx context.Context, item model.RecoveryWorkItem, recoveryLaunch model.RecoveryLaunch) (model.RecoveryWorkItem, error) {
	verified, cleanup, err := e.launch.ContainAndVerify(ctx, recoveryLaunch.Group, custodian.QuiescenceCauseRecovery)
	if err != nil {
		if errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
			return model.RecoveryWorkItem{}, err
		}
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
