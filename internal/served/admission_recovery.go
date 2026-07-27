package served

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

const admissionRecoveryMaxSteps = 1024

type admissionRecoveryExecutor struct {
	session admissionRecoverySession
	launch  launch.CustodianPort
	latch   *SafetyLatch
}

type admissionRecoverySession interface {
	WorkItems(context.Context) ([]model.RecoveryWorkItem, error)
	AdvanceRecovery(context.Context, model.RecoveryToken) (model.RecoveryWorkItem, error)
	FinalizePlanned(context.Context, model.RecoveryToken) error
	FinalizeUnresolved(context.Context, model.RecoveryToken) (model.SafetyRecord, error)
	RecordQuiescence(context.Context, any, model.LaunchOrdinal, custodian.VerifiedQuiescence) error
}

type AdmissionRecoveryReport struct {
	Mode               string `json:"mode,omitempty"`
	WorkItems          int    `json:"workItems"`
	QuiescedLaunches   int    `json:"quiescedLaunches"`
	FinalizedJobs      int    `json:"finalizedJobs"`
	OrphanedJobs       int    `json:"orphanedJobs"`
	UnresolvedLaunches int    `json:"unresolvedLaunches"`
	CleanupWarnings    int    `json:"cleanupWarnings,omitempty"`
	RecoveryPasses     int    `json:"recoveryPasses"`
}

func newAdmissionRecoveryExecutor(session admissionRecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch) *admissionRecoveryExecutor {
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
			if recoveryAborted(err) {
				return report, err
			}
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
				if recoveryAborted(err) {
					return report, err
				}
				return report, e.failClosed(err)
			}
			report.QuiescedLaunches += itemReport.QuiescedLaunches
			report.FinalizedJobs += itemReport.FinalizedJobs
			report.OrphanedJobs += itemReport.OrphanedJobs
			report.UnresolvedLaunches += itemReport.UnresolvedLaunches
			report.CleanupWarnings += itemReport.CleanupWarnings
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
	unresolved := map[model.LaunchOrdinal]bool{}
	for step := 0; step < admissionRecoveryMaxSteps; step++ {
		if err := current.Validate(); err != nil {
			return report, fmt.Errorf("%w: invalid recovery work item: %v", authority.ErrRecoveryNeeded, err)
		}
		if len(current.Launches) == 0 {
			if err := e.session.FinalizePlanned(ctx, current.Token); err != nil {
				return report, fmt.Errorf("%w: finalize planned recovery for %s: %w", authority.ErrRecoveryNeeded, current.JobID, err)
			}
			report.FinalizedJobs++
			return report, nil
		}

		progressed := false
		for _, recoveryLaunch := range current.Launches {
			if unresolved[recoveryLaunch.Ordinal] {
				continue
			}
			next, launchUnresolved, cleanupWarning, err := e.recoverLaunch(ctx, current, recoveryLaunch)
			if err != nil {
				return report, err
			}
			if launchUnresolved {
				unresolved[recoveryLaunch.Ordinal] = true
				report.UnresolvedLaunches++
				continue
			}
			if cleanupWarning != nil {
				report.CleanupWarnings++
			}
			report.QuiescedLaunches++
			current = next
			progressed = true
			break
		}
		if progressed {
			continue
		}
		if len(unresolved) != 0 {
			finalized, err := e.session.FinalizeUnresolved(ctx, current.Token)
			if err != nil {
				return report, fmt.Errorf("%w: finalize unresolved recovery for %s: %w", authority.ErrRecoveryNeeded, current.JobID, err)
			}
			report.FinalizedJobs++
			if finalized.Terminal != nil && finalized.Terminal.Outcome == model.OutcomeOrphaned {
				report.OrphanedJobs++
			}
			return report, nil
		}
		return report, fmt.Errorf("%w: recovery item %s made no launch progress", authority.ErrRecoveryNeeded, item.JobID)
	}
	return report, fmt.Errorf("%w: recovery item %s did not converge", authority.ErrRecoveryNeeded, item.JobID)
}

func (e *admissionRecoveryExecutor) recoverLaunch(ctx context.Context, item model.RecoveryWorkItem, recoveryLaunch model.RecoveryLaunch) (model.RecoveryWorkItem, bool, error, error) {
	verified, cleanup, err := e.launch.ContainAndVerify(ctx, recoveryLaunch.Group, custodian.QuiescenceCauseRecovery)
	if err != nil {
		if custodian.IsCleanupUnresolved(err) {
			return model.RecoveryWorkItem{}, true, nil, nil
		}
		if recoveryAborted(err) {
			return model.RecoveryWorkItem{}, false, nil, err
		}
		return model.RecoveryWorkItem{}, false, nil, fmt.Errorf("%w: contain recovery launch %s ordinal %s: %w", authority.ErrRecoveryNeeded, item.JobID, recoveryLaunch.Ordinal, err)
	}
	if err := item.Validate(); err != nil {
		return model.RecoveryWorkItem{}, false, nil, fmt.Errorf("%w: invalid recovery work item: %v", authority.ErrRecoveryNeeded, err)
	}
	if err := e.session.RecordQuiescence(ctx, item.Token, recoveryLaunch.Ordinal, verified); err != nil {
		if recoveryAborted(err) {
			return model.RecoveryWorkItem{}, false, nil, err
		}
		return model.RecoveryWorkItem{}, false, nil, fmt.Errorf("%w: record recovery quiescence for %s ordinal %s: %w", authority.ErrRecoveryNeeded, item.JobID, recoveryLaunch.Ordinal, err)
	}
	if cleanup.Err != nil {
		log.Printf("agentbus daemon: admission cleanup warning: job=%s ordinal=%s phase=recovery: %v", item.JobID, recoveryLaunch.Ordinal, cleanup.Err)
	}
	next, err := e.session.AdvanceRecovery(ctx, item.Token)
	if err != nil {
		if recoveryAborted(err) {
			return model.RecoveryWorkItem{}, false, cleanup.Err, err
		}
		return model.RecoveryWorkItem{}, false, cleanup.Err, fmt.Errorf("%w: advance recovery for %s: %w", authority.ErrRecoveryNeeded, item.JobID, err)
	}
	return next, false, cleanup.Err, nil
}

func recoveryAborted(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (e *admissionRecoveryExecutor) failClosed(reason error) error {
	if reason == nil {
		reason = authority.ErrRecoveryNeeded
	}
	e.latch.Trip(reason)
	return errors.Join(SafetyFailStopError{Reason: reason}, reason)
}
