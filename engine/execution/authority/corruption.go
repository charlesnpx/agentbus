package authority

import (
	"context"
	"fmt"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type JobRecoveryPlan struct {
	JobID      model.JobID
	Plan       model.RecoveryPlan
	Diagnostic string
}

func (r *Ready) RecoveryPlans(ctx context.Context, trigger model.RecoveryTrigger) ([]JobRecoveryPlan, error) {
	if r == nil || r.core == nil {
		return nil, ErrNotReady
	}
	if err := trigger.Validate(); err != nil {
		return nil, fmt.Errorf("%w: recovery_trigger: %v", ErrInvalidRequest, err)
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var plans []JobRecoveryPlan
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		jobPlans, err := recoveryPlansTx(tx, r.token.boot, trigger, false)
		if err != nil {
			return err
		}
		plans = jobPlans
		return nil
	}); err != nil {
		return nil, err
	}
	return plans, nil
}

func (r *Ready) CorruptionPlans(ctx context.Context) ([]JobRecoveryPlan, error) {
	if r == nil || r.core == nil {
		return nil, ErrNotReady
	}
	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var plans []JobRecoveryPlan
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			switch image.Safety.State {
			case repository.RecordCorrupt:
				plans = append(plans, JobRecoveryPlan{
					Plan:       model.RecoveryPlan{Next: model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}},
					Diagnostic: image.Safety.Diagnostic,
				})
			case repository.RecordValid:
				if image.Safety.Value.Terminal != nil || image.Projection.State != repository.RecordCorrupt {
					continue
				}
				plan, err := model.PlanRecovery(image.Safety.Value, model.RecoveryCorruption)
				if err != nil {
					return err
				}
				plans = append(plans, JobRecoveryPlan{
					JobID:      image.Safety.Value.JobID,
					Plan:       plan,
					Diagnostic: image.Projection.Diagnostic,
				})
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return plans, nil
}

func (r *Ready) QuarantineProjection(ctx context.Context, jobID model.JobID, diagnostic string) (repository.QuarantineRecord, error) {
	if r == nil || r.core == nil {
		return repository.QuarantineRecord{}, ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return repository.QuarantineRecord{}, fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
	}
	if strings.TrimSpace(diagnostic) == "" {
		return repository.QuarantineRecord{}, fmt.Errorf("%w: diagnostic is required", ErrInvalidRequest)
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var record repository.QuarantineRecord
	commit, err := r.core.repo.Update(ctx, func(tx repository.WriteTx) error {
		meta, err := r.core.requireReadyTx(tx, r.token)
		if err != nil {
			return err
		}
		image := tx.LoadJob(jobID)
		if image.Safety.State != repository.RecordValid {
			return requireRecord("safety", image.Safety.State, image.Safety.Diagnostic)
		}
		record = repository.QuarantineRecord{
			JobID:      jobID,
			Diagnostic: diagnostic,
			Generation: meta.Generation + 1,
		}
		return tx.PutQuarantine(record)
	})
	if err != nil {
		return repository.QuarantineRecord{}, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		return repository.QuarantineRecord{}, err
	}
	return record, nil
}

func (r *Ready) FailStop(ctx context.Context, reason string) error {
	if r == nil || r.core == nil {
		return ErrNotReady
	}
	r.core.mu.Lock()
	defer r.core.mu.Unlock()
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
		_, err := r.core.requireReadyTx(tx, r.token)
		return err
	}); err != nil {
		return err
	}
	r.core.failStopLocked(ctx, reason)
	return nil
}

func (core *authorityCore) failStopLocked(ctx context.Context, reason string) {
	_ = core.anchor.FailStop(ctx, core.boot.ref, reason)
	core.boot.phase = bootFailStopped
	core.boot.reason = reason
}

func recoveryPlansTx(tx repository.ReadTx, boot model.BootRef, trigger model.RecoveryTrigger, priorBootOnly bool) ([]JobRecoveryPlan, error) {
	images, err := tx.ListJobs(repository.JobFilter{})
	if err != nil {
		return nil, err
	}
	plans := make([]JobRecoveryPlan, 0)
	for _, image := range images {
		switch image.Safety.State {
		case repository.RecordCorrupt:
			plans = append(plans, JobRecoveryPlan{
				Plan:       model.RecoveryPlan{Next: model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}},
				Diagnostic: image.Safety.Diagnostic,
			})
		case repository.RecordValid:
			record := image.Safety.Value
			if record.Terminal != nil {
				continue
			}
			if priorBootOnly && record.AdmittedBy.BootID == boot.BootID {
				continue
			}
			plan, err := model.PlanRecovery(record, trigger)
			if err != nil {
				return nil, err
			}
			plans = append(plans, JobRecoveryPlan{JobID: record.JobID, Plan: plan})
		}
	}
	return plans, nil
}
