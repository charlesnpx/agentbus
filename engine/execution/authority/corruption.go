package authority

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

const authorityFailStopTimeout = 30 * time.Second

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
	if err := r.core.view(ctx, "ready recovery plans", func(tx repository.ReadTx) error {
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
	if err := r.core.view(ctx, "corruption plans", func(tx repository.ReadTx) error {
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
	commit, err := r.core.update(ctx, "quarantine projection", func(tx repository.WriteTx) error {
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
	failStopCtx, cancel := detachedAuthorityFailStopContext(ctx)
	defer cancel()
	r.core.mu.Lock()
	defer r.core.mu.Unlock()
	if err := r.core.view(failStopCtx, "ready fail-stop preflight", func(tx repository.ReadTx) error {
		_, err := r.core.requireReadyTx(tx, r.token)
		return err
	}); err != nil {
		return err
	}
	return r.core.failStopLockedWithContext(failStopCtx, reason)
}

func (s *RecoverySession) FailStop(ctx context.Context, reason string) error {
	if s == nil || s.core == nil {
		return ErrNotReady
	}
	failStopCtx, cancel := detachedAuthorityFailStopContext(ctx)
	defer cancel()
	s.core.mu.Lock()
	defer s.core.mu.Unlock()
	if err := s.core.view(failStopCtx, "recovery fail-stop preflight", func(tx repository.ReadTx) error {
		_, err := s.core.requireRecoveryTx(tx, s.token)
		return err
	}); err != nil {
		return err
	}
	return s.core.failStopLockedWithContext(failStopCtx, reason)
}

func (core *authorityCore) failStopLocked(ctx context.Context, reason string) error {
	failStopCtx, cancel := detachedAuthorityFailStopContext(ctx)
	defer cancel()
	return core.failStopLockedWithContext(failStopCtx, reason)
}

func detachedAuthorityFailStopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), authorityFailStopTimeout)
}

func (core *authorityCore) failStopLockedWithContext(ctx context.Context, reason string) error {
	err := core.anchor.FailStop(ctx, core.boot.ref, reason)
	core.boot.phase = bootFailStopped
	core.boot.reason = reason
	return err
}

func postDurableFailStopError(operation string, err, stopErr error) error {
	if stopErr == nil {
		return errors.Join(FailStoppedError{Reason: fmt.Sprintf("%s: %v", operation, err)}, err)
	}
	return fmt.Errorf("%w: %s after durable commit: %w; durable fail-stop: %w", ErrFailStopRecord, operation, err, stopErr)
}

func (core *authorityCore) tripSafetyLatchOnRepositoryCorruption(err error) {
	if core == nil {
		return
	}
	tripSafetyLatchOnRepositoryCorruption(core.latch, err)
}

func tripSafetyLatchOnRepositoryCorruption(latch safetyLatch, err error) {
	if latch == nil || err == nil {
		return
	}
	if safetySignificantRepositoryCorruption(err) {
		latch.Trip(err)
	}
}

func (core *authorityCore) failStopOnRepositoryCorruptionLocked(ctx context.Context, operation string, err error) error {
	if core == nil || err == nil || !safetySignificantRepositoryCorruption(err) {
		return err
	}
	if core.boot.phase != bootReady && core.boot.phase != bootReconciling {
		core.tripSafetyLatchOnRepositoryCorruption(err)
		return err
	}
	failStopCtx, cancel := detachedAuthorityFailStopContext(ctx)
	defer cancel()
	// Trip with the typed corruption error before persistence: the file-anchor
	// fail-stop hook fires mid-persistence with a stringified reason, and the
	// latch keeps only the first trip.
	core.tripSafetyLatchOnRepositoryCorruption(err)
	failStopErr := postDurableFailStopError(operation, err, core.failStopLockedWithContext(failStopCtx, fmt.Sprintf("%s: %v", operation, err)))
	core.tripSafetyLatchOnRepositoryCorruption(failStopErr)
	return failStopErr
}

func (core *authorityCore) view(ctx context.Context, operation string, fn func(repository.ReadTx) error) error {
	err := core.repo.View(ctx, fn)
	if err != nil {
		return core.failStopOnRepositoryCorruptionLocked(ctx, operation, err)
	}
	return nil
}

func (core *authorityCore) update(ctx context.Context, operation string, fn func(repository.WriteTx) error) (repository.Commit, error) {
	commit, err := core.repo.Update(ctx, fn)
	if err != nil {
		return commit, core.failStopOnRepositoryCorruptionLocked(ctx, operation, err)
	}
	return commit, nil
}

func safetySignificantRepositoryCorruption(err error) bool {
	if err == nil || !errors.Is(err, repository.ErrCorruptRecord) {
		return false
	}
	var corrupt repository.CorruptRecordKindError
	if errors.As(err, &corrupt) {
		return safetySignificantCorruptionKind(corrupt.Kind)
	}
	message := strings.ToLower(err.Error())
	safetyKinds := []string{"db_uuid", "meta", "safety", "binding_index", "binding", "tombstone"}
	for _, kind := range safetyKinds {
		if strings.Contains(message, kind) {
			return true
		}
	}
	if strings.Contains(message, "projection") || strings.Contains(message, "quarantine") {
		return false
	}
	return true
}

func safetySignificantCorruptionKind(kind string) bool {
	switch kind {
	case "db_uuid", "meta", "safety", "binding_index", "binding", "tombstone":
		return true
	case "projection", "quarantine":
		return false
	default:
		return true
	}
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
