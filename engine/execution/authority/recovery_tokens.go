package authority

import (
	"context"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type RecoveryLaunch struct {
	Ordinal model.LaunchOrdinal
	Group   model.GroupRef
}

type RecoveryWorkItem struct {
	Token    model.RecoveryToken
	JobID    model.JobID
	Launches []RecoveryLaunch
}

type issuedRecoveryToken struct {
	jobID    model.JobID
	revision uint64
	boot     model.BootRef
	consumed bool
}

func (s *RecoverySession) WorkItems(ctx context.Context) ([]RecoveryWorkItem, error) {
	if s == nil || s.core == nil {
		return nil, ErrNotReady
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	items := []RecoveryWorkItem{}
	if err := s.core.repo.View(ctx, func(tx repository.ReadTx) error {
		if _, err := s.core.requireRecoveryTx(tx, s.token); err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		plans, err := recoveryPlansTx(tx, s.token.boot, model.RecoveryStartupLoss, true)
		if err != nil {
			return err
		}
		for _, plan := range plans {
			if plan.JobID == "" {
				continue
			}
			item, err := s.recoveryWorkItemForPlanLocked(tx, plan)
			if err != nil {
				return err
			}
			items = append(items, item)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *RecoverySession) AdvanceRecovery(ctx context.Context, token model.RecoveryToken) (RecoveryWorkItem, error) {
	if s == nil || s.core == nil {
		return RecoveryWorkItem{}, ErrNotReady
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var item RecoveryWorkItem
	if err := s.core.repo.View(ctx, func(tx repository.ReadTx) error {
		if _, err := s.core.requireRecoveryTx(tx, s.token); err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		record, err := s.validateRecoveryTokenTx(tx, token)
		if err != nil {
			return err
		}
		plan, err := model.PlanRecovery(record, model.RecoveryStartupLoss)
		if err != nil {
			return err
		}
		item, err = s.recoveryWorkItemForRecordLocked(record, plan)
		return err
	}); err != nil {
		return RecoveryWorkItem{}, err
	}
	s.consumeRecoveryTokenLocked(token)
	return item, nil
}

func (s *RecoverySession) FinalizePlanned(ctx context.Context, token model.RecoveryToken) error {
	if s == nil || s.core == nil {
		return ErrNotReady
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	terminalCommitted := false
	commit, err := s.core.repo.Update(ctx, func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		record, err := s.validateRecoveryTokenTx(tx, token)
		if err != nil {
			return err
		}
		plan, err := model.PlanRecovery(record, model.RecoveryStartupLoss)
		if err != nil {
			return err
		}
		if plan.Next.Kind != model.RecoveryFinalizeCertified || plan.Next.Finalize == nil {
			return fmt.Errorf("%w: recovery token does not authorize planned finalization", ErrInvalidRequest)
		}
		finalize := *plan.Next.Finalize
		finalize.Intent.DerivedBy = s.token.boot
		applied, err := applyRecoveryCommandTx(tx, token.JobID, finalize, meta.Generation+1)
		if err != nil {
			return err
		}
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return err
	}
	s.consumeRecoveryTokenLocked(token)
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return err
	}
	if terminalCommitted {
		s.core.runtime.releaseTerminal(token.JobID)
	}
	return nil
}

func (s *RecoverySession) recordQuiescenceByToken(ctx context.Context, token model.RecoveryToken, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) error {
	if s == nil || s.core == nil {
		return ErrNotReady
	}
	if err := ordinal.Validate(); err != nil {
		return fmt.Errorf("%w: launch_ordinal: %v", ErrInvalidRequest, err)
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	terminalCommitted := false
	commit, err := s.core.repo.Update(ctx, func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		if _, err := s.validateRecoveryTokenTx(tx, token); err != nil {
			return err
		}
		applied, err := applyRecoveryQuiescenceTx(tx, token.JobID, ordinal, s.core.verifier, verified, s.token.boot, meta.Generation+1)
		if err != nil {
			return err
		}
		if _, err := model.PlanRecovery(applied.Record, model.RecoveryStartupLoss); err != nil {
			return err
		}
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return err
	}
	s.consumeRecoveryTokenLocked(token)
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return err
	}
	if terminalCommitted {
		s.core.runtime.releaseTerminal(token.JobID)
	}
	return nil
}

func (s *RecoverySession) validateRecoveryTokenTx(tx repository.ReadTx, token model.RecoveryToken) (model.SafetyRecord, error) {
	if err := token.Validate(); err != nil {
		return model.SafetyRecord{}, fmt.Errorf("%w: recovery_token: %v", ErrInvalidRequest, err)
	}
	issued, ok := s.tokens[token.Opaque]
	if !ok {
		return model.SafetyRecord{}, ErrStaleCapability
	}
	if issued.consumed {
		return model.SafetyRecord{}, ErrReplayConflict
	}
	if issued.jobID != token.JobID || issued.revision != token.BasedOnRevision || issued.boot != token.RecoveryBoot || token.RecoveryBoot != s.token.boot {
		return model.SafetyRecord{}, ErrStaleCapability
	}
	image := tx.LoadJob(token.JobID)
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return model.SafetyRecord{}, err
	}
	record := image.Safety.Value
	if err := model.ValidateSafetyRecord(record); err != nil {
		return model.SafetyRecord{}, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
	}
	if record.JobID != token.JobID {
		return model.SafetyRecord{}, fmt.Errorf("%w: recovery token job mismatch", repository.ErrInvalidRecord)
	}
	if record.Revision != token.BasedOnRevision {
		return model.SafetyRecord{}, ErrStaleCapability
	}
	return record, nil
}

func (s *RecoverySession) recoveryWorkItemForPlanLocked(tx repository.ReadTx, plan JobRecoveryPlan) (RecoveryWorkItem, error) {
	image := tx.LoadJob(plan.JobID)
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return RecoveryWorkItem{}, err
	}
	record := image.Safety.Value
	if err := model.ValidateSafetyRecord(record); err != nil {
		return RecoveryWorkItem{}, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
	}
	return s.recoveryWorkItemForRecordLocked(record, plan.Plan)
}

func (s *RecoverySession) recoveryWorkItemForRecordLocked(record model.SafetyRecord, plan model.RecoveryPlan) (RecoveryWorkItem, error) {
	token, err := s.issueRecoveryTokenLocked(record.JobID, record.Revision)
	if err != nil {
		return RecoveryWorkItem{}, err
	}
	item := RecoveryWorkItem{
		Token: token,
		JobID: record.JobID,
	}
	switch plan.Next.Kind {
	case model.RecoveryRetireThenFinalize, model.RecoveryContainThenFinalize:
		item.Launches = recoveryLaunches(record.Attempt.Launches)
	}
	return item, nil
}

func recoveryLaunches(slots model.LaunchSlots[model.LaunchProof]) []RecoveryLaunch {
	launches := make([]RecoveryLaunch, 0, slots.Count())
	if slots.First != nil && slots.First.Group != nil && slots.First.Quiescence == nil {
		launches = append(launches, RecoveryLaunch{Ordinal: model.LaunchOrdinalOne, Group: *slots.First.Group})
	}
	if slots.Second != nil && slots.Second.Group != nil && slots.Second.Quiescence == nil {
		launches = append(launches, RecoveryLaunch{Ordinal: model.LaunchOrdinalTwo, Group: *slots.Second.Group})
	}
	return launches
}

func (s *RecoverySession) issueRecoveryTokenLocked(jobID model.JobID, revision uint64) (model.RecoveryToken, error) {
	if s.tokens == nil {
		s.tokens = make(map[string]issuedRecoveryToken)
	}
	opaque, err := randomOpaqueToken("recovery")
	if err != nil {
		return model.RecoveryToken{}, err
	}
	token := model.RecoveryToken{
		JobID:           jobID,
		BasedOnRevision: revision,
		RecoveryBoot:    s.token.boot,
		Opaque:          opaque,
	}
	if err := token.Validate(); err != nil {
		return model.RecoveryToken{}, err
	}
	s.tokens[token.Opaque] = issuedRecoveryToken{
		jobID:    token.JobID,
		revision: token.BasedOnRevision,
		boot:     token.RecoveryBoot,
	}
	return token, nil
}

func (s *RecoverySession) consumeRecoveryTokenLocked(token model.RecoveryToken) {
	issued := s.tokens[token.Opaque]
	issued.consumed = true
	s.tokens[token.Opaque] = issued
}
