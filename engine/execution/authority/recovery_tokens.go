package authority

import (
	"context"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type RecoveryLaunch = model.RecoveryLaunch
type RecoveryWorkItem = model.RecoveryWorkItem

type issuedRecoveryToken struct {
	jobID        model.JobID
	revision     uint64
	boot         model.BootRef
	factRevision uint64
	consumed     bool
}

func (s *RecoverySession) WorkItems(ctx context.Context) ([]RecoveryWorkItem, error) {
	if s == nil || s.core == nil {
		return nil, ErrNotReady
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	items := []RecoveryWorkItem{}
	if err := s.core.view(ctx, "recovery work items", func(tx repository.ReadTx) error {
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
				return fmt.Errorf("%w: startup recovery plan has no job identity", ErrRecoveryNeeded)
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
	if err := s.core.view(ctx, "advance recovery", func(tx repository.ReadTx) error {
		if _, err := s.core.requireRecoveryTx(tx, s.token); err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		record, err := s.recoveryRecordAfterTokenTx(tx, token)
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
	commit, err := s.core.update(ctx, "finalize planned recovery", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		issued, record, err := s.validateRecoveryTokenTx(tx, token)
		if err != nil {
			return err
		}
		if issued.factRevision != 0 {
			return ErrReplayConflict
		}
		if record.Revision != issued.revision {
			return ErrStaleCapability
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

func (s *RecoverySession) FinalizeUnresolved(ctx context.Context, token model.RecoveryToken) (model.SafetyRecord, error) {
	if s == nil || s.core == nil {
		return model.SafetyRecord{}, ErrNotReady
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var finalized model.SafetyRecord
	terminalCommitted := false
	commit, err := s.core.update(ctx, "finalize unresolved recovery", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		record, err := s.recoveryRecordAfterTokenTx(tx, token)
		if err != nil {
			return err
		}
		intent, err := model.RecoveryTerminalIntent(record, model.RecoveryStartupLoss, false)
		if err != nil {
			return err
		}
		intent.DerivedBy = s.token.boot
		applied, err := applyRecoveryCommandTx(tx, token.JobID, model.Finalize{Ref: record.Attempt.Ref, Intent: intent}, meta.Generation+1)
		if err != nil {
			return err
		}
		finalized = applied.Record
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return model.SafetyRecord{}, err
	}
	s.consumeRecoveryTokenLocked(token)
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return model.SafetyRecord{}, err
	}
	if terminalCommitted {
		s.core.runtime.releaseTerminal(token.JobID)
	}
	return finalized, nil
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
	recordedRevision := uint64(0)
	commit, err := s.core.update(ctx, "recovery token quiescence", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		issued, record, err := s.validateRecoveryTokenTx(tx, token)
		if err != nil {
			return err
		}
		if issued.factRevision != 0 {
			return ErrReplayConflict
		}
		if record.Revision != issued.revision {
			return ErrStaleCapability
		}
		applied, err := applyRecoveryQuiescenceTx(tx, token.JobID, ordinal, s.core.verifier, verified, s.token.boot, meta.Generation+1)
		if err != nil {
			return err
		}
		if _, err := model.PlanRecovery(applied.Record, model.RecoveryStartupLoss); err != nil {
			return err
		}
		recordedRevision = applied.Record.Revision
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return err
	}
	s.markRecoveryTokenFactLocked(token, recordedRevision)
	if terminalCommitted {
		s.core.runtime.releaseTerminal(token.JobID)
	}
	return nil
}

func (s *RecoverySession) recoveryRecordAfterTokenTx(tx repository.ReadTx, token model.RecoveryToken) (model.SafetyRecord, error) {
	issued, record, err := s.validateRecoveryTokenTx(tx, token)
	if err != nil {
		return model.SafetyRecord{}, err
	}
	expectedRevision := issued.revision
	if issued.factRevision != 0 {
		expectedRevision = issued.factRevision
	}
	if record.Revision != expectedRevision {
		return model.SafetyRecord{}, ErrStaleCapability
	}
	return record, nil
}

func (s *RecoverySession) validateRecoveryTokenForCurrentRevisionTx(tx repository.ReadTx, token model.RecoveryToken) (issuedRecoveryToken, model.SafetyRecord, error) {
	issued, record, err := s.validateRecoveryTokenTx(tx, token)
	if err != nil {
		return issuedRecoveryToken{}, model.SafetyRecord{}, err
	}
	if record.Revision != issued.revision {
		return issuedRecoveryToken{}, model.SafetyRecord{}, ErrStaleCapability
	}
	return issued, record, nil
}

func (s *RecoverySession) validateRecoveryTokenTx(tx repository.ReadTx, token model.RecoveryToken) (issuedRecoveryToken, model.SafetyRecord, error) {
	if err := token.Validate(); err != nil {
		return issuedRecoveryToken{}, model.SafetyRecord{}, fmt.Errorf("%w: recovery_token: %v", ErrInvalidRequest, err)
	}
	issued, ok := s.tokens[token.Opaque]
	if !ok {
		return issuedRecoveryToken{}, model.SafetyRecord{}, ErrStaleCapability
	}
	if issued.consumed {
		return issuedRecoveryToken{}, model.SafetyRecord{}, ErrReplayConflict
	}
	if issued.jobID != token.JobID || issued.revision != token.BasedOnRevision || issued.boot != token.RecoveryBoot || token.RecoveryBoot != s.token.boot {
		return issuedRecoveryToken{}, model.SafetyRecord{}, ErrStaleCapability
	}
	image := tx.LoadJob(token.JobID)
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return issuedRecoveryToken{}, model.SafetyRecord{}, err
	}
	record := image.Safety.Value
	if err := model.ValidateSafetyRecord(record); err != nil {
		return issuedRecoveryToken{}, model.SafetyRecord{}, fatalStartup("safety %s is unsupported: %v", record.JobID, err)
	}
	if record.JobID != token.JobID {
		return issuedRecoveryToken{}, model.SafetyRecord{}, fmt.Errorf("%w: recovery token job mismatch", repository.ErrInvalidRecord)
	}
	return issued, record, nil
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
		Token:              token,
		JobID:              record.JobID,
		BasedOnRevision:    plan.BasedOnRevision,
		Trigger:            model.RecoveryStartupLoss,
		WorkspaceLayoutKey: record.WorkspaceLayoutKey,
	}
	switch plan.Next.Kind {
	case model.RecoveryRetireThenFinalize, model.RecoveryContainThenFinalize:
		item.Launches = recoveryLaunches(record.Attempt.Launches)
	}
	if err := item.Validate(); err != nil {
		return RecoveryWorkItem{}, err
	}
	return item, nil
}

func recoveryLaunches(slots model.LaunchSlots[model.LaunchProof]) []model.RecoveryLaunch {
	launches := make([]model.RecoveryLaunch, 0, slots.Count())
	if slots.First != nil && slots.First.Group != nil && slots.First.Quiescence == nil {
		launches = append(launches, model.RecoveryLaunch{Ordinal: model.LaunchOrdinalOne, Group: *slots.First.Group})
	}
	if slots.Second != nil && slots.Second.Group != nil && slots.Second.Quiescence == nil {
		launches = append(launches, model.RecoveryLaunch{Ordinal: model.LaunchOrdinalTwo, Group: *slots.Second.Group})
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

func (s *RecoverySession) markRecoveryTokenFactLocked(token model.RecoveryToken, revision uint64) {
	issued := s.tokens[token.Opaque]
	issued.factRevision = revision
	s.tokens[token.Opaque] = issued
}
