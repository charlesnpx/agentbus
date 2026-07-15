package model

type RecoveryTrigger uint8

const (
	RecoveryStartupLoss RecoveryTrigger = iota + 1
	RecoveryLiveLoss
	RecoveryCancelAfterGrant
	RecoveryCorruption
	RecoveryPostGrantFailure
	RecoveryShutdown
)

func (trigger RecoveryTrigger) Valid() bool {
	switch trigger {
	case RecoveryStartupLoss, RecoveryLiveLoss, RecoveryCancelAfterGrant, RecoveryCorruption, RecoveryPostGrantFailure, RecoveryShutdown:
		return true
	default:
		return false
	}
}

func (trigger RecoveryTrigger) Validate() error {
	if !trigger.Valid() {
		return invalid("recovery_trigger", "is unknown")
	}
	return nil
}

type RecoveryActionKind uint8

const (
	RecoveryRetireThenFinalize RecoveryActionKind = iota + 1
	RecoveryContainThenFinalize
	RecoveryFinalizeCertified
	RecoveryFatalUnprovable
)

type RecoveryAction struct {
	Kind     RecoveryActionKind
	Finalize *Finalize
}

type RecoveryPlan struct {
	BasedOnRevision uint64
	Next            RecoveryAction
}

func PlanRecovery(record SafetyRecord, trigger RecoveryTrigger) (RecoveryPlan, error) {
	if err := trigger.Validate(); err != nil {
		return RecoveryPlan{}, err
	}
	plan := RecoveryPlan{BasedOnRevision: record.Revision}
	if err := ValidateSafetyRecord(record); err != nil {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}
	if record.Terminal != nil {
		plan.Next = RecoveryAction{Kind: RecoveryFinalizeCertified}
		return plan, nil
	}
	if record.Mode == ModeLegacyUnfenced {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}

	intent := recoveryTerminalIntent(record, trigger)
	if finalizable(record, intent) {
		finalize := Finalize{Ref: record.Attempt.Ref, Intent: intent}
		plan.Next = RecoveryAction{Kind: RecoveryFinalizeCertified, Finalize: &finalize}
		return plan, nil
	}
	if needsContainment(record, trigger) {
		if !hasPreparedLaunch(record.Attempt) {
			plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
			return plan, nil
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			plan.Next = RecoveryAction{Kind: RecoveryContainThenFinalize}
			return plan, nil
		}
	}
	if !hasPreparedLaunch(record.Attempt) {
		plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
		return plan, nil
	}
	if !allLaunchGroupsQuiescent(record.Attempt) {
		plan.Next = RecoveryAction{Kind: RecoveryRetireThenFinalize}
		return plan, nil
	}
	plan.Next = RecoveryAction{Kind: RecoveryFatalUnprovable}
	return plan, nil
}

func finalizable(record SafetyRecord, intent TerminalIntent) bool {
	_, err := DeriveTerminalCertificate(record, intent)
	return err == nil
}

func needsContainment(record SafetyRecord, trigger RecoveryTrigger) bool {
	if hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt) {
		return true
	}
	switch trigger {
	case RecoveryCancelAfterGrant, RecoveryPostGrantFailure:
		return true
	default:
		return false
	}
}

func recoveryTerminalIntent(record SafetyRecord, trigger RecoveryTrigger) TerminalIntent {
	afterAuthorization := hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt)
	intent := TerminalIntent{DerivedBy: record.AdmittedBy}
	switch trigger {
	case RecoveryCancelAfterGrant:
		intent.Outcome = OutcomeCanceled
		if afterAuthorization {
			intent.Cause = CauseCanceledAfterAuthorization
		} else {
			intent.Cause = CauseCanceledBeforeAuthorization
		}
	case RecoveryCorruption:
		intent.Outcome = OutcomeQuarantined
		intent.Cause = CauseCorruptProjection
	case RecoveryLiveLoss:
		if afterAuthorization {
			intent.Outcome = OutcomeReaped
			intent.Cause = CauseSupervisorLostAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseSupervisorLostBeforeAuthorization
		}
	default:
		if afterAuthorization {
			intent.Outcome = OutcomeReaped
			intent.Cause = CauseDaemonRestartedAfterAuthorization
		} else {
			intent.Outcome = OutcomeFailed
			intent.Cause = CauseDaemonRestartedBeforeAuthorization
		}
	}
	return intent
}
