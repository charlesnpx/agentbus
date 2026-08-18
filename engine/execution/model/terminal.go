package model

import "github.com/charlesnpx/agentbus/engine"

func DeriveTerminalCertificate(record SafetyRecord, intent TerminalIntent) (TerminalCertificate, error) {
	if err := ValidateSafetyRecord(record); err != nil {
		return TerminalCertificate{}, invalidCommand("current safety record is invalid: %v", err)
	}
	if record.Terminal != nil {
		return TerminalCertificate{}, precondition("terminal certificate is already recorded")
	}
	intent, err := resolveTerminalIntent(record, intent)
	if err != nil {
		return TerminalCertificate{}, err
	}
	if record.Outcome != nil && record.Outcome.Outcome != intent.Outcome {
		return TerminalCertificate{}, precondition("terminal outcome does not match observed outcome")
	}
	contract := terminalContract(record, intent)
	result, err := terminalResult(record, intent.Outcome)
	if err != nil {
		return TerminalCertificate{}, err
	}
	proof, err := deriveTerminalProof(record, intent)
	if err != nil {
		return TerminalCertificate{}, err
	}
	if !validTerminalCompatibility(record, intent, proof) {
		return TerminalCertificate{}, precondition("terminal intent is incompatible with cause/outcome/proof table")
	}
	return TerminalCertificate{
		JobID:               record.JobID,
		Attempt:             record.Attempt.Ref,
		Outcome:             intent.Outcome,
		Proof:               proof,
		Cause:               intent.Cause,
		DerivedFromRevision: record.Revision,
		DerivedBy:           intent.DerivedBy,
		Result:              result,
		Contract:            contract,
	}, nil
}

func terminalContract(record SafetyRecord, intent TerminalIntent) *engine.ContractStamp {
	if record.Outcome != nil {
		return cloneContractStamp(record.Outcome.Contract)
	}
	return cloneContractStamp(intent.Contract)
}

func resolveTerminalIntent(record SafetyRecord, intent TerminalIntent) (TerminalIntent, error) {
	if err := intent.Outcome.ValidateTerminal(); err != nil {
		return TerminalIntent{}, invalidCommand("terminal intent outcome: %v", err)
	}
	if err := intent.Cause.Validate(); err != nil {
		return TerminalIntent{}, invalidCommand("terminal intent cause: %v", err)
	}
	derivedBy, err := resolveBootRef(intent.DerivedBy, record.AdmittedBy, "terminal.derived_by")
	if err != nil {
		return TerminalIntent{}, err
	}
	intent.DerivedBy = derivedBy
	if intent.PartialResult != nil {
		receipt := *intent.PartialResult
		if err := receipt.Validate(); err != nil {
			return TerminalIntent{}, invalidCommand("partial result receipt: %v", err)
		}
		if err := validateJobField("partial_result.job_id", receipt.JobID, record.JobID); err != nil {
			return TerminalIntent{}, invalidCommand("%v", err)
		}
		if !partialResultOutcome(intent.Outcome) {
			return TerminalIntent{}, invalidCommand("partial result requires timed_out or interrupted terminal outcome")
		}
		if !receipt.Result.Partial {
			return TerminalIntent{}, invalidCommand("partial result receipt must be marked partial")
		}
		if receipt.Result.PartialReason != partialResultReasonForOutcome(intent.Outcome) {
			return TerminalIntent{}, invalidCommand("partial result reason does not match terminal outcome")
		}
		intent.PartialResult = &receipt
	}
	return intent, nil
}

func terminalResult(record SafetyRecord, outcome Outcome) (*ResultRef, error) {
	if completionOutcome(outcome) {
		if record.Result == nil {
			return nil, precondition("completed terminal outcome requires result certificate")
		}
		if record.Result.Result.Partial {
			return nil, precondition("completed terminal outcome cannot use partial result certificate")
		}
		result := record.Result.Result
		return &result, nil
	}
	if !partialResultOutcome(outcome) || record.Result == nil {
		return nil, nil
	}
	if !record.Result.Result.Partial {
		return nil, precondition("non-completed terminal outcome cannot use final result certificate")
	}
	result := record.Result.Result
	return &result, nil
}

func partialResultOutcome(outcome Outcome) bool {
	return outcome == OutcomeTimedOut || outcome == OutcomeInterrupted
}

func partialResultReasonForOutcome(outcome Outcome) string {
	switch outcome {
	case OutcomeTimedOut:
		return PartialResultReasonTimeout
	case OutcomeInterrupted:
		return PartialResultReasonInterrupted
	default:
		return ""
	}
}

func deriveTerminalProof(record SafetyRecord, intent TerminalIntent) (TerminalProof, error) {
	if record.Mode == ModeLegacyUnfenced {
		if hasAnyLaunchEvidence(record.Attempt) {
			return 0, precondition("legacy unfenced terminal proof requires no launch evidence")
		}
		if !validLegacyUnfencedIntent(record, intent) {
			return 0, precondition("terminal intent is incompatible with legacy-unfenced proof")
		}
		return ProofLegacyUnfencedOutcome, nil
	}
	if !hasAnyGrant(record.Attempt) && !hasAnyRelease(record.Attempt) && validNeverPermittedIntent(intent) {
		return ProofNeverPermittedAndRetired, nil
	}
	if hasAnyLaunchEvidence(record.Attempt) && allLaunchGroupsQuiescent(record.Attempt) && validContainedIntent(record, intent) {
		return ProofContained, nil
	}
	if cleanQuiescentOutcomeAndRetired(record, intent) {
		return ProofCleanQuiescentOutcomeAndRetired, nil
	}
	if validExecutionImpossibleIntent(record, intent) {
		return ProofNeverPermittedAndRetired, nil
	}
	if validUnresolvedAbsenceIntent(record, intent) {
		return ProofUnresolvedAbsence, nil
	}
	return 0, precondition("terminal proof predicates are not satisfied")
}

func cleanQuiescentOutcomeAndRetired(record SafetyRecord, intent TerminalIntent) bool {
	if intent.Cause != CauseCompletedNormally || !cleanTerminalOutcome(intent.Outcome) {
		return false
	}
	if record.Outcome == nil || record.Outcome.Outcome != intent.Outcome {
		return false
	}
	if !hasAnyGrant(record.Attempt) {
		return false
	}
	if !allBoundLaunchesReleasedWhenGrantedAndQuiescent(record.Attempt) {
		return false
	}
	return !hasUnconsumedGrant(record.Attempt) && !hasActiveLaunch(record.Attempt)
}

func cleanTerminalOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeInterrupted:
		return true
	default:
		return false
	}
}

func validNeverPermittedIntent(intent TerminalIntent) bool {
	switch intent.Cause {
	case CauseCanceledBeforeAuthorization:
		return intent.Outcome == OutcomeCanceled
	case CauseDaemonRestartedBeforeAuthorization, CauseSupervisorLostBeforeAuthorization:
		return intent.Outcome == OutcomeFailed
	default:
		return false
	}
}

func validExecutionImpossibleIntent(record SafetyRecord, intent TerminalIntent) bool {
	if !terminalCauseBackedByDurableFact(record, intent.Cause) {
		return false
	}
	switch intent.Cause {
	case CauseCanceledBeforeAuthorization,
		CauseDaemonRestartedBeforeAuthorization,
		CauseSupervisorLostBeforeAuthorization:
		return !hasAnyGrant(record.Attempt) && !hasAnyRelease(record.Attempt) && validNeverPermittedIntent(intent)
	case CauseReleaseDefinitelyNotSent:
		return intent.Outcome == OutcomeFailed
	default:
		return false
	}
}

func validContainedIntent(record SafetyRecord, intent TerminalIntent) bool {
	if !terminalCauseBackedByDurableFact(record, intent.Cause) {
		return false
	}
	if record.Outcome != nil &&
		record.Outcome.Outcome == intent.Outcome &&
		recordedOutcomeCausePermitsIntent(intent.Cause, intent.Outcome) {
		switch intent.Cause {
		case CauseResponseUndeliverable,
			CauseCanceledAfterAuthorization,
			CauseDaemonRestartedAfterAuthorization,
			CauseSupervisorLostAfterAuthorization,
			CauseReleaseOutcomeUnknown,
			CauseCorruptProjection:
			return true
		}
	}
	switch intent.Cause {
	case CauseCanceledAfterAuthorization:
		return intent.Outcome == OutcomeCanceled
	case CauseDaemonRestartedAfterAuthorization, CauseSupervisorLostAfterAuthorization:
		return intent.Outcome == OutcomeReaped
	case CauseCorruptProjection:
		return intent.Outcome == OutcomeQuarantined
	case CauseReleaseOutcomeUnknown:
		return intent.Outcome == OutcomeReaped
	case CauseReleaseDefinitelyNotSent:
		return intent.Outcome == OutcomeFailed
	default:
		return false
	}
}

func validUnresolvedAbsenceIntent(record SafetyRecord, intent TerminalIntent) bool {
	if !terminalCauseBackedByDurableFact(record, intent.Cause) {
		return false
	}
	if !hasAnyGrant(record.Attempt) && !hasAnyRelease(record.Attempt) {
		return false
	}
	if allLaunchGroupsQuiescent(record.Attempt) {
		return false
	}
	if intent.Outcome == OutcomeOrphaned {
		return record.Outcome == nil && orphanedOutcomeCause(intent.Cause)
	}
	if intent.Outcome == OutcomeReaped {
		return false
	}
	if record.Outcome != nil {
		return record.Outcome.Outcome == intent.Outcome &&
			recordedOutcomeCausePermitsIntent(intent.Cause, intent.Outcome)
	}
	return intent.Cause == CauseCanceledAfterAuthorization && intent.Outcome == OutcomeCanceled
}

func orphanedOutcomeCause(cause TerminalCause) bool {
	switch cause {
	case CauseDaemonRestartedAfterAuthorization, CauseSupervisorLostAfterAuthorization, CauseReleaseOutcomeUnknown:
		return true
	default:
		return false
	}
}

func validLegacyUnfencedIntent(record SafetyRecord, intent TerminalIntent) bool {
	if !terminalCauseBackedByDurableFact(record, intent.Cause) {
		return false
	}
	if intent.Outcome == OutcomeOrphaned || intent.Outcome == OutcomeReaped {
		return false
	}
	if record.Outcome != nil {
		return record.Outcome.Outcome == intent.Outcome &&
			recordedOutcomeCausePermitsIntent(intent.Cause, intent.Outcome)
	}
	return validNeverPermittedIntent(intent)
}

func validTerminalCompatibility(record SafetyRecord, intent TerminalIntent, proof TerminalProof) bool {
	if !terminalCauseBackedByDurableFact(record, intent.Cause) {
		return false
	}
	if intent.Outcome == OutcomeOrphaned && record.Result != nil {
		return false
	}
	switch proof {
	case ProofNeverPermittedAndRetired:
		return validExecutionImpossibleIntent(record, intent)
	case ProofCleanQuiescentOutcomeAndRetired:
		return cleanQuiescentOutcomeAndRetired(record, intent)
	case ProofContained:
		return validContainedIntent(record, intent)
	case ProofLegacyUnfencedOutcome:
		return validLegacyUnfencedIntent(record, intent)
	case ProofUnresolvedAbsence:
		return validUnresolvedAbsenceIntent(record, intent)
	default:
		return false
	}
}

func recordedOutcomeCausePermitsIntent(cause TerminalCause, outcome Outcome) bool {
	switch cause {
	case CauseCompletedNormally:
		return cleanTerminalOutcome(outcome)
	case CauseResponseUndeliverable:
		return recordedExecutionOutcome(outcome)
	case CauseCanceledAfterAuthorization:
		return outcome == OutcomeCanceled
	case CauseDaemonRestartedAfterAuthorization, CauseSupervisorLostAfterAuthorization, CauseReleaseOutcomeUnknown:
		return recordedExecutionOutcome(outcome)
	case CauseCorruptProjection:
		return outcome == OutcomeQuarantined
	default:
		return false
	}
}

func terminalCauseBackedByDurableFact(record SafetyRecord, cause TerminalCause) bool {
	switch cause {
	case CauseReleaseDefinitelyNotSent:
		return authoritativeLaunchReleaseOutcome(record.Attempt, LaunchReleaseNotSent) &&
			!hasAttemptExecutionEvidence(record)
	case CauseReleaseOutcomeUnknown:
		return authoritativeLaunchReleaseOutcome(record.Attempt, LaunchReleaseSentUnknown) &&
			!hasRecordedExecutionOutcome(record)
	default:
		return true
	}
}

func authoritativeLaunchReleaseOutcome(proof AttemptProof, outcome LaunchReleaseOutcome) bool {
	launch, ok := authoritativeLaunch(proof)
	return ok && launch.ReleaseOutcome != nil && launch.ReleaseOutcome.Outcome == outcome
}

func authoritativeLaunch(proof AttemptProof) (*LaunchProof, bool) {
	ordinals := proof.Launches.FilledOrdinals()
	for i := len(ordinals) - 1; i >= 0; i-- {
		if launch, ok := proof.Launches.Get(ordinals[i]); ok {
			if launch.Grant != nil {
				return launch, true
			}
		}
	}
	return nil, false
}

func hasAttemptExecutionEvidence(record SafetyRecord) bool {
	if hasRecordedExecutionOutcome(record) {
		return true
	}
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && launchHasReleaseSentEvidence(*launch) {
			return true
		}
	}
	return false
}

func hasRecordedExecutionOutcome(record SafetyRecord) bool {
	return record.Outcome != nil && recordedExecutionOutcome(record.Outcome.Outcome)
}

func launchHasReleaseSentEvidence(launch LaunchProof) bool {
	if launch.Released != nil {
		return true
	}
	if launch.ReleaseOutcome == nil {
		return false
	}
	switch launch.ReleaseOutcome.Outcome {
	case LaunchReleaseSentUnknown, LaunchReleaseAcked:
		return true
	default:
		return false
	}
}

func recordedExecutionOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeInterrupted, OutcomeCanceled:
		return true
	default:
		return false
	}
}

func executionImpossibleCause(cause TerminalCause) bool {
	switch cause {
	case CauseReleaseDefinitelyNotSent,
		CauseCanceledBeforeAuthorization,
		CauseDaemonRestartedBeforeAuthorization,
		CauseSupervisorLostBeforeAuthorization:
		return true
	default:
		return false
	}
}
