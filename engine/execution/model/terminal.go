package model

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
	result, err := terminalResult(record, intent.Outcome)
	if err != nil {
		return TerminalCertificate{}, err
	}
	proof, err := deriveTerminalProof(record, intent)
	if err != nil {
		return TerminalCertificate{}, err
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
	}, nil
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
	return intent, nil
}

func terminalResult(record SafetyRecord, outcome Outcome) (*ResultRef, error) {
	if !completionOutcome(outcome) {
		return nil, nil
	}
	if record.Result == nil {
		return nil, precondition("completed terminal outcome requires result certificate")
	}
	result := record.Result.Result
	return &result, nil
}

func deriveTerminalProof(record SafetyRecord, intent TerminalIntent) (TerminalProof, error) {
	if record.Mode == ModeLegacyUnfenced {
		if hasAnyLaunchEvidence(record.Attempt) {
			return 0, precondition("legacy unfenced terminal proof requires no launch evidence")
		}
		return ProofLegacyUnfencedOutcome, nil
	}
	if !hasAnyGrant(record.Attempt) && !hasAnyRelease(record.Attempt) {
		if !allLaunchGroupsQuiescent(record.Attempt) {
			return 0, precondition("terminal derivation requires quiescence for every bound group")
		}
		if !validNeverPermittedIntent(intent) {
			return 0, precondition("terminal intent is incompatible with never-permitted proof")
		}
		return ProofNeverPermittedAndRetired, nil
	}
	if allLaunchGroupsQuiescent(record.Attempt) && validContainedIntent(intent) {
		return ProofContained, nil
	}
	if cleanQuiescentOutcomeAndRetired(record, intent) {
		return ProofCleanQuiescentOutcomeAndRetired, nil
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
	case CauseCanceledBeforeAuthorization, CauseResponseUndeliverable:
		return intent.Outcome == OutcomeCanceled
	case CauseDaemonRestartedBeforeAuthorization, CauseSupervisorLostBeforeAuthorization:
		return intent.Outcome == OutcomeFailed
	case CauseCorruptProjection:
		return intent.Outcome == OutcomeQuarantined
	default:
		return false
	}
}

func validContainedIntent(intent TerminalIntent) bool {
	switch intent.Cause {
	case CauseCanceledAfterAuthorization:
		return intent.Outcome == OutcomeCanceled
	case CauseDaemonRestartedAfterAuthorization, CauseSupervisorLostAfterAuthorization:
		return intent.Outcome == OutcomeReaped
	case CauseCorruptProjection:
		return intent.Outcome == OutcomeQuarantined
	case CauseReleaseOutcomeUnknown, CauseReleaseDefinitelyNotSent:
		return intent.Outcome == OutcomeFailed
	default:
		return false
	}
}
