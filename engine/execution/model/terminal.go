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
	if record.Attempt.Retirement == nil {
		return 0, precondition("terminal derivation requires retirement certificate")
	}
	if record.Attempt.Grants.Count() == 0 && record.Attempt.Consumed.Count() == 0 && record.Attempt.Quiescence.Count() == 0 {
		if !validNeverPermittedIntent(intent) {
			return 0, precondition("terminal intent is incompatible with never-permitted proof")
		}
		return ProofNeverPermittedAndRetired, nil
	}
	if record.Attempt.Containment != nil {
		if !validContainedIntent(intent) {
			return 0, precondition("terminal intent is incompatible with contained proof")
		}
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
	if record.Attempt.Containment != nil || record.Attempt.Grants.Count() == 0 {
		return false
	}
	for _, ordinal := range record.Attempt.Grants.FilledOrdinals() {
		if _, ok := record.Attempt.Consumed.Get(ordinal); !ok {
			return false
		}
		if _, ok := record.Attempt.Quiescence.Get(ordinal); !ok {
			return false
		}
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
	default:
		return false
	}
}
