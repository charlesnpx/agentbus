package execution

// PublicProjection is the model's complete internal-to-public status table.
func PublicProjection(decision Decision, dispatch Dispatch, outcome Outcome) Public {
	switch outcome {
	case OutcomeCompleted:
		return PublicCompleted
	case OutcomeCompletedNoncompliant:
		return PublicCompletedNoncompliant
	case OutcomeFailed:
		return PublicFailed
	case OutcomeTimedOut:
		return PublicTimedOut
	case OutcomeCanceled:
		return PublicCanceled
	case OutcomeReaped:
		return PublicReaped
	case OutcomeInterrupted:
		return PublicInterrupted
	case OutcomeQuarantined:
		return PublicQuarantined
	}

	switch dispatch {
	case DispatchNone, DispatchScheduled:
		return PublicQueued
	case DispatchSupervisorPrepared, DispatchPermitGranted:
		return PublicStarting
	case DispatchActive:
		return PublicRunning
	case DispatchReconciling, DispatchContained:
		return PublicOrphaned
	case DispatchResultPublishing:
		return PublicRunning
	case DispatchDone:
		if decision == DecisionTerminal {
			return PublicFailed
		}
		return PublicOrphaned
	default:
		return PublicFailed
	}
}

// ReachableInternal reports whether the tuple is expected to arise in the
// reference model. PublicProjection remains total even for unreachable tuples.
func ReachableInternal(decision Decision, dispatch Dispatch, outcome Outcome) bool {
	if decision == DecisionTerminal {
		return dispatch == DispatchDone && outcome != OutcomeNone
	}
	if outcome != OutcomeNone {
		return dispatch == DispatchResultPublishing || dispatch == DispatchContained || dispatch == DispatchActive || dispatch == DispatchSupervisorPrepared
	}
	if decision == DecisionAwaitingAck {
		return dispatch == DispatchNone || dispatch == DispatchSupervisorPrepared
	}
	return dispatch != DispatchDone
}

func terminalOutcome(outcome Outcome) bool {
	return outcome != OutcomeNone
}
