package execution

// PublicProjection is the model's complete internal-to-public status table.
func PublicProjection(decision Decision, dispatch Dispatch, outcome Outcome) Public {
	if !validDecision(decision) || !validDispatch(dispatch) || !validOutcome(outcome) {
		return ""
	}
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
	if !validDecision(decision) || !validDispatch(dispatch) || !validOutcome(outcome) {
		return false
	}
	if decision == DecisionTerminal {
		return dispatch == DispatchDone && terminalOutcome(outcome)
	}
	if outcome == OutcomeNone {
		switch decision {
		case DecisionAccepted:
			switch dispatch {
			case DispatchScheduled, DispatchSupervisorPrepared, DispatchPermitGranted, DispatchActive, DispatchReconciling, DispatchContained:
				return true
			default:
				return false
			}
		case DecisionAwaitingAck:
			return dispatch == DispatchSupervisorPrepared
		case DecisionCancelRequested:
			switch dispatch {
			case DispatchScheduled, DispatchSupervisorPrepared, DispatchPermitGranted, DispatchActive, DispatchReconciling, DispatchContained:
				return true
			default:
				return false
			}
		default:
			return false
		}
	}
	if !terminalOutcome(outcome) {
		return false
	}
	switch dispatch {
	case DispatchPermitGranted:
		return false
	case DispatchScheduled, DispatchSupervisorPrepared, DispatchActive, DispatchReconciling, DispatchContained, DispatchResultPublishing:
		return decision == DecisionAccepted || decision == DecisionCancelRequested
	default:
		return false
	}
}

func terminalOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeCanceled, OutcomeReaped, OutcomeInterrupted, OutcomeQuarantined:
		return true
	default:
		return false
	}
}
