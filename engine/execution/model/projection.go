package model

type ProjectionMetadata struct {
	SessionID string
}

func (metadata ProjectionMetadata) Validate() error {
	return validateOptionalToken("projection.session_id", metadata.SessionID)
}

type JobProjection struct {
	SchemaVersion uint16
	Revision      uint64
	JobID         JobID
	RequestKey    RequestKey
	TaskIdentity  TaskIdentity
	Mode          Mode
	Decision      Decision
	Dispatch      Dispatch
	Outcome       Outcome
	Public        PublicState
	TerminalCause TerminalCause
	SessionID     string
}

// Project rebuilds the read model from the proof-bearing SafetyRecord. The
// returned projection is cacheable, but it is not evidence and must not be used
// as authority for safety decisions.
func Project(record SafetyRecord, metadata ProjectionMetadata) (JobProjection, error) {
	if err := record.Validate(); err != nil {
		return JobProjection{}, err
	}
	if err := metadata.Validate(); err != nil {
		return JobProjection{}, err
	}
	decision := projectDecision(record)
	dispatch := projectDispatch(record)
	outcome := projectOutcome(record)
	return JobProjection{
		SchemaVersion: record.SchemaVersion,
		Revision:      record.Revision,
		JobID:         record.JobID,
		RequestKey:    record.RequestKey,
		TaskIdentity:  record.TaskIdentity,
		Mode:          record.Mode,
		Decision:      decision,
		Dispatch:      dispatch,
		Outcome:       outcome,
		Public:        PublicProjection(decision, dispatch, outcome),
		TerminalCause: projectTerminalCause(record),
		SessionID:     metadata.SessionID,
	}, nil
}

func projectDecision(record SafetyRecord) Decision {
	if record.Terminal != nil {
		return DecisionTerminal
	}
	if record.Cancel != nil {
		return DecisionCancelRequested
	}
	if record.Mode == ModeLegacyFenced && record.Acknowledgement == nil {
		return DecisionAwaitingAck
	}
	return DecisionAccepted
}

func projectOutcome(record SafetyRecord) Outcome {
	if record.Terminal != nil {
		return record.Terminal.Outcome
	}
	if record.Outcome != nil {
		return record.Outcome.Outcome
	}
	return OutcomeNone
}

func projectTerminalCause(record SafetyRecord) TerminalCause {
	if record.Terminal == nil {
		return 0
	}
	return record.Terminal.Cause
}

func projectDispatch(record SafetyRecord) Dispatch {
	if record.Terminal != nil {
		return DispatchDone
	}
	if record.Result != nil {
		return DispatchResultPublishing
	}
	if hasContainedQuiescence(record.Attempt) {
		return DispatchContained
	}
	if hasActiveLaunch(record.Attempt) {
		return DispatchActive
	}
	if hasUnconsumedGrant(record.Attempt) {
		return DispatchPermitGranted
	}
	if record.Cancel != nil && allLaunchGroupsQuiescent(record.Attempt) {
		return DispatchReconciling
	}
	if hasPreparedLaunch(record.Attempt) {
		return DispatchSupervisorPrepared
	}
	return DispatchScheduled
}

func hasUnconsumedGrant(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Grant != nil && launch.Released == nil {
			return true
		}
	}
	return false
}

func hasActiveLaunch(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Released != nil && launch.Quiescence == nil {
			return true
		}
	}
	return false
}

func hasPreparedLaunch(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Group != nil {
			return true
		}
	}
	return false
}

func hasContainedQuiescence(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Quiescence != nil && launch.Quiescence.Method == QuiescenceTermKill {
			return true
		}
	}
	return false
}

func PublicProjection(decision Decision, dispatch Dispatch, outcome Outcome) PublicState {
	if !decision.Valid() || !dispatch.Valid() || !outcome.Valid() {
		return 0
	}
	if decision == DecisionTerminal {
		if dispatch != DispatchDone || !terminalOutcome(outcome) {
			return 0
		}
		return terminalPublicForOutcome(outcome)
	}

	switch dispatch {
	case DispatchNone, DispatchScheduled:
		return PublicQueued
	case DispatchSupervisorPrepared, DispatchPermitGranted:
		return PublicStarting
	case DispatchActive, DispatchReconciling, DispatchContained, DispatchResultPublishing:
		return PublicRunning
	default:
		return 0
	}
}

func terminalPublicForOutcome(outcome Outcome) PublicState {
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
	case OutcomeOrphaned:
		return PublicOrphaned
	default:
		return 0
	}
}

func terminalPublicState(state PublicState) bool {
	switch state {
	case PublicCompleted, PublicCompletedNoncompliant, PublicInterrupted, PublicQuarantined, PublicFailed, PublicTimedOut, PublicCanceled, PublicReaped, PublicOrphaned:
		return true
	default:
		return false
	}
}

func ReachableInternal(decision Decision, dispatch Dispatch, outcome Outcome) bool {
	if !decision.Valid() || !dispatch.Valid() || !outcome.Valid() {
		return false
	}
	if decision == DecisionTerminal {
		return dispatch == DispatchDone && terminalOutcome(outcome)
	}
	if outcome == OutcomeNone {
		switch decision {
		case DecisionAccepted:
			switch dispatch {
			case DispatchScheduled, DispatchSupervisorPrepared, DispatchPermitGranted, DispatchActive, DispatchReconciling, DispatchContained, DispatchResultPublishing:
				return true
			default:
				return false
			}
		case DecisionAwaitingAck:
			return dispatch == DispatchScheduled || dispatch == DispatchSupervisorPrepared
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
