package model

type Mode uint8

const (
	ModeIdentifiedFenced Mode = iota + 1
	ModeLegacyFenced
	ModeLegacyUnfenced
)

func AllModes() []Mode {
	return []Mode{ModeIdentifiedFenced, ModeLegacyFenced, ModeLegacyUnfenced}
}

func (mode Mode) Valid() bool {
	switch mode {
	case ModeIdentifiedFenced, ModeLegacyFenced, ModeLegacyUnfenced:
		return true
	default:
		return false
	}
}

func (mode Mode) Validate() error {
	if !mode.Valid() {
		return invalid("mode", "is unknown")
	}
	return nil
}

func (mode Mode) String() string {
	switch mode {
	case ModeIdentifiedFenced:
		return "IdentifiedFenced"
	case ModeLegacyFenced:
		return "LegacyFenced"
	case ModeLegacyUnfenced:
		return "LegacyUnfenced"
	default:
		return ""
	}
}

type ContainmentScope uint8

const (
	ContainTargetProcessGroup ContainmentScope = iota + 1
	ContainDescendantTree
)

// CurrentContainmentScope states the v1 containment contract: Agentbus contains
// the target process group. Processes that escape via setsid or a new session
// are explicitly out of scope and require stronger platform facilities.
const CurrentContainmentScope = ContainTargetProcessGroup

type Decision uint8

const (
	DecisionAccepted Decision = iota + 1
	DecisionAwaitingAck
	DecisionCancelRequested
	DecisionTerminal
)

func AllDecisions() []Decision {
	return []Decision{DecisionAccepted, DecisionAwaitingAck, DecisionCancelRequested, DecisionTerminal}
}

func (decision Decision) Valid() bool {
	switch decision {
	case DecisionAccepted, DecisionAwaitingAck, DecisionCancelRequested, DecisionTerminal:
		return true
	default:
		return false
	}
}

func (decision Decision) String() string {
	switch decision {
	case DecisionAccepted:
		return "accepted"
	case DecisionAwaitingAck:
		return "awaiting_ack"
	case DecisionCancelRequested:
		return "cancel_requested"
	case DecisionTerminal:
		return "terminal"
	default:
		return ""
	}
}

type Dispatch uint8

const (
	DispatchNone Dispatch = iota + 1
	DispatchScheduled
	DispatchSupervisorPrepared
	DispatchPermitGranted
	DispatchActive
	DispatchReconciling
	DispatchContained
	DispatchResultPublishing
	DispatchDone
)

func AllDispatches() []Dispatch {
	return []Dispatch{
		DispatchNone,
		DispatchScheduled,
		DispatchSupervisorPrepared,
		DispatchPermitGranted,
		DispatchActive,
		DispatchReconciling,
		DispatchContained,
		DispatchResultPublishing,
		DispatchDone,
	}
}

func (dispatch Dispatch) Valid() bool {
	switch dispatch {
	case DispatchNone, DispatchScheduled, DispatchSupervisorPrepared, DispatchPermitGranted, DispatchActive, DispatchReconciling, DispatchContained, DispatchResultPublishing, DispatchDone:
		return true
	default:
		return false
	}
}

func (dispatch Dispatch) String() string {
	switch dispatch {
	case DispatchNone:
		return "none"
	case DispatchScheduled:
		return "scheduled"
	case DispatchSupervisorPrepared:
		return "supervisor_prepared"
	case DispatchPermitGranted:
		return "permit_granted"
	case DispatchActive:
		return "active"
	case DispatchReconciling:
		return "reconciling"
	case DispatchContained:
		return "contained"
	case DispatchResultPublishing:
		return "result_publishing"
	case DispatchDone:
		return "done"
	default:
		return ""
	}
}

type Outcome uint8

const (
	OutcomeNone Outcome = iota + 1
	OutcomeCompleted
	OutcomeCompletedNoncompliant
	OutcomeFailed
	OutcomeTimedOut
	OutcomeCanceled
	OutcomeReaped
	OutcomeInterrupted
	OutcomeQuarantined
)

func AllOutcomes() []Outcome {
	return []Outcome{
		OutcomeNone,
		OutcomeCompleted,
		OutcomeCompletedNoncompliant,
		OutcomeFailed,
		OutcomeTimedOut,
		OutcomeCanceled,
		OutcomeReaped,
		OutcomeInterrupted,
		OutcomeQuarantined,
	}
}

func (outcome Outcome) Valid() bool {
	switch outcome {
	case OutcomeNone, OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeCanceled, OutcomeReaped, OutcomeInterrupted, OutcomeQuarantined:
		return true
	default:
		return false
	}
}

func (outcome Outcome) ValidateTerminal() error {
	if !terminalOutcome(outcome) {
		return invalid("outcome", "must be terminal")
	}
	return nil
}

func (outcome Outcome) String() string {
	switch outcome {
	case OutcomeNone:
		return "none"
	case OutcomeCompleted:
		return "completed"
	case OutcomeCompletedNoncompliant:
		return "completed_noncompliant"
	case OutcomeFailed:
		return "failed"
	case OutcomeTimedOut:
		return "timed_out"
	case OutcomeCanceled:
		return "canceled"
	case OutcomeReaped:
		return "reaped"
	case OutcomeInterrupted:
		return "interrupted"
	case OutcomeQuarantined:
		return "quarantined"
	default:
		return ""
	}
}

type PublicState uint8

const (
	PublicQueued PublicState = iota + 1
	PublicStarting
	PublicRunning
	PublicRetrying
	PublicCompleted
	PublicCompletedNoncompliant
	PublicInterrupted
	PublicQuarantined
	PublicFailed
	PublicTimedOut
	PublicCanceled
	PublicReaped
	PublicOrphaned
)

func AllPublicStates() []PublicState {
	return []PublicState{
		PublicQueued,
		PublicStarting,
		PublicRunning,
		PublicRetrying,
		PublicCompleted,
		PublicCompletedNoncompliant,
		PublicInterrupted,
		PublicQuarantined,
		PublicFailed,
		PublicTimedOut,
		PublicCanceled,
		PublicReaped,
		PublicOrphaned,
	}
}

func (state PublicState) Valid() bool {
	switch state {
	case PublicQueued, PublicStarting, PublicRunning, PublicRetrying, PublicCompleted, PublicCompletedNoncompliant, PublicInterrupted, PublicQuarantined, PublicFailed, PublicTimedOut, PublicCanceled, PublicReaped, PublicOrphaned:
		return true
	default:
		return false
	}
}

func (state PublicState) String() string {
	switch state {
	case PublicQueued:
		return "queued"
	case PublicStarting:
		return "starting"
	case PublicRunning:
		return "running"
	case PublicRetrying:
		return "retrying"
	case PublicCompleted:
		return "completed"
	case PublicCompletedNoncompliant:
		return "completed_noncompliant"
	case PublicInterrupted:
		return "interrupted"
	case PublicQuarantined:
		return "quarantined"
	case PublicFailed:
		return "failed"
	case PublicTimedOut:
		return "timed_out"
	case PublicCanceled:
		return "canceled"
	case PublicReaped:
		return "reaped"
	case PublicOrphaned:
		return "orphaned"
	default:
		return ""
	}
}

type TerminalProof uint8

const (
	ProofNeverPermittedAndRetired TerminalProof = iota + 1
	ProofCleanQuiescentOutcomeAndRetired
	ProofContained
)

func (proof TerminalProof) Valid() bool {
	switch proof {
	case ProofNeverPermittedAndRetired, ProofCleanQuiescentOutcomeAndRetired, ProofContained:
		return true
	default:
		return false
	}
}

func (proof TerminalProof) Validate() error {
	if !proof.Valid() {
		return invalid("terminal_proof", "is unknown")
	}
	return nil
}

func (proof TerminalProof) String() string {
	switch proof {
	case ProofNeverPermittedAndRetired:
		return "NeverPermittedAndRetired"
	case ProofCleanQuiescentOutcomeAndRetired:
		return "CleanQuiescentOutcomeAndRetired"
	case ProofContained:
		return "Contained"
	default:
		return ""
	}
}

type TerminalCause uint8

const (
	CauseCompletedNormally TerminalCause = iota + 1
	CauseCanceledBeforeAuthorization
	CauseCanceledAfterAuthorization
	CauseDaemonRestartedBeforeAuthorization
	CauseDaemonRestartedAfterAuthorization
	CauseSupervisorLostBeforeAuthorization
	CauseSupervisorLostAfterAuthorization
	CauseCorruptProjection
	CauseResponseUndeliverable
)

func AllTerminalCauses() []TerminalCause {
	return []TerminalCause{
		CauseCompletedNormally,
		CauseCanceledBeforeAuthorization,
		CauseCanceledAfterAuthorization,
		CauseDaemonRestartedBeforeAuthorization,
		CauseDaemonRestartedAfterAuthorization,
		CauseSupervisorLostBeforeAuthorization,
		CauseSupervisorLostAfterAuthorization,
		CauseCorruptProjection,
		CauseResponseUndeliverable,
	}
}

func (cause TerminalCause) Valid() bool {
	switch cause {
	case CauseCompletedNormally, CauseCanceledBeforeAuthorization, CauseCanceledAfterAuthorization, CauseDaemonRestartedBeforeAuthorization, CauseDaemonRestartedAfterAuthorization, CauseSupervisorLostBeforeAuthorization, CauseSupervisorLostAfterAuthorization, CauseCorruptProjection, CauseResponseUndeliverable:
		return true
	default:
		return false
	}
}

func (cause TerminalCause) Validate() error {
	if !cause.Valid() {
		return invalid("terminal_cause", "is unknown")
	}
	return nil
}

func (cause TerminalCause) String() string {
	switch cause {
	case CauseCompletedNormally:
		return "completed_normally"
	case CauseCanceledBeforeAuthorization:
		return "canceled_before_authorization"
	case CauseCanceledAfterAuthorization:
		return "canceled_after_authorization"
	case CauseDaemonRestartedBeforeAuthorization:
		return "daemon_restarted_before_authorization"
	case CauseDaemonRestartedAfterAuthorization:
		return "daemon_restarted_after_authorization"
	case CauseSupervisorLostBeforeAuthorization:
		return "supervisor_lost_before_authorization"
	case CauseSupervisorLostAfterAuthorization:
		return "supervisor_lost_after_authorization"
	case CauseCorruptProjection:
		return "corrupt_projection"
	case CauseResponseUndeliverable:
		return "response_undeliverable"
	default:
		return ""
	}
}

func (cause TerminalCause) ProtocolString() string {
	return cause.String()
}

func terminalOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeCanceled, OutcomeReaped, OutcomeInterrupted, OutcomeQuarantined:
		return true
	default:
		return false
	}
}

func completionOutcome(outcome Outcome) bool {
	switch outcome {
	case OutcomeCompleted, OutcomeCompletedNoncompliant:
		return true
	default:
		return false
	}
}
