package authority

type DurabilityOutcome uint8

const (
	DefinitelyNotCommitted DurabilityOutcome = iota + 1
	CommittedAndAnchored
	CommitOutcomeUnknown
)

type DBCommitOutcome uint8

const (
	DBDefinitelyNotCommitted DBCommitOutcome = iota + 1
	DBCommitted
	DBCommitUnknown
)

func ClassifyDurableMutationOutcome(dbCommit DBCommitOutcome, anchorAdvanced bool) DurabilityOutcome {
	switch dbCommit {
	case DBDefinitelyNotCommitted:
		if anchorAdvanced {
			return CommitOutcomeUnknown
		}
		return DefinitelyNotCommitted
	case DBCommitted:
		if anchorAdvanced {
			return CommittedAndAnchored
		}
		return CommitOutcomeUnknown
	case DBCommitUnknown:
		return CommitOutcomeUnknown
	default:
		return CommitOutcomeUnknown
	}
}

type GrantDurabilityAction uint8

const (
	Proceed GrantDurabilityAction = iota + 1
	ContainFailStop
)

func SafeActionForGrantDurability(outcome DurabilityOutcome) GrantDurabilityAction {
	switch outcome {
	case DefinitelyNotCommitted, CommittedAndAnchored:
		return Proceed
	case CommitOutcomeUnknown:
		return ContainFailStop
	default:
		return ContainFailStop
	}
}
