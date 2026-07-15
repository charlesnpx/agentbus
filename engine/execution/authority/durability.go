package authority

type DurabilityOutcome uint8

const (
	DefinitelyNotCommitted DurabilityOutcome = iota + 1
	CommittedAndAnchored
	CommitOutcomeUnknown
)

func ClassifyDurableMutationOutcome(dbCommitted, anchorAdvanced bool) DurabilityOutcome {
	if !dbCommitted {
		return DefinitelyNotCommitted
	}
	if anchorAdvanced {
		return CommittedAndAnchored
	}
	return CommitOutcomeUnknown
}

type GrantDurabilityAction uint8

const (
	Proceed GrantDurabilityAction = iota + 1
	ContainFailStop
)

func SafeActionForGrantDurability(outcome DurabilityOutcome) GrantDurabilityAction {
	if outcome == CommitOutcomeUnknown {
		return ContainFailStop
	}
	return Proceed
}
