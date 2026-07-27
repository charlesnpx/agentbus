package model

type CleanupDisposition string

const (
	CleanupDispositionNoExecutionPossible CleanupDisposition = "no_execution_possible"
	CleanupDispositionVerifiedAbsent      CleanupDisposition = "verified_absent"
	CleanupDispositionUnresolved          CleanupDisposition = "unresolved"
)

func (disposition CleanupDisposition) Valid() bool {
	switch disposition {
	case CleanupDispositionNoExecutionPossible, CleanupDispositionVerifiedAbsent, CleanupDispositionUnresolved:
		return true
	default:
		return false
	}
}

func (disposition CleanupDisposition) String() string {
	return string(disposition)
}

func DeriveCleanupDisposition(record SafetyRecord) CleanupDisposition {
	if record.Terminal == nil {
		return ""
	}
	switch record.Terminal.Proof {
	case ProofUnresolvedAbsence:
		return CleanupDispositionUnresolved
	case ProofNeverPermittedAndRetired:
		return CleanupDispositionNoExecutionPossible
	}
	if allLaunchGroupsQuiescent(record.Attempt) {
		return CleanupDispositionVerifiedAbsent
	}
	return CleanupDispositionUnresolved
}
