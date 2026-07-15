package model

type LaunchOrdinal uint8

const (
	LaunchOrdinalOne LaunchOrdinal = iota + 1
	LaunchOrdinalTwo
)

func NewLaunchOrdinal(value uint8) (LaunchOrdinal, error) {
	ordinal := LaunchOrdinal(value)
	if err := ordinal.Validate(); err != nil {
		return 0, err
	}
	return ordinal, nil
}

func (ordinal LaunchOrdinal) Valid() bool {
	return ordinal == LaunchOrdinalOne || ordinal == LaunchOrdinalTwo
}

func (ordinal LaunchOrdinal) Validate() error {
	if !ordinal.Valid() {
		return invalid("launch_ordinal", "must be 1 or 2")
	}
	return nil
}

func (ordinal LaunchOrdinal) String() string {
	switch ordinal {
	case LaunchOrdinalOne:
		return "1"
	case LaunchOrdinalTwo:
		return "2"
	default:
		return ""
	}
}

type LaunchSlots[T any] struct {
	First  *T
	Second *T
}

func (slots LaunchSlots[T]) Get(ordinal LaunchOrdinal) (*T, bool) {
	switch ordinal {
	case LaunchOrdinalOne:
		return slots.First, slots.First != nil
	case LaunchOrdinalTwo:
		return slots.Second, slots.Second != nil
	default:
		return nil, false
	}
}

func (slots LaunchSlots[T]) Count() int {
	count := 0
	if slots.First != nil {
		count++
	}
	if slots.Second != nil {
		count++
	}
	return count
}

func (slots LaunchSlots[T]) FilledOrdinals() []LaunchOrdinal {
	out := make([]LaunchOrdinal, 0, 2)
	if slots.First != nil {
		out = append(out, LaunchOrdinalOne)
	}
	if slots.Second != nil {
		out = append(out, LaunchOrdinalTwo)
	}
	return out
}

type LaunchProof struct {
	Ordinal    LaunchOrdinal
	Group      *GroupRef
	Grant      *LaunchGrant
	Released   *LaunchReleaseFact
	Quiescence *QuiescenceCertificate
}

type AttemptProof struct {
	Ref      AttemptRef
	Launches LaunchSlots[LaunchProof]
}

func (proof AttemptProof) Validate() error {
	if err := proof.Ref.Validate(); err != nil {
		return err
	}
	return forEachLaunchSlot(proof.Launches, func(ordinal LaunchOrdinal, launch LaunchProof) error {
		if err := launch.Validate(); err != nil {
			return err
		}
		if launch.Ordinal != ordinal {
			return invalid("launch.ordinal", "does not match fixed slot")
		}
		if err := validateLaunchProofAttempt(proof.Ref, launch); err != nil {
			return err
		}
		return nil
	})
}

func (launch LaunchProof) Validate() error {
	if err := launch.Ordinal.Validate(); err != nil {
		return err
	}
	if launch.Group == nil {
		return invalid("launch.group", "durable group reference is required")
	}
	if err := launch.Group.Validate(); err != nil {
		return err
	}
	if launch.Group.Launch.Ordinal != launch.Ordinal {
		return invalid("launch.group.ordinal", "does not match launch ordinal")
	}
	if launch.Grant != nil {
		if err := launch.Grant.Validate(); err != nil {
			return err
		}
		if launch.Grant.Ordinal != launch.Ordinal {
			return invalid("launch_grant.ordinal", "does not match launch ordinal")
		}
	}
	if launch.Released != nil {
		if err := launch.Released.Validate(); err != nil {
			return err
		}
		if launch.Released.Ordinal != launch.Ordinal {
			return invalid("launch_release.ordinal", "does not match launch ordinal")
		}
		if launch.Grant == nil {
			return invalid("launch_release.grant", "matching grant is required")
		}
		if launch.Released.Nonce != launch.Grant.Nonce {
			return invalid("launch_release.nonce", "does not match grant nonce")
		}
	}
	if launch.Quiescence != nil {
		if err := launch.Quiescence.Validate(); err != nil {
			return err
		}
		if launch.Quiescence.Ordinal != launch.Ordinal {
			return invalid("quiescence.ordinal", "does not match launch ordinal")
		}
		if !launch.Quiescence.Group.Equal(*launch.Group) {
			return invalid("quiescence.group", "does not match durable group")
		}
	}
	return nil
}

func validateLaunchProofAttempt(ref AttemptRef, launch LaunchProof) error {
	if launch.Group != nil {
		if err := validateAttemptField("launch.group.attempt", launch.Group.Launch.Attempt, ref); err != nil {
			return err
		}
	}
	if launch.Grant != nil {
		if err := validateAttemptField("launch_grant.attempt", launch.Grant.Attempt, ref); err != nil {
			return err
		}
	}
	if launch.Released != nil {
		if err := validateAttemptField("launch_release.attempt", launch.Released.Attempt, ref); err != nil {
			return err
		}
	}
	if launch.Quiescence != nil {
		if err := validateAttemptField("quiescence.attempt", launch.Quiescence.Attempt, ref); err != nil {
			return err
		}
	}
	return nil
}

func forEachLaunchSlot[T any](slots LaunchSlots[T], fn func(LaunchOrdinal, T) error) error {
	if slots.First != nil {
		if err := fn(LaunchOrdinalOne, *slots.First); err != nil {
			return err
		}
	}
	if slots.Second != nil {
		if err := fn(LaunchOrdinalTwo, *slots.Second); err != nil {
			return err
		}
	}
	return nil
}

type SafetyRecord struct {
	SchemaVersion   uint16
	Revision        uint64
	JobID           JobID
	RequestKey      RequestKey
	TaskIdentity    TaskIdentity
	Mode            Mode
	AdmittedBy      BootRef
	Attempt         AttemptProof
	Acknowledgement *AcknowledgementFact
	Cancel          *CancelFact
	Outcome         *OutcomeFact
	Result          *ResultCertificate
	Terminal        *TerminalCertificate
}

func (record SafetyRecord) Validate() error {
	if err := validatePositiveUint16("schema_version", record.SchemaVersion); err != nil {
		return err
	}
	if err := validatePositiveUint64("revision", record.Revision); err != nil {
		return err
	}
	if err := record.JobID.Validate(); err != nil {
		return err
	}
	if err := record.RequestKey.Validate(); err != nil {
		return err
	}
	if err := record.TaskIdentity.Validate(); err != nil {
		return err
	}
	if err := record.Mode.Validate(); err != nil {
		return err
	}
	if err := record.AdmittedBy.Validate(); err != nil {
		return err
	}
	if err := record.Attempt.Validate(); err != nil {
		return err
	}
	if err := validateJobField("attempt.job_id", record.Attempt.Ref.JobID, record.JobID); err != nil {
		return err
	}
	if err := record.validateOptionalFacts(); err != nil {
		return err
	}
	return nil
}

func (record SafetyRecord) validateOptionalFacts() error {
	if record.Acknowledgement != nil {
		if err := record.Acknowledgement.Validate(); err != nil {
			return err
		}
		if err := validateAttemptField("acknowledgement.attempt", record.Acknowledgement.Attempt, record.Attempt.Ref); err != nil {
			return err
		}
	}
	if record.Cancel != nil {
		if err := record.Cancel.Validate(); err != nil {
			return err
		}
		if err := validateJobField("cancel.job_id", record.Cancel.JobID, record.JobID); err != nil {
			return err
		}
	}
	if record.Outcome != nil {
		if err := record.Outcome.Validate(); err != nil {
			return err
		}
		if err := validateAttemptField("outcome.attempt", record.Outcome.Attempt, record.Attempt.Ref); err != nil {
			return err
		}
	}
	if record.Result != nil {
		if err := record.Result.Validate(); err != nil {
			return err
		}
		if err := validateJobField("result.job_id", record.Result.JobID, record.JobID); err != nil {
			return err
		}
	}
	if record.Terminal != nil {
		if err := record.Terminal.Validate(); err != nil {
			return err
		}
		if err := validateAttemptField("terminal.attempt", record.Terminal.Attempt, record.Attempt.Ref); err != nil {
			return err
		}
		if err := validateJobField("terminal.job_id", record.Terminal.JobID, record.JobID); err != nil {
			return err
		}
		if record.Outcome != nil && record.Terminal.Outcome != record.Outcome.Outcome {
			return invalid("terminal.outcome", "does not match outcome fact")
		}
		if completionOutcome(record.Terminal.Outcome) && record.Result == nil {
			return invalid("terminal.result", "completed terminal certificate requires result certificate")
		}
		if record.Terminal.Result != nil && record.Result != nil && *record.Terminal.Result != record.Result.Result {
			return invalid("terminal.result", "does not match result certificate")
		}
		if err := record.validateTerminalProofSupport(); err != nil {
			return err
		}
	}
	return nil
}

func (record SafetyRecord) validateTerminalProofSupport() error {
	switch record.Terminal.Proof {
	case ProofNeverPermittedAndRetired:
		if hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt) {
			return invalid("terminal.proof", "never-permitted proof cannot have grant or release evidence")
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			return invalid("terminal.proof", "never-permitted proof requires quiescence for every bound group")
		}
	case ProofCleanQuiescentOutcomeAndRetired:
		if !hasAnyGrant(record.Attempt) {
			return invalid("terminal.proof", "clean proof requires launch grant evidence")
		}
		if !allGrantedLaunchesReleasedAndQuiescent(record.Attempt) {
			return invalid("terminal.proof", "clean proof requires every granted launch to be released and quiescent")
		}
	case ProofContained:
		if !hasAnyLaunchEvidence(record.Attempt) {
			return invalid("terminal.proof", "contained proof requires launch evidence")
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			return invalid("terminal.proof", "contained proof requires quiescence for every bound group")
		}
	default:
		return invalid("terminal.proof", "is unknown")
	}
	return nil
}
