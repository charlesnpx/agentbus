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

type AttemptProof struct {
	Ref         AttemptRef
	Supervisor  *SupervisorIdentity
	Grants      LaunchSlots[LaunchGrant]
	Consumed    LaunchSlots[LaunchConsumed]
	Quiescence  LaunchSlots[QuiescenceCertificate]
	Retirement  *RetirementCertificate
	Containment *ContainmentCertificate
}

func (proof AttemptProof) Validate() error {
	if err := proof.Ref.Validate(); err != nil {
		return err
	}
	if proof.Supervisor != nil {
		if err := proof.Supervisor.Validate(); err != nil {
			return err
		}
	}
	if err := proof.validateLaunchGrants(); err != nil {
		return err
	}
	if err := proof.validateLaunchConsumed(); err != nil {
		return err
	}
	if err := proof.validateQuiescence(); err != nil {
		return err
	}
	if proof.Retirement != nil {
		if err := proof.Retirement.Validate(); err != nil {
			return err
		}
		if err := validateAttemptField("retirement.attempt", proof.Retirement.Attempt, proof.Ref); err != nil {
			return err
		}
		if proof.Supervisor != nil && !proof.Retirement.Supervisor.Equal(*proof.Supervisor) {
			return invalid("retirement.supervisor", "supervisor identity mismatch")
		}
	}
	if proof.Containment != nil {
		if err := proof.Containment.Validate(); err != nil {
			return err
		}
		if err := validateAttemptField("containment.attempt", proof.Containment.Attempt, proof.Ref); err != nil {
			return err
		}
		if proof.Supervisor != nil && !proof.Containment.Supervisor.Equal(*proof.Supervisor) {
			return invalid("containment.supervisor", "supervisor identity mismatch")
		}
	}
	return nil
}

func (proof AttemptProof) validateLaunchGrants() error {
	return forEachLaunchSlot(proof.Grants, func(ordinal LaunchOrdinal, grant LaunchGrant) error {
		if err := grant.Validate(); err != nil {
			return err
		}
		if grant.Ordinal != ordinal {
			return invalid("launch_grant.ordinal", "does not match fixed slot")
		}
		if err := validateAttemptField("launch_grant.attempt", grant.Attempt, proof.Ref); err != nil {
			return err
		}
		if proof.Supervisor != nil && !grant.Supervisor.Equal(*proof.Supervisor) {
			return invalid("launch_grant.supervisor", "supervisor identity mismatch")
		}
		return nil
	})
}

func (proof AttemptProof) validateLaunchConsumed() error {
	return forEachLaunchSlot(proof.Consumed, func(ordinal LaunchOrdinal, consumed LaunchConsumed) error {
		if err := consumed.Validate(); err != nil {
			return err
		}
		if consumed.Ordinal != ordinal {
			return invalid("launch_consumed.ordinal", "does not match fixed slot")
		}
		if err := validateAttemptField("launch_consumed.attempt", consumed.Attempt, proof.Ref); err != nil {
			return err
		}
		grant, ok := proof.Grants.Get(ordinal)
		if !ok {
			return invalid("launch_consumed.grant", "matching grant is required")
		}
		if consumed.Nonce != grant.Nonce {
			return invalid("launch_consumed.nonce", "does not match grant nonce")
		}
		return nil
	})
}

func (proof AttemptProof) validateQuiescence() error {
	return forEachLaunchSlot(proof.Quiescence, func(ordinal LaunchOrdinal, certificate QuiescenceCertificate) error {
		if err := certificate.Validate(); err != nil {
			return err
		}
		if certificate.Ordinal != ordinal {
			return invalid("quiescence.ordinal", "does not match fixed slot")
		}
		if err := validateAttemptField("quiescence.attempt", certificate.Attempt, proof.Ref); err != nil {
			return err
		}
		consumed, ok := proof.Consumed.Get(ordinal)
		if !ok {
			return invalid("quiescence.consumed", "matching launch consumption is required")
		}
		if !certificate.Child.Equal(consumed.Child) {
			return invalid("quiescence.child", "does not match launch consumption")
		}
		return nil
	})
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
		if record.Attempt.Grants.Count() != 0 || record.Attempt.Consumed.Count() != 0 || record.Attempt.Quiescence.Count() != 0 {
			return invalid("terminal.proof", "never-permitted proof cannot have launch evidence")
		}
		if record.Attempt.Retirement == nil {
			return invalid("terminal.proof", "never-permitted proof requires retirement certificate")
		}
	case ProofCleanQuiescentOutcomeAndRetired:
		if record.Attempt.Retirement == nil {
			return invalid("terminal.proof", "clean proof requires retirement certificate")
		}
		if record.Attempt.Containment != nil {
			return invalid("terminal.proof", "clean proof cannot include containment")
		}
		if record.Attempt.Grants.Count() == 0 {
			return invalid("terminal.proof", "clean proof requires launch grant evidence")
		}
		for _, ordinal := range record.Attempt.Grants.FilledOrdinals() {
			if _, ok := record.Attempt.Consumed.Get(ordinal); !ok {
				return invalid("terminal.proof", "clean proof requires every grant to be consumed")
			}
			if _, ok := record.Attempt.Quiescence.Get(ordinal); !ok {
				return invalid("terminal.proof", "clean proof requires every launch to be quiescent")
			}
		}
	case ProofContained:
		if record.Attempt.Containment == nil {
			return invalid("terminal.proof", "contained proof requires containment certificate")
		}
		if record.Attempt.Retirement == nil {
			return invalid("terminal.proof", "contained proof requires retirement certificate")
		}
	default:
		return invalid("terminal.proof", "is unknown")
	}
	return nil
}
