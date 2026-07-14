package model

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidCommand       = errors.New("invalid command")
	ErrCommandPrecondition  = errors.New("command precondition failed")
	ErrConflictingDuplicate = errors.New("conflicting duplicate")
)

type ApplyResult struct {
	Record  SafetyRecord
	Changed bool
}

func ValidateSafetyRecord(record SafetyRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	projection, err := Project(record, ProjectionMetadata{})
	if err != nil {
		return err
	}
	if !ReachableInternal(projection.Decision, projection.Dispatch, projection.Outcome) {
		return invalid("projection", "unreachable safety record state")
	}
	return nil
}

func Apply(current SafetyRecord, command Command) (ApplyResult, error) {
	if err := ValidateSafetyRecord(current); err != nil {
		return ApplyResult{}, invalidCommand("current safety record is invalid: %v", err)
	}
	normalized, err := normalizeCommand(command)
	if err != nil {
		return ApplyResult{}, err
	}
	if current.Terminal != nil {
		return applyTerminalAbsorbing(current, normalized)
	}
	if current.Mode == ModeLegacyUnfenced {
		return ApplyResult{}, precondition("legacy unfenced submissions are outside admission authority")
	}

	next := cloneSafetyRecord(current)
	changed, err := applyOpen(&next, current, normalized)
	if err != nil {
		return ApplyResult{}, err
	}
	if !changed {
		return ApplyResult{Record: cloneSafetyRecord(current), Changed: false}, nil
	}
	if current.Revision == ^uint64(0) {
		return ApplyResult{}, precondition("safety record revision overflow")
	}
	next.Revision = current.Revision + 1
	if err := ValidateSafetyRecord(next); err != nil {
		return ApplyResult{}, fmt.Errorf("reducer produced invalid safety record: %w", err)
	}
	return ApplyResult{Record: next, Changed: true}, nil
}

func applyOpen(next *SafetyRecord, current SafetyRecord, command any) (bool, error) {
	switch c := command.(type) {
	case Acknowledge:
		return applyAcknowledge(next, current, c)
	case BeginReject:
		return applyBeginReject(next, current, c)
	case BindSupervisor:
		return applyBindSupervisor(next, current, c)
	case AuthorizeLaunch:
		return applyAuthorizeLaunch(next, current, c)
	case ObserveLaunchConsumed:
		return applyObserveLaunchConsumed(next, current, c)
	case ObserveLaunchQuiescent:
		return applyObserveLaunchQuiescent(next, current, c)
	case RequestCancel:
		return applyRequestCancel(next, current, c)
	case ObserveOutcome:
		return applyObserveOutcome(next, current, c)
	case CertifyRetirement:
		return applyCertifyRetirement(next, current, c)
	case CertifyContainment:
		return applyCertifyContainment(next, current, c)
	case CertifyResult:
		return applyCertifyResult(next, current, c)
	case Finalize:
		return applyFinalize(next, current, c)
	default:
		return false, invalidCommand("unsupported command %T", command)
	}
}

func applyAcknowledge(next *SafetyRecord, current SafetyRecord, command Acknowledge) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	acknowledgedBy, err := resolveBootRef(command.AcknowledgedBy, current.AdmittedBy, "acknowledgement.acknowledged_by")
	if err != nil {
		return false, err
	}
	fact := AcknowledgementFact{Attempt: current.Attempt.Ref, AcknowledgedBy: acknowledgedBy}
	if current.Acknowledgement != nil {
		return mergeFact(&next.Acknowledgement, fact, "acknowledgement")
	}
	if current.Mode != ModeLegacyFenced {
		return false, precondition("acknowledgement is only required for legacy fenced records")
	}
	if current.Cancel != nil || current.Outcome != nil || hasAnyLaunchEvidence(current.Attempt) {
		return false, precondition("acknowledgement requires an unmodified awaiting record")
	}
	return mergeFact(&next.Acknowledgement, fact, "acknowledgement")
}

func applyBeginReject(next *SafetyRecord, current SafetyRecord, command BeginReject) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	requestedBy, err := resolveBootRef(command.RequestedBy, current.AdmittedBy, "cancel.requested_by")
	if err != nil {
		return false, err
	}
	fact := CancelFact{JobID: current.JobID, RequestedBy: requestedBy}
	if current.Cancel != nil {
		return mergeFact(&next.Cancel, fact, "cancel")
	}
	if current.Mode != ModeLegacyFenced || current.Acknowledgement != nil {
		return false, precondition("reject can only begin for an unacknowledged legacy fenced record")
	}
	if current.Outcome != nil || hasAnyLaunchEvidence(current.Attempt) || current.Attempt.Retirement != nil || current.Attempt.Containment != nil {
		return false, precondition("reject requires no launch, outcome, or terminal evidence")
	}
	return mergeFact(&next.Cancel, fact, "cancel")
}

func applyBindSupervisor(next *SafetyRecord, current SafetyRecord, command BindSupervisor) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Supervisor.Validate(); err != nil {
		return false, invalidCommand("supervisor: %v", err)
	}
	if current.Attempt.Supervisor != nil {
		if current.Attempt.Supervisor.Equal(command.Supervisor) {
			return false, nil
		}
		return false, conflict("supervisor identity is already recorded")
	}
	if !acceptedOrAcknowledged(current) {
		return false, precondition("supervisor binding requires accepted or acknowledged state")
	}
	if current.Cancel != nil || current.Outcome != nil || hasAnyLaunchEvidence(current.Attempt) || current.Attempt.Retirement != nil || current.Attempt.Containment != nil {
		return false, precondition("supervisor binding must precede cancellation, launch, and retirement evidence")
	}
	supervisor := command.Supervisor
	next.Attempt.Supervisor = &supervisor
	return true, nil
}

func applyAuthorizeLaunch(next *SafetyRecord, current SafetyRecord, command AuthorizeLaunch) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Ordinal.Validate(); err != nil {
		return false, invalidCommand("launch ordinal: %v", err)
	}
	if err := LaunchNonce(command.Nonce).Validate(); err != nil {
		return false, invalidCommand("permit nonce: %v", err)
	}
	grantedBy, err := resolveBootRef(command.GrantedBy, current.AdmittedBy, "launch_grant.granted_by")
	if err != nil {
		return false, err
	}
	if current.Attempt.Supervisor == nil {
		return false, precondition("launch authorization requires durable supervisor identity")
	}
	grant := LaunchGrant{
		Attempt:    current.Attempt.Ref,
		Supervisor: *current.Attempt.Supervisor,
		Ordinal:    command.Ordinal,
		Nonce:      LaunchNonce(command.Nonce),
		GrantedBy:  grantedBy,
	}
	if existing, ok := current.Attempt.Grants.Get(command.Ordinal); ok {
		return mergeLaunchSlot(&next.Attempt.Grants, command.Ordinal, grant, "launch grant")
	} else if existing != nil {
		return false, conflict("launch grant slot is inconsistent")
	}
	if nonceUsedByOtherGrant(current.Attempt.Grants, command.Ordinal, LaunchNonce(command.Nonce)) {
		return false, conflict("permit nonce is already bound to another launch")
	}
	if current.Cancel != nil || current.Outcome != nil || current.Attempt.Retirement != nil || current.Attempt.Containment != nil {
		return false, precondition("launch authorization requires no cancellation, outcome, retirement, or containment evidence")
	}
	if !acceptedOrAcknowledged(current) {
		return false, precondition("launch authorization requires accepted or acknowledged state")
	}
	if command.Ordinal == LaunchOrdinalOne {
		if current.Attempt.Grants.Count() != 0 || current.Attempt.Consumed.Count() != 0 || current.Attempt.Quiescence.Count() != 0 {
			return false, precondition("launch ordinal 1 must be the first launch authority")
		}
	} else {
		if _, ok := current.Attempt.Grants.Get(LaunchOrdinalOne); !ok {
			return false, precondition("launch ordinal 2 requires ordinal 1 grant history")
		}
		if _, ok := current.Attempt.Quiescence.Get(LaunchOrdinalOne); !ok {
			return false, precondition("launch ordinal 2 requires ordinal 1 quiescence")
		}
		if hasUnconsumedGrant(current.Attempt) || hasActiveLaunch(current.Attempt) {
			return false, precondition("launch ordinal 2 requires no live ordinal")
		}
	}
	return mergeLaunchSlot(&next.Attempt.Grants, command.Ordinal, grant, "launch grant")
}

func applyObserveLaunchConsumed(next *SafetyRecord, current SafetyRecord, command ObserveLaunchConsumed) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Ordinal.Validate(); err != nil {
		return false, invalidCommand("launch ordinal: %v", err)
	}
	if err := command.Child.Validate(); err != nil {
		return false, invalidCommand("child identity: %v", err)
	}
	consumedBy, err := resolveBootRef(command.ConsumedBy, current.AdmittedBy, "launch_consumed.consumed_by")
	if err != nil {
		return false, err
	}
	observation, err := resolveEvidence(command.Observation, "launch_consumed", "launch consumed", "launch_consumed.observation")
	if err != nil {
		return false, err
	}
	grant, ok := current.Attempt.Grants.Get(command.Ordinal)
	if !ok {
		return false, precondition("launch consumption requires matching grant")
	}
	consumed := LaunchConsumed{
		Attempt:     current.Attempt.Ref,
		Ordinal:     command.Ordinal,
		Nonce:       grant.Nonce,
		Child:       command.Child,
		ConsumedBy:  consumedBy,
		Observation: observation,
	}
	if existing, ok := current.Attempt.Consumed.Get(command.Ordinal); ok {
		return mergeLaunchSlot(&next.Attempt.Consumed, command.Ordinal, consumed, "launch consumption")
	} else if existing != nil {
		return false, conflict("launch consumption slot is inconsistent")
	}
	if current.Attempt.Retirement != nil || current.Attempt.Containment != nil {
		return false, precondition("new launch consumption cannot follow retirement or containment")
	}
	return mergeLaunchSlot(&next.Attempt.Consumed, command.Ordinal, consumed, "launch consumption")
}

func applyObserveLaunchQuiescent(next *SafetyRecord, current SafetyRecord, command ObserveLaunchQuiescent) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	receipt := QuiescenceCertificate(command.Receipt)
	if err := receipt.Validate(); err != nil {
		return false, invalidCommand("quiescence receipt: %v", err)
	}
	if err := validateAttemptField("quiescence.attempt", receipt.Attempt, current.Attempt.Ref); err != nil {
		return false, invalidCommand("%v", err)
	}
	if existing, ok := current.Attempt.Quiescence.Get(receipt.Ordinal); ok {
		return mergeLaunchSlot(&next.Attempt.Quiescence, receipt.Ordinal, receipt, "quiescence")
	} else if existing != nil {
		return false, conflict("quiescence slot is inconsistent")
	}
	if _, ok := current.Attempt.Consumed.Get(receipt.Ordinal); !ok {
		return false, precondition("quiescence requires matching launch consumption")
	}
	if current.Attempt.Retirement != nil || current.Attempt.Containment != nil {
		return false, precondition("new quiescence cannot follow retirement or containment")
	}
	return mergeLaunchSlot(&next.Attempt.Quiescence, receipt.Ordinal, receipt, "quiescence")
}

func applyRequestCancel(next *SafetyRecord, current SafetyRecord, command RequestCancel) (bool, error) {
	if err := ensureJob(current, command.JobID); err != nil {
		return false, err
	}
	requestedBy, err := resolveBootRef(command.RequestedBy, current.AdmittedBy, "cancel.requested_by")
	if err != nil {
		return false, err
	}
	fact := CancelFact{JobID: current.JobID, RequestedBy: requestedBy}
	if current.Cancel != nil {
		return mergeFact(&next.Cancel, fact, "cancel")
	}
	if current.Mode == ModeLegacyFenced && current.Acknowledgement == nil {
		return false, precondition("unacknowledged legacy fenced cancellation requires BeginReject")
	}
	return mergeFact(&next.Cancel, fact, "cancel")
}

func applyObserveOutcome(next *SafetyRecord, current SafetyRecord, command ObserveOutcome) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Outcome.ValidateTerminal(); err != nil {
		return false, invalidCommand("outcome: %v", err)
	}
	fact := OutcomeFact{Attempt: current.Attempt.Ref, Outcome: command.Outcome}
	if current.Outcome != nil {
		return mergeFact(&next.Outcome, fact, "outcome")
	}
	if completionOutcome(command.Outcome) && current.Attempt.Consumed.Count() == 0 {
		return false, precondition("completed outcome requires consumed launch evidence")
	}
	if current.Cancel != nil && !hasAnyLaunchEvidence(current.Attempt) && command.Outcome != OutcomeCanceled {
		return false, precondition("cancel before authorization cannot be rewritten by outcome")
	}
	return mergeFact(&next.Outcome, fact, "outcome")
}

func applyCertifyRetirement(next *SafetyRecord, current SafetyRecord, command CertifyRetirement) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	receipt := RetirementCertificate(command.Receipt)
	if err := receipt.Validate(); err != nil {
		return false, invalidCommand("retirement receipt: %v", err)
	}
	if err := validateAttemptField("retirement.attempt", receipt.Attempt, current.Attempt.Ref); err != nil {
		return false, invalidCommand("%v", err)
	}
	if current.Attempt.Supervisor == nil {
		return false, precondition("retirement requires durable supervisor identity")
	}
	if !receipt.Supervisor.Equal(*current.Attempt.Supervisor) {
		return false, conflict("retirement supervisor does not match durable supervisor")
	}
	if current.Attempt.Retirement != nil {
		return mergeFact(&next.Attempt.Retirement, receipt, "retirement")
	}
	if current.Attempt.Containment == nil && (hasUnconsumedGrant(current.Attempt) || hasActiveLaunch(current.Attempt)) {
		return false, precondition("retirement before containment requires no live or uncertain launch")
	}
	return mergeFact(&next.Attempt.Retirement, receipt, "retirement")
}

func applyCertifyContainment(next *SafetyRecord, current SafetyRecord, command CertifyContainment) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	receipt := ContainmentCertificate(command.Receipt)
	if err := receipt.Validate(); err != nil {
		return false, invalidCommand("containment receipt: %v", err)
	}
	if err := validateAttemptField("containment.attempt", receipt.Attempt, current.Attempt.Ref); err != nil {
		return false, invalidCommand("%v", err)
	}
	if current.Attempt.Supervisor == nil {
		return false, precondition("containment requires durable supervisor identity")
	}
	if !receipt.Supervisor.Equal(*current.Attempt.Supervisor) {
		return false, conflict("containment supervisor does not match durable supervisor")
	}
	return mergeFact(&next.Attempt.Containment, receipt, "containment")
}

func applyCertifyResult(next *SafetyRecord, current SafetyRecord, command CertifyResult) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	receipt := ResultCertificate(command.Receipt)
	if err := receipt.Validate(); err != nil {
		return false, invalidCommand("result receipt: %v", err)
	}
	if err := validateJobField("result.job_id", receipt.JobID, current.JobID); err != nil {
		return false, invalidCommand("%v", err)
	}
	if current.Result != nil {
		return mergeFact(&next.Result, receipt, "result")
	}
	if current.Outcome == nil || !completionOutcome(current.Outcome.Outcome) {
		return false, precondition("result certificate requires completed outcome")
	}
	return mergeFact(&next.Result, receipt, "result")
}

func applyFinalize(next *SafetyRecord, current SafetyRecord, command Finalize) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	certificate, err := DeriveTerminalCertificate(current, command.Intent)
	if err != nil {
		return false, err
	}
	return mergeTerminal(next, certificate)
}

func applyTerminalAbsorbing(current SafetyRecord, command any) (ApplyResult, error) {
	finalize, ok := command.(Finalize)
	if !ok {
		return ApplyResult{}, precondition("terminal record is absorbing")
	}
	if err := ensureAttempt(current, finalize.Ref); err != nil {
		return ApplyResult{}, err
	}
	intent, err := resolveTerminalIntent(current, finalize.Intent)
	if err != nil {
		return ApplyResult{}, err
	}
	if current.Terminal.Outcome == intent.Outcome &&
		current.Terminal.Cause == intent.Cause &&
		current.Terminal.DerivedBy == intent.DerivedBy {
		return ApplyResult{Record: cloneSafetyRecord(current), Changed: false}, nil
	}
	return ApplyResult{}, conflict("terminal certificate is already recorded")
}

func normalizeCommand(command Command) (any, error) {
	if command == nil {
		return nil, invalidCommand("command is nil")
	}
	switch c := command.(type) {
	case Acknowledge:
		return c, nil
	case *Acknowledge:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case BeginReject:
		return c, nil
	case *BeginReject:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case BindSupervisor:
		return c, nil
	case *BindSupervisor:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case AuthorizeLaunch:
		return c, nil
	case *AuthorizeLaunch:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case ObserveLaunchConsumed:
		return c, nil
	case *ObserveLaunchConsumed:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case ObserveLaunchQuiescent:
		return c, nil
	case *ObserveLaunchQuiescent:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RequestCancel:
		return c, nil
	case *RequestCancel:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case ObserveOutcome:
		return c, nil
	case *ObserveOutcome:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case CertifyRetirement:
		return c, nil
	case *CertifyRetirement:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case CertifyContainment:
		return c, nil
	case *CertifyContainment:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case CertifyResult:
		return c, nil
	case *CertifyResult:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case Finalize:
		return c, nil
	case *Finalize:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	default:
		return nil, invalidCommand("unsupported command %T", command)
	}
}

func acceptedOrAcknowledged(record SafetyRecord) bool {
	return record.Mode == ModeIdentifiedFenced || (record.Mode == ModeLegacyFenced && record.Acknowledgement != nil)
}

func ensureAttempt(record SafetyRecord, ref AttemptRef) error {
	if err := ref.Validate(); err != nil {
		return invalidCommand("attempt ref: %v", err)
	}
	if !ref.Equal(record.Attempt.Ref) {
		return precondition("attempt precondition failed")
	}
	return nil
}

func ensureJob(record SafetyRecord, jobID JobID) error {
	if err := jobID.Validate(); err != nil {
		return invalidCommand("job id: %v", err)
	}
	if jobID != record.JobID {
		return precondition("job precondition failed")
	}
	return nil
}

func resolveBootRef(value, fallback BootRef, field string) (BootRef, error) {
	if emptyBootRef(value) {
		return fallback, nil
	}
	if err := value.Validate(); err != nil {
		return BootRef{}, invalidCommand("%s: %v", field, err)
	}
	return value, nil
}

func resolveEvidence(value Evidence, kind, detail, field string) (Evidence, error) {
	if !value.Present() {
		value = Evidence{Kind: kind, Detail: detail}
	}
	if err := value.Validate(); err != nil {
		return Evidence{}, invalidCommand("%s: %v", field, err)
	}
	return value, nil
}

func emptyBootRef(value BootRef) bool {
	return value.BootID == "" && value.OwnerID == ""
}

func hasAnyLaunchEvidence(proof AttemptProof) bool {
	return proof.Grants.Count() != 0 || proof.Consumed.Count() != 0 || proof.Quiescence.Count() != 0
}

func nonceUsedByOtherGrant(slots LaunchSlots[LaunchGrant], ordinal LaunchOrdinal, nonce LaunchNonce) bool {
	for _, filled := range slots.FilledOrdinals() {
		if filled == ordinal {
			continue
		}
		grant, ok := slots.Get(filled)
		if ok && grant.Nonce == nonce {
			return true
		}
	}
	return false
}

func mergeFact[T comparable](target **T, fact T, label string) (bool, error) {
	if *target == nil {
		copied := fact
		*target = &copied
		return true, nil
	}
	if **target == fact {
		return false, nil
	}
	return false, conflict("%s already recorded with different evidence", label)
}

func mergeLaunchSlot[T comparable](slots *LaunchSlots[T], ordinal LaunchOrdinal, fact T, label string) (bool, error) {
	existing, ok := slots.Get(ordinal)
	if !ok {
		copied := fact
		switch ordinal {
		case LaunchOrdinalOne:
			slots.First = &copied
		case LaunchOrdinalTwo:
			slots.Second = &copied
		default:
			return false, invalidCommand("launch ordinal is invalid")
		}
		return true, nil
	}
	if *existing == fact {
		return false, nil
	}
	return false, conflict("%s already recorded with different evidence", label)
}

func mergeTerminal(record *SafetyRecord, certificate TerminalCertificate) (bool, error) {
	if record.Terminal == nil {
		copied := certificate
		record.Terminal = &copied
		return true, nil
	}
	if terminalCertificateEqual(*record.Terminal, certificate) {
		return false, nil
	}
	return false, conflict("terminal certificate already recorded with different evidence")
}

func terminalCertificateEqual(left, right TerminalCertificate) bool {
	if left.JobID != right.JobID ||
		!left.Attempt.Equal(right.Attempt) ||
		left.Outcome != right.Outcome ||
		left.Proof != right.Proof ||
		left.Cause != right.Cause ||
		left.DerivedFromRevision != right.DerivedFromRevision ||
		left.DerivedBy != right.DerivedBy {
		return false
	}
	if left.Result == nil || right.Result == nil {
		return left.Result == nil && right.Result == nil
	}
	return *left.Result == *right.Result
}

func cloneSafetyRecord(record SafetyRecord) SafetyRecord {
	next := record
	next.Attempt.Supervisor = clonePtr(record.Attempt.Supervisor)
	next.Attempt.Grants = cloneLaunchSlots(record.Attempt.Grants)
	next.Attempt.Consumed = cloneLaunchSlots(record.Attempt.Consumed)
	next.Attempt.Quiescence = cloneLaunchSlots(record.Attempt.Quiescence)
	next.Attempt.Retirement = clonePtr(record.Attempt.Retirement)
	next.Attempt.Containment = clonePtr(record.Attempt.Containment)
	next.Acknowledgement = clonePtr(record.Acknowledgement)
	next.Cancel = clonePtr(record.Cancel)
	next.Outcome = clonePtr(record.Outcome)
	next.Result = clonePtr(record.Result)
	next.Terminal = clonePtr(record.Terminal)
	return next
}

func cloneLaunchSlots[T any](slots LaunchSlots[T]) LaunchSlots[T] {
	return LaunchSlots[T]{
		First:  clonePtr(slots.First),
		Second: clonePtr(slots.Second),
	}
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func invalidCommand(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCommand, fmt.Sprintf(format, args...))
}

func precondition(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCommandPrecondition, fmt.Sprintf(format, args...))
}

func conflict(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrConflictingDuplicate, fmt.Sprintf(format, args...))
}
