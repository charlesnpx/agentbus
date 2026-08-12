package model

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

var (
	ErrInvalidCommand       = errors.New("invalid command")
	ErrCommandPrecondition  = errors.New("command precondition failed")
	ErrConflictingDuplicate = errors.New("conflicting duplicate")
	ErrTerminalAbsorbed     = errors.New("terminal record is absorbing")
)

type TerminalAbsorbedError struct {
	Terminal TerminalCertificate
	Cause    error
}

func (e TerminalAbsorbedError) Error() string {
	terminal := fmt.Sprintf(
		"job=%s outcome=%s cause=%s proof=%s revision=%d",
		e.Terminal.JobID,
		e.Terminal.Outcome,
		e.Terminal.Cause,
		e.Terminal.Proof,
		e.Terminal.DerivedFromRevision,
	)
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", ErrTerminalAbsorbed, terminal)
	}
	return fmt.Sprintf("%s: %s: %v", ErrTerminalAbsorbed, terminal, e.Cause)
}

func (e TerminalAbsorbedError) Unwrap() error {
	return e.Cause
}

func (e TerminalAbsorbedError) Is(target error) bool {
	return target == ErrTerminalAbsorbed
}

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

func ApplyAcknowledge(current SafetyRecord, command Acknowledge) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyBeginReject(current SafetyRecord, command BeginReject) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyBindGroup(current SafetyRecord, command BindGroup) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyCommitGrant(current SafetyRecord, command CommitGrant) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRecordReleaseOutcome(current SafetyRecord, command RecordReleaseOutcome) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRecordRelease(current SafetyRecord, command RecordRelease) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRequestCancel(current SafetyRecord, command RequestCancel) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyObserveOutcome(current SafetyRecord, command ObserveOutcome) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyCertifyResult(current SafetyRecord, command CertifyResult) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRecordFinalAttemptStart(current SafetyRecord, command RecordFinalAttemptStart) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRecordFailure(current SafetyRecord, command RecordFailure) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyRecordTransportFrameDrops(current SafetyRecord, command RecordTransportFrameDrops) (ApplyResult, error) {
	return apply(current, command)
}

func ApplyFinalize(current SafetyRecord, command Finalize) (ApplyResult, error) {
	return apply(current, command)
}

func apply(current SafetyRecord, command Command) (ApplyResult, error) {
	if err := ValidateSafetyRecord(current); err != nil {
		return ApplyResult{}, invalidCommand("current safety record is invalid: %v", err)
	}
	normalized, err := normalizeCommand(command)
	if err != nil {
		return ApplyResult{}, err
	}
	if current.Terminal != nil {
		if failure, ok := normalized.(RecordFailure); ok {
			next := cloneSafetyRecord(current)
			changed, err := applyRecordFailure(&next, current, failure)
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
		if transport, ok := normalized.(RecordTransportFrameDrops); ok {
			next := cloneSafetyRecord(current)
			changed, err := applyRecordTransportFrameDrops(&next, current, transport)
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
		return applyTerminalAbsorbing(current, normalized)
	}
	if current.Mode == ModeLegacyUnfenced && !legacyUnfencedCommand(normalized) {
		return ApplyResult{}, precondition("legacy unfenced submissions cannot record fenced launch evidence")
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
	case BindGroup:
		return applyBindGroup(next, current, c)
	case CommitGrant:
		return applyCommitGrant(next, current, c)
	case RecordReleaseOutcome:
		return applyRecordReleaseOutcome(next, current, c)
	case RecordRelease:
		return applyRecordRelease(next, current, c)
	case RecordQuiescence:
		return applyRecordQuiescence(next, current, c)
	case RequestCancel:
		return applyRequestCancel(next, current, c)
	case ObserveOutcome:
		return applyObserveOutcome(next, current, c)
	case CertifyResult:
		return applyCertifyResult(next, current, c)
	case RecordFinalAttemptStart:
		return applyRecordFinalAttemptStart(next, current, c)
	case RecordFailure:
		return applyRecordFailure(next, current, c)
	case RecordTransportFrameDrops:
		return applyRecordTransportFrameDrops(next, current, c)
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
	if current.Cancel != nil || current.Outcome != nil || hasAnyPostAckLaunchEvidence(current.Attempt) {
		return false, precondition("acknowledgement requires no cancellation, outcome, grant, release, or quiescence evidence")
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
	if current.Outcome != nil || hasAnyPostAckLaunchEvidence(current.Attempt) {
		return false, precondition("reject requires no grant, release, quiescence, outcome, or terminal evidence")
	}
	return mergeFact(&next.Cancel, fact, "cancel")
}

func applyBindGroup(next *SafetyRecord, current SafetyRecord, command BindGroup) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Ordinal.Validate(); err != nil {
		return false, invalidCommand("launch ordinal: %v", err)
	}
	if err := command.Group.Validate(); err != nil {
		return false, invalidCommand("group: %v", err)
	}
	if !command.Group.Launch.Attempt.Equal(current.Attempt.Ref) {
		return false, invalidCommand("group launch attempt does not match command attempt")
	}
	if command.Group.Launch.Ordinal != command.Ordinal {
		return false, invalidCommand("group launch ordinal does not match command ordinal")
	}
	if existing, ok := current.Attempt.Launches.Get(command.Ordinal); ok {
		if existing.Group != nil && existing.Group.Equal(command.Group) {
			return false, nil
		}
		return false, conflict("group reference is already recorded")
	} else if existing != nil {
		return false, conflict("launch slot is inconsistent")
	}
	if !groupBindingAllowed(current) {
		return false, precondition("group binding requires accepted, acknowledged, or unacknowledged legacy fenced state")
	}
	if current.Cancel != nil || current.Outcome != nil {
		return false, precondition("group binding must precede cancellation and outcome evidence")
	}
	if command.Ordinal == LaunchOrdinalOne {
		if hasAnyLaunchEvidence(current.Attempt) {
			return false, precondition("launch ordinal 1 must be the first group binding")
		}
	} else {
		first, ok := current.Attempt.Launches.Get(LaunchOrdinalOne)
		if !ok || first.Quiescence == nil {
			return false, precondition("launch ordinal 2 requires ordinal 1 quiescence")
		}
		if hasUnconsumedGrant(current.Attempt) || hasActiveLaunch(current.Attempt) {
			return false, precondition("launch ordinal 2 requires no live ordinal")
		}
	}
	if err := ensureDistinctLaunchGroup(current.Attempt, command.Ordinal, command.Group); err != nil {
		return false, err
	}
	group := command.Group
	launch := LaunchProof{Ordinal: command.Ordinal, Group: &group}
	return mergeLaunchSlot(&next.Attempt.Launches, command.Ordinal, launch, "launch")
}

func applyCommitGrant(next *SafetyRecord, current SafetyRecord, command CommitGrant) (bool, error) {
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
	launch, ok := current.Attempt.Launches.Get(command.Ordinal)
	if !ok {
		return false, precondition("grant commit requires durable group reference")
	}
	grant := LaunchGrant{
		Attempt:   current.Attempt.Ref,
		Ordinal:   command.Ordinal,
		Nonce:     LaunchNonce(command.Nonce),
		GrantedBy: grantedBy,
	}
	if launch.Grant != nil {
		if *launch.Grant == grant {
			return false, nil
		}
		return false, conflict("launch grant already recorded with different evidence")
	}
	if nonceUsedByOtherGrant(current.Attempt, command.Ordinal, LaunchNonce(command.Nonce)) {
		return false, conflict("permit nonce is already bound to another launch")
	}
	if current.Cancel != nil || current.Outcome != nil {
		return false, precondition("grant commit requires no cancellation or outcome evidence")
	}
	if !acceptedOrAcknowledged(current) {
		return false, precondition("grant commit requires accepted or acknowledged state")
	}
	nextLaunch := *cloneLaunchProof(launch)
	nextLaunch.Grant = &grant
	return replaceLaunchSlot(&next.Attempt.Launches, command.Ordinal, nextLaunch)
}

func applyRecordReleaseOutcome(next *SafetyRecord, current SafetyRecord, command RecordReleaseOutcome) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Ordinal.Validate(); err != nil {
		return false, invalidCommand("launch ordinal: %v", err)
	}
	if err := command.Outcome.Validate(); err != nil {
		return false, invalidCommand("launch release outcome: %v", err)
	}
	recordedBy, err := resolveBootRef(command.RecordedBy, current.AdmittedBy, "launch_release_outcome.recorded_by")
	if err != nil {
		return false, err
	}
	launch, ok := current.Attempt.Launches.Get(command.Ordinal)
	if !ok || launch.Grant == nil {
		return false, precondition("launch release outcome requires matching grant")
	}
	if command.Outcome == LaunchReleaseAcked && launch.Released == nil {
		return false, precondition("acked launch release outcome requires release evidence")
	}
	if command.Outcome != LaunchReleaseAcked && launch.Released != nil {
		return false, conflict("launch release outcome contradicts release evidence")
	}
	fact := LaunchReleaseOutcomeFact{
		Attempt:    current.Attempt.Ref,
		Ordinal:    command.Ordinal,
		Outcome:    command.Outcome,
		RecordedBy: recordedBy,
	}
	if launch.ReleaseOutcome != nil {
		if *launch.ReleaseOutcome == fact {
			return false, nil
		}
		return false, conflict("launch release outcome already recorded with different evidence")
	}
	nextLaunch := *cloneLaunchProof(launch)
	nextLaunch.ReleaseOutcome = &fact
	return replaceLaunchSlot(&next.Attempt.Launches, command.Ordinal, nextLaunch)
}

func applyRecordRelease(next *SafetyRecord, current SafetyRecord, command RecordRelease) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	if err := command.Ordinal.Validate(); err != nil {
		return false, invalidCommand("launch ordinal: %v", err)
	}
	if err := command.Child.Validate(); err != nil {
		return false, invalidCommand("child identity: %v", err)
	}
	releasedBy, err := resolveBootRef(command.ReleasedBy, current.AdmittedBy, "launch_release.released_by")
	if err != nil {
		return false, err
	}
	observation, err := resolveEvidence(command.Observation, "launch_release", "launch released", "launch_release.observation")
	if err != nil {
		return false, err
	}
	launch, ok := current.Attempt.Launches.Get(command.Ordinal)
	if !ok || launch.Grant == nil {
		return false, precondition("launch release requires matching grant")
	}
	release := LaunchReleaseFact{
		Attempt:     current.Attempt.Ref,
		Ordinal:     command.Ordinal,
		Nonce:       launch.Grant.Nonce,
		Child:       command.Child,
		ReleasedBy:  releasedBy,
		Observation: observation,
	}
	if launch.Released != nil {
		if *launch.Released == release {
			return false, nil
		}
		return false, conflict("launch release already recorded with different evidence")
	}
	if launch.Quiescence != nil {
		return false, precondition("new launch release cannot follow quiescence")
	}
	releaseOutcome := LaunchReleaseOutcomeFact{
		Attempt:    current.Attempt.Ref,
		Ordinal:    command.Ordinal,
		Outcome:    LaunchReleaseAcked,
		RecordedBy: releasedBy,
	}
	if launch.ReleaseOutcome != nil {
		if *launch.ReleaseOutcome != releaseOutcome {
			return false, conflict("launch release outcome already recorded with different evidence")
		}
	} else if err := releaseOutcome.Validate(); err != nil {
		return false, invalidCommand("launch release outcome: %v", err)
	}
	nextLaunch := *cloneLaunchProof(launch)
	nextLaunch.Released = &release
	nextLaunch.ReleaseOutcome = &releaseOutcome
	return replaceLaunchSlot(&next.Attempt.Launches, command.Ordinal, nextLaunch)
}

func applyRecordQuiescence(next *SafetyRecord, current SafetyRecord, command RecordQuiescence) (bool, error) {
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
	launch, ok := current.Attempt.Launches.Get(receipt.Ordinal)
	if !ok || launch.Group == nil {
		return false, precondition("quiescence requires durable group reference")
	}
	if !receipt.Group.Equal(*launch.Group) {
		return false, conflict("quiescence group does not match durable group")
	}
	if launch.Quiescence != nil {
		if *launch.Quiescence == receipt {
			return false, nil
		}
		return false, conflict("quiescence already recorded with different evidence")
	}
	nextLaunch := *cloneLaunchProof(launch)
	nextLaunch.Quiescence = &receipt
	return replaceLaunchSlot(&next.Attempt.Launches, receipt.Ordinal, nextLaunch)
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
	if command.Outcome == OutcomeOrphaned {
		return false, invalidCommand("outcome: orphaned cannot be observed")
	}
	fact := OutcomeFact{Attempt: current.Attempt.Ref, Outcome: command.Outcome, Contract: cloneContractStamp(command.Contract)}
	if current.Outcome != nil {
		return mergeOutcomeFact(&next.Outcome, fact)
	}
	if completionOutcome(command.Outcome) && current.Mode != ModeLegacyUnfenced && !hasAnyRelease(current.Attempt) {
		return false, precondition("completed outcome requires release evidence")
	}
	if current.Cancel != nil && !hasAnyLaunchEvidence(current.Attempt) && command.Outcome != OutcomeCanceled {
		return false, precondition("cancel before authorization cannot be rewritten by outcome")
	}
	return mergeOutcomeFact(&next.Outcome, fact)
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

func applyRecordFinalAttemptStart(next *SafetyRecord, current SafetyRecord, command RecordFinalAttemptStart) (bool, error) {
	if err := ensureJob(current, command.JobID); err != nil {
		return false, err
	}
	startedAt := command.StartedAt
	if startedAt.IsZero() {
		reportFinalAttemptTimingWarning(current.JobID, "dropping zero final attempt start")
		return false, nil
	}
	if current.FinalAttemptEndedAt != nil {
		reportFinalAttemptTimingWarning(current.JobID, "dropping start after a final attempt end is already recorded")
		return false, nil
	}
	if current.FinalAttemptStartedAt != nil {
		if current.FinalAttemptStartedAt.Equal(startedAt) {
			return false, nil
		}
		if startedAt.Before(*current.FinalAttemptStartedAt) {
			reportFinalAttemptTimingWarning(current.JobID, "retry start %s precedes persisted start %s; replacing it because the current retry observation wins", startedAt.Format(time.RFC3339Nano), current.FinalAttemptStartedAt.Format(time.RFC3339Nano))
		}
	}
	// The command being applied identifies the current final attempt, so its
	// timestamp always replaces the prior one. Wall-clock order is diagnostic
	// only and must not become a precondition for a retry.
	next.FinalAttemptStartedAt = clonePtr(&startedAt)
	return true, nil
}

func applyFinalAttemptEnd(next *SafetyRecord, current SafetyRecord, endedAt *time.Time) bool {
	if endedAt == nil {
		return false
	}
	if current.FinalAttemptStartedAt == nil {
		// A job can reach terminal before an attempt starts (for example, an
		// early cancellation). In that case there is no final attempt to time.
		return false
	}
	if endedAt.IsZero() {
		reportFinalAttemptTimingWarning(current.JobID, "dropping zero final attempt end")
		return false
	}
	if endedAt.Before(*current.FinalAttemptStartedAt) {
		reportFinalAttemptTimingWarning(current.JobID, "dropping final attempt end %s because it precedes start %s", endedAt.Format(time.RFC3339Nano), current.FinalAttemptStartedAt.Format(time.RFC3339Nano))
		return false
	}
	if current.FinalAttemptEndedAt != nil {
		if current.FinalAttemptEndedAt.Equal(*endedAt) {
			return false
		}
		reportFinalAttemptTimingWarning(current.JobID, "dropping final attempt end %s because end %s is already recorded", endedAt.Format(time.RFC3339Nano), current.FinalAttemptEndedAt.Format(time.RFC3339Nano))
		return false
	}
	next.FinalAttemptEndedAt = clonePtr(endedAt)
	return true
}

func applyRecordFailure(next *SafetyRecord, current SafetyRecord, command RecordFailure) (bool, error) {
	if err := ensureJob(current, command.JobID); err != nil {
		return false, err
	}
	if err := ValidateFailureMetadata(command.Class, command.Reason); err != nil {
		return false, invalidCommand("failure metadata: %v", err)
	}
	if current.FailureClass != "" || current.FailureReason != "" {
		// The first failure observed is the one most directly connected to the
		// job's terminal path. Keep it authoritative when cleanup later fails.
		return false, nil
	}
	next.FailureClass = command.Class
	next.FailureReason = command.Reason
	return true, nil
}

func applyRecordTransportFrameDrops(next *SafetyRecord, current SafetyRecord, command RecordTransportFrameDrops) (bool, error) {
	if err := ensureJob(current, command.JobID); err != nil {
		return false, err
	}
	if err := validateTransportFrameDrops(&command.Drops); err != nil {
		return false, invalidCommand("transport frame drops: %v", err)
	}
	if current.TransportFrameDrops == nil {
		drops := command.Drops
		next.TransportFrameDrops = &drops
		return true, nil
	}
	merged := *current.TransportFrameDrops
	merged.Merge(command.Drops)
	if merged == *current.TransportFrameDrops {
		return false, nil
	}
	next.TransportFrameDrops = &merged
	return true, nil
}

func applyFinalize(next *SafetyRecord, current SafetyRecord, command Finalize) (bool, error) {
	if err := ensureAttempt(current, command.Ref); err != nil {
		return false, err
	}
	certificate, err := DeriveTerminalCertificate(current, command.Intent)
	if err != nil {
		return false, err
	}
	terminalChanged, err := mergeTerminal(next, certificate)
	if err != nil {
		return false, err
	}
	timingChanged := applyFinalAttemptEnd(next, current, command.Intent.FinalAttemptEndedAt)
	return terminalChanged || timingChanged, nil
}

func reportFinalAttemptTimingWarning(jobID JobID, format string, args ...any) {
	log.Printf("agentbus execution: final attempt timing warning: job=%s: %s", jobID, fmt.Sprintf(format, args...))
}

func applyTerminalAbsorbing(current SafetyRecord, command any) (ApplyResult, error) {
	finalize, ok := command.(Finalize)
	if !ok {
		return ApplyResult{}, terminalAbsorbed(current, precondition("terminal record is absorbing"))
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
	return ApplyResult{}, terminalAbsorbed(current, conflict("terminal certificate is already recorded"))
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
	case BindGroup:
		return c, nil
	case *BindGroup:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case CommitGrant:
		return c, nil
	case *CommitGrant:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordReleaseOutcome:
		return c, nil
	case *RecordReleaseOutcome:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordRelease:
		return c, nil
	case *RecordRelease:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordQuiescence:
		return c, nil
	case *RecordQuiescence:
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
	case CertifyResult:
		return c, nil
	case *CertifyResult:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordFinalAttemptStart:
		return c, nil
	case *RecordFinalAttemptStart:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordFailure:
		return c, nil
	case *RecordFailure:
		if c == nil {
			return nil, invalidCommand("command is nil")
		}
		return *c, nil
	case RecordTransportFrameDrops:
		return c, nil
	case *RecordTransportFrameDrops:
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

func groupBindingAllowed(record SafetyRecord) bool {
	return acceptedOrAcknowledged(record) || record.Mode == ModeLegacyFenced
}

func legacyUnfencedCommand(command any) bool {
	switch command.(type) {
	case RequestCancel, ObserveOutcome, CertifyResult, RecordFinalAttemptStart, RecordFailure, RecordTransportFrameDrops, Finalize:
		return true
	default:
		return false
	}
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
	return proof.Launches.Count() != 0
}

func hasAnyGrant(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Grant != nil {
			return true
		}
	}
	return false
}

func hasAnyRelease(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Released != nil {
			return true
		}
	}
	return false
}

func hasAnyQuiescence(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if ok && launch.Quiescence != nil {
			return true
		}
	}
	return false
}

func hasAnyPostAckLaunchEvidence(proof AttemptProof) bool {
	return hasAnyGrant(proof) || hasAnyRelease(proof) || hasAnyQuiescence(proof)
}

func allLaunchGroupsQuiescent(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if !ok || launch.Group == nil || launch.Quiescence == nil {
			return false
		}
	}
	return true
}

func allBoundLaunchesReleasedWhenGrantedAndQuiescent(proof AttemptProof) bool {
	for _, ordinal := range proof.Launches.FilledOrdinals() {
		launch, ok := proof.Launches.Get(ordinal)
		if !ok || launch.Group == nil || launch.Quiescence == nil {
			return false
		}
		if launch.Grant != nil && launch.Released == nil {
			return false
		}
	}
	return true
}

func nonceUsedByOtherGrant(proof AttemptProof, ordinal LaunchOrdinal, nonce LaunchNonce) bool {
	for _, filled := range proof.Launches.FilledOrdinals() {
		if filled == ordinal {
			continue
		}
		launch, ok := proof.Launches.Get(filled)
		if ok && launch.Grant != nil && launch.Grant.Nonce == nonce {
			return true
		}
	}
	return false
}

func ensureDistinctLaunchGroup(proof AttemptProof, ordinal LaunchOrdinal, group GroupRef) error {
	for _, filled := range proof.Launches.FilledOrdinals() {
		if filled == ordinal {
			continue
		}
		launch, ok := proof.Launches.Get(filled)
		if !ok || launch.Group == nil {
			continue
		}
		if launch.Group.CustodyID == group.CustodyID {
			return conflict("custody id is already bound to another launch")
		}
		if launch.Group.SamePhysicalIdentity(group) {
			return conflict("physical group identity is already bound to another launch")
		}
	}
	return nil
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

func mergeOutcomeFact(target **OutcomeFact, fact OutcomeFact) (bool, error) {
	if *target == nil {
		*target = cloneOutcomeFact(&fact)
		return true, nil
	}
	if outcomeFactEqual(**target, fact) {
		return false, nil
	}
	return false, conflict("outcome already recorded with different evidence")
}

func outcomeFactEqual(left, right OutcomeFact) bool {
	return left.Attempt.Equal(right.Attempt) &&
		left.Outcome == right.Outcome &&
		contractStampEqual(left.Contract, right.Contract)
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

func replaceLaunchSlot(slots *LaunchSlots[LaunchProof], ordinal LaunchOrdinal, launch LaunchProof) (bool, error) {
	switch ordinal {
	case LaunchOrdinalOne:
		slots.First = &launch
	case LaunchOrdinalTwo:
		slots.Second = &launch
	default:
		return false, invalidCommand("launch ordinal is invalid")
	}
	return true, nil
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
		return left.Result == nil && right.Result == nil && contractStampEqual(left.Contract, right.Contract)
	}
	return *left.Result == *right.Result && contractStampEqual(left.Contract, right.Contract)
}

func cloneSafetyRecord(record SafetyRecord) SafetyRecord {
	next := record
	next.Attempt.Launches = cloneLaunchSlots(record.Attempt.Launches)
	next.Acknowledgement = clonePtr(record.Acknowledgement)
	next.Cancel = clonePtr(record.Cancel)
	next.Outcome = cloneOutcomeFact(record.Outcome)
	next.Result = clonePtr(record.Result)
	next.Terminal = cloneTerminalCertificate(record.Terminal)
	next.FinalAttemptStartedAt = clonePtr(record.FinalAttemptStartedAt)
	next.FinalAttemptEndedAt = clonePtr(record.FinalAttemptEndedAt)
	next.TransportFrameDrops = cloneTransportFrameDrops(record.TransportFrameDrops)
	return next
}

func cloneOutcomeFact(fact *OutcomeFact) *OutcomeFact {
	if fact == nil {
		return nil
	}
	copied := *fact
	copied.Contract = cloneContractStamp(fact.Contract)
	return &copied
}

func contractStampEqual(left, right *engine.ContractStamp) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.Status != right.Status ||
		left.Reason != right.Reason ||
		left.ContractName != right.ContractName ||
		left.ContractSHA256 != right.ContractSHA256 ||
		left.Attempts != right.Attempts ||
		left.RetryUsed != right.RetryUsed ||
		!left.ValidatedAt.Equal(right.ValidatedAt) ||
		len(left.Missing) != len(right.Missing) {
		return false
	}
	for i := range left.Missing {
		if left.Missing[i] != right.Missing[i] {
			return false
		}
	}
	return true
}

func cloneTerminalCertificate(certificate *TerminalCertificate) *TerminalCertificate {
	if certificate == nil {
		return nil
	}
	copied := *certificate
	copied.Result = clonePtr(certificate.Result)
	copied.Contract = cloneContractStamp(certificate.Contract)
	return &copied
}

func cloneContractStamp(stamp *engine.ContractStamp) *engine.ContractStamp {
	if stamp == nil {
		return nil
	}
	copied := *stamp
	copied.Missing = append([]string(nil), stamp.Missing...)
	return &copied
}

func cloneLaunchSlots(slots LaunchSlots[LaunchProof]) LaunchSlots[LaunchProof] {
	return LaunchSlots[LaunchProof]{
		First:  cloneLaunchProof(slots.First),
		Second: cloneLaunchProof(slots.Second),
	}
}

func cloneLaunchProof(launch *LaunchProof) *LaunchProof {
	if launch == nil {
		return nil
	}
	copied := *launch
	copied.Group = clonePtr(launch.Group)
	copied.Grant = clonePtr(launch.Grant)
	copied.ReleaseOutcome = clonePtr(launch.ReleaseOutcome)
	copied.Released = clonePtr(launch.Released)
	copied.Quiescence = clonePtr(launch.Quiescence)
	return &copied
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

func terminalAbsorbed(record SafetyRecord, cause error) error {
	if record.Terminal == nil {
		return cause
	}
	return TerminalAbsorbedError{Terminal: *record.Terminal, Cause: cause}
}
