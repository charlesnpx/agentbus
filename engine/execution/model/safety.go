package model

import (
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
)

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
	Ordinal        LaunchOrdinal
	Group          *GroupRef
	Grant          *LaunchGrant
	ReleaseOutcome *LaunchReleaseOutcomeFact
	Released       *LaunchReleaseFact
	Quiescence     *QuiescenceCertificate
}

type AttemptProof struct {
	Ref      AttemptRef
	Launches LaunchSlots[LaunchProof]
}

func (proof AttemptProof) Validate() error {
	if err := proof.Ref.Validate(); err != nil {
		return err
	}
	if err := validateLaunchSlotTopology(proof.Launches); err != nil {
		return err
	}
	if err := forEachLaunchSlot(proof.Launches, func(ordinal LaunchOrdinal, launch LaunchProof) error {
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
	}); err != nil {
		return err
	}
	return validateLaunchGroupsDistinct(proof.Launches)
}

func validateLaunchSlotTopology[T any](slots LaunchSlots[T]) error {
	if slots.Second != nil && slots.First == nil {
		return invalid("launch_slots.topology", "launch slots must be contiguous from ordinal 1")
	}
	return nil
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
	if launch.ReleaseOutcome != nil {
		if err := launch.ReleaseOutcome.Validate(); err != nil {
			return err
		}
		if launch.ReleaseOutcome.Ordinal != launch.Ordinal {
			return invalid("launch_release_outcome.ordinal", "does not match launch ordinal")
		}
		if launch.Grant == nil {
			return invalid("launch_release_outcome.grant", "matching grant is required")
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
	if launch.ReleaseOutcome != nil && launch.Released != nil && launch.ReleaseOutcome.Outcome != LaunchReleaseAcked {
		return invalid("launch_release_outcome.outcome", "released launch must be acked")
	}
	if launch.ReleaseOutcome != nil && launch.ReleaseOutcome.Outcome == LaunchReleaseAcked && launch.Released == nil {
		return invalid("launch_release_outcome.outcome", "acked release requires release evidence")
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
	if launch.ReleaseOutcome != nil {
		if err := validateAttemptField("launch_release_outcome.attempt", launch.ReleaseOutcome.Attempt, ref); err != nil {
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

func validateLaunchGroupsDistinct(slots LaunchSlots[LaunchProof]) error {
	if slots.First == nil || slots.Second == nil || slots.First.Group == nil || slots.Second.Group == nil {
		return nil
	}
	if slots.First.Group.CustodyID == slots.Second.Group.CustodyID {
		return invalid("launch.group.custody_id", "must be unique per launch ordinal")
	}
	if slots.First.Group.SamePhysicalIdentity(*slots.Second.Group) {
		return invalid("launch.group.physical_identity", "must be unique per launch ordinal")
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
	SchemaVersion      uint16
	Revision           uint64
	JobID              JobID
	RequestKey         RequestKey
	WorkspaceLayoutKey WorkspaceKey
	TaskIdentity       TaskIdentity
	Mode               Mode
	AdmittedBy         BootRef
	Attempt            AttemptProof
	Acknowledgement    *AcknowledgementFact
	Cancel             *CancelFact
	Outcome            *OutcomeFact
	Result             *ResultCertificate
	Terminal           *TerminalCertificate
	// FinalAttemptStartedAt is the start of the final contract attempt, not
	// whole-job elapsed time. A retry replaces this value with its own start.
	FinalAttemptStartedAt *time.Time `json:"finalAttemptStartedAt,omitempty"`
	// FinalAttemptEndedAt is when that same final attempt reached terminal.
	FinalAttemptEndedAt *time.Time                  `json:"finalAttemptEndedAt,omitempty"`
	FailureReason       string                      `json:"failureReason,omitempty"`
	FailureClass        engine.FailureClass         `json:"failureClass,omitempty"`
	TransportFrameDrops *engine.TransportFrameDrops `json:"transportFrameDrops,omitempty"`
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
	if err := validateOptionalWorkspaceLayoutKey("workspace_layout_key", record.WorkspaceLayoutKey); err != nil {
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
	if err := ValidateFinalAttemptTiming(record.FinalAttemptStartedAt, record.FinalAttemptEndedAt); err != nil {
		return err
	}
	if record.FinalAttemptEndedAt != nil && record.Terminal == nil {
		return invalid("final_attempt.ended_at", "requires terminal certificate")
	}
	if err := ValidateFailureMetadata(record.FailureClass, record.FailureReason); err != nil {
		return err
	}
	if err := validateTransportFrameDrops(record.TransportFrameDrops); err != nil {
		return err
	}
	return nil
}

func validateTransportFrameDrops(drops *engine.TransportFrameDrops) error {
	if drops == nil {
		return nil
	}
	if drops.Count == 0 {
		return invalid("transport_frame_drops.count", "is required")
	}
	if drops.Bytes == 0 {
		return invalid("transport_frame_drops.bytes", "is required")
	}
	if len(drops.RedactedPrefix) == 0 || len(drops.RedactedPrefix) > 128 {
		return invalid("transport_frame_drops.redacted_prefix", "must be 1 to 128 bytes")
	}
	for _, value := range drops.RedactedPrefix {
		if value < ' ' || value > '~' {
			return invalid("transport_frame_drops.redacted_prefix", "must contain printable ASCII only")
		}
	}
	return nil
}

// ValidateFinalAttemptTiming accepts an absent legacy representation or a
// single final-attempt start/end pair. It deliberately permits a start without
// an end while the attempt is still running.
func ValidateFinalAttemptTiming(startedAt, endedAt *time.Time) error {
	if startedAt == nil && endedAt == nil {
		return nil
	}
	if startedAt == nil {
		return invalid("final_attempt.ended_at", "requires started_at")
	}
	if startedAt.IsZero() {
		return invalid("final_attempt.started_at", "is required")
	}
	if endedAt == nil {
		return nil
	}
	if endedAt.IsZero() {
		return invalid("final_attempt.ended_at", "is required")
	}
	if endedAt.Before(*startedAt) {
		return invalid("final_attempt.ended_at", "precedes started_at")
	}
	return nil
}

// ValidateFailureMetadata accepts the empty legacy representation or a complete
// persisted failure class and sanitized human-readable reason.
func ValidateFailureMetadata(class engine.FailureClass, reason string) error {
	if class == "" && reason == "" {
		return nil
	}
	if !class.Valid() {
		return invalid("failure.class", "is unknown")
	}
	if utf8.RuneCountInString(reason) > engine.FailureReasonMaxRunes {
		return invalid("failure.reason", "is too long")
	}
	for _, r := range reason {
		if !unicode.IsPrint(r) && r != ' ' {
			return invalid("failure.reason", "must not contain control characters")
		}
	}
	return validateText("failure.reason", reason, engine.FailureReasonMaxRunes*utf8.UTFMax)
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
		if err := record.validateTerminalCompatibility(); err != nil {
			return err
		}
	}
	return nil
}

func (record SafetyRecord) validateTerminalProofSupport() error {
	switch record.Terminal.Proof {
	case ProofNeverPermittedAndRetired:
		if !fencedMode(record.Mode) {
			return invalid("terminal.proof", "fenced proof requires fenced mode")
		}
		if validExecutionImpossibleIntent(record, TerminalIntent{Outcome: record.Terminal.Outcome, Cause: record.Terminal.Cause}) {
			return nil
		}
		if hasAnyGrant(record.Attempt) || hasAnyRelease(record.Attempt) {
			return invalid("terminal.proof", "never-permitted proof cannot have grant or release evidence")
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			return invalid("terminal.proof", "never-permitted proof requires quiescence for every bound group")
		}
	case ProofCleanQuiescentOutcomeAndRetired:
		if !fencedMode(record.Mode) {
			return invalid("terminal.proof", "fenced proof requires fenced mode")
		}
		if !hasAnyGrant(record.Attempt) {
			return invalid("terminal.proof", "clean proof requires launch grant evidence")
		}
		if !allBoundLaunchesReleasedWhenGrantedAndQuiescent(record.Attempt) {
			return invalid("terminal.proof", "clean proof requires every bound launch to be quiescent and every granted launch to be released")
		}
	case ProofContained:
		if !fencedMode(record.Mode) {
			return invalid("terminal.proof", "fenced proof requires fenced mode")
		}
		if !hasAnyLaunchEvidence(record.Attempt) {
			return invalid("terminal.proof", "contained proof requires launch evidence")
		}
		if !allLaunchGroupsQuiescent(record.Attempt) {
			return invalid("terminal.proof", "contained proof requires quiescence for every bound group")
		}
	case ProofLegacyUnfencedOutcome:
		if record.Mode != ModeLegacyUnfenced {
			return invalid("terminal.proof", "legacy-unfenced proof requires legacy-unfenced mode")
		}
		if hasAnyLaunchEvidence(record.Attempt) {
			return invalid("terminal.proof", "legacy-unfenced proof cannot have launch evidence")
		}
	case ProofUnresolvedAbsence:
		if !fencedMode(record.Mode) {
			return invalid("terminal.proof", "unresolved proof requires fenced mode")
		}
		if !hasAnyGrant(record.Attempt) && !hasAnyRelease(record.Attempt) {
			return invalid("terminal.proof", "unresolved proof requires authorization evidence")
		}
		if allLaunchGroupsQuiescent(record.Attempt) {
			return invalid("terminal.proof", "unresolved proof requires absence not proven")
		}
	default:
		return invalid("terminal.proof", "is unknown")
	}
	return nil
}

func (record SafetyRecord) validateTerminalCompatibility() error {
	if record.Terminal == nil {
		return nil
	}
	intent := TerminalIntent{
		Outcome: record.Terminal.Outcome,
		Cause:   record.Terminal.Cause,
	}
	if record.Terminal.Outcome == OutcomeOrphaned {
		if record.Outcome != nil {
			return invalid("terminal.outcome", "orphaned requires no observed outcome")
		}
		if record.Result != nil || record.Terminal.Result != nil {
			return invalid("terminal.result", "orphaned requires no result")
		}
		if record.Terminal.Proof != ProofUnresolvedAbsence {
			return invalid("terminal.proof", "orphaned requires unresolved absence proof")
		}
		if !orphanedOutcomeCause(record.Terminal.Cause) {
			return invalid("terminal.cause", "orphaned requires after-authorization or unknown-release cause")
		}
	}
	if !validTerminalCompatibility(record, intent, record.Terminal.Proof) {
		return invalid("terminal.cause_outcome_basis", "is not permitted")
	}
	return nil
}

func fencedMode(mode Mode) bool {
	return mode == ModeIdentifiedFenced || mode == ModeLegacyFenced
}
