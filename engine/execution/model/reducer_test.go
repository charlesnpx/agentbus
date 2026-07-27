package model

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestApplyCommandsValidatePredecessorsAndIdempotence(t *testing.T) {
	tests := []struct {
		name    string
		valid   SafetyRecord
		invalid SafetyRecord
		command Command
	}{
		{
			name:    "acknowledge",
			valid:   reducerLegacyAwaitingRecord(),
			invalid: reducerBaseRecord(),
			command: Acknowledge{Ref: reducerRef()},
		},
		{
			name:    "begin reject",
			valid:   reducerLegacyAwaitingRecord(),
			invalid: reducerLegacyAcknowledgedRecord(),
			command: BeginReject{Ref: reducerRef()},
		},
		{
			name:    "bind group",
			valid:   reducerBaseRecord(),
			invalid: reducerCanceledNoSupervisorRecord(t),
			command: BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Group: reducerGroup(LaunchOrdinalOne)},
		},
		{
			name:    "commit grant",
			valid:   reducerSupervisorRecord(),
			invalid: reducerBaseRecord(),
			command: CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"},
		},
		{
			name:    "record release",
			valid:   reducerGrantRecord(t),
			invalid: reducerSupervisorRecord(),
			command: RecordRelease{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Child: reducerChild(21)},
		},
		{
			name:    "record release outcome",
			valid:   reducerGrantRecord(t),
			invalid: reducerSupervisorRecord(),
			command: RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseNotSent},
		},
		{
			name:    "record quiescence",
			valid:   reducerConsumedRecord(t),
			invalid: reducerBaseRecord(),
			command: reducerQuiescenceCommand(t, reducerConsumedRecord(t), LaunchOrdinalOne),
		},
		{
			name:    "request cancel",
			valid:   reducerSupervisorRecord(),
			invalid: reducerLegacyAwaitingRecord(),
			command: RequestCancel{JobID: reducerJobID()},
		},
		{
			name:    "observe outcome",
			valid:   reducerConsumedRecord(t),
			invalid: reducerSupervisorRecord(),
			command: ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted},
		},
		{
			name:    "certify result",
			valid:   reducerCompletedOutcomeRecord(t),
			invalid: reducerConsumedRecord(t),
			command: reducerResultCommand(t, reducerCompletedOutcomeRecord(t)),
		},
		{
			name:    "finalize",
			valid:   reducerCanceledRetiredRecord(t),
			invalid: reducerCanceledRecord(t),
			command: Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := cloneSafetyRecord(tt.valid)
			result, err := apply(tt.valid, tt.command)
			if err != nil {
				t.Fatalf("Apply valid predecessor error = %v", err)
			}
			if !reflect.DeepEqual(tt.valid, before) {
				t.Fatal("Apply mutated its input record")
			}
			if !result.Changed {
				t.Fatal("valid command did not change record")
			}
			if result.Record.Revision != tt.valid.Revision+1 {
				t.Fatalf("revision = %d, want %d", result.Record.Revision, tt.valid.Revision+1)
			}
			if err := ValidateSafetyRecord(result.Record); err != nil {
				t.Fatalf("successful output failed ValidateSafetyRecord: %v", err)
			}

			repeated, err := apply(result.Record, tt.command)
			if err != nil {
				t.Fatalf("Apply duplicate error = %v", err)
			}
			if repeated.Changed {
				t.Fatal("same command/payload was not idempotent")
			}
			if repeated.Record.Revision != result.Record.Revision {
				t.Fatalf("duplicate advanced revision to %d from %d", repeated.Record.Revision, result.Record.Revision)
			}
			if err := ValidateSafetyRecord(repeated.Record); err != nil {
				t.Fatalf("duplicate output failed ValidateSafetyRecord: %v", err)
			}

			invalidBefore := cloneSafetyRecord(tt.invalid)
			if _, err := apply(tt.invalid, tt.command); err == nil {
				t.Fatal("Apply invalid predecessor succeeded")
			}
			if !reflect.DeepEqual(tt.invalid, invalidBefore) {
				t.Fatal("Apply mutated invalid predecessor input")
			}
		})
	}
}

func TestApplyRejectsConflictingDuplicates(t *testing.T) {
	tests := []struct {
		name     string
		record   SafetyRecord
		first    Command
		conflict Command
	}{
		{
			name:     "group",
			record:   reducerBaseRecord(),
			first:    BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Group: reducerGroup(LaunchOrdinalOne)},
			conflict: BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Group: reducerConflictingGroup(LaunchOrdinalOne)},
		},
		{
			name:     "grant nonce",
			record:   reducerSupervisorRecord(),
			first:    CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"},
			conflict: CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-other"},
		},
		{
			name:     "release outcome",
			record:   reducerGrantRecord(t),
			first:    RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseNotSent},
			conflict: RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseSentUnknown},
		},
		{
			name:     "outcome",
			record:   reducerConsumedRecord(t),
			first:    ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted},
			conflict: ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeFailed},
		},
		{
			name:     "terminal",
			record:   reducerCanceledRetiredRecord(t),
			first:    Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization}},
			conflict: Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeFailed, Cause: CauseDaemonRestartedBeforeAuthorization}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first, err := apply(tt.record, tt.first)
			if err != nil {
				t.Fatalf("first Apply error = %v", err)
			}
			if _, err := apply(first.Record, tt.conflict); !errors.Is(err, ErrConflictingDuplicate) {
				t.Fatalf("conflicting duplicate error = %v, want ErrConflictingDuplicate", err)
			}
		})
	}
}

func TestAuthorizeSecondOrdinalRequiresFirstQuiescence(t *testing.T) {
	first := reducerGrantRecord(t)
	if _, err := apply(first, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: reducerGroup(LaunchOrdinalTwo)}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("ordinal 2 bind before quiescence error = %v, want ErrCommandPrecondition", err)
	}
	quiescent := reducerQuiescentRecord(t)
	bound, err := apply(quiescent, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: reducerGroup(LaunchOrdinalTwo)})
	if err != nil {
		t.Fatalf("ordinal 2 bind after quiescence error = %v", err)
	}
	result, err := apply(bound.Record, CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Nonce: "nonce-2"})
	if err != nil {
		t.Fatalf("ordinal 2 grant after bind error = %v", err)
	}
	launch, ok := result.Record.Attempt.Launches.Get(LaunchOrdinalTwo)
	if !ok || launch.Grant == nil {
		t.Fatal("ordinal 2 grant was not recorded")
	}
}

func TestAuthorizeSecondOrdinalRejectsSharedCustodyOrPhysicalIdentity(t *testing.T) {
	record := reducerQuiescentRecord(t)
	first, ok := record.Attempt.Launches.Get(LaunchOrdinalOne)
	if !ok || first.Group == nil {
		t.Fatal("ordinal 1 group missing")
	}

	sharedCustody := reducerGroup(LaunchOrdinalTwo)
	sharedCustody.CustodyID = first.Group.CustodyID
	if _, err := apply(record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: sharedCustody}); !errors.Is(err, ErrConflictingDuplicate) {
		t.Fatalf("shared custody bind error = %v, want ErrConflictingDuplicate", err)
	}

	sharedPhysical := reducerGroup(LaunchOrdinalTwo)
	sharedPhysical.HostBootID = first.Group.HostBootID
	sharedPhysical.PGID = first.Group.PGID
	sharedPhysical.Leader = first.Group.Leader
	sharedPhysical.Monitor = first.Group.Monitor
	sharedPhysical.RetainedID = first.Group.RetainedID
	if _, err := apply(record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: sharedPhysical}); !errors.Is(err, ErrConflictingDuplicate) {
		t.Fatalf("shared physical identity bind error = %v, want ErrConflictingDuplicate", err)
	}

	for _, tt := range []struct {
		name   string
		mutate func(*GroupRef)
	}{
		{
			name: "different monitor",
			mutate: func(group *GroupRef) {
				group.Monitor = ProcessIdentity{PID: first.Group.Monitor.PID + 100, HighResStartToken: "different-monitor"}
				group.RetainedID = first.Group.RetainedID
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sharedTarget := reducerGroup(LaunchOrdinalTwo)
			sharedTarget.HostBootID = first.Group.HostBootID
			sharedTarget.PGID = first.Group.PGID
			sharedTarget.Leader = first.Group.Leader
			tt.mutate(&sharedTarget)
			if _, err := apply(record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: sharedTarget}); !errors.Is(err, ErrConflictingDuplicate) {
				t.Fatalf("shared target identity bind error = %v, want ErrConflictingDuplicate", err)
			}
		})
	}

	differentRetained := reducerGroup(LaunchOrdinalTwo)
	differentRetained.HostBootID = first.Group.HostBootID
	differentRetained.PGID = first.Group.PGID
	differentRetained.Leader = first.Group.Leader
	differentRetained.Monitor = first.Group.Monitor
	differentRetained.RetainedID = "different-retained"
	if _, err := apply(record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: differentRetained}); err != nil {
		t.Fatalf("different retained id bind error = %v, want nil", err)
	}

	forged := cloneSafetyRecord(record)
	forged.Attempt.Launches.Second = &LaunchProof{Ordinal: LaunchOrdinalTwo, Group: &sharedPhysical}
	if err := ValidateSafetyRecord(forged); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("forged shared physical safety error = %v, want ErrInvalidValue", err)
	}
}

func TestStaleActionReceiptCannotCertifyDifferentAttemptOrGroup(t *testing.T) {
	record := reducerGrantRecord(t)
	before := cloneSafetyRecord(record)

	staleAttempt := reducerContainmentCommand(t, record)
	staleAttempt.Receipt.Attempt.AttemptID = "attempt-stale"
	if _, err := apply(record, staleAttempt); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("stale attempt receipt error = %v, want ErrInvalidCommand", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("stale attempt receipt mutated input record")
	}

	wrongGroup := reducerContainmentCommand(t, record)
	wrongGroup.Receipt.Group.PGID++
	wrongGroup.Receipt.Group.Leader.PID++
	wrongGroup.Receipt.Group.Leader.HighResStartToken = "different-group"
	if _, err := apply(record, wrongGroup); !errors.Is(err, ErrConflictingDuplicate) {
		t.Fatalf("wrong group receipt error = %v, want ErrConflictingDuplicate", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("wrong group receipt mutated input record")
	}
}

func TestRecordReleaseStampsAckedReleaseOutcome(t *testing.T) {
	record := reducerConsumedRecord(t)
	launch, ok := record.Attempt.Launches.Get(LaunchOrdinalOne)
	if !ok || launch.ReleaseOutcome == nil {
		t.Fatal("release outcome was not recorded")
	}
	if launch.ReleaseOutcome.Outcome != LaunchReleaseAcked {
		t.Fatalf("release outcome = %s, want %s", launch.ReleaseOutcome.Outcome, LaunchReleaseAcked)
	}
	if launch.ReleaseOutcome.RecordedBy != record.AdmittedBy {
		t.Fatalf("release outcome recorded_by = %+v, want admitted_by %+v", launch.ReleaseOutcome.RecordedBy, record.AdmittedBy)
	}
}

func TestApplyDeepClonesLaunchProofPointers(t *testing.T) {
	record := reducerConsumedRecord(t)
	inputLaunch, ok := record.Attempt.Launches.Get(LaunchOrdinalOne)
	if !ok || inputLaunch.Group == nil || inputLaunch.Grant == nil || inputLaunch.ReleaseOutcome == nil || inputLaunch.Released == nil {
		t.Fatal("input launch proof is incomplete")
	}
	beforeGroup := *inputLaunch.Group
	beforeGrant := *inputLaunch.Grant
	beforeReleaseOutcome := *inputLaunch.ReleaseOutcome
	beforeRelease := *inputLaunch.Released

	result, err := apply(record, reducerQuiescenceCommand(t, record, LaunchOrdinalOne))
	if err != nil {
		t.Fatalf("record quiescence error = %v", err)
	}
	outputLaunch, ok := result.Record.Attempt.Launches.Get(LaunchOrdinalOne)
	if !ok || outputLaunch.Group == nil || outputLaunch.Grant == nil || outputLaunch.ReleaseOutcome == nil || outputLaunch.Released == nil || outputLaunch.Quiescence == nil {
		t.Fatal("output launch proof is incomplete")
	}
	outputLaunch.Group.PGID++
	outputLaunch.Group.Leader.PID++
	outputLaunch.Group.Leader.HighResStartToken = "forged-leader"
	outputLaunch.Grant.Nonce = "forged-nonce"
	outputLaunch.ReleaseOutcome.Outcome = LaunchReleaseSentUnknown
	outputLaunch.Released.Child.PID++
	outputLaunch.Released.Child.HighResStartToken = "forged-child"
	outputLaunch.Quiescence.Group = *outputLaunch.Group

	if !inputLaunch.Group.Equal(beforeGroup) {
		t.Fatalf("input group mutated through output alias: %#v, want %#v", *inputLaunch.Group, beforeGroup)
	}
	if *inputLaunch.Grant != beforeGrant {
		t.Fatalf("input grant mutated through output alias: %#v, want %#v", *inputLaunch.Grant, beforeGrant)
	}
	if *inputLaunch.ReleaseOutcome != beforeReleaseOutcome {
		t.Fatalf("input release outcome mutated through output alias: %#v, want %#v", *inputLaunch.ReleaseOutcome, beforeReleaseOutcome)
	}
	if *inputLaunch.Released != beforeRelease {
		t.Fatalf("input release mutated through output alias: %#v, want %#v", *inputLaunch.Released, beforeRelease)
	}
}

func TestCleanTerminalProofRequiresEveryCommittedGrantOrdinalCovered(t *testing.T) {
	uncovered := reducerCleanCompletedRecord(t)
	secondGroup := reducerGroup(LaunchOrdinalTwo)
	secondGrant := LaunchGrant{
		Attempt:   uncovered.Attempt.Ref,
		Ordinal:   LaunchOrdinalTwo,
		Nonce:     "nonce-2",
		GrantedBy: uncovered.AdmittedBy,
	}
	uncovered.Attempt.Launches.Second = &LaunchProof{Ordinal: LaunchOrdinalTwo, Group: &secondGroup, Grant: &secondGrant}
	uncovered.Terminal = &TerminalCertificate{
		JobID:               uncovered.JobID,
		Attempt:             uncovered.Attempt.Ref,
		Outcome:             OutcomeCompleted,
		Proof:               ProofCleanQuiescentOutcomeAndRetired,
		Cause:               CauseCompletedNormally,
		DerivedFromRevision: uncovered.Revision,
		DerivedBy:           uncovered.AdmittedBy,
		Result:              &uncovered.Result.Result,
	}
	if err := ValidateSafetyRecord(uncovered); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("clean terminal with uncovered second grant error = %v, want ErrInvalidValue", err)
	}

	record := reducerQuiescentRecord(t)
	record = reducerMustApply(t, record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: reducerGroup(LaunchOrdinalTwo)})
	record = reducerMustApply(t, record, CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Nonce: "nonce-2"})
	record = reducerMustApply(t, record, RecordRelease{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Child: reducerChild(22)})
	record = reducerMustApply(t, record, reducerQuiescenceCommand(t, record, LaunchOrdinalTwo))
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted})
	record = reducerMustApply(t, record, reducerResultCommand(t, record))
	finalized, err := apply(record, Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally}})
	if err != nil {
		t.Fatalf("clean finalize with both grant ordinals covered error = %v", err)
	}
	if finalized.Record.Terminal == nil || finalized.Record.Terminal.Proof != ProofCleanQuiescentOutcomeAndRetired {
		t.Fatalf("terminal = %#v, want clean quiescent proof", finalized.Record.Terminal)
	}
}

func TestCleanTerminalProofRequiresBoundUngrantedOrdinalQuiescence(t *testing.T) {
	record := reducerQuiescentRecord(t)
	record = reducerMustApply(t, record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: reducerGroup(LaunchOrdinalTwo)})
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted})
	record = reducerMustApply(t, record, reducerResultCommand(t, record))

	unresolved, err := apply(record, Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally}})
	if err != nil {
		t.Fatalf("finalize with unretired bound ordinal error = %v", err)
	}
	if unresolved.Record.Terminal == nil || unresolved.Record.Terminal.Proof != ProofUnresolvedAbsence {
		t.Fatalf("terminal = %#v, want unresolved proof for unretired bound ordinal", unresolved.Record.Terminal)
	}

	record = reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalTwo, QuiescenceAlreadyAbsent))
	finalized, err := apply(record, Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally}})
	if err != nil {
		t.Fatalf("clean finalize after retiring bound ordinal error = %v", err)
	}
	if finalized.Record.Terminal == nil || finalized.Record.Terminal.Proof != ProofCleanQuiescentOutcomeAndRetired {
		t.Fatalf("terminal = %#v, want clean quiescent proof", finalized.Record.Terminal)
	}
}

func TestDeriveTerminalCertificateSelectsProofs(t *testing.T) {
	tests := []struct {
		name   string
		record SafetyRecord
		intent TerminalIntent
		want   TerminalProof
	}{
		{
			name:   "never permitted",
			record: reducerCanceledRetiredRecord(t),
			intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization},
			want:   ProofNeverPermittedAndRetired,
		},
		{
			name:   "contained",
			record: reducerContainedRetiredRecord(t),
			intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledAfterAuthorization},
			want:   ProofContained,
		},
		{
			name:   "clean quiescent",
			record: reducerCleanCompletedRecord(t),
			intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
			want:   ProofCleanQuiescentOutcomeAndRetired,
		},
		{
			name:   "legacy unfenced",
			record: reducerLegacyUnfencedCompletedRecord(t),
			intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
			want:   ProofLegacyUnfencedOutcome,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			certificate, err := DeriveTerminalCertificate(tt.record, tt.intent)
			if err != nil {
				t.Fatalf("DeriveTerminalCertificate error = %v", err)
			}
			if certificate.Proof != tt.want {
				t.Fatalf("proof = %s, want %s", certificate.Proof, tt.want)
			}
			if certificate.DerivedFromRevision != tt.record.Revision {
				t.Fatalf("derived revision = %d, want %d", certificate.DerivedFromRevision, tt.record.Revision)
			}
		})
	}
}

func TestDeriveTerminalCertificateDecouplesOutcomeFromUnresolvedAbsence(t *testing.T) {
	record := reducerGrantRecord(t)
	certificate, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeOrphaned,
		Cause:   CauseDaemonRestartedAfterAuthorization,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate unresolved daemon-loss error = %v", err)
	}
	if certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("proof = %s, want %s", certificate.Proof, ProofUnresolvedAbsence)
	}

	if _, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeOrphaned,
		Cause:   CauseSupervisorLostAfterAuthorization,
	}); err != nil {
		t.Fatalf("DeriveTerminalCertificate unresolved supervisor-loss error = %v", err)
	}

	releaseUnknown := reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseSentUnknown})
	certificate, err = DeriveTerminalCertificate(releaseUnknown, TerminalIntent{
		Outcome: OutcomeOrphaned,
		Cause:   CauseReleaseOutcomeUnknown,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate unresolved release-unknown error = %v", err)
	}
	if certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("release-unknown proof = %s, want %s", certificate.Proof, ProofUnresolvedAbsence)
	}

	certificate, err = DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeCanceled,
		Cause:   CauseCanceledAfterAuthorization,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate canceled unresolved error = %v", err)
	}
	if certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("canceled proof = %s, want %s", certificate.Proof, ProofUnresolvedAbsence)
	}

	if _, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeReaped,
		Cause:   CauseDaemonRestartedAfterAuthorization,
	}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("reaped without quiescence error = %v, want ErrCommandPrecondition", err)
	}

	recorded := reducerMustApply(t, reducerConsumedRecord(t), ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeFailed})
	certificate, err = DeriveTerminalCertificate(recorded, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseCompletedNormally,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate normal failed unresolved error = %v", err)
	}
	if certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("normal failed proof = %s, want %s", certificate.Proof, ProofUnresolvedAbsence)
	}

	certificate, err = DeriveTerminalCertificate(recorded, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseResponseUndeliverable,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate response-undeliverable unresolved error = %v", err)
	}
	if certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("response-undeliverable proof = %s, want %s", certificate.Proof, ProofUnresolvedAbsence)
	}
}

func TestReleaseTerminalCausesRequireDurableLaunchFact(t *testing.T) {
	ackedQuiescent := reducerQuiescentRecord(t)
	if _, err := DeriveTerminalCertificate(ackedQuiescent, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseReleaseDefinitelyNotSent,
	}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("release-not-sent without not-sent fact error = %v, want ErrCommandPrecondition", err)
	}

	forgedNotSent := ackedQuiescent
	forgedNotSent.Terminal = &TerminalCertificate{
		JobID:               forgedNotSent.JobID,
		Attempt:             forgedNotSent.Attempt.Ref,
		Outcome:             OutcomeFailed,
		Proof:               ProofContained,
		Cause:               CauseReleaseDefinitelyNotSent,
		DerivedFromRevision: forgedNotSent.Revision,
		DerivedBy:           forgedNotSent.AdmittedBy,
	}
	if err := ValidateSafetyRecord(forgedNotSent); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("forged release-not-sent terminal error = %v, want ErrInvalidValue", err)
	}

	unproven := reducerGrantRecord(t)
	if _, err := DeriveTerminalCertificate(unproven, TerminalIntent{
		Outcome: OutcomeOrphaned,
		Cause:   CauseReleaseOutcomeUnknown,
	}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("release-unknown orphaned without sent-unknown fact error = %v, want ErrCommandPrecondition", err)
	}

	forgedUnknown := unproven
	forgedUnknown.Terminal = &TerminalCertificate{
		JobID:               forgedUnknown.JobID,
		Attempt:             forgedUnknown.Attempt.Ref,
		Outcome:             OutcomeOrphaned,
		Proof:               ProofUnresolvedAbsence,
		Cause:               CauseReleaseOutcomeUnknown,
		DerivedFromRevision: forgedUnknown.Revision,
		DerivedBy:           forgedUnknown.AdmittedBy,
	}
	if err := ValidateSafetyRecord(forgedUnknown); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("forged release-unknown terminal error = %v, want ErrInvalidValue", err)
	}
}

func TestReleaseDefinitelyNotSentRequiresAuthoritativeAttemptWideEvidence(t *testing.T) {
	genuineNotSent := reducerGrantRecord(t)
	genuineNotSent = reducerMustApply(t, genuineNotSent, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseNotSent})
	genuineNotSent = reducerMustApply(t, genuineNotSent, reducerContainmentCommand(t, genuineNotSent))
	certificate, err := DeriveTerminalCertificate(genuineNotSent, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseReleaseDefinitelyNotSent,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate genuine release-not-sent error = %v", err)
	}
	genuineNotSent.Terminal = &certificate
	if err := ValidateSafetyRecord(genuineNotSent); err != nil {
		t.Fatalf("ValidateSafetyRecord genuine release-not-sent error = %v", err)
	}
	if got := DeriveCleanupDisposition(genuineNotSent); got != CleanupDispositionNoExecutionPossible {
		t.Fatalf("genuine release-not-sent cleanup = %s, want %s", got, CleanupDispositionNoExecutionPossible)
	}

	record := reducerOrdinalOneReleasedOrdinalTwoReleaseOutcomeRecord(t, LaunchReleaseNotSent)
	if _, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseReleaseDefinitelyNotSent,
	}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("multi-ordinal release-not-sent terminal error = %v, want ErrCommandPrecondition", err)
	}

	forged := record
	forged.Terminal = &TerminalCertificate{
		JobID:               forged.JobID,
		Attempt:             forged.Attempt.Ref,
		Outcome:             OutcomeFailed,
		Proof:               ProofContained,
		Cause:               CauseReleaseDefinitelyNotSent,
		DerivedFromRevision: forged.Revision,
		DerivedBy:           forged.AdmittedBy,
	}
	if err := ValidateSafetyRecord(forged); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("forged multi-ordinal release-not-sent terminal error = %v, want ErrInvalidValue", err)
	}
	if got := DeriveCleanupDisposition(forged); got == CleanupDispositionNoExecutionPossible {
		t.Fatalf("forged multi-ordinal cleanup = %s, want not %s", got, CleanupDispositionNoExecutionPossible)
	}
}

func TestReleaseDefinitelyNotSentIgnoresBoundOnlyTrailingLaunch(t *testing.T) {
	record := reducerGrantRecord(t)
	record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseNotSent})
	record = reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalOne, QuiescenceAlreadyAbsent))

	secondGroup := reducerGroup(LaunchOrdinalTwo)
	record.Attempt.Launches.Second = &LaunchProof{Ordinal: LaunchOrdinalTwo, Group: &secondGroup}
	record.Revision++
	record = reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalTwo, QuiescenceAlreadyAbsent))

	second, ok := record.Attempt.Launches.Get(LaunchOrdinalTwo)
	if !ok || second.Grant != nil {
		t.Fatalf("ordinal 2 launch = %#v, want bound-only without grant", second)
	}

	intent, err := RecoveryTerminalIntent(record, RecoveryStartupLoss, true)
	if err != nil {
		t.Fatalf("RecoveryTerminalIntent error = %v", err)
	}
	if intent.Outcome != OutcomeFailed || intent.Cause != CauseReleaseDefinitelyNotSent {
		t.Fatalf("recovery intent = %+v, want failed/release-definitely-not-sent", intent)
	}
	if intent.Outcome == OutcomeReaped || intent.Outcome == OutcomeOrphaned ||
		intent.Cause == CauseDaemonRestartedAfterAuthorization ||
		intent.Cause == CauseSupervisorLostAfterAuthorization {
		t.Fatalf("recovery intent = %+v, want no restart-after-authorization reaped/orphaned classification", intent)
	}

	finalized, err := apply(record, Finalize{Ref: reducerRef(), Intent: intent})
	if err != nil {
		t.Fatalf("Finalize recovery intent error = %v", err)
	}
	if finalized.Record.Terminal == nil ||
		finalized.Record.Terminal.Outcome != OutcomeFailed ||
		finalized.Record.Terminal.Cause != CauseReleaseDefinitelyNotSent ||
		finalized.Record.Terminal.Proof != ProofContained {
		t.Fatalf("terminal = %#v, want failed/release-definitely-not-sent/contained", finalized.Record.Terminal)
	}
	if err := ValidateSafetyRecord(finalized.Record); err != nil {
		t.Fatalf("ValidateSafetyRecord finalized bound-only trailing launch error = %v", err)
	}
	if got := DeriveCleanupDisposition(finalized.Record); got != CleanupDispositionNoExecutionPossible {
		t.Fatalf("cleanup = %s, want %s", got, CleanupDispositionNoExecutionPossible)
	}
}

func TestReleaseOutcomeUnknownRequiresAuthoritativeLaunchWithoutRecordedOutcome(t *testing.T) {
	record := reducerOrdinalOneReleasedOrdinalTwoReleaseOutcomeRecord(t, LaunchReleaseSentUnknown)
	certificate, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeReaped,
		Cause:   CauseReleaseOutcomeUnknown,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate release-unknown without recorded outcome error = %v", err)
	}
	if certificate.Outcome != OutcomeReaped || certificate.Cause != CauseReleaseOutcomeUnknown || certificate.Proof != ProofContained {
		t.Fatalf("terminal = %#v, want reaped/release-unknown/contained", certificate)
	}

	record.Outcome = &OutcomeFact{Attempt: record.Attempt.Ref, Outcome: OutcomeFailed}
	if terminalCauseBackedByDurableFact(record, CauseReleaseOutcomeUnknown) {
		t.Fatal("release-unknown cause backed by durable fact with recorded execution outcome, want rejected")
	}
	if _, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeFailed,
		Cause:   CauseReleaseOutcomeUnknown,
	}); err == nil {
		t.Fatal("release-unknown constructor with recorded outcome succeeded, want error")
	}

	forged := record
	forged.Terminal = &TerminalCertificate{
		JobID:               forged.JobID,
		Attempt:             forged.Attempt.Ref,
		Outcome:             OutcomeFailed,
		Proof:               ProofContained,
		Cause:               CauseReleaseOutcomeUnknown,
		DerivedFromRevision: forged.Revision,
		DerivedBy:           forged.AdmittedBy,
	}
	if err := ValidateSafetyRecord(forged); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("forged release-unknown with recorded outcome error = %v, want ErrInvalidValue", err)
	}
}

func TestRecoveryTerminalIntentSelectsUnknownOutcomeFromAbsenceProof(t *testing.T) {
	releaseUnknown := reducerGrantRecord(t)
	releaseUnknown = reducerMustApply(t, releaseUnknown, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseSentUnknown})

	tests := []struct {
		name   string
		record SafetyRecord
		cause  TerminalCause
	}{
		{name: "after authorization", record: reducerGrantRecord(t), cause: CauseDaemonRestartedAfterAuthorization},
		{name: "release outcome unknown", record: releaseUnknown, cause: CauseReleaseOutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unprovenIntent, err := RecoveryTerminalIntent(tt.record, RecoveryStartupLoss, false)
			if err != nil {
				t.Fatalf("RecoveryTerminalIntent unproven error = %v", err)
			}
			if unprovenIntent.Outcome != OutcomeOrphaned || unprovenIntent.Cause != tt.cause {
				t.Fatalf("unproven intent = %+v, want orphaned/%s", unprovenIntent, tt.cause)
			}
			unproven, err := apply(tt.record, Finalize{Ref: reducerRef(), Intent: unprovenIntent})
			if err != nil {
				t.Fatalf("Finalize unproven recovery intent error = %v", err)
			}
			if unproven.Record.Terminal == nil || unproven.Record.Terminal.Proof != ProofUnresolvedAbsence {
				t.Fatalf("unproven terminal = %#v, want unresolved absence proof", unproven.Record.Terminal)
			}
			if got := DeriveCleanupDisposition(unproven.Record); got != CleanupDispositionUnresolved {
				t.Fatalf("unproven cleanup = %s, want %s", got, CleanupDispositionUnresolved)
			}

			provenRecord := reducerMustApply(t, tt.record, reducerQuiescenceCommand(t, tt.record, LaunchOrdinalOne))
			provenIntent, err := RecoveryTerminalIntent(provenRecord, RecoveryStartupLoss, true)
			if err != nil {
				t.Fatalf("RecoveryTerminalIntent proven error = %v", err)
			}
			if provenIntent.Outcome != OutcomeReaped || provenIntent.Cause != tt.cause {
				t.Fatalf("proven intent = %+v, want reaped/%s", provenIntent, tt.cause)
			}
			proven, err := apply(provenRecord, Finalize{Ref: reducerRef(), Intent: provenIntent})
			if err != nil {
				t.Fatalf("Finalize proven recovery intent error = %v", err)
			}
			if proven.Record.Terminal == nil || proven.Record.Terminal.Proof != ProofContained {
				t.Fatalf("proven terminal = %#v, want contained proof", proven.Record.Terminal)
			}
			if got := DeriveCleanupDisposition(proven.Record); got != CleanupDispositionVerifiedAbsent {
				t.Fatalf("proven cleanup = %s, want %s", got, CleanupDispositionVerifiedAbsent)
			}
		})
	}
}

func TestRecordedOutcomeCauseCompatibilityMatchesADR(t *testing.T) {
	for _, outcome := range []Outcome{OutcomeCompleted, OutcomeCompletedNoncompliant, OutcomeFailed, OutcomeTimedOut, OutcomeInterrupted} {
		if !recordedOutcomeCausePermitsIntent(CauseCompletedNormally, outcome) {
			t.Fatalf("%s + %s rejected, want ADR completion-class recorded outcome allowed", CauseCompletedNormally, outcome)
		}
		if !recordedOutcomeCausePermitsIntent(CauseResponseUndeliverable, outcome) {
			t.Fatalf("%s + %s rejected, want ADR completion-class recorded outcome allowed", CauseResponseUndeliverable, outcome)
		}
	}
	if recordedOutcomeCausePermitsIntent(CauseCompletedNormally, OutcomeCanceled) {
		t.Fatalf("%s + %s accepted, want canceled to use %s", CauseCompletedNormally, OutcomeCanceled, CauseCanceledAfterAuthorization)
	}
	if !recordedOutcomeCausePermitsIntent(CauseResponseUndeliverable, OutcomeCanceled) {
		t.Fatalf("%s + %s rejected, want ADR recorded outcome preserved", CauseResponseUndeliverable, OutcomeCanceled)
	}
	if !recordedOutcomeCausePermitsIntent(CauseCanceledAfterAuthorization, OutcomeCanceled) {
		t.Fatalf("%s + %s rejected, want canceled preserved under its authorization cause", CauseCanceledAfterAuthorization, OutcomeCanceled)
	}
	if recordedOutcomeCausePermitsIntent(CauseCanceledAfterAuthorization, OutcomeFailed) {
		t.Fatalf("%s + %s accepted, want only canceled", CauseCanceledAfterAuthorization, OutcomeFailed)
	}

	record := reducerMustApply(t, reducerConsumedRecord(t), ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCanceled})
	certificate, err := DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeCanceled,
		Cause:   CauseResponseUndeliverable,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate response undeliverable canceled error = %v", err)
	}
	if certificate.Outcome != OutcomeCanceled || certificate.Cause != CauseResponseUndeliverable || certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("terminal = %#v, want canceled/response-undeliverable/unresolved", certificate)
	}

	certificate, err = DeriveTerminalCertificate(record, TerminalIntent{
		Outcome: OutcomeCanceled,
		Cause:   CauseCanceledAfterAuthorization,
	})
	if err != nil {
		t.Fatalf("DeriveTerminalCertificate canceled after authorization error = %v", err)
	}
	if certificate.Outcome != OutcomeCanceled || certificate.Cause != CauseCanceledAfterAuthorization || certificate.Proof != ProofUnresolvedAbsence {
		t.Fatalf("terminal = %#v, want canceled/canceled-after-authorization/unresolved", certificate)
	}
}

func TestObserveOutcomeRejectsOrphaned(t *testing.T) {
	if _, err := apply(reducerGrantRecord(t), ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeOrphaned}); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("ObserveOutcome orphaned error = %v, want ErrInvalidCommand", err)
	}
}

func TestValidateSafetyRecordRejectsOrphanedIncompatibleCauseOrProof(t *testing.T) {
	wrongCause := reducerGrantRecord(t)
	wrongCause.Terminal = &TerminalCertificate{
		JobID:               wrongCause.JobID,
		Attempt:             wrongCause.Attempt.Ref,
		Outcome:             OutcomeOrphaned,
		Proof:               ProofUnresolvedAbsence,
		Cause:               CauseCompletedNormally,
		DerivedFromRevision: wrongCause.Revision,
		DerivedBy:           wrongCause.AdmittedBy,
	}
	if err := ValidateSafetyRecord(wrongCause); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("orphaned with completed-normally cause error = %v, want ErrInvalidValue", err)
	}

	provenAbsent := reducerRetiredRecord(t, reducerGrantRecord(t))
	provenAbsent.Terminal = &TerminalCertificate{
		JobID:               provenAbsent.JobID,
		Attempt:             provenAbsent.Attempt.Ref,
		Outcome:             OutcomeOrphaned,
		Proof:               ProofContained,
		Cause:               CauseDaemonRestartedAfterAuthorization,
		DerivedFromRevision: provenAbsent.Revision,
		DerivedBy:           provenAbsent.AdmittedBy,
	}
	if err := ValidateSafetyRecord(provenAbsent); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("orphaned with contained proof error = %v, want ErrInvalidValue", err)
	}
}

func TestValidateSafetyRecordRejectsOrphanedWithResult(t *testing.T) {
	base := reducerGrantRecord(t)
	result := reducerResultCommand(t, base).Receipt
	resultRef := result.Result

	tests := []struct {
		name   string
		mutate func(*SafetyRecord)
	}{
		{
			name: "record result",
			mutate: func(record *SafetyRecord) {
				record.Result = &result
			},
		},
		{
			name: "terminal result",
			mutate: func(record *SafetyRecord) {
				record.Terminal.Result = &resultRef
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := base
			record.Terminal = &TerminalCertificate{
				JobID:               record.JobID,
				Attempt:             record.Attempt.Ref,
				Outcome:             OutcomeOrphaned,
				Proof:               ProofUnresolvedAbsence,
				Cause:               CauseDaemonRestartedAfterAuthorization,
				DerivedFromRevision: record.Revision,
				DerivedBy:           record.AdmittedBy,
			}
			tt.mutate(&record)
			if err := ValidateSafetyRecord(record); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("orphaned with %s error = %v, want ErrInvalidValue", tt.name, err)
			}
		})
	}

	resultRecord := base
	resultRecord.Result = &result
	if _, err := DeriveTerminalCertificate(resultRecord, TerminalIntent{
		Outcome: OutcomeOrphaned,
		Cause:   CauseDaemonRestartedAfterAuthorization,
	}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("DeriveTerminalCertificate orphaned with record result error = %v, want ErrCommandPrecondition", err)
	}
}

func TestCleanupDispositionDerivation(t *testing.T) {
	tests := []struct {
		name   string
		record SafetyRecord
		want   CleanupDisposition
	}{
		{
			name: "no execution possible",
			record: reducerMustApply(t, reducerCanceledRetiredRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization},
			}),
			want: CleanupDispositionNoExecutionPossible,
		},
		{
			name: "release definitely not sent",
			record: func() SafetyRecord {
				record := reducerGrantRecord(t)
				record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseNotSent})
				record = reducerMustApply(t, record, reducerContainmentCommand(t, record))
				return reducerMustApply(t, record, Finalize{
					Ref:    reducerRef(),
					Intent: TerminalIntent{Outcome: OutcomeFailed, Cause: CauseReleaseDefinitelyNotSent},
				})
			}(),
			want: CleanupDispositionNoExecutionPossible,
		},
		{
			name: "legacy completed proof is unresolved",
			record: reducerMustApply(t, reducerLegacyUnfencedCompletedRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
			}),
			want: CleanupDispositionUnresolved,
		},
		{
			name: "empty launch set is not verified absent",
			record: reducerMustApply(t, reducerCanceledNoSupervisorRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization},
			}),
			want: CleanupDispositionNoExecutionPossible,
		},
		{
			name: "verified absent",
			record: reducerMustApply(t, reducerContainedRetiredRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledAfterAuthorization},
			}),
			want: CleanupDispositionVerifiedAbsent,
		},
		{
			name: "unresolved",
			record: reducerMustApply(t, reducerGrantRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeOrphaned, Cause: CauseDaemonRestartedAfterAuthorization},
			}),
			want: CleanupDispositionUnresolved,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeriveCleanupDisposition(tt.record); got != tt.want {
				t.Fatalf("DeriveCleanupDisposition() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestRecordedReleaseOutcomeSelectsADRCompatibleTerminal(t *testing.T) {
	tests := []struct {
		name           string
		releaseOutcome LaunchReleaseOutcome
		outcome        Outcome
		cause          TerminalCause
		cleanup        CleanupDisposition
	}{
		{
			name:           "not sent",
			releaseOutcome: LaunchReleaseNotSent,
			outcome:        OutcomeFailed,
			cause:          CauseReleaseDefinitelyNotSent,
			cleanup:        CleanupDispositionNoExecutionPossible,
		},
		{
			name:           "sent unknown verified absent",
			releaseOutcome: LaunchReleaseSentUnknown,
			outcome:        OutcomeReaped,
			cause:          CauseReleaseOutcomeUnknown,
			cleanup:        CleanupDispositionVerifiedAbsent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := reducerGrantRecord(t)
			record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: tt.releaseOutcome})
			record = reducerMustApply(t, record, reducerContainmentCommand(t, record))

			finalized, err := apply(record, Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: tt.outcome, Cause: tt.cause}})
			if err != nil {
				t.Fatalf("Finalize error = %v", err)
			}
			if finalized.Record.Terminal == nil ||
				finalized.Record.Terminal.Outcome != tt.outcome ||
				finalized.Record.Terminal.Cause != tt.cause {
				t.Fatalf("terminal = %#v, want %s/%s", finalized.Record.Terminal, tt.outcome, tt.cause)
			}
			if got := DeriveCleanupDisposition(finalized.Record); got != tt.cleanup {
				t.Fatalf("cleanup = %s, want %s", got, tt.cleanup)
			}
		})
	}
}

func TestReleaseOutcomeUnknownWithoutRecordedOutcomeCanOrphanWhenAbsenceUnresolved(t *testing.T) {
	record := reducerGrantRecord(t)
	record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: LaunchReleaseSentUnknown})
	finalized, err := apply(record, Finalize{
		Ref:    reducerRef(),
		Intent: TerminalIntent{Outcome: OutcomeOrphaned, Cause: CauseReleaseOutcomeUnknown},
	})
	if err != nil {
		t.Fatalf("Finalize release-unknown orphaned error = %v", err)
	}
	if finalized.Record.Terminal == nil ||
		finalized.Record.Terminal.Outcome != OutcomeOrphaned ||
		finalized.Record.Terminal.Proof != ProofUnresolvedAbsence ||
		finalized.Record.Terminal.Cause != CauseReleaseOutcomeUnknown {
		t.Fatalf("terminal = %#v, want orphaned/unresolved release-outcome-unknown", finalized.Record.Terminal)
	}
	if got := DeriveCleanupDisposition(finalized.Record); got != CleanupDispositionUnresolved {
		t.Fatalf("cleanup = %s, want %s", got, CleanupDispositionUnresolved)
	}
}

func TestPostGrantRecoveryTerminalizesFromAnyVerifiedQuiescenceMethod(t *testing.T) {
	tests := []struct {
		name   string
		method QuiescenceMethod
	}{
		{name: "already absent", method: QuiescenceAlreadyAbsent},
		{name: "natural exit", method: QuiescenceNaturalExit},
		{name: "host reboot", method: QuiescenceHostReboot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := reducerGrantRecord(t)
			record = reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalOne, tt.method))

			plan, err := PlanRecovery(record, RecoveryStartupLoss)
			if err != nil {
				t.Fatalf("PlanRecovery error = %v", err)
			}
			if plan.Next.Kind != RecoveryFinalizeCertified || plan.Next.Finalize == nil {
				t.Fatalf("plan = %#v, want finalize-certified action", plan.Next)
			}
			finalized, err := apply(record, *plan.Next.Finalize)
			if err != nil {
				t.Fatalf("Finalize after %s quiescence error = %v", tt.method, err)
			}
			if finalized.Record.Terminal == nil || finalized.Record.Terminal.Proof != ProofContained {
				t.Fatalf("terminal = %#v, want contained proof", finalized.Record.Terminal)
			}
		})
	}
}

func TestPlanRecoveryUsesRecordedReleaseOutcomeCause(t *testing.T) {
	tests := []struct {
		name           string
		releaseOutcome LaunchReleaseOutcome
		outcome        Outcome
		cause          TerminalCause
	}{
		{name: "not sent", releaseOutcome: LaunchReleaseNotSent, outcome: OutcomeFailed, cause: CauseReleaseDefinitelyNotSent},
		{name: "sent unknown", releaseOutcome: LaunchReleaseSentUnknown, outcome: OutcomeReaped, cause: CauseReleaseOutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := reducerGrantRecord(t)
			record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Outcome: tt.releaseOutcome})
			record = reducerMustApply(t, record, reducerContainmentCommand(t, record))

			plan, err := PlanRecovery(record, RecoveryStartupLoss)
			if err != nil {
				t.Fatalf("PlanRecovery error = %v", err)
			}
			if plan.Next.Kind != RecoveryFinalizeCertified || plan.Next.Finalize == nil {
				t.Fatalf("plan = %#v, want finalize-certified action", plan.Next)
			}
			if plan.Next.Finalize.Intent.Outcome != tt.outcome || plan.Next.Finalize.Intent.Cause != tt.cause {
				t.Fatalf("finalize intent = %+v, want %s/%s", plan.Next.Finalize.Intent, tt.outcome, tt.cause)
			}
		})
	}
}

func TestCompletedOutcomeWithUnprovableAbsenceStaysCompletedUnresolved(t *testing.T) {
	record := reducerCompletedOutcomeRecord(t)
	record = reducerMustApply(t, record, reducerResultCommand(t, record))

	finalized, err := apply(record, Finalize{
		Ref:    reducerRef(),
		Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
	})
	if err != nil {
		t.Fatalf("Finalize completed unresolved error = %v", err)
	}
	if finalized.Record.Terminal == nil ||
		finalized.Record.Terminal.Outcome != OutcomeCompleted ||
		finalized.Record.Terminal.Proof != ProofUnresolvedAbsence ||
		finalized.Record.Terminal.Result == nil {
		t.Fatalf("terminal = %#v, want completed with unresolved proof and result", finalized.Record.Terminal)
	}
	if got := DeriveCleanupDisposition(finalized.Record); got != CleanupDispositionUnresolved {
		t.Fatalf("cleanup = %s, want %s", got, CleanupDispositionUnresolved)
	}
	projection, err := Project(finalized.Record, ProjectionMetadata{})
	if err != nil {
		t.Fatalf("Project completed unresolved terminal: %v", err)
	}
	if projection.Outcome != OutcomeCompleted || projection.Public != PublicCompleted {
		t.Fatalf("projection outcome/public = %s/%s, want completed/completed", projection.Outcome, projection.Public)
	}
}

func TestPlanRecoveryPreservesRecordedOutcomeAfterAuthorization(t *testing.T) {
	tests := []struct {
		name   string
		record SafetyRecord
		want   Outcome
	}{
		{
			name:   "failed",
			record: reducerMustApply(t, reducerQuiescentRecord(t), ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeFailed}),
			want:   OutcomeFailed,
		},
		{
			name:   "completed",
			record: reducerCleanCompletedRecord(t),
			want:   OutcomeCompleted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := PlanRecovery(tt.record, RecoveryStartupLoss)
			if err != nil {
				t.Fatalf("PlanRecovery error = %v", err)
			}
			if plan.Next.Kind != RecoveryFinalizeCertified || plan.Next.Finalize == nil {
				t.Fatalf("plan = %#v, want finalize-certified action", plan.Next)
			}
			if plan.Next.Finalize.Intent.Outcome != tt.want {
				t.Fatalf("finalize outcome = %s, want recorded %s", plan.Next.Finalize.Intent.Outcome, tt.want)
			}
			finalized, err := apply(tt.record, *plan.Next.Finalize)
			if err != nil {
				t.Fatalf("Finalize recorded outcome error = %v", err)
			}
			if finalized.Record.Terminal == nil || finalized.Record.Terminal.Outcome != tt.want {
				t.Fatalf("terminal = %#v, want outcome %s", finalized.Record.Terminal, tt.want)
			}
			if finalized.Record.Terminal.Cause != CauseCompletedNormally {
				t.Fatalf("terminal cause = %s, want %s", finalized.Record.Terminal.Cause, CauseCompletedNormally)
			}
		})
	}
}

func TestPlanRecoveryAuthorizedWithoutRecordedOutcomeStillReaps(t *testing.T) {
	record := reducerGrantRecord(t)
	record = reducerMustApply(t, record, reducerQuiescenceCommand(t, record, LaunchOrdinalOne))

	plan, err := PlanRecovery(record, RecoveryStartupLoss)
	if err != nil {
		t.Fatalf("PlanRecovery error = %v", err)
	}
	if plan.Next.Kind != RecoveryFinalizeCertified || plan.Next.Finalize == nil {
		t.Fatalf("plan = %#v, want finalize-certified action", plan.Next)
	}
	if plan.Next.Finalize.Intent.Outcome != OutcomeReaped || plan.Next.Finalize.Intent.Cause != CauseDaemonRestartedAfterAuthorization {
		t.Fatalf("finalize intent = %+v, want reaped daemon-restarted-after-authorization", plan.Next.Finalize.Intent)
	}
	finalized, err := apply(record, *plan.Next.Finalize)
	if err != nil {
		t.Fatalf("Finalize authorized no-outcome recovery error = %v", err)
	}
	if finalized.Record.Terminal == nil ||
		finalized.Record.Terminal.Outcome != OutcomeReaped ||
		finalized.Record.Terminal.Cause != CauseDaemonRestartedAfterAuthorization {
		t.Fatalf("terminal = %#v, want reaped daemon-restarted-after-authorization", finalized.Record.Terminal)
	}
}

func TestPlanRecoveryContradictoryRecordedOutcomeMissingReleaseStaysFatalUnprovable(t *testing.T) {
	record := reducerGrantRecord(t)
	record = reducerMustApply(t, record, reducerContainmentCommand(t, record))
	record = reducerMustApply(t, record, ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeFailed})

	plan, err := PlanRecovery(record, RecoveryStartupLoss)
	if err != nil {
		t.Fatalf("PlanRecovery error = %v", err)
	}
	if plan.Next.Kind != RecoveryFatalUnprovable {
		t.Fatalf("plan = %#v, want fatal-unprovable", plan.Next)
	}
}

func TestOnlyTerminalGoSelectsProofKind(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	for _, name := range []string{"command.go", "reducer.go", "recovery.go"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(data)
		for _, proof := range []string{"ProofNeverPermittedAndRetired", "ProofCleanQuiescentOutcomeAndRetired", "ProofContained", "ProofLegacyUnfencedOutcome", "ProofUnresolvedAbsence"} {
			if strings.Contains(text, proof) {
				t.Fatalf("%s contains terminal proof selection %s", name, proof)
			}
		}
	}
}

func TestPlanRecoveryUsesOnePlannerForRecoveryTriggers(t *testing.T) {
	tests := []struct {
		name    string
		record  SafetyRecord
		trigger RecoveryTrigger
		want    RecoveryActionKind
	}{
		{name: "startup loss without grant", record: reducerSupervisorRecord(), trigger: RecoveryStartupLoss, want: RecoveryRetireThenFinalize},
		{name: "post grant failure before grant commit", record: reducerSupervisorRecord(), trigger: RecoveryPostGrantFailure, want: RecoveryRetireThenFinalize},
		{name: "live loss with grant", record: reducerGrantRecord(t), trigger: RecoveryLiveLoss, want: RecoveryContainThenFinalize},
		{name: "cancel with grant", record: reducerGrantRecord(t), trigger: RecoveryCancelAfterGrant, want: RecoveryContainThenFinalize},
		{name: "corrupt with grant", record: reducerGrantRecord(t), trigger: RecoveryCorruption, want: RecoveryContainThenFinalize},
		{name: "startup already certified", record: reducerRetiredRecord(t, reducerSupervisorRecord()), trigger: RecoveryStartupLoss, want: RecoveryFinalizeCertified},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := PlanRecovery(tt.record, tt.trigger)
			if err != nil {
				t.Fatalf("PlanRecovery error = %v", err)
			}
			if plan.BasedOnRevision != tt.record.Revision {
				t.Fatalf("based revision = %d, want %d", plan.BasedOnRevision, tt.record.Revision)
			}
			if plan.Next.Kind != tt.want {
				t.Fatalf("plan kind = %v, want %v", plan.Next.Kind, tt.want)
			}
			if tt.want == RecoveryFinalizeCertified && plan.Next.Finalize == nil {
				t.Fatal("finalize-certified plan did not include Finalize command")
			}
		})
	}
}

func reducerBaseRecord() SafetyRecord {
	ref := reducerRef()
	return SafetyRecord{
		SchemaVersion: 1,
		Revision:      1,
		JobID:         ref.JobID,
		RequestKey:    RequestKey{WorkspaceKey: "workspace/reducer", RequestID: "request-reducer"},
		TaskIdentity:  NewSHA256TaskIdentity([]byte("reducer task")),
		Mode:          ModeIdentifiedFenced,
		AdmittedBy:    reducerBoot(),
		Attempt:       AttemptProof{Ref: ref},
	}
}

func reducerLegacyAwaitingRecord() SafetyRecord {
	record := reducerBaseRecord()
	record.Mode = ModeLegacyFenced
	record.Acknowledgement = nil
	return record
}

func reducerLegacyAcknowledgedRecord() SafetyRecord {
	record := reducerLegacyAwaitingRecord()
	ack := AcknowledgementFact{Attempt: record.Attempt.Ref, AcknowledgedBy: record.AdmittedBy}
	record.Acknowledgement = &ack
	return record
}

func reducerSupervisorRecord() SafetyRecord {
	record := reducerBaseRecord()
	group := reducerGroup(LaunchOrdinalOne)
	record.Attempt.Launches.First = &LaunchProof{Ordinal: LaunchOrdinalOne, Group: &group}
	return record
}

func reducerGrantRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerSupervisorRecord(), CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"})
}

func reducerConsumedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerGrantRecord(t), RecordRelease{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Child: reducerChild(21)})
}

func reducerQuiescentRecord(t *testing.T) SafetyRecord {
	t.Helper()
	record := reducerConsumedRecord(t)
	return reducerMustApply(t, record, reducerQuiescenceCommand(t, record, LaunchOrdinalOne))
}

func reducerCanceledRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerSupervisorRecord(), RequestCancel{JobID: reducerJobID()})
}

func reducerCanceledNoSupervisorRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerBaseRecord(), RequestCancel{JobID: reducerJobID()})
}

func reducerRetiredRecord(t *testing.T, record SafetyRecord) SafetyRecord {
	t.Helper()
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && launch.Group != nil && launch.Quiescence == nil {
			record = reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, ordinal, QuiescenceAlreadyAbsent))
		}
	}
	return record
}

func reducerCanceledRetiredRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerRetiredRecord(t, reducerCanceledRecord(t))
}

func reducerContainedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	record := reducerGrantRecord(t)
	return reducerMustApply(t, record, reducerContainmentCommand(t, record))
}

func reducerContainedRetiredRecord(t *testing.T) SafetyRecord {
	t.Helper()
	record := reducerGrantRecord(t)
	record = reducerMustApply(t, record, reducerContainmentCommand(t, record))
	return reducerRetiredRecord(t, record)
}

func reducerCompletedOutcomeRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerConsumedRecord(t), ObserveOutcome{Ref: reducerRef(), Outcome: OutcomeCompleted})
}

func reducerCleanCompletedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	record := reducerCompletedOutcomeRecord(t)
	record = reducerMustApply(t, record, reducerQuiescenceCommand(t, record, LaunchOrdinalOne))
	return reducerMustApply(t, record, reducerResultCommand(t, record))
}

func reducerOrdinalOneReleasedOrdinalTwoReleaseOutcomeRecord(t *testing.T, outcome LaunchReleaseOutcome) SafetyRecord {
	t.Helper()
	record := reducerConsumedRecord(t)
	record = reducerMustApply(t, record, reducerQuiescenceCommand(t, record, LaunchOrdinalOne))
	record = reducerMustApply(t, record, BindGroup{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Group: reducerGroup(LaunchOrdinalTwo)})
	record = reducerMustApply(t, record, CommitGrant{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Nonce: "nonce-2"})
	record = reducerMustApply(t, record, RecordReleaseOutcome{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Outcome: outcome})
	return reducerMustApply(t, record, reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalTwo, QuiescenceAlreadyAbsent))
}

func reducerLegacyUnfencedCompletedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	record := reducerBaseRecord()
	record.Mode = ModeLegacyUnfenced
	record = reducerMustApply(t, record, ObserveOutcome{Ref: record.Attempt.Ref, Outcome: OutcomeCompleted})
	return reducerMustApply(t, record, reducerResultCommand(t, record))
}

func reducerMustApply(t *testing.T, record SafetyRecord, command Command) SafetyRecord {
	t.Helper()
	result, err := apply(record, command)
	if err != nil {
		t.Fatalf("apply(%T) error = %v", command, err)
	}
	if !result.Changed {
		t.Fatalf("apply(%T) did not change record", command)
	}
	if err := ValidateSafetyRecord(result.Record); err != nil {
		t.Fatalf("apply(%T) output invalid: %v", command, err)
	}
	return result.Record
}

func reducerQuiescenceCommand(t *testing.T, record SafetyRecord, ordinal LaunchOrdinal) RecordQuiescence {
	t.Helper()
	return reducerQuiescenceCommandWithMethod(t, record, ordinal, QuiescenceNaturalExit)
}

func reducerQuiescenceCommandWithMethod(t *testing.T, record SafetyRecord, ordinal LaunchOrdinal, method QuiescenceMethod) RecordQuiescence {
	t.Helper()
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil {
		t.Fatalf("missing launch group for ordinal %s", ordinal)
	}
	return RecordQuiescence{
		Ref: record.Attempt.Ref,
		Receipt: QuiescenceReceipt{
			Attempt:     record.Attempt.Ref,
			Ordinal:     ordinal,
			Group:       *launch.Group,
			Method:      method,
			CertifiedBy: record.AdmittedBy,
		},
	}
}

func reducerContainmentCommand(t *testing.T, record SafetyRecord) RecordQuiescence {
	t.Helper()
	return reducerQuiescenceCommandWithMethod(t, record, LaunchOrdinalOne, QuiescenceTermKill)
}

func reducerResultCommand(t *testing.T, record SafetyRecord) CertifyResult {
	t.Helper()
	return CertifyResult{
		Ref: record.Attempt.Ref,
		Receipt: ResultReceipt{
			JobID:       record.JobID,
			Result:      ResultRef{Path: "results/reducer.txt", Digest: "sha256:abc123", Bytes: 3},
			DirSynced:   mustEvidence(t, "dir_synced", "result directory fsynced"),
			CertifiedBy: record.AdmittedBy,
		},
	}
}

func reducerJobID() JobID {
	return "job-reducer"
}

func reducerRef() AttemptRef {
	return AttemptRef{JobID: reducerJobID(), AttemptID: "attempt-reducer", Epoch: 1}
}

func reducerBoot() BootRef {
	return BootRef{BootID: "boot-reducer", OwnerID: "owner-reducer"}
}

func reducerGroup(ordinal LaunchOrdinal) GroupRef {
	pgid := 20 + int(ordinal)
	return GroupRef{
		Version:   1,
		CustodyID: CustodyID("custody-reducer-" + ordinal.String()),
		Launch: LaunchKey{
			Attempt: reducerRef(),
			Ordinal: ordinal,
		},
		HostBootID:        "host-boot-reducer",
		PIDNamespaceState: PIDNamespaceNotApplicable,
		PGID:              pgid,
		Leader:            ProcessIdentity{PID: pgid, HighResStartToken: "leader-start-" + ordinal.String()},
		Monitor:           ProcessIdentity{PID: 30 + int(ordinal), HighResStartToken: "monitor-start-" + ordinal.String()},
		RetainedID:        "retained-" + ordinal.String(),
	}
}

func reducerConflictingGroup(ordinal LaunchOrdinal) GroupRef {
	group := reducerGroup(ordinal)
	group.CustodyID = CustodyID("custody-conflict-" + ordinal.String())
	group.PGID += 100
	group.Leader.PID += 100
	group.Leader.HighResStartToken = "leader-conflict-" + ordinal.String()
	return group
}

func reducerChild(pid int) ChildIdentity {
	return ChildIdentity{PID: pid, HighResStartToken: "child-start"}
}
