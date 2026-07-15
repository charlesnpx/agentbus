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

	if _, err := apply(record, Finalize{Ref: reducerRef(), Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally}}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("clean finalize with unretired bound ordinal error = %v, want ErrCommandPrecondition", err)
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
		for _, proof := range []string{"ProofNeverPermittedAndRetired", "ProofCleanQuiescentOutcomeAndRetired", "ProofContained"} {
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
	return GroupRef{
		Version:   1,
		CustodyID: CustodyID("custody-reducer-" + ordinal.String()),
		Launch: LaunchKey{
			Attempt: reducerRef(),
			Ordinal: ordinal,
		},
		HostBootID: "host-boot-reducer",
		PGID:       10 + int(ordinal),
		Leader:     ProcessIdentity{PID: 20 + int(ordinal), HighResStartToken: "leader-start-" + ordinal.String()},
		Monitor:    ProcessIdentity{PID: 30 + int(ordinal), HighResStartToken: "monitor-start-" + ordinal.String()},
		RetainedID: "retained-" + ordinal.String(),
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
