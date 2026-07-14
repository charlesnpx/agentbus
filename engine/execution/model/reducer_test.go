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
			name:    "bind supervisor",
			valid:   reducerBaseRecord(),
			invalid: reducerCanceledNoSupervisorRecord(t),
			command: BindSupervisor{Ref: reducerRef(), Supervisor: reducerSupervisor()},
		},
		{
			name:    "authorize launch",
			valid:   reducerSupervisorRecord(),
			invalid: reducerBaseRecord(),
			command: AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"},
		},
		{
			name:    "observe launch consumed",
			valid:   reducerGrantRecord(t),
			invalid: reducerSupervisorRecord(),
			command: ObserveLaunchConsumed{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Child: reducerChild(21)},
		},
		{
			name:    "observe launch quiescent",
			valid:   reducerConsumedRecord(t),
			invalid: reducerGrantRecord(t),
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
			name:    "certify retirement",
			valid:   reducerSupervisorRecord(),
			invalid: reducerGrantRecord(t),
			command: reducerRetirementCommand(t, reducerSupervisorRecord()),
		},
		{
			name:    "certify containment",
			valid:   reducerGrantRecord(t),
			invalid: reducerBaseRecord(),
			command: reducerContainmentCommand(t, reducerGrantRecord(t)),
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
			result, err := Apply(tt.valid, tt.command)
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

			repeated, err := Apply(result.Record, tt.command)
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
			if _, err := Apply(tt.invalid, tt.command); err == nil {
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
			name:     "supervisor",
			record:   reducerBaseRecord(),
			first:    BindSupervisor{Ref: reducerRef(), Supervisor: reducerSupervisor()},
			conflict: BindSupervisor{Ref: reducerRef(), Supervisor: SupervisorIdentity{PGID: 20, LeaderPID: 21, HighResStartToken: "supervisor-start-20"}},
		},
		{
			name:     "grant nonce",
			record:   reducerSupervisorRecord(),
			first:    AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"},
			conflict: AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-other"},
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
			first, err := Apply(tt.record, tt.first)
			if err != nil {
				t.Fatalf("first Apply error = %v", err)
			}
			if _, err := Apply(first.Record, tt.conflict); !errors.Is(err, ErrConflictingDuplicate) {
				t.Fatalf("conflicting duplicate error = %v, want ErrConflictingDuplicate", err)
			}
		})
	}
}

func TestAuthorizeSecondOrdinalRequiresFirstQuiescence(t *testing.T) {
	first := reducerGrantRecord(t)
	if _, err := Apply(first, AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Nonce: "nonce-2"}); !errors.Is(err, ErrCommandPrecondition) {
		t.Fatalf("ordinal 2 before quiescence error = %v, want ErrCommandPrecondition", err)
	}
	quiescent := reducerQuiescentRecord(t)
	result, err := Apply(quiescent, AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalTwo, Nonce: "nonce-2"})
	if err != nil {
		t.Fatalf("ordinal 2 after quiescence error = %v", err)
	}
	if _, ok := result.Record.Attempt.Grants.Get(LaunchOrdinalTwo); !ok {
		t.Fatal("ordinal 2 grant was not recorded")
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
	record := reducerSupervisorRecord()
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
	supervisor := reducerSupervisor()
	record.Attempt.Supervisor = &supervisor
	return record
}

func reducerGrantRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerSupervisorRecord(), AuthorizeLaunch{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Nonce: "nonce-1"})
}

func reducerConsumedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerGrantRecord(t), ObserveLaunchConsumed{Ref: reducerRef(), Ordinal: LaunchOrdinalOne, Child: reducerChild(21)})
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
	return reducerMustApply(t, record, reducerRetirementCommand(t, record))
}

func reducerCanceledRetiredRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerRetiredRecord(t, reducerCanceledRecord(t))
}

func reducerContainedRecord(t *testing.T) SafetyRecord {
	t.Helper()
	return reducerMustApply(t, reducerGrantRecord(t), reducerContainmentCommand(t, reducerGrantRecord(t)))
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
	record = reducerRetiredRecord(t, record)
	return reducerMustApply(t, record, reducerResultCommand(t, record))
}

func reducerMustApply(t *testing.T, record SafetyRecord, command Command) SafetyRecord {
	t.Helper()
	result, err := Apply(record, command)
	if err != nil {
		t.Fatalf("Apply(%T) error = %v", command, err)
	}
	if !result.Changed {
		t.Fatalf("Apply(%T) did not change record", command)
	}
	if err := ValidateSafetyRecord(result.Record); err != nil {
		t.Fatalf("Apply(%T) output invalid: %v", command, err)
	}
	return result.Record
}

func reducerQuiescenceCommand(t *testing.T, record SafetyRecord, ordinal LaunchOrdinal) ObserveLaunchQuiescent {
	t.Helper()
	consumed, ok := record.Attempt.Consumed.Get(ordinal)
	if !ok {
		t.Fatalf("missing consumed launch for ordinal %s", ordinal)
	}
	return ObserveLaunchQuiescent{
		Ref: record.Attempt.Ref,
		Receipt: QuiescenceReceipt{
			Attempt:     record.Attempt.Ref,
			Ordinal:     ordinal,
			Child:       consumed.Child,
			ChildExited: mustEvidence(t, "child_exit", "child exited"),
			GroupEmpty:  mustEvidence(t, "group_empty", "process group empty"),
			CertifiedBy: record.AdmittedBy,
		},
	}
}

func reducerRetirementCommand(t *testing.T, record SafetyRecord) CertifyRetirement {
	t.Helper()
	if record.Attempt.Supervisor == nil {
		t.Fatal("missing supervisor")
	}
	return CertifyRetirement{
		Ref: record.Attempt.Ref,
		Receipt: RetirementReceipt{
			Attempt:       record.Attempt.Ref,
			Supervisor:    *record.Attempt.Supervisor,
			ControlClosed: mustEvidence(t, "control_closed", "control channel closed"),
			WorkerExited:  mustEvidence(t, "worker_exit", "worker exited"),
			GroupEmpty:    mustEvidence(t, "group_empty", "process group empty"),
			CertifiedBy:   record.AdmittedBy,
		},
	}
}

func reducerContainmentCommand(t *testing.T, record SafetyRecord) CertifyContainment {
	t.Helper()
	if record.Attempt.Supervisor == nil {
		t.Fatal("missing supervisor")
	}
	return CertifyContainment{
		Ref: record.Attempt.Ref,
		Receipt: ContainmentReceipt{
			Attempt:      record.Attempt.Ref,
			Supervisor:   *record.Attempt.Supervisor,
			Signal:       mustEvidence(t, "containment_signal", "containment signal sent"),
			Verification: mustEvidence(t, "verified_absent", "process group absent"),
			CertifiedBy:  record.AdmittedBy,
		},
	}
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

func reducerSupervisor() SupervisorIdentity {
	return SupervisorIdentity{PGID: 10, LeaderPID: 11, HighResStartToken: "supervisor-start-10"}
}

func reducerChild(pid int) ChildIdentity {
	return ChildIdentity{PID: pid, HighResStartToken: "child-start"}
}
