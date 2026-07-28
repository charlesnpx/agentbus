package model

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestIDValueValidation(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
		want bool
	}{
		{name: "job id valid", run: func() error { _, err := NewJobID("job-0001"); return err }},
		{name: "job id empty", run: func() error { _, err := NewJobID(""); return err }, want: true},
		{name: "job id whitespace", run: func() error { _, err := NewJobID("job 1"); return err }, want: true},
		{name: "workspace key valid", run: func() error { _, err := NewWorkspaceKey("workspace/a"); return err }},
		{name: "workspace key newline", run: func() error { _, err := NewWorkspaceKey("workspace\n"); return err }, want: true},
		{name: "request id valid", run: func() error { _, err := NewRequestID("req-1"); return err }},
		{name: "boot ref valid", run: func() error { _, err := NewBootRef("boot-1", "owner-1"); return err }},
		{name: "boot ref missing owner", run: func() error { _, err := NewBootRef("boot-1", ""); return err }, want: true},
		{name: "attempt ref valid", run: func() error { _, err := NewAttemptRef("job-0001", "attempt-1", 1); return err }},
		{name: "attempt ref zero epoch", run: func() error { _, err := NewAttemptRef("job-0001", "attempt-1", 0); return err }, want: true},
		{name: "request key valid", run: func() error { _, err := NewRequestKey("workspace/a", "req-1"); return err }},
		{name: "request key missing request", run: func() error { _, err := NewRequestKey("workspace/a", ""); return err }, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if tt.want {
				if !errors.Is(err, ErrInvalidValue) {
					t.Fatalf("error = %v, want ErrInvalidValue", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestTaskIdentityValidation(t *testing.T) {
	identity := NewSHA256TaskIdentity([]byte("canonical task spec"))
	if err := identity.Validate(); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	if len(identity.Value) != 64 {
		t.Fatalf("sha256 identity length = %d, want 64", len(identity.Value))
	}

	tests := []struct {
		name     string
		mutate   func(TaskIdentity) TaskIdentity
		wantText string
	}{
		{name: "unsupported algorithm", mutate: func(in TaskIdentity) TaskIdentity {
			in.Algorithm = "md5"
			return in
		}, wantText: "unsupported"},
		{name: "bad length", mutate: func(in TaskIdentity) TaskIdentity {
			in.Value = "abc"
			return in
		}, wantText: "sha256"},
		{name: "uppercase hex", mutate: func(in TaskIdentity) TaskIdentity {
			in.Value = strings.ToUpper(in.Value)
			return in
		}, wantText: "lowercase"},
		{name: "not hex", mutate: func(in TaskIdentity) TaskIdentity {
			in.Value = strings.Repeat("z", 64)
			return in
		}, wantText: "hexadecimal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mutate(identity).Validate()
			if !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("error = %v, want ErrInvalidValue", err)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("error = %q, want text %q", err, tt.wantText)
			}
		})
	}

	futureVersion := identity
	futureVersion.Version = CurrentTaskIdentityVersion + 1
	if err := futureVersion.Validate(); err != nil {
		t.Fatalf("future sha256 identity version rejected as record shape: %v", err)
	}
	if _, err := TaskIdentityCodecFor(futureVersion.Algorithm, futureVersion.Version); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("future sha256 codec error = %v, want ErrInvalidValue", err)
	}
}

func TestEnumAndCertificateValidation(t *testing.T) {
	for _, mode := range AllModes() {
		if err := mode.Validate(); err != nil {
			t.Fatalf("mode %s invalid: %v", mode, err)
		}
	}
	for _, cause := range AllTerminalCauses() {
		if err := cause.Validate(); err != nil {
			t.Fatalf("cause %s invalid: %v", cause, err)
		}
		if cause.ProtocolString() == "" {
			t.Fatalf("cause %v has empty protocol string", cause)
		}
	}
	if err := LaunchOrdinalOne.Validate(); err != nil {
		t.Fatalf("ordinal one invalid: %v", err)
	}
	if err := LaunchOrdinalTwo.Validate(); err != nil {
		t.Fatalf("ordinal two invalid: %v", err)
	}
	if err := LaunchOrdinal(3).Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("ordinal 3 error = %v, want ErrInvalidValue", err)
	}
}

func TestSafetyRecordValidationRequiresMatchingProofIdentities(t *testing.T) {
	record := validSafetyRecord()
	if err := record.Validate(); err != nil {
		t.Fatalf("valid safety record rejected: %v", err)
	}

	mismatchJob := record
	mismatchJob.Attempt.Ref.JobID = "job-other"
	if err := mismatchJob.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("mismatched attempt job error = %v, want ErrInvalidValue", err)
	}

	missingGrant := cloneSafetyRecord(record)
	missingGrant.Attempt.Launches.First.Released = &LaunchReleaseFact{
		Attempt:     record.Attempt.Ref,
		Ordinal:     LaunchOrdinalOne,
		Nonce:       "nonce-1",
		Child:       mustChild(t),
		ReleasedBy:  record.AdmittedBy,
		Observation: mustEvidence(t, "started", "child observed"),
	}
	missingGrant.Attempt.Launches.First.Grant = nil
	if err := missingGrant.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("missing grant error = %v, want ErrInvalidValue", err)
	}

	wrongSlot := cloneSafetyRecord(record)
	wrongSlot.Attempt.Launches.First.Grant.Ordinal = LaunchOrdinalTwo
	if err := wrongSlot.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("wrong slot error = %v, want ErrInvalidValue", err)
	}
}

func TestAttemptProofValidationRejectsLaunchSlotTopologyGap(t *testing.T) {
	record := validSafetyRecord()
	group := *record.Attempt.Launches.First.Group
	group.CustodyID = "custody-2"
	group.Launch.Ordinal = LaunchOrdinalTwo
	group.PGID = 20
	group.Leader = ProcessIdentity{PID: 20, HighResStartToken: "leader-start-20"}
	group.Monitor = ProcessIdentity{PID: 22, HighResStartToken: "monitor-start-20"}
	group.RetainedID = "retained-2"
	group.RetainedDomainID = "retained-domain-2"
	group.RetainedDomainState = RetainedDomainKnown
	grant := *record.Attempt.Launches.First.Grant
	grant.Ordinal = LaunchOrdinalTwo

	record.Attempt.Launches.First = nil
	record.Attempt.Launches.Second = &LaunchProof{
		Ordinal: LaunchOrdinalTwo,
		Group:   &group,
		Grant:   &grant,
	}

	if err := record.Attempt.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("attempt proof topology gap error = %v, want ErrInvalidValue", err)
	}
	if err := ValidateSafetyRecord(record); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("safety record topology gap error = %v, want ErrInvalidValue", err)
	}
}

func TestBindingMustMatchSafetyRecordIdentity(t *testing.T) {
	record := validSafetyRecord()
	binding := Binding{
		RequestKey:   record.RequestKey,
		JobID:        record.JobID,
		TaskIdentity: record.TaskIdentity,
		Mode:         record.Mode,
	}
	if err := binding.Matches(record); err != nil {
		t.Fatalf("matching binding rejected: %v", err)
	}
	binding.TaskIdentity = NewSHA256TaskIdentity([]byte("different task"))
	if err := binding.Matches(record); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("mismatched binding error = %v, want ErrInvalidValue", err)
	}
}

func TestGroupRefValidationRequiresLeaderToHeadPGID(t *testing.T) {
	record := validSafetyRecord()
	group := *record.Attempt.Launches.First.Group
	group.Leader.PID = group.PGID + 1
	if err := group.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("group with non-leader pid error = %v, want ErrInvalidValue", err)
	}
}

func TestGroupRefValidationRejectsUnsafePGID(t *testing.T) {
	record := validSafetyRecord()
	for _, pgid := range []int{0, 1} {
		t.Run(fmt.Sprintf("pgid_%d", pgid), func(t *testing.T) {
			group := *record.Attempt.Launches.First.Group
			group.PGID = pgid
			group.Leader.PID = pgid

			err := group.Validate()
			if !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("group pgid %d error = %v, want ErrInvalidValue", pgid, err)
			}
			var validation ValidationError
			if !errors.As(err, &validation) || validation.Field != "group.pgid" {
				t.Fatalf("group pgid %d error = %v, want group.pgid validation error", pgid, err)
			}
		})
	}
}

func TestGroupRefValidationRequiresRetainedIdentityDomainPair(t *testing.T) {
	base := *validSafetyRecord().Attempt.Launches.First.Group
	tests := []struct {
		name  string
		group GroupRef
		field string
	}{
		{
			name: "known domain without retained id",
			group: func() GroupRef {
				group := base
				group.RetainedID = ""
				group.RetainedDomainID = "retained-domain-known"
				group.RetainedDomainState = RetainedDomainKnown
				return group
			}(),
			field: "group.retained_id",
		},
		{
			name: "retained id without retained domain",
			group: func() GroupRef {
				group := base
				group.RetainedID = "retained-present"
				group.RetainedDomainID = ""
				group.RetainedDomainState = RetainedDomainNotApplicable
				return group
			}(),
			field: "group.retained_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.group.Validate()
			if !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("Validate() error = %v, want ErrInvalidValue", err)
			}
			var validation ValidationError
			if !errors.As(err, &validation) || validation.Field != tt.field {
				t.Fatalf("Validate() error = %v, want %s validation error", err, tt.field)
			}
		})
	}
}

func TestGroupPhysicalIdentityIncludesLeaderReuseAcrossPGIDs(t *testing.T) {
	record := validSafetyRecord()
	first := *record.Attempt.Launches.First.Group
	second := first
	second.PGID = first.PGID + 1
	if !first.SamePhysicalIdentity(second) {
		t.Fatalf("SamePhysicalIdentity returned false for reused leader identity across pgids")
	}
}

func TestGroupPhysicalIdentityRequiresRetainedIDMatchWhenPresent(t *testing.T) {
	record := validSafetyRecord()
	first := *record.Attempt.Launches.First.Group
	first.RetainedID = "retained-first"
	first.RetainedDomainID = "retained-domain-first"
	first.RetainedDomainState = RetainedDomainKnown
	tests := []struct {
		name   string
		mutate func(*GroupRef)
	}{
		{
			name: "different_retained_id",
			mutate: func(group *GroupRef) {
				group.RetainedID = "different-retained"
			},
		},
		{
			name: "empty_vs_present",
			mutate: func(group *GroupRef) {
				group.RetainedID = ""
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			second := first
			tt.mutate(&second)
			if first.SamePhysicalIdentity(second) {
				t.Fatalf("SamePhysicalIdentity returned true for %s", tt.name)
			}
		})
	}
}

func TestTerminalProofRequiresSupportingCertificates(t *testing.T) {
	record := validSafetyRecord()
	record.Terminal = &TerminalCertificate{
		JobID:               record.JobID,
		Attempt:             record.Attempt.Ref,
		Outcome:             OutcomeCanceled,
		Proof:               ProofNeverPermittedAndRetired,
		Cause:               CauseCanceledBeforeAuthorization,
		DerivedFromRevision: record.Revision,
		DerivedBy:           record.AdmittedBy,
	}
	if err := record.Validate(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("terminal without supporting quiescence error = %v, want ErrInvalidValue", err)
	}

	record.Attempt.Launches.First.Grant = nil
	record.Attempt.Launches.First.Quiescence = &QuiescenceCertificate{
		Attempt:     record.Attempt.Ref,
		Ordinal:     LaunchOrdinalOne,
		Group:       *record.Attempt.Launches.First.Group,
		Method:      QuiescenceAlreadyAbsent,
		CertifiedBy: record.AdmittedBy,
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("terminal with supporting quiescence rejected: %v", err)
	}
}

func TestValidateSafetyRecordRejectsTerminalProofFromWrongMode(t *testing.T) {
	fencedRecords := []struct {
		name   string
		record SafetyRecord
	}{
		{
			name: "never_permitted",
			record: reducerMustApply(t, reducerCanceledRetiredRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledBeforeAuthorization},
			}),
		},
		{
			name: "clean_quiescent",
			record: reducerMustApply(t, reducerCleanCompletedRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
			}),
		},
		{
			name: "contained",
			record: reducerMustApply(t, reducerContainedRetiredRecord(t), Finalize{
				Ref:    reducerRef(),
				Intent: TerminalIntent{Outcome: OutcomeCanceled, Cause: CauseCanceledAfterAuthorization},
			}),
		},
	}
	for _, tt := range fencedRecords {
		t.Run(tt.name+"_valid", func(t *testing.T) {
			if err := ValidateSafetyRecord(tt.record); err != nil {
				t.Fatalf("valid fenced terminal record rejected: %v", err)
			}
		})
		t.Run(tt.name+"_legacy_unfenced_rejected", func(t *testing.T) {
			record := tt.record
			record.Mode = ModeLegacyUnfenced
			if err := ValidateSafetyRecord(record); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("legacy-unfenced record with %s error = %v, want ErrInvalidValue", tt.record.Terminal.Proof, err)
			}
		})
	}

	legacy := reducerMustApply(t, reducerLegacyUnfencedCompletedRecord(t), Finalize{
		Ref:    reducerRef(),
		Intent: TerminalIntent{Outcome: OutcomeCompleted, Cause: CauseCompletedNormally},
	})
	if err := ValidateSafetyRecord(legacy); err != nil {
		t.Fatalf("valid legacy-unfenced terminal record rejected: %v", err)
	}
	for _, mode := range []Mode{ModeIdentifiedFenced, ModeLegacyFenced} {
		t.Run(mode.String()+"_legacy_proof_rejected", func(t *testing.T) {
			record := legacy
			record.Mode = mode
			if err := ValidateSafetyRecord(record); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("%s record with legacy-unfenced proof error = %v, want ErrInvalidValue", mode, err)
			}
		})
	}
}

func TestValidateSafetyRecordValidatesOptionalWorkspaceLayoutKey(t *testing.T) {
	record := validSafetyRecord()
	record.WorkspaceLayoutKey = WorkspaceKey(strings.Repeat("a", 64))
	if err := ValidateSafetyRecord(record); err != nil {
		t.Fatalf("valid workspace layout key rejected: %v", err)
	}

	record.WorkspaceLayoutKey = "workspace-identified"
	if err := ValidateSafetyRecord(record); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("invalid workspace layout key error = %v, want ErrInvalidValue", err)
	}
}

func validSafetyRecord() SafetyRecord {
	boot := BootRef{BootID: "boot-1", OwnerID: "owner-1"}
	attempt := AttemptRef{JobID: "job-0001", AttemptID: "attempt-1", Epoch: 1}
	group := GroupRef{
		Version:           1,
		CustodyID:         "custody-1",
		Launch:            LaunchKey{Attempt: attempt, Ordinal: LaunchOrdinalOne},
		HostBootID:        "host-boot-1",
		PIDNamespaceState: PIDNamespaceNotApplicable,
		PGID:              10,
		Leader:            ProcessIdentity{PID: 10, HighResStartToken: "leader-start-10"},
		Monitor:           ProcessIdentity{PID: 12, HighResStartToken: "monitor-start-10"},
	}
	grant := LaunchGrant{
		Attempt:   attempt,
		Ordinal:   LaunchOrdinalOne,
		Nonce:     "nonce-1",
		GrantedBy: boot,
	}
	return SafetyRecord{
		SchemaVersion: 1,
		Revision:      1,
		JobID:         "job-0001",
		RequestKey:    RequestKey{WorkspaceKey: "workspace/a", RequestID: "req-1"},
		TaskIdentity:  NewSHA256TaskIdentity([]byte("canonical task")),
		Mode:          ModeIdentifiedFenced,
		AdmittedBy:    boot,
		Attempt: AttemptProof{
			Ref: attempt,
			Launches: LaunchSlots[LaunchProof]{First: &LaunchProof{
				Ordinal: LaunchOrdinalOne,
				Group:   &group,
				Grant:   &grant,
			}},
		},
	}
}

func mustEvidence(t *testing.T, kind, detail string) Evidence {
	t.Helper()
	evidence, err := NewEvidence(kind, detail)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func mustChild(t *testing.T) ChildIdentity {
	t.Helper()
	child, err := NewChildIdentity(21, "child-start-21")
	if err != nil {
		t.Fatal(err)
	}
	return child
}
