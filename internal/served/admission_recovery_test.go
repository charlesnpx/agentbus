package served

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestAdmissionRecoveryReportJSONIncludesADR13Counters(t *testing.T) {
	report := AdmissionRecoveryReport{
		Mode:               AdmissionRecoveryOnly.String(),
		WorkItems:          1,
		QuiescedLaunches:   2,
		FinalizedJobs:      3,
		OrphanedJobs:       4,
		UnresolvedLaunches: 5,
		CleanupWarnings:    0,
		RecoveryPasses:     6,
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"orphanedJobs", "unresolvedLaunches", "cleanupWarnings"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("recovery report JSON = %s, missing %s", raw, field)
		}
	}
}

func TestAdmissionRecoveryRejectsContradictoryGroupRefAsFatalIntegrity(t *testing.T) {
	base := admissionRecoveryTestWorkItem()
	tests := []struct {
		name   string
		mutate func(*model.GroupRef)
	}{
		{
			name: "known retained domain without retained id",
			mutate: func(group *model.GroupRef) {
				group.RetainedID = ""
				group.RetainedDomainID = "retained-domain-recovery"
				group.RetainedDomainState = model.RetainedDomainKnown
			},
		},
		{
			name: "retained id without retained domain",
			mutate: func(group *model.GroupRef) {
				group.RetainedID = "retained-recovery"
				group.RetainedDomainID = ""
				group.RetainedDomainState = model.RetainedDomainNotApplicable
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := base
			item.Launches = append([]model.RecoveryLaunch(nil), base.Launches...)
			tt.mutate(&item.Launches[0].Group)
			latch := NewSafetyLatch()
			launchPort := &admissionRecoveryRejectingLaunchPort{}

			report, err := newAdmissionRecoveryExecutor(&contradictionRecoveryFakeSession{items: []model.RecoveryWorkItem{item}}, launchPort, latch).RecoverReport(context.Background())
			if err == nil {
				t.Fatal("RecoverReport() error = nil, want fatal recovery error")
			}
			if !errors.Is(err, ErrSafetyFailStopped) {
				t.Fatalf("RecoverReport() error = %v, want ErrSafetyFailStopped", err)
			}
			if !errors.Is(err, authority.ErrRecoveryNeeded) {
				t.Fatalf("RecoverReport() error = %v, want ErrRecoveryNeeded", err)
			}
			if !errors.Is(err, model.ErrInvalidValue) {
				t.Fatalf("RecoverReport() error = %v, want ErrInvalidValue", err)
			}
			if errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
				t.Fatalf("RecoverReport() error = %v, want fatal integrity not retained reacquire unresolved", err)
			}
			if report.WorkItems != 1 {
				t.Fatalf("report.WorkItems = %d, want 1", report.WorkItems)
			}
			if launchPort.containCalls != 0 {
				t.Fatalf("ContainAndVerify calls = %d, want 0", launchPort.containCalls)
			}
			select {
			case <-latch.Done():
			default:
				t.Fatal("SafetyLatch was not tripped")
			}
			if reason := latch.Reason(); !errors.Is(reason, authority.ErrRecoveryNeeded) || !errors.Is(reason, model.ErrInvalidValue) {
				t.Fatalf("SafetyLatch reason = %v, want recovery invalid value", reason)
			}
		})
	}
}

func TestAdmissionRecoveryRetainedObjectUnresolvedDoesNotTripLatch(t *testing.T) {
	ctx := context.Background()
	item := admissionRecoveryTestWorkItem()
	group := item.Launches[0].Group
	group.RetainedDomainID = "retained-domain-served-recovery"
	group.RetainedDomainState = model.RetainedDomainKnown
	group.RetainedID = "retained-served-recovery"
	item.Launches[0].Group = group

	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.containErrByOrdinal = map[model.LaunchOrdinal]error{
		model.LaunchOrdinalOne: custodian.RetainedObjectReacquireUnresolvedError{
			Group: group,
			Cause: errors.New("retained object disappeared before absence proof"),
		},
	}
	session := &admissionRecoveryFakeSession{
		items: []model.RecoveryWorkItem{item},
		finalized: model.SafetyRecord{
			JobID: item.JobID,
			Terminal: &model.TerminalCertificate{
				Outcome: model.OutcomeOrphaned,
				Proof:   model.ProofUnresolvedAbsence,
				Cause:   model.CauseDaemonRestartedAfterAuthorization,
			},
		},
	}
	latch := NewSafetyLatch()

	report, err := recoverAdmissionBeforeReadyReport(ctx, session, launcher, latch)
	if err != nil {
		t.Fatal(err)
	}
	if report.UnresolvedLaunches != 1 || report.FinalizedJobs != 1 || report.OrphanedJobs != 1 {
		t.Fatalf("recovery report = %+v, want one retained-object unresolved orphan", report)
	}
	if session.finalizeUnresolvedCalls != 1 {
		t.Fatalf("FinalizeUnresolved calls = %d, want 1", session.finalizeUnresolvedCalls)
	}
	if reason := latch.Reason(); reason != nil {
		t.Fatalf("safety latch tripped: %v", reason)
	}
}

func admissionRecoveryTestWorkItem() model.RecoveryWorkItem {
	jobID := model.JobID("job-served-recovery")
	attempt := model.AttemptRef{JobID: jobID, AttemptID: "attempt-served-recovery", Epoch: 1}
	group := model.GroupRef{
		Version:   1,
		CustodyID: "custody-served-recovery",
		Launch: model.LaunchKey{
			Attempt: attempt,
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:          "host-boot-served-recovery",
		PIDNamespaceState:   model.PIDNamespaceNotApplicable,
		RetainedDomainState: model.RetainedDomainNotApplicable,
		PGID:                4411,
		Leader:              model.ProcessIdentity{PID: 4411, HighResStartToken: "leader-start-served-recovery"},
		Monitor:             model.ProcessIdentity{PID: 4412, HighResStartToken: "monitor-start-served-recovery"},
	}
	return model.RecoveryWorkItem{
		Token: model.RecoveryToken{
			JobID:           jobID,
			BasedOnRevision: 1,
			RecoveryBoot:    model.BootRef{BootID: "boot-served-recovery", OwnerID: "owner-served-recovery"},
			Opaque:          "token-served-recovery",
		},
		JobID:           jobID,
		BasedOnRevision: 1,
		Trigger:         model.RecoveryStartupLoss,
		Launches:        []model.RecoveryLaunch{{Ordinal: model.LaunchOrdinalOne, Group: group}},
	}
}

type contradictionRecoveryFakeSession struct {
	items []model.RecoveryWorkItem
}

func (s *contradictionRecoveryFakeSession) WorkItems(context.Context) ([]model.RecoveryWorkItem, error) {
	return append([]model.RecoveryWorkItem(nil), s.items...), nil
}

func (*contradictionRecoveryFakeSession) FinalizePlanned(context.Context, model.RecoveryToken) error {
	panic("FinalizePlanned should not be called for invalid recovery work")
}

func (*contradictionRecoveryFakeSession) RecordQuiescence(context.Context, any, model.LaunchOrdinal, custodian.VerifiedQuiescence) error {
	panic("RecordQuiescence should not be called for invalid recovery work")
}

func (*contradictionRecoveryFakeSession) AdvanceRecovery(context.Context, model.RecoveryToken) (model.RecoveryWorkItem, error) {
	panic("AdvanceRecovery should not be called for invalid recovery work")
}

func (*contradictionRecoveryFakeSession) FinalizeUnresolved(context.Context, model.RecoveryToken) (model.SafetyRecord, error) {
	panic("FinalizeUnresolved should not be called: contradictory durable identity is fatal, not job-local unresolved")
}

type admissionRecoveryRejectingLaunchPort struct {
	containCalls int
}

func (*admissionRecoveryRejectingLaunchPort) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (launch.PreparedProcess, error) {
	panic("Prepare should not be called during startup recovery")
}

func (p *admissionRecoveryRejectingLaunchPort) ContainAndVerify(context.Context, model.GroupRef, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	p.containCalls++
	return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, errors.New("unexpected contain")
}
