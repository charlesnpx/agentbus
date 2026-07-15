package served

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestServedAdmissionSupervisorStartsSeparateSessionPerOrdinal(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend("fake")
	supervisor := newServedAdmissionSupervisor(nil)
	jobID := model.JobID("job-served-ordinal")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-served-ordinal", Epoch: 1}
	if err := supervisor.Register(jobID, backend, engine.SessionOpts{CWD: t.TempDir()}); err != nil {
		t.Fatal(err)
	}

	first := observeServedLaunchOrdinal(t, ctx, supervisor, jobID, ref, model.LaunchOrdinalOne)
	second := observeServedLaunchOrdinal(t, ctx, supervisor, jobID, ref, model.LaunchOrdinalTwo)

	if got := backend.count.Load(); got != 2 {
		t.Fatalf("backend starts = %d, want 2", got)
	}
	if first.ID() == second.ID() {
		t.Fatalf("session id reused across launch ordinals: %s", first.ID())
	}
}

func TestServedAdmissionSupervisorRecoveryUsesGroupRefDecisionWithoutMinting(t *testing.T) {
	ctx := context.Background()
	backend := newFakeBackend("fake")
	supervisor := newServedAdmissionSupervisor(nil)
	jobID := model.JobID("job-served-recovery")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-served-recovery", Epoch: 1}
	if err := supervisor.Register(jobID, backend, engine.SessionOpts{CWD: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	prepared, err := supervisor.Prepare(ctx, coordinator.LaunchPlan{JobID: jobID, Ref: ref, Ordinal: model.LaunchOrdinalOne})
	if err != nil {
		t.Fatal(err)
	}

	if verified, err := supervisor.Retire(ctx, prepared); !errors.Is(err, custodian.ErrSupervisorUnavailable) || !strings.Contains(err.Error(), string(model.GroupRecoveryUnprovable)) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("same-boot Retire = verified:%#v err:%v, want unavailable recovery_unprovable with zero attestation", verified, err)
	}

	priorBoot := prepared
	priorBoot.Group.HostBootID = "prior-host-boot"
	if verified, err := supervisor.Contain(ctx, priorBoot); !errors.Is(err, custodian.ErrSupervisorUnavailable) || !strings.Contains(err.Error(), string(model.GroupRecoveryQuiescent)) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("different-boot Contain = verified:%#v err:%v, want unavailable quiescent with zero attestation", verified, err)
	}
}

func observeServedLaunchOrdinal(t *testing.T, ctx context.Context, supervisor *servedAdmissionSupervisor, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal) engine.Session {
	t.Helper()
	plan := coordinator.LaunchPlan{JobID: jobID, Ref: ref, Ordinal: ordinal}
	prepared, err := supervisor.Prepare(ctx, plan)
	if err != nil {
		t.Fatalf("Prepare ordinal %s: %v", ordinal, err)
	}
	grant := model.LaunchGrant{
		Attempt:   ref,
		Ordinal:   ordinal,
		Nonce:     model.LaunchNonce("nonce-" + ordinal.String()),
		GrantedBy: model.BootRef{BootID: "boot-served-ordinal", OwnerID: "owner-served-ordinal"},
	}
	if err := supervisor.SendPermit(ctx, prepared, grant); err != nil {
		t.Fatalf("SendPermit ordinal %s: %v", ordinal, err)
	}
	if _, err := supervisor.ObserveLaunch(ctx, prepared, grant); err != nil {
		t.Fatalf("ObserveLaunch ordinal %s: %v", ordinal, err)
	}
	session, _, err := supervisor.Started(jobID, ordinal)
	if err != nil {
		t.Fatalf("Started ordinal %s: %v", ordinal, err)
	}
	return session
}
