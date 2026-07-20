package served

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestServedAdmissionRuntimeUsesUnavailableCustodian(t *testing.T) {
	ctx := context.Background()
	runtime := newServedAdmissionRuntime(nil)
	support := runtime.runtime.Support()
	if support.ParkedExec || support.VerifiedContainment || !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %#v, want parked_exec:false verified_containment:false supervisor_unavailable", support)
	}
	if err := runtime.verifiedContainmentSupported(ctx); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("verifiedContainmentSupported error = %v, want supervisor_unavailable", err)
	}
	jobID := model.JobID("job-served-unavailable")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-served-unavailable", Epoch: 1}
	launchPort := runtime.launchPort()
	if _, err := launchPort.Prepare(ctx, command.ExecSpec{Argv: []string{"/bin/fake-agent"}}, model.LaunchKey{Attempt: ref, Ordinal: model.LaunchOrdinalOne}); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("launchPort Prepare error = %v, want supervisor_unavailable", err)
	}
	if verified, err := launchPort.ContainAndVerify(ctx, model.GroupRef{}, custodian.QuiescenceCauseContain); !errors.Is(err, custodian.ErrSupervisorUnavailable) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("launchPort ContainAndVerify = verified:%#v err:%v, want unavailable zero attestation", verified, err)
	}
	if runtime.hasActiveCustodies() {
		t.Fatal("hasActiveCustodies = true for unavailable runtime")
	}
}

func TestServedAdmissionRuntimeUsesInjectedRuntime(t *testing.T) {
	reason := errors.New("configured runtime unavailable")
	server := &Server{admissionRuntimeConfig: custodian.NewUnavailableRuntime(reason)}

	runtime := newServedAdmissionRuntime(server)
	if support := runtime.support(); !errors.Is(support.Reason, reason) {
		t.Fatalf("runtime support reason = %v, want injected reason", support.Reason)
	}
}

func TestServedAdmissionRuntimeNilConfigFailsClosed(t *testing.T) {
	server := &Server{}

	runtime := newServedAdmissionRuntime(server)
	if support := runtime.support(); !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) || support.VerifiedContainment || support.ParkedExec {
		t.Fatalf("runtime support = %+v, want fail-closed unavailable runtime", support)
	}
}
