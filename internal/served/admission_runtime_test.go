package served

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestServedAdmissionSupervisorUsesUnavailableCustodian(t *testing.T) {
	ctx := context.Background()
	supervisor := newServedAdmissionSupervisor(nil)
	support := supervisor.runtime.Support()
	if support.ParkedExec || support.VerifiedContainment || !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %#v, want parked_exec:false verified_containment:false supervisor_unavailable", support)
	}
	if err := supervisor.verifiedContainmentSupported(ctx); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("verifiedContainmentSupported error = %v, want supervisor_unavailable", err)
	}
	jobID := model.JobID("job-served-unavailable")
	ref := model.AttemptRef{JobID: jobID, AttemptID: "attempt-served-unavailable", Epoch: 1}
	plan := coordinator.LaunchPlan{JobID: jobID, Ref: ref, Ordinal: model.LaunchOrdinalOne}
	grant := model.LaunchGrant{Attempt: ref, Ordinal: model.LaunchOrdinalOne, Nonce: "nonce-served-unavailable", GrantedBy: model.BootRef{BootID: "boot-served-unavailable", OwnerID: "owner-served-unavailable"}}
	prepared := coordinator.PreparedSupervisor{Ref: ref, Ordinal: model.LaunchOrdinalOne}
	released := model.LaunchReleaseFact{Attempt: ref, Ordinal: model.LaunchOrdinalOne}

	if err := supervisor.Register(jobID, newFakeBackend("fake"), engine.SessionOpts{CWD: t.TempDir()}); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("Register error = %v, want supervisor_unavailable", err)
	}
	if _, err := supervisor.Prepare(ctx, plan); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("Prepare error = %v, want supervisor_unavailable", err)
	}
	if err := supervisor.SendPermit(ctx, prepared, grant); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("SendPermit error = %v, want supervisor_unavailable", err)
	}
	if _, err := supervisor.ObserveLaunch(ctx, prepared, grant); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("ObserveLaunch error = %v, want supervisor_unavailable", err)
	}
	if session, id, err := supervisor.Started(jobID, model.LaunchOrdinalOne); !errors.Is(err, custodian.ErrSupervisorUnavailable) || session != nil || id != "" {
		t.Fatalf("Started = session:%v id:%q err:%v, want unavailable zero values", session, id, err)
	}
	if verified, err := supervisor.VerifyQuiescence(ctx, prepared, released); !errors.Is(err, custodian.ErrSupervisorUnavailable) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("VerifyQuiescence = verified:%#v err:%v, want unavailable zero attestation", verified, err)
	}
	if verified, err := supervisor.Contain(ctx, prepared); !errors.Is(err, custodian.ErrSupervisorUnavailable) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("Contain = verified:%#v err:%v, want unavailable zero attestation", verified, err)
	}
	if verified, err := supervisor.Retire(ctx, prepared); !errors.Is(err, custodian.ErrSupervisorUnavailable) || verified != (custodian.VerifiedQuiescence{}) {
		t.Fatalf("Retire = verified:%#v err:%v, want unavailable zero attestation", verified, err)
	}
}

func TestServedAdmissionSupervisorRequiresAdvertisedRuntime(t *testing.T) {
	ctx := context.Background()
	support, err := custodian.NewSupport(custodian.Support{
		ParkedExec:             true,
		VerifiedContainment:    true,
		ImplementationCompiled: true,
		RuntimeProbePassed:     true,
		FeatureConfigured:      true,
		FeatureAdvertised:      false,
	})
	if err != nil {
		t.Fatalf("NewSupport() error = %v", err)
	}
	if support.AdvertisedAvailable() {
		t.Fatal("test support unexpectedly advertised available")
	}
	_, verifier := custodian.NewAttestationChannel()
	supervisor := &servedAdmissionSupervisor{
		runtime: fakeAdmissionRuntime{support: support, verifier: verifier},
	}

	if err := supervisor.verifiedContainmentSupported(ctx); !errors.Is(err, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("verifiedContainmentSupported error = %v, want supervisor_unavailable", err)
	}
}

type fakeAdmissionRuntime struct {
	support  custodian.Support
	verifier custodian.AttestationVerifier
}

func (r fakeAdmissionRuntime) Support() custodian.Support {
	return r.support
}

func (r fakeAdmissionRuntime) Verifier() custodian.AttestationVerifier {
	return r.verifier
}
