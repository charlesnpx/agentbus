package coordinator

import (
	"context"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type Supervisor interface {
	Prepare(context.Context, LaunchPlan) (PreparedSupervisor, error)
	SendPermit(context.Context, PreparedSupervisor, model.LaunchGrant) error
	ObserveLaunch(context.Context, PreparedSupervisor, model.LaunchGrant) (LaunchObservation, error)
	VerifyQuiescence(context.Context, PreparedSupervisor, model.LaunchConsumed) (model.QuiescenceReceipt, error)
	Contain(context.Context, PreparedSupervisor) (model.ContainmentReceipt, error)
	Retire(context.Context, PreparedSupervisor) (model.RetirementReceipt, error)
}

type LaunchPlan struct {
	JobID        model.JobID
	Ref          model.AttemptRef
	RequestKey   model.RequestKey
	TaskIdentity model.TaskIdentity
	SessionID    string
}

type PreparedSupervisor struct {
	Ref      model.AttemptRef
	Identity model.SupervisorIdentity
}

func (prepared PreparedSupervisor) ValidateFor(ref model.AttemptRef) error {
	if !prepared.Ref.Equal(ref) {
		return fmt.Errorf("prepared supervisor attempt mismatch")
	}
	if err := prepared.Identity.Validate(); err != nil {
		return fmt.Errorf("prepared supervisor identity: %w", err)
	}
	return nil
}

type LaunchObservation struct {
	Ordinal  model.LaunchOrdinal
	Child    model.ChildIdentity
	Evidence model.Evidence
}

func (observation LaunchObservation) ValidateFor(grant model.LaunchGrant) error {
	if observation.Ordinal != grant.Ordinal {
		return fmt.Errorf("launch observation ordinal mismatch")
	}
	if err := observation.Child.Validate(); err != nil {
		return fmt.Errorf("launch observation child: %w", err)
	}
	if observation.Evidence.Present() {
		if err := observation.Evidence.Validate(); err != nil {
			return fmt.Errorf("launch observation evidence: %w", err)
		}
	}
	return nil
}
