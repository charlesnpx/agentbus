package authority

import (
	"context"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// SubmissionCoordinator is the pre-launch admission contract above the launch
// controller. It owns the response-boundary decision for each submission mode;
// a runner inside Turn is too late for LegacyFenced because the parked launch
// must be prepared before the RPC acceptance response is written.
type SubmissionCoordinator interface {
	// SubmitIdentified accepts an identified fenced request. Once accepted, the
	// obligation is durable and response loss is recovered by replaying the same
	// request key.
	SubmitIdentified(context.Context, AcceptRequest) (AcceptResult, error)

	// PrepareLegacyFenced prepares the fenced legacy launch before the response
	// acknowledgement. Response success may later acknowledge/grant/release; a
	// response write failure rejects and retires without granting.
	PrepareLegacyFenced(context.Context, AcceptRequest) (LegacyFencedPreparation, error)
}

// LegacyFencedPreparation captures the contract fact that LegacyFenced launch
// parking precedes the acceptance response; S4C wires the concrete controller.
type LegacyFencedPreparation struct {
	Admission AcceptResult
	Ordinal   model.LaunchOrdinal
	Group     model.GroupRef
}
