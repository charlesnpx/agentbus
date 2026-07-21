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
	// obligation is durable and response loss while the daemon lives never
	// cancels it: the job runs exactly once, and replay returns the same job.
	// A daemon crash after durable acceptance but before launch authorization is
	// a distinct pre-authorization window. Startup recovery finalizes that job
	// failed-with-cause deterministically under at-most-once semantics, and
	// replay returns that terminal job: never a silent dangling acceptance,
	// never a double execution. A GRACEFUL Serve exit racing an acceptance is
	// equivalent to the daemon-crash windows generally: recovery finalizes by
	// recorded progress, with pre-authorization acceptances failed before
	// authorization and already-authorized launches reaped after authorization.
	// In every case at-most-once holds and replay returns the terminal job.
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
