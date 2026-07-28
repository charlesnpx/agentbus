package coordinator

import (
	"context"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type ResultPublisher interface {
	Publish(context.Context, model.JobID, []byte) (model.ResultReceipt, error)
	Verify(context.Context, model.ResultRef) (model.ResultReceipt, error)
}
