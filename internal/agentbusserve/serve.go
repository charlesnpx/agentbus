package agentbusserve

import (
	"context"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/served"
)

type Config = served.Config

func Serve(ctx context.Context, cfg Config) error {
	server, err := served.New(productionServedConfig(cfg))
	if err != nil {
		return err
	}
	return server.Serve(ctx)
}

func productionServedConfig(cfg Config) served.Config {
	cfg.Runtime = custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable)
	return cfg
}
