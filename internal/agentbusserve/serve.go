package agentbusserve

import (
	"context"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/served"
)

type Config struct {
	StateRoot    string
	CWD          string
	Backends     []engine.Backend
	Registry     *engine.PolicyRegistry
	Clock        engine.Clock
	ProcessTable engine.ProcessTable
}

func Serve(ctx context.Context, cfg Config) error {
	server, err := served.New(served.Config{
		StateRoot:    cfg.StateRoot,
		CWD:          cfg.CWD,
		Backends:     cfg.Backends,
		Registry:     cfg.Registry,
		Clock:        cfg.Clock,
		ProcessTable: cfg.ProcessTable,
	})
	if err != nil {
		return err
	}
	return server.Serve(ctx)
}
