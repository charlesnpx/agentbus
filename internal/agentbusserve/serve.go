package agentbusserve

import (
	"context"

	"github.com/charlesnpx/agentbus/internal/served"
)

type Config = served.Config
type StrictAdmissionOptions = served.StrictAdmissionOptions

func Serve(ctx context.Context, cfg Config) error {
	server, err := served.New(productionServedConfig(cfg))
	if err != nil {
		return err
	}
	return server.Serve(ctx)
}

func RecoverAdmissionRoot(ctx context.Context, cfg Config) (served.AdmissionRecoveryReport, error) {
	return served.RecoverAdmissionRoot(ctx, productionServedConfig(cfg))
}

func productionServedConfig(cfg Config) served.Config {
	strictCfg, _ := strictAdmissionServedConfig(cfg, StrictAdmissionOptions{})
	return strictCfg
}

func strictAdmissionServedConfig(cfg Config, opts StrictAdmissionOptions) (served.Config, error) {
	return served.StrictAdmissionConfig(cfg, opts)
}
