package agentbusserve

import (
	"context"
	"errors"
	"os"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/served"
)

type Config = served.Config
type StrictAdmissionOptions = served.StrictAdmissionOptions

func Serve(ctx context.Context, cfg Config) error {
	reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
	if err != nil {
		return err
	}
	if hasReporter {
		defer reporter.Close()
	}
	servedCfg, configErr := productionServedConfig(cfg)
	if configErr != nil {
		err := configErr
		if preflightErr := served.StrictAdmissionSupportPreflight(ctx, servedCfg); preflightErr != nil {
			err = errors.Join(preflightErr, configErr)
		}
		_ = servedCfg.Runtime.Close()
		reportStartupFailure(reporter, err)
		return err
	}
	if hasReporter {
		previousHook := servedCfg.ReadyHook
		servedCfg.ReadyHook = func(info served.ServeReadyInfo) error {
			if previousHook != nil {
				if err := previousHook(info); err != nil {
					return err
				}
			}
			canonicalRoot, err := daemonlaunch.CanonicalStateRoot(info.StateRoot)
			if err != nil {
				return err
			}
			if err := reporter.Ready(canonicalRoot, info.SocketPath); err != nil {
				return err
			}
			redirectStderrToDevNull()
			return nil
		}
	}
	server, err := served.NewAfterStrictAdmissionSupportPreflight(ctx, servedCfg)
	if err != nil {
		_ = servedCfg.Runtime.Close()
		reportStartupFailure(reporter, err)
		return err
	}
	err = server.Serve(ctx)
	if err != nil {
		reportStartupFailure(reporter, err)
	}
	return err
}

func RecoverAdmissionRoot(ctx context.Context, cfg Config) (served.AdmissionRecoveryReport, error) {
	servedCfg, err := productionServedConfig(cfg)
	if err != nil {
		return served.AdmissionRecoveryReport{}, err
	}
	return served.RecoverAdmissionRoot(ctx, servedCfg)
}

func productionServedConfig(cfg Config) (served.Config, error) {
	return strictAdmissionServedConfig(cfg, StrictAdmissionOptions{})
}

func strictAdmissionServedConfig(cfg Config, opts StrictAdmissionOptions) (served.Config, error) {
	return served.StrictAdmissionConfig(cfg, opts)
}

func redirectStderrToDevNull() {
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return
	}
	defer devNull.Close()
	_ = dupToStderr(devNull)
}

func reportStartupFailure(reporter *daemonlaunch.Reporter, err error) {
	if reporter == nil || reporter.Sent() || err == nil {
		return
	}
	_ = reporter.Failed(startupFailureCode(err), err.Error())
}

func startupFailureCode(err error) string {
	if err == nil {
		return "error"
	}
	var diagnostic served.AdmissionSupportDiagnostic
	switch {
	case errors.As(err, &diagnostic), errors.Is(err, served.ErrAdmissionStrictSupportUnavailable):
		return served.ErrAdmissionStrictSupportUnavailable.Error()
	case errors.Is(err, served.ErrDaemonAlreadyListening):
		return daemonlaunch.CodeAlreadyListening
	case errors.Is(err, served.ErrRuntimeConsumed):
		return served.ErrRuntimeConsumed.Error()
	case errors.Is(err, served.ErrSafetyFailStopped):
		return served.ErrSafetyFailStopped.Error()
	case errors.Is(err, authority.ErrRootSealed):
		return authority.ErrRootSealed.Error()
	case errors.Is(err, authority.ErrAdmissionContractMismatch):
		return authority.ErrAdmissionContractMismatch.Error()
	case errors.Is(err, authority.ErrAnchorInvariant):
		return authority.ErrAnchorInvariant.Error()
	case errors.Is(err, authority.ErrFailStopped):
		return authority.ErrFailStopped.Error()
	case errors.Is(err, authority.ErrFailStopRecord):
		return authority.ErrFailStopRecord.Error()
	case errors.Is(err, authority.ErrRecoveryNeeded):
		return authority.ErrRecoveryNeeded.Error()
	default:
		return "error"
	}
}
