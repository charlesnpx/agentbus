package agentbusserve

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/served"
)

type Config = served.Config
type StrictAdmissionOptions = served.StrictAdmissionOptions

var ErrShutdownDeadlineExceeded = served.ErrShutdownDeadlineExceeded

type servedServer interface {
	ServeWithStartupContext(context.Context, context.Context) error
	Shutdown(context.Context) error
	ShutdownTimeout() time.Duration
}

var productionServedConfigFunc = productionServedConfig

var newProductionServerAfterStrictAdmissionSupportPreflight = func(ctx context.Context, cfg served.Config) (servedServer, error) {
	return served.NewAfterStrictAdmissionSupportPreflight(ctx, cfg)
}

func Serve(ctx context.Context, cfg Config) error {
	if ctx == nil {
		ctx = context.Background()
	}
	reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
	if err != nil {
		return err
	}
	if hasReporter {
		defer reporter.Close()
	}
	startupCtx := ctx
	cancelStartup := func() {}
	if hasReporter {
		var startupErr error
		startupCtx, cancelStartup, startupErr = daemonlaunch.InheritedStartupContext(ctx)
		if startupErr != nil {
			reportStartupFailure(reporter, startupErr)
			return startupErr
		}
	}
	defer cancelStartup()
	servedCfg, configErr := productionServedConfigFunc(cfg)
	if configErr != nil {
		err := configErr
		if preflightErr := served.StrictAdmissionSupportPreflight(startupCtx, servedCfg); preflightErr != nil {
			err = errors.Join(preflightErr, configErr)
		}
		_ = servedCfg.Runtime.Close()
		reportStartupFailure(reporter, err)
		return err
	}
	if hasReporter {
		previousHook := servedCfg.ReadyHook
		servedCfg.ReadyHook = func(info served.ServeReadyInfo) error {
			if err := startupCtx.Err(); err != nil {
				return err
			}
			if previousHook != nil {
				if err := previousHook(info); err != nil {
					return err
				}
			}
			if err := startupCtx.Err(); err != nil {
				return err
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
	server, err := newProductionServerAfterStrictAdmissionSupportPreflight(startupCtx, servedCfg)
	if err != nil {
		_ = servedCfg.Runtime.Close()
		reportStartupFailure(reporter, err)
		return err
	}
	serviceCtx, stopService := context.WithCancel(context.WithoutCancel(ctx))
	defer stopService()
	done := make(chan error, 1)
	go func() {
		done <- server.ServeWithStartupContext(serviceCtx, startupCtx)
	}()
	select {
	case err = <-done:
		if err != nil {
			reportStartupFailure(reporter, err)
		}
		return err
	case <-ctx.Done():
		shutdownCtx := context.WithoutCancel(ctx)
		var cancelShutdown context.CancelFunc
		if timeout := server.ShutdownTimeout(); timeout > 0 {
			shutdownCtx, cancelShutdown = context.WithTimeout(shutdownCtx, timeout)
		} else {
			shutdownCtx, cancelShutdown = context.WithCancel(shutdownCtx)
		}
		shutdownErr := server.Shutdown(shutdownCtx)
		cancelShutdown()
		stopService()
		serveErr := <-done
		if shutdownErr != nil && !errors.Is(shutdownErr, served.ErrShutdownNotServing) {
			return shutdownErr
		}
		if shutdownErr != nil && errors.Is(shutdownErr, served.ErrShutdownNotServing) && ctx.Err() != nil && serveErr == context.Canceled {
			return nil
		}
		return serveErr
	}
}

func RecoverAdmissionRoot(ctx context.Context, cfg Config) (served.AdmissionRecoveryReport, error) {
	servedCfg, configErr := recoveryServedConfig(cfg)
	if configErr != nil {
		err := configErr
		if preflightErr := served.StrictAdmissionSupportPreflight(ctx, servedCfg); preflightErr != nil {
			err = errors.Join(preflightErr, configErr)
		}
		_ = servedCfg.Runtime.Close()
		return served.AdmissionRecoveryReport{}, err
	}
	report, err := served.RecoverAdmissionRoot(ctx, servedCfg)
	if err != nil {
		_ = servedCfg.Runtime.Close()
		return report, err
	}
	return report, nil
}

func productionServedConfig(cfg Config) (served.Config, error) {
	return strictAdmissionServedConfig(cfg, StrictAdmissionOptions{})
}

func recoveryServedConfig(cfg Config) (served.Config, error) {
	runtime, err := newRecoveryStrictAdmissionRuntime(StrictAdmissionOptions{})
	cfg.Runtime = runtime
	return cfg, err
}

var newRecoveryStrictAdmissionRuntime = served.NewStrictAdmissionRuntime

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
	case errors.Is(err, served.ErrAdmissionRootBusy):
		return daemonlaunch.CodeAdmissionRootBusy
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
