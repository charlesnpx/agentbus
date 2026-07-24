package agentbusserve

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
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
var canonicalStateRootFunc = daemonlaunch.CanonicalStateRoot

var newProductionServerAfterStrictAdmissionSupportPreflight = func(ctx context.Context, cfg served.Config) (servedServer, error) {
	return served.NewAfterStrictAdmissionSupportPreflight(ctx, cfg)
}

type readinessPublicationGuard struct {
	mu         sync.Mutex
	terminated error
}

func (guard *readinessPublicationGuard) Terminate(err error) {
	if err == nil {
		err = context.Canceled
	}
	guard.mu.Lock()
	if guard.terminated == nil {
		guard.terminated = err
	}
	guard.mu.Unlock()
}

func (guard *readinessPublicationGuard) Ready(ctx context.Context, reporter *daemonlaunch.Reporter, canonicalRoot, socketPath string) error {
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if guard.terminated != nil {
		return guard.terminated
	}
	if err := ctx.Err(); err != nil {
		guard.terminated = err
		return err
	}
	return reporter.Ready(canonicalRoot, socketPath)
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
	readinessGuard := &readinessPublicationGuard{}
	stopReadinessCancel := func() bool { return true }
	if hasReporter {
		stopReadinessCancel = context.AfterFunc(ctx, func() {
			readinessGuard.Terminate(ctx.Err())
		})
		defer stopReadinessCancel()
	}
	startupCtx := ctx
	cancelStartup := func() {}
	if hasReporter {
		var startupErr error
		startupCtx, cancelStartup, startupErr = daemonlaunch.InheritedStartupContext(ctx)
		if startupErr != nil {
			if cleanServeTerminationAfterCancel(ctx, startupErr) {
				return nil
			}
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
		if cleanServeTerminationAfterCancel(ctx, err) {
			return nil
		}
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
			canonicalRoot, err := canonicalStateRootFunc(info.StateRoot)
			if err != nil {
				return err
			}
			if err := readinessGuard.Ready(startupCtx, reporter, canonicalRoot, info.SocketPath); err != nil {
				return err
			}
			redirectStderrToDevNull()
			return nil
		}
	}
	server, err := newProductionServerAfterStrictAdmissionSupportPreflight(startupCtx, servedCfg)
	if err != nil {
		_ = servedCfg.Runtime.Close()
		if cleanServeTerminationAfterCancel(ctx, err) {
			return nil
		}
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
		if cleanServeTerminationAfterCancel(ctx, err) {
			return nil
		}
		if err != nil {
			reportStartupFailure(reporter, err)
		}
		return err
	case <-ctx.Done():
		readinessGuard.Terminate(ctx.Err())
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
			reportStartupFailure(reporter, shutdownErr)
			return shutdownErr
		}
		if shutdownErr != nil && errors.Is(shutdownErr, served.ErrShutdownNotServing) && cleanServeTerminationAfterCancel(ctx, serveErr) {
			return nil
		}
		if cleanServeTerminationAfterCancel(ctx, serveErr) {
			return nil
		}
		if serveErr != nil {
			reportStartupFailure(reporter, serveErr)
		}
		return serveErr
	}
}

func cleanServeTerminationAfterCancel(ctx context.Context, err error) bool {
	return ctx != nil &&
		ctx.Err() != nil &&
		!cancellationContainsBootstrapFailure(err) &&
		(errors.Is(err, context.Canceled) || errors.Is(err, served.ErrShutdownNotServing))
}

func cancellationContainsBootstrapFailure(err error) bool {
	if err == nil {
		return false
	}
	var startup *daemonlaunch.StartupError
	var safety served.SafetyFailStopError
	var diagnostic served.AdmissionSupportDiagnostic
	var alreadyListening served.DaemonAlreadyListeningError
	var rootBusy served.AdmissionRootBusyError
	var rootMissing served.AdmissionRootMissingError
	var rootIdentity served.AdmissionRootIdentityMismatchError
	var rootSchema served.AdmissionRootIncompatibleSchemaError
	var rootAnchor served.AdmissionRootAnchorError
	switch {
	case errors.As(err, &startup):
		return true
	case errors.As(err, &safety), errors.Is(err, served.ErrSafetyFailStopped):
		return true
	case errors.As(err, &diagnostic), errors.Is(err, served.ErrAdmissionStrictSupportUnavailable):
		return true
	case errors.As(err, &alreadyListening), errors.Is(err, served.ErrDaemonAlreadyListening):
		return true
	case errors.As(err, &rootBusy), errors.Is(err, served.ErrAdmissionRootBusy):
		return true
	case errors.As(err, &rootMissing), errors.Is(err, served.ErrAdmissionRootMissing):
		return true
	case errors.As(err, &rootIdentity), errors.As(err, &rootSchema), errors.As(err, &rootAnchor):
		return true
	case errors.Is(err, served.ErrRuntimeConsumed):
		return true
	case errors.Is(err, authority.ErrRootSealed),
		errors.Is(err, authority.ErrAdmissionContractMismatch),
		errors.Is(err, authority.ErrAnchorInvariant),
		errors.Is(err, authority.ErrFailStopped),
		errors.Is(err, authority.ErrFailStopRecord),
		errors.Is(err, authority.ErrRecoveryNeeded):
		return true
	case errors.Is(err, repository.ErrInvalidRecord),
		errors.Is(err, repository.ErrCorruptRecord),
		errors.Is(err, repository.ErrProjectionMismatch),
		errors.Is(err, repository.ErrAmbiguousCommit),
		errors.Is(err, repository.ErrDefinitelyNotCommitted):
		return true
	default:
		return false
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
