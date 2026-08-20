package agentbusserve

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/service"
)

// Config is the version-3 service configuration. agentbusserve owns only the
// foreground-process/readiness bridge; service owns daemon behavior.
type Config = service.Config

var ErrShutdownDeadlineExceeded = service.ErrShutdownDeadlineExceeded

type serviceServer interface {
	ServeWithStartupContext(context.Context, context.Context) error
	Shutdown(context.Context) error
	ShutdownTimeout() time.Duration
}

var canonicalStateRootFunc = daemonlaunch.CanonicalStateRoot
var newServiceServer = func(cfg service.Config) (serviceServer, error) {
	return service.New(cfg)
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

// Serve runs the version-3 service while preserving the launcher's readiness
// FD protocol. The readiness frame is published only after service has bound
// its socket and completed restart reconciliation.
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

	if hasReporter {
		previousHook := cfg.ReadyHook
		cfg.ReadyHook = func(info service.ServeReadyInfo) error {
			if err := startupCtx.Err(); err != nil {
				return err
			}
			if previousHook != nil {
				if err := previousHook(info); err != nil {
					return err
				}
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

	server, err := newServiceServer(cfg)
	if err != nil {
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
		if shutdownErr != nil && !errors.Is(shutdownErr, service.ErrShutdownNotServing) {
			reportStartupFailure(reporter, shutdownErr)
			return shutdownErr
		}
		if shutdownErr != nil && errors.Is(shutdownErr, service.ErrShutdownNotServing) && cleanServeTerminationAfterCancel(ctx, serveErr) {
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
		(err == nil || errors.Is(err, context.Canceled) || errors.Is(err, service.ErrShutdownNotServing))
}

func cancellationContainsBootstrapFailure(err error) bool {
	if err == nil {
		return false
	}
	var startup *daemonlaunch.StartupError
	var alreadyListening service.DaemonAlreadyListeningError
	return errors.As(err, &startup) ||
		errors.As(err, &alreadyListening) ||
		errors.Is(err, service.ErrDaemonAlreadyListening)
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
	if errors.Is(err, service.ErrDaemonAlreadyListening) {
		return daemonlaunch.CodeAlreadyListening
	}
	return "error"
}
