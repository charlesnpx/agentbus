package served

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

var ErrAdmissionRootMissing = errors.New("agentbus admission root missing")

type AdmissionRootMissingError struct {
	Path  string
	Cause error
}

func (e AdmissionRootMissingError) Error() string {
	if e.Path == "" {
		return ErrAdmissionRootMissing.Error()
	}
	if e.Cause == nil {
		return fmt.Sprintf("%s: %s", ErrAdmissionRootMissing, e.Path)
	}
	return fmt.Sprintf("%s: %s: %v", ErrAdmissionRootMissing, e.Path, e.Cause)
}

func (e AdmissionRootMissingError) Is(target error) bool {
	return target == ErrAdmissionRootMissing
}

func (e AdmissionRootMissingError) Unwrap() error {
	return e.Cause
}

func RecoverAdmissionRoot(ctx context.Context, cfg Config) (AdmissionRecoveryReport, error) {
	server, err := newAdmissionRecoveryServer(cfg)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	return server.recoverAdmissionRoot(ctx)
}

func newAdmissionRecoveryServer(cfg Config) (*Server, error) {
	root, err := existingAdmissionStateRoot(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	cwd := cfg.CWD
	if cwd == "" {
		cwd, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	socketPath := cfg.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(root, protocol.SocketName)
	}
	clock := cfg.Clock
	if clock == nil {
		clock = engine.ClockFunc(time.Now)
	}
	processes := cfg.ProcessTable
	if processes == nil {
		processes = engine.NativeProcessTable{}
	}
	return &Server{
		stateRoot:                    root,
		cwd:                          cwd,
		socketPath:                   socketPath,
		tokenPath:                    filepath.Join(root, protocol.TokenFileName),
		token:                        cfg.Token,
		backends:                     map[string]engine.Backend{},
		registry:                     engine.NewPolicyRegistry(),
		clock:                        clock,
		processes:                    processes,
		processGroups:                cfg.ProcessGroups,
		cancelGrace:                  cfg.CancelGrace,
		cancelWaiter:                 cfg.CancelWaiter,
		idleTimeout:                  -1,
		idleCheckInterval:            time.Minute,
		binaryIdentityProbe:          cfg.BinaryIdentityProbe,
		inlineResultCap:              engine.DefaultInlineResultCap,
		leaseDuration:                defaultLeaseDuration,
		heartbeatInterval:            defaultHeartbeat,
		reapInterval:                 engine.DefaultReapInterval,
		gcInterval:                   engine.DefaultGCInterval,
		reapTickInterval:             engine.DefaultReapInterval,
		reapTickFactory:              newTickerSource,
		readyHook:                    cfg.ReadyHook,
		safetyLatch:                  NewSafetyLatch(),
		safetyDrainTimeout:           defaultSafetyDrain,
		stores:                       make(map[string]*engine.Store),
		storesByKey:                  make(map[string]*engine.Store),
		jobStores:                    make(map[string]*engine.Store),
		admissionJobs:                make(map[string]struct{}),
		admissionEffectMu:            make(map[string]*sync.Mutex),
		admissionRuntimeConfig:       cfg.Runtime,
		admissionProbeRunner:         cfg.ProbeRunner,
		admissionBootstrapperFactory: openExistingAdmissionBootstrapper,
		activeJobs:                   make(map[string]*activeJob),
		lastActivity:                 clock.Now().UTC(),
	}, nil
}

func existingAdmissionStateRoot(root string) (string, error) {
	var err error
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			return "", err
		}
	}
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", AdmissionRootMissingError{Path: root, Cause: err}
		}
		return "", err
	}
	if !info.IsDir() {
		return "", AdmissionRootMissingError{Path: root, Cause: fmt.Errorf("not a directory")}
	}
	return root, nil
}

func openExistingAdmissionBootstrapper(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
	repoPath := filepath.Join(s.stateRoot, admissionRepositoryFile)
	if err := requireExistingAdmissionRepositoryFile(repoPath); err != nil {
		return nil, nil, nil, err
	}
	return openAdmissionBootstrapper(ctx, s)
}

func requireExistingAdmissionRepositoryFile(repoPath string) error {
	info, err := os.Stat(repoPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return AdmissionRootMissingError{Path: repoPath, Cause: err}
		}
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: admission repository is not a regular file: %s", repository.ErrInvalidRecord, repoPath)
	}
	if info.Size() == 0 {
		return fmt.Errorf("%w: admission repository is zero-length: %s", repository.ErrInvalidRecord, repoPath)
	}
	return nil
}

func (server *Server) recoverAdmissionRoot(ctx context.Context) (AdmissionRecoveryReport, error) {
	if server == nil {
		return AdmissionRecoveryReport{}, authority.ErrNotReady
	}
	server.ensureSafetyLatch()
	server.admissionStateMu.Lock()
	runtime := server.admissionRuntime
	if runtime == nil {
		runtime = newServedAdmissionRuntime(server)
		server.admissionRuntime = runtime
	}
	if runtime.consumed() {
		server.admissionStateMu.Unlock()
		return AdmissionRecoveryReport{}, ErrRuntimeConsumed
	}
	server.admissionStateMu.Unlock()
	defer func() {
		_ = runtime.close()
		server.admissionStateMu.Lock()
		if server.admissionRuntime == runtime {
			server.admissionRuntime = nil
		}
		server.admissionStateMu.Unlock()
	}()
	if err := server.failUnavailableStrictRuntimeBeforeRepository(ctx, runtime); err != nil {
		return AdmissionRecoveryReport{}, err
	}

	factory := server.admissionBootstrapperFactory
	if factory == nil {
		factory = openAdmissionBootstrapper
	}
	bootstrapper, _, closer, err := factory(ctx, server)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	defer closeRecoveryOnlyRepository(closer)

	boot, err := server.admissionDaemonBoot()
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	metadata, err := session.RootMetadata(ctx)
	if err != nil {
		return AdmissionRecoveryReport{}, err
	}
	if err := authority.ValidateAdmissionRootContract(metadata); err != nil {
		return AdmissionRecoveryReport{}, err
	}
	support := server.assessAdmissionSupportWithRetry(ctx, runtime)
	if !strictSupportAvailable(support) {
		diagnostic := newAdmissionSupportDiagnostic(metadata, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
		logAdmissionSupportDiagnostic(diagnostic)
		return AdmissionRecoveryReport{}, diagnostic
	}
	report, err := recoverAdmissionBeforeReadyReport(ctx, session, runtime.launchPort(), server.safetyLatch)
	if err != nil {
		return report, err
	}
	report.Mode = AdmissionRecoveryOnly.String()
	if _, err := session.SealReady(ctx); err != nil {
		return report, err
	}
	return report, nil
}

func closeRecoveryOnlyRepository(closer io.Closer) {
	if closer != nil {
		_ = closer.Close()
	}
}
