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
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

var ErrAdmissionRootMissing = errors.New("agentbus admission root missing")

var (
	admissionRecoveryAfterPreflightForTest func() error
	admissionRecoveryBeforeBeginForTest    func() error
)

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
		return AdmissionRecoveryReport{}, errors.Join(err, cfg.Runtime.Close())
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
	anchorPath := filepath.Join(s.stateRoot, admissionAnchorFile)
	preflight, err := requireExistingInitializedAdmissionRoot(ctx, repoPath, anchorPath, s.socketPath)
	if err != nil {
		return nil, nil, nil, err
	}
	if admissionRecoveryAfterPreflightForTest != nil {
		if err := admissionRecoveryAfterPreflightForTest(); err != nil {
			return nil, nil, nil, err
		}
	}
	return openAdmissionBootstrapperWithOptions(ctx, s, admissionBootstrapperOpenOptions{
		expectedRepositoryIdentity: &preflight.repositoryIdentity,
		openExistingNoInitialize:   true,
		requireInitializedAnchor:   true,
		verifyInitializedStructure: true,
	})
}

type initializedAdmissionRootPreflight struct {
	repositoryIdentity bboltrepo.FileIdentity
	dbUUID             string
	schemaMajor        uint16
}

type AdmissionRootIdentityMismatchError struct {
	Path      string
	Preflight bboltrepo.FileIdentity
	Opened    bboltrepo.FileIdentity
}

func (e AdmissionRootIdentityMismatchError) Error() string {
	return fmt.Sprintf("%s: admission repository identity changed after preflight: %s (preflight dev=%d ino=%d, opened dev=%d ino=%d)", authority.ErrAnchorInvariant, e.Path, e.Preflight.Dev, e.Preflight.Ino, e.Opened.Dev, e.Opened.Ino)
}

func (e AdmissionRootIdentityMismatchError) Is(target error) bool {
	return target == authority.ErrAnchorInvariant
}

type AdmissionRootIncompatibleSchemaError struct {
	Path          string
	SchemaVersion uint16
	Cause         error
}

func (e AdmissionRootIncompatibleSchemaError) Error() string {
	return fmt.Sprintf("admission repository incompatible schema: %s: schema_version=%d: %v", e.Path, e.SchemaVersion, e.Cause)
}

func (e AdmissionRootIncompatibleSchemaError) Is(target error) bool {
	return target == repository.ErrInvalidRecord
}

func (e AdmissionRootIncompatibleSchemaError) Unwrap() error {
	return e.Cause
}

func unsupportedAdmissionRootSchemaError(repoPath string, schemaVersion uint16, cause error) error {
	if cause == nil {
		cause = fmt.Errorf("%w: meta.schema_version %d is unsupported", repository.ErrInvalidRecord, schemaVersion)
	}
	return AdmissionRootIncompatibleSchemaError{Path: repoPath, SchemaVersion: schemaVersion, Cause: cause}
}

type AdmissionRootAnchorError struct {
	Path  string
	Cause error
}

func (e AdmissionRootAnchorError) Error() string {
	return fmt.Sprintf("%s: anchor %s: %v", authority.ErrAnchorInvariant, e.Path, e.Cause)
}

func (e AdmissionRootAnchorError) Is(target error) bool {
	return target == authority.ErrAnchorInvariant
}

func (e AdmissionRootAnchorError) Unwrap() error {
	return e.Cause
}

func admissionRootRepositoryIdentityMismatchError(repoPath string, preflight, opened bboltrepo.FileIdentity) error {
	return AdmissionRootIdentityMismatchError{Path: repoPath, Preflight: preflight, Opened: opened}
}

func requireExistingInitializedAdmissionRoot(ctx context.Context, repoPath, anchorPath, socketPath string) (initializedAdmissionRootPreflight, error) {
	if err := ctx.Err(); err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	if err := requireExistingAdmissionRepositoryFile(repoPath); err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	repo, err := openReadOnlyAdmissionRepositoryWithContentionRetry(ctx, repoPath, socketPath)
	if err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	defer closeRecoveryOnlyRepository(repo)
	identity, err := repo.OpenedFileIdentity()
	if err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	if err := verifyOpenedAdmissionRepositoryInitialized(ctx, repoPath, repo); err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	if err := requireExistingAdmissionAnchorFile(anchorPath, dbUUID, schemaMajor); err != nil {
		return initializedAdmissionRootPreflight{}, err
	}
	return initializedAdmissionRootPreflight{repositoryIdentity: identity, dbUUID: dbUUID, schemaMajor: schemaMajor}, nil
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

func verifyOpenedAdmissionRepositoryInitialized(ctx context.Context, repoPath string, repo *bboltrepo.Repository) error {
	schemaVersion, err := repo.AuthorityMetaSchemaVersion()
	if err != nil {
		return err
	}
	if schemaVersion != repository.CurrentAuthorityMetaSchemaVersion {
		return unsupportedAdmissionRootSchemaError(repoPath, schemaVersion, nil)
	}
	if err := repo.VerifyInitializedStructure(); err != nil {
		return err
	}
	return repo.View(ctx, func(tx repository.ReadTx) error {
		return verifyAdmissionRepositoryMetaForRecovery(repoPath, tx.Meta())
	})
}

func verifyAdmissionRepositoryMetaForRecovery(repoPath string, meta repository.Record[repository.AuthorityMeta]) error {
	switch meta.State {
	case repository.RecordValid:
	case repository.RecordCorrupt:
		return fmt.Errorf("%w: meta: %s", repository.ErrCorruptRecord, meta.Diagnostic)
	default:
		return fmt.Errorf("%w: meta is %s", repository.ErrInvalidRecord, meta.State)
	}
	if meta.Value.SchemaVersion != repository.CurrentAuthorityMetaSchemaVersion {
		return unsupportedAdmissionRootSchemaError(repoPath, meta.Value.SchemaVersion, nil)
	}
	return meta.Value.Validate()
}

func requireExistingAdmissionAnchorFile(anchorPath, dbUUID string, schemaMajor uint16) error {
	info, err := os.Stat(anchorPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: anchor is missing: %s", authority.ErrAnchorInvariant, anchorPath)
		}
		return AdmissionRootAnchorError{Path: anchorPath, Cause: err}
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: anchor is not a regular file: %s", authority.ErrAnchorInvariant, anchorPath)
	}
	snapshot, err := authority.LoadFileAnchorSnapshot(anchorPath)
	if err != nil {
		return AdmissionRootAnchorError{Path: anchorPath, Cause: err}
	}
	if !snapshot.Initialized {
		return fmt.Errorf("%w: anchor is missing: %s", authority.ErrAnchorInvariant, anchorPath)
	}
	if snapshot.DBUUID != dbUUID || snapshot.SchemaMajor != schemaMajor {
		return fmt.Errorf("%w: anchor identity does not match repository", authority.ErrAnchorInvariant)
	}
	return nil
}

func (server *Server) recoverAdmissionRoot(ctx context.Context) (report AdmissionRecoveryReport, err error) {
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
		closeErr := runtime.close()
		server.admissionStateMu.Lock()
		if server.admissionRuntime == runtime {
			server.admissionRuntime = nil
		}
		server.admissionStateMu.Unlock()
		err = errors.Join(err, closeErr)
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
	if admissionRecoveryBeforeBeginForTest != nil {
		if err := admissionRecoveryBeforeBeginForTest(); err != nil {
			return AdmissionRecoveryReport{}, err
		}
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
	report, err = recoverAdmissionBeforeReadyReport(ctx, session, runtime.launchPort(), server.safetyLatch)
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
