package served

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

const (
	admissionRepositoryFile = "admission.bbolt"
	admissionAnchorFile     = "admission-anchor.json"

	admissionFailStopTimeout        = 30 * time.Second
	admissionRepositoryOpenTimeout  = 100 * time.Millisecond
	admissionRepositoryCloseTimeout = 5 * time.Second
	admissionContentionRetryDelay   = 50 * time.Millisecond
	admissionContentionFallback     = 2 * time.Second
	admissionProbeReasonMaxRunes    = engine.FailureReasonMaxRunes
)

var admissionDetachedCleanupTimeout = 30 * time.Second

var ErrRuntimeConsumed = errors.New("admission runtime consumed")

var (
	admissionRecordReleaseBeforeCommitForTest func() error
	admissionRepositoryBeforeCreateForTest    func() error
)

type admissionClosingError struct{}

func (admissionClosingError) Error() string { return "admission authority is shutting down" }

func (admissionClosingError) Is(target error) bool { return target == authority.ErrNotReady }

var errAdmissionClosing = admissionClosingError{}

type admissionBootstrapper = authority.Bootstrapper
type admissionReady = authority.Ready
type admissionCoordinator = coordinator.Coordinator

type admissionBootstrapperFactory func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error)

type admissionBootstrapperOpenOptions struct {
	expectedRepositoryIdentity *bboltrepo.FileIdentity
	openExistingNoInitialize   bool
	requireInitializedAnchor   bool
	verifyInitializedStructure bool
}

type admissionProbeableBackend interface {
	ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error)
}

// admissionSetupProbeCacheFingerprintBackend is an optional adapter contract.
// It identifies the on-disk setup evidence that a pinned backend needs before
// strict admission may safely attempt one more probe.
type admissionSetupProbeCacheFingerprintBackend interface {
	SetupProbeCacheFingerprint() (string, error)
}

type admissionPinnedBackend struct {
	setupCacheFingerprint      string
	setupCacheFingerprintKnown bool
	reprobeInFlight            bool
}

type admissionInstance struct {
	runtime     *servedAdmissionRuntime
	epoch       uint64
	backendMu   sync.RWMutex
	descriptors map[string]admissionBackendDescriptor
	policy      ServeAdmissionPolicy
	pinned      map[string]admissionPinnedBackend

	bootstrapper *admissionBootstrapper
	ready        *admissionReady
	coordinator  *admissionCoordinator
	submission   *servedSubmissionCoordinator
	repository   repository.Repository
	close        io.Closer
}

func (instance *admissionInstance) descriptor(name string) (admissionBackendDescriptor, bool) {
	if instance == nil {
		return admissionBackendDescriptor{}, false
	}
	instance.backendMu.RLock()
	defer instance.backendMu.RUnlock()
	if instance.descriptors == nil {
		return admissionBackendDescriptor{}, false
	}
	descriptor, ok := instance.descriptors[name]
	return descriptor, ok
}

func (instance *admissionInstance) descriptorAndFenceability(name string) (admissionBackendDescriptor, ServeBackendFenceability, bool) {
	if instance == nil {
		return admissionBackendDescriptor{}, ServeBackendFenceability{}, false
	}
	instance.backendMu.RLock()
	defer instance.backendMu.RUnlock()
	if instance.descriptors == nil {
		return admissionBackendDescriptor{}, ServeBackendFenceability{}, false
	}
	descriptor, ok := instance.descriptors[name]
	if !ok {
		return admissionBackendDescriptor{}, ServeBackendFenceability{}, false
	}
	fenceability, _ := instance.policy.backendFenceability(name)
	return descriptor, fenceability, true
}

type admissionBackendDescriptor struct {
	name             string
	backend          engine.Backend
	probeError       error
	capabilities     model.ExecutionCapabilities
	controlledRunner bool
	fenceable        bool
	unfenceableCause string
}

type admissionBackendContractViolationError struct {
	backend string
}

func (e admissionBackendContractViolationError) Error() string {
	if e.backend == "" {
		return "backend contract violation: descriptor claimed controlled-runner but session lacks ordinal-bound runner capability"
	}
	return fmt.Sprintf("backend contract violation for %s: descriptor claimed controlled-runner but session lacks ordinal-bound runner capability", e.backend)
}

type AdmissionServeMode uint8

const (
	AdmissionStrictIdentified AdmissionServeMode = iota + 1
	AdmissionRecoveryOnly
	AdmissionFatal
)

func (mode AdmissionServeMode) String() string {
	switch mode {
	case AdmissionStrictIdentified:
		return "strict_identified"
	case AdmissionRecoveryOnly:
		return "recovery_only"
	case AdmissionFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

type admissionStartupHooks struct {
	AfterMetadataRead       func(authority.AdmissionRootMetadata)
	BeforeRecovery          func()
	AfterRecovery           func()
	BeforeSupportAssessment func()
	AfterSupportAssessment  func(custodian.Support)
	BeforePolicyInstall     func()
}

// ServeAdmissionPolicy holds the strict-admission policy derived during Serve
// bootstrap. Its strict-route and runtime fields never change; a backend's
// fenceability entry may be refreshed only after a newly observed setup cache
// passes a fresh strict probe.
type ServeAdmissionPolicy struct {
	Mode                      AdmissionServeMode
	AcceptIdentified          bool
	AdvertiseRequestID        bool
	Reason                    error
	strictRouteEnabled        bool
	strictRouteDisabledReason string
	runtimeSupport            custodian.Support
	runtimeAssessment         custodian.SupportAssessment
	backends                  map[string]ServeBackendFenceability
}

type ServeBackendFenceability struct {
	Backend          string
	Capabilities     model.ExecutionCapabilities
	ControlledRunner bool
	Fenceable        bool
	Reason           string
}

func deriveServeAdmissionPolicy(metadata authority.AdmissionRootMetadata, runtimeSupport custodian.Support, descriptors map[string]admissionBackendDescriptor) ServeAdmissionPolicy {
	mode := AdmissionFatal
	if strictSupportAvailable(runtimeSupport) {
		mode = AdmissionStrictIdentified
	}
	reason := error(nil)
	if mode == AdmissionFatal {
		reason = newAdmissionSupportDiagnostic(metadata, runtimeSupport.Assessment, true)
	}
	policy := ServeAdmissionPolicy{
		Mode:               mode,
		AcceptIdentified:   mode == AdmissionStrictIdentified,
		AdvertiseRequestID: false,
		Reason:             reason,
		strictRouteEnabled: mode != AdmissionFatal,
		runtimeSupport:     runtimeSupport,
		runtimeAssessment:  runtimeSupport.Assessment,
		backends:           make(map[string]ServeBackendFenceability, len(descriptors)),
	}
	if mode == AdmissionFatal {
		policy.strictRouteDisabledReason = reason.Error()
	}
	for name, descriptor := range descriptors {
		policy.backends[name] = admissionBackendFenceability(name, descriptor)
	}
	return policy
}

func admissionBackendFenceability(name string, descriptor admissionBackendDescriptor) ServeBackendFenceability {
	return ServeBackendFenceability{
		Backend:          name,
		Capabilities:     descriptor.capabilities,
		ControlledRunner: descriptor.controlledRunner,
		Fenceable:        descriptor.fenceable,
		Reason:           descriptor.unfenceableCause,
	}
}

func (policy ServeAdmissionPolicy) backendFenceability(name string) (ServeBackendFenceability, bool) {
	if policy.backends == nil {
		return ServeBackendFenceability{}, false
	}
	fenceability, ok := policy.backends[name]
	return fenceability, ok
}

func (policy ServeAdmissionPolicy) strictRuntimeAvailable() bool {
	return strictSupportAvailable(policy.runtimeSupport)
}

const admissionSupportMaxAttempts = 3

var ErrAdmissionStrictSupportUnavailable = errors.New("strict admission support unavailable")

type AdmissionSupportDiagnostic struct {
	Metadata       authority.AdmissionRootMetadata
	Assessment     custodian.SupportAssessment
	RetryExhausted bool
	FailStopped    bool
}

func (e AdmissionSupportDiagnostic) Error() string {
	state := "root"
	if e.Metadata.Activated {
		state = "activated root"
	}
	message := fmt.Sprintf("%s: %s support class=%s attempts=%d cleanup_safe=%t", ErrAdmissionStrictSupportUnavailable, state, e.Assessment.Class, e.Assessment.Attempts, e.Assessment.CleanupSafe)
	if e.RetryExhausted {
		message += " retry_exhausted=true"
	}
	if e.FailStopped {
		message += " fail_stopped=true"
	}
	if e.Assessment.Cause != nil {
		message += ": " + e.Assessment.Cause.Error()
	}
	return message
}

func (e AdmissionSupportDiagnostic) Unwrap() error {
	if e.Assessment.Cause != nil {
		return e.Assessment.Cause
	}
	return ErrAdmissionStrictSupportUnavailable
}

func (e AdmissionSupportDiagnostic) Is(target error) bool {
	return target == ErrAdmissionStrictSupportUnavailable
}

func newAdmissionSupportDiagnostic(metadata authority.AdmissionRootMetadata, assessment custodian.SupportAssessment, retryExhausted bool) AdmissionSupportDiagnostic {
	return AdmissionSupportDiagnostic{
		Metadata:       metadata,
		Assessment:     assessment,
		RetryExhausted: retryExhausted,
	}
}

func StrictAdmissionSupportPreflight(ctx context.Context, cfg Config) error {
	_, err := strictAdmissionSupportPreflight(ctx, cfg)
	return err
}

func strictAdmissionSupportPreflight(ctx context.Context, cfg Config) (custodian.Support, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runtime := newServedAdmissionRuntimeFromRuntime(cfg.Runtime)
	server := &Server{}
	support := server.assessAdmissionSupportWithRetry(ctx, runtime)
	if strictSupportAvailable(support) {
		return support, nil
	}
	diagnostic := newAdmissionSupportDiagnostic(authority.AdmissionRootMetadata{}, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
	if err := alreadyListeningAfterRetryableSupportContention(ctx, cfg, support); err != nil {
		return support, err
	}
	logAdmissionSupportDiagnostic(diagnostic)
	return support, diagnostic
}

func alreadyListeningAfterRetryableSupportContention(ctx context.Context, cfg Config, support custodian.Support) error {
	if !retryableCgroupRootLeaseContention(support) {
		return nil
	}
	socketPath := cfg.SocketPath
	if socketPath == "" {
		var err error
		socketPath, err = SocketPath(cfg.StateRoot)
		if err != nil {
			return nil
		}
	}
	contentionCtx, cancel := admissionContentionContext(ctx)
	defer cancel()
	for {
		dialTimeout, err := admissionContentionAttemptTimeout(contentionCtx, admissionRepositoryOpenTimeout)
		if err != nil {
			return nil
		}
		if admissionSocketDialableWithin(socketPath, dialTimeout) {
			return DaemonAlreadyListeningError{SocketPath: socketPath}
		}
		select {
		case <-contentionCtx.Done():
			return nil
		case <-time.After(admissionContentionRetryDelay):
		}
	}
}

func retryableCgroupRootLeaseContention(support custodian.Support) bool {
	return support.Assessment.Class == custodian.SupportRetryable &&
		errors.Is(support.Assessment.Cause, cgroup.ErrRootLeaseUnavailable)
}

// NewAfterStrictAdmissionSupportPreflight runs the strict support probe before
// mutating daemon state, then creates the state root and token through New.
// On success the returned server reuses the preflight support assessment during
// admission bootstrap so startup does not run the same support probe twice.
func NewAfterStrictAdmissionSupportPreflight(ctx context.Context, cfg Config) (*Server, error) {
	support, err := strictAdmissionSupportPreflight(ctx, cfg)
	if err != nil {
		return nil, err
	}
	root, err := ensureStateRoot(cfg.StateRoot)
	if err != nil {
		return nil, err
	}
	cfg.StateRoot = root
	server, err := New(cfg)
	if err != nil {
		return nil, err
	}
	runtime := newServedAdmissionRuntimeFromRuntime(cfg.Runtime)
	runtime.supportOverride = &support
	server.admissionRuntime = runtime
	return server, nil
}

func (s *Server) requireActivatedAdmissionSupport(ctx context.Context, session *authority.RecoverySession, runtime *servedAdmissionRuntime, metadata authority.AdmissionRootMetadata) (custodian.Support, error) {
	if s.admissionStartupHooks.BeforeSupportAssessment != nil {
		s.admissionStartupHooks.BeforeSupportAssessment()
	}
	support := s.assessAdmissionSupportWithRetry(ctx, runtime)
	if s.admissionStartupHooks.AfterSupportAssessment != nil {
		s.admissionStartupHooks.AfterSupportAssessment(support)
	}
	if strictSupportAvailable(support) {
		return support, nil
	}
	diagnostic := newAdmissionSupportDiagnostic(metadata, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
	if support.Assessment.Class == custodian.SupportUnsafe {
		if session != nil {
			stopErr := session.FailStop(ctx, diagnostic.Error())
			if stopErr != nil {
				err := fmt.Errorf("%w: unsafe strict admission support: %w; fail-stop persistence: %w", authority.ErrFailStopRecord, diagnostic, stopErr)
				logAdmissionSupportDiagnostic(diagnostic)
				return support, err
			}
		}
		diagnostic.FailStopped = true
		if s.safetyLatch != nil {
			s.safetyLatch.Trip(diagnostic)
		}
	}
	logAdmissionSupportDiagnostic(diagnostic)
	if diagnostic.FailStopped {
		return support, errors.Join(SafetyFailStopError{Reason: diagnostic}, diagnostic)
	}
	return support, diagnostic
}

func (s *Server) assessAdmissionSupportWithRetry(ctx context.Context, runtime *servedAdmissionRuntime) custodian.Support {
	support := runtime.assessSupport(ctx)
	return s.assessAdmissionSupportWithRetryFrom(ctx, runtime, support)
}

func (s *Server) assessAdmissionSupportWithRetryFrom(ctx context.Context, runtime *servedAdmissionRuntime, support custodian.Support) custodian.Support {
	assessment := normalizeAdmissionSupportAssessment(support)
	support.Assessment = assessment
	totalAttempts := admissionSupportAttemptCost(assessment)
	cleanupSafe := assessment.CleanupSafe
	for assessment.Class == custodian.SupportRetryable && totalAttempts < admissionSupportMaxAttempts {
		if err := ctx.Err(); err != nil {
			assessment = custodian.SupportAssessment{
				Class:       custodian.SupportRetryable,
				Cause:       errors.Join(assessment.Cause, err),
				Attempts:    totalAttempts,
				CleanupSafe: cleanupSafe,
			}
			support.Assessment = assessment
			support.RuntimeProbeResult = assessment.Cause
			support.Reason = assessment.Cause
			return support
		}
		next := runtime.assessSupport(ctx)
		nextAssessment := normalizeAdmissionSupportAssessment(next)
		totalAttempts += admissionSupportAttemptCost(nextAssessment)
		cleanupSafe = cleanupSafe && nextAssessment.CleanupSafe
		nextAssessment.Attempts = totalAttempts
		nextAssessment.CleanupSafe = cleanupSafe
		next.Assessment = nextAssessment
		if nextAssessment.Class == custodian.SupportAvailable {
			next.RuntimeProbeResult = nil
			next.Reason = nil
		} else {
			next.RuntimeProbeResult = nextAssessment.Cause
			next.Reason = nextAssessment.Cause
		}
		support = next
		assessment = nextAssessment
	}
	if support.Assessment.Class == custodian.SupportRetryable {
		support.Assessment.Attempts = totalAttempts
	}
	return support
}

func normalizeAdmissionSupportAssessment(support custodian.Support) custodian.SupportAssessment {
	assessment := support.Assessment
	if assessment != (custodian.SupportAssessment{}) {
		if assessment.Attempts == 0 && assessment.Class != custodian.SupportAvailable {
			assessment.Attempts = 1
		}
		return assessment
	}
	if support.RuntimeProbePassed {
		return custodian.SupportAssessment{Class: custodian.SupportAvailable, Attempts: 1, CleanupSafe: true}
	}
	cause := support.RuntimeProbeResult
	if cause == nil {
		cause = custodian.ErrSupervisorUnavailable
	}
	return custodian.SupportAssessment{Class: custodian.SupportUnsupported, Cause: cause, Attempts: 1, CleanupSafe: true}
}

func admissionSupportAttemptCost(assessment custodian.SupportAssessment) int {
	if assessment.Attempts > 0 {
		return assessment.Attempts
	}
	return 1
}

func strictSupportAvailable(support custodian.Support) bool {
	return support.Assessment.Class == custodian.SupportAvailable &&
		support.RuntimeProbePassed &&
		support.ParkedExec &&
		support.VerifiedContainment
}

func logAdmissionSupportDiagnostic(err error) {
	var diagnostic AdmissionSupportDiagnostic
	if errors.As(err, &diagnostic) {
		log.Printf("agentbus daemon: strict admission support diagnostic: class=%s attempts=%d cleanup_safe=%t retry_exhausted=%t fail_stopped=%t cause=%v", diagnostic.Assessment.Class, diagnostic.Assessment.Attempts, diagnostic.Assessment.CleanupSafe, diagnostic.RetryExhausted, diagnostic.FailStopped, diagnostic.Assessment.Cause)
		return
	}
	if err != nil {
		log.Printf("agentbus daemon: strict admission support diagnostic: %v", err)
	}
}

func (s *Server) bootstrapAdmission(ctx context.Context) error {
	s.admissionStateMu.Lock()
	defer s.admissionStateMu.Unlock()

	if s.admissionInstance != nil {
		return nil
	}
	runtime := s.admissionRuntime
	if runtime == nil {
		runtime = newServedAdmissionRuntime(s)
	}
	if runtime.consumed() {
		return ErrRuntimeConsumed
	}
	s.admissionRuntime = runtime
	var closer io.Closer
	closeOnErr := true
	defer func() {
		if closeOnErr {
			if closer != nil {
				_ = closer.Close()
			}
			_ = runtime.close()
			s.admissionRuntime = nil
		}
	}()
	if err := s.failUnavailableStrictRuntimeBeforeRepository(ctx, runtime); err != nil {
		return err
	}
	descriptors, pinnedBackends, err := s.probeAdmissionBackends(ctx)
	if err != nil {
		return err
	}

	factory := s.admissionBootstrapperFactory
	if factory == nil {
		factory = openAdmissionBootstrapper
	}
	bootstrapper, repo, closer, err := factory(ctx, s)
	if err != nil {
		return err
	}

	boot, err := s.admissionDaemonBoot()
	if err != nil {
		return err
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return err
	}
	metadata, err := session.RootMetadata(ctx)
	if err != nil {
		return err
	}
	if err := authority.ValidateAdmissionRootContract(metadata); err != nil {
		return err
	}
	if s.admissionStartupHooks.AfterMetadataRead != nil {
		s.admissionStartupHooks.AfterMetadataRead(metadata)
	}

	var support custodian.Support
	if metadata.Activated {
		if s.admissionStartupHooks.BeforeRecovery != nil {
			s.admissionStartupHooks.BeforeRecovery()
		}
		if err := recoverAdmissionBeforeReady(ctx, session, runtime.launchPort(), s.safetyLatch, s.clock); err != nil {
			return err
		}
		if s.admissionStartupHooks.AfterRecovery != nil {
			s.admissionStartupHooks.AfterRecovery()
		}
		support, err = s.requireActivatedAdmissionSupport(ctx, session, runtime, metadata)
		if err != nil {
			return err
		}
	} else {
		support = s.assessAdmissionSupportWithRetry(ctx, runtime)
		if !strictSupportAvailable(support) {
			err := newAdmissionSupportDiagnostic(metadata, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
			logAdmissionSupportDiagnostic(err)
			return err
		}
		activated, _, err := session.ActivateRoot(ctx)
		if err != nil {
			return err
		}
		metadata = activated
		if s.admissionStartupHooks.BeforeRecovery != nil {
			s.admissionStartupHooks.BeforeRecovery()
		}
		if err := recoverAdmissionBeforeReady(ctx, session, runtime.launchPort(), s.safetyLatch, s.clock); err != nil {
			return err
		}
		if s.admissionStartupHooks.AfterRecovery != nil {
			s.admissionStartupHooks.AfterRecovery()
		}
	}
	if s.admissionStartupHooks.BeforePolicyInstall != nil {
		s.admissionStartupHooks.BeforePolicyInstall()
	}
	policy := deriveServeAdmissionPolicy(metadata, support, descriptors)
	if policy.Mode == AdmissionFatal {
		if policy.Reason != nil {
			logAdmissionSupportDiagnostic(policy.Reason)
			return policy.Reason
		}
		return errors.New("admission policy is fatal")
	}
	if err := authority.ValidateAdmissionRootContract(metadata); err != nil {
		return err
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		return err
	}
	adapter := &servedAdmissionAuthority{ready: ready, latch: s.safetyLatch, clock: s.clock}
	owner, err := model.NewOwnerID(s.nextID("coordinator"))
	if err != nil {
		return err
	}
	launchController, err := launch.New(servedLaunchAuthority{ready: ready, latch: s.safetyLatch}, runtime.launchPort())
	if err != nil {
		return err
	}
	coord, err := coordinator.New(adapter, servedCoordinatorLaunchContainment{controller: launchController}, servedResultPublisher{server: s}, owner)
	if err != nil {
		return err
	}
	submission := &servedSubmissionCoordinator{
		ready:  ready,
		owner:  owner,
		launch: launchController,
		latch:  s.safetyLatch,
	}

	epoch := s.admissionCloseEpoch.Load()
	s.admissionOpenEpoch.Store(epoch)
	s.admissionBootstrapper = bootstrapper
	s.admissionReady = ready
	s.admissionCoordinator = coord
	s.admissionOwnedWorkChecker = coord
	s.admissionSubmission = submission
	s.admissionRuntime = runtime
	s.admissionRepository = repo
	s.admissionClose = closer
	s.admissionInstance = &admissionInstance{
		runtime:      runtime,
		epoch:        epoch,
		descriptors:  cloneAdmissionBackendDescriptors(descriptors),
		policy:       policy,
		pinned:       pinnedBackends,
		bootstrapper: bootstrapper,
		ready:        ready,
		coordinator:  coord,
		submission:   submission,
		repository:   repo,
		close:        closer,
	}
	closeOnErr = false
	return nil
}

func (s *Server) failUnavailableStrictRuntimeBeforeRepository(ctx context.Context, runtime *servedAdmissionRuntime) error {
	if runtime == nil || !runtime.unavailableProcess() {
		return nil
	}
	support := s.assessAdmissionSupportWithRetry(ctx, runtime)
	if strictSupportAvailable(support) {
		return nil
	}
	// C9 startup order requires the strict runtime/lease outcome before the
	// admission repository is opened; lease contention must surface as the typed
	// support diagnostic instead of blocking behind bbolt's file lock.
	diagnostic := newAdmissionSupportDiagnostic(authority.AdmissionRootMetadata{}, support.Assessment, support.Assessment.Class == custodian.SupportRetryable)
	logAdmissionSupportDiagnostic(diagnostic)
	return diagnostic
}

func (s *Server) closeServeAdmission() error {
	return s.closeServeAdmissionContext(context.Background())
}

func (s *Server) closeServeAdmissionContext(ctx context.Context) error {
	return s.closeServeAdmissionSnapshot(ctx, s.currentServeAdmissionSnapshot())
}

func (s *Server) closeServeAdmissionSnapshot(ctx context.Context, snapshot *serveAdmissionSnapshot) error {
	if snapshot == nil {
		return nil
	}
	if !snapshot.closeStarted.CompareAndSwap(false, true) {
		return nil
	}
	snapshot.closeErr = s.closeServeAdmissionSnapshotOnce(ctx, snapshot)
	return snapshot.closeErr
}

func (s *Server) closeServeAdmissionSnapshotOnce(ctx context.Context, snapshot *serveAdmissionSnapshot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.admissionStateMu.RLock()
	current := s.admissionInstance == snapshot.instance
	s.admissionStateMu.RUnlock()
	if !current {
		return closeServeAdmissionSnapshotResources(ctx, snapshot, false, nil, time.Now().Add(admissionRepositoryCloseTimeout))
	}
	// Lock order for the admission shutdown path is submitMu -> stateMu.
	// Identified submit never takes submitMu while holding stateMu; it snapshots
	// under stateMu, releases it, and re-checks publication under submitMu
	// before durable acceptance. That gives close a single order to serialize
	// acceptance and state clearing without a lock cycle.
	s.admissionCloseEpoch.Add(1)
	deadline := admissionCloseDeadline(ctx, admissionRepositoryCloseTimeout)
	submitLockErr := s.lockAdmissionSubmitUntilContext(ctx, deadline)
	submitLocked := submitLockErr == nil
	if !submitLocked && errors.Is(submitLockErr, ErrShutdownDeadlineExceeded) {
		log.Printf("agentbus daemon: admission shutdown timed out acquiring submit serialization; a submit is wedged; clearing published state and leaking stalled submit")
		// Contract reconciliation: if close cannot acquire admissionSubmitMu
		// before the shutdown deadline, a submit is wedged inside the durable
		// path. We clear published admission state anyway and skip repository
		// close because the leaked submit may own that repository. From this
		// point the stronger "no durable accept lands after close begins"
		// guarantee degrades to the SubmissionCoordinator graceful-exit ==
		// crash-window contract: if the leaked submit later commits durably,
		// the next Serve startup recovery finalizes it deterministically by
		// recorded progress (at-most-once; replay returns the terminal job).
		// The closing marker makes a leaked submit re-check fail instead of
		// returning success after state was cleared; if a response write already
		// raced out, that is the documented window.
	}
	if !submitLocked && !errors.Is(submitLockErr, ErrShutdownDeadlineExceeded) {
		return submitLockErr
	}
	s.admissionStateMu.Lock()

	current = s.admissionInstance == snapshot.instance
	closer := snapshot.closer
	runtime := snapshot.runtime
	if current {
		closer = s.admissionClose
		runtime = s.admissionRuntime
	}
	if runtime != nil && snapshot.instance != nil {
		runtime.markConsumed()
	}
	if current {
		s.admissionBootstrapper = nil
		s.admissionReady = nil
		s.admissionCoordinator = nil
		s.admissionOwnedWorkChecker = nil
		s.admissionSubmission = nil
		s.admissionRuntime = nil
		s.admissionRepository = nil
		s.admissionClose = nil
		s.admissionInstance = nil
		s.admissionDaemonBootOnce = sync.Once{}
		s.admissionDaemonBootRef = model.BootRef{}
		s.admissionDaemonBootRefErr = nil
	}
	s.admissionStateMu.Unlock()
	if submitLocked {
		s.admissionSubmitMu.Unlock()
	}

	if !current {
		return closeServeAdmissionSnapshotResources(ctx, snapshot, false, nil, deadline)
	}

	if closer != nil {
		if !submitLocked {
			log.Printf("agentbus daemon: admission repository close skipped after submit serialization timeout; submit is wedged and may own the repository; leaking handle at shutdown")
			if runtime != nil {
				log.Printf("agentbus daemon: admission runtime close skipped after submit serialization timeout; submit is wedged and may own the runtime; leaking runtime at shutdown")
			}
			return submitLockErr
		}
		err := closeAdmissionResourceBeforeDeadline(ctx, "repository", closer, deadline)
		if runtime != nil {
			err = errors.Join(err, closeAdmissionResourceBeforeDeadline(ctx, "runtime", runtimeCloser{runtime: runtime}, deadline))
		}
		return err
	}
	if runtime != nil {
		if !submitLocked {
			log.Printf("agentbus daemon: admission runtime close skipped after submit serialization timeout; submit is wedged and may own the runtime; leaking runtime at shutdown")
			return submitLockErr
		}
		return closeAdmissionResourceBeforeDeadline(ctx, "runtime", runtimeCloser{runtime: runtime}, deadline)
	}
	if !submitLocked {
		return submitLockErr
	}
	return nil
}

func closeServeAdmissionSnapshotResources(ctx context.Context, snapshot *serveAdmissionSnapshot, submitLocked bool, submitLockErr error, deadline time.Time) error {
	if snapshot == nil {
		return nil
	}
	if snapshot.runtime != nil && snapshot.instance != nil {
		snapshot.runtime.markConsumed()
	}
	if snapshot.closer != nil {
		err := closeAdmissionResourceBeforeDeadline(ctx, "repository", snapshot.closer, deadline)
		if snapshot.runtime != nil {
			err = errors.Join(err, closeAdmissionResourceBeforeDeadline(ctx, "runtime", runtimeCloser{runtime: snapshot.runtime}, deadline))
		}
		return err
	}
	if snapshot.runtime != nil {
		return closeAdmissionResourceBeforeDeadline(ctx, "runtime", runtimeCloser{runtime: snapshot.runtime}, deadline)
	}
	if !submitLocked {
		return submitLockErr
	}
	return nil
}

func admissionCloseDeadline(ctx context.Context, cap time.Duration) time.Time {
	deadline := time.Now().Add(cap)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		return ctxDeadline
	}
	return deadline
}

func (s *Server) beginAdmissionClosing(ctx context.Context, snapshot *serveAdmissionSnapshot) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if snapshot == nil || snapshot.instance == nil {
		return nil
	}
	s.admissionStateMu.RLock()
	current := s.admissionInstance == snapshot.instance
	s.admissionStateMu.RUnlock()
	if !current {
		return nil
	}
	s.admissionCloseEpoch.Add(1)
	if err := s.lockAdmissionSubmitContext(ctx); err != nil {
		return err
	}
	s.admissionSubmitMu.Unlock()
	return nil
}

func (s *Server) lockAdmissionSubmitContext(ctx context.Context) error {
	for {
		if s.admissionSubmitMu.TryLock() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Server) lockAdmissionSubmitUntilContext(ctx context.Context, deadline time.Time) error {
	for {
		if s.admissionSubmitMu.TryLock() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ErrShutdownDeadlineExceeded
		}
		sleep := 10 * time.Millisecond
		if remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type runtimeCloser struct {
	runtime *servedAdmissionRuntime
}

func (c runtimeCloser) Close() error {
	return c.runtime.close()
}

func closeAdmissionResourceBeforeDeadline(ctx context.Context, name string, closer io.Closer, deadline time.Time) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		log.Printf("agentbus daemon: admission %s close timed out; leaking handle at shutdown", name)
		return fmt.Errorf("%w: admission %s close timed out", ErrShutdownDeadlineExceeded, name)
	}
	done := make(chan error, 1)
	go func() {
		done <- closer.Close()
	}()
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Printf("agentbus daemon: admission %s close timed out; leaking handle at shutdown", name)
		} else {
			log.Printf("agentbus daemon: admission %s close canceled; leaking handle at shutdown", name)
		}
		return ctx.Err()
	case <-timer.C:
		log.Printf("agentbus daemon: admission %s close timed out; leaking handle at shutdown", name)
		return fmt.Errorf("%w: admission %s close timed out", ErrShutdownDeadlineExceeded, name)
	}
}

func (s *Server) admissionCurrentServeClosing() bool {
	return s.admissionCloseEpoch.Load() != s.admissionOpenEpoch.Load()
}

func (s *Server) admissionInstanceClosing(instance *admissionInstance) bool {
	return instance != nil && s.admissionCloseEpoch.Load() != instance.epoch
}

// The admission guards use snapshot-checkout, NOT lock-across-operation: the
// read lock protects only the reference read. Holding it across fn would let
// one stalled operation (e.g. a stuck ownership probe) block closeServeAdmission's
// write lock past the safety fail-stop drain deadline — fail-stop MUST win over
// stalled work. The trade-off is explicit: an operation checked out before the
// close races it and receives typed errors from the cleared/closed objects
// (never a hang, never corrupted state); new operations after close fail fast
// on the nil-instance check.
func (s *Server) withAdmissionCoordinator(fn func(*admissionCoordinator) error) error {
	s.admissionStateMu.RLock()
	coordinatorRef := s.admissionCoordinator
	ready := s.admissionInstance != nil && coordinatorRef != nil
	s.admissionStateMu.RUnlock()
	if !ready {
		return coordinator.ErrCoordinatorNotReady
	}
	return fn(coordinatorRef)
}

func (s *Server) withAdmissionSubmission(fn func(*servedSubmissionCoordinator) error) error {
	s.admissionStateMu.RLock()
	submissionRef := s.admissionSubmission
	ready := s.admissionInstance != nil && submissionRef != nil
	s.admissionStateMu.RUnlock()
	if !ready {
		return authority.ErrNotReady
	}
	return fn(submissionRef)
}

func (s *Server) probeAdmissionBackends(ctx context.Context) (map[string]admissionBackendDescriptor, map[string]admissionPinnedBackend, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	runner := s.admissionProbeRunner
	if runner == nil {
		runner = command.DirectProbeRunner{}
	}
	backends := s.backendSnapshot()
	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make(map[string]admissionBackendDescriptor, len(names))
	pinned := make(map[string]admissionPinnedBackend)
	for _, name := range names {
		backend := backends[name]
		probeable, ok := backend.(admissionProbeableBackend)
		if !ok {
			probeErr := model.IncompatibleExecutionCapabilitiesError{
				Reason: "strict backend does not implement ProbeBackend; admission cannot verify command-runner capabilities",
			}
			if s.admissionUnprobeableBackends == nil {
				s.admissionUnprobeableBackends = make(map[string]error)
			}
			s.admissionUnprobeableBackends[name] = probeErr
			descriptors[name] = s.admissionBackendDescriptor(name, backend, probeErr)
			continue
		}
		fingerprint, fingerprintKnown := admissionSetupProbeCacheFingerprint(backend)
		probed, err := probeable.ProbeBackend(ctx, runner)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, nil, fmt.Errorf("probe strict backend %s canceled by serve context: %w", name, ctxErr)
			}
			// Probe cancellation is fatal only when the Serve parent context
			// is canceled. A backend-returned context sentinel while that
			// parent is still live is an environment-class probe failure:
			// record the backend unfenceable so strict identified admission
			// rejects it pre-accept, while other backends and legacy traffic
			// keep serving.
			probeErr := admissionProbeFailureError(name, err)
			if s.admissionUnprobeableBackends == nil {
				s.admissionUnprobeableBackends = make(map[string]error)
			}
			s.admissionUnprobeableBackends[name] = probeErr
			descriptors[name] = s.admissionBackendDescriptor(name, backend, probeErr)
			pinned[name] = admissionPinnedBackend{
				setupCacheFingerprint:      fingerprint,
				setupCacheFingerprintKnown: fingerprintKnown,
			}
			continue
		}
		if probed == nil {
			return nil, nil, fmt.Errorf("probe strict backend %s: nil probed backend", name)
		}
		if probed.Name() != name {
			return nil, nil, fmt.Errorf("probe strict backend %s: probed backend changed name to %s", name, probed.Name())
		}
		s.replaceBackend(name, probed)
		if s.admissionUnprobeableBackends != nil {
			delete(s.admissionUnprobeableBackends, name)
		}
		descriptors[name] = s.admissionBackendDescriptor(name, probed, nil)
	}
	if len(pinned) == 0 {
		pinned = nil
	}
	return descriptors, pinned, nil
}

func admissionProbeFailureError(name string, err error) error {
	message := "unknown probe failure"
	if err != nil {
		message = err.Error()
	}
	message = sanitizeAdmissionProbeReason(message)
	return fmt.Errorf("probe strict backend %s failed: %s", name, message)
}

func sanitizeAdmissionProbeReason(message string) string {
	const suffix = "..."
	runes := make([]rune, 0, admissionProbeReasonMaxRunes)
	for _, r := range message {
		if unicode.IsPrint(r) || r == ' ' {
			runes = append(runes, r)
		} else {
			runes = append(runes, ' ')
		}
		if len(runes) > admissionProbeReasonMaxRunes {
			break
		}
	}
	if len(runes) <= admissionProbeReasonMaxRunes {
		return string(runes)
	}
	keep := admissionProbeReasonMaxRunes - len(suffix)
	if keep < 0 {
		keep = 0
	}
	return string(runes[:keep]) + suffix
}

func (s *Server) admissionBackendDescriptor(name string, backend engine.Backend, probeErr error) admissionBackendDescriptor {
	caps := model.ExecutionCapabilities{
		ExternalRunner: admissionBackendExternalRunner(backend),
		FencedLaunch:   false,
	}
	controlled := admissionBackendControlledRunner(backend)
	fenceable := !caps.ExternalRunner && controlled && probeErr == nil
	reason := ""
	switch {
	case probeErr != nil:
		reason = probeErr.Error()
	case caps.ExternalRunner:
		reason = "strict identified admission requires an in-process backend runner"
	case !controlled:
		reason = "identified fenced admission requires a controlled command runner before acceptance"
	}
	return admissionBackendDescriptor{
		name:             name,
		backend:          backend,
		probeError:       probeErr,
		capabilities:     caps,
		controlledRunner: controlled,
		fenceable:        fenceable,
		unfenceableCause: reason,
	}
}

func cloneAdmissionBackendDescriptors(src map[string]admissionBackendDescriptor) map[string]admissionBackendDescriptor {
	if src == nil {
		return nil
	}
	dst := make(map[string]admissionBackendDescriptor, len(src))
	for name, descriptor := range src {
		dst[name] = descriptor
	}
	return dst
}

func admissionSetupProbeCacheFingerprint(backend engine.Backend) (string, bool) {
	provider, ok := backend.(admissionSetupProbeCacheFingerprintBackend)
	if !ok {
		return "", false
	}
	fingerprint, err := provider.SetupProbeCacheFingerprint()
	if err != nil || fingerprint == "" {
		return "", false
	}
	return fingerprint, true
}

func (instance *admissionInstance) setupCacheRefreshCandidate(name string) (admissionBackendDescriptor, string, bool, bool) {
	if instance == nil {
		return admissionBackendDescriptor{}, "", false, false
	}
	instance.backendMu.RLock()
	defer instance.backendMu.RUnlock()
	pinned, ok := instance.pinned[name]
	if !ok || pinned.reprobeInFlight {
		return admissionBackendDescriptor{}, "", false, false
	}
	descriptor, ok := instance.descriptors[name]
	if !ok || descriptor.probeError == nil {
		return admissionBackendDescriptor{}, "", false, false
	}
	return descriptor, pinned.setupCacheFingerprint, pinned.setupCacheFingerprintKnown, true
}

func (instance *admissionInstance) beginSetupCacheRefresh(name, fingerprint string) (admissionBackendDescriptor, bool) {
	if instance == nil {
		return admissionBackendDescriptor{}, false
	}
	instance.backendMu.Lock()
	defer instance.backendMu.Unlock()
	pinned, ok := instance.pinned[name]
	if !ok || pinned.reprobeInFlight || (pinned.setupCacheFingerprintKnown && pinned.setupCacheFingerprint == fingerprint) {
		return admissionBackendDescriptor{}, false
	}
	descriptor, ok := instance.descriptors[name]
	if !ok || descriptor.probeError == nil {
		return admissionBackendDescriptor{}, false
	}
	pinned.reprobeInFlight = true
	instance.pinned[name] = pinned
	return descriptor, true
}

func (instance *admissionInstance) replaceBackendDescriptorLocked(name string, descriptor admissionBackendDescriptor) {
	if instance.descriptors == nil {
		return
	}
	instance.descriptors[name] = descriptor
	if instance.policy.backends != nil {
		instance.policy.backends[name] = admissionBackendFenceability(name, descriptor)
	}
}

// refreshAdmissionBackendOnSetupCacheChange is deliberately a snapshot-checkout
// operation. It holds no admission lock while ProbeBackend can spawn a process;
// simultaneous submitters see the existing pin until this one finishes.
func (s *Server) refreshAdmissionBackendOnSetupCacheChange(ctx context.Context, instance *admissionInstance, name string) {
	descriptor, priorFingerprint, priorFingerprintKnown, refreshable := instance.setupCacheRefreshCandidate(name)
	if !refreshable {
		return
	}
	fingerprint, changed := admissionSetupProbeCacheFingerprint(descriptor.backend)
	if !changed || (priorFingerprintKnown && fingerprint == priorFingerprint) {
		return
	}
	descriptor, started := instance.beginSetupCacheRefresh(name, fingerprint)
	if !started {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	probeable, ok := descriptor.backend.(admissionProbeableBackend)
	if !ok {
		s.finishAdmissionBackendSetupCacheRefresh(instance, name, fingerprint, nil, errors.New("strict backend no longer implements ProbeBackend"), true, true)
		return
	}
	runner := s.admissionProbeRunner
	if runner == nil {
		runner = command.DirectProbeRunner{}
	}
	probed, probeErr := probeable.ProbeBackend(ctx, runner)
	if ctx.Err() != nil {
		// A client cancellation must not consume a freshly observed cache
		// revision or turn that cancellation into a daemon-lifetime pin.
		s.finishAdmissionBackendSetupCacheRefresh(instance, name, fingerprint, nil, nil, false, false)
		return
	}
	if probeErr == nil && probed == nil {
		probeErr = errors.New("probe returned nil backend")
	}
	if probeErr == nil && probed.Name() != name {
		probeErr = fmt.Errorf("probe returned backend named %s", probed.Name())
	}
	postFingerprint, postKnown := admissionSetupProbeCacheFingerprint(descriptor.backend)
	cacheStable := postKnown && postFingerprint == fingerprint
	if probeErr != nil {
		probeErr = admissionProbeFailureError(name, probeErr)
	}
	// A failed post-probe read cannot establish that this revision is still the
	// cache revision. Leave the prior fingerprint in place so the next
	// submission can retry the same candidate. A known different revision is
	// still consumed: the next refresh will observe and probe that revision.
	s.finishAdmissionBackendSetupCacheRefresh(instance, name, fingerprint, probed, probeErr, cacheStable, postKnown)
}

func (s *Server) finishAdmissionBackendSetupCacheRefresh(instance *admissionInstance, name, fingerprint string, probed engine.Backend, probeErr error, cacheStable, consumeFingerprint bool) {
	// State publication is short and happens only after the probe. A closing or
	// replacement Serve wins; its checked-out instance is never modified.
	// The backend-map lock is always innermost: admissionStateMu ->
	// instance.backendMu -> backendMapMu. Readers acquire only backendMapMu and
	// never acquire admission locks, so this adds no reverse lock ordering.
	s.admissionStateMu.Lock()
	defer s.admissionStateMu.Unlock()
	if s.admissionInstance != instance || s.admissionInstanceClosing(instance) {
		return
	}
	instance.backendMu.Lock()
	defer instance.backendMu.Unlock()
	pinned, ok := instance.pinned[name]
	if !ok || !pinned.reprobeInFlight {
		return
	}
	pinned.reprobeInFlight = false
	if !consumeFingerprint {
		instance.pinned[name] = pinned
		return
	}
	pinned.setupCacheFingerprint = fingerprint
	pinned.setupCacheFingerprintKnown = true
	if cacheStable && probeErr == nil {
		descriptor := s.admissionBackendDescriptor(name, probed, nil)
		instance.replaceBackendDescriptorLocked(name, descriptor)
		s.replaceBackend(name, probed)
		delete(instance.pinned, name)
		if s.admissionUnprobeableBackends != nil {
			delete(s.admissionUnprobeableBackends, name)
		}
		return
	}
	if cacheStable && probeErr != nil {
		if descriptor, ok := instance.descriptors[name]; ok {
			instance.replaceBackendDescriptorLocked(name, s.admissionBackendDescriptor(name, descriptor.backend, probeErr))
		}
		if s.admissionUnprobeableBackends == nil {
			s.admissionUnprobeableBackends = make(map[string]error)
		}
		s.admissionUnprobeableBackends[name] = probeErr
	}
	instance.pinned[name] = pinned
}

func (s *Server) admissionDaemonBoot() (model.BootRef, error) {
	s.admissionDaemonBootOnce.Do(func() {
		bootID, err := s.admissionDaemonID("boot")
		if err != nil {
			s.admissionDaemonBootRefErr = err
			return
		}
		ownerID, err := s.admissionDaemonID("owner")
		if err != nil {
			s.admissionDaemonBootRefErr = err
			return
		}
		s.admissionDaemonBootRef, s.admissionDaemonBootRefErr = model.NewBootRef(bootID, ownerID)
	})
	return s.admissionDaemonBootRef, s.admissionDaemonBootRefErr
}

func (s *Server) admissionDaemonID(prefix string) (string, error) {
	entropy, err := randomToken()
	if err != nil {
		return "", fmt.Errorf("generate %s admission daemon identity entropy: %w", prefix, err)
	}
	return s.nextID(prefix) + "_" + entropy, nil
}

type servedCoordinatorLaunchContainment struct {
	controller *launch.LaunchController
}

func (c servedCoordinatorLaunchContainment) ContainAndVerify(ctx context.Context, launchContext launch.LaunchContext, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	if c.controller == nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, launch.ErrCustodianRequired
	}
	return c.controller.ContainAndVerifyWithCleanup(ctx, launchContext, group, cause)
}

func openAdmissionBootstrapper(ctx context.Context, s *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
	return openAdmissionBootstrapperWithOptions(ctx, s, admissionBootstrapperOpenOptions{})
}

func openAdmissionBootstrapperWithOptions(ctx context.Context, s *Server, openOptions admissionBootstrapperOpenOptions) (*admissionBootstrapper, repository.Repository, io.Closer, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	repoPath := filepath.Join(s.stateRoot, admissionRepositoryFile)
	anchorPath := filepath.Join(s.stateRoot, admissionAnchorFile)
	var openedIdentity bboltrepo.FileIdentity
	var repo *bboltrepo.Repository
	repoCreated := false
	var err error
	if openOptions.openExistingNoInitialize {
		if openOptions.expectedRepositoryIdentity == nil {
			return nil, nil, nil, fmt.Errorf("%w: expected admission repository identity is required for no-init recovery open", repository.ErrInvalidRecord)
		}
		repo, err = openExistingAdmissionRepositoryWithContentionRetry(ctx, repoPath, s.socketPath)
	} else {
		repo, repoCreated, err = openAdmissionRepositoryWithContentionRetry(ctx, repoPath, anchorPath, s.socketPath)
	}
	if err != nil {
		return nil, nil, nil, translateAdmissionRepositoryOpenError(repoPath, err)
	}
	openedIdentity, err = repo.OpenedFileIdentity()
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	if openOptions.expectedRepositoryIdentity != nil && openedIdentity != *openOptions.expectedRepositoryIdentity {
		_ = repo.Close()
		return nil, nil, nil, admissionRootRepositoryIdentityMismatchError(repoPath, *openOptions.expectedRepositoryIdentity, openedIdentity)
	}
	if openOptions.verifyInitializedStructure {
		// Recovery preflight already proved this dev+ino had the required schema,
		// buckets, meta keys, valid meta, and matching anchor. The no-init open
		// rechecks the same inode without creating or repairing buckets before
		// Begin can run.
		if err := verifyOpenedAdmissionRepositoryInitialized(ctx, repoPath, repo); err != nil {
			_ = repo.Close()
			return nil, nil, nil, err
		}
	}
	if err := auditOpenedAdmissionRepository(ctx, repo); err != nil {
		if !admissionAuditFindingsRepairableAtStartup(err) {
			_ = repo.Close()
			if errors.Is(err, repository.ErrCorruptRecord) {
				s.safetyLatch.Trip(err)
			}
			return nil, nil, nil, err
		}
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		_ = repo.Close()
		if errors.Is(err, repository.ErrCorruptRecord) {
			s.safetyLatch.Trip(err)
		}
		return nil, nil, nil, err
	}
	anchorOptions := []authority.FileAnchorOption{
		authority.WithFileAnchorFailStopHook(func(reason string) {
			s.safetyLatch.Trip(safetyFailStopReason(reason))
		}),
	}
	if openOptions.requireInitializedAnchor {
		anchorOptions = append(anchorOptions, authority.WithFileAnchorRequireInitialized())
	}
	if !repoCreated {
		anchorOptions = append(anchorOptions, authority.WithFileAnchorRequireInitialized())
	}
	anchor := authority.NewFileAnchor(
		anchorPath,
		dbUUID,
		schemaMajor,
		anchorOptions...,
	)
	options := []authority.BootstrapperOption{authority.WithAnchor(anchor), authority.WithSafetyLatch(s.safetyLatch)}
	if s.admissionRuntime != nil {
		options = append(options, authority.WithQuiescenceVerifier(s.admissionRuntime.quiescenceVerifier()))
	}
	bootstrapper, err := authority.NewBootstrapper(repo, options...)
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	return bootstrapper, repo, repo, nil
}

func translateAdmissionRepositoryOpenError(repoPath string, err error) error {
	var mismatch bboltrepo.FileIdentityMismatchError
	if errors.As(err, &mismatch) {
		return admissionRootRepositoryIdentityMismatchError(repoPath, mismatch.Expected, mismatch.Opened)
	}
	var unsupportedSchema bboltrepo.UnsupportedAuthorityMetaSchemaVersionError
	if errors.As(err, &unsupportedSchema) {
		return unsupportedAdmissionRootSchemaError(repoPath, unsupportedSchema.SchemaVersion, err)
	}
	return err
}

func openAdmissionRepositoryWithContentionRetry(ctx context.Context, repoPath, anchorPath, socketPath string) (*bboltrepo.Repository, bool, error) {
	timeout, err := admissionContentionAttemptTimeout(ctx, admissionRepositoryOpenTimeout)
	if err != nil {
		return nil, false, err
	}
	repo, created, err := openAdmissionRepositoryOnce(repoPath, anchorPath, timeout)
	if err == nil {
		return repo, created, err
	}
	if admissionRepositoryCreateRaced(repoPath, err) {
		return nil, false, waitForConcurrentAdmissionRepositoryCreator(ctx, socketPath, err)
	}
	if !errors.Is(err, bolt.ErrTimeout) {
		return nil, false, err
	}
	err = retryAdmissionRepositoryContention(ctx, repoPath, socketPath, err, func(timeout time.Duration) (bool, error) {
		repo, created, err = openAdmissionRepositoryOnce(repoPath, anchorPath, timeout)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, bolt.ErrTimeout) {
			return true, err
		}
		return false, err
	})
	if err != nil {
		return nil, false, err
	}
	return repo, created, nil
}

func openAdmissionRepositoryOnce(repoPath, anchorPath string, timeout time.Duration) (*bboltrepo.Repository, bool, error) {
	if _, err := os.Stat(repoPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, false, err
		}
		if err := requireAdmissionAnchorAbsentForCreate(anchorPath); err != nil {
			return nil, false, err
		}
		if admissionRepositoryBeforeCreateForTest != nil {
			if err := admissionRepositoryBeforeCreateForTest(); err != nil {
				return nil, false, err
			}
		}
		repo, createErr := bboltrepo.Create(repoPath, &bolt.Options{Timeout: timeout})
		return repo, true, createErr
	}
	repo, err := bboltrepo.OpenExisting(repoPath, &bolt.Options{Timeout: timeout})
	return repo, false, err
}

// admissionRepositoryCreateRaced recognizes only the create-specific race:
// openAdmissionRepositoryOnce had already observed no repository and no
// anchor, then bbolt's O_EXCL create lost to a regular file at the same path.
// A pre-existing, malformed, symlinked, or otherwise invalid root never takes
// this branch because it is opened through OpenExisting instead.
func admissionRepositoryCreateRaced(repoPath string, err error) bool {
	var alreadyExists bboltrepo.RepositoryAlreadyExistsError
	if !errors.As(err, &alreadyExists) || alreadyExists.Path != repoPath {
		return false
	}
	info, statErr := os.Lstat(repoPath)
	return statErr == nil && info.Mode().IsRegular()
}

// waitForConcurrentAdmissionRepositoryCreator waits only after the exact
// O_EXCL create race above. It does not retry repository validation or convert
// invalid-record errors into success: the only convergence proof is that the
// peer becomes live at the expected socket, after which daemonlaunch performs
// its token-authenticated hello verification before reporting ExistingDaemon.
func waitForConcurrentAdmissionRepositoryCreator(ctx context.Context, socketPath string, cause error) error {
	contentionCtx, cancel := admissionContentionContext(ctx)
	defer cancel()
	for {
		if err := contentionCtx.Err(); err != nil {
			return concurrentAdmissionRepositoryCreatorUnavailable(ctx, cause)
		}
		dialTimeout, err := admissionContentionAttemptTimeout(contentionCtx, admissionRepositoryOpenTimeout)
		if err != nil {
			return concurrentAdmissionRepositoryCreatorUnavailable(ctx, cause)
		}
		if admissionSocketDialableWithin(socketPath, dialTimeout) {
			return DaemonAlreadyListeningError{SocketPath: socketPath}
		}
		select {
		case <-contentionCtx.Done():
			return concurrentAdmissionRepositoryCreatorUnavailable(ctx, cause)
		case <-time.After(admissionContentionRetryDelay):
		}
	}
}

func concurrentAdmissionRepositoryCreatorUnavailable(ctx context.Context, cause error) error {
	if parentErr := ctx.Err(); parentErr != nil {
		return errors.Join(cause, parentErr)
	}
	return cause
}

func requireAdmissionAnchorAbsentForCreate(anchorPath string) error {
	if _, err := os.Lstat(anchorPath); err == nil {
		return fmt.Errorf("%w: anchor exists without admission repository: %s", authority.ErrAnchorInvariant, anchorPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func admissionAuditFindingsRepairableAtStartup(err error) bool {
	kinds := repository.IntegrityFindingKinds(err)
	if len(kinds) == 0 {
		return false
	}
	for _, kind := range kinds {
		switch kind {
		case "projection", "quarantine":
		default:
			return false
		}
	}
	return true
}

func openExistingAdmissionRepositoryWithContentionRetry(ctx context.Context, repoPath, socketPath string) (*bboltrepo.Repository, error) {
	timeout, err := admissionContentionAttemptTimeout(ctx, admissionRepositoryOpenTimeout)
	if err != nil {
		return nil, err
	}
	repo, err := bboltrepo.OpenExisting(repoPath, &bolt.Options{Timeout: timeout})
	if err == nil || !errors.Is(err, bolt.ErrTimeout) {
		return repo, err
	}
	err = retryAdmissionRepositoryContention(ctx, repoPath, socketPath, err, func(timeout time.Duration) (bool, error) {
		repo, err = bboltrepo.OpenExisting(repoPath, &bolt.Options{Timeout: timeout})
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, bolt.ErrTimeout) {
			return true, err
		}
		return false, err
	})
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func openReadOnlyAdmissionRepositoryWithContentionRetry(ctx context.Context, repoPath, socketPath string) (*bboltrepo.Repository, error) {
	timeout, err := admissionContentionAttemptTimeout(ctx, admissionRepositoryOpenTimeout)
	if err != nil {
		return nil, err
	}
	repo, err := bboltrepo.OpenExistingReadOnly(repoPath, &bolt.Options{Timeout: timeout})
	if err == nil {
		return repo, nil
	}
	if !errors.Is(err, bolt.ErrTimeout) {
		return nil, translateAdmissionRepositoryOpenError(repoPath, err)
	}
	err = retryAdmissionRepositoryContention(ctx, repoPath, socketPath, err, func(timeout time.Duration) (bool, error) {
		repo, err = bboltrepo.OpenExistingReadOnly(repoPath, &bolt.Options{Timeout: timeout})
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, bolt.ErrTimeout) {
			return true, err
		}
		return false, err
	})
	if err != nil {
		return nil, translateAdmissionRepositoryOpenError(repoPath, err)
	}
	return repo, nil
}

func auditOpenedAdmissionRepository(ctx context.Context, repo *bboltrepo.Repository) error {
	if repo == nil {
		return fmt.Errorf("%w: admission repository is required", repository.ErrInvalidRecord)
	}
	return repo.AuditIntegrity(ctx)
}

func retryAdmissionRepositoryContention(ctx context.Context, repoPath, socketPath string, cause error, retry func(time.Duration) (bool, error)) error {
	contentionCtx, cancel := admissionContentionContext(ctx)
	defer cancel()
	last := cause
	for {
		if err := contentionCtx.Err(); err != nil {
			return admissionContentionExpiredError(ctx, repoPath, socketPath, last, err)
		}
		dialTimeout, err := admissionContentionAttemptTimeout(contentionCtx, admissionRepositoryOpenTimeout)
		if err != nil {
			return admissionContentionExpiredError(ctx, repoPath, socketPath, last, err)
		}
		if admissionSocketDialableWithin(socketPath, dialTimeout) {
			return DaemonAlreadyListeningError{SocketPath: socketPath}
		}
		if err := contentionCtx.Err(); err != nil {
			return admissionContentionExpiredError(ctx, repoPath, socketPath, last, err)
		}
		openTimeout, err := admissionContentionAttemptTimeout(contentionCtx, admissionRepositoryOpenTimeout)
		if err != nil {
			return admissionContentionExpiredError(ctx, repoPath, socketPath, last, err)
		}
		done, err := retry(openTimeout)
		if done {
			return err
		}
		if err != nil {
			last = err
		}
		select {
		case <-contentionCtx.Done():
			return admissionContentionExpiredError(ctx, repoPath, socketPath, last, contentionCtx.Err())
		case <-time.After(admissionContentionRetryDelay):
		}
	}
}

func admissionContentionAttemptTimeout(ctx context.Context, max time.Duration) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if max <= 0 {
		return 0, nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return max, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 0, context.DeadlineExceeded
	}
	if remaining < max {
		return remaining, nil
	}
	return max, nil
}

func admissionContentionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, admissionContentionFallback)
}

func admissionContentionExpiredError(parent context.Context, repoPath, socketPath string, cause, expiration error) error {
	if errors.Is(expiration, context.DeadlineExceeded) {
		return AdmissionRootBusyError{Path: repoPath, SocketPath: socketPath, Cause: cause}
	}
	if parentErr := parent.Err(); parentErr != nil {
		return parentErr
	}
	return expiration
}

func recoverAdmissionBeforeReady(ctx context.Context, session *authority.RecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch, clock engine.Clock) error {
	return newAdmissionRecoveryExecutor(session, launchPort, latch, clock).Recover(ctx)
}

func recoverAdmissionBeforeReadyReport(ctx context.Context, session admissionRecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch, clocks ...engine.Clock) (AdmissionRecoveryReport, error) {
	return newAdmissionRecoveryExecutor(session, launchPort, latch, clocks...).RecoverReport(ctx)
}

type servedSubmissionCoordinator struct {
	ready  *authority.Ready
	owner  model.OwnerID
	launch *launch.LaunchController
	latch  *SafetyLatch
}

type admissionParkableBackend interface {
	AdmissionParkable() bool
}

type admissionControlledRunnerBackend interface {
	AdmissionControlledRunner() bool
}

func admissionBackendExternalRunner(backend engine.Backend) bool {
	if backend == nil {
		return true
	}
	if parkable, ok := backend.(admissionParkableBackend); ok {
		return !parkable.AdmissionParkable()
	}
	return true
}

func admissionBackendControlledRunner(backend engine.Backend) bool {
	if backend == nil {
		return false
	}
	if controlled, ok := backend.(admissionControlledRunnerBackend); ok {
		return controlled.AdmissionControlledRunner()
	}
	return false
}

type admissionLaunchBinding struct {
	coordinator       *servedSubmissionCoordinator
	jobID             model.JobID
	attempt           model.AttemptRef
	containmentIntent *launch.ContainmentIntent
}

type ordinalBoundSession interface {
	TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
}

func (s *Server) admissionTurnEvents(ctx context.Context, run jobRun, input engine.TurnInput, ordinal model.LaunchOrdinal) (<-chan engine.Event, error) {
	if err := ordinal.Validate(); err != nil {
		return nil, classifyFailureError(terminalFailureBackendNotStarted, err)
	}
	// Snapshot-checkout: launch preparation and the turn must not hold
	// admissionStateMu (see activeWork). The coordinator-identity comparison
	// runs against the snapshot; a submission coordinator replaced by a
	// sequential re-Serve fails the identity check exactly as before.
	s.admissionStateMu.RLock()
	submission := s.admissionSubmission
	ready := s.admissionInstance != nil && submission != nil
	s.admissionStateMu.RUnlock()
	if !ready || run.admissionLaunch.coordinator != submission {
		return nil, classifyFailureError(terminalFailureBackendNotStarted, authority.ErrNotReady)
	}
	if run.admissionLaunch.coordinator == nil {
		return nil, classifyFailureError(terminalFailureBackendNotStarted, custodian.ErrSupervisorUnavailable)
	}
	session, ok := run.session.(ordinalBoundSession)
	if !ok {
		return nil, classifyFailureError(terminalFailureBackendNotStarted, fmt.Errorf("%w: backend session does not support ordinal-bound runners", custodian.ErrSupervisorUnavailable))
	}
	runner, err := run.admissionLaunch.coordinator.LaunchRunner(run.admissionLaunch, ordinal)
	if err != nil {
		// LaunchRunner can fail after launch preparation has crossed the point
		// where backend work may be possible. Its provenance is therefore
		// deliberately conservative rather than claiming no backend started.
		return nil, classifyFailureError(terminalFailureBackendRan, err)
	}
	if run.active != nil {
		runner = admissionActiveRunner{inner: runner, active: run.active}
	}
	events, err := session.TurnWithRunner(ctx, input, runner)
	if err != nil {
		return nil, classifyFailureError(terminalFailureBackendRan, err)
	}
	return events, nil
}

type admissionActiveRunner struct {
	inner  command.Runner
	active *activeJob
}

func (r admissionActiveRunner) Start(ctx context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	running, err := r.inner.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	if r.active.recordAdmissionCommand(running) {
		interruptCtx, cancel := context.WithTimeout(context.Background(), admissionDetachedCleanupTimeout)
		err = running.Interrupt(interruptCtx)
		cancel()
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func (c *servedSubmissionCoordinator) SubmitIdentified(ctx context.Context, request authority.AcceptRequest) (authority.AcceptResult, error) {
	if c == nil || c.ready == nil {
		return authority.AcceptResult{}, authority.ErrNotReady
	}
	request.Mode = model.ModeIdentifiedFenced
	accepted, err := c.ready.AcceptAndClaim(ctx, request, c.owner)
	if err != nil {
		return accepted, err
	}
	if accepted.Record.Terminal != nil || accepted.Replayed {
		return accepted, nil
	}
	return accepted, nil
}

func (c *servedSubmissionCoordinator) LaunchRunner(binding admissionLaunchBinding, ordinal model.LaunchOrdinal) (command.Runner, error) {
	if c == nil || c.launch == nil {
		return nil, custodian.ErrSupervisorUnavailable
	}
	return c.launch.Runner(launch.RunnerBinding{
		Context: launch.LaunchContext{
			JobID:   binding.jobID,
			Attempt: binding.attempt,
			Ordinal: ordinal,
		},
		ContainmentIntent: binding.containmentIntent,
	})
}

func (c *servedSubmissionCoordinator) failStop(ctx context.Context, err error) error {
	if c == nil || c.ready == nil {
		return authority.ErrNotReady
	}
	failStopCtx, cancel := detachedAdmissionFailStopContext(ctx)
	defer cancel()
	var stopErr error
	if err == nil {
		stopErr = c.ready.FailStop(failStopCtx, "")
	} else {
		stopErr = c.ready.FailStop(failStopCtx, err.Error())
	}
	if stopErr == nil {
		c.latch.Trip(err)
	}
	return stopErr
}

func (s *Server) failStopAdmissionReady(ctx context.Context, err error) error {
	if s == nil {
		return authority.ErrNotReady
	}
	// Snapshot-checkout (see activeWork): FailStop must not hold the state
	// lock — it is on the safety path that races closeServeAdmission.
	s.admissionStateMu.RLock()
	ready := s.admissionReady
	ok := s.admissionInstance != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !ok {
		return authority.ErrNotReady
	}
	var stopErr error
	if err == nil {
		stopErr = ready.FailStop(ctx, "")
	} else {
		stopErr = ready.FailStop(ctx, err.Error())
	}
	if stopErr == nil {
		s.safetyLatch.Trip(err)
	}
	return stopErr
}

func (s *Server) failStopAdmissionRepositoryCorruption(ctx context.Context, operation string, err error) error {
	if s == nil {
		return err
	}
	s.admissionStateMu.RLock()
	ready := s.admissionReady
	ok := s.admissionInstance != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !ok {
		if s.safetyLatch != nil && errors.Is(err, repository.ErrCorruptRecord) {
			s.safetyLatch.Trip(err)
		}
		return err
	}
	return ready.FailStopRepositoryCorruption(ctx, operation, err)
}

func detachedAdmissionFailStopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), admissionFailStopTimeout)
}

func detachedAdmissionCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), admissionDetachedCleanupTimeout)
}

func admissionDurabilityError(step string, outcome launch.DurabilityOutcome, err error) error {
	switch outcome {
	case launch.CommittedAndAnchored:
		if err == nil {
			return nil
		}
		return fmt.Errorf("%s committed with error: %w", step, err)
	case launch.DefinitelyNotCommitted:
		if err == nil {
			return fmt.Errorf("%w: %s", launch.ErrDurabilityNotCommitted, step)
		}
		return fmt.Errorf("%w: %s: %v", launch.ErrDurabilityNotCommitted, step, err)
	default:
		if err == nil {
			return fmt.Errorf("%w: %s", launch.ErrDurabilityUnknown, step)
		}
		return fmt.Errorf("%w: %s: %v", launch.ErrDurabilityUnknown, step, err)
	}
}

type servedLaunchAuthority struct {
	ready *authority.Ready
	latch *SafetyLatch
}

func (a servedLaunchAuthority) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef) (launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return launch.DefinitelyNotCommitted, authority.ErrNotReady
	}
	applied, err := a.ready.BindGroup(ctx, jobID, ref, ordinal, group)
	return applied.Durability, err
}

func (a servedLaunchAuthority) AllocateGrant(ctx context.Context, ref model.AttemptRef, ordinal model.LaunchOrdinal) (model.LaunchGrant, launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return model.LaunchGrant{}, launch.DefinitelyNotCommitted, authority.ErrNotReady
	}
	return a.ready.AllocateGrant(ctx, ref, ordinal)
}

func (a servedLaunchAuthority) RecordReleaseOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, outcome model.LaunchReleaseOutcome) (launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return launch.DefinitelyNotCommitted, authority.ErrNotReady
	}
	applied, err := a.ready.RecordReleaseOutcome(ctx, jobID, ref, ordinal, outcome)
	return applied.Durability, err
}

func (a servedLaunchAuthority) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return launch.DefinitelyNotCommitted, authority.ErrNotReady
	}
	if admissionRecordReleaseBeforeCommitForTest != nil {
		if err := admissionRecordReleaseBeforeCommitForTest(); err != nil {
			return launch.DefinitelyNotCommitted, err
		}
	}
	applied, err := a.ready.RecordRelease(ctx, jobID, ref, ordinal, child, evidence)
	return applied.Durability, err
}

func (a servedLaunchAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return launch.DefinitelyNotCommitted, authority.ErrNotReady
	}
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return applied.Durability, err
}

func (a servedLaunchAuthority) FailStop(ctx context.Context, err error) error {
	if a.ready == nil {
		return authority.ErrNotReady
	}
	var stopErr error
	if err == nil {
		stopErr = a.ready.FailStop(ctx, "")
	} else {
		stopErr = a.ready.FailStop(ctx, err.Error())
	}
	if stopErr == nil {
		a.latch.Trip(err)
	}
	return stopErr
}

type servedAdmissionAuthority struct {
	ready *authority.Ready
	latch *SafetyLatch
	clock engine.Clock
}

func (a *servedAdmissionAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RequestCancel(ctx context.Context, jobID model.JobID) (coordinator.StepResult, error) {
	applied, err := a.ready.RequestCancel(ctx, jobID)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome, contract *engine.ContractStamp) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordOutcome(ctx, jobID, ref, outcome, contract)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RecordResult(ctx context.Context, jobID model.JobID, ref model.AttemptRef, receipt model.ResultReceipt) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordResult(ctx, jobID, ref, receipt)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent) (coordinator.StepResult, error) {
	if a.clock != nil {
		endedAt := a.clock.Now().UTC()
		intent.FinalAttemptEndedAt = &endedAt
	}
	applied, err := a.ready.Finalize(ctx, jobID, ref, intent)
	return admissionStepResult(applied, err)
}

func admissionStepResult(applied authority.ApplyResult, err error) (coordinator.StepResult, error) {
	if err != nil {
		return coordinator.StepResult{}, err
	}
	return coordinator.StepResult{Record: applied.Record, Projection: applied.Projection, Changed: applied.Changed}, nil
}

func (a *servedAdmissionAuthority) Snapshot(ctx context.Context, jobID model.JobID) (coordinator.JobSnapshot, error) {
	image, err := a.ready.LoadJob(ctx, jobID)
	if err != nil {
		return coordinator.JobSnapshot{}, err
	}
	if err := authorityImageSafetyCorruption(image); err != nil {
		return coordinator.JobSnapshot{}, a.ready.FailStopRepositoryCorruption(ctx, "admission snapshot", err)
	}
	if image.Safety.State != repository.RecordValid {
		return coordinator.JobSnapshot{}, fmt.Errorf("safety state = %s", image.Safety.State)
	}
	if image.Projection.State != repository.RecordValid {
		return coordinator.JobSnapshot{}, fmt.Errorf("projection state = %s", image.Projection.State)
	}
	return coordinator.JobSnapshot{Record: image.Safety.Value, Projection: image.Projection.Value}, nil
}

func (a *servedAdmissionAuthority) RecoveryPlan(ctx context.Context, jobID model.JobID, trigger model.RecoveryTrigger) (model.RecoveryPlan, error) {
	snapshot, err := a.Snapshot(ctx, jobID)
	if err != nil {
		return model.RecoveryPlan{}, err
	}
	if trigger == model.RecoveryCancelAfterGrant && !admissionHasAuthorizationEvidence(snapshot.Record) {
		return admissionCancelBeforeAuthorizationPlan(snapshot.Record), nil
	}
	return model.PlanRecovery(snapshot.Record, trigger)
}

func (a *servedAdmissionAuthority) HasOwnedWork(ctx context.Context) (bool, error) {
	snapshot, err := a.ready.RuntimeSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0, nil
}

func (a *servedAdmissionAuthority) FailStop(ctx context.Context, err error) error {
	if a == nil || a.ready == nil {
		return authority.ErrNotReady
	}
	var stopErr error
	if err == nil {
		stopErr = a.ready.FailStop(ctx, "")
	} else {
		stopErr = a.ready.FailStop(ctx, err.Error())
	}
	if stopErr == nil {
		a.latch.Trip(err)
	}
	return stopErr
}

func admissionHasAuthorizationEvidence(record model.SafetyRecord) bool {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && (launch.Grant != nil || launch.Released != nil) {
			return true
		}
	}
	return false
}

func admissionCancelBeforeAuthorizationPlan(record model.SafetyRecord) model.RecoveryPlan {
	plan := model.RecoveryPlan{BasedOnRevision: record.Revision}
	if record.Terminal != nil {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryFinalizeCertified}
		return plan
	}
	intent := model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseCanceledBeforeAuthorization,
	}
	if _, err := model.DeriveTerminalCertificate(record, intent); err == nil {
		finalize := model.Finalize{Ref: record.Attempt.Ref, Intent: intent}
		plan.Next = model.RecoveryAction{Kind: model.RecoveryFinalizeCertified, Finalize: &finalize}
		return plan
	}
	if !admissionAllLaunchGroupsQuiescent(record) {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryRetireThenFinalize}
		return plan
	}
	plan.Next = model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}
	return plan
}

func admissionAllLaunchGroupsQuiescent(record model.SafetyRecord) bool {
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if !ok || launch.Group == nil || launch.Quiescence == nil {
			return false
		}
	}
	return true
}

func admissionUnquiescedOrdinals(record model.SafetyRecord) []model.LaunchOrdinal {
	ordinals := make([]model.LaunchOrdinal, 0, record.Attempt.Launches.Count())
	for _, ordinal := range record.Attempt.Launches.FilledOrdinals() {
		launch, ok := record.Attempt.Launches.Get(ordinal)
		if ok && launch.Group != nil && launch.Quiescence == nil {
			ordinals = append(ordinals, ordinal)
		}
	}
	return ordinals
}

func jsonFieldPresent(raw json.RawMessage, field string) (bool, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return false, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return false, err
	}
	_, ok := envelope[field]
	return ok, nil
}

func (s *Server) handleIdentifiedJobSubmit(ctx context.Context, raw json.RawMessage, precheck strictJobSubmitPrecheck) requestOutcome {
	if s.admissionCurrentServeClosing() {
		return requestOutcome{err: admissionProtocolError(errAdmissionClosing)}
	}
	// Snapshot-checkout: the whole submit (validation, session construction,
	// durable acceptance) runs against the checked-out instance, never under
	// admissionStateMu (see activeWork). A submit racing closeServeAdmission
	// gets typed errors from the closing objects.
	s.admissionStateMu.RLock()
	instance := s.admissionInstance
	s.admissionStateMu.RUnlock()
	if s.admissionInstanceClosing(instance) {
		return requestOutcome{err: admissionProtocolError(errAdmissionClosing)}
	}
	if instance == nil || !instance.policy.strictRouteEnabled || instance.ready == nil || instance.submission == nil {
		reason := "admission authority is not ready"
		if instance != nil && instance.policy.strictRouteDisabledReason != "" {
			reason = instance.policy.strictRouteDisabledReason
		}
		return requestOutcome{err: strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectUnavailableNativeRuntime,
			reason,
			protocol.ErrorData{},
		)}
	}

	requestKey, err := model.NewRequestKey(precheck.WorkspaceKey, precheck.RequestID)
	if err != nil {
		return requestOutcome{err: strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)}
	}

	rawTaskSpec := precheck.RawTaskSpec

	replay, err := instance.ready.LookupReplay(ctx, requestKey)
	if err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}
	switch replay.State {
	case authority.ReplayLive:
		if replay.Binding.Mode != model.ModeIdentifiedFenced {
			return requestOutcome{err: strictAdmissionReplayConflictError("request is already bound to a different admission mode")}
		}
		if errObj := strictAdmissionReplayIdentityError(replay.Binding.TaskIdentity, rawTaskSpec); errObj != nil {
			return requestOutcome{err: errObj}
		}
		return requestOutcome{result: protocol.JobSubmitResult{
			JobID:        replay.Record.JobID.String(),
			State:        admissionState(replay.Projection.Public),
			Deduplicated: true,
		}}
	case authority.ReplayExpired:
		if errObj := strictAdmissionReplayIdentityError(replay.Tombstone.TaskIdentity, rawTaskSpec); errObj != nil {
			return requestOutcome{err: errObj}
		}
		return requestOutcome{err: admissionProtocolError(authority.ErrRequestExpired)}
	}

	var params protocol.JobSubmitParams
	if err := decodeStrict(raw, &params); err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{})}
	}
	spec := params.TaskSpec
	var descriptor admissionBackendDescriptor
	if spec.Backend != "" {
		s.refreshAdmissionBackendOnSetupCacheChange(ctx, instance, spec.Backend)
		if s.admissionInstanceClosing(instance) {
			return requestOutcome{err: admissionProtocolError(errAdmissionClosing)}
		}
		var fenceability ServeBackendFenceability
		var ok bool
		descriptor, fenceability, ok = instance.descriptorAndFenceability(spec.Backend)
		if !ok {
			return requestOutcome{err: strictAdmissionProtocolError(
				protocol.ErrorBackendUnavailable,
				protocol.AdmissionRejectUnsupportedBackend,
				"backend is unavailable",
				protocol.ErrorData{Backend: spec.Backend},
			)}
		}
		if !fenceability.Fenceable {
			message := fenceability.Reason
			if message == "" {
				message = "backend is not fenceable for strict identified admission"
			}
			return requestOutcome{err: strictAdmissionProtocolError(
				protocol.ErrorCapabilityMissing,
				protocol.AdmissionRejectUnfenceableBackend,
				message,
				protocol.ErrorData{Backend: spec.Backend},
			)}
		}
	}
	if errObj := validateTaskSpecEnvelope(raw); errObj != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(errObj.Message, protocol.ErrorData{Backend: spec.Backend})}
	}
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return requestOutcome{err: strictAdmissionInvalidConfigError("taskSpec requires backend, absolute cwd, write, and prompt", protocol.ErrorData{Backend: spec.Backend})}
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMs)
	if errObj != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(errObj.Message, protocol.ErrorData{Backend: spec.Backend})}
	}
	policy, err := s.resolvePolicy(spec.Policy)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	if descriptor.backend == nil {
		var ok bool
		descriptor, ok = instance.descriptor(spec.Backend)
		if !ok {
			return requestOutcome{err: strictAdmissionProtocolError(
				protocol.ErrorBackendUnavailable,
				protocol.AdmissionRejectUnsupportedBackend,
				"backend is unavailable",
				protocol.ErrorData{Backend: spec.Backend},
			)}
		}
	}
	canonicalCWD, err := engine.CanonicalWorkspace(spec.CWD)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	workspaceLayoutKey := model.WorkspaceKey(engine.WorkspaceKey(canonicalCWD))
	taskIdentity, err := model.TaskIdentityFromRawTaskSpec(rawTaskSpec)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	if !instance.policy.strictRuntimeAvailable() {
		return requestOutcome{err: strictAdmissionRuntimeUnavailableError(instance.policy.runtimeAssessment, protocol.ErrorData{Backend: spec.Backend})}
	}

	// The authority owns job IDs, so an isolated Codex home cannot be named
	// until after durable acceptance. Creating a Codex adapter session does not
	// start its app-server process; it only prepares the session object that the
	// later admitted turn will launch. Other backends retain the established
	// construct-before-accept ordering.
	isolateCodexHome := spec.Backend == "codex" && !s.codexHomeInherit
	var session engine.Session
	admissionSessionID := s.nextID("ses")
	if !isolateCodexHome {
		session, err = descriptor.backend.Start(ctx, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout})
		if err != nil {
			return requestOutcome{err: backendError(err)}
		}
		if _, ok := session.(ordinalBoundSession); !ok {
			err := admissionBackendContractViolationError{backend: spec.Backend}
			return requestOutcome{err: strictAdmissionProtocolError(
				protocol.ErrorCapabilityMissing,
				protocol.AdmissionRejectUnfenceableBackend,
				err.Error(),
				protocol.ErrorData{Backend: spec.Backend},
			)}
		}
		// CLI-adapter sessions have NO id at Start time: the backend stream
		// assigns it during the first turn. An empty ID is therefore normal;
		// served supplies a durable admission-session ID in that case.
		if backendSessionID := session.ID(); backendSessionID != "" {
			if err := authority.ValidateSessionID(backendSessionID); err != nil {
				return requestOutcome{err: backendSessionMetadataError(spec.Backend, backendSessionID, err)}
			}
			admissionSessionID = backendSessionID
		}
	}
	request := authority.AcceptRequest{
		RequestKey:         requestKey,
		WorkspaceLayoutKey: workspaceLayoutKey,
		TaskIdentity:       taskIdentity,
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          admissionSessionID,
	}
	s.admissionSubmitMu.Lock()
	if s.admissionInstanceClosing(instance) {
		s.admissionSubmitMu.Unlock()
		return requestOutcome{err: admissionProtocolError(errAdmissionClosing)}
	}
	if !s.admissionInstanceStillReadyLocked(instance) {
		s.admissionSubmitMu.Unlock()
		return requestOutcome{err: admissionProtocolError(authority.ErrNotReady)}
	}
	accepted, err := instance.submission.SubmitIdentified(ctx, request)
	closing := s.admissionInstanceClosing(instance)
	s.admissionSubmitMu.Unlock()
	if err != nil {
		if admissionAcceptCommitted(accepted) {
			return requestOutcome{err: admissionPostAcceptError(accepted, err)}
		}
		return requestOutcome{err: admissionProtocolError(err)}
	}
	if closing {
		if admissionAcceptCommitted(accepted) {
			return requestOutcome{err: admissionPostAcceptError(accepted, errAdmissionClosing)}
		}
		return requestOutcome{err: admissionProtocolError(errAdmissionClosing)}
	}
	jobID := accepted.Record.JobID.String()
	if accepted.Replayed {
		return requestOutcome{result: protocol.JobSubmitResult{
			JobID:        jobID,
			State:        admissionState(accepted.Projection.Public),
			Deduplicated: true,
		}}
	}
	jobModelID := accepted.Record.JobID
	var store *engine.Store
	s.mu.Lock()
	if opened, storeErr := s.storeForCWDLocked(canonicalCWD); storeErr == nil {
		store = opened
		s.jobStores[jobID] = opened
	}
	s.mu.Unlock()
	s.markAdmissionJob(jobID, instance)
	containmentIntent := &launch.ContainmentIntent{}
	run := jobRun{
		jobID:               jobID,
		sessionID:           admissionSessionID,
		backend:             spec.Backend,
		backendImpl:         descriptor.backend,
		cwd:                 spec.CWD,
		model:               spec.Model,
		effort:              spec.Effort,
		store:               store,
		session:             session,
		prompt:              spec.Prompt,
		write:               spec.Write,
		policy:              policy.policy,
		contract:            policy.contract,
		contractName:        policy.name,
		contractHash:        policy.hash,
		timeout:             timeout,
		codexIsolated:       isolateCodexHome,
		admissionControlled: true,
		admissionMode:       model.ModeIdentifiedFenced,
		admissionAccepted:   accepted,
		admissionLaunch: admissionLaunchBinding{
			coordinator:       instance.submission,
			jobID:             jobModelID,
			attempt:           accepted.Record.Attempt.Ref,
			containmentIntent: containmentIntent,
		},
	}
	return requestOutcome{
		result:       protocol.JobSubmitResult{JobID: jobID, State: engine.StateQueued},
		after:        func() { s.handleAdmissionResponseOutcome(ctx, run, true) },
		onAckFailure: func(error) { s.handleAdmissionResponseOutcome(ctx, run, false) },
	}
}

func (s *Server) admissionInstanceStillReadyLocked(instance *admissionInstance) bool {
	s.admissionStateMu.RLock()
	defer s.admissionStateMu.RUnlock()
	return instance != nil &&
		s.admissionInstance == instance &&
		instance.ready != nil &&
		instance.submission != nil
}

func strictAdmissionInvalidConfigError(message string, data protocol.ErrorData) *protocol.ErrorObject {
	return strictAdmissionProtocolError(protocol.ErrorInvalidTaskSpec, protocol.AdmissionRejectInvalidStrictConfig, message, data)
}

func strictAdmissionReplayIdentityError(recorded model.TaskIdentity, rawTaskSpec json.RawMessage) *protocol.ErrorObject {
	if err := model.TaskIdentityMatchesRawTaskSpec(recorded, rawTaskSpec); err != nil {
		if recorded.Algorithm != model.TaskIdentityAlgorithmSHA256 || recorded.Version != model.CurrentTaskIdentityVersion {
			return strictAdmissionProtocolError(
				protocol.ErrorInvalidTaskSpec,
				protocol.AdmissionRejectRequestFingerprintUnsupported,
				err.Error(),
				protocol.ErrorData{},
			)
		}
		return strictAdmissionReplayConflictError(err.Error())
	}
	return nil
}

func strictAdmissionReplayConflictError(message string) *protocol.ErrorObject {
	if strings.TrimSpace(message) == "" {
		message = authority.ErrReplayConflict.Error()
	}
	return strictAdmissionProtocolError(
		protocol.ErrorInvalidTaskSpec,
		protocol.AdmissionRejectReplayConflict,
		message,
		protocol.ErrorData{},
	)
}

func (s *Server) strictRouteDisabledPrecheck() *protocol.ErrorObject {
	if s.admissionCurrentServeClosing() {
		return admissionProtocolError(errAdmissionClosing)
	}
	s.admissionStateMu.RLock()
	defer s.admissionStateMu.RUnlock()

	instance := s.admissionInstance
	if instance == nil || !instance.policy.strictRouteEnabled || instance.ready == nil || instance.submission == nil {
		reason := "admission authority is not ready"
		if instance != nil && instance.policy.strictRouteDisabledReason != "" {
			reason = instance.policy.strictRouteDisabledReason
		}
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectUnavailableNativeRuntime,
			reason,
			protocol.ErrorData{},
		)
	}
	return nil
}

func strictAdmissionRuntimeUnavailableError(assessment custodian.SupportAssessment, data protocol.ErrorData) *protocol.ErrorObject {
	data.RuntimeSupport = runtimeSupportAssessmentData(assessment)
	message := "strict native runtime is unavailable"
	if assessment.Cause != nil {
		message += ": " + assessment.Cause.Error()
	}
	return strictAdmissionProtocolError(protocol.ErrorCapabilityMissing, protocol.AdmissionRejectUnavailableNativeRuntime, message, data)
}

func backendSessionMetadataError(backend, sessionID string, err error) *protocol.ErrorObject {
	message := "backend returned invalid session id"
	if err != nil && err.Error() != "" {
		message += ": " + err.Error()
	}
	return protocol.NewError(protocol.ErrorBackendUnavailable, message, protocol.ErrorData{Backend: backend, SessionID: sessionID})
}

func strictAdmissionProtocolError(code, cause, message string, data protocol.ErrorData) *protocol.ErrorObject {
	data.AdmissionCause = cause
	return protocol.NewError(code, message, data)
}

func runtimeSupportAssessmentData(assessment custodian.SupportAssessment) *protocol.RuntimeSupportAssessmentData {
	data := &protocol.RuntimeSupportAssessmentData{
		Class:       assessment.Class.String(),
		Attempts:    assessment.Attempts,
		CleanupSafe: assessment.CleanupSafe,
	}
	if assessment.Cause != nil {
		data.Cause = assessment.Cause.Error()
	}
	return data
}

func rawTaskSpecFromSubmitParams(raw json.RawMessage) (json.RawMessage, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	taskSpec, ok := envelope["taskSpec"]
	if !ok {
		return nil, errors.New("taskSpec is required")
	}
	return append(json.RawMessage(nil), taskSpec...), nil
}

func (s *Server) launchAdmittedJob(ctx context.Context, run jobRun) {
	var launched jobRun
	var runCtx context.Context
	err := s.withAdmissionJobEffectErr(run.jobID, func() error {
		var ok bool
		var launchErr error
		launched, runCtx, ok, launchErr = s.prepareAdmittedJobLaunch(ctx, run)
		if launchErr != nil {
			return launchErr
		}
		if !ok {
			return nil
		}
		return nil
	})
	if err != nil {
		// prepareAdmittedJobLaunch only reads admission state, prepares the
		// accepted job's Codex session when needed, and registers the active job;
		// it does not call LaunchRunner or start a backend turn.
		if launched.active == nil {
			launched.admissionLaunchFailed = true
			s.finalizeAdmittedLaunchFailure(launched, err)
		} else {
			s.completeRunFailure(launched, engine.StateFailed, "", nil, terminalFailureBackendNotStarted, err)
		}
		return
	}
	if launched.active == nil {
		return
	}
	s.runJob(runCtx, launched)
}

func (s *Server) finalizeAdmittedLaunchFailure(run jobRun, cause error) {
	jobID, err := model.NewJobID(run.jobID)
	if err != nil {
		s.handleRunFinalizationError(run, err)
		return
	}
	err = s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
		snapshot, err := coord.Snapshot(context.Background(), jobID)
		if err != nil {
			return err
		}
		if snapshot.Record.Terminal != nil || snapshot.Record.Cancel != nil {
			return nil
		}
		if err := coord.Finalize(context.Background(), jobID, model.TerminalIntent{
			Outcome: model.OutcomeFailed,
			Cause:   model.CauseDaemonRestartedBeforeAuthorization,
		}); err != nil {
			if err := reconcileAdmissionFinalizationContention(context.Background(), coord, jobID, err); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.handleRunFinalizationError(run, err)
		return
	}
	if err := s.recordFailureMetadata(run, terminalFailureFor(terminalFailureBackendNotStarted, cause, false)); err != nil {
		log.Printf("agentbus daemon: job %s preparation failure metadata persistence failed: %v", run.jobID, err)
	}
	if err := s.cleanupAdmittedCodexHomeFromCommittedTerminal(run); err != nil {
		s.handleRunFinalizationError(run, err)
	}
}

func (s *Server) cleanupAdmittedCodexHomeFromCommittedTerminal(run jobRun) error {
	s.cleanupManagedCodexHomeForAdmissionRun(run)
	return nil
}

func (s *Server) handleAdmissionResponseOutcome(ctx context.Context, run jobRun, delivered bool) {
	action := model.OnResponseOutcome(run.admissionMode, delivered)
	switch action {
	case model.RunAcceptedObligation, model.RetainObligationForReplay:
		go s.launchAdmittedJob(ctx, run)
	default:
		_ = s.failStopAdmissionReady(ctx, fmt.Errorf("unsupported strict admission response action %s for mode %s", action, run.admissionMode))
	}
}

func (s *Server) prepareAdmittedJobLaunch(ctx context.Context, run jobRun) (jobRun, context.Context, bool, error) {
	jobID := model.JobID(run.jobID)
	var snapshot coordinator.JobSnapshot
	if err := s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
		var err error
		snapshot, err = coord.Snapshot(ctx, jobID)
		return err
	}); err != nil {
		return run, nil, false, err
	}
	if snapshot.Record.Terminal != nil || snapshot.Record.Cancel != nil {
		return run, nil, false, nil
	}
	s.mu.Lock()
	_, active := s.activeJobs[run.jobID]
	s.mu.Unlock()
	if active {
		return run, nil, false, nil
	}
	if run.session == nil {
		if run.backend != "codex" || !run.codexIsolated {
			return run, nil, false, errors.New("admission session is not ready")
		}
		if run.backendImpl == nil {
			return run, nil, false, errors.New("admission Codex backend is unavailable")
		}
		layout, err := engine.LayoutForWorkspace(s.stateRoot, run.cwd)
		if err != nil {
			return run, nil, false, err
		}
		run.codexHome, run.managedCodexHome, err = s.prepareCodexHome(layout, run.jobID)
		if err != nil {
			return run, nil, false, err
		}
		session, err := run.backendImpl.Start(ctx, engine.SessionOpts{
			CWD:        run.cwd,
			Write:      run.write,
			Model:      run.model,
			Effort:     run.effort,
			Timeout:    run.timeout,
			EnvOverlay: map[string]string{"CODEX_HOME": run.codexHome},
		})
		if err != nil {
			return run, nil, false, err
		}
		if _, ok := session.(ordinalBoundSession); !ok {
			return run, nil, false, admissionBackendContractViolationError{backend: run.backend}
		}
		if backendSessionID := session.ID(); backendSessionID != "" {
			if err := authority.ValidateSessionID(backendSessionID); err != nil {
				return run, nil, false, fmt.Errorf("backend returned invalid session id: %w", err)
			}
		}
		run.session = session
	}
	runCtx, cancel := context.WithCancel(ctx)
	activeJob := &activeJob{jobID: run.jobID, sessionID: run.sessionID, session: run.session, cancel: cancel, containmentIntent: run.admissionLaunch.containmentIntent}
	run.active = activeJob
	s.addActiveJob(activeJob)
	return run, runCtx, true, nil
}

func admissionProtocolError(err error) *protocol.ErrorObject {
	switch {
	case errors.Is(err, errAdmissionClosing):
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectAdmissionClosing,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, authority.ErrReplayConflict):
		return strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectReplayConflict,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, authority.ErrRequestExpired):
		return strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectRequestExpired,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, authority.ErrRootSealed):
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectRootSealed,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, repository.ErrCorruptRecord):
		return strictAdmissionProtocolError(
			protocol.ErrorBackendUnavailable,
			protocol.AdmissionRejectRootCorrupt,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, authority.ErrFailStopped):
		return strictAdmissionProtocolError(
			protocol.ErrorBackendUnavailable,
			protocol.AdmissionRejectRootFailStopped,
			err.Error(),
			protocol.ErrorData{},
		)
	case errors.Is(err, authority.ErrInvalidRequest):
		// Served validates backend session metadata before authority ingress; remaining invalid requests are client-owned.
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, model.ErrIncompatibleExecutionCapabilities):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	case errors.Is(err, custodian.ErrSupervisorUnavailable):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	case admissionAuthorityNotReadyError(err):
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectUnavailableNativeRuntime,
			admissionNotReadyMessage(err),
			protocol.ErrorData{},
		)
	default:
		return protocol.NewError(protocol.ErrorBackendUnavailable, admissionServerFailureMessage(err), protocol.ErrorData{})
	}
}

func admissionServerFailureMessage(err error) string {
	if err == nil || err.Error() == "" {
		return "admission authority failed"
	}
	return "admission authority failed: " + err.Error()
}

func admissionAuthorityNotReadyError(err error) bool {
	return errors.Is(err, authority.ErrNotReady) ||
		errors.Is(err, coordinator.ErrCoordinatorNotReady) ||
		errors.Is(err, bolt.ErrDatabaseNotOpen)
}

func admissionNotReadyMessage(err error) string {
	if err == nil || err.Error() == "" {
		return "admission authority is not ready"
	}
	return "admission authority is not ready: " + err.Error()
}

func admissionAcceptCommitted(accepted authority.AcceptResult) bool {
	return accepted.Record.JobID != "" && accepted.Binding.JobID == accepted.Record.JobID
}

func admissionPostAcceptError(accepted authority.AcceptResult, err error) *protocol.ErrorObject {
	jobID := accepted.Record.JobID.String()
	if errors.Is(err, errAdmissionClosing) {
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectAdmissionClosing,
			err.Error(),
			protocol.ErrorData{JobID: jobID},
		)
	}
	// S4E wires listener halt and recovery consumption; this path only reports
	// that the obligation was durably accepted and the authority fail-stopped.
	return protocol.NewError(protocol.ErrorBackendUnavailable, fmt.Sprintf("admission accepted job %s and fail-stopped before launch: %v", jobID, err), protocol.ErrorData{JobID: jobID})
}

func admissionState(state model.PublicState) engine.JobState {
	return engine.JobState(state.String())
}

func admissionCleanupDisposition(record model.SafetyRecord) string {
	if record.Terminal == nil {
		return ""
	}
	return model.DeriveCleanupDisposition(record).String()
}

// authorityFinalAttemptTiming and authorityFailureMetadata are the
// response-boundary twins of the projection guards: Project filters new
// records, and these also protect projections persisted by an older daemon
// written before those guards existed.
func authorityFinalAttemptTiming(projection model.JobProjection) (*time.Time, *time.Time) {
	if projection.Decision != model.DecisionTerminal || projection.FinalAttemptStartedAt == nil || projection.FinalAttemptEndedAt == nil {
		return nil, nil
	}
	return cloneTime(projection.FinalAttemptStartedAt), cloneTime(projection.FinalAttemptEndedAt)
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// authorityFailureMetadata exposes failure metadata only for failure or
// interrupted terminal states.
func authorityFailureMetadata(projection model.JobProjection) (string, engine.FailureClass) {
	switch projection.Public {
	case model.PublicFailed, model.PublicInterrupted, model.PublicQuarantined:
		return projection.FailureReason, projection.FailureClass
	default:
		return "", ""
	}
}

func (s *Server) authorityStatus(jobID string) (protocol.JobStatus, bool, *protocol.ErrorObject) {
	record, projection, ok, errObj := s.authorityJobProjection(jobID)
	if !ok || errObj != nil {
		return protocol.JobStatus{}, ok, errObj
	}
	finalAttemptStartedAt, finalAttemptEndedAt := authorityFinalAttemptTiming(projection)
	failureReason, failureClass := authorityFailureMetadata(projection)
	reported := s.reportedModel(projection.JobID.String())
	status := protocol.JobStatus{
		JobID:                 projection.JobID.String(),
		SessionID:             projection.SessionID,
		State:                 admissionState(projection.Public),
		CleanupDisposition:    admissionCleanupDisposition(record),
		ModelReported:         reported,
		FinalAttemptStartedAt: finalAttemptStartedAt,
		FinalAttemptEndedAt:   finalAttemptEndedAt,
		FailureReason:         failureReason,
		FailureClass:          failureClass,
	}
	if started, lastEvent, _, ok := s.jobLivenessSnapshot(status.JobID); ok {
		startedAt := started
		updatedAt := lastEvent
		heartbeatAt := lastEvent
		status.StartedAt = &startedAt
		status.UpdatedAt = &updatedAt
		status.HeartbeatAt = &heartbeatAt
	}
	return status, true, nil
}

func (s *Server) listAuthorityStatuses() ([]protocol.JobStatus, *protocol.ErrorObject) {
	// Snapshot-checkout (see activeWork): repository reads must not hold
	// admissionStateMu.
	s.admissionStateMu.RLock()
	repo := s.admissionRepository
	ready := s.admissionReady
	ok := s.admissionInstance != nil && repo != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !ok {
		return nil, nil
	}
	var statuses []protocol.JobStatus
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			status, ok, err := authorityStatusFromImage(image)
			if err != nil {
				return err
			}
			if ok {
				status.ModelReported = s.reportedModel(status.JobID)
				if started, lastEvent, _, ok := s.jobLivenessSnapshot(status.JobID); ok {
					startedAt := started
					updatedAt := lastEvent
					heartbeatAt := lastEvent
					status.StartedAt = &startedAt
					status.UpdatedAt = &updatedAt
					status.HeartbeatAt = &heartbeatAt
				}
				statuses = append(statuses, status)
			}
		}
		return nil
	}); err != nil {
		if errors.Is(err, repository.ErrCorruptRecord) {
			err = s.failStopAdmissionRepositoryCorruption(context.Background(), "list authority statuses", err)
		}
		return nil, admissionProtocolError(err)
	}
	return statuses, nil
}

func (s *Server) authorityResult(jobID string) (protocol.JobResult, bool, *protocol.ErrorObject) {
	record, projection, ok, errObj := s.authorityJobProjection(jobID)
	if !ok || errObj != nil {
		return protocol.JobResult{}, ok, errObj
	}
	var result *engine.ResultInfo
	var contract *engine.ContractStamp
	if record.Terminal != nil && record.Terminal.Result != nil {
		result = s.authorityResultInfo(*record.Terminal.Result)
	}
	if record.Terminal != nil {
		contract = record.Terminal.Contract
	}
	finalAttemptStartedAt, finalAttemptEndedAt := authorityFinalAttemptTiming(projection)
	failureReason, failureClass := authorityFailureMetadata(projection)
	reported := s.reportedModel(projection.JobID.String())
	if result != nil {
		result.ModelReported = reported
	}
	return protocol.JobResult{
		JobID:                 projection.JobID.String(),
		SessionID:             projection.SessionID,
		State:                 admissionState(projection.Public),
		CleanupDisposition:    admissionCleanupDisposition(record),
		Result:                result,
		ModelReported:         reported,
		Contract:              contract,
		FinalAttemptStartedAt: finalAttemptStartedAt,
		FinalAttemptEndedAt:   finalAttemptEndedAt,
		FailureReason:         failureReason,
		FailureClass:          failureClass,
	}, true, nil
}

func (s *Server) authorityJobProjection(jobID string) (model.SafetyRecord, model.JobProjection, bool, *protocol.ErrorObject) {
	// Snapshot-checkout (see activeWork): authority reads must not hold
	// admissionStateMu.
	s.admissionStateMu.RLock()
	ready := s.admissionReady
	ok := s.admissionInstance != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !ok {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	image, err := ready.LoadJob(context.Background(), modelJobID)
	if err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, false, admissionProtocolError(err)
	}
	if authorityImageEmpty(image) {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	if err := authorityImageSafetyCorruption(image); err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, false, admissionProtocolError(s.failStopAdmissionRepositoryCorruption(context.Background(), "authority job projection", err))
	}
	if image.Safety.State != repository.RecordValid {
		return model.SafetyRecord{}, model.JobProjection{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority safety record is not valid", protocol.ErrorData{JobID: jobID})
	}
	if image.Projection.State != repository.RecordValid {
		return model.SafetyRecord{}, model.JobProjection{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority projection is not valid", protocol.ErrorData{JobID: jobID})
	}
	return image.Safety.Value, image.Projection.Value, true, nil
}

func authorityStatusFromImage(image repository.JobImage) (protocol.JobStatus, bool, error) {
	if authorityImageEmpty(image) {
		return protocol.JobStatus{}, false, nil
	}
	jobID := ""
	if image.Safety.State == repository.RecordValid {
		jobID = image.Safety.Value.JobID.String()
	} else if image.Projection.State == repository.RecordValid {
		jobID = image.Projection.Value.JobID.String()
	}
	if err := authorityImageSafetyCorruption(image); err != nil {
		return protocol.JobStatus{}, false, err
	}
	if image.Safety.State != repository.RecordValid {
		return protocol.JobStatus{}, false, fmt.Errorf("%w: authority safety record is not valid for %s", authority.ErrInvalidRequest, jobID)
	}
	if image.Projection.State != repository.RecordValid {
		return protocol.JobStatus{}, false, fmt.Errorf("%w: authority projection is not valid for %s", authority.ErrInvalidRequest, jobID)
	}
	projection := image.Projection.Value
	finalAttemptStartedAt, finalAttemptEndedAt := authorityFinalAttemptTiming(projection)
	failureReason, failureClass := authorityFailureMetadata(projection)
	return protocol.JobStatus{
		JobID:                 projection.JobID.String(),
		SessionID:             projection.SessionID,
		State:                 admissionState(projection.Public),
		CleanupDisposition:    admissionCleanupDisposition(image.Safety.Value),
		FinalAttemptStartedAt: finalAttemptStartedAt,
		FinalAttemptEndedAt:   finalAttemptEndedAt,
		FailureReason:         failureReason,
		FailureClass:          failureClass,
	}, true, nil
}

func authorityImageEmpty(image repository.JobImage) bool {
	return image.Binding.State == repository.RecordMissing &&
		image.Safety.State == repository.RecordMissing &&
		image.Projection.State == repository.RecordMissing &&
		image.Quarantine.State == repository.RecordMissing
}

func authorityImageSafetyCorruption(image repository.JobImage) error {
	jobID := authorityImageJobID(image)
	if image.Binding.State == repository.RecordCorrupt {
		return repository.CorruptRecordError(authorityBindingCorruptionKind(image.Binding.Diagnostic), jobID, image.Binding.Diagnostic)
	}
	if image.Safety.State == repository.RecordCorrupt {
		return repository.CorruptRecordError("safety", jobID, image.Safety.Diagnostic)
	}
	if image.Safety.State == repository.RecordValid && image.Binding.State == repository.RecordMissing {
		return repository.CorruptRecordError("binding", jobID, "missing binding")
	}
	if image.Safety.State == repository.RecordMissing && (image.Binding.State == repository.RecordValid || image.Projection.State == repository.RecordValid) {
		return repository.CorruptRecordError("safety", jobID, "missing safety record")
	}
	return nil
}

func authorityBindingCorruptionKind(diagnostic string) string {
	diagnostic = strings.ToLower(diagnostic)
	if strings.Contains(diagnostic, "binding_index") || strings.Contains(diagnostic, "binding index") {
		return "binding_index"
	}
	return "binding"
}

func authorityImageJobID(image repository.JobImage) string {
	if image.Safety.State == repository.RecordValid {
		return image.Safety.Value.JobID.String()
	}
	if image.Binding.State == repository.RecordValid {
		return image.Binding.Value.JobID.String()
	}
	if image.Projection.State == repository.RecordValid {
		return image.Projection.Value.JobID.String()
	}
	if image.Quarantine.State == repository.RecordValid {
		return image.Quarantine.Value.JobID.String()
	}
	return ""
}

func (s *Server) authorityResultInfo(ref model.ResultRef) *engine.ResultInfo {
	info := &engine.ResultInfo{
		ResultPath: ref.Path,
		SHA256:     ref.Digest,
		Bytes:      ref.Bytes,
	}
	inlineCap := s.inlineResultCap
	if inlineCap <= 0 {
		inlineCap = engine.DefaultInlineResultCap
	}
	if ref.Bytes >= int64(inlineCap) {
		stat, err := os.Stat(ref.Path)
		if err != nil || !stat.Mode().IsRegular() || stat.Size() != ref.Bytes {
			return info
		}
		info.TextElided = true
		return info
	}
	// Bounded read: the certified byte count gates hydration, but the file on
	// disk could have been replaced or grown since certification — never read
	// more than inlineCap+1 bytes, then require the exact certified length and
	// digest before serving inline text. Any mismatch or I/O failure omits the
	// text (the path/digest metadata remains authoritative).
	f, err := os.Open(ref.Path)
	if err != nil {
		return info
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, int64(inlineCap)+1))
	if err != nil || int64(len(raw)) != ref.Bytes {
		return info
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) == ref.Digest {
		info.Text = string(raw)
	}
	return info
}
