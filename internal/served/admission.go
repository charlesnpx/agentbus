package served

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
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
	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

const (
	admissionRepositoryFile = "admission.bbolt"
	admissionAnchorFile     = "admission-anchor.json"

	admissionFailStopTimeout        = 30 * time.Second
	admissionRepositoryCloseTimeout = 5 * time.Second
	admissionProbeReasonMaxRunes    = 512
)

var admissionDetachedCleanupTimeout = 30 * time.Second

type admissionBootstrapper = authority.Bootstrapper
type admissionReady = authority.Ready
type admissionCoordinator = coordinator.Coordinator

type admissionBootstrapperFactory func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error)

type admissionProbeableBackend interface {
	ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error)
}

type admissionInstance struct {
	runtime     *servedAdmissionRuntime
	descriptors map[string]admissionBackendDescriptor
	policy      ServeAdmissionPolicy

	bootstrapper *admissionBootstrapper
	ready        *admissionReady
	coordinator  *admissionCoordinator
	submission   *servedSubmissionCoordinator
	repository   repository.Repository
	close        io.Closer
}

func (instance *admissionInstance) descriptor(name string) (admissionBackendDescriptor, bool) {
	if instance == nil || instance.descriptors == nil {
		return admissionBackendDescriptor{}, false
	}
	descriptor, ok := instance.descriptors[name]
	return descriptor, ok
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

// ServeAdmissionPolicy is the immutable strict-admission policy derived during
// Serve bootstrap from the qualified runtime and probed backend descriptors.
type ServeAdmissionPolicy struct {
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

func deriveServeAdmissionPolicy(runtimeSupport custodian.Support, descriptors map[string]admissionBackendDescriptor) ServeAdmissionPolicy {
	policy := ServeAdmissionPolicy{
		strictRouteEnabled: true,
		runtimeSupport:     runtimeSupport,
		runtimeAssessment:  runtimeSupport.Assessment,
		backends:           make(map[string]ServeBackendFenceability, len(descriptors)),
	}
	for name, descriptor := range descriptors {
		policy.backends[name] = ServeBackendFenceability{
			Backend:          name,
			Capabilities:     descriptor.capabilities,
			ControlledRunner: descriptor.controlledRunner,
			Fenceable:        descriptor.fenceable,
			Reason:           descriptor.unfenceableCause,
		}
	}
	return policy
}

func (policy ServeAdmissionPolicy) backendFenceability(name string) (ServeBackendFenceability, bool) {
	if policy.backends == nil {
		return ServeBackendFenceability{}, false
	}
	fenceability, ok := policy.backends[name]
	return fenceability, ok
}

func (policy ServeAdmissionPolicy) strictRuntimeAvailable() bool {
	return policy.runtimeAssessment.Class == custodian.SupportAvailable &&
		policy.runtimeSupport.RuntimeProbePassed &&
		policy.runtimeSupport.ParkedExec &&
		policy.runtimeSupport.VerifiedContainment
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
		s.admissionRuntime = runtime
	}
	descriptors, err := s.probeAdmissionBackends(ctx, runtime)
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
	closeOnErr := true
	defer func() {
		if closeOnErr && closer != nil {
			_ = closer.Close()
		}
	}()

	boot, err := s.admissionDaemonBoot()
	if err != nil {
		return err
	}
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return err
	}
	if err := recoverAdmissionBeforeReady(ctx, session, runtime.launchPort(), s.safetyLatch); err != nil {
		return err
	}
	if err := s.reapKnownStores(); err != nil {
		return err
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		return err
	}
	adapter := &servedAdmissionAuthority{ready: ready, latch: s.safetyLatch}
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
		descriptors:  cloneAdmissionBackendDescriptors(descriptors),
		policy:       deriveServeAdmissionPolicy(runtime.support(), descriptors),
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

func (s *Server) closeServeAdmission() error {
	// Lock order for the admission shutdown path is submitMu -> stateMu.
	// Identified submit never takes submitMu while holding stateMu; it snapshots
	// under stateMu, releases it, and re-checks publication under submitMu
	// before durable acceptance. That gives close a single order to serialize
	// acceptance and state clearing without a lock cycle.
	s.admissionSubmitMu.Lock()
	s.admissionStateMu.Lock()

	closer := s.admissionClose
	s.admissionBootstrapper = nil
	s.admissionReady = nil
	s.admissionCoordinator = nil
	s.admissionOwnedWorkChecker = nil
	s.admissionSubmission = nil
	s.admissionRepository = nil
	s.admissionClose = nil
	s.admissionInstance = nil
	s.admissionDaemonBootOnce = sync.Once{}
	s.admissionDaemonBootRef = model.BootRef{}
	s.admissionDaemonBootRefErr = nil
	s.admissionStateMu.Unlock()
	s.admissionSubmitMu.Unlock()

	if closer != nil {
		return closeAdmissionRepositoryWithTimeout(closer)
	}
	return nil
}

func closeAdmissionRepositoryWithTimeout(closer io.Closer) error {
	done := make(chan error, 1)
	go func() {
		done <- closer.Close()
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(admissionRepositoryCloseTimeout):
		log.Printf("agentbus daemon: admission repository close timed out; leaking handle at shutdown")
		return nil
	}
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

func (s *Server) probeAdmissionBackends(ctx context.Context, runtime *servedAdmissionRuntime) (map[string]admissionBackendDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runner := s.admissionProbeRunner
	if runner == nil {
		runner = command.DirectProbeRunner{}
	}
	names := make([]string, 0, len(s.backends))
	for name := range s.backends {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make(map[string]admissionBackendDescriptor, len(names))
	for _, name := range names {
		backend := s.backends[name]
		probeable, ok := backend.(admissionProbeableBackend)
		if !ok {
			probeErr := model.IncompatibleExecutionCapabilitiesError{
				Reason: "strict backend does not implement ProbeBackend; admission cannot verify command-runner capabilities",
			}
			if s.admissionUnprobeableBackends == nil {
				s.admissionUnprobeableBackends = make(map[string]error)
			}
			s.admissionUnprobeableBackends[name] = probeErr
			descriptors[name] = s.admissionBackendDescriptor(name, backend, runtime, probeErr)
			continue
		}
		probed, err := probeable.ProbeBackend(ctx, runner)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("probe strict backend %s canceled by serve context: %w", name, ctxErr)
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
			descriptors[name] = s.admissionBackendDescriptor(name, backend, runtime, probeErr)
			continue
		}
		if probed == nil {
			return nil, fmt.Errorf("probe strict backend %s: nil probed backend", name)
		}
		if probed.Name() != name {
			return nil, fmt.Errorf("probe strict backend %s: probed backend changed name to %s", name, probed.Name())
		}
		s.backends[name] = probed
		if s.admissionUnprobeableBackends != nil {
			delete(s.admissionUnprobeableBackends, name)
		}
		descriptors[name] = s.admissionBackendDescriptor(name, probed, runtime, nil)
	}
	return descriptors, nil
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

func (s *Server) admissionBackendDescriptor(name string, backend engine.Backend, runtime *servedAdmissionRuntime, probeErr error) admissionBackendDescriptor {
	caps := model.ExecutionCapabilities{
		ExternalRunner: admissionBackendExternalRunner(backend),
		FencedLaunch:   runtime != nil && runtime.support().ParkedExec,
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
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	repo, err := bboltrepo.NewRepository(filepath.Join(s.stateRoot, admissionRepositoryFile))
	if err != nil {
		return nil, nil, nil, err
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	anchor := &fileAuthorityAnchor{
		path:        filepath.Join(s.stateRoot, admissionAnchorFile),
		dbUUID:      dbUUID,
		schemaMajor: schemaMajor,
		latch:       s.safetyLatch,
	}
	options := []authority.BootstrapperOption{authority.WithAnchor(anchor)}
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

func recoverAdmissionBeforeReady(ctx context.Context, session *authority.RecoverySession, launchPort launch.CustodianPort, latch *SafetyLatch) error {
	return newAdmissionRecoveryExecutor(session, launchPort, latch).Recover(ctx)
}

type fileAuthorityAnchor struct {
	mu          sync.Mutex
	path        string
	dbUUID      string
	schemaMajor uint16
	latch       *SafetyLatch
}

func (a *fileAuthorityAnchor) Begin(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot, err := a.load()
	if err != nil {
		return "", err
	}
	if err := a.ensureIdentity(&snapshot, generation); err != nil {
		return "", err
	}
	if snapshot.Generation < generation {
		snapshot.Generation = generation
	}
	token := fmt.Sprintf("recovery-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	snapshot.Phase = "reconciling"
	snapshot.Boot = boot
	snapshot.Token = token
	snapshot.Reason = ""
	if err := a.save(snapshot); err != nil {
		return "", err
	}
	return token, nil
}

func (a *fileAuthorityAnchor) SealReady(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot, err := a.load()
	if err != nil {
		return "", err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return "", err
	}
	if snapshot.Generation != generation {
		return "", authority.ErrStaleCapability
	}
	if snapshot.Phase == "fail_stopped" {
		return "", authority.ErrFailStopped
	}
	token := fmt.Sprintf("ready-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	snapshot.Phase = "ready"
	snapshot.Boot = boot
	snapshot.Token = token
	snapshot.Reason = ""
	if err := a.save(snapshot); err != nil {
		return "", err
	}
	return token, nil
}

func (a *fileAuthorityAnchor) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot, err := a.load()
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	if snapshot.Phase == "fail_stopped" {
		return authority.ErrFailStopped
	}
	if snapshot.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", authority.ErrAnchorInvariant, snapshot.Generation, generation)
	}
	snapshot.Generation = generation
	snapshot.Boot = boot
	return a.save(snapshot)
}

func (a *fileAuthorityAnchor) FailStop(ctx context.Context, boot model.BootRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot, err := a.load()
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	snapshot.Phase = "fail_stopped"
	snapshot.Boot = boot
	snapshot.Reason = reason
	if err := a.save(snapshot); err != nil {
		return err
	}
	a.latch.Trip(safetyFailStopReason(reason))
	return nil
}

func (a *fileAuthorityAnchor) VerifyReady(boot model.BootRef, token string, generation uint64) error {
	return a.verify("ready", boot, token, generation)
}

func (a *fileAuthorityAnchor) VerifyRecovery(boot model.BootRef, token string, generation uint64) error {
	return a.verify("reconciling", boot, token, generation)
}

func (a *fileAuthorityAnchor) verify(phase string, boot model.BootRef, token string, generation uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	snapshot, err := a.load()
	if err != nil {
		return err
	}
	if err := a.requireIdentity(snapshot); err != nil {
		return err
	}
	if snapshot.Phase == "fail_stopped" {
		return authority.ErrFailStopped
	}
	if snapshot.Phase != phase || snapshot.Boot != boot || snapshot.Token != token || snapshot.Generation != generation {
		return authority.ErrStaleCapability
	}
	return nil
}

func (a *fileAuthorityAnchor) load() (authority.AnchorSnapshot, error) {
	raw, err := os.ReadFile(a.path)
	if errors.Is(err, os.ErrNotExist) {
		return authority.AnchorSnapshot{}, nil
	}
	if err != nil {
		return authority.AnchorSnapshot{}, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return authority.AnchorSnapshot{}, fmt.Errorf("%w: anchor is empty", authority.ErrAnchorInvariant)
	}
	var snapshot authority.AnchorSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return authority.AnchorSnapshot{}, fmt.Errorf("%w: anchor is corrupt: %v", authority.ErrAnchorInvariant, err)
	}
	return snapshot, nil
}

func (a *fileAuthorityAnchor) save(snapshot authority.AnchorSnapshot) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return atomicWriteDurable(a.path, raw, 0o600)
}

func (a *fileAuthorityAnchor) ensureIdentity(snapshot *authority.AnchorSnapshot, generation uint64) error {
	if err := a.validateIdentity(); err != nil {
		return err
	}
	if !snapshot.Initialized {
		if generation != 0 {
			return fmt.Errorf("%w: missing anchor for initialized db generation %d", authority.ErrAnchorInvariant, generation)
		}
		*snapshot = authority.AnchorSnapshot{
			Initialized: true,
			DBUUID:      a.dbUUID,
			SchemaMajor: a.schemaMajor,
			Generation:  generation,
		}
		return nil
	}
	if err := a.requireIdentity(*snapshot); err != nil {
		return err
	}
	if snapshot.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", authority.ErrAnchorInvariant, snapshot.Generation, generation)
	}
	return nil
}

func (a *fileAuthorityAnchor) requireIdentity(snapshot authority.AnchorSnapshot) error {
	if err := a.validateIdentity(); err != nil {
		return err
	}
	if !snapshot.Initialized {
		return fmt.Errorf("%w: anchor is missing", authority.ErrAnchorInvariant)
	}
	if snapshot.DBUUID != a.dbUUID {
		return fmt.Errorf("%w: db uuid mismatch", authority.ErrAnchorInvariant)
	}
	if snapshot.SchemaMajor != a.schemaMajor {
		return fmt.Errorf("%w: schema major mismatch", authority.ErrAnchorInvariant)
	}
	return nil
}

func (a *fileAuthorityAnchor) validateIdentity() error {
	if a.dbUUID == "" || a.schemaMajor == 0 {
		return fmt.Errorf("%w: invalid anchor identity", authority.ErrAnchorInvariant)
	}
	return nil
}

type servedSubmissionCoordinator struct {
	ready  *authority.Ready
	owner  model.OwnerID
	launch *launch.LaunchController
	latch  *SafetyLatch
}

var _ authority.SubmissionCoordinator = (*servedSubmissionCoordinator)(nil)

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
	coordinator *servedSubmissionCoordinator
	jobID       model.JobID
	attempt     model.AttemptRef
}

type ordinalBoundSession interface {
	TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
}

func (s *Server) admissionTurnEvents(ctx context.Context, run jobRun, input engine.TurnInput, ordinal model.LaunchOrdinal) (<-chan engine.Event, error) {
	if err := ordinal.Validate(); err != nil {
		return nil, err
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
		return nil, authority.ErrNotReady
	}
	if run.admissionLaunch.coordinator == nil {
		return nil, custodian.ErrSupervisorUnavailable
	}
	session, ok := run.session.(ordinalBoundSession)
	if !ok {
		return nil, fmt.Errorf("%w: backend session does not support ordinal-bound runners", custodian.ErrSupervisorUnavailable)
	}
	runner, err := run.admissionLaunch.coordinator.LaunchRunner(run.admissionLaunch, ordinal)
	if err != nil {
		return nil, err
	}
	return session.TurnWithRunner(ctx, input, runner)
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

func (c *servedSubmissionCoordinator) PrepareLegacyFenced(ctx context.Context, request authority.AcceptRequest) (authority.LegacyFencedPreparation, error) {
	if c == nil || c.ready == nil {
		return authority.LegacyFencedPreparation{}, authority.ErrNotReady
	}
	request.Mode = model.ModeLegacyFenced
	accepted, err := c.ready.Accept(ctx, request)
	if err != nil {
		return authority.LegacyFencedPreparation{Admission: accepted}, err
	}
	return authority.LegacyFencedPreparation{
		Admission: accepted,
		Ordinal:   model.LaunchOrdinalOne,
	}, nil
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
	})
}

type legacyFencedPrepareRunner struct {
	coordinator *servedSubmissionCoordinator
	preparation authority.LegacyFencedPreparation
	command     *legacyFencedCommand
}

func (r *legacyFencedPrepareRunner) Start(ctx context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	if r == nil || r.coordinator == nil {
		return nil, authority.ErrNotReady
	}
	cmd, preparation, err := r.coordinator.prepareLegacyFencedCommand(ctx, r.preparation.Admission, spec)
	if err != nil {
		return nil, err
	}
	r.preparation = preparation
	r.command = cmd
	return cmd, nil
}

func (c *servedSubmissionCoordinator) prepareLegacyFencedCommand(ctx context.Context, accepted authority.AcceptResult, spec command.ExecSpec) (*legacyFencedCommand, authority.LegacyFencedPreparation, error) {
	if c == nil || c.launch == nil || c.ready == nil {
		return nil, authority.LegacyFencedPreparation{}, authority.ErrNotReady
	}
	if accepted.Record.JobID == "" {
		return nil, authority.LegacyFencedPreparation{}, fmt.Errorf("%w: LegacyFenced admission is empty", authority.ErrInvalidRequest)
	}
	ordinal := model.LaunchOrdinalOne
	launchContext := launch.LaunchContext{
		JobID:   accepted.Record.JobID,
		Attempt: accepted.Record.Attempt.Ref,
		Ordinal: ordinal,
	}
	request := launch.LaunchRequest{
		Context: launchContext,
		Exec:    spec,
	}
	if err := request.Validate(); err != nil {
		return nil, authority.LegacyFencedPreparation{}, err
	}
	prepared, err := c.launch.Prepare(ctx, request)
	if err != nil {
		return nil, authority.LegacyFencedPreparation{}, err
	}
	group := prepared.Ref()
	outcome, bindErr := c.launch.BindGroup(ctx, launchContext, group)
	if err := c.handleLegacyPrepareDurability(ctx, "bind_group", outcome, bindErr, prepared, launchContext); err != nil {
		return nil, authority.LegacyFencedPreparation{}, err
	}
	preparation := authority.LegacyFencedPreparation{
		Admission: accepted,
		Ordinal:   ordinal,
		Group:     group,
	}
	return newLegacyFencedCommand(c, launchContext, group, prepared), preparation, nil
}

func (c *servedSubmissionCoordinator) handleLegacyPrepareDurability(ctx context.Context, step string, outcome launch.DurabilityOutcome, stepErr error, prepared launch.PreparedProcess, launchContext launch.LaunchContext) error {
	mapped := admissionDurabilityError(step, outcome, stepErr)
	switch outcome {
	case launch.CommittedAndAnchored:
		if mapped == nil {
			return nil
		}
		return errors.Join(mapped, c.abortLegacyPrepared(ctx, prepared, true, launchContext))
	case launch.DefinitelyNotCommitted:
		return errors.Join(mapped, c.abortLegacyPrepared(ctx, prepared, false, launchContext))
	default:
		reason := errors.Join(mapped, c.abortLegacyPrepared(ctx, prepared, false, launchContext))
		return errors.Join(reason, c.failStop(ctx, reason), launch.ErrFailClosed)
	}
}

func (c *servedSubmissionCoordinator) abortLegacyPrepared(ctx context.Context, prepared launch.PreparedProcess, groupDurable bool, launchContext launch.LaunchContext) error {
	if prepared == nil {
		return nil
	}
	verified, cleanup, abortErr := prepared.AbortAndVerify(ctx)
	if abortErr != nil {
		reason := fmt.Errorf("abort legacy fenced prepared process: %w", abortErr)
		return errors.Join(reason, c.failStop(ctx, reason), launch.ErrFailClosed)
	}
	if !groupDurable {
		return cleanup.Err
	}
	outcome, err := c.launch.RecordQuiescence(ctx, launchContext, verified)
	if mapped := admissionDurabilityError("record_quiescence", outcome, err); mapped != nil {
		return errors.Join(mapped, c.failStop(ctx, mapped), launch.ErrFailClosed)
	}
	return cleanup.Err
}

func (c *servedSubmissionCoordinator) acknowledgeGrantAndReleaseLegacyFenced(ctx context.Context, cmd *legacyFencedCommand) error {
	if cmd == nil {
		return fmt.Errorf("%w: legacy fenced command is missing", authority.ErrInvalidRequest)
	}
	return cmd.grantAndRelease(ctx)
}

func (c *servedSubmissionCoordinator) rejectAndRetireLegacyFenced(ctx context.Context, cmd *legacyFencedCommand) error {
	if cmd == nil {
		return fmt.Errorf("%w: legacy fenced command is missing", authority.ErrInvalidRequest)
	}
	return cmd.reject(ctx, model.CauseResponseUndeliverable)
}

func (c *servedSubmissionCoordinator) rejectLegacyFencedBeforePrepare(ctx context.Context, accepted authority.AcceptResult) error {
	if c == nil || c.ready == nil {
		return authority.ErrNotReady
	}
	if accepted.Record.JobID == "" {
		return fmt.Errorf("%w: LegacyFenced admission is empty", authority.ErrInvalidRequest)
	}
	if _, err := c.ready.BeginReject(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref); err != nil {
		return err
	}
	_, err := c.ready.Finalize(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseResponseUndeliverable,
	})
	return err
}

func (c *servedSubmissionCoordinator) rejectLegacyUnfencedBeforeRun(ctx context.Context, accepted authority.AcceptResult) error {
	if c == nil || c.ready == nil {
		return authority.ErrNotReady
	}
	if accepted.Record.JobID == "" {
		return fmt.Errorf("%w: LegacyUnfenced admission is empty", authority.ErrInvalidRequest)
	}
	if _, err := c.ready.RequestCancel(ctx, accepted.Record.JobID); err != nil {
		return err
	}
	_, err := c.ready.Finalize(ctx, accepted.Record.JobID, accepted.Record.Attempt.Ref, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   model.CauseResponseUndeliverable,
	})
	return err
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

type legacyFencedCommand struct {
	coordinator   *servedSubmissionCoordinator
	launchContext launch.LaunchContext
	group         model.GroupRef
	prepared      launch.PreparedProcess
	stdin         *legacyFencedStdin
	stdoutReader  *io.PipeReader
	stdoutWriter  *io.PipeWriter
	stderrReader  *io.PipeReader
	stderrWriter  *io.PipeWriter

	mu       sync.Mutex
	running  launch.RunningProcess
	released bool
	rejected bool
	grant    model.LaunchGrant

	doneOnce sync.Once
	done     chan struct{}
	exit     command.ExitObservation
	err      error
}

func newLegacyFencedCommand(coordinator *servedSubmissionCoordinator, launchContext launch.LaunchContext, group model.GroupRef, prepared launch.PreparedProcess) *legacyFencedCommand {
	stdoutReader, stdoutWriter := io.Pipe()
	stderrReader, stderrWriter := io.Pipe()
	return &legacyFencedCommand{
		coordinator:   coordinator,
		launchContext: launchContext,
		group:         group,
		prepared:      prepared,
		stdin:         &legacyFencedStdin{},
		stdoutReader:  stdoutReader,
		stdoutWriter:  stdoutWriter,
		stderrReader:  stderrReader,
		stderrWriter:  stderrWriter,
		done:          make(chan struct{}),
	}
}

func (cmd *legacyFencedCommand) Stdin() io.WriteCloser {
	return cmd.stdin
}

func (cmd *legacyFencedCommand) Stdout() io.ReadCloser {
	return cmd.stdoutReader
}

func (cmd *legacyFencedCommand) Stderr() io.ReadCloser {
	return cmd.stderrReader
}

func (cmd *legacyFencedCommand) Wait(ctx context.Context) (command.ExitObservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-cmd.done:
		return cmd.result()
	case <-ctx.Done():
		if err := cmd.Interrupt(context.Background()); err != nil {
			exit, finalErr := cmd.result()
			return exit, errors.Join(ctx.Err(), err, finalErr)
		}
		exit, finalErr := cmd.result()
		return exit, errors.Join(ctx.Err(), finalErr)
	}
}

func (cmd *legacyFencedCommand) Interrupt(ctx context.Context) error {
	cmd.mu.Lock()
	running := cmd.running
	released := cmd.released
	rejected := cmd.rejected
	cmd.mu.Unlock()
	if rejected {
		return nil
	}
	if !released || running == nil {
		return cmd.reject(ctx, model.CauseCanceledBeforeAuthorization)
	}
	verified, cleanup, err := running.ContainAndVerify(ctx, custodian.QuiescenceCauseContain)
	if err != nil {
		return err
	}
	outcome, recordErr := cmd.coordinator.launch.RecordQuiescence(ctx, cmd.launchContext, verified)
	if mapped := admissionDurabilityError("record_quiescence", outcome, recordErr); mapped != nil {
		return mapped
	}
	cmd.finish(command.ExitObservation{}, cleanup.Err)
	return cleanup.Err
}

func (cmd *legacyFencedCommand) ProcessRef() (engine.ProcessRef, int) {
	return engine.ProcessRef{
		PID:       cmd.group.Leader.PID,
		PGID:      cmd.group.PGID,
		StartTime: cmd.group.Leader.HighResStartToken,
	}, cmd.group.Leader.PID
}

func (cmd *legacyFencedCommand) grantAndRelease(ctx context.Context) error {
	cmd.mu.Lock()
	if cmd.rejected {
		err := cmd.err
		cmd.mu.Unlock()
		if err == nil {
			err = fmt.Errorf("%w: legacy fenced command was rejected", authority.ErrInvalidRequest)
		}
		return err
	}
	if cmd.released {
		cmd.mu.Unlock()
		return nil
	}
	cmd.mu.Unlock()

	if _, err := cmd.coordinator.ready.Acknowledge(ctx, cmd.launchContext.JobID, cmd.launchContext.Attempt); err != nil {
		cmd.failPrepared(ctx, err)
		return err
	}
	grant, outcome, err := cmd.coordinator.launch.AllocateGrant(ctx, cmd.launchContext)
	if mapped := admissionDurabilityError("allocate_grant", outcome, err); mapped != nil {
		cmd.failPrepared(ctx, mapped)
		return mapped
	}
	running, physicalReleaseOutcome, err := cmd.coordinator.launch.Release(ctx, cmd.prepared)
	switch physicalReleaseOutcome {
	case custodian.ReleaseAccepted:
		if err != nil {
			mapped := fmt.Errorf("%w: release accepted with error: %v", launch.ErrReleaseUncertain, err)
			cmd.failReleasedByGroup(ctx, mapped)
			return mapped
		}
	case custodian.ReleaseDefinitelyNotSent:
		mapped := launch.ErrReleaseUncertain
		if err != nil {
			mapped = fmt.Errorf("%w: release definitely not sent: %v", launch.ErrReleaseUncertain, err)
		}
		cmd.failPrepared(ctx, mapped)
		return mapped
	case custodian.ReleaseOutcomeUnknown:
		mapped := launch.ErrReleaseUncertain
		if err != nil {
			mapped = fmt.Errorf("%w: release outcome unknown: %v", launch.ErrReleaseUncertain, err)
		}
		cmd.failReleasedByGroup(ctx, mapped)
		return mapped
	default:
		mapped := fmt.Errorf("%w: invalid release outcome %d", launch.ErrReleaseUncertain, physicalReleaseOutcome)
		if err != nil {
			mapped = errors.Join(mapped, err)
		}
		cmd.failReleasedByGroup(ctx, mapped)
		return mapped
	}
	if running == nil {
		mapped := fmt.Errorf("%w: release returned nil running process", launch.ErrReleaseUncertain)
		cmd.failReleasedByGroup(ctx, mapped)
		return mapped
	}
	if !running.Ref().Equal(cmd.group) {
		mapped := fmt.Errorf("%w: released group mismatch", launch.ErrReleaseUncertain)
		cmd.failReleasedByGroup(ctx, mapped)
		return mapped
	}
	child, evidence, err := admissionReleaseObservation(cmd.group)
	if err != nil {
		cmd.failReleased(ctx, running, err)
		return err
	}
	releaseOutcome, releaseErr := cmd.coordinator.launch.RecordRelease(ctx, cmd.launchContext, cmd.group)
	if mapped := admissionDurabilityError("record_release", releaseOutcome, releaseErr); mapped != nil {
		cmd.failReleased(ctx, running, mapped)
		return mapped
	}
	_ = child
	_ = evidence

	cmd.mu.Lock()
	cmd.running = running
	cmd.released = true
	cmd.grant = grant
	cmd.mu.Unlock()

	if err := cmd.stdin.attach(running.Stdin()); err != nil {
		cmd.failReleased(ctx, running, err)
		return err
	}
	go copyAndClosePipe(cmd.stdoutWriter, running.Stdout())
	go copyAndClosePipe(cmd.stderrWriter, running.Stderr())
	go cmd.waitAndRecordQuiescence(context.WithoutCancel(ctx), running)
	return nil
}

func (cmd *legacyFencedCommand) reject(ctx context.Context, cause model.TerminalCause) error {
	cmd.mu.Lock()
	if cmd.rejected {
		err := cmd.err
		cmd.mu.Unlock()
		return err
	}
	if cmd.released {
		cmd.mu.Unlock()
		return fmt.Errorf("%w: legacy fenced command already released", authority.ErrInvalidRequest)
	}
	cmd.rejected = true
	cmd.mu.Unlock()

	err := cmd.rejectAuthority(ctx, cause)
	cmd.closePipesWithError(err)
	cmd.finish(command.ExitObservation{}, err)
	return err
}

func (cmd *legacyFencedCommand) rejectAuthority(ctx context.Context, cause model.TerminalCause) error {
	cleanupCtx, cancel := detachedAdmissionCleanupContext(ctx)
	defer cancel()

	var rejectErr error
	if _, err := cmd.coordinator.ready.BeginReject(cleanupCtx, cmd.launchContext.JobID, cmd.launchContext.Attempt); err != nil {
		rejectErr = fmt.Errorf("begin legacy fenced reject: %w", err)
	}
	verified, cleanup, err := cmd.prepared.AbortAndVerify(cleanupCtx)
	if err != nil {
		reason := fmt.Errorf("retire legacy fenced prepared process: %w", err)
		return errors.Join(rejectErr, reason, cmd.coordinator.failStop(ctx, errors.Join(rejectErr, reason)), launch.ErrFailClosed)
	}
	outcome, err := cmd.coordinator.launch.RecordQuiescence(cleanupCtx, cmd.launchContext, verified)
	if mapped := admissionDurabilityError("record_quiescence", outcome, err); mapped != nil {
		reason := errors.Join(rejectErr, mapped)
		return errors.Join(reason, cmd.coordinator.failStop(ctx, reason), launch.ErrFailClosed)
	}
	if rejectErr != nil {
		reason := errors.Join(rejectErr, cleanup.Err)
		return errors.Join(reason, cmd.coordinator.failStop(ctx, reason), launch.ErrFailClosed)
	}
	_, err = cmd.coordinator.ready.Finalize(cleanupCtx, cmd.launchContext.JobID, cmd.launchContext.Attempt, model.TerminalIntent{
		Outcome: model.OutcomeCanceled,
		Cause:   cause,
	})
	if err != nil {
		reason := errors.Join(err, cleanup.Err)
		return errors.Join(reason, cmd.coordinator.failStop(ctx, reason), launch.ErrFailClosed)
	}
	return cleanup.Err
}

func (cmd *legacyFencedCommand) failPrepared(ctx context.Context, reason error) {
	err := errors.Join(reason, cmd.coordinator.abortLegacyPrepared(ctx, cmd.prepared, true, cmd.launchContext))
	cmd.closePipesWithError(err)
	cmd.finish(command.ExitObservation{}, err)
}

func (cmd *legacyFencedCommand) failReleasedByGroup(ctx context.Context, reason error) {
	cleanupCtx, cancel := detachedAdmissionCleanupContext(ctx)
	defer cancel()

	verified, cleanup, containErr := cmd.coordinator.launch.ContainAndVerifyWithCleanup(cleanupCtx, cmd.launchContext, cmd.group, custodian.QuiescenceCauseContain)
	if containErr == nil {
		outcome, recordErr := cmd.coordinator.launch.RecordQuiescence(cleanupCtx, cmd.launchContext, verified)
		containErr = errors.Join(cleanup.Err, admissionDurabilityError("record_quiescence", outcome, recordErr))
	}
	err := errors.Join(reason, containErr, cmd.coordinator.failStop(ctx, errors.Join(reason, containErr)), launch.ErrFailClosed)
	cmd.closePipesWithError(err)
	cmd.finish(command.ExitObservation{}, err)
}

func (cmd *legacyFencedCommand) failReleased(ctx context.Context, running launch.RunningProcess, reason error) {
	cleanupCtx, cancel := detachedAdmissionCleanupContext(ctx)
	defer cancel()

	verified, cleanup, containErr := running.ContainAndVerify(cleanupCtx, custodian.QuiescenceCauseContain)
	if containErr == nil {
		outcome, recordErr := cmd.coordinator.launch.RecordQuiescence(cleanupCtx, cmd.launchContext, verified)
		containErr = errors.Join(cleanup.Err, admissionDurabilityError("record_quiescence", outcome, recordErr))
	}
	err := errors.Join(reason, containErr, cmd.coordinator.failStop(ctx, errors.Join(reason, containErr)), launch.ErrFailClosed)
	cmd.closePipesWithError(err)
	cmd.finish(command.ExitObservation{}, err)
}

func (cmd *legacyFencedCommand) waitAndRecordQuiescence(ctx context.Context, running launch.RunningProcess) {
	exit, verified, cleanup, err := running.WaitAndVerify(ctx)
	if err == nil {
		outcome, recordErr := cmd.coordinator.launch.RecordQuiescence(ctx, cmd.launchContext, verified)
		err = errors.Join(cleanup.Err, admissionDurabilityError("record_quiescence", outcome, recordErr))
	}
	cmd.finish(exit, err)
}

func (cmd *legacyFencedCommand) closePipesWithError(err error) {
	_ = cmd.stdin.Close()
	_ = cmd.stdoutWriter.CloseWithError(err)
	_ = cmd.stderrWriter.CloseWithError(err)
}

func (cmd *legacyFencedCommand) finish(exit command.ExitObservation, err error) {
	cmd.doneOnce.Do(func() {
		cmd.mu.Lock()
		cmd.exit = exit
		cmd.err = err
		cmd.mu.Unlock()
		close(cmd.done)
	})
}

func (cmd *legacyFencedCommand) result() (command.ExitObservation, error) {
	cmd.mu.Lock()
	defer cmd.mu.Unlock()
	return cmd.exit, cmd.err
}

type legacyFencedStdin struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	target io.WriteCloser
	closed bool
}

func (s *legacyFencedStdin) Write(p []byte) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if s.target == nil {
		n, err := s.buffer.Write(p)
		s.mu.Unlock()
		return n, err
	}
	target := s.target
	s.mu.Unlock()
	return target.Write(p)
}

func (s *legacyFencedStdin) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	target := s.target
	s.mu.Unlock()
	if target != nil {
		return target.Close()
	}
	return nil
}

func (s *legacyFencedStdin) attach(target io.WriteCloser) error {
	if target == nil {
		return nil
	}
	s.mu.Lock()
	if s.target != nil {
		s.mu.Unlock()
		return nil
	}
	data := append([]byte(nil), s.buffer.Bytes()...)
	closed := s.closed
	s.buffer.Reset()
	s.target = target
	s.mu.Unlock()
	if len(data) != 0 {
		if _, err := target.Write(data); err != nil {
			return err
		}
	}
	if closed {
		return target.Close()
	}
	return nil
}

func copyAndClosePipe(dst *io.PipeWriter, src io.ReadCloser) {
	if src == nil {
		_ = dst.Close()
		return
	}
	_, err := io.Copy(dst, src)
	_ = src.Close()
	if err != nil {
		_ = dst.CloseWithError(err)
		return
	}
	_ = dst.Close()
}

func admissionReleaseObservation(group model.GroupRef) (model.ChildIdentity, model.Evidence, error) {
	child := model.ChildIdentity{
		PID:               group.Leader.PID,
		HighResStartToken: group.Leader.HighResStartToken,
	}
	if err := child.Validate(); err != nil {
		return model.ChildIdentity{}, model.Evidence{}, err
	}
	evidence, err := model.NewEvidence("custodian_release", fmt.Sprintf("release acknowledged for custody %s ordinal %s", group.CustodyID, group.Launch.Ordinal))
	if err != nil {
		return model.ChildIdentity{}, model.Evidence{}, err
	}
	return child, evidence, nil
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

func (a servedLaunchAuthority) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (launch.DurabilityOutcome, error) {
	if a.ready == nil {
		return launch.DefinitelyNotCommitted, authority.ErrNotReady
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
}

func (a *servedAdmissionAuthority) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordQuiescence(ctx, jobID, ordinal, verified)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RequestCancel(ctx context.Context, jobID model.JobID) (coordinator.StepResult, error) {
	applied, err := a.ready.RequestCancel(ctx, jobID)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordOutcome(ctx, jobID, ref, outcome)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RecordResult(ctx context.Context, jobID model.JobID, ref model.AttemptRef, receipt model.ResultReceipt) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordResult(ctx, jobID, ref, receipt)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent) (coordinator.StepResult, error) {
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

func jobSubmitIdentified(raw json.RawMessage, params protocol.JobSubmitParams) (bool, error) {
	workspaceKeyPresent, err := jsonFieldPresent(raw, "workspaceKey")
	if err != nil {
		return false, err
	}
	requestIDPresent, err := jsonFieldPresent(raw, "requestId")
	if err != nil {
		return false, err
	}
	return workspaceKeyPresent || requestIDPresent || params.WorkspaceKey != "" || params.RequestID != "", nil
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

func (s *Server) handleIdentifiedJobSubmit(ctx context.Context, raw json.RawMessage, params protocol.JobSubmitParams) requestOutcome {
	// Snapshot-checkout: the whole submit (validation, session construction,
	// durable acceptance) runs against the checked-out instance, never under
	// admissionStateMu (see activeWork). A submit racing closeServeAdmission
	// gets typed errors from the closing objects.
	s.admissionStateMu.RLock()
	instance := s.admissionInstance
	s.admissionStateMu.RUnlock()
	if instance == nil || !instance.policy.strictRouteEnabled || instance.ready == nil || instance.submission == nil {
		reason := "admission authority is not ready"
		if instance != nil && instance.policy.strictRouteDisabledReason != "" {
			reason = instance.policy.strictRouteDisabledReason
		}
		return requestOutcome{err: strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectStrictRouteDisabled,
			reason,
			protocol.ErrorData{},
		)}
	}

	// Strict admission rejection order is policy/route, identity, static
	// backend fenceability, strict config, native runtime, replay, then durable
	// acceptance. Dynamic session capability verification is the ordering
	// exception: a backend can violate its controlled-runner descriptor only
	// when Start constructs a session, so that contract check runs immediately
	// after Start and before any durable admission mutation.
	requestKey, err := model.NewRequestKey(params.WorkspaceKey, params.RequestID)
	if err != nil {
		return requestOutcome{err: strictAdmissionProtocolError(
			protocol.ErrorInvalidTaskSpec,
			protocol.AdmissionRejectMissingIdentity,
			err.Error(),
			protocol.ErrorData{},
		)}
	}

	spec := params.TaskSpec
	var descriptor admissionBackendDescriptor
	if spec.Backend != "" {
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
		fenceability, _ := instance.policy.backendFenceability(spec.Backend)
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

	var strictParams protocol.JobSubmitParams
	if err := decodeStrict(raw, &strictParams); err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	params = strictParams
	spec = params.TaskSpec
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
	rawTaskSpec, err := rawTaskSpecFromSubmitParams(raw)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	taskIdentity, err := model.TaskIdentityFromRawTaskSpec(rawTaskSpec)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	canonicalCWD, err := engine.CanonicalWorkspace(spec.CWD)
	if err != nil {
		return requestOutcome{err: strictAdmissionInvalidConfigError(err.Error(), protocol.ErrorData{Backend: spec.Backend})}
	}
	workspaceLayoutKey := model.WorkspaceKey(engine.WorkspaceKey(canonicalCWD))

	if !instance.policy.strictRuntimeAvailable() {
		return requestOutcome{err: strictAdmissionRuntimeUnavailableError(instance.policy.runtimeAssessment, protocol.ErrorData{Backend: spec.Backend})}
	}

	replay, err := instance.ready.LookupReplay(ctx, requestKey)
	if err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}
	switch replay.State {
	case authority.ReplayLive:
		if !replay.Binding.TaskIdentity.Equal(taskIdentity) || replay.Binding.Mode != model.ModeIdentifiedFenced {
			return requestOutcome{err: admissionProtocolError(authority.ErrReplayConflict)}
		}
		return requestOutcome{result: protocol.JobSubmitResult{
			JobID:        replay.Record.JobID.String(),
			State:        admissionState(replay.Projection.Public),
			Deduplicated: true,
		}}
	case authority.ReplayExpired:
		if !replay.Tombstone.TaskIdentity.Equal(taskIdentity) {
			return requestOutcome{err: admissionProtocolError(authority.ErrReplayConflict)}
		}
		return requestOutcome{err: admissionProtocolError(authority.ErrRequestExpired)}
	}

	admissionSessionID := s.nextID("ses")
	session, err := descriptor.backend.Start(ctx, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout})
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
	if id := session.ID(); id != "" {
		admissionSessionID = id
	}
	request := authority.AcceptRequest{
		RequestKey:         requestKey,
		WorkspaceLayoutKey: workspaceLayoutKey,
		TaskIdentity:       taskIdentity,
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          admissionSessionID,
	}
	s.admissionSubmitMu.Lock()
	if !s.admissionInstanceStillReadyLocked(instance) {
		s.admissionSubmitMu.Unlock()
		return requestOutcome{err: admissionProtocolError(authority.ErrNotReady)}
	}
	accepted, err := instance.submission.SubmitIdentified(ctx, request)
	s.admissionSubmitMu.Unlock()
	if err != nil {
		if admissionAcceptCommitted(accepted) {
			return requestOutcome{err: admissionPostAcceptError(accepted, err)}
		}
		return requestOutcome{err: admissionProtocolError(err)}
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
	s.markAdmissionJob(jobID)
	run := jobRun{
		jobID:               jobID,
		sessionID:           admissionSessionID,
		backend:             spec.Backend,
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
		admissionControlled: true,
		admissionMode:       model.ModeIdentifiedFenced,
		admissionAccepted:   accepted,
		admissionLaunch: admissionLaunchBinding{
			coordinator: instance.submission,
			jobID:       jobModelID,
			attempt:     accepted.Record.Attempt.Ref,
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

func (s *Server) strictRouteDisabledPrecheck() *protocol.ErrorObject {
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
			protocol.AdmissionRejectStrictRouteDisabled,
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
		if !ok {
			return nil
		}
		return launchErr
	})
	if err != nil {
		_ = s.finalizeTerminal(run, engine.StateFailed, "", nil)
		return
	}
	if launched.active == nil {
		return
	}
	s.runJob(runCtx, launched)
}

func (s *Server) handleAdmissionResponseOutcome(ctx context.Context, run jobRun, delivered bool) {
	action := model.OnResponseOutcome(run.admissionMode, delivered)
	switch action {
	case model.RunAcceptedObligation, model.RetainObligationForReplay:
		go s.launchAdmittedJob(ctx, run)
	case model.AcknowledgeGrantAndRelease:
		if err := s.withAdmissionSubmission(func(submission *servedSubmissionCoordinator) error {
			return submission.acknowledgeGrantAndReleaseLegacyFenced(ctx, run.legacyFencedCommand)
		}); err != nil {
			_ = s.finalizeTerminal(run, engine.StateFailed, "", nil)
			return
		}
		go s.launchAdmittedJob(ctx, run)
	case model.RejectAndRetireNoGrant:
		err := s.withAdmissionSubmission(func(submission *servedSubmissionCoordinator) error {
			if run.legacyFencedCommand != nil {
				return submission.rejectAndRetireLegacyFenced(ctx, run.legacyFencedCommand)
			}
			return submission.rejectLegacyFencedBeforePrepare(ctx, run.admissionAccepted)
		})
		if err != nil {
			_ = s.failStopAdmissionReady(ctx, err)
		}
	case model.RunLegacyUnfenced:
		go s.launchLegacyUnfencedJob(ctx, run)
	case model.RejectLegacyUnfencedBeforeRun:
		if err := s.withAdmissionSubmission(func(submission *servedSubmissionCoordinator) error {
			return submission.rejectLegacyUnfencedBeforeRun(ctx, run.admissionAccepted)
		}); err != nil {
			_ = s.failStopAdmissionReady(ctx, err)
		}
	}
}

func (s *Server) launchLegacyUnfencedJob(ctx context.Context, run jobRun) {
	var launched jobRun
	var runCtx context.Context
	err := s.withAdmissionJobEffectErr(run.jobID, func() error {
		var snapshot coordinator.JobSnapshot
		if err := s.withAdmissionCoordinator(func(coord *admissionCoordinator) error {
			var err error
			snapshot, err = coord.Snapshot(ctx, model.JobID(run.jobID))
			return err
		}); err != nil {
			return err
		}
		if snapshot.Record.Terminal != nil || snapshot.Record.Cancel != nil {
			return nil
		}
		s.mu.Lock()
		_, active := s.activeJobs[run.jobID]
		backend := s.backends[run.backend]
		s.mu.Unlock()
		if active {
			return nil
		}
		if backend == nil {
			return errors.New("backend is unavailable")
		}
		session, err := backend.Start(ctx, engine.SessionOpts{CWD: run.cwd, Write: run.write, Model: run.model, Effort: run.effort, Timeout: run.timeout})
		if err != nil {
			return err
		}
		run.session = session
		var cancel context.CancelFunc
		runCtx, cancel = context.WithCancel(ctx)
		activeJob := &activeJob{jobID: run.jobID, sessionID: run.sessionID, session: session, cancel: cancel}
		run.active = activeJob
		launched = run
		s.addActiveJob(activeJob)
		return nil
	})
	if err != nil {
		_ = s.finalizeTerminal(run, engine.StateFailed, "", nil)
		return
	}
	if launched.active == nil {
		return
	}
	s.runJob(runCtx, launched)
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
		return run, nil, false, errors.New("admission session is not ready")
	}
	runCtx, cancel := context.WithCancel(ctx)
	activeJob := &activeJob{jobID: run.jobID, sessionID: run.sessionID, session: run.session, cancel: cancel}
	run.active = activeJob
	s.addActiveJob(activeJob)
	return run, runCtx, true, nil
}

func admissionProtocolError(err error) *protocol.ErrorObject {
	switch {
	case errors.Is(err, authority.ErrReplayConflict):
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, authority.ErrRequestExpired):
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, model.ErrIncompatibleExecutionCapabilities):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	case errors.Is(err, custodian.ErrSupervisorUnavailable):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	case admissionAuthorityNotReadyError(err):
		return strictAdmissionProtocolError(
			protocol.ErrorCapabilityMissing,
			protocol.AdmissionRejectStrictRouteDisabled,
			admissionNotReadyMessage(err),
			protocol.ErrorData{},
		)
	default:
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
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
	// S4E wires listener halt and recovery consumption; this path only reports
	// that the obligation was durably accepted and the authority fail-stopped.
	return protocol.NewError(protocol.ErrorBackendUnavailable, fmt.Sprintf("admission accepted job %s and fail-stopped before launch: %v", jobID, err), protocol.ErrorData{JobID: jobID})
}

func admissionState(state model.PublicState) engine.JobState {
	return engine.JobState(state.String())
}

func (s *Server) authorityStatus(jobID string) (protocol.JobStatus, bool, *protocol.ErrorObject) {
	_, projection, ok, errObj := s.authorityJobProjection(jobID)
	if !ok || errObj != nil {
		return protocol.JobStatus{}, ok, errObj
	}
	return protocol.JobStatus{
		JobID:     projection.JobID.String(),
		SessionID: projection.SessionID,
		State:     admissionState(projection.Public),
	}, true, nil
}

func (s *Server) listAuthorityStatuses() ([]protocol.JobStatus, *protocol.ErrorObject) {
	// Snapshot-checkout (see activeWork): repository reads must not hold
	// admissionStateMu.
	s.admissionStateMu.RLock()
	repo := s.admissionRepository
	ready := s.admissionInstance != nil && repo != nil
	s.admissionStateMu.RUnlock()
	if !ready {
		return nil, nil
	}
	var statuses []protocol.JobStatus
	var authorityListErr *protocol.ErrorObject
	if err := repo.View(context.Background(), func(tx repository.ReadTx) error {
		images, err := tx.ListJobs(repository.JobFilter{})
		if err != nil {
			return err
		}
		for _, image := range images {
			status, ok, errObj := authorityStatusFromImage(image)
			if errObj != nil {
				authorityListErr = errObj
				return errors.New(errObj.Message)
			}
			if ok {
				statuses = append(statuses, status)
			}
		}
		return nil
	}); err != nil {
		if authorityListErr != nil {
			return nil, authorityListErr
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
	if record.Terminal != nil && record.Terminal.Result != nil {
		result = authorityResultInfo(*record.Terminal.Result)
	} else if record.Result != nil {
		result = authorityResultInfo(record.Result.Result)
	}
	return protocol.JobResult{
		JobID:     projection.JobID.String(),
		SessionID: projection.SessionID,
		State:     admissionState(projection.Public),
		Result:    result,
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
	if image.Safety.State == repository.RecordMissing && image.Projection.State == repository.RecordMissing {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	if image.Safety.State != repository.RecordValid {
		return model.SafetyRecord{}, model.JobProjection{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority safety record is not valid", protocol.ErrorData{JobID: jobID})
	}
	if image.Projection.State != repository.RecordValid {
		return model.SafetyRecord{}, model.JobProjection{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority projection is not valid", protocol.ErrorData{JobID: jobID})
	}
	return image.Safety.Value, image.Projection.Value, true, nil
}

func authorityStatusFromImage(image repository.JobImage) (protocol.JobStatus, bool, *protocol.ErrorObject) {
	if image.Safety.State == repository.RecordMissing && image.Projection.State == repository.RecordMissing {
		return protocol.JobStatus{}, false, nil
	}
	jobID := ""
	if image.Safety.State == repository.RecordValid {
		jobID = image.Safety.Value.JobID.String()
	} else if image.Projection.State == repository.RecordValid {
		jobID = image.Projection.Value.JobID.String()
	}
	if image.Safety.State != repository.RecordValid {
		return protocol.JobStatus{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority safety record is not valid", protocol.ErrorData{JobID: jobID})
	}
	if image.Projection.State != repository.RecordValid {
		return protocol.JobStatus{}, false, protocol.NewError(protocol.ErrorInvalidTaskSpec, "authority projection is not valid", protocol.ErrorData{JobID: jobID})
	}
	projection := image.Projection.Value
	return protocol.JobStatus{
		JobID:     projection.JobID.String(),
		SessionID: projection.SessionID,
		State:     admissionState(projection.Public),
	}, true, nil
}

func authorityResultInfo(ref model.ResultRef) *engine.ResultInfo {
	return &engine.ResultInfo{
		ResultPath: ref.Path,
		SHA256:     ref.Digest,
		Bytes:      ref.Bytes,
	}
}
