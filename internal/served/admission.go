package served

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
)

const (
	admissionRepositoryFile = "admission.bbolt"
	admissionAnchorFile     = "admission-anchor.json"

	admissionFailStopTimeout = 30 * time.Second
)

var admissionDetachedCleanupTimeout = 30 * time.Second

type admissionBootstrapper = authority.Bootstrapper
type admissionReady = authority.Ready
type admissionCoordinator = coordinator.Coordinator

type admissionBootstrapperFactory func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error)

type admissionProbeableBackend interface {
	ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error)
}

func (s *Server) bootstrapAdmission(ctx context.Context) error {
	runtime := s.admissionRuntime
	if runtime == nil {
		runtime = newServedAdmissionRuntime(s)
		s.admissionRuntime = runtime
	}
	if err := runtime.verifiedContainmentSupported(ctx); err != nil {
		s.jobsRequestIDEnabled = false
		return err
	}
	if s.admissionReady != nil && s.admissionCoordinator != nil {
		return nil
	}
	if err := s.probeAdmissionBackends(ctx); err != nil {
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
	closeOnErr = false
	return nil
}

func (s *Server) probeAdmissionBackends(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
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
	for _, name := range names {
		backend := s.backends[name]
		probeable, ok := backend.(admissionProbeableBackend)
		if !ok {
			if s.admissionUnprobeableBackends == nil {
				s.admissionUnprobeableBackends = make(map[string]error)
			}
			s.admissionUnprobeableBackends[name] = model.IncompatibleExecutionCapabilitiesError{
				Reason: "strict backend does not implement ProbeBackend; admission cannot verify command-runner capabilities",
			}
			continue
		}
		probed, err := probeable.ProbeBackend(ctx, runner)
		if err != nil {
			return fmt.Errorf("probe strict backend %s: %w", name, err)
		}
		if probed == nil {
			return fmt.Errorf("probe strict backend %s: nil probed backend", name)
		}
		if probed.Name() != name {
			return fmt.Errorf("probe strict backend %s: probed backend changed name to %s", name, probed.Name())
		}
		s.backends[name] = probed
		if s.admissionUnprobeableBackends != nil {
			delete(s.admissionUnprobeableBackends, name)
		}
	}
	return nil
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

func (s *Server) admissionExecutionCapabilities(backend engine.Backend) model.ExecutionCapabilities {
	return model.ExecutionCapabilities{
		ExternalRunner: admissionBackendExternalRunner(backend),
		FencedLaunch:   s.admissionRuntime != nil && s.admissionRuntime.support().ParkedExec,
	}
}

func (s *Server) admissionBackendProbeError(name string) error {
	if s == nil || s.admissionUnprobeableBackends == nil {
		return nil
	}
	return s.admissionUnprobeableBackends[name]
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

func (c *servedSubmissionCoordinator) SubmitRoutedIdentified(ctx context.Context, request authority.AcceptRequest, caps model.ExecutionCapabilities) (authority.AcceptResult, error) {
	mode, err := model.RouteSubmissionMode(caps)
	if err != nil {
		return authority.AcceptResult{}, err
	}
	if mode != model.ModeIdentifiedFenced {
		return authority.AcceptResult{}, fmt.Errorf("%w: routed mode %s is outside identified fenced submit", authority.ErrInvalidRequest, mode)
	}
	request.Mode = mode
	return c.SubmitIdentified(ctx, request)
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

func (c *servedSubmissionCoordinator) SubmitLegacyUnfenced(ctx context.Context, request authority.AcceptRequest) (authority.AcceptResult, error) {
	if c == nil || c.ready == nil {
		return authority.AcceptResult{}, authority.ErrNotReady
	}
	request.Mode = model.ModeLegacyUnfenced
	return c.ready.Accept(ctx, request)
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
	if s == nil || s.admissionReady == nil {
		return authority.ErrNotReady
	}
	var stopErr error
	if err == nil {
		stopErr = s.admissionReady.FailStop(ctx, "")
	} else {
		stopErr = s.admissionReady.FailStop(ctx, err.Error())
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
	if !s.jobsRequestIDEnabled {
		return requestOutcome{err: protocol.NewError(protocol.ErrorCapabilityMissing, "jobs.requestId capability is disabled", protocol.ErrorData{})}
	}
	if s.admissionCoordinator == nil || s.admissionReady == nil || s.admissionSubmission == nil || s.admissionRuntime == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorCapabilityMissing, "admission authority is not ready", protocol.ErrorData{})}
	}
	if err := s.admissionRuntime.verifiedContainmentSupported(ctx); err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}

	requestKey, err := model.NewRequestKey(params.WorkspaceKey, params.RequestID)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	rawTaskSpec, err := rawTaskSpecFromSubmitParams(raw)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	taskIdentity, err := model.TaskIdentityFromRawTaskSpec(rawTaskSpec)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}

	if errObj := validateTaskSpecEnvelope(raw); errObj != nil {
		return requestOutcome{err: errObj}
	}
	spec := params.TaskSpec
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec requires backend, absolute cwd, write, and prompt", protocol.ErrorData{})}
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMs)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	policy, err := s.resolvePolicy(spec.Policy)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	backend, ok := s.backends[spec.Backend]
	if !ok {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "backend is unavailable", protocol.ErrorData{})}
	}
	if err := s.admissionBackendProbeError(spec.Backend); err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}
	caps := s.admissionExecutionCapabilities(backend)
	mode, err := model.RouteSubmissionMode(caps)
	if err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}
	if mode == model.ModeIdentifiedFenced && !admissionBackendControlledRunner(backend) {
		return requestOutcome{err: admissionProtocolError(model.IncompatibleExecutionCapabilitiesError{
			Capabilities: caps,
			Reason:       "identified fenced admission requires a controlled command runner before acceptance",
		})}
	}
	canonicalCWD, err := engine.CanonicalWorkspace(spec.CWD)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}
	workspaceLayoutKey := model.WorkspaceKey(engine.WorkspaceKey(canonicalCWD))
	s.mu.Lock()
	store, err := s.storeForCWDLocked(canonicalCWD)
	s.mu.Unlock()
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}

	s.admissionSubmitMu.Lock()
	defer s.admissionSubmitMu.Unlock()

	replay, err := s.admissionReady.LookupReplay(ctx, requestKey)
	if err != nil {
		return requestOutcome{err: admissionProtocolError(err)}
	}
	switch replay.State {
	case authority.ReplayLive:
		if !replay.Binding.TaskIdentity.Equal(taskIdentity) || replay.Binding.Mode != mode {
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
	var session engine.Session
	if mode != model.ModeLegacyUnfenced {
		session, err = backend.Start(ctx, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout})
		if err != nil {
			return requestOutcome{err: backendError(err)}
		}
		if _, ok := session.(ordinalBoundSession); !ok {
			return requestOutcome{err: admissionProtocolError(custodian.ErrSupervisorUnavailable)}
		}
		if id := session.ID(); id != "" {
			admissionSessionID = id
		}
	}
	request := authority.AcceptRequest{
		RequestKey:         requestKey,
		WorkspaceLayoutKey: workspaceLayoutKey,
		TaskIdentity:       taskIdentity,
		Mode:               mode,
		SessionID:          admissionSessionID,
	}
	var accepted authority.AcceptResult
	var legacyFencedPreparation authority.LegacyFencedPreparation
	switch mode {
	case model.ModeIdentifiedFenced:
		accepted, err = s.admissionSubmission.SubmitIdentified(ctx, request)
	case model.ModeLegacyFenced:
		legacyFencedPreparation, err = s.admissionSubmission.PrepareLegacyFenced(ctx, request)
		accepted = legacyFencedPreparation.Admission
	case model.ModeLegacyUnfenced:
		accepted, err = s.admissionSubmission.SubmitLegacyUnfenced(ctx, request)
	default:
		err = model.IncompatibleExecutionCapabilitiesError{Capabilities: caps}
	}
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
	s.mu.Lock()
	s.jobStores[jobID] = store
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
		admissionMode:       mode,
		admissionAccepted:   accepted,
		admissionLaunch: admissionLaunchBinding{
			coordinator: s.admissionSubmission,
			jobID:       jobModelID,
			attempt:     accepted.Record.Attempt.Ref,
		},
	}
	if mode == model.ModeLegacyFenced {
		sessionWithRunner, ok := session.(ordinalBoundSession)
		if !ok {
			s.handleAdmissionResponseOutcome(ctx, run, false)
			return requestOutcome{err: admissionProtocolError(custodian.ErrSupervisorUnavailable)}
		}
		prepareRunner := &legacyFencedPrepareRunner{
			coordinator: s.admissionSubmission,
			preparation: legacyFencedPreparation,
		}
		events, err := sessionWithRunner.TurnWithRunner(ctx, engine.TurnInput{
			Prompt:  applyPrologue(policy.policy, spec.Prompt),
			Write:   spec.Write,
			Timeout: timeout,
		}, prepareRunner)
		if err != nil {
			run.legacyFencedCommand = prepareRunner.command
			s.handleAdmissionResponseOutcome(ctx, run, false)
			return requestOutcome{err: backendError(err)}
		}
		run.prestartedEvents = events
		run.legacyFencedCommand = prepareRunner.command
	}
	return requestOutcome{
		result:       protocol.JobSubmitResult{JobID: jobID, State: engine.StateQueued},
		after:        func() { s.handleAdmissionResponseOutcome(ctx, run, true) },
		onAckFailure: func(error) { s.handleAdmissionResponseOutcome(ctx, run, false) },
	}
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
		if err := s.admissionSubmission.acknowledgeGrantAndReleaseLegacyFenced(ctx, run.legacyFencedCommand); err != nil {
			_ = s.finalizeTerminal(run, engine.StateFailed, "", nil)
			return
		}
		go s.launchAdmittedJob(ctx, run)
	case model.RejectAndRetireNoGrant:
		var err error
		if run.legacyFencedCommand != nil {
			err = s.admissionSubmission.rejectAndRetireLegacyFenced(ctx, run.legacyFencedCommand)
		} else {
			err = s.admissionSubmission.rejectLegacyFencedBeforePrepare(ctx, run.admissionAccepted)
		}
		if err != nil && s.admissionReady != nil {
			_ = s.failStopAdmissionReady(ctx, err)
		}
	case model.RunLegacyUnfenced:
		go s.launchLegacyUnfencedJob(ctx, run)
	case model.RejectLegacyUnfencedBeforeRun:
		if err := s.admissionSubmission.rejectLegacyUnfencedBeforeRun(ctx, run.admissionAccepted); err != nil && s.admissionReady != nil {
			_ = s.failStopAdmissionReady(ctx, err)
		}
	}
}

func (s *Server) launchLegacyUnfencedJob(ctx context.Context, run jobRun) {
	var launched jobRun
	var runCtx context.Context
	err := s.withAdmissionJobEffectErr(run.jobID, func() error {
		if s.admissionCoordinator == nil {
			return errors.New("admission authority is not ready")
		}
		snapshot, err := s.admissionCoordinator.Snapshot(ctx, model.JobID(run.jobID))
		if err != nil {
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
	if s.admissionCoordinator == nil {
		return run, nil, false, errors.New("admission authority is not ready")
	}
	jobID := model.JobID(run.jobID)
	snapshot, err := s.admissionCoordinator.Snapshot(ctx, jobID)
	if err != nil {
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
	case errors.Is(err, authority.ErrNotReady), errors.Is(err, coordinator.ErrCoordinatorNotReady):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	default:
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
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
	if s.admissionRepository == nil {
		return nil, nil
	}
	var statuses []protocol.JobStatus
	var authorityListErr *protocol.ErrorObject
	if err := s.admissionRepository.View(context.Background(), func(tx repository.ReadTx) error {
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
	if s.admissionReady == nil {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	modelJobID, err := model.NewJobID(jobID)
	if err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, false, nil
	}
	image, err := s.admissionReady.LoadJob(context.Background(), modelJobID)
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
