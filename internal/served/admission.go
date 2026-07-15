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
	"sync"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/coordinator"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	admissionRepositoryFile = "admission.bbolt"
	admissionAnchorFile     = "admission-anchor.json"
)

type admissionBootstrapper = authority.Bootstrapper
type admissionReady = authority.Ready
type admissionCoordinator = coordinator.Coordinator

type admissionBootstrapperFactory func(context.Context, *Server) (*admissionBootstrapper, repository.Repository, io.Closer, error)

func (s *Server) bootstrapAdmission(ctx context.Context) error {
	supervisor := s.admissionSupervisor
	if supervisor == nil {
		supervisor = newServedAdmissionSupervisor(s)
		s.admissionSupervisor = supervisor
	}
	if err := supervisor.verifiedContainmentSupported(ctx); err != nil {
		s.jobsRequestIDEnabled = false
		return err
	}
	if s.admissionReady != nil && s.admissionCoordinator != nil {
		return nil
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

	boot, err := model.NewBootRef(s.nextID("boot"), s.nextID("owner"))
	if err != nil {
		return err
	}
	supervisor.SetBoot(boot)
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return err
	}
	if err := recoverAdmissionBeforeReady(ctx, session, repo, supervisor, boot); err != nil {
		return err
	}
	if err := s.reapKnownStores(); err != nil {
		return err
	}
	ready, err := session.SealReady(ctx)
	if err != nil {
		return err
	}
	adapter := &servedAdmissionAuthority{ready: ready}
	owner, err := model.NewOwnerID(s.nextID("coordinator"))
	if err != nil {
		return err
	}
	coord, err := coordinator.New(adapter, supervisor, servedResultPublisher{server: s}, owner)
	if err != nil {
		return err
	}

	s.admissionBootstrapper = bootstrapper
	s.admissionReady = ready
	s.admissionCoordinator = coord
	s.admissionSupervisor = supervisor
	s.admissionClose = closer
	closeOnErr = false
	return nil
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
	}
	options := []authority.BootstrapperOption{authority.WithAnchor(anchor)}
	if s.admissionSupervisor != nil {
		options = append(options, authority.WithQuiescenceVerifier(s.admissionSupervisor.runtime.Verifier()))
	}
	bootstrapper, err := authority.NewBootstrapper(repo, options...)
	if err != nil {
		_ = repo.Close()
		return nil, nil, nil, err
	}
	return bootstrapper, repo, repo, nil
}

func recoverAdmissionBeforeReady(ctx context.Context, session *authority.RecoverySession, repo repository.Repository, supervisor coordinator.Supervisor, boot model.BootRef) error {
	const maxStartupRecoverySteps = 1024
	for i := 0; i < maxStartupRecoverySteps; i++ {
		plans, err := session.Plans(ctx)
		if err != nil {
			return err
		}
		if len(plans) == 0 {
			return nil
		}
		progressed := false
		jobs, err := admissionStartupRecoveryJobs(ctx, repo, boot)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return fmt.Errorf("%w: startup recovery could not identify job for %d plan(s)", authority.ErrRecoveryNeeded, len(plans))
		}
		for _, job := range jobs {
			switch job.plan.Next.Kind {
			case model.RecoveryFinalizeCertified:
				if job.plan.Next.Finalize == nil {
					return fmt.Errorf("%w: startup recovery finalize action missing receipt", authority.ErrRecoveryNeeded)
				}
				if err := session.Finalize(ctx, job.plan.Next.Finalize.Ref, job.plan.Next.Finalize.Intent); err != nil {
					return err
				}
				progressed = true
			case model.RecoveryRetireThenFinalize:
				for _, ordinal := range admissionUnquiescedOrdinals(job.record) {
					prepared, err := admissionPreparedFromRecord(job.record, ordinal)
					if err != nil {
						return err
					}
					verified, err := supervisor.Retire(ctx, prepared)
					if err != nil {
						return err
					}
					if err := session.RecordQuiescence(ctx, job.record.JobID, ordinal, verified); err != nil {
						return err
					}
				}
				progressed = true
			case model.RecoveryContainThenFinalize:
				for _, ordinal := range admissionUnquiescedOrdinals(job.record) {
					prepared, err := admissionPreparedFromRecord(job.record, ordinal)
					if err != nil {
						return err
					}
					verified, err := supervisor.Contain(ctx, prepared)
					if err != nil {
						return err
					}
					if err := session.RecordQuiescence(ctx, job.record.JobID, ordinal, verified); err != nil {
						return err
					}
				}
				progressed = true
			case model.RecoveryFatalUnprovable:
				return fmt.Errorf("%w: startup recovery action %d is fatal for %s", authority.ErrRecoveryNeeded, job.plan.Next.Kind, job.record.JobID)
			default:
				return fmt.Errorf("%w: startup recovery action %d is unknown", authority.ErrRecoveryNeeded, job.plan.Next.Kind)
			}
		}
		if !progressed {
			return fmt.Errorf("%w: startup recovery made no progress", authority.ErrRecoveryNeeded)
		}
	}
	return fmt.Errorf("%w: startup recovery did not converge", authority.ErrRecoveryNeeded)
}

type fileAuthorityAnchor struct {
	mu          sync.Mutex
	path        string
	dbUUID      string
	schemaMajor uint16
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
	return a.save(snapshot)
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

type servedAdmissionAuthority struct {
	ready *authority.Ready
}

func (a *servedAdmissionAuthority) Accept(ctx context.Context, request coordinator.AdmissionRequest) (coordinator.AdmissionResult, error) {
	accepted, err := a.ready.Accept(ctx, authority.AcceptRequest{
		RequestKey:   request.RequestKey,
		TaskIdentity: request.TaskIdentity,
		Mode:         request.Mode,
		SessionID:    request.SessionID,
	})
	if err != nil {
		return coordinator.AdmissionResult{}, err
	}
	return coordinator.AdmissionResult{Record: accepted.Record, Projection: accepted.Projection, Replayed: accepted.Replayed}, nil
}

func (a *servedAdmissionAuthority) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef) (coordinator.StepResult, error) {
	applied, err := a.ready.BindGroup(ctx, jobID, ref, ordinal, group)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) CommitGrant(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, nonce model.PermitNonce) (coordinator.StepResult, error) {
	applied, err := a.ready.CommitGrant(ctx, jobID, ref, ordinal, nonce)
	return admissionStepResult(applied, err)
}

func (a *servedAdmissionAuthority) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, evidence model.Evidence) (coordinator.StepResult, error) {
	applied, err := a.ready.RecordRelease(ctx, jobID, ref, ordinal, child, evidence)
	return admissionStepResult(applied, err)
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

func (a *servedAdmissionAuthority) ClaimPending(ctx context.Context, ref model.AttemptRef, owner model.OwnerID) error {
	return a.ready.ClaimPending(ctx, ref, owner)
}

func (a *servedAdmissionAuthority) HasOwnedWork(ctx context.Context) (bool, error) {
	snapshot, err := a.ready.RuntimeSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return len(snapshot.Pending) != 0 || len(snapshot.Owned) != 0, nil
}

func (a *servedAdmissionAuthority) FailStop(ctx context.Context, err error) error {
	if err == nil {
		return a.ready.FailStop(ctx, "")
	}
	return a.ready.FailStop(ctx, err.Error())
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
	if s.admissionCoordinator == nil || s.admissionReady == nil || s.admissionSupervisor == nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorCapabilityMissing, "admission authority is not ready", protocol.ErrorData{})}
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

	s.admissionSubmitMu.Lock()
	defer s.admissionSubmitMu.Unlock()

	replay, err := s.admissionReady.LookupReplay(ctx, requestKey)
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
	s.mu.Lock()
	store, err := s.storeForCWDLocked(spec.CWD)
	s.mu.Unlock()
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})}
	}

	admissionSessionID := s.nextID("ses")
	accepted, err := s.admissionCoordinator.Submit(ctx, coordinator.AdmissionRequest{
		RequestKey:   requestKey,
		TaskIdentity: taskIdentity,
		Mode:         model.ModeIdentifiedFenced,
		SessionID:    admissionSessionID,
	})
	if err != nil {
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
	if err := s.admissionSupervisor.Register(jobModelID, backend, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout}); err != nil {
		_ = s.admissionCoordinator.Cancel(context.Background(), jobModelID, nil)
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	if err := s.admissionCoordinator.PrepareSupervisor(ctx, jobModelID, nil); err != nil {
		_ = s.admissionCoordinator.Cancel(context.Background(), jobModelID, nil)
		return requestOutcome{err: admissionProtocolError(err)}
	}
	s.mu.Lock()
	s.jobStores[jobID] = store
	s.mu.Unlock()
	if err := s.createQueuedRecord(store, jobID, admissionSessionID, spec.Backend, spec.Tags, policy.policy, policy.contract, false); err != nil {
		_ = s.admissionCoordinator.Cancel(context.Background(), jobModelID, nil)
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	s.markAdmissionJob(jobID)
	run := jobRun{
		jobID:               jobID,
		sessionID:           admissionSessionID,
		backend:             spec.Backend,
		store:               store,
		prompt:              spec.Prompt,
		write:               spec.Write,
		policy:              policy.policy,
		contract:            policy.contract,
		contractName:        policy.name,
		contractHash:        policy.hash,
		timeout:             timeout,
		admissionControlled: true,
	}
	return requestOutcome{
		result:       protocol.JobSubmitResult{JobID: jobID, State: engine.StateQueued},
		after:        func() { go s.launchAdmittedJob(ctx, run) },
		onAckFailure: func(error) { s.abortUndeliveredAdmissionRun(run) },
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

func (s *Server) prepareAdmittedJobLaunch(ctx context.Context, run jobRun) (jobRun, context.Context, bool, error) {
	if s.admissionCoordinator == nil || s.admissionSupervisor == nil {
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
	nonce := model.PermitNonce("permit-" + run.jobID + "-1")
	if err := s.admissionCoordinator.GrantPermit(ctx, jobID, 1, nonce, nil); err != nil {
		return run, nil, false, err
	}
	snapshot, err = s.admissionCoordinator.Snapshot(ctx, jobID)
	if err != nil {
		return run, nil, false, err
	}
	if snapshot.Record.Terminal != nil || snapshot.Record.Cancel != nil {
		return run, nil, false, nil
	}
	if err := s.admissionCoordinator.Start(ctx, jobID, nil); err != nil {
		return run, nil, false, err
	}
	session, sessionID, err := s.admissionSupervisor.Started(jobID, model.LaunchOrdinalOne)
	if err != nil {
		return run, nil, false, err
	}
	if sessionID != "" {
		run.sessionID = sessionID
	}
	run.session = session
	runCtx, cancel := context.WithCancel(ctx)
	active := &activeJob{jobID: run.jobID, sessionID: run.sessionID, session: session, cancel: cancel}
	run.active = active
	s.addActiveJob(active)
	s.admissionSupervisor.AttachActive(jobID, active)
	return run, runCtx, true, nil
}

func (s *Server) abortUndeliveredAdmissionRun(run jobRun) {
	jobID := model.JobID(run.jobID)
	if s.admissionCoordinator != nil {
		_ = s.admissionCoordinator.Cancel(context.Background(), jobID, nil)
	}
	if run.store != nil {
		_, _ = run.store.Cancel(run.jobID)
	}
	s.removeActiveJob(run.jobID)
	if run.onDone != nil {
		run.onDone()
	}
}

func admissionProtocolError(err error) *protocol.ErrorObject {
	switch {
	case errors.Is(err, authority.ErrReplayConflict):
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, authority.ErrRequestExpired):
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, custodian.ErrSupervisorUnavailable):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	case errors.Is(err, authority.ErrNotReady), errors.Is(err, coordinator.ErrCoordinatorNotReady):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	default:
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
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

func authorityResultInfo(ref model.ResultRef) *engine.ResultInfo {
	return &engine.ResultInfo{
		ResultPath: ref.Path,
		SHA256:     ref.Digest,
		Bytes:      ref.Bytes,
	}
}
