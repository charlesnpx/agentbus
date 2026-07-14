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

type admissionBootstrapperFactory func(context.Context, *Server) (*admissionBootstrapper, io.Closer, error)

func (s *Server) bootstrapAdmission(ctx context.Context) error {
	if s.admissionReady != nil && s.admissionCoordinator != nil {
		return nil
	}

	factory := s.admissionBootstrapperFactory
	if factory == nil {
		factory = openAdmissionBootstrapper
	}
	bootstrapper, closer, err := factory(ctx, s)
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
	session, err := bootstrapper.Begin(ctx, boot)
	if err != nil {
		return err
	}
	if err := recoverAdmissionBeforeReady(ctx, session); err != nil {
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
	coord, err := coordinator.New(adapter, servedDormantSupervisor{}, nil, owner)
	if err != nil {
		return err
	}

	s.admissionBootstrapper = bootstrapper
	s.admissionReady = ready
	s.admissionCoordinator = coord
	s.admissionClose = closer
	closeOnErr = false
	return nil
}

func openAdmissionBootstrapper(ctx context.Context, s *Server) (*admissionBootstrapper, io.Closer, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	repo, err := bboltrepo.NewRepository(filepath.Join(s.stateRoot, admissionRepositoryFile))
	if err != nil {
		return nil, nil, err
	}
	dbUUID, schemaMajor, err := repo.AnchorIdentity()
	if err != nil {
		_ = repo.Close()
		return nil, nil, err
	}
	anchor := &fileAuthorityAnchor{
		path:        filepath.Join(s.stateRoot, admissionAnchorFile),
		dbUUID:      dbUUID,
		schemaMajor: schemaMajor,
	}
	bootstrapper, err := authority.NewBootstrapper(repo, authority.WithAnchor(anchor))
	if err != nil {
		_ = repo.Close()
		return nil, nil, err
	}
	return bootstrapper, repo, nil
}

func recoverAdmissionBeforeReady(ctx context.Context, session *authority.RecoverySession) error {
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
		for _, plan := range plans {
			switch plan.Next.Kind {
			case model.RecoveryFinalizeCertified:
				if plan.Next.Finalize == nil {
					return fmt.Errorf("%w: startup recovery finalize action missing receipt", authority.ErrRecoveryNeeded)
				}
				if err := session.ApplyReceipt(ctx, *plan.Next.Finalize); err != nil {
					return err
				}
				progressed = true
			case model.RecoveryRetireThenFinalize, model.RecoveryContainThenFinalize, model.RecoveryFatalUnprovable:
				return fmt.Errorf("%w: startup recovery action %d requires supervisor reconciliation before Ready", authority.ErrRecoveryNeeded, plan.Next.Kind)
			default:
				return fmt.Errorf("%w: startup recovery action %d is unknown", authority.ErrRecoveryNeeded, plan.Next.Kind)
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
	return atomicWrite(a.path, raw, 0o600)
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

func (a *servedAdmissionAuthority) Apply(ctx context.Context, jobID model.JobID, command model.Command) (coordinator.StepResult, error) {
	applied, err := a.ready.Apply(ctx, jobID, command)
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
	if trigger == model.RecoveryCancelAfterGrant && !admissionHasLaunchEvidence(snapshot.Record) {
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

func admissionHasLaunchEvidence(record model.SafetyRecord) bool {
	return record.Attempt.Grants.Count() != 0 || record.Attempt.Consumed.Count() != 0 || record.Attempt.Quiescence.Count() != 0
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
	if record.Attempt.Supervisor != nil && record.Attempt.Retirement == nil {
		plan.Next = model.RecoveryAction{Kind: model.RecoveryRetireThenFinalize}
		return plan
	}
	plan.Next = model.RecoveryAction{Kind: model.RecoveryFatalUnprovable}
	return plan
}

type servedDormantSupervisor struct{}

func (servedDormantSupervisor) Prepare(context.Context, coordinator.LaunchPlan) (coordinator.PreparedSupervisor, error) {
	return coordinator.PreparedSupervisor{}, errors.New("admission supervisor is not wired")
}

func (servedDormantSupervisor) SendPermit(context.Context, coordinator.PreparedSupervisor, model.LaunchGrant) error {
	return errors.New("admission supervisor is not wired")
}

func (servedDormantSupervisor) ObserveLaunch(context.Context, coordinator.PreparedSupervisor, model.LaunchGrant) (coordinator.LaunchObservation, error) {
	return coordinator.LaunchObservation{}, errors.New("admission supervisor is not wired")
}

func (servedDormantSupervisor) VerifyQuiescence(context.Context, coordinator.PreparedSupervisor, model.LaunchConsumed) (model.QuiescenceReceipt, error) {
	return model.QuiescenceReceipt{}, errors.New("admission supervisor is not wired")
}

func (servedDormantSupervisor) Contain(context.Context, coordinator.PreparedSupervisor) (model.ContainmentReceipt, error) {
	return model.ContainmentReceipt{}, errors.New("admission supervisor is not wired")
}

func (servedDormantSupervisor) Retire(context.Context, coordinator.PreparedSupervisor) (model.RetirementReceipt, error) {
	return model.RetirementReceipt{}, errors.New("admission supervisor is not wired")
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
	if s.admissionCoordinator == nil {
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

	if errObj := validateTaskSpecEnvelope(raw); errObj != nil {
		return requestOutcome{err: errObj}
	}
	spec := params.TaskSpec
	if spec.Backend == "" || spec.CWD == "" || !filepath.IsAbs(spec.CWD) || spec.Prompt == "" {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, "taskSpec requires backend, absolute cwd, write, and prompt", protocol.ErrorData{JobID: jobID})}
	}
	timeout, errObj := timeoutFromMillis(spec.TimeoutMs)
	if errObj != nil {
		return requestOutcome{err: errObj}
	}
	policy, err := s.resolvePolicy(spec.Policy)
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	backend, ok := s.backends[spec.Backend]
	if !ok {
		return requestOutcome{err: protocol.NewError(protocol.ErrorBackendUnavailable, "backend is unavailable", protocol.ErrorData{JobID: jobID})}
	}

	s.mu.Lock()
	store, err := s.storeForCWDLocked(spec.CWD)
	if err == nil {
		s.jobStores[jobID] = store
	}
	s.mu.Unlock()
	if err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	if err := s.createQueuedRecord(store, jobID, admissionSessionID, spec.Backend, spec.Tags, policy.policy, policy.contract, false); err != nil {
		return requestOutcome{err: protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{JobID: jobID})}
	}
	session, err := backend.Start(ctx, engine.SessionOpts{CWD: spec.CWD, Write: spec.Write, Model: spec.Model, Effort: spec.Effort, Timeout: timeout})
	if err != nil {
		return requestOutcome{err: backendError(err)}
	}
	sessionID := admissionSessionID
	if id := session.ID(); id != "" {
		sessionID = id
	}
	runCtx, cancel := context.WithCancel(ctx)
	active := &activeJob{jobID: jobID, sessionID: sessionID, session: session, cancel: cancel}
	s.addActiveJob(active)
	run := jobRun{
		jobID:        jobID,
		sessionID:    sessionID,
		backend:      spec.Backend,
		store:        store,
		session:      session,
		prompt:       spec.Prompt,
		write:        spec.Write,
		policy:       policy.policy,
		contract:     policy.contract,
		contractName: policy.name,
		contractHash: policy.hash,
		timeout:      timeout,
		active:       active,
	}
	return requestOutcome{
		result:       protocol.JobSubmitResult{JobID: jobID, State: engine.StateQueued},
		after:        func() { go s.runJob(runCtx, run) },
		onAckFailure: func(error) { s.abortUndeliveredRun(run, engine.StateCanceled) },
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

func admissionProtocolError(err error) *protocol.ErrorObject {
	switch {
	case errors.Is(err, authority.ErrReplayConflict):
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	case errors.Is(err, authority.ErrNotReady), errors.Is(err, coordinator.ErrCoordinatorNotReady):
		return protocol.NewError(protocol.ErrorCapabilityMissing, err.Error(), protocol.ErrorData{})
	default:
		return protocol.NewError(protocol.ErrorInvalidTaskSpec, err.Error(), protocol.ErrorData{})
	}
}

func admissionState(state model.PublicState) engine.JobState {
	return engine.JobState(state.String())
}
