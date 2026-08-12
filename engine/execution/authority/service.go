package authority

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

var (
	ErrInvalidRequest  = errors.New("authority invalid request")
	ErrNotReady        = errors.New("authority not ready")
	ErrRecoveryActive  = errors.New("authority recovery active")
	ErrStaleCapability = errors.New("authority stale capability")
	ErrFailStopped     = errors.New("authority fail-stopped")
	ErrReplayConflict  = errors.New("authority replay conflict")
	ErrRequestExpired  = errors.New("authority request expired")
	ErrNotFound        = errors.New("authority record not found")
	ErrRecoveryNeeded  = errors.New("authority recovery needed")
	ErrFailStopRecord  = errors.New("authority fail-stop record failed")
	ErrRootSealed      = errors.New("authority root permanently sealed")
)

type FailStoppedError struct {
	Reason string
}

func (e FailStoppedError) Error() string {
	if strings.TrimSpace(e.Reason) == "" {
		return ErrFailStopped.Error()
	}
	return fmt.Sprintf("%s: %s", ErrFailStopped, e.Reason)
}

func (e FailStoppedError) Is(target error) bool {
	return target == ErrFailStopped
}

const safetySchemaVersion = uint16(1)

type bootPhase uint8

const (
	bootNone bootPhase = iota
	bootReconciling
	bootReady
	bootFailStopped
)

type authorityCore struct {
	mu       syncMutex
	repo     repository.Repository
	anchor   Anchor
	runtime  *runtimeRegistry
	verifier custodian.AttestationVerifier
	boot     bootStatus
	latch    safetyLatch
}

type syncMutex interface {
	Lock()
	Unlock()
}

type bootStatus struct {
	ref        model.BootRef
	phase      bootPhase
	token      string
	generation uint64
	reason     string
}

type readyCapability struct {
	boot       model.BootRef
	token      string
	generation uint64
}

type recoveryCapability struct {
	boot       model.BootRef
	token      string
	generation uint64
}

type Ready struct {
	core  *authorityCore
	token readyCapability
}

type AcceptRequest struct {
	RequestKey         model.RequestKey
	WorkspaceLayoutKey model.WorkspaceKey
	TaskIdentity       model.TaskIdentity
	Mode               model.Mode
	SessionID          string
	Timeout            *engine.TimeoutResolution
}

type AcceptResult struct {
	Binding    model.Binding
	Record     model.SafetyRecord
	Projection model.JobProjection
	Commit     repository.Commit
	Replayed   bool
}

func ValidateSessionID(sessionID string) error {
	return model.ProjectionMetadata{SessionID: sessionID}.Validate()
}

type ApplyResult struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
	Commit     repository.Commit
	Durability DurabilityOutcome
	Changed    bool
}

type ApplyOption func(*applyConfig)

type applyConfig struct {
	sessionID          string
	afterAnchorForTest func() error
}

func WithApplySessionID(sessionID string) ApplyOption {
	return func(config *applyConfig) {
		config.sessionID = sessionID
	}
}

func withApplyAfterAnchorForTest(fn func() error) ApplyOption {
	return func(config *applyConfig) {
		config.afterAnchorForTest = fn
	}
}

func (r *Ready) Boot() model.BootRef {
	if r == nil {
		return model.BootRef{}
	}
	return r.token.boot
}

func (r *Ready) Generation() uint64 {
	if r == nil {
		return 0
	}
	return r.token.generation
}

func (r *Ready) Accept(ctx context.Context, request AcceptRequest) (AcceptResult, error) {
	return r.accept(ctx, request, nil)
}

func (r *Ready) AcceptAndClaim(ctx context.Context, request AcceptRequest, owner model.OwnerID) (AcceptResult, error) {
	if r == nil || r.core == nil {
		return AcceptResult{}, ErrNotReady
	}
	if err := owner.Validate(); err != nil {
		return AcceptResult{}, fmt.Errorf("%w: owner_id: %v", ErrInvalidRequest, err)
	}
	return r.accept(ctx, request, &owner)
}

func (r *Ready) accept(ctx context.Context, request AcceptRequest, claimOwner *model.OwnerID) (AcceptResult, error) {
	if r == nil || r.core == nil {
		return AcceptResult{}, ErrNotReady
	}
	request, err := normalizeAcceptRequest(request)
	if err != nil {
		return AcceptResult{}, err
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var result AcceptResult
	commit, err := r.core.update(ctx, "accept", func(tx repository.WriteTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		accepted, err := acceptTx(tx, request, r.token.boot)
		if err != nil {
			return err
		}
		result = accepted
		return nil
	})
	if err != nil {
		result.Commit = commit
		if ClassifyDurableMutationOutcome(classifyRepositoryCommitError(err), false) == DefinitelyNotCommitted {
			return AcceptResult{}, err
		}
		stopErr := r.core.failStopLocked(ctx, fmt.Sprintf("accept durable outcome unknown: %v", err))
		return result, postDurableFailStopError("accept durable outcome unknown", err, stopErr)
	}
	result.Commit = commit
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		return result, err
	}
	if result.Replayed {
		return result, nil
	}
	if claimOwner != nil {
		if err := r.core.runtime.registerAndClaimPending(result.Record.Attempt.Ref, *claimOwner); err != nil {
			stopErr := r.core.failStopLocked(ctx, fmt.Sprintf("claim accepted attempt %s: %v", result.Record.JobID, err))
			return result, postDurableFailStopError("claim accepted attempt", err, stopErr)
		}
		return result, nil
	}
	if err := r.core.runtime.registerPending(result.Record.Attempt.Ref); err != nil {
		stopErr := r.core.failStopLocked(ctx, fmt.Sprintf("register pending: %v", err))
		return result, postDurableFailStopError("register pending", err, stopErr)
	}
	return result, nil
}

func (r *Ready) BindGroup(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, group model.GroupRef, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.BindGroup{Ref: ref, Ordinal: ordinal, Group: group}, options...)
}

func (r *Ready) Acknowledge(ctx context.Context, jobID model.JobID, ref model.AttemptRef, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.Acknowledge{Ref: ref}, options...)
}

func (r *Ready) BeginReject(ctx context.Context, jobID model.JobID, ref model.AttemptRef, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.BeginReject{Ref: ref}, options...)
}

func (r *Ready) CommitGrant(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, nonce model.PermitNonce, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.CommitGrant{Ref: ref, Ordinal: ordinal, Nonce: nonce}, options...)
}

func (r *Ready) RecordReleaseOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, outcome model.LaunchReleaseOutcome, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordReleaseOutcome{Ref: ref, Ordinal: ordinal, Outcome: outcome}, options...)
}

func (r *Ready) RecordRelease(ctx context.Context, jobID model.JobID, ref model.AttemptRef, ordinal model.LaunchOrdinal, child model.ChildIdentity, observation model.Evidence, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordRelease{Ref: ref, Ordinal: ordinal, Child: child, Observation: observation}, options...)
}

func (r *Ready) RequestCancel(ctx context.Context, jobID model.JobID, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RequestCancel{JobID: jobID}, options...)
}

func (r *Ready) RecordOutcome(ctx context.Context, jobID model.JobID, ref model.AttemptRef, outcome model.Outcome, contract *engine.ContractStamp, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.ObserveOutcome{Ref: ref, Outcome: outcome, Contract: contract}, options...)
}

func (r *Ready) RecordResult(ctx context.Context, jobID model.JobID, ref model.AttemptRef, receipt model.ResultReceipt, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.CertifyResult{Ref: ref, Receipt: receipt}, options...)
}

// RecordFinalAttemptStart durably records the start of the attempt that is
// currently final. A later contract retry replaces this start timestamp.
func (r *Ready) RecordFinalAttemptStart(ctx context.Context, jobID model.JobID, startedAt time.Time, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordFinalAttemptStart{JobID: jobID, StartedAt: startedAt}, options...)
}

// RecordFailure durably preserves the first terminal-failure explanation
// without changing the job's state machine outcome or terminal certificate.
func (r *Ready) RecordFailure(ctx context.Context, jobID model.JobID, class engine.FailureClass, reason string, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordFailure{JobID: jobID, Class: class, Reason: reason}, options...)
}

// RecordTransportFrameDrops durably preserves bounded metadata about backend
// frames discarded by the transport reader without retaining payload bytes.
func (r *Ready) RecordTransportFrameDrops(ctx context.Context, jobID model.JobID, drops engine.TransportFrameDrops, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordTransportFrameDrops{JobID: jobID, Drops: drops}, options...)
}

// RecordCancellation durably preserves the first cancellation explanation
// without changing the job's state machine outcome or terminal certificate.
func (r *Ready) RecordCancellation(ctx context.Context, jobID model.JobID, origin engine.CancellationOrigin, reason string, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.RecordCancellation{JobID: jobID, Origin: origin, Reason: reason}, options...)
}

func (r *Ready) Finalize(ctx context.Context, jobID model.JobID, ref model.AttemptRef, intent model.TerminalIntent, options ...ApplyOption) (ApplyResult, error) {
	return r.apply(ctx, jobID, model.Finalize{Ref: ref, Intent: intent}, options...)
}

func (r *Ready) RecordQuiescence(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence, options ...ApplyOption) (ApplyResult, error) {
	if r == nil || r.core == nil {
		return ApplyResult{Durability: DefinitelyNotCommitted}, ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return ApplyResult{Durability: DefinitelyNotCommitted}, fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
	}
	if err := ordinal.Validate(); err != nil {
		return ApplyResult{Durability: DefinitelyNotCommitted}, fmt.Errorf("%w: launch_ordinal: %v", ErrInvalidRequest, err)
	}
	config := applyConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var result ApplyResult
	terminalCommitted := false
	commit, err := r.core.update(ctx, "record quiescence", func(tx repository.WriteTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		applied, err := applyQuiescenceTx(tx, jobID, ordinal, r.core.verifier, verified, r.token.boot, config.sessionID)
		if err != nil {
			return err
		}
		result = applied
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		result.Durability = ClassifyDurableMutationOutcome(classifyRepositoryCommitError(err), false)
		return result, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		result.Commit = commit
		result.Durability = ClassifyDurableMutationOutcome(DBCommitted, false)
		return result, err
	}
	result.Commit = commit
	result.Durability = ClassifyDurableMutationOutcome(DBCommitted, true)
	if terminalCommitted {
		r.core.runtime.releaseTerminal(jobID)
	}
	if config.afterAnchorForTest != nil {
		return result, config.afterAnchorForTest()
	}
	return result, nil
}

func (r *Ready) apply(ctx context.Context, jobID model.JobID, command model.Command, options ...ApplyOption) (ApplyResult, error) {
	if r == nil || r.core == nil {
		return ApplyResult{Durability: DefinitelyNotCommitted}, ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return ApplyResult{Durability: DefinitelyNotCommitted}, fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
	}
	config := applyConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var result ApplyResult
	terminalCommitted := false
	boundCommand := commandWithBoot(command, r.token.boot)
	commit, err := r.core.update(ctx, "apply command", func(tx repository.WriteTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		applied, err := applyCommandTx(tx, jobID, boundCommand, config.sessionID)
		if err != nil {
			return err
		}
		result = applied
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		result.Durability = ClassifyDurableMutationOutcome(classifyRepositoryCommitError(err), false)
		return result, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		result.Commit = commit
		result.Durability = ClassifyDurableMutationOutcome(DBCommitted, false)
		return result, err
	}
	result.Commit = commit
	result.Durability = ClassifyDurableMutationOutcome(DBCommitted, true)
	if terminalCommitted {
		r.core.runtime.releaseTerminal(jobID)
	}
	if config.afterAnchorForTest != nil {
		return result, config.afterAnchorForTest()
	}
	return result, nil
}

func classifyRepositoryCommitError(err error) DBCommitOutcome {
	if errors.Is(err, repository.ErrAmbiguousCommit) {
		return DBCommitUnknown
	}
	if errors.Is(err, repository.ErrDefinitelyNotCommitted) {
		return DBDefinitelyNotCommitted
	}
	return DBCommitUnknown
}

func (r *Ready) LoadJob(ctx context.Context, jobID model.JobID) (repository.JobImage, error) {
	if r == nil || r.core == nil {
		return repository.JobImage{}, ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return repository.JobImage{}, fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var image repository.JobImage
	if err := r.core.view(ctx, "load job", func(tx repository.ReadTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		image = tx.LoadJob(jobID)
		return nil
	}); err != nil {
		return repository.JobImage{}, err
	}
	return image, nil
}

func normalizeAcceptRequest(request AcceptRequest) (AcceptRequest, error) {
	if err := request.RequestKey.Validate(); err != nil {
		return AcceptRequest{}, fmt.Errorf("%w: request_key: %v", ErrInvalidRequest, err)
	}
	if err := request.TaskIdentity.Validate(); err != nil {
		return AcceptRequest{}, fmt.Errorf("%w: task_identity: %v", ErrInvalidRequest, err)
	}
	if request.Mode == 0 {
		request.Mode = model.ModeIdentifiedFenced
	}
	if err := request.Mode.Validate(); err != nil {
		return AcceptRequest{}, fmt.Errorf("%w: mode: %v", ErrInvalidRequest, err)
	}
	if err := ValidateSessionID(request.SessionID); err != nil {
		return AcceptRequest{}, fmt.Errorf("%w: projection metadata: %v", ErrInvalidRequest, err)
	}
	if request.Timeout != nil && !request.Timeout.Valid() {
		return AcceptRequest{}, fmt.Errorf("%w: timeout is invalid", ErrInvalidRequest)
	}
	if _, err := model.Project(model.SafetyRecord{
		SchemaVersion:      safetySchemaVersion,
		Revision:           1,
		JobID:              model.JobID("job-validation"),
		RequestKey:         request.RequestKey,
		WorkspaceLayoutKey: request.WorkspaceLayoutKey,
		TaskIdentity:       request.TaskIdentity,
		Mode:               request.Mode,
		AdmittedBy:         model.BootRef{BootID: "boot-validation", OwnerID: "owner-validation"},
		Attempt: model.AttemptProof{
			Ref: model.AttemptRef{JobID: "job-validation", AttemptID: "attempt-validation", Epoch: 1},
		},
	}, model.ProjectionMetadata{SessionID: request.SessionID}); err != nil {
		return AcceptRequest{}, fmt.Errorf("%w: projection metadata: %v", ErrInvalidRequest, err)
	}
	return request, nil
}

func acceptTx(tx repository.WriteTx, request AcceptRequest, boot model.BootRef) (AcceptResult, error) {
	replay := tx.LookupRequest(request.RequestKey)
	if err := rejectBadRequestImage(replay); err != nil {
		return AcceptResult{}, err
	}
	if replay.Tombstone.State == repository.RecordValid {
		return AcceptResult{}, fmt.Errorf("%w: request %s", ErrRequestExpired, request.RequestKey)
	}
	if replay.Binding.State == repository.RecordValid {
		return replayAcceptance(tx, replay.Binding.Value, request)
	}

	jobID, err := tx.AllocateJobID()
	if err != nil {
		return AcceptResult{}, err
	}
	attemptID := model.AttemptID("attempt-" + jobID.String())
	if err := attemptID.Validate(); err != nil {
		return AcceptResult{}, fmt.Errorf("%w: allocated attempt_id: %v", ErrInvalidRequest, err)
	}
	record := model.SafetyRecord{
		SchemaVersion:      safetySchemaVersion,
		Revision:           1,
		JobID:              jobID,
		RequestKey:         request.RequestKey,
		WorkspaceLayoutKey: request.WorkspaceLayoutKey,
		TaskIdentity:       request.TaskIdentity,
		Mode:               request.Mode,
		Timeout:            engine.CloneTimeoutResolution(request.Timeout),
		AdmittedBy:         boot,
		Attempt: model.AttemptProof{
			Ref: model.AttemptRef{JobID: jobID, AttemptID: attemptID, Epoch: 1},
		},
	}
	if err := model.ValidateSafetyRecord(record); err != nil {
		return AcceptResult{}, fmt.Errorf("%w: initial safety: %v", ErrInvalidRequest, err)
	}
	binding := model.Binding{
		RequestKey:   request.RequestKey,
		JobID:        jobID,
		TaskIdentity: request.TaskIdentity,
		Mode:         request.Mode,
	}
	if err := binding.Matches(record); err != nil {
		return AcceptResult{}, fmt.Errorf("%w: binding: %v", ErrInvalidRequest, err)
	}
	projection, err := model.Project(record, model.ProjectionMetadata{SessionID: request.SessionID})
	if err != nil {
		return AcceptResult{}, err
	}
	if err := tx.PutBinding(binding); err != nil {
		return AcceptResult{}, err
	}
	if err := tx.PutSafety(record, 0); err != nil {
		return AcceptResult{}, err
	}
	if err := tx.PutProjection(projection); err != nil {
		return AcceptResult{}, err
	}
	return AcceptResult{
		Binding:    binding,
		Record:     record,
		Projection: projection,
	}, nil
}

func replayAcceptance(tx repository.ReadTx, binding model.Binding, request AcceptRequest) (AcceptResult, error) {
	if !binding.TaskIdentity.Equal(request.TaskIdentity) || binding.Mode != request.Mode {
		return AcceptResult{}, fmt.Errorf("%w: request %s already bound to different task", ErrReplayConflict, request.RequestKey)
	}
	image := tx.LoadJob(binding.JobID)
	record, projection, err := validJobImage(image)
	if err != nil {
		return AcceptResult{}, err
	}
	if err := binding.Matches(record); err != nil {
		return AcceptResult{}, fmt.Errorf("%w: binding mismatch: %v", repository.ErrInvalidRecord, err)
	}
	return AcceptResult{
		Binding:    binding,
		Record:     record,
		Projection: projection,
		Replayed:   true,
	}, nil
}

func applyCommandTx(tx repository.WriteTx, jobID model.JobID, command model.Command, sessionID string) (ApplyResult, error) {
	image := tx.LoadJob(jobID)
	record, projection, err := validJobImage(image)
	if err != nil {
		return ApplyResult{}, err
	}
	if sessionID == "" {
		sessionID = projection.SessionID
	}
	applied, err := applyLogicalCommand(record, command)
	if err != nil {
		return ApplyResult{}, err
	}
	if !applied.Changed {
		return ApplyResult{Record: record, Projection: projection}, nil
	}
	nextProjection, err := model.Project(applied.Record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutSafety(applied.Record, record.Revision); err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutProjection(nextProjection); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Record: applied.Record, Projection: nextProjection, Changed: true}, nil
}

func applyQuiescenceTx(tx repository.WriteTx, jobID model.JobID, ordinal model.LaunchOrdinal, verifier custodian.AttestationVerifier, verified custodian.VerifiedQuiescence, boot model.BootRef, sessionID string) (ApplyResult, error) {
	image := tx.LoadJob(jobID)
	record, projection, err := validJobImage(image)
	if err != nil {
		return ApplyResult{}, err
	}
	if sessionID == "" {
		sessionID = projection.SessionID
	}
	certificate, err := verifyQuiescenceForRecord(record, ordinal, verifier, verified, boot)
	if err != nil {
		return ApplyResult{}, err
	}
	applied, err := applyVerifiedQuiescence(record, certificate)
	if err != nil {
		return ApplyResult{}, err
	}
	if !applied.Changed {
		return ApplyResult{Record: record, Projection: projection}, nil
	}
	nextProjection, err := model.Project(applied.Record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutSafety(applied.Record, record.Revision); err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutProjection(nextProjection); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Record: applied.Record, Projection: nextProjection, Changed: true}, nil
}

func applyCommandToLoadedTx(tx repository.WriteTx, record model.SafetyRecord, projection model.JobProjection, command model.Command, sessionID string) (ApplyResult, error) {
	applied, err := applyLogicalCommand(record, command)
	if err != nil {
		return ApplyResult{}, err
	}
	if !applied.Changed {
		return ApplyResult{Record: record, Projection: projection}, nil
	}
	nextProjection, err := model.Project(applied.Record, model.ProjectionMetadata{SessionID: sessionID})
	if err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutSafety(applied.Record, record.Revision); err != nil {
		return ApplyResult{}, err
	}
	if err := tx.PutProjection(nextProjection); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Record: applied.Record, Projection: nextProjection, Changed: true}, nil
}

func verifyQuiescenceForRecord(record model.SafetyRecord, ordinal model.LaunchOrdinal, verifier custodian.AttestationVerifier, verified custodian.VerifiedQuiescence, boot model.BootRef) (model.QuiescenceCertificate, error) {
	launch, ok := record.Attempt.Launches.Get(ordinal)
	if !ok || launch.Group == nil {
		return model.QuiescenceCertificate{}, fmt.Errorf("%w: durable group reference missing for ordinal %s", ErrInvalidRequest, ordinal)
	}
	physical, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		return model.QuiescenceCertificate{}, err
	}
	if !physical.Group.Equal(*launch.Group) {
		return model.QuiescenceCertificate{}, fmt.Errorf("%w: quiescence group mismatch", ErrInvalidRequest)
	}
	certificate := model.QuiescenceCertificate{
		Attempt:     record.Attempt.Ref,
		Ordinal:     ordinal,
		Group:       physical.Group,
		Method:      physical.Method,
		CertifiedBy: boot,
	}
	return certificate, nil
}

func applyVerifiedQuiescence(record model.SafetyRecord, certificate model.QuiescenceCertificate) (model.ApplyResult, error) {
	if err := model.ValidateSafetyRecord(record); err != nil {
		return model.ApplyResult{}, fmt.Errorf("%w: current safety record is invalid: %v", model.ErrInvalidCommand, err)
	}
	if err := certificate.Validate(); err != nil {
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence receipt: %v", model.ErrInvalidCommand, err)
	}
	if !certificate.Attempt.Equal(record.Attempt.Ref) {
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence attempt mismatch", model.ErrInvalidCommand)
	}
	launch, ok := record.Attempt.Launches.Get(certificate.Ordinal)
	if !ok || launch.Group == nil {
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence requires durable group reference", model.ErrCommandPrecondition)
	}
	if !certificate.Group.Equal(*launch.Group) {
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence group does not match durable group", model.ErrConflictingDuplicate)
	}
	if launch.Quiescence != nil {
		if *launch.Quiescence == certificate {
			return model.ApplyResult{Record: record, Changed: false}, nil
		}
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence already recorded with different evidence", model.ErrConflictingDuplicate)
	}
	if record.Revision == ^uint64(0) {
		return model.ApplyResult{}, fmt.Errorf("%w: safety record revision overflow", model.ErrCommandPrecondition)
	}

	next := record
	next.Attempt.Launches = cloneLaunchSlotsForAuthority(record.Attempt.Launches)
	nextLaunch, ok := next.Attempt.Launches.Get(certificate.Ordinal)
	if !ok || nextLaunch.Group == nil {
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence requires durable group reference", model.ErrCommandPrecondition)
	}
	receipt := certificate
	nextLaunch.Quiescence = &receipt
	next.Revision = record.Revision + 1
	if err := model.ValidateSafetyRecord(next); err != nil {
		return model.ApplyResult{}, fmt.Errorf("reducer produced invalid safety record: %w", err)
	}
	return model.ApplyResult{Record: next, Changed: true}, nil
}

func cloneLaunchSlotsForAuthority(slots model.LaunchSlots[model.LaunchProof]) model.LaunchSlots[model.LaunchProof] {
	return model.LaunchSlots[model.LaunchProof]{
		First:  cloneLaunchProofForAuthority(slots.First),
		Second: cloneLaunchProofForAuthority(slots.Second),
	}
}

func cloneLaunchProofForAuthority(launch *model.LaunchProof) *model.LaunchProof {
	if launch == nil {
		return nil
	}
	copied := *launch
	copied.Group = clonePtrForAuthority(launch.Group)
	copied.Grant = clonePtrForAuthority(launch.Grant)
	copied.Released = clonePtrForAuthority(launch.Released)
	copied.Quiescence = clonePtrForAuthority(launch.Quiescence)
	return &copied
}

func clonePtrForAuthority[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func applyLogicalCommand(record model.SafetyRecord, command model.Command) (model.ApplyResult, error) {
	switch c := command.(type) {
	case model.Acknowledge:
		return model.ApplyAcknowledge(record, c)
	case *model.Acknowledge:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyAcknowledge(record, *c)
	case model.BeginReject:
		return model.ApplyBeginReject(record, c)
	case *model.BeginReject:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyBeginReject(record, *c)
	case model.BindGroup:
		return model.ApplyBindGroup(record, c)
	case *model.BindGroup:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyBindGroup(record, *c)
	case model.CommitGrant:
		return model.ApplyCommitGrant(record, c)
	case *model.CommitGrant:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyCommitGrant(record, *c)
	case model.RecordReleaseOutcome:
		return model.ApplyRecordReleaseOutcome(record, c)
	case *model.RecordReleaseOutcome:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordReleaseOutcome(record, *c)
	case model.RecordRelease:
		return model.ApplyRecordRelease(record, c)
	case *model.RecordRelease:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordRelease(record, *c)
	case model.RequestCancel:
		return model.ApplyRequestCancel(record, c)
	case *model.RequestCancel:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRequestCancel(record, *c)
	case model.ObserveOutcome:
		return model.ApplyObserveOutcome(record, c)
	case *model.ObserveOutcome:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyObserveOutcome(record, *c)
	case model.CertifyResult:
		return model.ApplyCertifyResult(record, c)
	case *model.CertifyResult:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyCertifyResult(record, *c)
	case model.RecordFinalAttemptStart:
		return model.ApplyRecordFinalAttemptStart(record, c)
	case *model.RecordFinalAttemptStart:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordFinalAttemptStart(record, *c)
	case model.RecordFailure:
		return model.ApplyRecordFailure(record, c)
	case *model.RecordFailure:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordFailure(record, *c)
	case model.RecordTransportFrameDrops:
		return model.ApplyRecordTransportFrameDrops(record, c)
	case *model.RecordTransportFrameDrops:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordTransportFrameDrops(record, *c)
	case model.RecordCancellation:
		return model.ApplyRecordCancellation(record, c)
	case *model.RecordCancellation:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyRecordCancellation(record, *c)
	case model.Finalize:
		return model.ApplyFinalize(record, c)
	case *model.Finalize:
		if c == nil {
			return model.ApplyResult{}, fmt.Errorf("%w: command is nil", model.ErrInvalidCommand)
		}
		return model.ApplyFinalize(record, *c)
	case model.RecordQuiescence, *model.RecordQuiescence:
		return model.ApplyResult{}, fmt.Errorf("%w: quiescence must be recorded through verified authority ingress", ErrInvalidRequest)
	default:
		return model.ApplyResult{}, fmt.Errorf("%w: unsupported command %T", model.ErrInvalidCommand, command)
	}
}

func validJobImage(image repository.JobImage) (model.SafetyRecord, model.JobProjection, error) {
	if err := requireRecord("binding", image.Binding.State, image.Binding.Diagnostic); err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, err
	}
	if err := requireRecord("safety", image.Safety.State, image.Safety.Diagnostic); err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, err
	}
	if err := requireRecord("projection", image.Projection.State, image.Projection.Diagnostic); err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, err
	}
	record := image.Safety.Value
	projection := image.Projection.Value
	if err := image.Binding.Value.Matches(record); err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, fmt.Errorf("%w: binding: %v", repository.ErrInvalidRecord, err)
	}
	expected, err := model.Project(record, model.ProjectionMetadata{SessionID: projection.SessionID})
	if err != nil {
		return model.SafetyRecord{}, model.JobProjection{}, fmt.Errorf("%w: project safety %s: %v", repository.ErrInvalidRecord, record.JobID, err)
	}
	if !reflect.DeepEqual(projection, expected) {
		return model.SafetyRecord{}, model.JobProjection{}, fmt.Errorf("%w: projection %s does not match safety revision %d", repository.ErrProjectionMismatch, record.JobID, record.Revision)
	}
	return record, projection, nil
}

func rejectBadRequestImage(image repository.RequestImage) error {
	if err := rejectRequestRecord("binding", image.Binding.State, image.Binding.Diagnostic); err != nil {
		return err
	}
	return rejectRequestRecord("tombstone", image.Tombstone.State, image.Tombstone.Diagnostic)
}

func rejectRequestRecord(kind string, state repository.RecordState, diagnostic string) error {
	if state == repository.RecordCorrupt {
		return repository.CorruptRecordError(kind, "", diagnostic)
	}
	return nil
}

func requireRecord(kind string, state repository.RecordState, diagnostic string) error {
	switch state {
	case repository.RecordValid:
		return nil
	case repository.RecordCorrupt:
		return repository.CorruptRecordError(kind, "", diagnostic)
	case repository.RecordMissing:
		return fmt.Errorf("%w: %s is missing", ErrNotFound, kind)
	default:
		return fmt.Errorf("%w: %s has unknown state", repository.ErrInvalidRecord, kind)
	}
}

func (core *authorityCore) requireMeta(tx repository.ReadTx) (repository.AuthorityMeta, error) {
	meta := tx.Meta()
	switch meta.State {
	case repository.RecordValid:
		if err := meta.Value.Validate(); err != nil {
			return repository.AuthorityMeta{}, err
		}
		return meta.Value, nil
	case repository.RecordCorrupt:
		diagnostic := meta.Diagnostic
		if diagnostic == "" {
			diagnostic = "corrupt"
		}
		return repository.AuthorityMeta{}, fmt.Errorf("%w: meta: %s", repository.ErrCorruptRecord, diagnostic)
	case repository.RecordMissing:
		return repository.AuthorityMeta{}, fmt.Errorf("%w: meta is missing", repository.ErrInvalidRecord)
	default:
		return repository.AuthorityMeta{}, fmt.Errorf("%w: meta has unknown state", repository.ErrInvalidRecord)
	}
}

func (core *authorityCore) requireReadyTx(tx repository.ReadTx, token readyCapability) (repository.AuthorityMeta, error) {
	meta, err := core.requireMeta(tx)
	if err != nil {
		return repository.AuthorityMeta{}, err
	}
	if meta.Sealed {
		return repository.AuthorityMeta{}, ErrRootSealed
	}
	if core.boot.phase == bootFailStopped {
		return repository.AuthorityMeta{}, ErrFailStopped
	}
	if core.boot.phase != bootReady {
		return repository.AuthorityMeta{}, ErrNotReady
	}
	if token.token == "" || core.boot.token != token.token || core.boot.ref != token.boot {
		return repository.AuthorityMeta{}, ErrStaleCapability
	}
	if meta.Generation != token.generation || core.boot.generation != token.generation {
		return repository.AuthorityMeta{}, ErrStaleCapability
	}
	if verifier, ok := core.anchor.(anchorVerifier); ok {
		if err := verifier.VerifyReady(token.boot, token.token, token.generation); err != nil {
			return repository.AuthorityMeta{}, err
		}
	}
	return meta, nil
}

func (core *authorityCore) requireRecoveryTx(tx repository.ReadTx, token recoveryCapability) (repository.AuthorityMeta, error) {
	meta, err := core.requireMeta(tx)
	if err != nil {
		return repository.AuthorityMeta{}, err
	}
	if meta.Sealed {
		return repository.AuthorityMeta{}, ErrRootSealed
	}
	if core.boot.phase == bootFailStopped {
		return repository.AuthorityMeta{}, ErrFailStopped
	}
	if core.boot.phase != bootReconciling {
		return repository.AuthorityMeta{}, ErrRecoveryActive
	}
	if token.token == "" || core.boot.token != token.token || core.boot.ref != token.boot {
		return repository.AuthorityMeta{}, ErrStaleCapability
	}
	if meta.Generation != token.generation || core.boot.generation != token.generation {
		return repository.AuthorityMeta{}, ErrStaleCapability
	}
	if verifier, ok := core.anchor.(anchorVerifier); ok {
		if err := verifier.VerifyRecovery(token.boot, token.token, token.generation); err != nil {
			return repository.AuthorityMeta{}, err
		}
	}
	return meta, nil
}

func (core *authorityCore) advanceReadyLocked(ctx context.Context, token *readyCapability, generation uint64) error {
	if err := core.anchor.Advance(ctx, token.boot, generation); err != nil {
		stopErr := core.failStopLocked(ctx, fmt.Sprintf("anchor advance: %v", err))
		return postDurableFailStopError("anchor advance", err, stopErr)
	}
	token.generation = generation
	core.boot.generation = generation
	return nil
}

func (core *authorityCore) advanceRecoveryLocked(ctx context.Context, token *recoveryCapability, generation uint64) error {
	if err := core.anchor.Advance(ctx, token.boot, generation); err != nil {
		stopErr := core.failStopLocked(ctx, fmt.Sprintf("anchor advance: %v", err))
		return postDurableFailStopError("anchor advance", err, stopErr)
	}
	token.generation = generation
	core.boot.generation = generation
	return nil
}

func commandWithBoot(command model.Command, boot model.BootRef) model.Command {
	switch c := command.(type) {
	case model.Acknowledge:
		if emptyBootRef(c.AcknowledgedBy) {
			c.AcknowledgedBy = boot
		}
		return c
	case *model.Acknowledge:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.AcknowledgedBy) {
			next.AcknowledgedBy = boot
		}
		return next
	case model.BeginReject:
		if emptyBootRef(c.RequestedBy) {
			c.RequestedBy = boot
		}
		return c
	case *model.BeginReject:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.RequestedBy) {
			next.RequestedBy = boot
		}
		return next
	case model.CommitGrant:
		if emptyBootRef(c.GrantedBy) {
			c.GrantedBy = boot
		}
		return c
	case *model.CommitGrant:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.GrantedBy) {
			next.GrantedBy = boot
		}
		return next
	case model.RecordRelease:
		if emptyBootRef(c.ReleasedBy) {
			c.ReleasedBy = boot
		}
		return c
	case *model.RecordRelease:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.ReleasedBy) {
			next.ReleasedBy = boot
		}
		return next
	case model.RecordQuiescence:
		if emptyBootRef(c.Receipt.CertifiedBy) {
			c.Receipt.CertifiedBy = boot
		}
		return c
	case *model.RecordQuiescence:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.Receipt.CertifiedBy) {
			next.Receipt.CertifiedBy = boot
		}
		return next
	case model.RequestCancel:
		if emptyBootRef(c.RequestedBy) {
			c.RequestedBy = boot
		}
		return c
	case *model.RequestCancel:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.RequestedBy) {
			next.RequestedBy = boot
		}
		return next
	case model.CertifyResult:
		if emptyBootRef(c.Receipt.CertifiedBy) {
			c.Receipt.CertifiedBy = boot
		}
		return c
	case *model.CertifyResult:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.Receipt.CertifiedBy) {
			next.Receipt.CertifiedBy = boot
		}
		return next
	case model.RecordFinalAttemptStart, *model.RecordFinalAttemptStart,
		model.RecordFailure, *model.RecordFailure,
		model.RecordTransportFrameDrops, *model.RecordTransportFrameDrops,
		model.RecordCancellation, *model.RecordCancellation:
		return command
	case model.Finalize:
		if emptyBootRef(c.Intent.DerivedBy) {
			c.Intent.DerivedBy = boot
		}
		return c
	case *model.Finalize:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.Intent.DerivedBy) {
			next.Intent.DerivedBy = boot
		}
		return next
	default:
		return command
	}
}

func emptyBootRef(ref model.BootRef) bool {
	return ref.BootID == "" && ref.OwnerID == ""
}

func commandJobID(command model.Command) (model.JobID, error) {
	switch c := command.(type) {
	case model.Acknowledge:
		return c.Ref.JobID, nil
	case *model.Acknowledge:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.BeginReject:
		return c.Ref.JobID, nil
	case *model.BeginReject:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.BindGroup:
		return c.Ref.JobID, nil
	case *model.BindGroup:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.CommitGrant:
		return c.Ref.JobID, nil
	case *model.CommitGrant:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.RecordRelease:
		return c.Ref.JobID, nil
	case *model.RecordRelease:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.RecordQuiescence:
		return c.Ref.JobID, nil
	case *model.RecordQuiescence:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.RequestCancel:
		return c.JobID, nil
	case *model.RequestCancel:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.JobID, nil
	case model.ObserveOutcome:
		return c.Ref.JobID, nil
	case *model.ObserveOutcome:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.CertifyResult:
		return c.Receipt.JobID, nil
	case *model.CertifyResult:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Receipt.JobID, nil
	case model.RecordFinalAttemptStart:
		return c.JobID, nil
	case *model.RecordFinalAttemptStart:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.JobID, nil
	case model.RecordFailure:
		return c.JobID, nil
	case *model.RecordFailure:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.JobID, nil
	case model.RecordTransportFrameDrops:
		return c.JobID, nil
	case *model.RecordTransportFrameDrops:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.JobID, nil
	case model.RecordCancellation:
		return c.JobID, nil
	case *model.RecordCancellation:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.JobID, nil
	case model.Finalize:
		return c.Ref.JobID, nil
	case *model.Finalize:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	default:
		return "", fmt.Errorf("%w: unsupported command %T", ErrInvalidRequest, command)
	}
}
