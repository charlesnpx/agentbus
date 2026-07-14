package authority

import (
	"context"
	"errors"
	"fmt"
	"reflect"

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
)

const safetySchemaVersion = uint16(1)

type bootPhase uint8

const (
	bootNone bootPhase = iota
	bootReconciling
	bootReady
	bootFailStopped
)

type authorityCore struct {
	mu      syncMutex
	repo    repository.Repository
	anchor  Anchor
	runtime *runtimeRegistry
	boot    bootStatus
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
	RequestKey   model.RequestKey
	TaskIdentity model.TaskIdentity
	Mode         model.Mode
	SessionID    string
}

type AcceptResult struct {
	Binding    model.Binding
	Record     model.SafetyRecord
	Projection model.JobProjection
	Commit     repository.Commit
	Replayed   bool
}

type ApplyResult struct {
	Record     model.SafetyRecord
	Projection model.JobProjection
	Commit     repository.Commit
	Changed    bool
}

type ApplyOption func(*applyConfig)

type applyConfig struct {
	sessionID string
}

func WithApplySessionID(sessionID string) ApplyOption {
	return func(config *applyConfig) {
		config.sessionID = sessionID
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
	commit, err := r.core.repo.Update(ctx, func(tx repository.WriteTx) error {
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
		return AcceptResult{}, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		return AcceptResult{}, err
	}
	result.Commit = commit
	if !result.Replayed {
		if err := r.core.runtime.registerPending(result.Record.Attempt.Ref); err != nil {
			r.core.failStopLocked(ctx, fmt.Sprintf("register pending: %v", err))
			return result, err
		}
	}
	return result, nil
}

func (r *Ready) Apply(ctx context.Context, jobID model.JobID, command model.Command, options ...ApplyOption) (ApplyResult, error) {
	if r == nil || r.core == nil {
		return ApplyResult{}, ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return ApplyResult{}, fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
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
	commit, err := r.core.repo.Update(ctx, func(tx repository.WriteTx) error {
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
		return ApplyResult{}, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		return ApplyResult{}, err
	}
	result.Commit = commit
	if terminalCommitted {
		r.core.runtime.releaseTerminal(jobID)
	}
	return result, nil
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
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
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
	if request.Mode != model.ModeIdentifiedFenced {
		return AcceptRequest{}, fmt.Errorf("%w: mode %s is outside authority acceptance", ErrInvalidRequest, request.Mode)
	}
	if _, err := model.Project(model.SafetyRecord{
		SchemaVersion: safetySchemaVersion,
		Revision:      1,
		JobID:         model.JobID("job-validation"),
		RequestKey:    request.RequestKey,
		TaskIdentity:  request.TaskIdentity,
		Mode:          request.Mode,
		AdmittedBy:    model.BootRef{BootID: "boot-validation", OwnerID: "owner-validation"},
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
		SchemaVersion: safetySchemaVersion,
		Revision:      1,
		JobID:         jobID,
		RequestKey:    request.RequestKey,
		TaskIdentity:  request.TaskIdentity,
		Mode:          request.Mode,
		AdmittedBy:    boot,
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
	applied, err := model.Apply(record, command)
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
		if diagnostic == "" {
			diagnostic = "corrupt"
		}
		return fmt.Errorf("%w: %s: %s", repository.ErrCorruptRecord, kind, diagnostic)
	}
	return nil
}

func requireRecord(kind string, state repository.RecordState, diagnostic string) error {
	switch state {
	case repository.RecordValid:
		return nil
	case repository.RecordCorrupt:
		if diagnostic == "" {
			diagnostic = "corrupt"
		}
		return fmt.Errorf("%w: %s: %s", repository.ErrCorruptRecord, kind, diagnostic)
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
		core.failStopLocked(ctx, fmt.Sprintf("anchor advance: %v", err))
		return err
	}
	token.generation = generation
	core.boot.generation = generation
	return nil
}

func (core *authorityCore) advanceRecoveryLocked(ctx context.Context, token *recoveryCapability, generation uint64) error {
	if err := core.anchor.Advance(ctx, token.boot, generation); err != nil {
		core.failStopLocked(ctx, fmt.Sprintf("anchor advance: %v", err))
		return err
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
	case model.AuthorizeLaunch:
		if emptyBootRef(c.GrantedBy) {
			c.GrantedBy = boot
		}
		return c
	case *model.AuthorizeLaunch:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.GrantedBy) {
			next.GrantedBy = boot
		}
		return next
	case model.ObserveLaunchConsumed:
		if emptyBootRef(c.ConsumedBy) {
			c.ConsumedBy = boot
		}
		return c
	case *model.ObserveLaunchConsumed:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.ConsumedBy) {
			next.ConsumedBy = boot
		}
		return next
	case model.ObserveLaunchQuiescent:
		if emptyBootRef(c.Receipt.CertifiedBy) {
			c.Receipt.CertifiedBy = boot
		}
		return c
	case *model.ObserveLaunchQuiescent:
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
	case model.CertifyRetirement:
		if emptyBootRef(c.Receipt.CertifiedBy) {
			c.Receipt.CertifiedBy = boot
		}
		return c
	case *model.CertifyRetirement:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.Receipt.CertifiedBy) {
			next.Receipt.CertifiedBy = boot
		}
		return next
	case model.CertifyContainment:
		if emptyBootRef(c.Receipt.CertifiedBy) {
			c.Receipt.CertifiedBy = boot
		}
		return c
	case *model.CertifyContainment:
		if c == nil {
			return c
		}
		next := *c
		if emptyBootRef(next.Receipt.CertifiedBy) {
			next.Receipt.CertifiedBy = boot
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
	case model.BindSupervisor:
		return c.Ref.JobID, nil
	case *model.BindSupervisor:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.AuthorizeLaunch:
		return c.Ref.JobID, nil
	case *model.AuthorizeLaunch:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.ObserveLaunchConsumed:
		return c.Ref.JobID, nil
	case *model.ObserveLaunchConsumed:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.ObserveLaunchQuiescent:
		return c.Ref.JobID, nil
	case *model.ObserveLaunchQuiescent:
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
	case model.CertifyRetirement:
		return c.Ref.JobID, nil
	case *model.CertifyRetirement:
		if c == nil {
			return "", fmt.Errorf("%w: nil command", ErrInvalidRequest)
		}
		return c.Ref.JobID, nil
	case model.CertifyContainment:
		return c.Ref.JobID, nil
	case *model.CertifyContainment:
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
