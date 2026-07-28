package authority

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

var ErrBootActive = errors.New("authority boot already active")

type Anchor interface {
	Begin(context.Context, model.BootRef, uint64) (string, error)
	SealReady(context.Context, model.BootRef, uint64) (string, error)
	Advance(context.Context, model.BootRef, uint64) error
	FailStop(context.Context, model.BootRef, string) error
}

type anchorVerifier interface {
	VerifyReady(model.BootRef, string, uint64) error
	VerifyRecovery(model.BootRef, string, uint64) error
}

type beginStartabilityAnchor interface {
	probeBeginStartable(context.Context, uint64) (AnchorSnapshot, error)
}

type beginStartabilityProbe struct {
	Generation uint64
	Anchor     AnchorSnapshot
	HasAnchor  bool
}

type beginStartabilityError struct {
	Barrier string
	Cause   error
}

func (e beginStartabilityError) Error() string {
	return fmt.Sprintf("%s: %v", e.Barrier, e.Cause)
}

func (e beginStartabilityError) Unwrap() error {
	return e.Cause
}

type Bootstrapper struct {
	core *authorityCore
}

type RecoverySession struct {
	core   *authorityCore
	token  recoveryCapability
	tokens map[string]issuedRecoveryToken
}

type BootstrapperOption func(*bootstrapConfig)

type safetyLatch interface {
	Trip(error)
}

type bootstrapConfig struct {
	anchor                Anchor
	anchorStore           *AnchorStore
	quiescenceVerifier    custodian.AttestationVerifier
	hasQuiescenceVerifier bool
	safetyLatch           safetyLatch
}

func WithAnchor(anchor Anchor) BootstrapperOption {
	return func(config *bootstrapConfig) {
		config.anchor = anchor
	}
}

func WithAnchorStore(anchorStore *AnchorStore) BootstrapperOption {
	return func(config *bootstrapConfig) {
		config.anchorStore = anchorStore
	}
}

func WithQuiescenceVerifier(verifier custodian.AttestationVerifier) BootstrapperOption {
	return func(config *bootstrapConfig) {
		config.quiescenceVerifier = verifier
		config.hasQuiescenceVerifier = true
	}
}

func WithSafetyLatch(latch interface{ Trip(error) }) BootstrapperOption {
	return func(config *bootstrapConfig) {
		config.safetyLatch = latch
	}
}

func NewBootstrapper(repo repository.Repository, options ...BootstrapperOption) (*Bootstrapper, error) {
	if repo == nil {
		return nil, errors.New("authority repository is required")
	}
	config := bootstrapConfig{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	anchor := config.anchor
	if anchor == nil {
		dbUUID, schemaMajor, err := repositoryAnchorIdentity(repo)
		if err != nil {
			tripSafetyLatchOnRepositoryCorruption(config.safetyLatch, err)
			return nil, err
		}
		anchorStore := config.anchorStore
		if anchorStore == nil {
			anchorStore = defaultAnchorStoreFor(dbUUID, schemaMajor)
		}
		anchor = anchorStore.Adapter(dbUUID, schemaMajor)
	}
	verifier := config.quiescenceVerifier
	if !config.hasQuiescenceVerifier {
		_, verifier = custodian.NewAttestationChannel()
	}
	return &Bootstrapper{
		core: &authorityCore{
			mu:       &sync.Mutex{},
			repo:     repo,
			anchor:   anchor,
			runtime:  newRuntimeRegistry(),
			verifier: verifier,
			latch:    config.safetyLatch,
		},
	}, nil
}

func (b *Bootstrapper) Begin(ctx context.Context, boot model.BootRef) (*RecoverySession, error) {
	if b == nil || b.core == nil {
		return nil, ErrNotReady
	}
	if err := boot.Validate(); err != nil {
		return nil, fmt.Errorf("%w: boot_ref: %v", ErrInvalidRequest, err)
	}

	b.core.mu.Lock()
	defer b.core.mu.Unlock()
	if b.core.boot.phase != bootNone {
		return nil, ErrBootActive
	}

	probe, err := probeBeginStartability(ctx, b.core.repo, b.core.anchor)
	if err != nil {
		b.core.tripSafetyLatchOnRepositoryCorruption(err)
		return nil, err
	}
	generation := probe.Generation
	token, err := b.core.anchor.Begin(ctx, boot, generation)
	if err != nil {
		return nil, err
	}
	b.core.boot = bootStatus{
		ref:        boot,
		phase:      bootReconciling,
		token:      token,
		generation: generation,
	}
	return &RecoverySession{
		core:   b.core,
		tokens: make(map[string]issuedRecoveryToken),
		token: recoveryCapability{
			boot:       boot,
			token:      token,
			generation: generation,
		},
	}, nil
}

func probeBeginStartability(ctx context.Context, repo repository.Repository, anchor Anchor) (beginStartabilityProbe, error) {
	var probe beginStartabilityProbe
	// probeBeginStartability is the read-only equivalent of Bootstrapper.Begin's startability gate: it accepts exactly the roots Begin can begin, and rejects the same sealed metadata, startup-matrix, anchor identity/generation, and persisted fail-stop barriers without repair.
	if err := repo.View(ctx, func(tx repository.ReadTx) error {
		core := authorityCore{}
		meta, err := core.requireMeta(tx)
		if err != nil {
			return err
		}
		if meta.Sealed {
			return beginStartabilityError{Barrier: "sealed", Cause: ErrRootSealed}
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return beginStartabilityError{Barrier: "matrix-invalid", Cause: err}
		}
		probe.Generation = meta.Generation
		return nil
	}); err != nil {
		return beginStartabilityProbe{}, err
	}
	if anchorProbe, ok := anchor.(beginStartabilityAnchor); ok {
		snapshot, err := anchorProbe.probeBeginStartable(ctx, probe.Generation)
		if err != nil {
			barrier := "anchor"
			if errors.Is(err, ErrFailStopped) {
				barrier = "fail_stopped"
			}
			return beginStartabilityProbe{}, beginStartabilityError{Barrier: barrier, Cause: err}
		}
		probe.Anchor = snapshot
		probe.HasAnchor = true
	}
	return probe, nil
}

func (a anchorAdapter) probeBeginStartable(ctx context.Context, generation uint64) (AnchorSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return AnchorSnapshot{}, err
	}
	if err := validateAnchorIdentity(a.dbUUID, a.schemaMajor); err != nil {
		return AnchorSnapshot{}, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	snapshot := a.store.state
	if !snapshot.Initialized {
		if generation != 0 {
			return AnchorSnapshot{}, fmt.Errorf("%w: missing anchor for initialized db generation %d", ErrAnchorInvariant, generation)
		}
		return AnchorSnapshot{
			Initialized: true,
			DBUUID:      a.dbUUID,
			SchemaMajor: a.schemaMajor,
			Generation:  generation,
		}, nil
	}
	if err := a.store.requireIdentityLocked(a.dbUUID, a.schemaMajor); err != nil {
		return AnchorSnapshot{}, err
	}
	if snapshot.Generation > generation {
		return AnchorSnapshot{}, fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", ErrAnchorInvariant, snapshot.Generation, generation)
	}
	if snapshot.Phase == "fail_stopped" {
		return AnchorSnapshot{}, FailStoppedError{Reason: snapshot.Reason}
	}
	return snapshot, nil
}

func (s *RecoverySession) Plans(ctx context.Context) ([]model.RecoveryPlan, error) {
	if s == nil || s.core == nil {
		return nil, ErrNotReady
	}
	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var plans []model.RecoveryPlan
	if err := s.core.view(ctx, "recovery plans", func(tx repository.ReadTx) error {
		if _, err := s.core.requireRecoveryTx(tx, s.token); err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		jobPlans, err := recoveryPlansTx(tx, s.token.boot, model.RecoveryStartupLoss, true)
		if err != nil {
			return err
		}
		plans = make([]model.RecoveryPlan, 0, len(jobPlans))
		for _, plan := range jobPlans {
			plans = append(plans, plan.Plan)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return plans, nil
}

func (s *RecoverySession) Finalize(ctx context.Context, ref model.AttemptRef, intent model.TerminalIntent) error {
	return s.applyReceipt(ctx, model.Finalize{Ref: ref, Intent: intent})
}

func (s *RecoverySession) RecordQuiescence(ctx context.Context, subject any, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) error {
	switch value := subject.(type) {
	case model.JobID:
		return s.recordQuiescenceByJobID(ctx, value, ordinal, verified)
	case model.RecoveryToken:
		return s.recordQuiescenceByToken(ctx, value, ordinal, verified)
	default:
		return fmt.Errorf("%w: recovery quiescence subject %T", ErrInvalidRequest, subject)
	}
}

func (s *RecoverySession) recordQuiescenceByJobID(ctx context.Context, jobID model.JobID, ordinal model.LaunchOrdinal, verified custodian.VerifiedQuiescence) error {
	if s == nil || s.core == nil {
		return ErrNotReady
	}
	if err := jobID.Validate(); err != nil {
		return fmt.Errorf("%w: job_id: %v", ErrInvalidRequest, err)
	}
	if err := ordinal.Validate(); err != nil {
		return fmt.Errorf("%w: launch_ordinal: %v", ErrInvalidRequest, err)
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	terminalCommitted := false
	commit, err := s.core.update(ctx, "recovery quiescence", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		applied, err := applyRecoveryQuiescenceTx(tx, jobID, ordinal, s.core.verifier, verified, s.token.boot, meta.Generation+1)
		if err != nil {
			return err
		}
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return err
	}
	if terminalCommitted {
		s.core.runtime.releaseTerminal(jobID)
	}
	return nil
}

func (s *RecoverySession) applyReceipt(ctx context.Context, command model.Command) error {
	if s == nil || s.core == nil {
		return ErrNotReady
	}
	jobID, err := commandJobID(command)
	if err != nil {
		return err
	}

	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	boundCommand := commandWithBoot(command, s.token.boot)
	terminalCommitted := false
	commit, err := s.core.update(ctx, "recovery receipt", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		applied, err := applyRecoveryCommandTx(tx, jobID, boundCommand, meta.Generation+1)
		if err != nil {
			return err
		}
		terminalCommitted = applied.Changed && applied.Record.Terminal != nil
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return err
	}
	if terminalCommitted {
		s.core.runtime.releaseTerminal(jobID)
	}
	return nil
}

func (s *RecoverySession) SealReady(ctx context.Context) (*Ready, error) {
	if s == nil || s.core == nil {
		return nil, ErrNotReady
	}
	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var recoveredRuntime *runtimeRegistry
	commit, err := s.core.update(ctx, "seal ready", func(tx repository.WriteTx) error {
		meta, err := s.core.requireRecoveryTx(tx, s.token)
		if err != nil {
			return err
		}
		repairs, err := startupMatrixTx(tx)
		if err != nil {
			return err
		}
		plans, err := recoveryPlansTx(tx, s.token.boot, model.RecoveryStartupLoss, true)
		if err != nil {
			return err
		}
		if len(plans) != 0 {
			return fmt.Errorf("%w: %d prior boot job(s)", ErrRecoveryNeeded, len(plans))
		}
		recoveredRuntime, err = runtimeRegistryForBootTx(tx, s.token.boot)
		if err != nil {
			return err
		}
		if err := repairStartupProjectionsTx(tx, repairs, meta.Generation+1); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := s.core.advanceRecoveryLocked(ctx, &s.token, commit.Generation); err != nil {
		return nil, err
	}
	if recoveredRuntime == nil {
		recoveredRuntime = newRuntimeRegistry()
	}
	token, err := s.core.anchor.SealReady(ctx, s.token.boot, s.token.generation)
	if err != nil {
		stopErr := s.core.failStopLocked(ctx, fmt.Sprintf("anchor seal ready: %v", err))
		return nil, postDurableFailStopError("anchor seal ready", err, stopErr)
	}
	s.core.runtime = recoveredRuntime
	s.core.boot = bootStatus{
		ref:        s.token.boot,
		phase:      bootReady,
		token:      token,
		generation: s.token.generation,
	}
	return &Ready{
		core: s.core,
		token: readyCapability{
			boot:       s.token.boot,
			token:      token,
			generation: s.token.generation,
		},
	}, nil
}

type FakeAnchor struct {
	mu         sync.Mutex
	boot       model.BootRef
	generation uint64
	token      string
	failed     bool
	reason     string
}

var defaultAnchorStores sync.Map

func NewFakeAnchor() *FakeAnchor {
	return &FakeAnchor{}
}

// defaultAnchorStoreFor caches default anchor stores by the repository's
// ANCHOR IDENTITY (db uuid + schema major), never by repository pointer.
// The anchor is a per-database fact: two bootstrappers over the same database
// must share one store, and a freshly allocated repository that happens to
// reuse a freed repository's address must NOT inherit that repository's
// anchor store (a pointer key did exactly that under load, tripping
// "db uuid mismatch" when the recycled store was already bound to the old
// database's uuid).
func defaultAnchorStoreFor(dbUUID string, schemaMajor uint16) *AnchorStore {
	key := fmt.Sprintf("%s#%d", dbUUID, schemaMajor)
	if existing, ok := defaultAnchorStores.Load(key); ok {
		return existing.(*AnchorStore)
	}
	anchorStore := NewAnchorStore()
	actual, _ := defaultAnchorStores.LoadOrStore(key, anchorStore)
	return actual.(*AnchorStore)
}

func defaultAnchorKey(repo repository.Repository) string {
	value := reflect.ValueOf(repo)
	if value.IsValid() && value.Kind() == reflect.Ptr && !value.IsNil() {
		return fmt.Sprintf("%s:%x", value.Type(), value.Pointer())
	}
	return fmt.Sprintf("%T", repo)
}

func repositoryAnchorIdentity(repo repository.Repository) (string, uint16, error) {
	if identified, ok := repo.(repository.AnchorIdentified); ok {
		return identified.AnchorIdentity()
	}
	return defaultAnchorKey(repo), repository.CurrentAuthorityMetaSchemaVersion, nil
}

func (a *FakeAnchor) Begin(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	token := fmt.Sprintf("recovery-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	a.boot = boot
	a.generation = generation
	a.token = token
	a.failed = false
	a.reason = ""
	return token, nil
}

func (a *FakeAnchor) SealReady(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	token := fmt.Sprintf("ready-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	a.boot = boot
	a.generation = generation
	a.token = token
	return token, nil
}

func (a *FakeAnchor) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.boot = boot
	a.generation = generation
	return nil
}

func (a *FakeAnchor) FailStop(ctx context.Context, boot model.BootRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.boot = boot
	a.failed = true
	a.reason = reason
	return nil
}

func (a *FakeAnchor) VerifyReady(boot model.BootRef, token string, generation uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failed {
		return ErrFailStopped
	}
	if a.boot != boot || a.token != token || a.generation != generation {
		return ErrStaleCapability
	}
	return nil
}

func (a *FakeAnchor) VerifyRecovery(boot model.BootRef, token string, generation uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failed {
		return ErrFailStopped
	}
	if a.boot != boot || a.token != token || a.generation != generation {
		return ErrStaleCapability
	}
	return nil
}
