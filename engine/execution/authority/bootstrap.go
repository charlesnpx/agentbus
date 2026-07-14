package authority

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

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

type Bootstrapper struct {
	core *authorityCore
}

type RecoverySession struct {
	core  *authorityCore
	token recoveryCapability
}

type BootstrapperOption func(*bootstrapConfig)

type bootstrapConfig struct {
	anchor      Anchor
	anchorStore *AnchorStore
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
			return nil, err
		}
		anchorStore := config.anchorStore
		if anchorStore == nil {
			anchorStore = defaultAnchorStoreFor(repo)
		}
		anchor = anchorStore.Adapter(dbUUID, schemaMajor)
	}
	return &Bootstrapper{
		core: &authorityCore{
			mu:      &sync.Mutex{},
			repo:    repo,
			anchor:  anchor,
			runtime: newRuntimeRegistry(),
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

	var generation uint64
	if err := b.core.repo.View(ctx, func(tx repository.ReadTx) error {
		meta, err := b.core.requireMeta(tx)
		if err != nil {
			return err
		}
		if _, err := startupMatrixTx(tx); err != nil {
			return err
		}
		generation = meta.Generation
		return nil
	}); err != nil {
		return nil, err
	}
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
		core: b.core,
		token: recoveryCapability{
			boot:       boot,
			token:      token,
			generation: generation,
		},
	}, nil
}

func (s *RecoverySession) Plans(ctx context.Context) ([]model.RecoveryPlan, error) {
	if s == nil || s.core == nil {
		return nil, ErrNotReady
	}
	s.core.mu.Lock()
	defer s.core.mu.Unlock()

	var plans []model.RecoveryPlan
	if err := s.core.repo.View(ctx, func(tx repository.ReadTx) error {
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

func (s *RecoverySession) ApplyReceipt(ctx context.Context, command model.Command) error {
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
	commit, err := s.core.repo.Update(ctx, func(tx repository.WriteTx) error {
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
	commit, err := s.core.repo.Update(ctx, func(tx repository.WriteTx) error {
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
		s.core.failStopLocked(ctx, fmt.Sprintf("anchor seal ready: %v", err))
		return nil, err
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

func defaultAnchorStoreFor(repo repository.Repository) *AnchorStore {
	key := defaultAnchorKey(repo)
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
	if identified, ok := repo.(anchorIdentityRepository); ok {
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
