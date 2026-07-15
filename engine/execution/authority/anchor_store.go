package authority

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

var ErrAnchorInvariant = errors.New("authority anchor invariant violation")

type AnchorOperation string

const (
	AnchorBegin     AnchorOperation = "begin"
	AnchorSealReady AnchorOperation = "seal_ready"
	AnchorAdvance   AnchorOperation = "advance"
	AnchorComplete  AnchorOperation = "complete"
	AnchorFailStop  AnchorOperation = "fail_stop"
)

type AnchorSnapshot struct {
	Initialized bool
	DBUUID      string
	SchemaMajor uint16
	Generation  uint64
	Phase       string
	Boot        model.BootRef
	Token       string
	Reason      string
}

type AnchorStore struct {
	mu       sync.Mutex
	state    AnchorSnapshot
	failNext map[AnchorOperation]error
}

type anchorAdapter struct {
	store       *AnchorStore
	dbUUID      string
	schemaMajor uint16
}

type anchorIdentityRepository interface {
	AnchorIdentity() (string, uint16, error)
}

func NewAnchorStore() *AnchorStore {
	return &AnchorStore{failNext: map[AnchorOperation]error{}}
}

func NewAnchorStoreFromSnapshotBytes(data []byte) (*AnchorStore, error) {
	var snap AnchorSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	store := NewAnchorStore()
	store.state = snap
	return store, nil
}

func (s *AnchorStore) SnapshotBytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(s.state)
	if err != nil {
		panic(err)
	}
	return data
}

func (s *AnchorStore) Adapter(dbUUID string, schemaMajor uint16) Anchor {
	return anchorAdapter{store: s, dbUUID: dbUUID, schemaMajor: schemaMajor}
}

func (s *AnchorStore) FailNextForTest(operation AnchorOperation, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext == nil {
		s.failNext = map[AnchorOperation]error{}
	}
	if err == nil {
		err = fmt.Errorf("%w: injected %s failure", ErrAnchorInvariant, operation)
	}
	s.failNext[operation] = err
}

func (s *AnchorStore) ForceStateForTest(snapshot AnchorSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = snapshot
}

func (a anchorAdapter) Begin(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	if err := validateAnchorIdentity(a.dbUUID, a.schemaMajor); err != nil {
		return "", err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if err := a.store.failLocked(AnchorBegin); err != nil {
		return "", err
	}
	if err := a.store.ensureIdentityLocked(a.dbUUID, a.schemaMajor, generation); err != nil {
		return "", err
	}
	if a.store.state.Generation < generation {
		if err := a.store.failLocked(AnchorAdvance); err != nil {
			return "", err
		}
		a.store.state.Generation = generation
	}
	token := fmt.Sprintf("recovery-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	a.store.state.Phase = "reconciling"
	a.store.state.Boot = boot
	a.store.state.Token = token
	a.store.state.Reason = ""
	return token, nil
}

func (a anchorAdapter) SealReady(ctx context.Context, boot model.BootRef, generation uint64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := boot.Validate(); err != nil {
		return "", err
	}
	if err := validateAnchorIdentity(a.dbUUID, a.schemaMajor); err != nil {
		return "", err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if err := a.store.failLocked(AnchorSealReady); err != nil {
		return "", err
	}
	if err := a.store.requireIdentityLocked(a.dbUUID, a.schemaMajor); err != nil {
		return "", err
	}
	if a.store.state.Generation != generation {
		return "", ErrStaleCapability
	}
	if a.store.state.Phase == "fail_stopped" {
		return "", ErrFailStopped
	}
	token := fmt.Sprintf("ready-%s-%s-%d", boot.BootID, boot.OwnerID, generation)
	a.store.state.Phase = "ready"
	a.store.state.Boot = boot
	a.store.state.Token = token
	a.store.state.Reason = ""
	return token, nil
}

func (a anchorAdapter) Advance(ctx context.Context, boot model.BootRef, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	if err := validateAnchorIdentity(a.dbUUID, a.schemaMajor); err != nil {
		return err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if err := a.store.failLocked(AnchorAdvance); err != nil {
		return err
	}
	if err := a.store.requireIdentityLocked(a.dbUUID, a.schemaMajor); err != nil {
		return err
	}
	if a.store.state.Phase == "fail_stopped" {
		return ErrFailStopped
	}
	if a.store.state.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", ErrAnchorInvariant, a.store.state.Generation, generation)
	}
	a.store.state.Generation = generation
	a.store.state.Boot = boot
	if err := a.store.failLocked(AnchorComplete); err != nil {
		return err
	}
	return nil
}

func (a anchorAdapter) FailStop(ctx context.Context, boot model.BootRef, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := boot.Validate(); err != nil {
		return err
	}
	if err := validateAnchorIdentity(a.dbUUID, a.schemaMajor); err != nil {
		return err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if err := a.store.failLocked(AnchorFailStop); err != nil {
		return err
	}
	if err := a.store.requireIdentityLocked(a.dbUUID, a.schemaMajor); err != nil {
		return err
	}
	a.store.state.Phase = "fail_stopped"
	a.store.state.Boot = boot
	a.store.state.Reason = reason
	return nil
}

func (a anchorAdapter) VerifyReady(boot model.BootRef, token string, generation uint64) error {
	return a.verify("ready", boot, token, generation)
}

func (a anchorAdapter) VerifyRecovery(boot model.BootRef, token string, generation uint64) error {
	return a.verify("reconciling", boot, token, generation)
}

func (a anchorAdapter) verify(phase string, boot model.BootRef, token string, generation uint64) error {
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if err := a.store.requireIdentityLocked(a.dbUUID, a.schemaMajor); err != nil {
		return err
	}
	if a.store.state.Phase == "fail_stopped" {
		return ErrFailStopped
	}
	if a.store.state.Phase != phase || a.store.state.Boot != boot || a.store.state.Token != token || a.store.state.Generation != generation {
		return ErrStaleCapability
	}
	return nil
}

func (s *AnchorStore) ensureIdentityLocked(dbUUID string, schemaMajor uint16, generation uint64) error {
	if !s.state.Initialized {
		if generation != 0 {
			return fmt.Errorf("%w: missing anchor for initialized db generation %d", ErrAnchorInvariant, generation)
		}
		s.state = AnchorSnapshot{
			Initialized: true,
			DBUUID:      dbUUID,
			SchemaMajor: schemaMajor,
			Generation:  generation,
		}
		return nil
	}
	if err := s.requireIdentityLocked(dbUUID, schemaMajor); err != nil {
		return err
	}
	if s.state.Generation > generation {
		return fmt.Errorf("%w: anchor generation %d is ahead of db generation %d", ErrAnchorInvariant, s.state.Generation, generation)
	}
	return nil
}

func (s *AnchorStore) requireIdentityLocked(dbUUID string, schemaMajor uint16) error {
	if !s.state.Initialized {
		return fmt.Errorf("%w: anchor is missing", ErrAnchorInvariant)
	}
	if s.state.DBUUID != dbUUID {
		return fmt.Errorf("%w: db uuid mismatch", ErrAnchorInvariant)
	}
	if s.state.SchemaMajor != schemaMajor {
		return fmt.Errorf("%w: schema major mismatch", ErrAnchorInvariant)
	}
	return nil
}

func (s *AnchorStore) failLocked(operation AnchorOperation) error {
	if s.failNext == nil {
		return nil
	}
	err := s.failNext[operation]
	if err == nil {
		return nil
	}
	delete(s.failNext, operation)
	return err
}

func validateAnchorIdentity(dbUUID string, schemaMajor uint16) error {
	if dbUUID == "" {
		return fmt.Errorf("%w: db uuid is required", repository.ErrInvalidRecord)
	}
	if schemaMajor == 0 {
		return fmt.Errorf("%w: schema major is required", repository.ErrInvalidRecord)
	}
	return nil
}
