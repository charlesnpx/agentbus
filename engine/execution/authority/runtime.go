package authority

import (
	"context"
	"fmt"
	"sort"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

var ErrRuntimeConflict = fmt.Errorf("%w: runtime ownership conflict", ErrInvalidRequest)

type RuntimeSnapshot struct {
	Pending []model.AttemptRef
	Owned   []OwnedAttempt
}

type OwnedAttempt struct {
	Ref   model.AttemptRef
	Owner model.OwnerID
}

type runtimeRegistry struct {
	pending map[model.JobID]model.AttemptRef
	owned   map[model.JobID]OwnedAttempt
}

func newRuntimeRegistry() *runtimeRegistry {
	return &runtimeRegistry{
		pending: map[model.JobID]model.AttemptRef{},
		owned:   map[model.JobID]OwnedAttempt{},
	}
}

func (registry *runtimeRegistry) registerPending(ref model.AttemptRef) error {
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_ref: %v", ErrInvalidRequest, err)
	}
	if owned, ok := registry.owned[ref.JobID]; ok && !owned.Ref.Equal(ref) {
		return fmt.Errorf("%w: job %s already owned", ErrRuntimeConflict, ref.JobID)
	}
	if pending, ok := registry.pending[ref.JobID]; ok && !pending.Equal(ref) {
		return fmt.Errorf("%w: job %s already pending", ErrRuntimeConflict, ref.JobID)
	}
	if _, ok := registry.owned[ref.JobID]; ok {
		return nil
	}
	registry.pending[ref.JobID] = ref
	return nil
}

func (registry *runtimeRegistry) claim(ref model.AttemptRef, owner model.OwnerID) error {
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%w: attempt_ref: %v", ErrInvalidRequest, err)
	}
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("%w: owner_id: %v", ErrInvalidRequest, err)
	}
	if owned, ok := registry.owned[ref.JobID]; ok {
		if owned.Ref.Equal(ref) && owned.Owner == owner {
			return nil
		}
		return fmt.Errorf("%w: job %s already owned", ErrRuntimeConflict, ref.JobID)
	}
	pending, ok := registry.pending[ref.JobID]
	if !ok {
		return fmt.Errorf("%w: job %s is not pending", ErrRuntimeConflict, ref.JobID)
	}
	if !pending.Equal(ref) {
		return fmt.Errorf("%w: job %s pending attempt mismatch", ErrRuntimeConflict, ref.JobID)
	}
	delete(registry.pending, ref.JobID)
	registry.owned[ref.JobID] = OwnedAttempt{Ref: ref, Owner: owner}
	return nil
}

func (registry *runtimeRegistry) registerAndClaimPending(ref model.AttemptRef, owner model.OwnerID) error {
	if err := registry.registerPending(ref); err != nil {
		return err
	}
	return registry.claim(ref, owner)
}

func (registry *runtimeRegistry) releaseTerminal(jobID model.JobID) {
	delete(registry.pending, jobID)
	delete(registry.owned, jobID)
}

func (registry *runtimeRegistry) snapshot() RuntimeSnapshot {
	pending := make([]model.AttemptRef, 0, len(registry.pending))
	for _, ref := range registry.pending {
		pending = append(pending, ref)
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].JobID < pending[j].JobID
	})

	owned := make([]OwnedAttempt, 0, len(registry.owned))
	for _, ref := range registry.owned {
		owned = append(owned, ref)
	}
	sort.Slice(owned, func(i, j int) bool {
		return owned[i].Ref.JobID < owned[j].Ref.JobID
	})
	return RuntimeSnapshot{Pending: pending, Owned: owned}
}

func (r *Ready) ClaimPending(ctx context.Context, ref model.AttemptRef, owner model.OwnerID) error {
	if r == nil || r.core == nil {
		return ErrNotReady
	}
	r.core.mu.Lock()
	defer r.core.mu.Unlock()
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
		_, err := r.core.requireReadyTx(tx, r.token)
		return err
	}); err != nil {
		return err
	}
	return r.core.runtime.claim(ref, owner)
}

func (r *Ready) RuntimeSnapshot(ctx context.Context) (RuntimeSnapshot, error) {
	if r == nil || r.core == nil {
		return RuntimeSnapshot{}, ErrNotReady
	}
	r.core.mu.Lock()
	defer r.core.mu.Unlock()
	if err := r.core.repo.View(ctx, func(tx repository.ReadTx) error {
		_, err := r.core.requireReadyTx(tx, r.token)
		return err
	}); err != nil {
		return RuntimeSnapshot{}, err
	}
	return r.core.runtime.snapshot(), nil
}
