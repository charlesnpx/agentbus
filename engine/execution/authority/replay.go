package authority

import (
	"context"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

type ReplayState uint8

const (
	ReplayMissing ReplayState = iota + 1
	ReplayLive
	ReplayExpired
)

type ReplayResult struct {
	State      ReplayState
	Binding    model.Binding
	Tombstone  repository.Tombstone
	Record     model.SafetyRecord
	Projection model.JobProjection
}

func (r *Ready) LookupReplay(ctx context.Context, key model.RequestKey) (ReplayResult, error) {
	if r == nil || r.core == nil {
		return ReplayResult{}, ErrNotReady
	}
	if err := key.Validate(); err != nil {
		return ReplayResult{}, fmt.Errorf("%w: request_key: %v", ErrInvalidRequest, err)
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var result ReplayResult
	if err := r.core.view(ctx, "lookup replay", func(tx repository.ReadTx) error {
		if _, err := r.core.requireReadyTx(tx, r.token); err != nil {
			return err
		}
		replay, err := lookupReplayTx(tx, key)
		if err != nil {
			return err
		}
		result = replay
		return nil
	}); err != nil {
		return ReplayResult{}, err
	}
	return result, nil
}

func (r *Ready) Expire(ctx context.Context, key model.RequestKey) (repository.Tombstone, error) {
	if r == nil || r.core == nil {
		return repository.Tombstone{}, ErrNotReady
	}
	if err := key.Validate(); err != nil {
		return repository.Tombstone{}, fmt.Errorf("%w: request_key: %v", ErrInvalidRequest, err)
	}

	r.core.mu.Lock()
	defer r.core.mu.Unlock()

	var tombstone repository.Tombstone
	commit, err := r.core.update(ctx, "expire request", func(tx repository.WriteTx) error {
		meta, err := r.core.requireReadyTx(tx, r.token)
		if err != nil {
			return err
		}
		image := tx.LookupRequest(key)
		if err := rejectBadRequestImage(image); err != nil {
			return err
		}
		if image.Tombstone.State == repository.RecordValid {
			tombstone = image.Tombstone.Value
			return nil
		}
		if image.Binding.State != repository.RecordValid {
			return fmt.Errorf("%w: request %s", ErrNotFound, key)
		}
		job := tx.LoadJob(image.Binding.Value.JobID)
		record, _, err := validJobImage(job)
		if err != nil {
			return err
		}
		if record.Terminal == nil {
			return fmt.Errorf("%w: request %s is nonterminal", ErrRecoveryNeeded, key)
		}
		tombstone = repository.Tombstone{
			RequestKey:        key,
			JobID:             record.JobID,
			TaskIdentity:      record.TaskIdentity,
			ExpiredGeneration: meta.Generation + 1,
		}
		if err := tx.DeleteLiveJob(record.JobID); err != nil {
			return err
		}
		return tx.PutTombstone(tombstone)
	})
	if err != nil {
		return repository.Tombstone{}, err
	}
	if err := r.core.advanceReadyLocked(ctx, &r.token, commit.Generation); err != nil {
		return repository.Tombstone{}, err
	}
	return tombstone, nil
}

func lookupReplayTx(tx repository.ReadTx, key model.RequestKey) (ReplayResult, error) {
	image := tx.LookupRequest(key)
	if err := rejectBadRequestImage(image); err != nil {
		return ReplayResult{}, err
	}
	if image.Tombstone.State == repository.RecordValid {
		return ReplayResult{State: ReplayExpired, Tombstone: image.Tombstone.Value}, nil
	}
	if image.Binding.State != repository.RecordValid {
		return ReplayResult{State: ReplayMissing}, nil
	}
	job := tx.LoadJob(image.Binding.Value.JobID)
	record, projection, err := validJobImage(job)
	if err != nil {
		return ReplayResult{}, err
	}
	return ReplayResult{
		State:      ReplayLive,
		Binding:    image.Binding.Value,
		Record:     record,
		Projection: projection,
	}, nil
}
