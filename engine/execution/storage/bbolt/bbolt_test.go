package bbolt

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/repository/repositorytest"
	bolt "go.etcd.io/bbolt"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.RunRepositoryContract(t, repositorytest.Factory{
		New: func(t *testing.T) repository.Repository {
			t.Helper()
			repo := newReopeningRepository(t)
			return repo
		},
		Snapshot: func(t *testing.T, repo repository.Repository) []byte {
			t.Helper()
			bboltRepo, ok := repo.(interface{ SnapshotBytes() []byte })
			if !ok {
				t.Fatalf("repo type = %T, want SnapshotBytes", repo)
			}
			return bboltRepo.SnapshotBytes()
		},
		CorruptSafety: func(t *testing.T, repo repository.Repository, jobID model.JobID, diagnostic string) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectCorruptSafetyForTest(model.JobID, string)
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectCorruptSafetyForTest", repo)
			}
			bboltRepo.InjectCorruptSafetyForTest(jobID, diagnostic)
		},
	})
}

type reopeningRepository struct {
	t    *testing.T
	path string
	repo *Repository
}

func newReopeningRepository(t *testing.T) *reopeningRepository {
	t.Helper()
	wrapper := &reopeningRepository{
		t:    t,
		path: filepath.Join(t.TempDir(), "admission.db"),
	}
	wrapper.reopen()
	t.Cleanup(func() {
		if wrapper.repo != nil {
			if err := wrapper.repo.Close(); err != nil {
				t.Fatalf("close bbolt repository: %v", err)
			}
		}
	})
	return wrapper
}

func (r *reopeningRepository) View(ctx context.Context, fn func(repository.ReadTx) error) error {
	return r.repo.View(ctx, fn)
}

func (r *reopeningRepository) Update(ctx context.Context, fn func(repository.WriteTx) error) (repository.Commit, error) {
	commit, err := r.repo.Update(ctx, fn)
	r.reopen()
	return commit, err
}

func (r *reopeningRepository) SnapshotBytes() []byte {
	return r.repo.SnapshotBytes()
}

func (r *reopeningRepository) InjectCorruptSafetyForTest(jobID model.JobID, diagnostic string) {
	r.repo.InjectCorruptSafetyForTest(jobID, diagnostic)
	r.reopen()
}

func (r *reopeningRepository) reopen() {
	r.t.Helper()
	if r.repo != nil {
		if err := r.repo.Close(); err != nil {
			r.t.Fatalf("close bbolt repository before reopen: %v", err)
		}
	}
	repo, err := Open(r.path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		r.t.Fatalf("open bbolt repository: %v", err)
	}
	r.repo = repo
}
