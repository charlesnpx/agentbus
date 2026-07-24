package bbolt

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
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
		MissingMeta: func(t *testing.T, repo repository.Repository) {
			t.Helper()
			bboltRepo, ok := repo.(interface {
				InjectMissingMetaForTest()
			})
			if !ok {
				t.Fatalf("repo type = %T, want InjectMissingMetaForTest", repo)
			}
			bboltRepo.InjectMissingMetaForTest()
		},
	})
}

func TestAdmissionRepositoryRequiredBucketsMatchInitializedRepository(t *testing.T) {
	path := filepath.Join(t.TempDir(), "admission.db")
	repo, err := Open(path, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer repo.Close()

	var got []string
	if err := repo.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			got = append(got, string(name))
			return nil
		})
	}); err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	want := append([]string(nil), AdmissionRepositoryRequiredBuckets...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("initialized buckets = %v, want %v", got, want)
	}
	if err := repo.VerifyInitializedStructure(); err != nil {
		t.Fatalf("VerifyInitializedStructure() error = %v", err)
	}
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

func (r *reopeningRepository) InjectMissingMetaForTest() {
	r.repo.InjectMissingMetaForTest()
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
