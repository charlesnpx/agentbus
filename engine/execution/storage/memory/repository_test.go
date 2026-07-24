package memory

import (
	"context"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/repository/repositorytest"
)

func TestRepositoryContract(t *testing.T) {
	repositorytest.RunRepositoryContract(t, repositorytest.Factory{
		New: func(t *testing.T) repository.Repository {
			t.Helper()
			return NewRepository()
		},
		Snapshot: func(t *testing.T, repo repository.Repository) []byte {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			return memoryRepo.SnapshotBytes()
		},
		CorruptSafety: func(t *testing.T, repo repository.Repository, jobID model.JobID, diagnostic string) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.InjectCorruptSafetyForTest(jobID, diagnostic)
		},
		CorruptBinding: func(t *testing.T, repo repository.Repository, key model.RequestKey, diagnostic string) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.InjectCorruptBindingForTest(key, diagnostic)
		},
		CorruptTombstone: func(t *testing.T, repo repository.Repository, key model.RequestKey, diagnostic string) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.InjectCorruptTombstoneForTest(key, diagnostic)
		},
		MissingMeta: func(t *testing.T, repo repository.Repository) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.InjectMissingMetaForTest()
		},
		FailCommitAfterCommit: func(t *testing.T, repo repository.Repository, err error) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.FailCommitAfterCommitForTest(err)
		},
		Audit: func(t *testing.T, repo repository.Repository) error {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			return memoryRepo.AuditIntegrity(context.Background())
		},
	})
}
