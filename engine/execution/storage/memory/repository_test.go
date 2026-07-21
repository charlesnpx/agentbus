package memory

import (
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
		MissingMeta: func(t *testing.T, repo repository.Repository) {
			t.Helper()
			memoryRepo, ok := repo.(*Repository)
			if !ok {
				t.Fatalf("repo type = %T, want *memory.Repository", repo)
			}
			memoryRepo.InjectMissingMetaForTest()
		},
	})
}
