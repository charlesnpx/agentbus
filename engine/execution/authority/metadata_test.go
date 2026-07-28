package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"github.com/charlesnpx/agentbus/engine/execution/storage/memory"
)

func TestCandidateAdmissionContractV1ActivationRefusedTyped(t *testing.T) {
	repo := memory.NewRepository()
	repo = forgeMemoryAdmissionContractVersionForTest(t, repo, 1)
	session, err := beginRecoveryWithAnchorStore(t, repo, NewAnchorStore(), "candidate-contract-v1")
	if err != nil {
		t.Fatal(err)
	}
	before := repo.SnapshotBytes()

	_, _, err = session.ActivateRoot(context.Background())
	if !errors.Is(err, ErrAdmissionContractMismatch) {
		t.Fatalf("ActivateRoot() error = %v, want ErrAdmissionContractMismatch", err)
	}
	var incompatible IncompatibleAdmissionContractVersionError
	if !errors.As(err, &incompatible) || incompatible.RootContractVersion != 1 || incompatible.DaemonContractVersion != CurrentAdmissionContractVersion || incompatible.Activated {
		t.Fatalf("ActivateRoot() error = %#v %v, want candidate v1 incompatible contract", incompatible, err)
	}
	if !bytes.Equal(before, repo.SnapshotBytes()) {
		t.Fatal("candidate v1 activation refusal mutated repository")
	}
}

func forgeMemoryAdmissionContractVersionForTest(t *testing.T, repo *memory.Repository, version uint16) *memory.Repository {
	t.Helper()
	type memorySnapshot struct {
		DBUUID          string                                      `json:"dbUUID"`
		Generation      uint64                                      `json:"generation"`
		NextJobSequence uint64                                      `json:"nextJobSequence"`
		Meta            repository.Record[repository.AuthorityMeta] `json:"meta"`
		Bindings        json.RawMessage                             `json:"bindings"`
		Tombstones      json.RawMessage                             `json:"tombstones"`
		Safety          json.RawMessage                             `json:"safety"`
		Projections     json.RawMessage                             `json:"projections"`
		Quarantines     json.RawMessage                             `json:"quarantines"`
	}
	var snapshot memorySnapshot
	if err := json.Unmarshal(repo.SnapshotBytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Meta.Value.AdmissionRoot.ContractVersion = version
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := memory.NewRepositoryFromSnapshotBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	return forged
}
