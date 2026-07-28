package served

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bolt "go.etcd.io/bbolt"
)

func TestActivatedBboltV1RootFailsTypedBeforeSocketBindAndLeavesFileUntouched(t *testing.T) {
	root := shortTempDir(t)
	initial, err := bootstrapAdmissionRootForTest(t, root)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.closeServeAdmission(); err != nil {
		t.Fatal(err)
	}
	forgeServedBboltAdmissionContractVersionForTest(t, root, 1)
	repoPath := filepath.Join(root, admissionRepositoryFile)
	before := readFileBytes(t, repoPath)

	restart := newTestServerAtRoot(t, root, shortTempDir(t), newFakeBackend("fake"))
	configureTestAdmissionRuntime(t, restart, newAdmissionFakeLaunchCustodian(t), true)
	restart.listenerFactory = func() (net.Listener, socketFileIdentity, error) {
		return nil, socketFileIdentity{}, errors.New("listener must not open for activated v1 contract mismatch")
	}
	err = restart.Serve(context.Background())
	if !errors.Is(err, authority.ErrAdmissionContractMismatch) {
		t.Fatalf("Serve() error = %v, want ErrAdmissionContractMismatch", err)
	}
	var incompatible authority.IncompatibleAdmissionContractVersionError
	if !errors.As(err, &incompatible) || incompatible.RootContractVersion != 1 || incompatible.DaemonContractVersion != authority.CurrentAdmissionContractVersion || !incompatible.Activated {
		t.Fatalf("Serve() error = %#v %v, want activated v1 incompatible contract", incompatible, err)
	}
	assertNoServeAdmissionPublished(t, restart)
	after := readFileBytes(t, repoPath)
	if !bytes.Equal(before, after) {
		t.Fatal("activated v1 contract mismatch mutated admission repository")
	}
}

type servedBboltEnvelopeForTest struct {
	Kind          string          `json:"kind"`
	SchemaVersion uint16          `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	Payload       json.RawMessage `json:"payload"`
	Checksum      string          `json:"checksum"`
}

func forgeServedBboltAdmissionContractVersionForTest(t *testing.T, root string, version uint16) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(root, admissionRepositoryFile), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("meta"))
		if bucket == nil {
			return errors.New("meta bucket missing")
		}
		raw := bucket.Get([]byte("authority"))
		if raw == nil {
			return errors.New("authority meta missing")
		}
		var envelope servedBboltEnvelopeForTest
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
		var meta repository.AuthorityMeta
		if err := json.Unmarshal(envelope.Payload, &meta); err != nil {
			return err
		}
		meta.AdmissionRoot.ContractVersion = version
		payload, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		envelope.Payload = append(json.RawMessage(nil), payload...)
		envelope.Checksum = checksumServedBboltEnvelopeForTest(envelope.Kind, envelope.SchemaVersion, envelope.Revision, []byte("authority"), envelope.Payload)
		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("authority"), encoded)
	}); err != nil {
		t.Fatal(err)
	}
}

func checksumServedBboltEnvelopeForTest(kind string, schemaVersion uint16, revision uint64, key, payload []byte) string {
	hash := sha256.New()
	checksumServedBboltFieldForTest(hash, kind)
	checksumServedBboltFieldForTest(hash, strconv.FormatUint(uint64(schemaVersion), 10))
	checksumServedBboltFieldForTest(hash, strconv.FormatUint(revision, 10))
	checksumServedBboltFieldForTest(hash, string(key))
	checksumServedBboltFieldForTest(hash, string(payload))
	return hex.EncodeToString(hash.Sum(nil))
}

func checksumServedBboltFieldForTest(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}
