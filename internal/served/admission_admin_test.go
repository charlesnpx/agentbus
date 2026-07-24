package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
	bolt "go.etcd.io/bbolt"
)

func TestRecoverAdmissionRootMissingRootDoesNotCreate(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "missing")

	_, err := RecoverAdmissionRoot(context.Background(), Config{
		StateRoot: root,
		CWD:       t.TempDir(),
		Runtime:   custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
	})
	if !errors.Is(err, ErrAdmissionRootMissing) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionRootMissing", err)
	}
	if _, statErr := os.Stat(root); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("state root stat = %v, want missing", statErr)
	}
}

func TestRecoverAdmissionRootUnavailableStrictSupportDoesNotInitializeExistingEmptyRoot(t *testing.T) {
	root := t.TempDir()

	_, err := RecoverAdmissionRoot(context.Background(), Config{
		StateRoot: root,
		CWD:       t.TempDir(),
		Runtime:   custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
	})
	var diagnostic AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("state root entries after recovery = %s, want empty root", strings.Join(names, ", "))
	}
}

func TestRecoverAdmissionRootRejectsUninitializedRepositoryWithoutMutation(t *testing.T) {
	root := t.TempDir()
	createRawBboltFile(t, filepath.Join(root, admissionRepositoryFile))

	err := recoverAdmissionRootWithAvailableRuntime(t, root)
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrCorruptRecord", err)
	}
}

func TestRecoverAdmissionRootRejectsMissingBucketWithoutMutation(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	deleteAdmissionRepositoryBucket(t, filepath.Join(root, admissionRepositoryFile), "safety")

	err := recoverAdmissionRootWithAvailableRuntime(t, root)
	if !errors.Is(err, repository.ErrCorruptRecord) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrCorruptRecord", err)
	}
}

func TestRecoverAdmissionRootRejectsGenerationZeroMissingAnchorWithoutMutation(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	anchorPath := filepath.Join(root, admissionAnchorFile)
	if err := os.Remove(anchorPath); err != nil {
		t.Fatal(err)
	}

	err := recoverAdmissionRootWithAvailableRuntime(t, root)
	if !errors.Is(err, authority.ErrAnchorInvariant) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAnchorInvariant", err)
	}
	if _, statErr := os.Stat(anchorPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("anchor stat after recovery = %v, want missing", statErr)
	}
}

func TestRecoverAdmissionRootRejectsRepositorySwapAfterPreflightWithoutMintingAnchor(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	replacementRoot := initializedGenerationZeroAdmissionRoot(t)
	repoPath := filepath.Join(root, admissionRepositoryFile)
	anchorPath := filepath.Join(root, admissionAnchorFile)
	replacementRepoPath := filepath.Join(replacementRoot, admissionRepositoryFile)

	setAdmissionRecoveryAfterPreflightHookForTest(t, func() error {
		if err := os.Remove(anchorPath); err != nil {
			return err
		}
		return os.Rename(replacementRepoPath, repoPath)
	})

	err := recoverAdmissionRootWithAvailableRuntimeNoHash(t, root)
	var mismatch AdmissionRootIdentityMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want AdmissionRootIdentityMismatchError", err, err)
	}
	if !errors.Is(err, authority.ErrAnchorInvariant) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAnchorInvariant", err)
	}
	if _, statErr := os.Stat(anchorPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("anchor stat after swapped recovery = %v, want missing", statErr)
	}
}

func TestRecoverAdmissionRootRejectsAnchorRemovedBeforeBeginWithoutMinting(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	anchorPath := filepath.Join(root, admissionAnchorFile)
	setAdmissionRecoveryBeforeBeginHookForTest(t, func() error {
		return os.Remove(anchorPath)
	})

	err := recoverAdmissionRootWithAvailableRuntimeNoHash(t, root)
	if !errors.Is(err, authority.ErrAnchorInvariant) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAnchorInvariant", err)
	}
	if _, statErr := os.Stat(anchorPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("anchor stat after recovery = %v, want missing", statErr)
	}
}

func TestRecoverAdmissionRootRejectsMetaRemovedAfterPreflightWithoutMutation(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	repoPath := filepath.Join(root, admissionRepositoryFile)
	setAdmissionRecoveryAfterPreflightHookForTest(t, func() error {
		deleteAdmissionRepositoryMetaKey(t, repoPath, "authority")
		return nil
	})

	err := recoverAdmissionRootWithAvailableRuntimeNoHash(t, root)
	if !errors.Is(err, repository.ErrInvalidRecord) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrInvalidRecord", err)
	}
}

func TestRecoverAdmissionRootPreflightLockContentionReportsRootBusyWithoutMutation(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	repoPath := filepath.Join(root, admissionRepositoryFile)
	holder, err := bboltrepo.Open(repoPath, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("hold repository lock: %v", err)
	}
	defer holder.Close()

	err = recoverAdmissionRootWithAvailableRuntime(t, root)
	var busy AdmissionRootBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want AdmissionRootBusyError", err, err)
	}
	if !errors.Is(err, ErrAdmissionRootBusy) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionRootBusy", err)
	}
}

func TestRecoverAdmissionRootWrapsUnreadableAnchorAsTypedRootError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read owner-denied files")
	}
	root := initializedGenerationZeroAdmissionRoot(t)
	anchorPath := filepath.Join(root, admissionAnchorFile)
	if err := os.Chmod(anchorPath, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(anchorPath, 0o600)
	})

	err := recoverAdmissionRootWithAvailableRuntimeNoHash(t, root)
	var anchorErr AdmissionRootAnchorError
	if !errors.As(err, &anchorErr) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want AdmissionRootAnchorError", err, err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want *os.PathError in chain", err, err)
	}
	if !errors.Is(err, authority.ErrAnchorInvariant) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAnchorInvariant", err)
	}
}

func TestRecoverAdmissionRootWrapsUnsupportedMetaSchemaAsTypedRootError(t *testing.T) {
	root := initializedGenerationZeroAdmissionRoot(t)
	setAdmissionRepositoryMetaSchemaVersion(t, filepath.Join(root, admissionRepositoryFile), repository.CurrentAuthorityMetaSchemaVersion+1)

	err := recoverAdmissionRootWithAvailableRuntime(t, root)
	var schemaErr AdmissionRootIncompatibleSchemaError
	if !errors.As(err, &schemaErr) {
		t.Fatalf("RecoverAdmissionRoot error = %T %v, want AdmissionRootIncompatibleSchemaError", err, err)
	}
	if !errors.Is(err, repository.ErrInvalidRecord) {
		t.Fatalf("RecoverAdmissionRoot error = %v, want ErrInvalidRecord", err)
	}
}

func TestRecoverAdmissionRootClosesRuntimeOnEarlyServerConstructionErrors(t *testing.T) {
	tests := []struct {
		name string
		root func(t *testing.T) string
	}{
		{
			name: "missing root",
			root: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "missing")
			},
		},
		{
			name: "root is file",
			root: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "state")
				if err := os.WriteFile(path, []byte("not a directory"), 0o600); err != nil {
					t.Fatal(err)
				}
				return path
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var closes atomic.Int64
			_, err := RecoverAdmissionRoot(context.Background(), Config{
				StateRoot: tt.root(t),
				CWD:       t.TempDir(),
				Runtime: custodian.NewUnavailableRuntimeForTest(custodian.ErrSupervisorUnavailable, func() error {
					closes.Add(1)
					return nil
				}),
			})
			if !errors.Is(err, ErrAdmissionRootMissing) {
				t.Fatalf("RecoverAdmissionRoot error = %v, want ErrAdmissionRootMissing", err)
			}
			if got := closes.Load(); got != 1 {
				t.Fatalf("runtime closes = %d, want 1", got)
			}
		})
	}
}

func TestRetryableCgroupLeaseContentionCanConvergeOnDialableDaemon(t *testing.T) {
	root := shortTempDir(t)
	socketPath := filepath.Join(root, protocol.SocketName)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		if strings.Contains(err.Error(), "bind: operation not permitted") {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()

	support := custodian.Support{
		Assessment: custodian.SupportAssessment{
			Class:       custodian.SupportRetryable,
			Cause:       cgroup.ErrRootLeaseUnavailable,
			Attempts:    3,
			CleanupSafe: true,
		},
	}
	err = alreadyListeningAfterRetryableSupportContention(context.Background(), Config{StateRoot: root}, support)
	if !errors.Is(err, ErrDaemonAlreadyListening) {
		t.Fatalf("alreadyListeningAfterRetryableSupportContention error = %v, want ErrDaemonAlreadyListening", err)
	}

	support.Assessment.Class = custodian.SupportUnsupported
	if err := alreadyListeningAfterRetryableSupportContention(context.Background(), Config{StateRoot: root}, support); err != nil {
		t.Fatalf("unsupported cgroup lease support mapped to convergence: %v", err)
	}
}

func recoverAdmissionRootWithAvailableRuntime(t *testing.T, root string) error {
	t.Helper()
	before := hashRootFiles(t, root)
	err := recoverAdmissionRootWithAvailableRuntimeNoHash(t, root)
	after := hashRootFiles(t, root)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("root file hashes after recovery = %#v, want %#v", after, before)
	}
	return err
}

func recoverAdmissionRootWithAvailableRuntimeNoHash(t *testing.T, root string) error {
	t.Helper()
	server, err := newAdmissionRecoveryServer(Config{
		StateRoot: root,
		CWD:       t.TempDir(),
		Runtime:   custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
	})
	if err != nil {
		t.Fatal(err)
	}
	var closes atomic.Int64
	server.admissionRuntime = closeCountingServedAdmissionRuntime(t, &closes, admissionSupportForClass(t, custodian.SupportAvailable, true, 1))
	_, err = server.recoverAdmissionRoot(context.Background())
	if got := closes.Load(); got != 1 {
		t.Fatalf("runtime closes = %d, want 1", got)
	}
	return err
}

func setAdmissionRecoveryAfterPreflightHookForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := admissionRecoveryAfterPreflightForTest
	admissionRecoveryAfterPreflightForTest = hook
	t.Cleanup(func() {
		admissionRecoveryAfterPreflightForTest = previous
	})
}

func setAdmissionRecoveryBeforeBeginHookForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := admissionRecoveryBeforeBeginForTest
	admissionRecoveryBeforeBeginForTest = hook
	t.Cleanup(func() {
		admissionRecoveryBeforeBeginForTest = previous
	})
}

func initializedGenerationZeroAdmissionRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := authority.ResetEmptyAdmissionRoot(context.Background(), root); err != nil {
		t.Fatalf("initialize admission root: %v", err)
	}
	return root
}

func createRawBboltFile(t *testing.T, path string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func deleteAdmissionRepositoryBucket(t *testing.T, path, bucket string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket))
	}); err != nil {
		t.Fatalf("delete bbolt bucket %q: %v", bucket, err)
	}
}

func deleteAdmissionRepositoryMetaKey(t *testing.T, path, key string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		if meta == nil {
			return errors.New("meta bucket is missing")
		}
		return meta.Delete([]byte(key))
	}); err != nil {
		t.Fatalf("delete bbolt meta key %q: %v", key, err)
	}
}

type testBboltEnvelope struct {
	Kind          string          `json:"kind"`
	SchemaVersion uint16          `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	Payload       json.RawMessage `json:"payload"`
	Checksum      string          `json:"checksum"`
}

func setAdmissionRepositoryMetaSchemaVersion(t *testing.T, path string, schemaVersion uint16) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		metaBucket := tx.Bucket([]byte("meta"))
		if metaBucket == nil {
			return errors.New("meta bucket is missing")
		}
		raw := metaBucket.Get([]byte("authority"))
		if raw == nil {
			return errors.New("authority meta is missing")
		}
		var envelope testBboltEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return err
		}
		var payload map[string]any
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return err
		}
		payload["SchemaVersion"] = schemaVersion
		nextPayload, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		envelope.Payload = append(json.RawMessage(nil), nextPayload...)
		envelope.Checksum = testBboltChecksumEnvelope(envelope.Kind, envelope.SchemaVersion, envelope.Revision, []byte("authority"), envelope.Payload)
		nextRaw, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		return metaBucket.Put([]byte("authority"), nextRaw)
	}); err != nil {
		t.Fatalf("set unsupported meta schema: %v", err)
	}
}

func testBboltChecksumEnvelope(kind string, schemaVersion uint16, revision uint64, key, payload []byte) string {
	hash := sha256.New()
	testBboltChecksumField(hash, kind)
	testBboltChecksumField(hash, strconv.FormatUint(uint64(schemaVersion), 10))
	testBboltChecksumField(hash, strconv.FormatUint(revision, 10))
	testBboltChecksumField(hash, string(key))
	testBboltChecksumField(hash, string(payload))
	return hex.EncodeToString(hash.Sum(nil))
}

func testBboltChecksumField(hash interface{ Write([]byte) (int, error) }, value string) {
	_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(value))
	_, _ = hash.Write([]byte{0})
}

func hashRootFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	hashes := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		hashes[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hashes
}
