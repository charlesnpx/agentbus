package served

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/repository"
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
	after := hashRootFiles(t, root)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("root file hashes after recovery = %#v, want %#v", after, before)
	}
	return err
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
