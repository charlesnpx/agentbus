package served

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/protocol"
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
