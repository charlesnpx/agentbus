//go:build unix

package authority

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func TestInspectAdmissionRootBusyWithFIFOPIDFileKeepsHolderUnknown(t *testing.T) {
	root := t.TempDir()
	repo, err := bboltrepo.Create(filepath.Join(root, AdmissionRepositoryFile))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	if err := unix.Mkfifo(filepath.Join(root, admissionDaemonPIDFile), 0o600); err != nil {
		t.Fatal(err)
	}

	oldTimeout := adminOpenTimeout
	adminOpenTimeout = 20 * time.Millisecond
	defer func() { adminOpenTimeout = oldTimeout }()

	done := make(chan error, 1)
	go func() {
		_, err := InspectAdmissionRoot(context.Background(), root)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrRootBusy) || !errors.Is(err, bolt.ErrTimeout) {
			t.Fatalf("inspect busy FIFO-pid root error = %v, want ErrRootBusy and bolt.ErrTimeout", err)
		}
		if got := err.Error(); !strings.Contains(got, "lock holder liveness is unknown") || !strings.Contains(got, "if an agentbus daemon holds the database") {
			t.Fatalf("inspect busy FIFO-pid root error = %q, want unknown-holder diagnostic", err)
		}
	case <-time.After(time.Second):
		t.Fatal("inspect busy FIFO-pid root did not return promptly")
	}
}
