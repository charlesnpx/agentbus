//go:build darwin

package procgroup

import (
	"os"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func TestDarwinCurrentProcessStartTokenStable(t *testing.T) {
	pid := os.Getpid()
	first, err := ReadProcessStartToken(pid)
	if err != nil {
		t.Fatalf("ReadProcessStartToken(self) first error = %v", err)
	}
	if !strings.Contains(first.String(), "-uid-") {
		t.Fatalf("start token %q does not include real uid", first)
	}
	if !strings.Contains(first.String(), "-ppid-") {
		t.Fatalf("start token %q does not include parent pid", first)
	}

	for i := 0; i < 5; i++ {
		got, err := ReadProcessStartToken(pid)
		if err != nil {
			t.Fatalf("ReadProcessStartToken(self) iteration %d error = %v", i, err)
		}
		if got != first {
			t.Fatalf("ReadProcessStartToken(self) iteration %d = %q, want %q", i, got, first)
		}
	}
}

func TestDarwinFabricatedUIDPPIDMismatchClassifiesReused(t *testing.T) {
	kinfo := readDarwinKinfoForTest(t, os.Getpid())
	current, err := darwinProcessSnapshot(kinfo)
	if err != nil {
		t.Fatalf("darwinProcessSnapshot(current) error = %v", err)
	}
	domain, err := CurrentKernelDomain()
	if err != nil {
		t.Fatalf("CurrentKernelDomain() error = %v", err)
	}
	claim, err := NewProcessClaim(current.PID, current.PGID, current.StartToken, domain)
	if err != nil {
		t.Fatalf("NewProcessClaim(current) error = %v", err)
	}

	reusedKinfo := kinfo
	reusedKinfo.Eproc.Pcred.P_ruid++
	reusedKinfo.Eproc.Ppid++
	reused, err := darwinProcessSnapshot(reusedKinfo)
	if err != nil {
		t.Fatalf("darwinProcessSnapshot(reused) error = %v", err)
	}
	if reused.StartToken == current.StartToken {
		t.Fatalf("fabricated reused token = %q, want different from %q", reused.StartToken, current.StartToken)
	}

	reader := fakeKernelReader{
		domain: domain,
		processes: map[int]processSnapshot{
			current.PID: reused,
		},
	}
	if got := classifyProcess(reader, claim); got != model.ProcessIdentityReused {
		t.Fatalf("classifyProcess(fabricated uid/ppid mismatch) = %v, want %v", got, model.ProcessIdentityReused)
	}
}

func TestDarwinHostBootIDStable(t *testing.T) {
	first, err := HostBootID()
	if err != nil {
		t.Fatalf("HostBootID() first error = %v", err)
	}
	if first == "" {
		t.Fatal("HostBootID() first returned empty id")
	}

	for i := 0; i < 5; i++ {
		got, err := HostBootID()
		if err != nil {
			t.Fatalf("HostBootID() iteration %d error = %v", i, err)
		}
		if got != first {
			t.Fatalf("HostBootID() iteration %d = %q, want %q", i, got, first)
		}
	}
}

func readDarwinKinfoForTest(t *testing.T, pid int) unix.KinfoProc {
	t.Helper()
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		t.Fatalf("SysctlKinfoProcSlice(kern.proc.pid, %d) error = %v", pid, err)
	}
	if len(processes) != 1 {
		t.Fatalf("SysctlKinfoProcSlice(kern.proc.pid, %d) returned %d entries, want 1", pid, len(processes))
	}
	return processes[0]
}
