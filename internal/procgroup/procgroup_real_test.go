//go:build darwin || linux

package procgroup

import (
	"os"
	"os/exec"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestRealCurrentProcessMatchesKernelReadClaim(t *testing.T) {
	claim, err := ReadProcessClaim(os.Getpid())
	if err != nil {
		t.Fatalf("ReadProcessClaim(self) error = %v", err)
	}
	if got := ClassifyProcess(claim); got != model.ProcessIdentityMatching {
		t.Fatalf("ClassifyProcess(self) = %v, want %v", got, model.ProcessIdentityMatching)
	}
	if claim.KernelDomainID.PIDNamespaceState != model.PIDNamespaceNotApplicable && claim.KernelDomainID.PIDNamespaceState != model.PIDNamespaceKnown {
		t.Fatalf("kernel domain PID namespace state = %v", claim.KernelDomainID.PIDNamespaceState)
	}
}

func TestRealExitedChildIsMissing(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "0.1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	claim, err := ReadProcessClaim(cmd.Process.Pid)
	waitErr := cmd.Wait()
	if err != nil {
		t.Fatalf("ReadProcessClaim(child) error = %v", err)
	}
	if waitErr != nil {
		t.Fatalf("wait child: %v", waitErr)
	}
	if got := ClassifyProcess(claim); got != model.ProcessIdentityMissing {
		t.Fatalf("ClassifyProcess(exited child) = %v, want %v", got, model.ProcessIdentityMissing)
	}
}

func TestRealLiveChildAlteredStartTokenIsReused(t *testing.T) {
	cmd := startSleep(t)
	claim, err := ReadProcessClaim(cmd.Process.Pid)
	if err != nil {
		t.Fatalf("ReadProcessClaim(child) error = %v", err)
	}
	claim.StartToken += "-altered"
	if got := ClassifyProcess(claim); got != model.ProcessIdentityReused {
		t.Fatalf("ClassifyProcess(altered child) = %v, want %v", got, model.ProcessIdentityReused)
	}
}

func TestRealGroupLiveAndAbsent(t *testing.T) {
	cmd := startSleep(t)
	claim, err := ReadProcessClaim(cmd.Process.Pid)
	if err != nil {
		t.Fatalf("ReadProcessClaim(child) error = %v", err)
	}
	liveGroup := GroupClaim{PGID: claim.PGID, KernelDomainID: claim.KernelDomainID}
	// On a host that cannot prove process-group observation (unreadable foreign
	// /proc entries, restricted PID namespace, hidepid), ClassifyGroup returns
	// unknown and the containment code correctly fails closed. This test asserts
	// the definite observations, so it skips-with-diagnostic there rather than
	// failing. (Cannot use internal/procgroup/procgrouptest here — that would be
	// an import cycle; the same probe logic is inlined.)
	switch got := ClassifyGroup(liveGroup); got {
	case model.GroupLive:
	case model.GroupExistenceUnknown:
		t.Skipf("process-group observation unprovable: ClassifyGroup(live child group) = %v, want %v", got, model.GroupLive)
	default:
		t.Fatalf("ClassifyGroup(live child group) = %v, want %v (wrong definite answer indicates a classification regression, not a host limitation)", got, model.GroupLive)
	}

	for pgid := 2147483647; pgid > 2147483547; pgid-- {
		absentGroup := GroupClaim{PGID: pgid, KernelDomainID: claim.KernelDomainID}
		got := ClassifyGroup(absentGroup)
		if got == model.GroupAbsent {
			return
		}
		if got == model.GroupExistenceUnknown {
			t.Skipf("process-group observation unprovable: ClassifyGroup(absent candidate %d) = %v", pgid, got)
		}
	}
	t.Fatal("could not find an absent process group candidate")
}

func startSleep(t *testing.T) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("/bin/sleep", "1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start sleep: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Wait()
	})
	return cmd
}
