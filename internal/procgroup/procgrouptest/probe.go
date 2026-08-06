//go:build darwin || linux

// Package procgrouptest provides a shared test helper that skips tests which
// require provable process-group observation on hosts where the kernel cannot
// prove it. It lives in a non-test file so it can be imported from test files
// in other packages (parklaunch, cmd/agentbus, ...).
//
// The containment subsystem re-reads the kernel and maps any uncertain
// observation to "unknown", then correctly fails closed. On a constrained host
// — a sandbox with unreadable foreign /proc entries, a restricted PID
// namespace, or hidepid — procgroup.ClassifyGroup cannot prove a group is live
// or absent and returns model.GroupExistenceUnknown. Tests that assume a
// definite observation (the happy path, or an after-containment absence proof)
// cannot run meaningfully there; they should skip with a diagnostic rather than
// fail, because the fail-closed behavior itself is exercised by dedicated
// assertion tests.
package procgrouptest

import (
	"os/exec"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

// RequireProvableProcessGroupObservation skips the calling test unless the host
// can prove BOTH liveness and absence of a process group via
// procgroup.ClassifyGroup. The skip message names the specific observation that
// came back unknown so a CI run surfaces the exact host limitation.
func RequireProvableProcessGroupObservation(tb testing.TB) {
	tb.Helper()

	cmd := exec.Command("/bin/sleep", "1")
	if err := cmd.Start(); err != nil {
		tb.Fatalf("procgrouptest: start probe child: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	claim, err := procgroup.ReadProcessClaim(cmd.Process.Pid)
	if err != nil {
		tb.Skipf("process-group observation unprovable: ReadProcessClaim(probe child) error = %v", err)
	}

	live := procgroup.GroupClaim{PGID: claim.PGID, KernelDomainID: claim.KernelDomainID}
	if got := procgroup.ClassifyGroup(live); got != model.GroupLive {
		tb.Skipf("process-group observation unprovable: ClassifyGroup(live probe group) = %v "+
			"(host cannot prove group liveness; e.g. unreadable foreign /proc or restricted PID namespace)", got)
	}

	// Mirror TestRealGroupLiveAndAbsent: scan a small window of high PGIDs for a
	// provably-absent group. An unknown result means the host cannot prove
	// absence and the test must skip.
	for pgid := 2147483647; pgid > 2147483547; pgid-- {
		switch procgroup.ClassifyGroup(procgroup.GroupClaim{PGID: pgid, KernelDomainID: claim.KernelDomainID}) {
		case model.GroupAbsent:
			return // host proves both liveness and absence
		case model.GroupExistenceUnknown:
			tb.Skipf("process-group observation unprovable: ClassifyGroup(absent candidate %d) = unknown "+
				"(host cannot prove group absence)", pgid)
		}
	}
	tb.Skip("process-group observation unprovable: no provably-absent candidate pgid found")
}
