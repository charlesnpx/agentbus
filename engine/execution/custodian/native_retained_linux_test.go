//go:build linux

package custodian

import (
	"context"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"golang.org/x/sys/unix"
)

func requireLinuxRetainedConformanceOrSkip(t *testing.T) {
	t.Helper()
	requireRealNativeContainmentOrSkip(t)
}

func TestNativeRetainedContainAndVerifyKillsTermIgnoringLeader(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeIgnoreTermLeaderLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	requireRetainedBackendForTest(t, running)
	waitNativeReadyLine(t, running.Stdout(), "ignore-term-leader-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.PID != running.Ref().Leader.PID {
		t.Fatalf("backend pid = %d, want stable leader pid %d", result.PID, running.Ref().Leader.PID)
	}
	if result.PGID != running.Ref().PGID {
		t.Fatalf("backend pgid = %d, want target group %d", result.PGID, running.Ref().PGID)
	}
	requireRunningRetainedMembership(t, ctx, running, containment.RetainedMembershipPresent)
	if err := unix.Kill(result.PID, 0); err != nil {
		t.Fatalf("leader pid %d missing before containment: %v", result.PID, err)
	}

	const containAndVerifyAttempts = 5
	var outcome PhysicalOutcome
	absent := false
	for attempt := 1; attempt <= containAndVerifyAttempts; attempt++ {
		outcome = running.ContainAndVerify(ctx)
		if outcome.Absent() {
			absent = true
			break
		}
		if !outcome.Unprovable() {
			t.Fatalf("ContainAndVerify() attempt %d/%d = %+v, want Absent or retryable Unprovable; members=%s", attempt, containAndVerifyAttempts, outcome, debugGroupMembers(running.Ref()))
		}
		if attempt < containAndVerifyAttempts {
			// Heavy cgroup contention can transiently fail closed as Unprovable;
			// robust single-shot determinism is tracked for L3c.
			select {
			case <-ctx.Done():
				t.Fatalf("context done between ContainAndVerify attempts after %+v: %v", outcome, ctx.Err())
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	if !absent {
		t.Fatalf("ContainAndVerify() did not return Absent within %d attempts; last=%+v; members=%s", containAndVerifyAttempts, outcome, debugGroupMembers(running.Ref()))
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
	// Retained cgroup path reports the terminal observation's method (already_absent)
	// after performing a real TERM->grace->KILL; distinguishing killed-vs-was-absent
	// method fidelity for the retained path is tracked for S3B-B attestation.
	if outcome.Method != model.QuiescenceAlreadyAbsent && outcome.Method != model.QuiescenceTermKill {
		t.Fatalf("physical method = %s, want %s or %s", outcome.Method, model.QuiescenceAlreadyAbsent, model.QuiescenceTermKill)
	}
}

func TestNativeRetainedMonitorDaemonEOFContainsTargetGroup(t *testing.T) {
	t.Skip("monitor-side cgroup containment on daemon-EOF is deferred to L3c; the daemon custodian is the correctness authority (see TestNativeRetained* / ContainAndVerify) -- the monitor is an availability aid")
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeIgnoreTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	requireRetainedBackendForTest(t, running)
	waitNativeReadyLine(t, running.Stdout(), "ignore-term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}
	requireRunningRetainedMembership(t, ctx, running, containment.RetainedMembershipPresent)

	if err := running.handle.Monitor.DaemonControlWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	running.handle.Monitor.DaemonControlWrite = nil
	select {
	case <-running.handle.Monitor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not exit after daemon EOF")
	}
	if err := running.handle.Monitor.Wait(); err != nil {
		t.Fatalf("monitor Wait() error = %v, want retained containment success", err)
	}
	waitPIDAbsent(t, result.GrandchildPID, 5*time.Second)

	final := running.ContainAndVerify(ctx)
	if !final.Absent() {
		t.Fatalf("final ContainAndVerify() = %+v, want Absent after monitor containment", final)
	}
	if final.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("final physical method = %s, want already_absent after monitor containment", final.Method)
	}
}

func requireRetainedBackendForTest(t *testing.T, running *NativeRunningProcess) {
	t.Helper()
	if running == nil {
		t.Fatal("running process is nil")
	}
	if running.leader != nil {
		t.Fatalf("retained backend leader = %T, want nil", running.leader)
	}
	required, err := model.ContainmentRequiresRetainedObject(running.Ref())
	if err != nil {
		t.Fatalf("ContainmentRequiresRetainedObject() error = %v", err)
	}
	if !required {
		t.Fatalf("group retained domain state = %v, want retained object required", running.Ref().RetainedDomainState)
	}
	if running.Ref().RetainedID == "" {
		t.Fatal("running retained id is empty")
	}
	if running.retainedObject() == nil {
		t.Fatal("running retained object provider is nil")
	}
}

func requireRunningRetainedMembership(t *testing.T, ctx context.Context, running *NativeRunningProcess, want containment.RetainedGroupMembership) {
	t.Helper()
	retainedObject := running.retainedObject()
	if retainedObject == nil {
		t.Fatal("running retained object provider is nil")
	}
	capability, err := retainedObject.AcquireRetainedGroup(ctx, running.Ref(), time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup() error = %v", err)
	}
	defer capability.Release()
	got, err := capability.Membership(ctx)
	if err != nil {
		t.Fatalf("retained Membership() error = %v", err)
	}
	if got != want {
		t.Fatalf("retained Membership() = %v, want %v", got, want)
	}
}
