//go:build linux

package custodian

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"golang.org/x/sys/unix"
)

func requireLinuxRetainedConformanceOrSkip(t *testing.T) {
	t.Helper()
	requireRealNativeContainmentOrSkip(t)
}

func TestNewNativeRuntimeSelfTestUsesReturnedSingleLeaseInstance(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	options := nativeRuntimeOptionsForRetainedTest(t)

	runtimeBundle, err := NewNativeRuntime(options)
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v support=%+v", err, runtimeBundle.Support())
	}
	native, ok := runtimeBundle.Process().(*NativeCustodian)
	if !ok || native == nil {
		t.Fatalf("NewNativeRuntime() process = %T, want *NativeCustodian", runtimeBundle.Process())
	}
	defer cleanupNativeCustodianForTest(t, native)

	assessment := runtimeBundle.SupportAssessment()
	if assessment.Class != SupportAvailable || assessment.Attempts != 1 || !assessment.CleanupSafe {
		t.Fatalf("SupportAssessment() = %+v, want available attempts=1 cleanup-safe", assessment)
	}
	record := native.selfTest
	if !record.CleanupVerified || record.Ref == (model.GroupRef{}) {
		t.Fatalf("self-test record = %+v, want verified cleanup and retained ref", record)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		t.Fatal(err)
	}
	if record.ExecPath != exe {
		t.Fatalf("self-test exec path = %s, want current executable %s", record.ExecPath, exe)
	}
	absent, err := stableIndependentAbsent(ctx, record.Ref)
	if err != nil {
		t.Fatalf("stableIndependentAbsent(self-test ref) error = %v", err)
	}
	if !absent {
		t.Fatalf("self-test process group %d is still live", record.Ref.PGID)
	}
	if err := native.verifySelfTestRetainedAbsent(ctx, record.Ref); err != nil {
		t.Fatalf("self-test retained cgroup cleanup proof error = %v", err)
	}

	second, secondErr := NewNativeRuntime(options)
	if secondErr == nil {
		if closer, ok := second.Process().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		t.Fatal("second NewNativeRuntime() while first is open error = nil, want typed single-lease failure")
	}
	secondAssessment := second.SupportAssessment()
	if secondAssessment.Class != SupportRetryable || secondAssessment.Attempts != 1 || !secondAssessment.CleanupSafe || !errors.Is(secondAssessment.Cause, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("second SupportAssessment() = %+v err=%v, want retryable attempts=1 cleanup-safe ErrRootLeaseUnavailable cause", secondAssessment, secondErr)
	}
	if !errors.Is(secondErr, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("second NewNativeRuntime() error = %v, want ErrRootLeaseUnavailable", secondErr)
	}
	if _, ok := second.Process().(UnavailableCustodian); !ok {
		t.Fatalf("second NewNativeRuntime() process = %T, want UnavailableCustodian", second.Process())
	}

	if err := native.Close(); err != nil {
		t.Fatalf("first native Close() error = %v", err)
	}
	third, err := NewNativeRuntime(options)
	if err != nil {
		t.Fatalf("third NewNativeRuntime() after Close error = %v support=%+v", err, third.Support())
	}
	if closer, ok := third.Process().(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			t.Fatalf("third native Close() error = %v", err)
		}
	}
}

func nativeRuntimeOptionsForRetainedTest(t *testing.T) NativeOptions {
	t.Helper()
	exe := nativeTestBinaryPath(t)
	return NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    nativeMonitorCommand(t),
		ContainmentParams: defaultNativeTestParams(),
		WorkerEnv:         nativeAgentbusEnv(),
		WorkerDir:         filepath.Dir(exe),
	}
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

	// Single-shot: a live TERM-ignoring member is placed in the target cgroup, so
	// containment must prove Absent via a real TERM->grace->KILL. A fail-closed
	// Unprovable surfaces loudly with its reason rather than being masked by retry;
	// any rare transient must be root-caused (tracked: L3c real-cgroup transient).
	outcome := running.ContainPhysical(ctx)
	if !outcome.Absent() {
		t.Fatalf("ContainPhysical() = %+v, want Absent; members=%s", outcome, debugGroupMembers(running.Ref()))
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
	// Retained terminal method fidelity (term_kill vs already_absent after a
	// real kill) is tracked for L3c; H2 only requires live-before, Absent, and
	// gone-after proof.
	if outcome.Method != model.QuiescenceTermKill && outcome.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("physical method = %s, want %s or %s", outcome.Method, model.QuiescenceTermKill, model.QuiescenceAlreadyAbsent)
	}
	requireRetainedLeafGone(t, ctx, native, running.Ref())
}

func TestNativeRetainedPreparedAbortReapsLeaderBeforeContainment(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeSimpleLaunchSpec(t)

	prepared, err := native.Prepare(ctx, spec.Exec, spec.LaunchKey)
	if err != nil {
		t.Fatalf("NativeCustodian.Prepare() error = %v", err)
	}
	ref := prepared.Ref()
	requireNativeFileAbsent(t, resultPath)
	requireRetainedMembershipForRef(t, ctx, native, ref, containment.RetainedMembershipPresent)

	verified, cleanup, err := prepared.AbortAndVerify(ctx)
	if err != nil {
		t.Fatalf("prepared AbortAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("prepared AbortAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	if verified == (VerifiedQuiescence{}) {
		t.Fatal("prepared AbortAndVerify() returned zero attestation")
	}
	waitGroupAbsent(t, ref, 5*time.Second)
	requireRetainedLeafGone(t, ctx, native, ref)
}

func TestNativeRetainedMonitorDaemonEOFContainsTargetGroup(t *testing.T) {
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
	// Deliberately NO retained-membership assertion here: acquiring through the
	// daemon-side shared manager would re-establish the delegated-root flock
	// that beforeMonitorBind released, and the monitor subprocess's containment
	// acquisition on daemon EOF would then fail EAGAIN. The production lease
	// shape between monitor readiness and EOF containment is "daemon holds no
	// root flock" — the test must preserve it. Group membership is already
	// proven by the backend result file (PGID match above).

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
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
	requireRetainedLeafGone(t, ctx, native, running.Ref())
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

// requireRetainedMembershipForRef verifies leaf membership through the
// custodian's OWN shared retained manager. A fresh cgroup.New manager can
// NEVER verify while the custodian lives: per C9 the custodian holds the one
// exclusive delegated-root lease for its lifetime, so a second in-process
// manager's acquisition would always fail typed with ErrRootLeaseUnavailable.
// Process-group absence stays independently verified via kill(-pgid,0) in
// waitGroupAbsent; a fully lease-free independent leaf oracle is R6/R7A scope.
func requireRetainedMembershipForRef(t *testing.T, ctx context.Context, native *NativeCustodian, ref model.GroupRef, want containment.RetainedGroupMembership) {
	t.Helper()
	manager := native.retainedGroupSnapshot()
	if manager == nil {
		t.Fatal("custodian shared retained manager is nil")
	}
	capability, err := manager.AcquireRetainedGroup(ctx, ref, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup(%s) error = %v", ref.RetainedID, err)
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

// requireRetainedLeafGone proves leaf absence through the custodian's own
// shared retained manager (same C9 single-lease rationale as above).
func requireRetainedLeafGone(t *testing.T, ctx context.Context, native *NativeCustodian, ref model.GroupRef) {
	t.Helper()
	manager := native.retainedGroupSnapshot()
	if manager == nil {
		t.Fatal("custodian shared retained manager is nil")
	}
	if err := proveRetainedGroupAbsent(ctx, manager, ref); err != nil {
		t.Fatalf("retained leaf still exists or cannot be proven gone: %v", err)
	}
}
