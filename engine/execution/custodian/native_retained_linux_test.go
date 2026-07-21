//go:build linux

package custodian

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestNativeCgroupRuntimeProbeContainsLiveMember(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var helperPID int
	liveBeforeContainment := false

	outcome, err := probeNativeCgroupRuntime(ctx, nativeCgroupProbeConfig{
		startHelper: func(ctx context.Context) (*nativeCgroupProbeHelper, error) {
			helper, err := startNativeCgroupProbeHelper(ctx)
			if err != nil {
				return nil, err
			}
			helperPID = helper.pid()
			return helper, nil
		},
		beforeContainment: func(ctx context.Context, helper *nativeCgroupProbeHelper) error {
			if err := helper.requireRunning(ctx); err != nil {
				return err
			}
			liveBeforeContainment = true
			return nil
		},
	})
	if err != nil {
		t.Fatalf("probeNativeCgroupRuntime() error = %v", err)
	}
	if !liveBeforeContainment {
		t.Fatal("probe helper liveness was not confirmed before containment")
	}
	if !outcome.Absent() {
		t.Fatalf("probeNativeCgroupRuntime() outcome = %+v, want Absent", outcome)
	}
	waitPIDAbsent(t, helperPID, 5*time.Second)
}

func TestNativeCgroupRuntimeProbeFailsWhenHelperExitsBeforeContainment(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "exit-before-containment")
	script := `trap '' TERM; while [ ! -f "$1" ]; do sleep 0.01; done; exit 23`

	_, err := probeNativeCgroupRuntime(ctx, nativeCgroupProbeConfig{
		startHelper: func(ctx context.Context) (*nativeCgroupProbeHelper, error) {
			return startNativeCgroupProbeHelperCommand(ctx, "/bin/sh", "-c", script, "probe-helper", marker)
		},
		beforeContainment: func(ctx context.Context, helper *nativeCgroupProbeHelper) error {
			if err := os.WriteFile(marker, []byte("exit\n"), 0600); err != nil {
				return err
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				err := helper.requireRunning(ctx)
				if err != nil {
					if strings.Contains(err.Error(), "exited") {
						return nil
					}
					return err
				}
				time.Sleep(10 * time.Millisecond)
			}
			return fmt.Errorf("helper did not self-exit before containment")
		},
	})
	if err == nil {
		t.Fatal("probeNativeCgroupRuntime() error = nil, want fail-closed helper liveness error")
	}
	if !strings.Contains(err.Error(), "live membership") || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("probeNativeCgroupRuntime() error = %v, want live-membership helper-exited failure", err)
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
	requireRetainedLeafGone(t, ctx, running.Ref())
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
	requireRetainedMembershipForRef(t, ctx, ref, containment.RetainedMembershipPresent)

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
	requireRetainedLeafGone(t, ctx, ref)
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
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
	requireRetainedLeafGone(t, ctx, running.Ref())
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

func requireRetainedMembershipForRef(t *testing.T, ctx context.Context, ref model.GroupRef, want containment.RetainedGroupMembership) {
	t.Helper()
	manager, err := cgroup.New("")
	if err != nil {
		t.Fatalf("cgroup.New(\"\") error = %v", err)
	}
	defer manager.Close()
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

func requireRetainedLeafGone(t *testing.T, ctx context.Context, ref model.GroupRef) {
	t.Helper()
	manager, err := cgroup.New("")
	if err != nil {
		t.Fatalf("cgroup.New(\"\") error = %v", err)
	}
	defer manager.Close()
	if err := manager.ProveRetainedGroupAbsent(ctx, ref); err != nil {
		t.Fatalf("retained leaf still exists or cannot be proven gone: %v", err)
	}
}
