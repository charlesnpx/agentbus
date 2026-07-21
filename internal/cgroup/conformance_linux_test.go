//go:build linux

package cgroup_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
)

type retainedConformanceCapability interface {
	containment.RetainedGroupCapability
	PlacePID(context.Context, int) error
	Remove(context.Context) error
}

type retainedConformanceMonitorCapability interface {
	retainedConformanceCapability
	MonitorLeafFile(context.Context) (*os.File, error)
	ReleaseRootLease() error
}

func TestCgroupV2RetainedCapabilityLifecycleConformance(t *testing.T) {
	if os.Getenv("AGENTBUS_CGROUP_CONFORMANCE") != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run real cgroup-v2 conformance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	manager, err := cgroup.New("")
	if err != nil {
		t.Skipf("cgroup.New(\"\") unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Logf("cgroup manager Close() cleanup error = %v", err)
		}
	})

	support := manager.Probe(ctx)
	if !support.Strict() {
		t.Skipf("strict cgroup-v2 support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}

	now := time.Now()
	capCreateRaw, err := manager.AcquireRetainedGroup(ctx, model.GroupRef{}, now)
	if err != nil {
		t.Fatalf("CREATE AcquireRetainedGroup(empty) error = %v", err)
	}
	if capCreateRaw == nil {
		t.Fatalf("CREATE AcquireRetainedGroup(empty) capability = nil")
	}
	capCreate, ok := capCreateRaw.(retainedConformanceCapability)
	if !ok {
		t.Fatalf("CREATE capability = %T, want retained capability with PlacePID and Remove", capCreateRaw)
	}
	t.Cleanup(func() {
		cleanupRetainedCapability(t, capCreate)
	})

	idCreate := capCreate.Identity()
	if idCreate.RetainedID == "" {
		t.Fatalf("CREATE Identity().RetainedID = empty")
	}
	if idCreate.KernelDomainID.RetainedDomainState != model.RetainedDomainKnown {
		t.Fatalf("CREATE Identity().KernelDomainID.RetainedDomainState = %v, want %v; identity=%+v", idCreate.KernelDomainID.RetainedDomainState, model.RetainedDomainKnown, idCreate)
	}
	if err := idCreate.KernelDomainID.Validate(); err != nil {
		t.Fatalf("CREATE Identity().KernelDomainID invalid: %v; identity=%+v", err, idCreate)
	}

	helper := startCgroupConformanceHelper(t)
	if err := capCreate.PlacePID(ctx, helper.pid); err != nil {
		t.Fatalf("PLACE PlacePID(%d) error = %v", helper.pid, err)
	}
	requireRetainedMembership(t, ctx, "PLACE capCreate.Membership", capCreate, containment.RetainedMembershipPresent)

	target := retainedTargetFromIdentity(idCreate, helper.pid)
	if err := target.Validate(); err != nil {
		t.Fatalf("OPEN-BY-ID target GroupRef invalid: %v; target=%+v", err, target)
	}
	capOpenRaw, err := manager.AcquireRetainedGroup(ctx, target, now)
	if err != nil {
		t.Fatalf("OPEN-BY-ID AcquireRetainedGroup(target) error = %v; target=%+v", err, target)
	}
	if capOpenRaw == nil {
		t.Fatalf("OPEN-BY-ID AcquireRetainedGroup(target) capability = nil; target=%+v", target)
	}
	capOpen, ok := capOpenRaw.(retainedConformanceCapability)
	if !ok {
		t.Fatalf("OPEN-BY-ID capability = %T, want retained capability with PlacePID and Remove", capOpenRaw)
	}
	t.Cleanup(func() {
		cleanupRetainedCapability(t, capOpen)
	})

	idOpen := capOpen.Identity()
	requireRetainedIdentityEqual(t, "IDENTITY STABILITY capOpen.Identity", idOpen, idCreate)

	requireRetainedMembership(t, ctx, "OPENED capOpen.Membership", capOpen, containment.RetainedMembershipPresent)
	held, err := capOpen.StillHeld(ctx)
	if err != nil {
		t.Fatalf("OPENED capOpen.StillHeld() error = %v", err)
	}
	if !held {
		t.Fatalf("OPENED capOpen.StillHeld() = false, want true; identity=%+v", capOpen.Identity())
	}

	termResult, err := capOpen.SignalTerm(ctx)
	if err != nil {
		t.Fatalf("TEARDOWN capOpen.SignalTerm() error = %v", err)
	}
	if termResult != containment.SignalDelivered {
		t.Fatalf("TEARDOWN capOpen.SignalTerm() = %v, want %v", termResult, containment.SignalDelivered)
	}
	helper.requireStillRunningAfter(t, "TEARDOWN after SignalTerm", 100*time.Millisecond)

	killResult, err := capOpen.Kill(ctx)
	if err != nil {
		t.Fatalf("TEARDOWN capOpen.Kill() error = %v", err)
	}
	if killResult != containment.SignalDelivered {
		t.Fatalf("TEARDOWN capOpen.Kill() = %v, want %v", killResult, containment.SignalDelivered)
	}
	if err := helper.waitForExit(2 * time.Second); err != nil {
		t.Fatalf("TEARDOWN helper pid %d did not exit after Kill(): %v", helper.pid, err)
	}
	if got, err := waitForRetainedMembership(ctx, capOpen, containment.RetainedMembershipEmpty, 2*time.Second); err != nil {
		t.Fatalf("TEARDOWN capOpen.Membership() did not reach %v: last=%v err=%v", containment.RetainedMembershipEmpty, got, err)
	}

	if err := capOpen.Remove(ctx); err != nil {
		t.Fatalf("RETIRE capOpen.Remove() error = %v", err)
	}
	if err := manager.ProveRetainedGroupAbsent(ctx, target); err != nil {
		t.Fatalf("RETIRE retained leaf still exists after Remove(): %v", err)
	}
	if err := capOpen.Release(); err != nil {
		t.Fatalf("RETIRE capOpen.Release() error = %v", err)
	}
	if err := capCreate.Release(); err != nil {
		t.Fatalf("RETIRE capCreate.Release() error = %v", err)
	}
}

func TestCgroupV2InheritedMonitorCleanupConformance(t *testing.T) {
	if os.Getenv("AGENTBUS_CGROUP_CONFORMANCE") != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run real cgroup-v2 conformance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	manager := newStrictConformanceManager(t, ctx)
	capability := acquireMonitorConformanceCapability(t, ctx, manager)
	t.Cleanup(func() {
		cleanupRetainedCapability(t, capability)
	})
	leafFile, err := capability.MonitorLeafFile(ctx)
	if err != nil {
		t.Fatalf("MonitorLeafFile() error = %v", err)
	}
	defer leafFile.Close()
	target := retainedTargetFromIdentity(capability.Identity(), os.Getpid())
	if err := capability.ReleaseRootLease(); err != nil {
		t.Fatalf("ReleaseRootLease() before inherited cleanup error = %v", err)
	}

	inheritedRaw, err := cgroup.NewInheritedRetainedGroupObjectFromLeafFD(int(leafFile.Fd())).AcquireRetainedGroup(ctx, target, time.Now())
	if err != nil {
		t.Fatalf("inherited AcquireRetainedGroup() error = %v", err)
	}
	inherited, ok := inheritedRaw.(retainedConformanceCapability)
	if !ok {
		_ = inheritedRaw.Release()
		t.Fatalf("inherited capability = %T, want retained cleanup capability", inheritedRaw)
	}
	defer inherited.Release()

	if err := inherited.Remove(ctx); err != nil {
		t.Fatalf("inherited Remove() with owner flock absent error = %v", err)
	}
	if err := manager.ProveRetainedGroupAbsent(ctx, target); err != nil {
		t.Fatalf("inherited cleanup did not retire retained leaf: %v", err)
	}
}

func TestCgroupV2InheritedMonitorCleanupDoesNotRemoveContenderReplacement(t *testing.T) {
	if os.Getenv("AGENTBUS_CGROUP_CONFORMANCE") != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run real cgroup-v2 conformance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	manager := newStrictConformanceManager(t, ctx)
	capability := acquireMonitorConformanceCapability(t, ctx, manager)
	t.Cleanup(func() {
		cleanupRetainedCapability(t, capability)
	})
	leafFile, err := capability.MonitorLeafFile(ctx)
	if err != nil {
		t.Fatalf("MonitorLeafFile() error = %v", err)
	}
	defer leafFile.Close()
	target := retainedTargetFromIdentity(capability.Identity(), os.Getpid())
	leafName, err := retainedLeafNameForConformance(target.RetainedID)
	if err != nil {
		t.Fatalf("parse retained leaf name: %v", err)
	}
	if err := capability.ReleaseRootLease(); err != nil {
		t.Fatalf("ReleaseRootLease() before contender error = %v", err)
	}

	contender, err := cgroup.New("")
	if err != nil {
		t.Fatalf("contender cgroup.New(\"\") error = %v", err)
	}
	t.Cleanup(func() {
		if err := contender.Close(); err != nil {
			t.Logf("contender Close() cleanup error = %v", err)
		}
	})
	if err := contender.HoldRootLease(ctx); err != nil {
		t.Fatalf("contender HoldRootLease() error = %v", err)
	}
	replacementPath := "/sys/fs/cgroup/" + leafName
	if err := os.Remove(replacementPath); err != nil {
		t.Fatalf("remove original retained leaf for contender replacement: %v", err)
	}
	if err := os.Mkdir(replacementPath, 0o755); err != nil {
		t.Fatalf("create contender replacement leaf: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(replacementPath)
	})

	inheritedRaw, err := cgroup.NewInheritedRetainedGroupObjectFromLeafFD(int(leafFile.Fd())).AcquireRetainedGroup(ctx, target, time.Now())
	if err != nil {
		t.Fatalf("inherited AcquireRetainedGroup() after replacement error = %v", err)
	}
	inherited, ok := inheritedRaw.(retainedConformanceCapability)
	if !ok {
		_ = inheritedRaw.Release()
		t.Fatalf("inherited capability = %T, want retained cleanup capability", inheritedRaw)
	}
	defer inherited.Release()

	err = inherited.Remove(ctx)
	if !errors.Is(err, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("inherited Remove() while contender holds root lease error = %v, want ErrRootLeaseUnavailable", err)
	}
	if !strings.Contains(err.Error(), "unleased cleanup skipped: root lease held by another owner") {
		t.Fatalf("inherited Remove() while contender holds root lease error = %q, want cleanup skip reason", err)
	}
	if _, err := os.Stat(replacementPath); err != nil {
		t.Fatalf("contender replacement stat after inherited cleanup attempt = %v, want present", err)
	}
}

func TestCgroupV2InheritedMonitorCleanupDoesNotRemoveStaleReplacement(t *testing.T) {
	if os.Getenv("AGENTBUS_CGROUP_CONFORMANCE") != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run real cgroup-v2 conformance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	manager := newStrictConformanceManager(t, ctx)
	capability := acquireMonitorConformanceCapability(t, ctx, manager)
	t.Cleanup(func() {
		cleanupRetainedCapability(t, capability)
	})
	leafFile, err := capability.MonitorLeafFile(ctx)
	if err != nil {
		t.Fatalf("MonitorLeafFile() error = %v", err)
	}
	defer leafFile.Close()
	target := retainedTargetFromIdentity(capability.Identity(), os.Getpid())
	leafName, err := retainedLeafNameForConformance(target.RetainedID)
	if err != nil {
		t.Fatalf("parse retained leaf name: %v", err)
	}
	if err := capability.ReleaseRootLease(); err != nil {
		t.Fatalf("ReleaseRootLease() before stale replacement error = %v", err)
	}

	replacementPath := "/sys/fs/cgroup/" + leafName
	if err := os.Remove(replacementPath); err != nil {
		t.Fatalf("remove original retained leaf for stale replacement: %v", err)
	}
	if err := os.Mkdir(replacementPath, 0o755); err != nil {
		t.Fatalf("create stale replacement leaf: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Remove(replacementPath)
	})

	inheritedRaw, err := cgroup.NewInheritedRetainedGroupObjectFromLeafFD(int(leafFile.Fd())).AcquireRetainedGroup(ctx, target, time.Now())
	if err != nil {
		t.Fatalf("inherited AcquireRetainedGroup() after stale replacement error = %v", err)
	}
	inherited, ok := inheritedRaw.(retainedConformanceCapability)
	if !ok {
		_ = inheritedRaw.Release()
		t.Fatalf("inherited capability = %T, want retained cleanup capability", inheritedRaw)
	}
	defer inherited.Release()

	if err := inherited.Remove(ctx); err != nil {
		t.Fatalf("inherited Remove() after stale replacement error = %v, want nil", err)
	}
	if _, err := os.Stat(replacementPath); err != nil {
		t.Fatalf("stale replacement stat after inherited cleanup attempt = %v, want present", err)
	}
}

func TestCgroupConformanceHelperProcess(t *testing.T) {
	if os.Getenv("AGENTBUS_CGROUP_CONFORMANCE_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	if ready := os.NewFile(uintptr(3), "cgroup-conformance-ready"); ready != nil {
		_, _ = ready.Write([]byte{'1'})
		_ = ready.Close()
	}
	for {
		time.Sleep(time.Hour)
	}
}

func retainedTargetFromIdentity(identity containment.RetainedGroupIdentity, pid int) model.GroupRef {
	return model.GroupRef{
		Version:   1,
		CustodyID: model.CustodyID("cgroup-conformance-custody"),
		Launch: model.LaunchKey{
			Attempt: model.AttemptRef{
				JobID:     model.JobID("cgroup-conformance-job"),
				AttemptID: model.AttemptID("cgroup-conformance-attempt"),
				Epoch:     1,
			},
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:          identity.KernelDomainID.HostBootID,
		PIDNamespaceID:      identity.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState:   identity.KernelDomainID.PIDNamespaceState,
		RetainedDomainID:    identity.KernelDomainID.RetainedDomainID,
		RetainedDomainState: identity.KernelDomainID.RetainedDomainState,
		PGID:                pid,
		Leader: model.ProcessIdentity{
			PID:               pid,
			HighResStartToken: fmt.Sprintf("cgroup-conformance-leader-%d", pid),
		},
		Monitor: model.ProcessIdentity{
			PID:               pid,
			HighResStartToken: fmt.Sprintf("cgroup-conformance-monitor-%d", pid),
		},
		RetainedID: identity.RetainedID,
	}
}

func newStrictConformanceManager(t *testing.T, ctx context.Context) *cgroup.Manager {
	t.Helper()
	manager, err := cgroup.New("")
	if err != nil {
		t.Skipf("cgroup.New(\"\") unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Logf("cgroup manager Close() cleanup error = %v", err)
		}
	})
	support := manager.Probe(ctx)
	if !support.Strict() {
		t.Skipf("strict cgroup-v2 support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}
	return manager
}

func acquireMonitorConformanceCapability(t *testing.T, ctx context.Context, manager *cgroup.Manager) retainedConformanceMonitorCapability {
	t.Helper()
	raw, err := manager.AcquireRetainedGroup(ctx, model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup(empty) error = %v", err)
	}
	capability, ok := raw.(retainedConformanceMonitorCapability)
	if !ok {
		_ = raw.Release()
		t.Fatalf("capability = %T, want retained monitor capability", raw)
	}
	return capability
}

func retainedLeafNameForConformance(retainedID string) (string, error) {
	parts := strings.Split(retainedID, ".")
	if len(parts) != 4 || parts[0] != "cg2a" {
		return "", fmt.Errorf("retained id %q does not carry cgroup identity", retainedID)
	}
	leaf, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	name := string(leaf)
	if name == "" || strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid retained leaf name %q", name)
	}
	return name, nil
}

func requireRetainedIdentityEqual(t *testing.T, label string, got, want containment.RetainedGroupIdentity) {
	t.Helper()
	if got.RetainedID == want.RetainedID &&
		got.KernelDomainID.HostBootID == want.KernelDomainID.HostBootID &&
		got.KernelDomainID.PIDNamespaceID == want.KernelDomainID.PIDNamespaceID &&
		got.KernelDomainID.PIDNamespaceState == want.KernelDomainID.PIDNamespaceState &&
		got.KernelDomainID.RetainedDomainID == want.KernelDomainID.RetainedDomainID &&
		got.KernelDomainID.RetainedDomainState == want.KernelDomainID.RetainedDomainState {
		return
	}
	t.Fatalf("%s = retainedID=%q kernelDomain=%+v, want retainedID=%q kernelDomain=%+v", label, got.RetainedID, got.KernelDomainID, want.RetainedID, want.KernelDomainID)
}

func requireRetainedMembership(t *testing.T, ctx context.Context, label string, capability retainedConformanceCapability, want containment.RetainedGroupMembership) {
	t.Helper()
	got, err := capability.Membership(ctx)
	if err != nil {
		t.Fatalf("%s error = %v; got membership=%v want %v", label, err, got, want)
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

func waitForRetainedMembership(ctx context.Context, capability retainedConformanceCapability, want containment.RetainedGroupMembership, timeout time.Duration) (containment.RetainedGroupMembership, error) {
	deadline := time.Now().Add(timeout)
	var last containment.RetainedGroupMembership
	var lastErr error
	for {
		last, lastErr = capability.Membership(ctx)
		if lastErr == nil && last == want {
			return last, nil
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("timeout after %s waiting for membership %v; last error=%v", timeout, want, lastErr)
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, ctx.Err()
		case <-timer.C:
		}
	}
}

func cleanupRetainedCapability(t *testing.T, capability retainedConformanceCapability) {
	t.Helper()
	if capability == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	membership, err := capability.Membership(ctx)
	if err == nil {
		if membership == containment.RetainedMembershipPresent {
			_, _ = capability.Kill(ctx)
			membership, _ = waitForRetainedMembership(ctx, capability, containment.RetainedMembershipEmpty, 2*time.Second)
		}
		if membership == containment.RetainedMembershipEmpty {
			_ = capability.Remove(ctx)
		}
	}
	if err := capability.Release(); err != nil {
		t.Logf("retained capability Release() cleanup error = %v", err)
	}
}

type cgroupConformanceHelper struct {
	pid  int
	done chan error
}

func startCgroupConformanceHelper(t *testing.T) *cgroupConformanceHelper {
	t.Helper()

	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("PLACE create conformance helper readiness pipe error = %v", err)
	}
	defer readyReader.Close()
	defer readyWriter.Close()

	cmd := exec.Command(os.Args[0], "-test.run=TestCgroupConformanceHelperProcess", "--")
	cmd.Env = append(os.Environ(), "AGENTBUS_CGROUP_CONFORMANCE_HELPER=1")
	cmd.ExtraFiles = []*os.File{readyWriter}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		t.Fatalf("PLACE start conformance helper error = %v", err)
	}
	readyWriter.Close()
	helper := &cgroupConformanceHelper{
		pid:  cmd.Process.Pid,
		done: make(chan error, 1),
	}
	go func() {
		helper.done <- cmd.Wait()
		close(helper.done)
	}()
	t.Cleanup(func() {
		helper.killAndWait(t)
	})
	if err := helper.waitForReady(readyReader, 2*time.Second); err != nil {
		helper.killAndWait(t)
		t.Fatalf("PLACE conformance helper readiness error = %v", err)
	}

	pgid, err := syscall.Getpgid(helper.pid)
	if err != nil {
		helper.killAndWait(t)
		t.Fatalf("PLACE helper Getpgid(%d) error = %v", helper.pid, err)
	}
	if pgid != helper.pid {
		helper.killAndWait(t)
		t.Fatalf("PLACE helper pgid = %d, want own pgid %d", pgid, helper.pid)
	}
	return helper
}

func (helper *cgroupConformanceHelper) waitForReady(reader *os.File, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ready := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := io.ReadFull(reader, buf[:])
		if err == nil && buf[0] != '1' {
			err = fmt.Errorf("unexpected readiness byte %q", buf[0])
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		return err
	case err := <-helper.done:
		return fmt.Errorf("helper pid %d exited before readiness: %v", helper.pid, err)
	case <-timer.C:
		return fmt.Errorf("helper pid %d did not report readiness after %s", helper.pid, timeout)
	}
}

func (helper *cgroupConformanceHelper) requireStillRunningAfter(t *testing.T, label string, delay time.Duration) {
	t.Helper()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case err, ok := <-helper.done:
		if !ok {
			t.Fatalf("%s: helper pid %d already reaped", label, helper.pid)
		}
		t.Fatalf("%s: helper pid %d exited early: %v", label, helper.pid, err)
	case <-timer.C:
	}
}

func (helper *cgroupConformanceHelper) waitForExit(timeout time.Duration) error {
	if helper == nil {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case _, ok := <-helper.done:
		if !ok {
			return nil
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("pid %d still running after %s", helper.pid, timeout)
	}
}

func (helper *cgroupConformanceHelper) killAndWait(t *testing.T) {
	t.Helper()
	if helper == nil || helper.pid <= 0 {
		return
	}
	_ = syscall.Kill(-helper.pid, syscall.SIGKILL)
	if err := helper.waitForExit(2 * time.Second); err != nil {
		t.Logf("conformance helper cleanup wait error = %v", err)
	}
}
