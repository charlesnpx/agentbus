//go:build linux

package custodian

import (
	"context"
	"errors"
	"io"
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

func TestLinuxNativeContainmentBackendSelection(t *testing.T) {
	ctx := context.Background()

	t.Run("cgroup present selects retained backend", func(t *testing.T) {
		manager := newFakeNativeRetainedManager()
		native := &NativeCustodian{
			options: NativeOptions{
				newRetainedGroup: func() (containment.RetainedGroupObject, error) {
					return manager, nil
				},
			},
			running:   make(map[string]*NativeRunningProcess),
			finalized: make(map[string]*NativeRunningProcess),
		}
		cleanupNativeCustodianForTest(t, native)

		backend, err := newNativeContainmentBackend(ctx, native)
		if err != nil {
			t.Fatalf("newNativeContainmentBackend() error = %v", err)
		}
		defer func() {
			if err := backend.close(ctx); err != nil {
				t.Fatalf("backend close error = %v", err)
			}
		}()
		if _, ok := backend.(*retainedNativeContainmentBackend); !ok {
			t.Fatalf("backend = %T, want retainedNativeContainmentBackend", backend)
		}
		if backend.retainedID() == "" || backend.retainedObject() == nil {
			t.Fatalf("retained backend id/object = %q/%T, want populated retained object", backend.retainedID(), backend.retainedObject())
		}
	})

	t.Run("cgroup unsupported falls back to leader backend", func(t *testing.T) {
		native := &NativeCustodian{
			options: NativeOptions{
				newRetainedGroup: func() (containment.RetainedGroupObject, error) {
					return nil, cgroup.ErrUnsupported
				},
			},
			running:   make(map[string]*NativeRunningProcess),
			finalized: make(map[string]*NativeRunningProcess),
		}
		cleanupNativeCustodianForTest(t, native)

		backend, err := newNativeContainmentBackend(ctx, native)
		if err != nil {
			t.Fatalf("newNativeContainmentBackend() error = %v", err)
		}
		if _, ok := backend.(*leaderNativeContainmentBackend); !ok {
			t.Fatalf("backend = %T, want leaderNativeContainmentBackend", backend)
		}
		if backend.retainedID() != "" || backend.retainedObject() != nil {
			t.Fatalf("leader backend retained id/object = %q/%T, want no retained object", backend.retainedID(), backend.retainedObject())
		}
	})

	t.Run("prelaunch retained setup failure falls back to leader backend", func(t *testing.T) {
		native := &NativeCustodian{
			options: NativeOptions{
				newRetainedGroup: func() (containment.RetainedGroupObject, error) {
					return retainedSetupUnsupportedManager{}, nil
				},
			},
			running:   make(map[string]*NativeRunningProcess),
			finalized: make(map[string]*NativeRunningProcess),
		}
		cleanupNativeCustodianForTest(t, native)

		backend, err := newNativeContainmentBackend(ctx, native)
		if err != nil {
			t.Fatalf("newNativeContainmentBackend() error = %v", err)
		}
		if _, ok := backend.(*leaderNativeContainmentBackend); !ok {
			t.Fatalf("backend = %T, want leaderNativeContainmentBackend", backend)
		}
	})

	t.Run("root lease contention does not fall back", func(t *testing.T) {
		native := &NativeCustodian{
			options: NativeOptions{
				newRetainedGroup: func() (containment.RetainedGroupObject, error) {
					return nil, cgroup.ErrRootLeaseUnavailable
				},
			},
			running:   make(map[string]*NativeRunningProcess),
			finalized: make(map[string]*NativeRunningProcess),
		}
		cleanupNativeCustodianForTest(t, native)

		backend, err := newNativeContainmentBackend(ctx, native)
		if err == nil {
			t.Fatalf("newNativeContainmentBackend() error = nil backend=%T, want root lease contention", backend)
		}
		if !errors.Is(err, cgroup.ErrRootLeaseUnavailable) {
			t.Fatalf("newNativeContainmentBackend() error = %v, want ErrRootLeaseUnavailable", err)
		}
		if errors.Is(err, ErrRetainedContainmentUnavailable) && retainedContainmentFallbackAllowed(err) {
			t.Fatalf("root lease error = %v was classified fallback-allowed", err)
		}
	})
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
	defer func() {
		if err := runtimeBundle.Close(); err != nil {
			t.Fatalf("native runtime Close() error = %v", err)
		}
	}()

	support := runtimeBundle.SelfTest(ctx)
	assessment := support.Assessment
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
	secondAssessment := second.SupportAssessment()
	if !errors.Is(secondErr, cgroup.ErrRootLeaseUnavailable) {
		_ = second.Close()
		t.Fatalf("second NewNativeRuntime() while first is open error = %v, want ErrRootLeaseUnavailable", secondErr)
	}
	if secondAssessment.Class != SupportRetryable || secondAssessment.Attempts != 1 || !secondAssessment.CleanupSafe || !errors.Is(secondAssessment.Cause, cgroup.ErrRootLeaseUnavailable) {
		_ = second.Close()
		t.Fatalf("second SupportAssessment() = %+v, want construction retryable attempts=1 cleanup-safe ErrRootLeaseUnavailable cause", secondAssessment)
	}
	if _, ok := second.Process().(UnavailableCustodian); !ok {
		_ = second.Close()
		t.Fatalf("second NewNativeRuntime() process = %T, want UnavailableCustodian after construction contention", second.Process())
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second native runtime Close() error = %v", err)
	}

	if err := runtimeBundle.Close(); err != nil {
		t.Fatalf("first native runtime Close() error = %v", err)
	}
	third, err := NewNativeRuntime(options)
	if err != nil {
		t.Fatalf("third NewNativeRuntime() after Close error = %v support=%+v", err, third.Support())
	}
	thirdAssessment := third.SelfTest(ctx).Assessment
	if thirdAssessment.Class != SupportAvailable || thirdAssessment.Attempts != 1 || !thirdAssessment.CleanupSafe {
		_ = third.Close()
		t.Fatalf("third SelfTest() = %+v, want available attempts=1 cleanup-safe", thirdAssessment)
	}
	if err := third.Close(); err != nil {
		t.Fatalf("third native runtime Close() error = %v", err)
	}
}

type retainedSetupUnsupportedManager struct{}

func (retainedSetupUnsupportedManager) AcquireRetainedGroup(context.Context, model.GroupRef, time.Time) (containment.RetainedGroupCapability, error) {
	return retainedSetupUnsupportedCapability{}, nil
}

type retainedSetupUnsupportedCapability struct{}

func (retainedSetupUnsupportedCapability) Identity() containment.RetainedGroupIdentity {
	return containment.RetainedGroupIdentity{}
}

func (retainedSetupUnsupportedCapability) Membership(context.Context) (containment.RetainedGroupMembership, error) {
	return containment.RetainedMembershipUnknown, nil
}

func (retainedSetupUnsupportedCapability) StillHeld(context.Context) (bool, error) {
	return true, nil
}

func (retainedSetupUnsupportedCapability) SignalTerm(context.Context) (containment.SignalResult, error) {
	return containment.SignalUnprovable, nil
}

func (retainedSetupUnsupportedCapability) Kill(context.Context) (containment.SignalResult, error) {
	return containment.SignalUnprovable, nil
}

func (retainedSetupUnsupportedCapability) Release() error {
	return nil
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
	// The daemon may still hold the delegated-root flock here. The monitor must
	// not acquire that flock on EOF containment; it already received an inherited
	// retained leaf capability at spawn. Group membership is proven by the backend
	// result file (PGID match above).

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
	// Lifetime-lease contract: the monitor SKIPS leaf removal while the daemon
	// custodian holds the delegated-root flock (typed unleased-cleanup skip) —
	// absence is proven, but the leaf is the LEASED owner's to reap. Verify the
	// leaf survived the monitor, then run the daemon-side containment (leased
	// cleanup) and prove the leaf gone.
	requireRetainedMembershipForRef(t, ctx, native, running.Ref(), containment.RetainedMembershipEmpty)
	verified, cleanup, err := running.ContainAndVerify(ctx, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("daemon-side ContainAndVerify() after monitor EOF error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("daemon-side leased cleanup error = %v, want nil", cleanup.Err)
	}
	if verified == (VerifiedQuiescence{}) {
		t.Fatal("daemon-side ContainAndVerify() returned zero attestation")
	}
	requireRetainedLeafGone(t, ctx, native, running.Ref())
}

func TestNativeRetainedLaunchReadyWhilePriorLaunchCleanupRuns(t *testing.T) {
	requireLinuxRetainedConformanceOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	specA, _ := nativeSimpleLaunchSpec(t)

	runningA, err := native.Launch(ctx, specA)
	if err != nil {
		t.Fatalf("launch A error = %v", err)
	}
	defer cleanupNativeRunning(t, runningA)
	if _, err := io.ReadAll(runningA.Stdout()); err != nil {
		t.Fatalf("read launch A stdout: %v", err)
	}

	delayPath := filepath.Join(t.TempDir(), "hold-monitor-ready")
	enteredPath := delayPath + ".entered"
	releasePath := delayPath + ".release"
	if err := unix.Mkfifo(enteredPath, 0o600); err != nil {
		t.Fatalf("create monitor entered fifo: %v", err)
	}
	if err := unix.Mkfifo(releasePath, 0o600); err != nil {
		t.Fatalf("create monitor release fifo: %v", err)
	}
	native.options.MonitorCommand.Env = append(native.options.MonitorCommand.Env, nativeHelperMonitorDelayReady+"="+delayPath)
	specB, _ := nativeSimpleLaunchSpec(t)
	type prepareResult struct {
		prepared PreparedProcess
		err      error
	}
	preparedDone := make(chan prepareResult, 1)
	go func() {
		prepared, err := native.Prepare(ctx, specB.Exec, specB.LaunchKey)
		preparedDone <- prepareResult{prepared: prepared, err: err}
	}()
	waitNativeMonitorDelayEntered(t, enteredPath)

	select {
	case result := <-preparedDone:
		t.Fatalf("launch B prepare completed before readiness delay released: prepared=%T err=%v", result.prepared, result.err)
	default:
	}
	if _, err := runningA.Wait(ctx); err != nil {
		t.Fatalf("launch A Wait() during launch B bind-before-ready window error = %v", err)
	}
	requireRetainedLeafGone(t, ctx, native, runningA.Ref())
	releaseNativeMonitorDelay(t, releasePath)

	var result prepareResult
	select {
	case result = <-preparedDone:
	case <-ctx.Done():
		t.Fatalf("launch B prepare did not complete after readiness delay released: %v", ctx.Err())
	}
	if result.err != nil {
		t.Fatalf("launch B prepare error = %v", result.err)
	}
	if result.prepared == nil {
		t.Fatal("launch B prepare returned nil prepared process")
	}
	refB := result.prepared.Ref()
	if err := refB.Validate(); err != nil {
		t.Fatalf("launch B prepared ref invalid: %v", err)
	}
	verified, cleanup, err := result.prepared.AbortAndVerify(ctx)
	if err != nil {
		t.Fatalf("launch B AbortAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("launch B AbortAndVerify() cleanup error = %v", cleanup.Err)
	}
	if verified == (VerifiedQuiescence{}) {
		t.Fatal("launch B AbortAndVerify() returned zero attestation")
	}
	requireRetainedLeafGone(t, ctx, native, refB)
}

func waitNativeMonitorDelayEntered(t *testing.T, path string) {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatalf("open monitor entered fifo: %v", err)
	}
	defer unix.Close(fd)
	deadline := time.Now().Add(5 * time.Second)
	var buf [1]byte
	for {
		n, err := unix.Read(fd, buf[:])
		if n == 1 {
			if buf[0] != '1' {
				t.Fatalf("monitor entered fifo byte = %q, want '1'", buf[0])
			}
			return
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			t.Fatalf("read monitor entered fifo: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for monitor entered fifo %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func releaseNativeMonitorDelay(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for {
		fd, err := unix.Open(path, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err == nil {
			_, writeErr := unix.Write(fd, []byte{'1'})
			_ = unix.Close(fd)
			if writeErr != nil {
				t.Fatalf("write monitor release fifo: %v", writeErr)
			}
			return
		}
		lastErr = err
		if !errors.Is(err, unix.ENXIO) && !errors.Is(err, unix.EINTR) {
			t.Fatalf("open monitor release fifo: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out opening monitor release fifo %s: %v", path, lastErr)
		}
		time.Sleep(10 * time.Millisecond)
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
