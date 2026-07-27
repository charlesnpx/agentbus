//go:build darwin || linux

package custodian

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

const (
	nativeHelperEnv                  = "AGENTBUS_NATIVE_CUSTODIAN_HELPER"
	nativeHelperSimple               = "simple"
	nativeHelperTermGrandchild       = "term-grandchild"
	nativeHelperIgnoreTermGrandchild = "ignore-term-grandchild"
	nativeHelperIgnoreTermLeader     = "ignore-term-leader"
	nativeHelperGrandchild           = "grandchild"
	nativeHelperMonitor              = "monitor"
	nativeHelperOfflineModcacheEnv   = "AGENTBUS_OFFLINE_MODCACHE"
	nativeHelperAgentbusGOFLAGS      = "GOFLAGS=-mod=mod"
	nativeHelperAgentbusGOPROXY      = "GOPROXY=off"
	nativeHelperRetainedNoopMonitor  = "AGENTBUS_NATIVE_RETAINED_NOOP_MONITOR"
	nativeHelperMonitorDelayReady    = "AGENTBUS_NATIVE_MONITOR_DELAY_READY_PATH"
	nativeCgroupConformanceEnv       = "AGENTBUS_CGROUP_CONFORMANCE"
)

var (
	nativeAgentbusBuildOnce sync.Once
	nativeAgentbusBuildPath string
	nativeAgentbusBuildErr  error
)

type countingQuiescenceIssuer struct {
	inner AttestationIssuer
	count atomic.Int32
}

func (issuer *countingQuiescenceIssuer) AttestQuiescence(quiescence PhysicalQuiescence) (VerifiedQuiescence, error) {
	issuer.count.Add(1)
	return issuer.inner.AttestQuiescence(quiescence)
}

func requireRealNativeContainmentOrSkip(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return
	}
	if os.Getenv(nativeCgroupConformanceEnv) == "1" {
		return
	}
	t.Skip("real cgroup conformance deferred to the privileged-container unit; set AGENTBUS_CGROUP_CONFORMANCE=1 to run")
}

func requireLeaderRetentionModelOrSkip(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("leader-retention model is Darwin-only; Linux retained cgroup backend has no leader retention")
	}
	requireRealNativeContainmentOrSkip(t)
}

func TestNativeHelperSimpleProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperSimple {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeSimpleHelper(args))
}

func TestNativeHelperTermGrandchildProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperTermGrandchild {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeTermGrandchildHelper(args))
}

func TestNativeHelperIgnoreTermGrandchildProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperIgnoreTermGrandchild {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeIgnoreTermGrandchildHelper(args))
}

func TestNativeHelperIgnoreTermLeaderProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperIgnoreTermLeader {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeIgnoreTermLeaderHelper(args))
}

func TestNativeHelperGrandchildProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperGrandchild {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeGrandchildHelper(args))
}

func TestNativeHelperMonitorProcess(t *testing.T) {
	if os.Getenv(nativeHelperEnv) != nativeHelperMonitor {
		return
	}
	args, ok := nativeHelperArgs()
	if !ok {
		os.Exit(97)
	}
	os.Exit(runNativeMonitorHelper(args))
}

func TestNativeCustodianLaunchesParkedBackendAndObservesExit(t *testing.T) {
	requireRealNativeContainmentOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)

	stdout, err := io.ReadAll(running.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if !strings.Contains(string(stdout), "native-simple") {
		t.Fatalf("stdout = %q, want native-simple marker", stdout)
	}
	exit, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean exit", exit)
	}
	result := readNativeBackendResult(t, resultPath)
	if result.PID != running.Ref().Leader.PID {
		t.Fatalf("backend pid = %d, want stable leader pid %d", result.PID, running.Ref().Leader.PID)
	}
	if result.PGID != running.Ref().PGID {
		t.Fatalf("backend pgid = %d, want group %d", result.PGID, running.Ref().PGID)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeWaitReturnsCachedResultAfterFinalization(t *testing.T) {
	requireRealNativeContainmentOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}

	first, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("first Wait() error = %v", err)
	}
	second, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("second Wait() error = %v, want cached nil error", err)
	}
	if second != first {
		t.Fatalf("second Wait() exit = %+v, want cached %+v", second, first)
	}
}

func TestNativeWaitIgnoresCallerClosedStreamsAndSharesFinalizedOutcome(t *testing.T) {
	requireRealNativeContainmentOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	closeNativeExposedStreamsForTest(t, running)

	exit, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v, want nil after caller-closed streams", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean exit", exit)
	}
	second, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("cached Wait() error = %v, want nil", err)
	}
	if second != exit {
		t.Fatalf("cached Wait() exit = %+v, want %+v", second, exit)
	}
	cached := running.finalOutcome
	if !cached.Absent() || cached.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("cached physical outcome = %+v, want already-absent", cached)
	}
	canceled, cancelContain := context.WithCancel(context.Background())
	cancelContain()
	if got := running.ContainPhysical(canceled); got != cached {
		t.Fatalf("ContainAndVerify(canceled) after Wait = %+v, want cached %+v", got, cached)
	}
}

func TestNativeContainAndVerifyKillsTermIgnoringGrandchild(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}

	outcome := running.ContainPhysical(ctx)
	if !outcome.Absent() {
		onlyLeader, onlyLeaderErr := groupHasNoMembersExceptLeader(running.Ref())
		t.Fatalf("ContainAndVerify() = %+v, want Absent; unreaped=%t onlyLeader=%t onlyLeaderErr=%v members=%s", outcome, running.leader.unreapedFor(running.Ref()), onlyLeader, onlyLeaderErr, debugGroupMembers(running.Ref()))
	}
	if outcome.Method != model.QuiescenceTermKill {
		t.Fatalf("physical method = %s, want term_kill", outcome.Method)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeContainAndVerifyIgnoresCallerClosedStreamsAndSharesFinalizedOutcome(t *testing.T) {
	requireRealNativeContainmentOrSkip(t)
	if runtime.GOOS == "linux" {
		t.Skip("this test asserts the term_kill physical method, whose robust fixture staging on the cgroup path (keeping a TERM-ignoring member live through containment) is deferred to L3c; the caller-closed-streams / shared-finalized-outcome behavior is covered on Linux by TestNativeRetained* and the outcomes observed here are always safe (Absent or Unprovable, never a false absence)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}
	closeNativeExposedStreamsForTest(t, running)

	outcome := running.ContainPhysical(ctx)
	if !outcome.Absent() {
		t.Fatalf("ContainAndVerify() = %+v, want Absent after caller-closed streams", outcome)
	}
	if outcome.Method != model.QuiescenceTermKill {
		t.Fatalf("physical method = %s, want term_kill", outcome.Method)
	}
	if running.finalOutcome != outcome {
		t.Fatalf("cached physical outcome = %+v, want returned %+v", running.finalOutcome, outcome)
	}
	canceled, cancelContain := context.WithCancel(context.Background())
	cancelContain()
	if got := running.ContainPhysical(canceled); got != outcome {
		t.Fatalf("ContainAndVerify(canceled) after finalization = %+v, want cached %+v", got, outcome)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeZombieOnlyGroupIsUnprovableUntilOwnerReaps(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	params := defaultNativeTestParams()
	params.PollTimeout = 100 * time.Millisecond
	params.PollInterval = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, params)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	if running.handle.LeaderReaped() {
		t.Fatal("leader was reaped before owner finalization")
	}

	outcome := containPhysical(ctx, running.Ref(), params, running.leader, nil)
	if outcome.Absent() {
		t.Fatalf("containPhysical zombie-only outcome = %+v, want Unprovable before owner reap", outcome)
	}
	if outcome.Reason != containment.ReasonAbsenceDeadlineExceeded {
		t.Fatalf("containPhysical zombie-only reason = %s, want %s", outcome.Reason, containment.ReasonAbsenceDeadlineExceeded)
	}
	final := running.ContainPhysical(ctx)
	if !final.Absent() {
		t.Fatalf("ContainAndVerify() after owner reap = %+v, want Absent", final)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeWaitAndContainShareSerializedFinalization(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	issuer, verifier := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	native.issuer = counting
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}

	waitDone := make(chan command.ExitObservation, 1)
	waitVerified := make(chan VerifiedQuiescence, 1)
	waitCleanup := make(chan CleanupStatus, 1)
	waitErr := make(chan error, 1)
	go func() {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer waitCancel()
		exit, verified, cleanup, err := running.WaitAndVerify(waitCtx)
		waitDone <- exit
		waitVerified <- verified
		waitCleanup <- cleanup
		waitErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if running.handle.LeaderReaped() {
		t.Fatal("Wait reaped leader while residual group members remained")
	}
	verified, cleanup, err := running.ContainAndVerify(ctx, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("ContainAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("ContainAndVerify() verifier error = %v", err)
	}
	if !payload.Group.Equal(running.Ref()) || payload.Method != model.QuiescenceTermKill {
		t.Fatalf("ContainAndVerify() payload = %+v, want term_kill for %+v", payload, running.Ref())
	}
	if !running.finalOutcome.Absent() {
		t.Fatalf("final physical outcome = %+v, want Absent", running.finalOutcome)
	}
	exit := <-waitDone
	if cleanup := <-waitCleanup; cleanup.Err != nil {
		t.Fatalf("WaitAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	waitPayload, err := verifier.VerifyQuiescence(<-waitVerified)
	if err != nil {
		t.Fatalf("WaitAndVerify() verifier error = %v", err)
	}
	if !waitPayload.Group.Equal(running.Ref()) || waitPayload.Method != model.QuiescenceTermKill {
		t.Fatalf("WaitAndVerify() payload = %+v, want term_kill for %+v", waitPayload, running.Ref())
	}
	err = <-waitErr
	if exit.Signal == "" && !exit.Exited {
		t.Fatalf("Wait exit observation = %+v, want cached leader exit", exit)
	}
	if err != nil {
		t.Fatalf("Wait() error = %v, want nil for TERM-handled helper", err)
	}
	if got := counting.count.Load(); got != 1 {
		t.Fatalf("AttestQuiescence calls = %d, want 1", got)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeWaitAndVerifyContainsResidualGroupAfterLeaderExit(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	issuer, verifier := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	native.issuer = counting
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}

	exit, verified, cleanup, err := running.WaitAndVerify(ctx)
	if err != nil {
		t.Fatalf("WaitAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("WaitAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("WaitAndVerify() verifier error = %v", err)
	}
	if !payload.Group.Equal(running.Ref()) || payload.Method != model.QuiescenceTermKill {
		t.Fatalf("WaitAndVerify() payload = %+v, want term_kill for %+v", payload, running.Ref())
	}
	if exit.Signal == "" && !exit.Exited {
		t.Fatalf("WaitAndVerify() exit observation = %+v, want leader exit", exit)
	}
	if !running.WaitContained() {
		t.Fatal("WaitContained() = false, want true after wait-driven containment")
	}
	if got := counting.count.Load(); got != 1 {
		t.Fatalf("AttestQuiescence calls = %d, want 1", got)
	}
	waitGroupAbsent(t, running.Ref(), 5*time.Second)
}

func TestNativeContainAndVerifyUnprovableWhenLeaderReapedOutOfBand(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPGID != running.Ref().PGID {
		t.Fatalf("grandchild pgid = %d, want target group %d", result.GrandchildPGID, running.Ref().PGID)
	}

	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	state, err := running.handle.WaitState()
	if err != nil {
		t.Fatalf("reap leader WaitState() error = %v", err)
	}
	exit := exitObservationForState(state)
	if !exit.Exited {
		t.Fatalf("leader exit observation = %+v, want exited", exit)
	}
	outcome := running.ContainPhysical(ctx)
	if !outcome.Unprovable() {
		t.Fatalf("ContainAndVerify() = %+v, want Unprovable after reaping leader", outcome)
	}
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d was killed after unprovable outcome: %v", result.GrandchildPID, err)
	}
}

func TestNativeMonitorDaemonEOFContainsTermIgnoringGrandchild(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeIgnoreTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "ignore-term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}

	if err := running.handle.Monitor.DaemonControlWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	running.handle.Monitor.DaemonControlWrite = nil
	select {
	case <-running.handle.Monitor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not exit after daemon EOF")
	}
	_ = running.handle.Monitor.Wait()
	waitPIDAbsent(t, result.GrandchildPID, 5*time.Second)
	final := running.ContainPhysical(ctx)
	if !final.Absent() {
		t.Fatalf("final ContainAndVerify() = %+v, want Absent after monitor containment", final)
	}
}

func TestNativeMonitorDaemonEOFDoesNotKillAfterLeaderReaped(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	if _, err := running.handle.WaitState(); err != nil {
		t.Fatalf("reap leader WaitState() error = %v", err)
	}
	waitLeaderNotMatching(t, running.Ref(), 5*time.Second)
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d missing before monitor EOF: %v", result.GrandchildPID, err)
	}

	if err := running.handle.Monitor.DaemonControlWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	running.handle.Monitor.DaemonControlWrite = nil
	select {
	case <-running.handle.Monitor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not exit after daemon EOF")
	}
	if err := running.handle.Monitor.Wait(); err == nil {
		t.Fatal("monitor Wait() error = nil, want unprovable containment failure")
	}
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d was killed after unprovable monitor outcome: %v", result.GrandchildPID, err)
	}
}

func TestNativeMonitorDaemonEOFDoesNotKillUnreapedZombieLeader(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	result := readNativeBackendResult(t, resultPath)
	if result.GrandchildPID <= 0 {
		t.Fatalf("grandchild pid = %d, want positive", result.GrandchildPID)
	}
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	if running.handle.LeaderReaped() {
		t.Fatal("leader was reaped before monitor containment")
	}
	leader := waitLeaderRunState(t, running.Ref(), procgroup.ProcessRunStateZombie, 5*time.Second)
	if leader.Identity != model.ProcessIdentityMatching {
		t.Fatalf("leader identity before monitor EOF = %s, want matching", leader.Identity)
	}
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d missing before monitor EOF: %v", result.GrandchildPID, err)
	}

	if err := running.handle.Monitor.DaemonControlWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	running.handle.Monitor.DaemonControlWrite = nil
	select {
	case <-running.handle.Monitor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("monitor did not exit after daemon EOF")
	}
	if err := running.handle.Monitor.Wait(); err == nil {
		t.Fatal("monitor Wait() error = nil, want unprovable containment failure")
	}
	if err := unix.Kill(result.GrandchildPID, 0); err != nil {
		t.Fatalf("grandchild pid %d was killed after unreaped-zombie monitor outcome: %v", result.GrandchildPID, err)
	}
	if running.handle.LeaderReaped() {
		t.Fatal("non-parent monitor reaped the leader")
	}
}

func TestNativePreReleaseHandleFailureAbortsUnreleasedWorker(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	native.options.newLeaderRetention = func(model.GroupRef) (*leaderRetention, error) {
		return nil, errors.New("injected leader handle failure")
	}
	spec, resultPath := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err == nil {
		cleanupNativeRunning(t, running)
		t.Fatal("Launch() succeeded, want pre-release handle failure")
	}
	if !strings.Contains(err.Error(), "injected leader handle failure") {
		t.Fatalf("Launch() error = %v, want injected handle failure", err)
	}
	if _, statErr := os.Stat(resultPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("backend result stat = %v, want not exist because release never happened", statErr)
	}
}

func TestNativeCanceledWaitsDoNotLeakReapers(t *testing.T) {
	requireLeaderRetentionModelOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	native := newNativeCustodianForTest(t, defaultNativeTestParams())
	spec, resultPath := nativeTermGrandchildLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")
	_ = readNativeBackendResult(t, resultPath)
	before := runtime.NumGoroutine()
	if err := unix.Kill(running.Ref().Leader.PID, unix.SIGTERM); err != nil {
		t.Fatalf("signal leader TERM: %v", err)
	}
	if err := running.leader.waitExited(ctx); err != nil {
		t.Fatalf("wait leader exit notification: %v", err)
	}
	for i := 0; i < 20; i++ {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _ = running.Wait(waitCtx)
		waitCancel()
	}
	time.Sleep(100 * time.Millisecond)
	if running.handle.LeaderReaped() {
		t.Fatal("canceled Wait reaped leader later")
	}
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("goroutines before=%d after=%d, want no repeated wait leak", before, after)
	}
	outcome := running.ContainPhysical(ctx)
	if !outcome.Absent() {
		t.Fatalf("ContainAndVerify() = %+v, want Absent after canceled waits", outcome)
	}
}

func TestNativeContainAndVerifyUnprovableWhenBoundsExpire(t *testing.T) {
	requireRealNativeContainmentOrSkip(t)
	params := defaultNativeTestParams()
	params.GracePeriod = 200 * time.Millisecond
	native := newNativeCustodianForTest(t, params)
	spec, _ := nativeTermGrandchildLaunchSpec(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "term-grandchild-ready")

	containCtx, containCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer containCancel()
	outcome := running.ContainPhysical(containCtx)
	if !outcome.Unprovable() {
		t.Fatalf("ContainAndVerify() = %+v, want Unprovable when bounds expire", outcome)
	}
	if outcome.Reason != containment.ReasonContextDone {
		t.Fatalf("ContainAndVerify() reason = %s, want %s", outcome.Reason, containment.ReasonContextDone)
	}
}

func TestAttestationBridgeMintsOnlyFromProvenAbsentPhysicalOutcome(t *testing.T) {
	issuer, verifier := NewAttestationChannel()
	quiescence := testPhysicalQuiescence()
	outcome := PhysicalOutcome{
		Kind:     PhysicalOutcomeAbsent,
		Group:    quiescence.Group,
		Method:   model.QuiescenceTermKill,
		Decision: model.SignalDirectly,
	}

	verified, cleanup, err := attestPhysicalOutcome(issuer, outcome)
	if err != nil {
		t.Fatalf("attestPhysicalOutcome(absent) error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("attestPhysicalOutcome(absent) cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("paired VerifyQuiescence() error = %v", err)
	}
	if !payload.Group.Equal(outcome.Group) || payload.Method != outcome.Method {
		t.Fatalf("verified payload = %+v, want group=%+v method=%s", payload, outcome.Group, outcome.Method)
	}
	_, differentVerifier := NewAttestationChannel()
	if _, err := differentVerifier.VerifyQuiescence(verified); !errors.Is(err, ErrInvalidAttestation) {
		t.Fatalf("different-channel VerifyQuiescence() error = %v, want ErrInvalidAttestation", err)
	}

	physicalErr := errors.New("physical proof unavailable")
	unprovable := PhysicalOutcome{
		Kind:     PhysicalOutcomeUnprovable,
		Group:    quiescence.Group,
		Reason:   containment.ReasonProbeUnprovable,
		Decision: model.Unprovable,
		Err:      physicalErr,
	}
	unverified, cleanup, err := attestPhysicalOutcome(issuer, unprovable)
	if !errors.Is(err, physicalErr) {
		t.Fatalf("attestPhysicalOutcome(unprovable) error = %v, want physical error", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("attestPhysicalOutcome(unprovable) cleanup error = %v, want nil", cleanup.Err)
	}
	if unverified != (VerifiedQuiescence{}) {
		t.Fatalf("attestPhysicalOutcome(unprovable) returned attestation %+v, want zero", unverified)
	}
	if _, err := verifier.VerifyQuiescence(unverified); !errors.Is(err, ErrInvalidAttestation) {
		t.Fatalf("VerifyQuiescence(unprovable result) error = %v, want ErrInvalidAttestation", err)
	}
}

func TestWaitBeforeProbeSignalerStartsWaitExactlyAtFirstProbeAfterSignal(t *testing.T) {
	target := testPhysicalQuiescence().Group
	events := make([]string, 0, 4)
	inner := &recordingWaitBeforeProbeSignaler{events: &events}
	signaler := &waitBeforeProbeSignaler{
		inner: inner,
		beforeProbe: func() {
			events = append(events, "start_wait")
		},
	}

	if _, err := signaler.SignalGroup(context.Background(), target, containment.SignalTerminate); err != nil {
		t.Fatalf("SignalGroup() error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"signal"}) {
		t.Fatalf("events after SignalGroup = %#v, want signal only", events)
	}
	if _, err := signaler.ProbeGroup(context.Background(), target); err != nil {
		t.Fatalf("ProbeGroup() error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"signal", "start_wait", "probe"}) {
		t.Fatalf("events after first ProbeGroup = %#v, want signal/start_wait/probe", events)
	}
	if _, err := signaler.ProbeGroup(context.Background(), target); err != nil {
		t.Fatalf("second ProbeGroup() error = %v", err)
	}
	if !reflect.DeepEqual(events, []string{"signal", "start_wait", "probe", "probe"}) {
		t.Fatalf("events after second ProbeGroup = %#v, want start_wait exactly once", events)
	}
}

type recordingWaitBeforeProbeSignaler struct {
	events *[]string
}

func (signaler *recordingWaitBeforeProbeSignaler) SignalGroup(context.Context, model.GroupRef, containment.Signal) (containment.SignalResult, error) {
	*signaler.events = append(*signaler.events, "signal")
	return containment.SignalDelivered, nil
}

func (signaler *recordingWaitBeforeProbeSignaler) ProbeGroup(context.Context, model.GroupRef) (containment.ProbeResult, error) {
	*signaler.events = append(*signaler.events, "probe")
	return containment.ProbeAbsent, nil
}

func TestFinalAttestationSerializedOnce(t *testing.T) {
	issuer, verifier := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	quiescence := testPhysicalQuiescence()
	process := &NativeRunningProcess{
		custodian: &NativeCustodian{issuer: counting},
		finalized: true,
		finalOutcome: PhysicalOutcome{
			Kind:     PhysicalOutcomeAbsent,
			Group:    quiescence.Group,
			Method:   model.QuiescenceAlreadyAbsent,
			Decision: model.AlreadyAbsent,
		},
	}

	const callers = 32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			process.lifecycleMu.Lock()
			verified, cleanup, err := process.finalAttestationLocked()
			process.lifecycleMu.Unlock()
			if err != nil {
				errs <- err
				return
			}
			if cleanup.Err != nil {
				errs <- fmt.Errorf("cleanup error = %v", cleanup.Err)
				return
			}
			payload, err := verifier.VerifyQuiescence(verified)
			if err != nil {
				errs <- err
				return
			}
			if !payload.Group.Equal(quiescence.Group) || payload.Method != model.QuiescenceAlreadyAbsent {
				errs <- fmt.Errorf("payload = %+v", payload)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := counting.count.Load(); got != 1 {
		t.Fatalf("AttestQuiescence calls = %d, want 1", got)
	}
}

func TestCustodianContainAndVerifyUsesFinalizedCacheAfterRunningEviction(t *testing.T) {
	issuer, verifier := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	quiescence := testPhysicalQuiescence()
	native := &NativeCustodian{
		issuer:    counting,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}
	process := &NativeRunningProcess{
		custodian: native,
		group:     quiescence.Group,
	}
	native.running[groupKey(quiescence.Group)] = process
	if got := native.ActiveCustodyCount(); got != 1 {
		t.Fatalf("ActiveCustodyCount() with running process = %d, want 1", got)
	}
	runtimeBundle := Runtime{process: native}
	if got := runtimeBundle.ActiveCustodyCount(); got != 1 {
		t.Fatalf("Runtime ActiveCustodyCount() with native process = %d, want 1", got)
	}
	finalOutcome := PhysicalOutcome{
		Kind:     PhysicalOutcomeAbsent,
		Group:    quiescence.Group,
		Method:   model.QuiescenceTermKill,
		Decision: model.SignalDirectly,
	}

	process.lifecycleMu.Lock()
	cached, err := process.cacheFinalLocked(context.Background(), finalOutcome, command.ExitObservation{}, nil)
	if err != nil {
		process.lifecycleMu.Unlock()
		t.Fatalf("cacheFinalLocked() error = %v", err)
	}
	first, cleanup, err := process.finalAttestationLocked()
	process.lifecycleMu.Unlock()
	if err != nil {
		t.Fatalf("finalAttestationLocked() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("finalAttestationLocked() cleanup error = %v, want nil", cleanup.Err)
	}
	if cached != finalOutcome {
		t.Fatalf("cached outcome = %+v, want %+v", cached, finalOutcome)
	}
	if native.lookup(quiescence.Group) != nil {
		t.Fatal("running lookup returned process after finalization, want eviction")
	}
	if got := native.ActiveCustodyCount(); got != 0 {
		t.Fatalf("ActiveCustodyCount() after finalized eviction = %d, want 0", got)
	}

	second, cleanup, err := native.ContainAndVerify(context.Background(), quiescence.Group, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("custodian ContainAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("custodian ContainAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	if second != first {
		t.Fatalf("custodian ContainAndVerify() attestation = %+v, want cached %+v", second, first)
	}
	payload, err := verifier.VerifyQuiescence(second)
	if err != nil {
		t.Fatalf("VerifyQuiescence() error = %v", err)
	}
	if !payload.Group.Equal(quiescence.Group) || payload.Method != model.QuiescenceTermKill {
		t.Fatalf("payload = %+v, want group=%+v method=%s", payload, quiescence.Group, model.QuiescenceTermKill)
	}
	if got := counting.count.Load(); got != 1 {
		t.Fatalf("AttestQuiescence calls = %d, want 1", got)
	}
}

func TestNativeCustodianDoesNotMintProofAndProductionUnavailable(t *testing.T) {
	nativeType := fmt.Sprintf("%T", PhysicalOutcome{})
	if strings.Contains(nativeType, "VerifiedQuiescence") {
		t.Fatalf("physical outcome type = %s, must not be VerifiedQuiescence", nativeType)
	}

	runtime := NewUnavailableRuntime(ErrSupervisorUnavailable)
	if _, ok := runtime.Process().(UnavailableCustodian); !ok {
		t.Fatalf("production runtime process = %T, want UnavailableCustodian", runtime.Process())
	}
	if runtime.Support().VerifiedContainment || runtime.Support().AdvertisedAvailable() {
		t.Fatalf("production runtime support = %+v, want unavailable", runtime.Support())
	}
	verified, cleanup, err := runtime.Process().ContainAndVerify(context.Background(), testPhysicalQuiescence().Group, QuiescenceCauseContain)
	if !errors.Is(err, ErrSupervisorUnavailable) {
		t.Fatalf("production ContainAndVerify() error = %v, want ErrSupervisorUnavailable", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("production ContainAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	if verified != (VerifiedQuiescence{}) {
		t.Fatalf("production ContainAndVerify() verified = %+v, want zero", verified)
	}
	if _, err := runtime.Verifier().VerifyQuiescence(verified); !errors.Is(err, ErrInvalidAttestation) {
		t.Fatalf("production VerifyQuiescence() error = %v, want ErrInvalidAttestation", err)
	}
}

func TestUnavailableRuntimeSelfTestReturnsOriginalSupport(t *testing.T) {
	reason := errors.New("sentinel unavailable runtime")
	runtime := NewUnavailableRuntime(reason)
	before := runtime.Support()
	after := runtime.SelfTest(context.Background())
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("SelfTest support = %+v, want original %+v", after, before)
	}
	if current := runtime.Support(); !reflect.DeepEqual(current, before) {
		t.Fatalf("Support after SelfTest = %+v, want original %+v", current, before)
	}
	if before.ImplementationCompiled {
		t.Fatalf("ImplementationCompiled = true, want unavailable sentinel to remain uncompiled")
	}
}

func TestNewNativeRuntimeProbeExercisesContainmentButDoesNotAdvertise(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("C6 native runtime availability is Linux cgroup-v2 only")
	}
	if runtime.GOOS == "linux" && os.Getenv(nativeCgroupConformanceEnv) != "1" {
		t.Skip("set AGENTBUS_CGROUP_CONFORMANCE=1 to run the real Linux cgroup native runtime probe")
	}
	requireRealNativeContainmentOrSkip(t)
	exe := nativeTestBinaryPath(t)
	options := NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    nativeMonitorCommand(t),
		ContainmentParams: defaultNativeTestParams(),
		WorkerEnv:         nativeAgentbusEnv(),
		WorkerDir:         filepath.Dir(exe),
	}

	// Single-shot: the real cgroup self-test probe must pass on the first attempt.
	// A fail-closed probe result (RuntimeProbePassed=false) surfaces loudly with its
	// reason rather than being masked by retry; any rare transient must be
	// root-caused (tracked: L3c real-cgroup transient investigation).
	runtimeBundle, err := NewNativeRuntime(options)
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}
	if initial := runtimeBundle.Support(); !errors.Is(initial.Reason, ErrNativeRuntimeSelfTestRequired) || initial.RuntimeProbePassed {
		t.Fatalf("initial native runtime support = %+v, want explicit self-test required", initial)
	}
	support := runtimeBundle.SelfTest(context.Background())
	if support.Assessment.Class != SupportAvailable || !support.RuntimeProbePassed || !support.VerifiedContainment || support.RuntimeProbeResult != nil {
		t.Fatalf("native runtime support = %+v, want passed containment probe; NewNativeRuntime error = %v", support, err)
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
	if support.FeatureConfigured || support.FeatureAdvertised || support.AdvertisedAvailable() {
		t.Fatalf("native runtime support = %+v, want capability off/not advertised", support)
	}
	quiescence := testPhysicalQuiescence()
	verified, cleanup, err := attestPhysicalOutcome(native.issuer, PhysicalOutcome{
		Kind:     PhysicalOutcomeAbsent,
		Group:    quiescence.Group,
		Method:   model.QuiescenceAlreadyAbsent,
		Decision: model.AlreadyAbsent,
	})
	if err != nil {
		t.Fatalf("native runtime issuer attest error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("native runtime issuer cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := runtimeBundle.Verifier().VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("native runtime verifier error = %v", err)
	}
	if !payload.Group.Equal(quiescence.Group) || payload.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("native runtime verified payload = %+v, want %+v", payload, quiescence)
	}
}

func TestRetainedNativeContainmentBackendClosePropagatesRemoveFailure(t *testing.T) {
	manager := newFakeNativeRetainedManager()
	capabilityRaw, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup() error = %v", err)
	}
	capability := capabilityRaw.(*fakeNativeRetainedCapability)
	removeErr := errors.New("injected retained remove failure")
	capability.leaf.removeErr = removeErr
	backend := &retainedNativeContainmentBackend{capability: capability}

	err = backend.close(context.Background())
	if !errors.Is(err, removeErr) {
		t.Fatalf("backend.close() error = %v, want remove failure", err)
	}
	if capability.leaf.releases != 1 {
		t.Fatalf("capability releases = %d, want 1", capability.leaf.releases)
	}
	if capability.leaf.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1", capability.leaf.removeCalls)
	}
}

func TestNativeRetainedContainAndVerifyPreservesCleanupFailureAfterFinalization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	native.issuer = issuer
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	removeErr := errors.New("injected retained remove failure")
	manager.setMembership(running.Ref().RetainedID, containment.RetainedMembershipEmpty)
	manager.setRemoveErr(running.Ref().RetainedID, removeErr)

	first, cleanup, err := running.ContainAndVerify(ctx, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("first ContainAndVerify() error = %v, want nil", err)
	}
	if !errors.Is(cleanup.Err, removeErr) {
		t.Fatalf("first ContainAndVerify() cleanup error = %v, want retained remove failure", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(first)
	if err != nil {
		t.Fatalf("first VerifyQuiescence() error = %v", err)
	}
	if !payload.Group.Equal(running.Ref()) || payload.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("first attestation payload = %+v, want already-absent for %+v", payload, running.Ref())
	}
	if !running.finalOutcome.Absent() || running.finalOutcome.Err != nil {
		t.Fatalf("cached final outcome = %+v, want clean absence fact", running.finalOutcome)
	}
	if !errors.Is(running.finalErr, removeErr) {
		t.Fatalf("cached final error = %v, want retained remove failure", running.finalErr)
	}

	second, cleanup, err := running.ContainAndVerify(ctx, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("second ContainAndVerify() error = %v, want nil", err)
	}
	if !errors.Is(cleanup.Err, removeErr) {
		t.Fatalf("second ContainAndVerify() cleanup error = %v, want retained remove failure", cleanup.Err)
	}
	if second != first {
		t.Fatalf("second ContainAndVerify() attestation = %+v, want cached %+v", second, first)
	}
}

func TestNativeRetainedWaitCompletesWithoutLeaderRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if running.leader != nil {
		t.Fatalf("retained backend leader = %T, want nil", running.leader)
	}
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	manager.setMembership(running.Ref().RetainedID, containment.RetainedMembershipEmpty)

	exit, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean retained worker exit", exit)
	}
	leaf := manager.leafForRetainedID(t, running.Ref().RetainedID)
	if leaf.removeCalls != 1 || !leaf.removed {
		t.Fatalf("leaf remove calls/removed = %d/%t, want 1/true", leaf.removeCalls, leaf.removed)
	}
	if leaf.rootLeaseReleases != 0 {
		t.Fatalf("root lease releases = %d, want 0", leaf.rootLeaseReleases)
	}
	if leaf.termCalls != 0 || leaf.killCalls != 0 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 0/0", leaf.termCalls, leaf.killCalls)
	}
	if !running.finalOutcome.Absent() || running.finalOutcome.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("final outcome = %+v, want already-absent", running.finalOutcome)
	}
}

func TestNativeRetainedWaitContainsResidualWithoutLeaderRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	manager.setTermIgnored(running.Ref().RetainedID, true)

	exit, err := running.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() with retained residual error = %v", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean retained worker exit", exit)
	}
	leaf := manager.leafForRetainedID(t, running.Ref().RetainedID)
	if leaf.termCalls != 1 || leaf.killCalls != 1 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 1/1", leaf.termCalls, leaf.killCalls)
	}
	if leaf.removeCalls != 1 || !leaf.removed {
		t.Fatalf("leaf remove calls/removed = %d/%t, want 1/true", leaf.removeCalls, leaf.removed)
	}
	if !running.finalOutcome.Absent() || running.finalOutcome.Method != model.QuiescenceTermKill {
		t.Fatalf("final outcome = %+v, want term-kill", running.finalOutcome)
	}
}

func TestNativeContainAndVerifyRecoveryReattachesRetainedGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	manager := newFakeNativeRetainedManager()
	native := newRecoveryNativeCustodianForTest(t, defaultNativeTestParams(), issuer, manager)
	group := newRecoveryRetainedGroupRefForTest(t, manager)
	manager.setMembership(group.RetainedID, containment.RetainedMembershipPresent)
	manager.setTermIgnored(group.RetainedID, true)
	before := manager.leafForRetainedID(t, group.RetainedID)

	verified, cleanup, err := native.ContainAndVerify(ctx, group, QuiescenceCauseRecovery)
	if err != nil {
		t.Fatalf("ContainAndVerify(recovery) error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify(recovery) cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence(recovery) error = %v", err)
	}
	if !payload.Group.Equal(group) || payload.Method != model.QuiescenceTermKill {
		t.Fatalf("payload = %+v, want term-kill for %+v", payload, group)
	}
	leaf := manager.leafForRetainedID(t, group.RetainedID)
	if leaf.openCalls != before.openCalls+1 {
		t.Fatalf("retained open calls = %d, want %d", leaf.openCalls, before.openCalls+1)
	}
	if leaf.termCalls != 1 || leaf.killCalls != 1 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 1/1", leaf.termCalls, leaf.killCalls)
	}
	if leaf.membership != containment.RetainedMembershipEmpty {
		t.Fatalf("retained membership = %v, want empty", leaf.membership)
	}
	if leaf.releases != before.releases+1 {
		t.Fatalf("retained releases = %d, want %d", leaf.releases, before.releases+1)
	}
}

func TestNativeContainAndVerifyRecoveryTreatsMissingRetainedGroupAsAlreadyAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	manager := newFakeNativeRetainedManager()
	native := newRecoveryNativeCustodianForTest(t, defaultNativeTestParams(), issuer, manager)
	group := newRecoveryRetainedGroupRefForTest(t, manager)
	manager.setMembership(group.RetainedID, containment.RetainedMembershipPresent)
	manager.removeLeaf(group.RetainedID)
	before := manager.leafForRetainedID(t, group.RetainedID)

	verified, cleanup, err := native.ContainAndVerify(ctx, group, QuiescenceCauseRecovery)
	if err != nil {
		t.Fatalf("ContainAndVerify(recovery missing retained group) error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify(recovery missing retained group) cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence(recovery missing retained group) error = %v", err)
	}
	if !payload.Group.Equal(group) || payload.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("payload = %+v, want already-absent for %+v", payload, group)
	}
	leaf := manager.leafForRetainedID(t, group.RetainedID)
	if leaf.openCalls != before.openCalls+1 {
		t.Fatalf("retained open calls = %d, want %d", leaf.openCalls, before.openCalls+1)
	}
	if leaf.termCalls != 0 || leaf.killCalls != 0 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 0/0", leaf.termCalls, leaf.killCalls)
	}
	if leaf.releases != before.releases {
		t.Fatalf("retained releases = %d, want unchanged %d", leaf.releases, before.releases)
	}
}

func TestNativeContainAndVerifyRecoverySelectsProcessGroupForUnretainedGroupRef(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	retainedFactoryCalls := atomic.Int64{}
	native := &NativeCustodian{
		options: NativeOptions{
			ContainmentParams: defaultNativeTestParams(),
			newRetainedGroup: func() (containment.RetainedGroupObject, error) {
				retainedFactoryCalls.Add(1)
				return nil, errors.New("retained manager must not be used for unretained recovery")
			},
		},
		issuer:    issuer,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}
	cleanupNativeCustodianForTest(t, native)
	domain, err := procgroup.CurrentKernelDomain()
	if err != nil {
		t.Fatal(err)
	}
	domain.RetainedDomainID = ""
	domain.RetainedDomainState = model.RetainedDomainNotApplicable
	if err := domain.Validate(); err != nil {
		t.Fatal(err)
	}
	pgid := absentProcessGroupForDomain(t, domain)
	group := model.GroupRef{
		Version:   1,
		CustodyID: "custody-native-recovery-unretained",
		Launch: model.LaunchKey{
			Attempt: model.AttemptRef{JobID: "job-native-recovery-unretained", AttemptID: "attempt-native-recovery-unretained", Epoch: 1},
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:          domain.HostBootID,
		PIDNamespaceID:      domain.PIDNamespaceID,
		PIDNamespaceState:   domain.PIDNamespaceState,
		RetainedDomainState: model.RetainedDomainNotApplicable,
		PGID:                pgid,
		Leader: model.ProcessIdentity{
			PID:               pgid,
			HighResStartToken: "leader-start-native-recovery-unretained",
		},
		Monitor: model.ProcessIdentity{
			PID:               pgid - 1,
			HighResStartToken: "monitor-start-native-recovery-unretained",
		},
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("unretained recovery group Validate() error = %v", err)
	}

	verified, cleanup, err := native.ContainAndVerify(ctx, group, QuiescenceCauseRecovery)
	if err != nil {
		t.Fatalf("ContainAndVerify(unretained recovery) error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify(unretained recovery) cleanup error = %v, want nil", cleanup.Err)
	}
	payload, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence(unretained recovery) error = %v", err)
	}
	if !payload.Group.Equal(group) || payload.Method != model.QuiescenceAlreadyAbsent {
		t.Fatalf("payload = %+v, want process-group already-absent for %+v", payload, group)
	}
	if got := retainedFactoryCalls.Load(); got != 0 {
		t.Fatalf("retained factory calls = %d, want 0 for unretained recovery", got)
	}
}

func TestNativeContainAndVerifyRecoveryMissingRetainedGroupRequiresProcessGroupAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, _ := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	manager := newFakeNativeRetainedManager()
	native := newRecoveryNativeCustodianForTest(t, defaultNativeTestParams(), counting, manager)
	capability, err := manager.AcquireRetainedGroup(ctx, model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup(create) error = %v", err)
	}
	identity := capability.Identity()
	if err := capability.Release(); err != nil {
		t.Fatalf("release created retained group: %v", err)
	}
	cmd := exec.Command("/bin/sh", "-c", "exec sleep 60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start live process group: %v", err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- cmd.Wait()
	}()
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = unix.Kill(-cmd.Process.Pid, unix.SIGKILL)
		}
		select {
		case <-waitDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("live process group cleanup wait timed out")
		}
	})
	leader := waitNativeProcessGroupLeaderClaim(t, ctx, cmd.Process.Pid)
	monitor, err := procgroup.ReadProcessClaim(os.Getpid())
	if err != nil {
		t.Fatalf("read monitor identity: %v", err)
	}
	attempt := model.AttemptRef{JobID: "job-native-recovery-live", AttemptID: "attempt-native-recovery-live", Epoch: 1}
	group := model.GroupRef{
		Version:             1,
		CustodyID:           "custody-native-recovery-live",
		Launch:              model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		HostBootID:          identity.KernelDomainID.HostBootID,
		PIDNamespaceID:      identity.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState:   identity.KernelDomainID.PIDNamespaceState,
		RetainedDomainID:    identity.KernelDomainID.RetainedDomainID,
		RetainedDomainState: identity.KernelDomainID.RetainedDomainState,
		PGID:                leader.PGID,
		Leader: model.ProcessIdentity{
			PID:               leader.PID,
			HighResStartToken: leader.StartToken.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               monitor.PID,
			HighResStartToken: monitor.StartToken.String(),
		},
		RetainedID: identity.RetainedID,
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("live recovery group Validate() error = %v", err)
	}
	manager.setMembership(group.RetainedID, containment.RetainedMembershipPresent)
	manager.removeLeaf(group.RetainedID)

	verified, cleanup, err := native.ContainAndVerify(ctx, group, QuiescenceCauseRecovery)
	if err == nil {
		t.Fatal("ContainAndVerify(recovery live process group with missing retained leaf) error = nil, want unprovable")
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	if verified != (VerifiedQuiescence{}) {
		t.Fatalf("verified = %+v, want zero attestation", verified)
	}
	if !errors.Is(err, ErrNativeCustodianUnavailable) {
		t.Fatalf("ContainAndVerify() error = %v, want ErrNativeCustodianUnavailable", err)
	}
	if !errors.Is(err, ErrRetainedObjectReacquireUnresolved) {
		t.Fatalf("ContainAndVerify() error = %v, want ErrRetainedObjectReacquireUnresolved", err)
	}
	if counting.count.Load() != 0 {
		t.Fatalf("quiescence attestations = %d, want 0", counting.count.Load())
	}
}

func TestNativeContainAndVerifyRecoveryMissingRetainedGroupMismatchedDomainIsUnprovable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, _ := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	manager := newFakeNativeRetainedManager()
	native := newRecoveryNativeCustodianForTest(t, defaultNativeTestParams(), counting, manager)
	group := newRecoveryRetainedGroupRefForTest(t, manager)
	manager.setMembership(group.RetainedID, containment.RetainedMembershipPresent)
	manager.removeLeaf(group.RetainedID)
	manager.setCurrentDomain(t, differentKernelDomainForTest(t, group.KernelDomain()))
	before := manager.leafForRetainedID(t, group.RetainedID)

	verified, cleanup, err := native.ContainAndVerify(ctx, group, QuiescenceCauseRecovery)
	if err == nil {
		t.Fatalf("ContainAndVerify(recovery missing retained group in mismatched domain) error = nil, want unprovable")
	}
	if cleanup.Err != nil {
		t.Fatalf("ContainAndVerify() cleanup error = %v, want nil", cleanup.Err)
	}
	if verified != (VerifiedQuiescence{}) {
		t.Fatalf("verified = %+v, want zero attestation", verified)
	}
	if !errors.Is(err, ErrNativeCustodianUnavailable) {
		t.Fatalf("ContainAndVerify() error = %v, want ErrNativeCustodianUnavailable", err)
	}
	if !errors.Is(err, ErrRetainedObjectReacquireUnresolved) {
		t.Fatalf("ContainAndVerify() error = %v, want ErrRetainedObjectReacquireUnresolved", err)
	}
	if counting.count.Load() != 0 {
		t.Fatalf("quiescence attestations = %d, want 0", counting.count.Load())
	}
	leaf := manager.leafForRetainedID(t, group.RetainedID)
	if leaf.openCalls != before.openCalls+1 {
		t.Fatalf("retained open calls = %d, want %d", leaf.openCalls, before.openCalls+1)
	}
	if leaf.termCalls != 0 || leaf.killCalls != 0 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 0/0", leaf.termCalls, leaf.killCalls)
	}
	if leaf.releases != before.releases {
		t.Fatalf("retained releases = %d, want unchanged %d", leaf.releases, before.releases)
	}
}

func TestNativeRetainedCanceledWaitsDoNotLeakReapers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	manager.setTermIgnored(running.Ref().RetainedID, true)

	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _ = running.Wait(waitCtx)
		waitCancel()
	}
	time.Sleep(100 * time.Millisecond)
	if running.finalized {
		t.Fatalf("running finalized after canceled waits while retained object was still populated: %+v", running.finalOutcome)
	}
	after := runtime.NumGoroutine()
	if after > before+5 {
		t.Fatalf("goroutines before=%d after=%d, want no repeated wait leak", before, after)
	}

	manager.setMembership(running.Ref().RetainedID, containment.RetainedMembershipEmpty)
	if _, err := running.Wait(ctx); err != nil {
		t.Fatalf("cleanup Wait() after retained object empty error = %v", err)
	}
}

func TestNativeRetainedSharedManagerSequentialAndConcurrentLaunches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	var factoryCalls int
	native := newNativeCustodianWithRetainedFactoryForTest(t, defaultNativeTestParams(), func() (containment.RetainedGroupObject, error) {
		factoryCalls++
		return manager, nil
	})

	first := launchAndWaitRetainedSimple(t, ctx, native, manager)
	second := launchAndWaitRetainedSimple(t, ctx, native, manager)
	if first.RetainedID == second.RetainedID {
		t.Fatalf("sequential retained ids both = %q, want distinct leaves", first.RetainedID)
	}

	specA, _ := nativeSimpleLaunchSpec(t)
	specB, _ := nativeSimpleLaunchSpec(t)
	type launchResult struct {
		group model.GroupRef
		err   error
	}
	results := make(chan launchResult, 2)
	for _, spec := range []NativeLaunchSpec{specA, specB} {
		spec := spec
		go func() {
			running, err := native.Launch(ctx, spec)
			if err != nil {
				results <- launchResult{err: err}
				return
			}
			defer cleanupNativeRunning(t, running)
			if _, err := io.ReadAll(running.Stdout()); err != nil {
				results <- launchResult{err: err}
				return
			}
			manager.setMembership(running.Ref().RetainedID, containment.RetainedMembershipEmpty)
			if _, err := running.Wait(ctx); err != nil {
				results <- launchResult{err: err}
				return
			}
			results <- launchResult{group: running.Ref()}
		}()
	}
	concurrent := make([]model.GroupRef, 0, 2)
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent retained launch %d error = %v", i, result.err)
		}
		concurrent = append(concurrent, result.group)
	}
	if concurrent[0].RetainedID == concurrent[1].RetainedID {
		t.Fatalf("concurrent retained ids both = %q, want distinct leaves", concurrent[0].RetainedID)
	}
	if factoryCalls != 1 {
		t.Fatalf("retained manager factory calls = %d, want 1", factoryCalls)
	}
	if err := native.Close(); err != nil {
		t.Fatalf("NativeCustodian.Close() error = %v", err)
	}
	if manager.closeCalls != 1 {
		t.Fatalf("retained manager close calls = %d, want 1", manager.closeCalls)
	}
}

func TestNativeRetainedLeafRetiredAfterPlacePIDFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	manager.placeErr = errors.New("injected PlacePID failure")
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	spec, _ := nativeSimpleLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err == nil {
		cleanupNativeRunning(t, running)
		t.Fatal("Launch() succeeded, want injected PlacePID failure")
	}
	if !strings.Contains(err.Error(), "injected PlacePID failure") {
		t.Fatalf("Launch() error = %v, want injected PlacePID failure", err)
	}
	leaves := manager.leavesSnapshot()
	if len(leaves) != 1 {
		t.Fatalf("fake retained leaves = %d, want 1", len(leaves))
	}
	if leaves[0].removeCalls != 1 || !leaves[0].removed {
		t.Fatalf("early-error leaf remove calls/removed = %d/%t, want 1/true", leaves[0].removeCalls, leaves[0].removed)
	}
	if err := native.Close(); err != nil {
		t.Fatalf("NativeCustodian.Close() error = %v", err)
	}
}

type nativeBackendResult struct {
	PID            int `json:"pid"`
	PGID           int `json:"pgid"`
	GrandchildPID  int `json:"grandchildPid,omitempty"`
	GrandchildPGID int `json:"grandchildPgid,omitempty"`
}

func newNativeCustodianForTest(t *testing.T, params containment.Params) *NativeCustodian {
	t.Helper()
	native := newNativeCustodianWithRetainedFactoryForTest(t, params, nil)
	cleanupNativeCustodianForTest(t, native)
	return native
}

func newNativeCustodianWithRetainedManagerForTest(t *testing.T, params containment.Params, manager *fakeNativeRetainedManager) *NativeCustodian {
	t.Helper()
	return newNativeCustodianWithRetainedFactoryForTest(t, params, func() (containment.RetainedGroupObject, error) {
		return manager, nil
	})
}

func newNativeCustodianWithRetainedFactoryForTest(t *testing.T, params containment.Params, factory func() (containment.RetainedGroupObject, error)) *NativeCustodian {
	t.Helper()
	exe := nativeTestBinaryPath(t)
	monitorCommand := nativeMonitorCommand(t)
	if factory != nil {
		monitorCommand.Env = append(monitorCommand.Env, nativeHelperRetainedNoopMonitor+"=1")
	}
	native, err := NewNativeCustodian(NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    monitorCommand,
		ContainmentParams: params,
		WorkerEnv:         nativeAgentbusEnv(),
		WorkerDir:         filepath.Dir(exe),
		newRetainedGroup:  factory,
	})
	if err != nil {
		t.Fatalf("NewNativeCustodian() error = %v", err)
	}
	// NewNativeCustodian no longer wires an attestation issuer (R3C moved that
	// to NewNativeRuntime, whose self-tested instance owns the channel). Bare
	// test custodians must attach one or every mint fails ErrInvalidAttestation.
	issuer, _ := NewAttestationChannel()
	native.issuer = issuer
	return native
}

func newRecoveryNativeCustodianForTest(t *testing.T, params containment.Params, issuer quiescenceAttestationIssuer, manager *fakeNativeRetainedManager) *NativeCustodian {
	t.Helper()
	native := &NativeCustodian{
		options: NativeOptions{
			ContainmentParams: params,
			newRetainedGroup: func() (containment.RetainedGroupObject, error) {
				return manager, nil
			},
		},
		issuer:    issuer,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}
	cleanupNativeCustodianForTest(t, native)
	return native
}

func cleanupNativeCustodianForTest(t *testing.T, native *NativeCustodian) {
	t.Helper()
	t.Cleanup(func() {
		if err := native.Close(); err != nil {
			t.Fatalf("NativeCustodian.Close() error = %v", err)
		}
	})
}

func launchAndWaitRetainedSimple(t *testing.T, ctx context.Context, native *NativeCustodian, manager *fakeNativeRetainedManager) model.GroupRef {
	t.Helper()
	spec, _ := nativeSimpleLaunchSpec(t)
	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	if _, err := io.ReadAll(running.Stdout()); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	manager.setMembership(running.Ref().RetainedID, containment.RetainedMembershipEmpty)
	if _, err := running.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return running.Ref()
}

func newRecoveryRetainedGroupRefForTest(t *testing.T, manager *fakeNativeRetainedManager) model.GroupRef {
	t.Helper()
	capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now())
	if err != nil {
		t.Fatalf("AcquireRetainedGroup(create) error = %v", err)
	}
	if capability == nil {
		t.Fatal("AcquireRetainedGroup(create) capability is nil")
	}
	identity := capability.Identity()
	if err := capability.Release(); err != nil {
		t.Fatalf("release created retained group: %v", err)
	}
	pgid := absentProcessGroupForDomain(t, identity.KernelDomainID)
	attempt := model.AttemptRef{JobID: "job-native-recovery", AttemptID: "attempt-native-recovery", Epoch: 1}
	group := model.GroupRef{
		Version:   1,
		CustodyID: "custody-native-recovery",
		Launch: model.LaunchKey{
			Attempt: attempt,
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:          identity.KernelDomainID.HostBootID,
		PIDNamespaceID:      identity.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState:   identity.KernelDomainID.PIDNamespaceState,
		RetainedDomainID:    identity.KernelDomainID.RetainedDomainID,
		RetainedDomainState: identity.KernelDomainID.RetainedDomainState,
		PGID:                pgid,
		Leader: model.ProcessIdentity{
			PID:               pgid,
			HighResStartToken: "leader-start-native-recovery",
		},
		Monitor: model.ProcessIdentity{
			PID:               pgid - 1,
			HighResStartToken: "monitor-start-native-recovery",
		},
		RetainedID: identity.RetainedID,
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("recovery group Validate() error = %v", err)
	}
	return group
}

func absentProcessGroupForDomain(t *testing.T, domain model.KernelDomainID) int {
	t.Helper()
	for pgid := 2147483647; pgid > 2147483547; pgid-- {
		claim, err := procgroup.NewGroupClaim(pgid, domain)
		if err != nil {
			t.Fatalf("NewGroupClaim(%d) error = %v", pgid, err)
		}
		got := procgroup.ClassifyGroup(claim)
		if got == model.GroupAbsent {
			return pgid
		}
		if got == model.GroupExistenceUnknown {
			t.Fatalf("ClassifyGroup(absent candidate %d) = %v", pgid, got)
		}
	}
	t.Fatal("could not find an absent process group candidate")
	return 0
}

func waitNativeProcessGroupLeaderClaim(t *testing.T, ctx context.Context, pid int) procgroup.ProcessClaim {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last procgroup.ProcessClaim
	var lastErr error
	for {
		claim, err := procgroup.ReadProcessClaim(pid)
		if err == nil && claim.PID == pid && claim.PGID == pid {
			return claim
		}
		if err == nil {
			last = claim
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d did not become process-group leader: last=%+v err=%v", pid, last, lastErr)
		}
		if err := sleepContext(ctx, 10*time.Millisecond); err != nil {
			t.Fatalf("wait process-group leader: %v", err)
		}
	}
}

func differentKernelDomainForTest(t *testing.T, domain model.KernelDomainID) model.KernelDomainID {
	t.Helper()
	different := domain
	if different.PIDNamespaceState == model.PIDNamespaceKnown {
		different.PIDNamespaceID = different.PIDNamespaceID + "-different"
	} else {
		different.HostBootID = different.HostBootID + "-different"
	}
	if err := different.Validate(); err != nil {
		t.Fatalf("different kernel domain Validate() error = %v", err)
	}
	if different.ProvablySame(domain) {
		t.Fatalf("different kernel domain %+v is provably same as %+v", different, domain)
	}
	return different
}

func defaultNativeTestParams() containment.Params {
	return containment.Params{
		GracePeriod:                100 * time.Millisecond,
		PollInterval:               20 * time.Millisecond,
		PollTimeout:                3 * time.Second,
		TrustedMonitorWait:         100 * time.Millisecond,
		TrustedMonitorPollInterval: 20 * time.Millisecond,
	}
}

func nativeSimpleLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "simple-result.json")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperSimpleProcess$",
			"--",
			"--result", resultPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperSimple),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeTermGrandchildLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "term-grandchild-result.json")
	readyPath := filepath.Join(dir, "grandchild-ready")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperTermGrandchildProcess$",
			"--",
			"--result", resultPath,
			"--grandchild-ready", readyPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperTermGrandchild),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeIgnoreTermGrandchildLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "ignore-term-grandchild-result.json")
	readyPath := filepath.Join(dir, "ignore-term-grandchild-ready")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperIgnoreTermGrandchildProcess$",
			"--",
			"--result", resultPath,
			"--grandchild-ready", readyPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperIgnoreTermGrandchild),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeIgnoreTermLeaderLaunchSpec(t *testing.T) (NativeLaunchSpec, string) {
	t.Helper()
	dir := t.TempDir()
	resultPath := filepath.Join(dir, "ignore-term-leader-result.json")
	exe := nativeTestBinaryPath(t)
	spec := nativeLaunchSpec(t, command.ExecSpec{
		Argv: []string{
			exe,
			"-test.run=^TestNativeHelperIgnoreTermLeaderProcess$",
			"--",
			"--result", resultPath,
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperIgnoreTermLeader),
		Dir: filepath.Dir(exe),
	})
	return spec, resultPath
}

func nativeLaunchSpec(t *testing.T, exec command.ExecSpec) NativeLaunchSpec {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	attempt := model.AttemptRef{JobID: model.JobID("job-native-" + suffix), AttemptID: model.AttemptID("attempt-native-" + suffix), Epoch: 1}
	launchKey := model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne}
	return NativeLaunchSpec{
		Exec:      exec,
		CustodyID: model.CustodyID("custody-native-" + suffix),
		LaunchKey: launchKey,
	}
}

func nativeMonitorCommand(t *testing.T) parklaunch.CommandSpec {
	t.Helper()
	exe := nativeTestBinaryPath(t)
	return parklaunch.CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"-test.run=^TestNativeHelperMonitorProcess$",
			"--",
			"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
			"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
			"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
			"--leaf-fd", strconv.Itoa(parklaunch.MonitorLeafFD),
		},
		Env: append(os.Environ(), nativeHelperEnv+"="+nativeHelperMonitor),
		Dir: filepath.Dir(exe),
	}
}

func builtNativeAgentbusPath(t *testing.T) string {
	t.Helper()
	nativeAgentbusBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "agentbus-native-custodian-bin-")
		if err != nil {
			nativeAgentbusBuildErr = err
			return
		}
		nativeAgentbusBuildPath = filepath.Join(dir, "agentbus")
		cmd := exec.Command("go", "build", "-o", nativeAgentbusBuildPath, "./cmd/agentbus")
		cmd.Dir = nativeRepoRootFromCaller()
		cmd.Env = nativeAgentbusEnv()
		output, err := cmd.CombinedOutput()
		if err != nil {
			nativeAgentbusBuildErr = fmt.Errorf("go build ./cmd/agentbus: %w\n%s", err, output)
		}
	})
	if nativeAgentbusBuildErr != nil {
		t.Fatal(nativeAgentbusBuildErr)
	}
	return nativeAgentbusBuildPath
}

func nativeAgentbusEnv() []string {
	return append(os.Environ(), nativeAgentbusGoEnv()...)
}

func nativeAgentbusGoEnv() []string {
	env := []string{
		nativeHelperAgentbusGOFLAGS,
		nativeHelperAgentbusGOPROXY,
	}
	if modcache := os.Getenv(nativeHelperOfflineModcacheEnv); modcache != "" {
		env = append(env, "GOMODCACHE="+modcache)
	}
	return env
}

func nativeRepoRootFromCaller() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func nativeTestBinaryPath(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}

func waitNativeReadyLine(t *testing.T, reader io.Reader, want string) {
	t.Helper()
	done := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(reader).ReadString('\n')
		done <- line
	}()
	select {
	case line := <-done:
		if !strings.Contains(line, want) {
			t.Fatalf("ready line = %q, want %q", line, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", want)
	}
}

func readNativeBackendResult(t *testing.T, path string) nativeBackendResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil {
			var result nativeBackendResult
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatal(err)
			}
			return result
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for result %s: %v", path, lastErr)
	return nativeBackendResult{}
}

func cleanupNativeRunning(t *testing.T, running *NativeRunningProcess) {
	t.Helper()
	if running == nil {
		return
	}
	group := running.Ref()
	if group.PGID > 1 {
		err := unix.Kill(-group.PGID, unix.SIGKILL)
		if err != nil && !errors.Is(err, unix.ESRCH) {
			t.Fatalf("cleanup kill group %d: %v", group.PGID, err)
		}
	}
	if running.handle != nil && !running.handle.LeaderReaped() {
		_, _ = running.handle.WaitState()
	}
	waitGroupAbsent(t, group, 5*time.Second)
	if running.handle != nil && running.handle.Monitor != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := running.handle.Monitor.Stop(ctx); err != nil && !strings.Contains(err.Error(), "signal: killed") {
			t.Fatalf("cleanup monitor stop: %v", err)
		}
	}
	if running.leader != nil {
		_ = running.leader.close()
	}
	cleanupUnfinalizedNativeRunning(t, running, group)
}

func cleanupUnfinalizedNativeRunning(t *testing.T, running *NativeRunningProcess, group model.GroupRef) {
	t.Helper()
	running.lifecycleMu.Lock()
	finalized := running.finalized
	running.lifecycleMu.Unlock()
	if finalized {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if running.containment != nil {
		if err := running.containment.close(ctx); err != nil {
			t.Fatalf("cleanup containment close: %v", err)
		}
	}
	if running.custodian != nil {
		running.custodian.forget(group)
	}
}

func closeNativeExposedStreamsForTest(t *testing.T, running *NativeRunningProcess) {
	t.Helper()
	if running == nil {
		t.Fatal("running process is nil")
	}
	closers := []struct {
		name   string
		closer io.Closer
	}{
		{name: "stdin", closer: running.Stdin()},
		{name: "stdout", closer: running.Stdout()},
		{name: "stderr", closer: running.Stderr()},
	}
	for _, stream := range closers {
		if stream.closer == nil {
			continue
		}
		if err := stream.closer.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("close %s: %v", stream.name, err)
		}
	}
}

func waitGroupAbsent(t *testing.T, group model.GroupRef, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last model.GroupExistenceObservation
	for {
		claim, err := procgroup.NewGroupClaim(group.PGID, group.KernelDomain())
		if err != nil {
			t.Fatal(err)
		}
		last = procgroup.ClassifyGroup(claim)
		if last == model.GroupAbsent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %d observation = %s after %s, want absent", group.PGID, last, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitPIDAbsent(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		_, err := procgroup.ReadProcessClaim(pid)
		if errors.Is(err, procgroup.ErrProcessMissing) {
			return
		}
		if err != nil && time.Now().After(deadline) {
			t.Fatalf("pid %d read error after %s: %v", pid, timeout, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("pid %d still exists after %s", pid, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitLeaderRunState(t *testing.T, group model.GroupRef, state procgroup.ProcessRunState, timeout time.Duration) procgroup.ProcessObservation {
	t.Helper()
	leader, err := procgroup.NewProcessClaim(
		group.Leader.PID,
		group.PGID,
		procgroup.StartToken(group.Leader.HighResStartToken),
		group.KernelDomain(),
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	var last procgroup.ProcessObservation
	for {
		last = procgroup.ObserveProcess(leader)
		if last.RunState == state {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader pid %d run state = %s after %s, want %s (identity=%s)", group.Leader.PID, last.RunState, timeout, state, last.Identity)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitLeaderNotMatching(t *testing.T, group model.GroupRef, timeout time.Duration) {
	t.Helper()
	leader, err := procgroup.NewProcessClaim(
		group.Leader.PID,
		group.PGID,
		procgroup.StartToken(group.Leader.HighResStartToken),
		group.KernelDomain(),
	)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for {
		identity := procgroup.ClassifyProcess(leader)
		if identity != model.ProcessIdentityMatching {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader pid %d still matches after %s", group.Leader.PID, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type fakeNativeRetainedManager struct {
	mu            sync.Mutex
	next          int
	leaves        map[string]*fakeNativeRetainedLeaf
	currentDomain model.KernelDomainID
	placeErr      error
	closeCalls    int
}

type fakeNativeRetainedLeaf struct {
	retainedID        string
	domain            model.KernelDomainID
	membership        containment.RetainedGroupMembership
	ignoreTerm        bool
	removeErr         error
	placedPIDs        []int
	openCalls         int
	termCalls         int
	killCalls         int
	removeCalls       int
	rootLeaseReleases int
	removed           bool
	releases          int
}

type fakeNativeRetainedCapability struct {
	manager  *fakeNativeRetainedManager
	leaf     *fakeNativeRetainedLeaf
	released bool
}

type fakeNativeRetainedContinuity struct {
	capability *fakeNativeRetainedCapability
	begin      time.Time
}

func newFakeNativeRetainedManager() *fakeNativeRetainedManager {
	return &fakeNativeRetainedManager{leaves: map[string]*fakeNativeRetainedLeaf{}}
}

func (manager *fakeNativeRetainedManager) AcquireRetainedGroup(_ context.Context, target model.GroupRef, _ time.Time) (containment.RetainedGroupCapability, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	var leaf *fakeNativeRetainedLeaf
	if target.RetainedID == "" {
		manager.next++
		retainedID := fmt.Sprintf("retained-fake-%d", manager.next)
		domain, err := procgroup.CurrentKernelDomain()
		if err != nil {
			return nil, err
		}
		domain.RetainedDomainID = fmt.Sprintf("domain-fake-%d", manager.next)
		domain.RetainedDomainState = model.RetainedDomainKnown
		if err := domain.Validate(); err != nil {
			return nil, err
		}
		manager.currentDomain = domain
		leaf = &fakeNativeRetainedLeaf{
			retainedID: retainedID,
			domain:     domain,
			membership: containment.RetainedMembershipEmpty,
		}
		manager.leaves[retainedID] = leaf
	} else {
		leaf = manager.leaves[target.RetainedID]
		if leaf != nil {
			leaf.openCalls++
		}
		if leaf == nil || leaf.removed {
			return nil, fmt.Errorf("%w: unknown retained id %q", unix.ENOENT, target.RetainedID)
		}
	}
	return &fakeNativeRetainedCapability{manager: manager, leaf: leaf}, nil
}

func (manager *fakeNativeRetainedManager) ProveRetainedGroupAbsent(_ context.Context, target model.GroupRef) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if err := target.Validate(); err != nil {
		return err
	}
	leaf := manager.leaves[target.RetainedID]
	if leaf == nil {
		return fmt.Errorf("%w: retained leaf identity is unknown", ErrNativeCustodianUnavailable)
	}
	if err := manager.currentDomain.Validate(); err != nil {
		return fmt.Errorf("%w: current retained root domain is unverifiable: %v", ErrNativeCustodianUnavailable, err)
	}
	if !manager.currentDomain.ProvablySame(target.KernelDomain()) {
		return fmt.Errorf("%w: current retained root domain does not match target", ErrNativeCustodianUnavailable)
	}
	if !leaf.removed {
		return fmt.Errorf("%w: retained leaf still exists", ErrNativeCustodianUnavailable)
	}
	return nil
}

func (manager *fakeNativeRetainedManager) Close() error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.closeCalls++
	return nil
}

func (manager *fakeNativeRetainedManager) setMembership(retainedID string, membership containment.RetainedGroupMembership) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	leaf := manager.leaves[retainedID]
	if leaf != nil {
		leaf.membership = membership
	}
}

func (manager *fakeNativeRetainedManager) setTermIgnored(retainedID string, ignored bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	leaf := manager.leaves[retainedID]
	if leaf != nil {
		leaf.ignoreTerm = ignored
	}
}

func (manager *fakeNativeRetainedManager) setRemoveErr(retainedID string, err error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	leaf := manager.leaves[retainedID]
	if leaf != nil {
		leaf.removeErr = err
	}
}

func (manager *fakeNativeRetainedManager) removeLeaf(retainedID string) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	leaf := manager.leaves[retainedID]
	if leaf != nil {
		leaf.removed = true
	}
}

func (manager *fakeNativeRetainedManager) setCurrentDomain(t *testing.T, domain model.KernelDomainID) {
	t.Helper()
	if err := domain.Validate(); err != nil {
		t.Fatalf("current domain Validate() error = %v", err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.currentDomain = domain
}

func (manager *fakeNativeRetainedManager) leafForRetainedID(t *testing.T, retainedID string) *fakeNativeRetainedLeaf {
	t.Helper()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	leaf := manager.leaves[retainedID]
	if leaf == nil {
		t.Fatalf("retained leaf %q missing", retainedID)
	}
	copyLeaf := *leaf
	return &copyLeaf
}

func (manager *fakeNativeRetainedManager) leavesSnapshot() []fakeNativeRetainedLeaf {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	out := make([]fakeNativeRetainedLeaf, 0, len(manager.leaves))
	for _, leaf := range manager.leaves {
		out = append(out, *leaf)
	}
	return out
}

func (capability *fakeNativeRetainedCapability) Identity() containment.RetainedGroupIdentity {
	if capability == nil || capability.leaf == nil {
		return containment.RetainedGroupIdentity{}
	}
	return containment.RetainedGroupIdentity{
		RetainedID:     capability.leaf.retainedID,
		KernelDomainID: capability.leaf.domain,
	}
}

func (capability *fakeNativeRetainedCapability) Membership(context.Context) (containment.RetainedGroupMembership, error) {
	if err := capability.usable(); err != nil {
		return containment.RetainedMembershipUnknown, err
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	if capability.leaf.removed {
		return containment.RetainedMembershipUnknown, fmt.Errorf("%w: retained leaf was removed", ErrNativeCustodianUnavailable)
	}
	return capability.leaf.membership, nil
}

func (capability *fakeNativeRetainedCapability) StillHeld(context.Context) (bool, error) {
	if capability == nil || capability.released || capability.leaf == nil {
		return false, nil
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	return !capability.leaf.removed, nil
}

func (capability *fakeNativeRetainedCapability) SignalTerm(context.Context) (containment.SignalResult, error) {
	return capability.signalRetained(true)
}

func (capability *fakeNativeRetainedCapability) Kill(context.Context) (containment.SignalResult, error) {
	return capability.signalRetained(false)
}

func (capability *fakeNativeRetainedCapability) PlacePID(_ context.Context, pid int) error {
	if err := capability.usable(); err != nil {
		return err
	}
	if capability.manager.placeErr != nil {
		return capability.manager.placeErr
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	capability.leaf.placedPIDs = append(capability.leaf.placedPIDs, pid)
	capability.leaf.membership = containment.RetainedMembershipPresent
	return nil
}

func (capability *fakeNativeRetainedCapability) PlaceProcess(ctx context.Context, expected procgroup.ProcessClaim) error {
	observation := procgroup.ObserveProcess(expected)
	if observation.Identity != model.ProcessIdentityMatching || observation.RunState != procgroup.ProcessRunStateRunning {
		return fmt.Errorf("fake retained placement identity observation = %+v, want matching running", observation)
	}
	return capability.PlacePID(ctx, expected.PID)
}

func (capability *fakeNativeRetainedCapability) ReleaseRootLease() error {
	if err := capability.usable(); err != nil {
		return err
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	capability.leaf.rootLeaseReleases++
	return nil
}

func (capability *fakeNativeRetainedCapability) Remove(context.Context) error {
	if err := capability.usable(); err != nil {
		return err
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	if capability.leaf.membership != containment.RetainedMembershipEmpty {
		return fmt.Errorf("retained leaf still populated")
	}
	capability.leaf.removeCalls++
	if capability.leaf.removeErr != nil {
		return capability.leaf.removeErr
	}
	if capability.leaf.removed {
		return nil
	}
	capability.leaf.removed = true
	return nil
}

func (capability *fakeNativeRetainedCapability) Release() error {
	if capability == nil || capability.released {
		return nil
	}
	capability.released = true
	if capability.manager != nil && capability.leaf != nil {
		capability.manager.mu.Lock()
		capability.leaf.releases++
		capability.manager.mu.Unlock()
	}
	return nil
}

func (capability *fakeNativeRetainedCapability) BeginGroupContinuity(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, observedAt time.Time) containment.GroupContinuity {
	_ = ctx
	if capability == nil || observedAt.IsZero() || observation.Group != model.GroupLive || observation.Leader != model.ProcessIdentityMatching {
		return brokenContinuity{}
	}
	identity := capability.Identity()
	if identity.RetainedID != target.RetainedID || !identity.KernelDomainID.ProvablySame(target.KernelDomain()) {
		return brokenContinuity{}
	}
	return fakeNativeRetainedContinuity{capability: capability, begin: observedAt}
}

func (continuity fakeNativeRetainedContinuity) ConfirmContinuouslyLive(ctx context.Context, target model.GroupRef, observation model.ContainmentObservation, begin, end time.Time) containment.GroupContinuityEvidence {
	_ = ctx
	if continuity.capability == nil || begin.Before(continuity.begin) || end.Before(begin) || observation.Group != model.GroupLive {
		return containment.GroupContinuityEvidence{}
	}
	evidence, err := containment.NewGroupContinuityEvidence(target, begin, end)
	if err != nil {
		return containment.GroupContinuityEvidence{}
	}
	return evidence
}

func (capability *fakeNativeRetainedCapability) signalRetained(term bool) (containment.SignalResult, error) {
	if err := capability.usable(); err != nil {
		return containment.SignalUnprovable, err
	}
	capability.manager.mu.Lock()
	defer capability.manager.mu.Unlock()
	if term {
		capability.leaf.termCalls++
	} else {
		capability.leaf.killCalls++
	}
	if capability.leaf.membership == containment.RetainedMembershipEmpty {
		return containment.SignalTargetAbsent, nil
	}
	if term && capability.leaf.ignoreTerm {
		return containment.SignalDelivered, nil
	}
	capability.leaf.membership = containment.RetainedMembershipEmpty
	return containment.SignalDelivered, nil
}

func (capability *fakeNativeRetainedCapability) usable() error {
	if capability == nil || capability.released || capability.leaf == nil || capability.manager == nil {
		return errors.New("fake retained capability is closed")
	}
	return nil
}

func nativeHelperArgs() ([]string, bool) {
	for i, arg := range os.Args {
		if arg == "--" && i+1 <= len(os.Args) {
			return os.Args[i+1:], true
		}
	}
	return nil, false
}

func runNativeSimpleHelper(args []string) int {
	fs := flag.NewFlagSet("native-simple", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" {
		return 2
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	fmt.Printf("native-simple pid=%d pgid=%d\n", os.Getpid(), pgid)
	return writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid})
}

func runNativeTermGrandchildHelper(args []string) int {
	fs := flag.NewFlagSet("native-term-grandchild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	grandchildReady := fs.String("grandchild-ready", "", "grandchild ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" || *grandchildReady == "" {
		return 2
	}
	exe, err := os.Executable()
	if err != nil {
		return 3
	}
	cmd := exec.Command(exe,
		"-test.run=^TestNativeHelperGrandchildProcess$",
		"--",
		"--ready", *grandchildReady,
	)
	cmd.Env = append(os.Environ(), nativeHelperEnv+"="+nativeHelperGrandchild)
	cmd.Dir = filepath.Dir(exe)
	if err := cmd.Start(); err != nil {
		return 4
	}
	if err := waitForFile(*grandchildReady, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return 5
	}
	childPGID, err := readGrandchildPGID(*grandchildReady)
	if err != nil {
		_ = cmd.Process.Kill()
		return 6
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		_ = cmd.Process.Kill()
		return 7
	}
	if code := writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid, GrandchildPID: cmd.Process.Pid, GrandchildPGID: childPGID}); code != 0 {
		_ = cmd.Process.Kill()
		return code
	}
	fmt.Printf("term-grandchild-ready pid=%d pgid=%d child=%d\n", os.Getpid(), pgid, cmd.Process.Pid)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	return 0
}

func runNativeIgnoreTermGrandchildHelper(args []string) int {
	fs := flag.NewFlagSet("native-ignore-term-grandchild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	grandchildReady := fs.String("grandchild-ready", "", "grandchild ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" || *grandchildReady == "" {
		return 2
	}
	signal.Ignore(syscall.SIGTERM)
	exe, err := os.Executable()
	if err != nil {
		return 3
	}
	cmd := exec.Command(exe,
		"-test.run=^TestNativeHelperGrandchildProcess$",
		"--",
		"--ready", *grandchildReady,
	)
	cmd.Env = append(os.Environ(), nativeHelperEnv+"="+nativeHelperGrandchild)
	cmd.Dir = filepath.Dir(exe)
	if err := cmd.Start(); err != nil {
		return 4
	}
	if err := waitForFile(*grandchildReady, 5*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return 5
	}
	childPGID, err := readGrandchildPGID(*grandchildReady)
	if err != nil {
		_ = cmd.Process.Kill()
		return 6
	}
	pgid, err := unix.Getpgid(0)
	if err != nil {
		_ = cmd.Process.Kill()
		return 7
	}
	if code := writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid, GrandchildPID: cmd.Process.Pid, GrandchildPGID: childPGID}); code != 0 {
		_ = cmd.Process.Kill()
		return code
	}
	fmt.Printf("ignore-term-grandchild-ready pid=%d pgid=%d child=%d\n", os.Getpid(), pgid, cmd.Process.Pid)
	for {
		time.Sleep(time.Hour)
	}
}

func runNativeIgnoreTermLeaderHelper(args []string) int {
	fs := flag.NewFlagSet("native-ignore-term-leader", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	resultPath := fs.String("result", "", "result path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *resultPath == "" {
		return 2
	}
	signal.Ignore(syscall.SIGTERM)
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	if code := writeNativeBackendResult(*resultPath, nativeBackendResult{PID: os.Getpid(), PGID: pgid}); code != 0 {
		return code
	}
	fmt.Printf("ignore-term-leader-ready pid=%d pgid=%d\n", os.Getpid(), pgid)
	for {
		time.Sleep(time.Hour)
	}
}

func runNativeGrandchildHelper(args []string) int {
	fs := flag.NewFlagSet("native-grandchild", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	readyPath := fs.String("ready", "", "ready path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *readyPath == "" {
		return 2
	}
	signal.Ignore(syscall.SIGTERM)
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return 3
	}
	if err := os.WriteFile(*readyPath, []byte(strconv.Itoa(pgid)+"\n"), 0o600); err != nil {
		return 3
	}
	for {
		time.Sleep(time.Hour)
	}
}

func runNativeMonitorHelper(args []string) int {
	fs := flag.NewFlagSet("native-monitor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	daemonFD := fs.Int("daemon-fd", -1, "daemon fd")
	targetFD := fs.Int("target-fd", -1, "target fd")
	readyFD := fs.Int("ready-fd", -1, "ready fd")
	leafFD := fs.Int("leaf-fd", -1, "leaf fd")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *daemonFD < 3 || *targetFD < 3 || *readyFD < 3 {
		return 2
	}
	var containmentImpl parklaunch.Containment = RealContainment{Params: defaultNativeTestParams()}
	if os.Getenv(nativeHelperRetainedNoopMonitor) == "1" {
		containmentImpl = noopNativeMonitorContainment{}
	} else {
		if *leafFD < 3 {
			return 2
		}
		// The SAME composition point production uses
		// (nativecustody.RunMonitorFromFDs) — never a hand-mirrored copy.
		containmentImpl = NewInheritedMonitorContainment(defaultNativeTestParams(), *leafFD)
	}
	if delayPath := os.Getenv(nativeHelperMonitorDelayReady); delayPath != "" {
		containmentImpl = delayReadyNativeMonitorContainment{
			inner: containmentImpl,
			path:  delayPath,
		}
	}
	err := parklaunch.RunMonitorFromFDs(context.Background(), *daemonFD, *targetFD, *readyFD, containmentImpl)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 3
	}
	return 0
}

type noopNativeMonitorContainment struct{}

func (noopNativeMonitorContainment) Contain(context.Context, model.GroupRef) error {
	return nil
}

func (containment noopNativeMonitorContainment) BindContainmentTarget(context.Context, model.GroupRef) (parklaunch.Containment, error) {
	return containment, nil
}

type delayReadyNativeMonitorContainment struct {
	inner parklaunch.Containment
	path  string
}

func (containment delayReadyNativeMonitorContainment) Contain(ctx context.Context, group model.GroupRef) error {
	if containment.inner == nil {
		return nil
	}
	return containment.inner.Contain(ctx, group)
}

func (containment delayReadyNativeMonitorContainment) BindContainmentTarget(ctx context.Context, group model.GroupRef) (parklaunch.Containment, error) {
	bound := containment.inner
	if binder, ok := bound.(parklaunch.TargetBindingContainment); ok {
		next, err := binder.BindContainmentTarget(ctx, group)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, fmt.Errorf("delayed monitor target binding returned nil")
		}
		bound = next
	}
	if nativeMonitorDelayUsesFIFO(containment.path) {
		return bound, waitNativeMonitorDelayFIFO(ctx, containment.path)
	}
	if err := os.WriteFile(containment.path+".entered", []byte("1\n"), 0o600); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(containment.path); errors.Is(err, os.ErrNotExist) {
			return bound, nil
		} else if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func nativeMonitorDelayUsesFIFO(path string) bool {
	return nativeMonitorDelayPathIsFIFO(path+".entered") && nativeMonitorDelayPathIsFIFO(path+".release")
}

func nativeMonitorDelayPathIsFIFO(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode()&os.ModeNamedPipe != 0
}

func waitNativeMonitorDelayFIFO(ctx context.Context, path string) error {
	entered, err := os.OpenFile(path+".entered", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if _, err := entered.Write([]byte{'1'}); err != nil {
		_ = entered.Close()
		return err
	}
	if err := entered.Close(); err != nil {
		return err
	}
	releaseFD, err := unix.Open(path+".release", unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(releaseFD)
	var ack [1]byte
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		n, err := unix.Read(releaseFD, ack[:])
		if n == 1 {
			if ack[0] != '1' {
				return fmt.Errorf("unexpected monitor delay release byte %q", ack[0])
			}
			return nil
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func writeNativeBackendResult(path string, result nativeBackendResult) int {
	raw, err := json.Marshal(result)
	if err != nil {
		return 7
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return 8
	}
	return 0
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", path)
}

func readGrandchildPGID(path string) (int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pgid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, err
	}
	if pgid <= 0 {
		return 0, fmt.Errorf("grandchild pgid must be positive")
	}
	return pgid, nil
}
