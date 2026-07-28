//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
)

func TestNativeSelfTestClassificationTable(t *testing.T) {
	unsafeCauses := []struct {
		name  string
		cause error
	}{
		{name: "uncertain release unproven cleanup", cause: errors.New("uncertain release and unproven cleanup")},
		{name: "exec before release", cause: errors.New("exec-before-release marker")},
		{name: "cannot prove probe cgroup empty", cause: errors.New("probe cgroup not empty")},
		{name: "leaked fds preventing monitor eof", cause: errors.New("monitor eof blocked by leaked fd")},
		{name: "contradictory identity", cause: errors.New("contradictory identity")},
		{name: "attestation mismatch", cause: errors.New("attestation mismatch")},
	}
	for _, tt := range unsafeCauses {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
				calls++
				return unsafeNativeSelfTest(tt.cause, false)
			})
			if calls != 1 {
				t.Fatalf("attempt calls = %d, want 1", calls)
			}
			if assessment.Class != SupportUnsafe || !errors.Is(assessment.Cause, tt.cause) {
				t.Fatalf("assessment = %+v, want unsafe cause %v", assessment, tt.cause)
			}
		})
	}

	t.Run("never created failures are retryable to bound", func(t *testing.T) {
		cause := errors.New("process never created")
		assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
			return retryableNativeSelfTest(cause, true)
		})
		if assessment.Class != SupportRetryable || assessment.Attempts != 3 || !assessment.CleanupSafe || !errors.Is(assessment.Cause, cause) {
			t.Fatalf("assessment = %+v, want retryable attempts=3 cleanup-safe cause", assessment)
		}
	})

	t.Run("fully cleaned retry followed by success is available", func(t *testing.T) {
		calls := 0
		cause := errors.New("transient")
		assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
			calls++
			if calls == 1 {
				return retryableNativeSelfTest(cause, true)
			}
			return nativeSelfTestAttemptResult{Class: SupportAvailable, CleanupSafe: true}
		})
		if assessment.Class != SupportAvailable || assessment.Attempts != 2 || !assessment.CleanupSafe {
			t.Fatalf("assessment = %+v, want available attempts=2 cleanup-safe", assessment)
		}
	})

	t.Run("cleanup unverifiable escalates unsafe and stops", func(t *testing.T) {
		calls := 0
		cause := errors.New("cleanup unverified")
		assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
			calls++
			if calls == 1 {
				return retryableNativeSelfTest(cause, false)
			}
			return nativeSelfTestAttemptResult{Class: SupportAvailable, CleanupSafe: true}
		})
		if calls != 1 {
			t.Fatalf("attempt calls = %d, want 1", calls)
		}
		if assessment.Class != SupportUnsafe || assessment.Attempts != 1 || assessment.CleanupSafe || !errors.Is(assessment.Cause, cause) {
			t.Fatalf("assessment = %+v, want unsafe attempts=1 cleanup unsafe cause", assessment)
		}
	})

	t.Run("unsupported stops without retry", func(t *testing.T) {
		calls := 0
		cause := errors.New("unsupported platform")
		assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
			calls++
			return unsupportedNativeSelfTest(cause, true)
		})
		if calls != 1 {
			t.Fatalf("attempt calls = %d, want 1", calls)
		}
		if assessment.Class != SupportUnsupported || assessment.Attempts != 1 || !assessment.CleanupSafe || !errors.Is(assessment.Cause, cause) {
			t.Fatalf("assessment = %+v, want unsupported attempts=1 cleanup-safe cause", assessment)
		}
	})
}

func TestNativeSelfTestRootLeaseContentionIsRetryable(t *testing.T) {
	calls := 0
	cause := nativePrepareFailure(fmt.Errorf("prepare: %w", cgroup.ErrRootLeaseUnavailable), false, true)
	assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
		calls++
		return classifyNativeSelfTestPrepareFailure(cause)
	})
	if calls != 3 {
		t.Fatalf("attempt calls = %d, want 3", calls)
	}
	if assessment.Class != SupportRetryable || assessment.Attempts != 3 || !assessment.CleanupSafe {
		t.Fatalf("assessment = %+v, want retryable attempts=3 cleanup safe", assessment)
	}
	if !errors.Is(assessment.Cause, ErrNativeRuntimeSelfTestRetry) || !errors.Is(assessment.Cause, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("assessment cause = %v, want retryable wrapping ErrRootLeaseUnavailable", assessment.Cause)
	}
}

func TestNativeSelfTestContradictoryAvailableIsUnsafe(t *testing.T) {
	cause := errors.New("available result carried cause")
	tests := []struct {
		name   string
		result nativeSelfTestAttemptResult
	}{
		{
			name: "available with cause",
			result: nativeSelfTestAttemptResult{
				Class:       SupportAvailable,
				Cause:       cause,
				CleanupSafe: true,
			},
		},
		{
			name: "available with unsafe cleanup",
			result: nativeSelfTestAttemptResult{
				Class:       SupportAvailable,
				CleanupSafe: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
				return tt.result
			})
			if assessment.Class != SupportUnsafe || assessment.Attempts != 1 || assessment.CleanupSafe {
				t.Fatalf("assessment = %+v, want unsafe attempts=1 cleanup unsafe", assessment)
			}
			if !errors.Is(assessment.Cause, ErrNativeRuntimeSelfTestUnsafe) {
				t.Fatalf("assessment cause = %v, want ErrNativeRuntimeSelfTestUnsafe", assessment.Cause)
			}
			if tt.result.Cause != nil && !errors.Is(assessment.Cause, tt.result.Cause) {
				t.Fatalf("assessment cause = %v, want preserved cause %v", assessment.Cause, tt.result.Cause)
			}
		})
	}
}

func TestNativeSelfTestPrepareFailureEvidenceClassification(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantClass   SupportClass
		cleanupSafe bool
	}{
		{
			name:        "no creation is retryable",
			err:         nativePrepareFailure(errors.New("process never created"), false, true),
			wantClass:   SupportRetryable,
			cleanupSafe: true,
		},
		{
			name:        "created and cleanup verified is retryable",
			err:         nativePrepareFailure(errors.New("created then cleaned"), true, true),
			wantClass:   SupportRetryable,
			cleanupSafe: true,
		},
		{
			name:        "worker created then cleanup failure is unsafe",
			err:         nativePrepareFailure(errors.New("worker created cleanup failed"), true, false),
			wantClass:   SupportUnsafe,
			cleanupSafe: false,
		},
		{
			name:        "identity contradiction without evidence is unsafe",
			err:         errors.New("identity contradiction after retained creation"),
			wantClass:   SupportUnsafe,
			cleanupSafe: false,
		},
		{
			name:        "armed monitor cleanup failure is unsafe",
			err:         nativePrepareFailure(errors.New("armed monitor cleanup failed"), true, false),
			wantClass:   SupportUnsafe,
			cleanupSafe: false,
		},
		{
			name:        "cancellation mid prepare without cleanup proof is unsafe",
			err:         errors.Join(context.Canceled, errors.New("prepare canceled after worker start")),
			wantClass:   SupportUnsafe,
			cleanupSafe: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := classifyNativeSelfTestPrepareFailure(tt.err)
			if result.Class != tt.wantClass || result.CleanupSafe != tt.cleanupSafe {
				t.Fatalf("classifyNativeSelfTestPrepareFailure() = %+v, want class=%s cleanupSafe=%t", result, tt.wantClass, tt.cleanupSafe)
			}
			if result.Cause == nil || !errors.Is(result.Cause, tt.err) {
				t.Fatalf("classification cause = %v, want wrapped %v", result.Cause, tt.err)
			}
		})
	}
}

func TestNativeRuntimeConstructionContentionIsSingleShotRetryable(t *testing.T) {
	cause := fmt.Errorf("hold root lease: %w", cgroup.ErrRootLeaseUnavailable)
	assessment := nativeRuntimeConstructionAssessment(cause)
	if assessment.Class != SupportRetryable || assessment.Attempts != 1 || !assessment.CleanupSafe || !errors.Is(assessment.Cause, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("nativeRuntimeConstructionAssessment() = %+v, want retryable attempts=1 cleanup-safe ErrRootLeaseUnavailable", assessment)
	}
}

func TestNativeSelfTestExecSpecUsesConfiguredAgentbusPath(t *testing.T) {
	agentbusPath := filepath.Join(t.TempDir(), "agentbus")
	spec, markerPath, cleanup, err := nativeSelfTestExecSpec(agentbusPath)
	if err != nil {
		t.Fatalf("nativeSelfTestExecSpec() error = %v", err)
	}
	defer cleanup()
	if len(spec.Argv) < 2 || spec.Argv[0] != agentbusPath {
		t.Fatalf("self-test argv = %v, want argv[0] configured agentbus path %s", spec.Argv, agentbusPath)
	}
	if spec.Argv[1] != nativeSelfTestFixtureCommand {
		t.Fatalf("self-test argv[1] = %q, want %q", spec.Argv[1], nativeSelfTestFixtureCommand)
	}
	if err := requireSelfTestFixtureAbsent(markerPath); err != nil {
		t.Fatalf("fresh marker absence = %v", err)
	}
}

func TestNativeSelfTestQualificationRequiresActiveQuiescenceMethod(t *testing.T) {
	group := testPhysicalQuiescence().Group
	for _, tt := range []struct {
		name       string
		method     model.QuiescenceMethod
		wantClass  SupportClass
		wantActive bool
	}{
		{name: "already absent rejected", method: model.QuiescenceAlreadyAbsent, wantClass: SupportUnsupported},
		{name: "natural exit rejected", method: model.QuiescenceNaturalExit, wantClass: SupportUnsupported},
		{name: "term kill accepted", method: model.QuiescenceTermKill, wantClass: SupportAvailable, wantActive: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			issuer, verifier := NewAttestationChannel()
			verified, err := issuer.AttestQuiescence(PhysicalQuiescence{Group: group, Method: tt.method})
			if err != nil {
				t.Fatalf("AttestQuiescence() error = %v", err)
			}
			assessment := runClassifiedNativeSelfTest(context.Background(), 1, func(context.Context, int) nativeSelfTestAttemptResult {
				physical, err := verifySelfTestAttestation(verifier, verified, group)
				if err != nil {
					return unsafeNativeSelfTest(err, true)
				}
				if err := requireSelfTestActiveQuiescenceMethod(physical); err != nil {
					return unsupportedNativeSelfTest(err, true)
				}
				return nativeSelfTestAttemptResult{Class: SupportAvailable, CleanupSafe: true}
			})
			if assessment.Class != tt.wantClass {
				t.Fatalf("assessment = %+v, want class=%s", assessment, tt.wantClass)
			}
			if tt.wantActive && assessment.Cause != nil {
				t.Fatalf("active assessment cause = %v, want nil", assessment.Cause)
			}
			if !tt.wantActive && !errors.Is(assessment.Cause, ErrNativeRuntimeUnsupported) {
				t.Fatalf("assessment cause = %v, want ErrNativeRuntimeUnsupported", assessment.Cause)
			}
		})
	}
}

func TestNativeSelfTestRecoveredSchedulingTimeoutIsRetryable(t *testing.T) {
	calls := 0
	timeoutCause := fmt.Errorf("%w: fixture did not execute before containment: %w", ErrNativeRuntimeSelfTestUnsafe, context.DeadlineExceeded)
	assessment := runClassifiedNativeSelfTest(context.Background(), 2, func(context.Context, int) nativeSelfTestAttemptResult {
		calls++
		if calls == 1 {
			return classifyNativeSelfTestRecoveredFailure(timeoutCause, nativeSelfTestCleanupResult{CleanupSafe: true})
		}
		return nativeSelfTestAttemptResult{Class: SupportAvailable, CleanupSafe: true}
	})
	if assessment.Class != SupportAvailable || assessment.Attempts != 2 || !assessment.CleanupSafe {
		t.Fatalf("assessment = %+v, want retry after recovered timeout then available", assessment)
	}

	recoveryErr := errors.New("recovery containment failed")
	result := classifyNativeSelfTestRecoveredFailure(timeoutCause, nativeSelfTestCleanupResult{Err: recoveryErr, CleanupSafe: true})
	if result.Class != SupportUnsafe || !result.CleanupSafe || !errors.Is(result.Cause, recoveryErr) {
		t.Fatalf("failed recovery classification = %+v, want unsafe cleanup-safe preserving recovery error", result)
	}
}

func TestNativeSelfTestRetainedActiveContainmentRecordsTermKill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	native.issuer = issuer
	spec, _ := nativeIgnoreTermLeaderLaunchSpec(t)

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "ignore-term-leader-ready")
	ref := running.Ref()
	manager.setTermIgnored(ref.RetainedID, true)
	manager.setSignalProcessGroup(ref.RetainedID, true)

	verified, cleanup, err := running.containSelfTestActiveAndVerify(ctx, QuiescenceCauseContain)
	if err != nil {
		t.Fatalf("containSelfTestActiveAndVerify() error = %v", err)
	}
	if cleanup.Err != nil {
		t.Fatalf("containSelfTestActiveAndVerify() cleanup error = %v", cleanup.Err)
	}
	physical, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence() error = %v", err)
	}
	if !physical.Group.Equal(ref) || physical.Method != model.QuiescenceTermKill {
		t.Fatalf("self-test physical = %+v, want term_kill for %+v", physical, ref)
	}
	leaf := manager.leafForRetainedID(t, ref.RetainedID)
	if leaf.termCalls != 1 || leaf.killCalls != 1 {
		t.Fatalf("retained signal calls term/kill = %d/%d, want 1/1", leaf.termCalls, leaf.killCalls)
	}
	if err := native.Close(); err != nil {
		t.Fatalf("NativeCustodian.Close() error = %v", err)
	}
}

func TestNativeSelfTestFailurePathLastResortReapsFixtureAndAllowsClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	issuer, verifier := NewAttestationChannel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	native.issuer = issuer
	spec, _ := nativeIgnoreTermLeaderLaunchSpec(t)
	killErr := errors.New("retained kill failed")

	running, err := native.Launch(ctx, spec)
	if err != nil {
		t.Fatalf("Launch() error = %v", err)
	}
	defer cleanupNativeRunning(t, running)
	waitNativeReadyLine(t, running.Stdout(), "ignore-term-leader-ready")
	ref := running.Ref()
	manager.setTermIgnored(ref.RetainedID, true)
	manager.setKillErr(ref.RetainedID, killErr)
	manager.setSyncMembershipWithProcessGroup(ref.RetainedID, ref.PGID)

	recovery := native.recoverSelfTestRunningCleanup(ctx, verifier, running, ref)
	if !recovery.CleanupSafe {
		t.Fatalf("recoverSelfTestRunningCleanup() = %+v, want cleanup safe after last-resort teardown", recovery)
	}
	if recovery.Err == nil || !errors.Is(recovery.Err, killErr) {
		t.Fatalf("recoverSelfTestRunningCleanup() error = %v, want retained kill failure preserved", recovery.Err)
	}
	waitGroupAbsent(t, ref, 5*time.Second)
	if err := native.Close(); err != nil {
		t.Fatalf("NativeCustodian.Close() error = %v", err)
	}
}

func TestNewNativeRuntimeDarwinQualifiesAfterSelfTest(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin strict runtime qualification is macOS-only")
	}
	exe := nativeTestBinaryPath(t)
	runtimeBundle, err := NewNativeRuntime(NativeOptions{
		AgentbusPath:      builtNativeAgentbusPath(t),
		MonitorCommand:    nativeMonitorCommand(t),
		ContainmentParams: defaultNativeTestParams(),
		WorkerEnv:         nativeAgentbusEnv(),
		WorkerDir:         filepath.Dir(exe),
	})
	if err != nil {
		t.Fatalf("NewNativeRuntime() error = %v", err)
	}
	defer func() {
		if err := runtimeBundle.Close(); err != nil {
			t.Fatalf("native runtime Close() error = %v", err)
		}
	}()

	assessment := runtimeBundle.SupportAssessment()
	if assessment.Class != SupportUnsupported || !errors.Is(assessment.Cause, ErrNativeRuntimeSelfTestRequired) || assessment.Attempts != 0 || !assessment.CleanupSafe {
		t.Fatalf("initial SupportAssessment() = %+v, want self-test-required with no attempts and cleanup safe", assessment)
	}
	if _, ok := runtimeBundle.Process().(*NativeCustodian); !ok {
		t.Fatalf("Darwin Process() = %T, want *NativeCustodian", runtimeBundle.Process())
	}
	support := runtimeBundle.SelfTest(context.Background())
	if support.Assessment.Class != SupportAvailable || !support.RuntimeProbePassed || !support.ParkedExec || !support.VerifiedContainment || support.RuntimeProbeResult != nil {
		t.Fatalf("Darwin support after SelfTest = %+v, want passed parked exec and verified containment probe", support)
	}
	native, ok := runtimeBundle.Process().(*NativeCustodian)
	if !ok || native == nil {
		t.Fatalf("Darwin Process() after SelfTest = %T, want *NativeCustodian", runtimeBundle.Process())
	}
	if native.selfTest.Method != model.QuiescenceTermKill {
		t.Fatalf("Darwin self-test quiescence method = %s, want %s", native.selfTest.Method, model.QuiescenceTermKill)
	}
}
