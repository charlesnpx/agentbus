//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
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

func TestNativeSelfTestPostConstructionRootLeaseLossIsUnsafe(t *testing.T) {
	calls := 0
	cause := nativePrepareFailure(fmt.Errorf("prepare: %w", cgroup.ErrRootLeaseUnavailable), false, true)
	assessment := runClassifiedNativeSelfTest(context.Background(), 3, func(context.Context, int) nativeSelfTestAttemptResult {
		calls++
		return classifyNativeSelfTestPrepareFailure(cause)
	})
	if calls != 1 {
		t.Fatalf("attempt calls = %d, want 1", calls)
	}
	if assessment.Class != SupportUnsafe || assessment.Attempts != 1 || assessment.CleanupSafe {
		t.Fatalf("assessment = %+v, want unsafe attempts=1 cleanup unsafe", assessment)
	}
	if !errors.Is(assessment.Cause, ErrNativeRuntimeSelfTestUnsafe) || !errors.Is(assessment.Cause, cgroup.ErrRootLeaseUnavailable) {
		t.Fatalf("assessment cause = %v, want unsafe wrapping ErrRootLeaseUnavailable", assessment.Cause)
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

func TestNativeSelfTestExecSpecUsesCurrentExecutable(t *testing.T) {
	spec, markerPath, cleanup, err := nativeSelfTestExecSpec()
	if err != nil {
		t.Fatalf("nativeSelfTestExecSpec() error = %v", err)
	}
	defer cleanup()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Argv) < 2 || spec.Argv[0] != exe {
		t.Fatalf("self-test argv = %v, want argv[0] current executable %s", spec.Argv, exe)
	}
	if spec.Argv[1] != nativeSelfTestFixtureCommand {
		t.Fatalf("self-test argv[1] = %q, want %q", spec.Argv[1], nativeSelfTestFixtureCommand)
	}
	if err := requireSelfTestFixtureAbsent(markerPath); err != nil {
		t.Fatalf("fresh marker absence = %v", err)
	}
}

func TestNewNativeRuntimeDarwinUnsupported(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin strict-unavailable support is macOS-only")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.Abs(exe)
	if err != nil {
		t.Fatal(err)
	}
	runtimeBundle, err := NewNativeRuntime(NativeOptions{
		AgentbusPath: exe,
		MonitorCommand: parklaunch.CommandSpec{
			Path: exe,
		},
		ContainmentParams: containment.Params{
			GracePeriod:  20 * time.Millisecond,
			PollInterval: 20 * time.Millisecond,
			PollTimeout:  2 * time.Second,
		},
		WorkerDir: filepath.Dir(exe),
	})
	if err == nil {
		t.Fatal("NewNativeRuntime() error = nil, want Darwin unsupported cause")
	}
	assessment := runtimeBundle.SupportAssessment()
	if assessment.Class != SupportUnsupported || assessment.Cause == nil || assessment.Attempts != 0 || !assessment.CleanupSafe {
		t.Fatalf("SupportAssessment() = %+v, want Darwin unsupported with no attempts and cleanup safe", assessment)
	}
	support := runtimeBundle.Support()
	if support.ParkedExec || support.VerifiedContainment || support.RuntimeProbePassed {
		t.Fatalf("Darwin support = %+v, want strict unavailable capability flags", support)
	}
	if _, ok := runtimeBundle.Process().(UnavailableCustodian); !ok {
		t.Fatalf("Darwin Process() = %T, want UnavailableCustodian", runtimeBundle.Process())
	}
}
