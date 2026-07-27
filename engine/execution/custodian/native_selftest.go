//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
)

const (
	nativeSelfTestMaxAttempts    = 3
	nativeSelfTestAttemptTimeout = 10 * time.Second
	nativeSelfTestFixturePoll    = 20 * time.Millisecond
	nativeSelfTestFixtureCommand = "internal-native-self-test-fixture"
)

var (
	ErrNativeRuntimeUnsupported    = errors.New("native runtime unsupported")
	ErrNativeRuntimeSelfTestRetry  = errors.New("native runtime self-test retryable")
	ErrNativeRuntimeSelfTestUnsafe = errors.New("native runtime self-test unsafe")
)

type nativeRuntimeSelfTestRecord struct {
	Ref             model.GroupRef
	ExecPath        string
	Method          model.QuiescenceMethod
	Attempts        int
	CleanupVerified bool
}

type nativeSelfTestAttemptResult struct {
	Class       SupportClass
	Cause       error
	CleanupSafe bool
	Ref         model.GroupRef
	ExecPath    string
}

type nativeSelfTestAttemptFunc func(context.Context, int) nativeSelfTestAttemptResult

type nativePrepareFailureError struct {
	cause           error
	created         bool
	cleanupVerified bool
}

func (err *nativePrepareFailureError) Error() string {
	if err == nil || err.cause == nil {
		return "native prepare failed"
	}
	return err.cause.Error()
}

func (err *nativePrepareFailureError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}

func nativePrepareFailure(cause error, created, cleanupVerified bool) error {
	if cause == nil {
		return nil
	}
	return &nativePrepareFailureError{
		cause:           cause,
		created:         created,
		cleanupVerified: cleanupVerified,
	}
}

func nativePrepareFailureEvidence(err error) (created bool, cleanupVerified bool, ok bool) {
	var prepared *nativePrepareFailureError
	if !errors.As(err, &prepared) || prepared == nil {
		return false, false, false
	}
	return prepared.created, prepared.cleanupVerified, true
}

func nativeRuntimeConstructionAssessment(cause error) SupportAssessment {
	if cause == nil {
		return SupportAssessment{
			Class:       SupportAvailable,
			Attempts:    0,
			CleanupSafe: true,
		}
	}
	if errors.Is(cause, cgroup.ErrRootLeaseUnavailable) {
		return SupportAssessment{
			Class:       SupportRetryable,
			Cause:       cause,
			Attempts:    1,
			CleanupSafe: true,
		}
	}
	class := SupportUnsupported
	if !nativeRuntimePlatformUnsupportedError(cause) {
		class = SupportUnsafe
	}
	return SupportAssessment{
		Class:       class,
		Cause:       cause,
		CleanupSafe: true,
	}
}

func (custodian *NativeCustodian) SelfTest(ctx context.Context, verifier AttestationVerifier) SupportAssessment {
	if ctx == nil {
		ctx = context.Background()
	}
	return runClassifiedNativeSelfTest(ctx, nativeSelfTestMaxAttempts, func(parent context.Context, attempt int) nativeSelfTestAttemptResult {
		attemptCtx, cancel := context.WithTimeout(parent, nativeSelfTestAttemptTimeout)
		defer cancel()
		return custodian.selfTestAttempt(attemptCtx, verifier, attempt)
	})
}

func runClassifiedNativeSelfTest(ctx context.Context, maxAttempts int, attempt nativeSelfTestAttemptFunc) SupportAssessment {
	if ctx == nil {
		ctx = context.Background()
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if attempt == nil {
		return SupportAssessment{
			Class:       SupportUnsafe,
			Cause:       fmt.Errorf("%w: self-test attempt function is nil", ErrNativeRuntimeSelfTestUnsafe),
			CleanupSafe: false,
		}
	}
	cleanupSafe := true
	var lastCause error
	for i := 1; i <= maxAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return SupportAssessment{
				Class:       SupportRetryable,
				Cause:       fmt.Errorf("%w: %w", ErrNativeRuntimeSelfTestRetry, err),
				Attempts:    i - 1,
				CleanupSafe: cleanupSafe,
			}
		}
		result := normalizeNativeSelfTestAttemptResult(attempt(ctx, i))
		cleanupSafe = cleanupSafe && result.CleanupSafe
		switch result.Class {
		case SupportAvailable:
			return SupportAssessment{
				Class:       SupportAvailable,
				Attempts:    i,
				CleanupSafe: cleanupSafe,
			}
		case SupportUnsupported:
			return SupportAssessment{
				Class:       SupportUnsupported,
				Cause:       result.Cause,
				Attempts:    i,
				CleanupSafe: cleanupSafe,
			}
		case SupportUnsafe:
			return SupportAssessment{
				Class:       SupportUnsafe,
				Cause:       result.Cause,
				Attempts:    i,
				CleanupSafe: cleanupSafe,
			}
		case SupportRetryable:
			lastCause = result.Cause
			if !result.CleanupSafe {
				return SupportAssessment{
					Class: SupportUnsafe,
					Cause: errors.Join(
						fmt.Errorf("%w: retry denied by unverified cleanup", ErrNativeRuntimeSelfTestUnsafe),
						result.Cause,
					),
					Attempts:    i,
					CleanupSafe: false,
				}
			}
			if i == maxAttempts {
				return SupportAssessment{
					Class:       SupportRetryable,
					Cause:       result.Cause,
					Attempts:    i,
					CleanupSafe: cleanupSafe,
				}
			}
		default:
			return SupportAssessment{
				Class:       SupportUnsafe,
				Cause:       fmt.Errorf("%w: invalid self-test class %d", ErrNativeRuntimeSelfTestUnsafe, result.Class),
				Attempts:    i,
				CleanupSafe: false,
			}
		}
	}
	if lastCause == nil {
		lastCause = ErrNativeRuntimeSelfTestRetry
	}
	return SupportAssessment{
		Class:       SupportRetryable,
		Cause:       lastCause,
		Attempts:    maxAttempts,
		CleanupSafe: cleanupSafe,
	}
}

func normalizeNativeSelfTestAttemptResult(result nativeSelfTestAttemptResult) nativeSelfTestAttemptResult {
	switch result.Class {
	case SupportAvailable:
		if result.Cause != nil || !result.CleanupSafe {
			result.Cause = errors.Join(
				fmt.Errorf("%w: contradictory available self-test result", ErrNativeRuntimeSelfTestUnsafe),
				result.Cause,
			)
			result.Class = SupportUnsafe
			result.CleanupSafe = false
		}
	case SupportRetryable, SupportUnsupported, SupportUnsafe:
		if result.Cause == nil {
			result.Cause = fmt.Errorf("%w: missing self-test cause", ErrNativeRuntimeSelfTestUnsafe)
			result.Class = SupportUnsafe
			result.CleanupSafe = false
		}
	default:
		result.Cause = fmt.Errorf("%w: invalid self-test class %d", ErrNativeRuntimeSelfTestUnsafe, result.Class)
		result.Class = SupportUnsafe
		result.CleanupSafe = false
	}
	return result
}

func classifyNativeSelfTestPrepareFailure(err error) nativeSelfTestAttemptResult {
	if nativeRuntimePlatformUnsupportedError(err) {
		return unsupportedNativeSelfTest(fmt.Errorf("%w: %w", ErrNativeRuntimeUnsupported, err), true)
	}
	if errors.Is(err, cgroup.ErrRootLeaseUnavailable) {
		return retryableNativeSelfTest(fmt.Errorf("%w: retained cgroup root lease unavailable during self-test: %w", ErrNativeRuntimeSelfTestRetry, err), true)
	}
	created, cleanupVerified, ok := nativePrepareFailureEvidence(err)
	if ok && (!created || cleanupVerified) {
		return retryableNativeSelfTest(fmt.Errorf("%w: prepare: %w", ErrNativeRuntimeSelfTestRetry, err), true)
	}
	return unsafeNativeSelfTest(fmt.Errorf("%w: prepare failed without verified cleanup: %w", ErrNativeRuntimeSelfTestUnsafe, err), false)
}

func (custodian *NativeCustodian) selfTestAttempt(ctx context.Context, verifier AttestationVerifier, attempt int) nativeSelfTestAttemptResult {
	if custodian == nil {
		return unsafeNativeSelfTest(fmt.Errorf("%w: custodian is nil", ErrNativeRuntimeSelfTestUnsafe), false)
	}
	execSpec, markerPath, cleanupTemp, err := nativeSelfTestExecSpec(custodian.options.AgentbusPath)
	if err != nil {
		return retryableNativeSelfTest(fmt.Errorf("%w: build exec spec: %w", ErrNativeRuntimeSelfTestRetry, err), true)
	}
	defer cleanupTemp()

	prepared, err := custodian.Prepare(ctx, execSpec, nativeSelfTestLaunchKey(attempt))
	if err != nil {
		return classifyNativeSelfTestPrepareFailure(err)
	}
	ref := prepared.Ref()
	if err := ref.Validate(); err != nil {
		_, cleanup, abortErr := prepared.AbortAndVerify(context.WithoutCancel(ctx))
		return unsafeNativeSelfTest(errors.Join(
			fmt.Errorf("%w: invalid prepared ref: %v", ErrNativeRuntimeSelfTestUnsafe, err),
			abortErr,
			cleanup.Err,
		), cleanup.Err == nil && abortErr == nil)
	}
	if err := requireSelfTestFixtureAbsent(markerPath); err != nil {
		_, cleanup, abortErr := prepared.AbortAndVerify(context.WithoutCancel(ctx))
		_ = custodian.verifySelfTestClean(context.WithoutCancel(ctx), ref)
		return unsafeNativeSelfTest(errors.Join(err, abortErr, cleanup.Err), false)
	}

	running, releaseOutcome, releaseErr := prepared.Release(ctx)
	if releaseErr != nil || releaseOutcome != ReleaseAccepted || running == nil {
		cleanErr := custodian.recoverSelfTestReleaseFailure(context.WithoutCancel(ctx), verifier, prepared, running, ref)
		return unsafeNativeSelfTest(errors.Join(
			fmt.Errorf("%w: release self-test fixture: outcome=%s err=%v", ErrNativeRuntimeSelfTestUnsafe, releaseOutcome, releaseErr),
			cleanErr,
		), cleanErr == nil)
	}

	if err := waitSelfTestFixtureExecuted(ctx, markerPath); err != nil {
		cleanErr := custodian.recoverSelfTestRunningCleanup(context.WithoutCancel(ctx), verifier, running, ref)
		return unsafeNativeSelfTest(errors.Join(err, cleanErr), cleanErr == nil)
	}

	verified, cleanup, containErr := running.ContainAndVerify(ctx, QuiescenceCauseContain)
	if containErr != nil {
		cleanErr := custodian.recoverSelfTestRunningCleanup(context.WithoutCancel(ctx), verifier, running, ref)
		return unsafeNativeSelfTest(errors.Join(
			fmt.Errorf("%w: contain active self-test fixture: %w", ErrNativeRuntimeSelfTestUnsafe, containErr),
			cleanup.Err,
			cleanErr,
		), cleanup.Err == nil && cleanErr == nil)
	}
	physical, err := verifySelfTestAttestation(verifier, verified, ref)
	if err != nil {
		cleanErr := custodian.verifySelfTestClean(context.WithoutCancel(ctx), ref)
		return unsafeNativeSelfTest(errors.Join(err, cleanup.Err, cleanErr), cleanup.Err == nil && cleanErr == nil)
	}
	if err := requireSelfTestActiveQuiescenceMethod(physical); err != nil {
		cleanErr := custodian.verifySelfTestClean(context.WithoutCancel(ctx), ref)
		if cleanup.Err == nil && cleanErr == nil {
			return unsupportedNativeSelfTest(err, true)
		}
		return unsafeNativeSelfTest(errors.Join(err, cleanup.Err, cleanErr), false)
	}
	if cleanup.Err != nil {
		cleanErr := custodian.recoverSelfTestCleanup(context.WithoutCancel(ctx), verifier, ref)
		if cleanErr != nil {
			return unsafeNativeSelfTest(errors.Join(
				fmt.Errorf("%w: cleanup status: %w", ErrNativeRuntimeSelfTestUnsafe, cleanup.Err),
				cleanErr,
			), false)
		}
		return retryableNativeSelfTest(fmt.Errorf("%w: cleanup status: %w", ErrNativeRuntimeSelfTestRetry, cleanup.Err), true)
	}
	if err := custodian.verifySelfTestClean(ctx, ref); err != nil {
		return unsafeNativeSelfTest(fmt.Errorf("%w: %w", ErrNativeRuntimeSelfTestUnsafe, err), false)
	}
	custodian.selfTest = nativeRuntimeSelfTestRecord{
		Ref:             ref,
		ExecPath:        execSpec.Argv[0],
		Method:          physical.Method,
		Attempts:        attempt,
		CleanupVerified: true,
	}
	return nativeSelfTestAttemptResult{
		Class:       SupportAvailable,
		CleanupSafe: true,
		Ref:         ref,
		ExecPath:    execSpec.Argv[0],
	}
}

func nativeSelfTestExecSpec(agentbusPath string) (command.ExecSpec, string, func(), error) {
	if agentbusPath == "" {
		return command.ExecSpec{}, "", func() {}, fmt.Errorf("agentbus path is required")
	}
	exe, err := filepath.Abs(agentbusPath)
	if err != nil {
		return command.ExecSpec{}, "", func() {}, err
	}
	dir, err := os.MkdirTemp("", "agentbus-native-selftest-")
	if err != nil {
		return command.ExecSpec{}, "", func() {}, err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	markerPath := filepath.Join(dir, "fixture-executed")
	return command.ExecSpec{
		Argv: []string{
			exe,
			nativeSelfTestFixtureCommand,
			"--marker", markerPath,
		},
		Env: os.Environ(),
		Dir: filepath.Dir(exe),
	}, markerPath, cleanup, nil
}

func nativeSelfTestLaunchKey(attempt int) model.LaunchKey {
	if attempt <= 0 {
		attempt = 1
	}
	return model.LaunchKey{
		Attempt: model.AttemptRef{
			JobID:     "job-native-self-test",
			AttemptID: model.AttemptID(fmt.Sprintf("attempt-native-self-test-%d", attempt)),
			Epoch:     1,
		},
		Ordinal: model.LaunchOrdinalOne,
	}
}

func verifySelfTestAttestation(verifier AttestationVerifier, verified VerifiedQuiescence, ref model.GroupRef) (PhysicalQuiescence, error) {
	physical, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		return PhysicalQuiescence{}, fmt.Errorf("%w: attestation mismatch: %w", ErrNativeRuntimeSelfTestUnsafe, err)
	}
	if !physical.Group.Equal(ref) {
		return PhysicalQuiescence{}, fmt.Errorf("%w: attested group does not match probe group", ErrNativeRuntimeSelfTestUnsafe)
	}
	return physical, nil
}

func requireSelfTestActiveQuiescenceMethod(physical PhysicalQuiescence) error {
	required := nativeRuntimePlatformSelfTestQuiescenceMethod()
	if physical.Method == required {
		return nil
	}
	return fmt.Errorf(
		"%w: active self-test containment was not proven: quiescence method=%s want=%s",
		ErrNativeRuntimeUnsupported,
		physical.Method,
		required,
	)
}

func (custodian *NativeCustodian) recoverSelfTestReleaseFailure(ctx context.Context, verifier AttestationVerifier, prepared PreparedProcess, running RunningProcess, ref model.GroupRef) error {
	if running != nil {
		return custodian.recoverSelfTestRunningCleanup(ctx, verifier, running, ref)
	}
	if prepared != nil {
		verified, cleanup, err := prepared.AbortAndVerify(ctx)
		if err == nil {
			if _, verifyErr := verifySelfTestAttestation(verifier, verified, ref); verifyErr != nil {
				err = verifyErr
			}
		}
		return errors.Join(err, cleanup.Err, custodian.verifySelfTestClean(ctx, ref))
	}
	return custodian.recoverSelfTestCleanup(ctx, verifier, ref)
}

func (custodian *NativeCustodian) recoverSelfTestRunningCleanup(ctx context.Context, verifier AttestationVerifier, running RunningProcess, ref model.GroupRef) error {
	if running == nil {
		return custodian.recoverSelfTestCleanup(ctx, verifier, ref)
	}
	verified, cleanup, err := running.ContainAndVerify(ctx, QuiescenceCauseRecovery)
	if err == nil {
		if _, verifyErr := verifySelfTestAttestation(verifier, verified, ref); verifyErr != nil {
			err = verifyErr
		}
	}
	return errors.Join(err, cleanup.Err, custodian.verifySelfTestClean(ctx, ref))
}

func (custodian *NativeCustodian) recoverSelfTestCleanup(ctx context.Context, verifier AttestationVerifier, ref model.GroupRef) error {
	verified, cleanup, err := custodian.ContainAndVerify(ctx, ref, QuiescenceCauseRecovery)
	if err != nil {
		return errors.Join(err, cleanup.Err)
	}
	if _, err := verifySelfTestAttestation(verifier, verified, ref); err != nil {
		return err
	}
	if cleanup.Err != nil {
		return cleanup.Err
	}
	return custodian.verifySelfTestClean(ctx, ref)
}

func (custodian *NativeCustodian) verifySelfTestClean(ctx context.Context, ref model.GroupRef) error {
	absent, err := stableIndependentAbsent(ctx, ref)
	if err != nil {
		return fmt.Errorf("independent probe group absence: %w", err)
	}
	if !absent {
		return fmt.Errorf("probe process group is not stably absent")
	}
	if err := custodian.verifySelfTestRetainedAbsent(ctx, ref); err != nil {
		return err
	}
	return nil
}

func waitSelfTestFixtureExecuted(ctx context.Context, markerPath string) error {
	if markerPath == "" {
		return fmt.Errorf("%w: fixture marker path is empty", ErrNativeRuntimeSelfTestUnsafe)
	}
	for {
		if _, err := os.Stat(markerPath); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: stat fixture marker: %v", ErrNativeRuntimeSelfTestUnsafe, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: fixture did not execute before containment: %w", ErrNativeRuntimeSelfTestUnsafe, ctx.Err())
		case <-time.After(nativeSelfTestFixturePoll):
		}
	}
}

func (custodian *NativeCustodian) verifySelfTestRetainedAbsent(ctx context.Context, ref model.GroupRef) error {
	requiresRetained, err := model.ContainmentRequiresRetainedObject(ref)
	if err != nil {
		return err
	}
	if !requiresRetained {
		return nil
	}
	manager := custodian.retainedGroupSnapshot()
	if manager == nil {
		return fmt.Errorf("probe retained manager is nil")
	}
	if err := proveRetainedGroupAbsent(ctx, manager, ref); err != nil {
		return fmt.Errorf("probe retained cgroup absence: %w", err)
	}
	return nil
}

func requireSelfTestFixtureAbsent(markerPath string) error {
	if markerPath == "" {
		return fmt.Errorf("%w: fixture marker path is empty", ErrNativeRuntimeSelfTestUnsafe)
	}
	if _, err := os.Stat(markerPath); err == nil {
		return fmt.Errorf("%w: fixture exec marker exists before release", ErrNativeRuntimeSelfTestUnsafe)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: stat fixture marker: %v", ErrNativeRuntimeSelfTestUnsafe, err)
	}
	return nil
}

func retryableNativeSelfTest(cause error, cleanupSafe bool) nativeSelfTestAttemptResult {
	return nativeSelfTestAttemptResult{Class: SupportRetryable, Cause: cause, CleanupSafe: cleanupSafe}
}

func unsupportedNativeSelfTest(cause error, cleanupSafe bool) nativeSelfTestAttemptResult {
	return nativeSelfTestAttemptResult{Class: SupportUnsupported, Cause: cause, CleanupSafe: cleanupSafe}
}

func unsafeNativeSelfTest(cause error, cleanupSafe bool) nativeSelfTestAttemptResult {
	return nativeSelfTestAttemptResult{Class: SupportUnsafe, Cause: cause, CleanupSafe: cleanupSafe}
}
