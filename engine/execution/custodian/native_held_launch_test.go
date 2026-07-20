//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

func TestGenerateNativeReleaseSecretRandomAndValid(t *testing.T) {
	seen := map[model.ReleaseSecret]bool{}
	for range 32 {
		secret, err := generateNativeReleaseSecret()
		if err != nil {
			t.Fatalf("generateNativeReleaseSecret() error = %v", err)
		}
		if err := secret.Validate(); err != nil {
			t.Fatalf("generated secret Validate() error = %v", err)
		}
		if !strings.HasPrefix(secret.String(), "native-release-secret-v1-") {
			t.Fatalf("generated secret prefix = %q, want native prefix", secret)
		}
		if seen[secret] {
			t.Fatalf("generated duplicate release secret %q", secret)
		}
		seen[secret] = true
	}
}

func TestNativeHeldLaunchGeneratesInternalSecretAndAbortPrepared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	_, verifier := setNativeHeldIssuerForTest(native)
	spec, resultPath := nativeSimpleLaunchSpec(t)
	externalSecret := model.ReleaseSecret("release-external-native-held")
	spec.ReleaseSecret = externalSecret

	launch, err := prepareNativeHeldLaunch(ctx, native, spec)
	if err != nil {
		t.Fatalf("prepareNativeHeldLaunch() error = %v", err)
	}
	core := requireNativeHeldCore(t, launch)
	secret := core.spec.ReleaseSecret
	if secret == "" || secret == externalSecret {
		t.Fatalf("internal release secret = %q, want non-empty and not caller supplied %q", secret, externalSecret)
	}
	if err := secret.Validate(); err != nil {
		t.Fatalf("internal release secret Validate() error = %v", err)
	}
	for _, fragment := range []string{
		string(spec.CustodyID),
		string(spec.LaunchKey.Attempt.JobID),
		string(spec.LaunchKey.Attempt.AttemptID),
		spec.LogicalGrant.Nonce.String(),
	} {
		if fragment != "" && strings.Contains(secret.String(), fragment) {
			t.Fatalf("internal release secret contains logical identity fragment %q", fragment)
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", launch.Ref()), secret.String()) {
		t.Fatal("group ref exposes internal release secret")
	}
	requireNativeFileAbsent(t, resultPath)
	time.Sleep(100 * time.Millisecond)
	requireNativeFileAbsent(t, resultPath)

	verified, err := launch.AbortAndVerify(ctx)
	if err != nil {
		t.Fatalf("AbortAndVerify() error = %v", err)
	}
	quiescence, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence() error = %v", err)
	}
	if !quiescence.Group.Equal(launch.Ref()) {
		t.Fatalf("quiescence group = %+v, want %+v", quiescence.Group, launch.Ref())
	}
	waitGroupAbsent(t, launch.Ref(), 5*time.Second)
	requireNativeFileAbsent(t, resultPath)
}

func TestNativeHeldLaunchPrivateReleaseExecsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	manager := newFakeNativeRetainedManager()
	native := newNativeCustodianWithRetainedManagerForTest(t, defaultNativeTestParams(), manager)
	_, verifier := setNativeHeldIssuerForTest(native)
	spec, resultPath := nativeSimpleLaunchSpec(t)
	spec.ReleaseSecret = model.ReleaseSecret("release-external-native-held-ignored")

	launch, err := prepareNativeHeldLaunch(ctx, native, spec)
	if err != nil {
		t.Fatalf("prepareNativeHeldLaunch() error = %v", err)
	}
	secret := requireNativeHeldCore(t, launch).spec.ReleaseSecret
	requireNativeFileAbsent(t, resultPath)
	time.Sleep(100 * time.Millisecond)
	requireNativeFileAbsent(t, resultPath)

	running, outcome, err := launch.Release(ctx)
	if err != nil || outcome != ReleaseAccepted || running == nil {
		t.Fatalf("Release() = (%v, %s, %v), want running accepted nil", running, outcome, err)
	}
	nativeRunning, ok := running.(*NativeRunningProcess)
	if !ok {
		t.Fatalf("Release() running type = %T, want *NativeRunningProcess", running)
	}
	defer cleanupNativeRunning(t, nativeRunning)
	if !nativeRunning.Ref().Equal(launch.Ref()) {
		t.Fatalf("running ref = %+v, want launch ref %+v", nativeRunning.Ref(), launch.Ref())
	}
	if got := native.ActiveCustodyCount(); got != 1 {
		t.Fatalf("ActiveCustodyCount() after release = %d, want 1", got)
	}
	stdout, err := io.ReadAll(nativeRunning.Stdout())
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if !strings.Contains(string(stdout), "native-simple") {
		t.Fatalf("stdout = %q, want native-simple marker", stdout)
	}
	rawResult, err := os.ReadFile(resultPath)
	if err != nil {
		t.Fatalf("read backend result: %v", err)
	}
	if strings.Contains(string(rawResult), secret.String()) {
		t.Fatal("backend result exposes internal release secret")
	}
	result := readNativeBackendResult(t, resultPath)
	if result.PID != nativeRunning.Ref().Leader.PID || result.PGID != nativeRunning.Ref().PGID {
		t.Fatalf("backend result = %+v, want pid=%d pgid=%d", result, nativeRunning.Ref().Leader.PID, nativeRunning.Ref().PGID)
	}

	again, againOutcome, againErr := launch.Release(ctx)
	if again != nil || againOutcome != ReleaseDefinitelyNotSent || !errors.Is(againErr, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("second Release() = (%v, %s, %v), want nil definitely_not_sent already-consumed", again, againOutcome, againErr)
	}
	exit, verified, err := nativeRunning.WaitAndVerify(ctx)
	if err != nil {
		t.Fatalf("WaitAndVerify() error = %v", err)
	}
	if !exit.Exited || exit.Code != 0 || exit.Signal != "" {
		t.Fatalf("exit observation = %+v, want clean exit", exit)
	}
	quiescence, err := verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence() error = %v", err)
	}
	if !quiescence.Group.Equal(nativeRunning.Ref()) {
		t.Fatalf("quiescence group = %+v, want %+v", quiescence.Group, nativeRunning.Ref())
	}
	waitGroupAbsent(t, nativeRunning.Ref(), 5*time.Second)
}

func TestNativeHeldLaunchCanceledReleaseMapsUnknownContainsAndDoesNotResend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	native, issuer := newNativeHeldAbsentCustodianForTest()
	spec := heldLaunchTestSpec(t)
	ref := nativeHeldAbsentGroupForTest(t, spec.LaunchKey)
	releaseStarted := make(chan struct{})
	prepared := &fakeNativeParkPrepared{
		ref:            ref,
		releaseStarted: releaseStarted,
		release: func(ctx context.Context) (*parklaunch.ParkedHandle, error) {
			<-ctx.Done()
			return nil, errors.Join(parklaunch.ErrReleaseOutcomeUnknown, ctx.Err())
		},
	}
	backend := &fakeNativeHeldBackend{witnessAcquiredValue: true}
	effects := &nativeHeldLaunchEffects{
		custodian: native,
		prepared: map[string]*nativeHeldPreparedLaunch{
			groupKey(ref): {prepared: prepared, backend: backend},
		},
	}
	launch := &HeldLaunchCore{
		spec:    spec,
		effects: effects,
		ref:     ref,
		state:   HeldLaunchStatePrepared,
	}

	releaseDone := make(chan releaseResult, 1)
	go func() {
		running, outcome, err := launch.Release(ctx)
		releaseDone <- releaseResult{running: running, outcome: outcome, err: err}
	}()
	waitClosed(t, releaseStarted, "native fake release start")

	controlDone := make(chan error, 1)
	go func() {
		_, err := launch.HandleControlLoss(context.Background(), true)
		controlDone <- err
	}()

	release := <-releaseDone
	controlErr := <-controlDone
	if release.running != nil || release.outcome != ReleaseOutcomeUnknown ||
		!errors.Is(release.err, parklaunch.ErrReleaseOutcomeUnknown) ||
		!errors.Is(release.err, context.Canceled) {
		t.Fatalf("Release() = (%v, %s, %v), want nil unknown release-outcome-unknown+canceled", release.running, release.outcome, release.err)
	}
	if controlErr != nil && !errors.Is(controlErr, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("HandleControlLoss() error = %v, want nil or already-consumed race", controlErr)
	}
	preparedSnapshot := prepared.snapshot()
	if preparedSnapshot.releaseCalls != 1 || preparedSnapshot.containCalls != 1 || preparedSnapshot.abortCalls != 0 {
		t.Fatalf("prepared calls = release:%d contain:%d abort:%d, want 1/1/0", preparedSnapshot.releaseCalls, preparedSnapshot.containCalls, preparedSnapshot.abortCalls)
	}
	if backend.snapshotCloseCalls() != 1 {
		t.Fatalf("backend close calls = %d, want 1", backend.snapshotCloseCalls())
	}
	if issuer.count.Load() == 0 {
		t.Fatal("containment did not produce a quiescence attestation")
	}
	if _, ok := effects.lookupPrepared(ref); ok {
		t.Fatal("prepared entry retained after contain-and-verify")
	}
	again, againOutcome, againErr := launch.Release(ctx)
	if again != nil || againOutcome != ReleaseDefinitelyNotSent || !errors.Is(againErr, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("second Release() = (%v, %s, %v), want nil definitely_not_sent already-consumed", again, againOutcome, againErr)
	}
	if _, abortErr := launch.AbortAndVerify(ctx); !errors.Is(abortErr, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("AbortAndVerify() after canceled release error = %v, want already-consumed", abortErr)
	}
	waitGroupAbsent(t, ref, time.Second)
}

func TestNativeReleaseOutcomeFromParklaunchMapping(t *testing.T) {
	handle := &parklaunch.ParkedHandle{}
	tests := []struct {
		name    string
		handle  *parklaunch.ParkedHandle
		err     error
		outcome ReleaseOutcome
	}{
		{name: "accepted handle", handle: handle, outcome: ReleaseAccepted},
		{name: "nil handle success fails closed", outcome: ReleaseOutcomeUnknown},
		{name: "ambiguous ack", err: errors.Join(parklaunch.ErrReleaseOutcomeUnknown, context.Canceled), outcome: ReleaseOutcomeUnknown},
		{name: "channel lost before release", err: fmt.Errorf("%w: pipe closed", parklaunch.ErrChannelLostBeforeRelease), outcome: ReleaseDefinitelyNotSent},
		{name: "other error unsure", err: errors.New("unexpected release failure"), outcome: ReleaseOutcomeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nativeReleaseOutcomeFromParklaunch(tt.handle, tt.err); got != tt.outcome {
				t.Fatalf("nativeReleaseOutcomeFromParklaunch() = %s, want %s", got, tt.outcome)
			}
		})
	}
}

func TestNativeCustodianPublicPrepareRemainsBoundaryStub(t *testing.T) {
	var native NativeCustodian
	spec := heldLaunchTestSpec(t)
	prepared, err := native.Prepare(context.Background(), command.ExecSpec{Argv: []string{"/bin/echo"}}, spec.LaunchKey)
	if prepared != nil || !errors.Is(err, ErrNativePreparedBoundary) {
		t.Fatalf("NativeCustodian.Prepare() = (%v, %v), want nil ErrNativePreparedBoundary", prepared, err)
	}
}

func setNativeHeldIssuerForTest(native *NativeCustodian) (AttestationIssuer, AttestationVerifier) {
	issuer, verifier := NewAttestationChannel()
	native.issuer = issuer
	return issuer, verifier
}

func requireNativeHeldCore(t *testing.T, launch HeldLaunch) *HeldLaunchCore {
	t.Helper()
	core, ok := launch.(*HeldLaunchCore)
	if !ok {
		t.Fatalf("native held launch type = %T, want *HeldLaunchCore", launch)
	}
	return core
}

func requireNativeFileAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("path %s exists, want absent", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", path, err)
	}
}

func newNativeHeldAbsentCustodianForTest() (*NativeCustodian, *countingQuiescenceIssuer) {
	issuer, _ := NewAttestationChannel()
	counting := &countingQuiescenceIssuer{inner: issuer}
	return &NativeCustodian{
		options: NativeOptions{
			ContainmentParams: defaultNativeTestParams(),
		},
		issuer:    counting,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}, counting
}

func nativeHeldAbsentGroupForTest(t *testing.T, key model.LaunchKey) model.GroupRef {
	t.Helper()
	domain, err := procgroup.CurrentKernelDomain()
	if err != nil {
		t.Fatal(err)
	}
	domain.RetainedDomainID = ""
	domain.RetainedDomainState = model.RetainedDomainNotApplicable
	if err := domain.Validate(); err != nil {
		t.Fatalf("test domain Validate() error = %v", err)
	}
	pgid := absentProcessGroupForDomain(t, domain)
	group := model.GroupRef{
		Version:             1,
		CustodyID:           model.CustodyID("custody-native-held-absent"),
		Launch:              key,
		HostBootID:          domain.HostBootID,
		PIDNamespaceID:      domain.PIDNamespaceID,
		PIDNamespaceState:   domain.PIDNamespaceState,
		RetainedDomainState: model.RetainedDomainNotApplicable,
		PGID:                pgid,
		Leader:              model.ProcessIdentity{PID: pgid, HighResStartToken: "leader-start-native-held-absent"},
		Monitor:             model.ProcessIdentity{PID: os.Getpid(), HighResStartToken: "monitor-start-native-held-absent"},
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("test group Validate() error = %v", err)
	}
	return group
}

type fakeNativeParkPrepared struct {
	mu sync.Mutex

	ref model.GroupRef

	releaseStarted     chan struct{}
	releaseStartedOnce sync.Once
	release            func(context.Context) (*parklaunch.ParkedHandle, error)
	abort              func(context.Context) error
	contain            func(context.Context) error

	releaseCalls int
	abortCalls   int
	containCalls int
}

type fakeNativeParkPreparedSnapshot struct {
	releaseCalls int
	abortCalls   int
	containCalls int
}

func (prepared *fakeNativeParkPrepared) Ref() model.GroupRef {
	return prepared.ref
}

func (prepared *fakeNativeParkPrepared) Release(ctx context.Context) (*parklaunch.ParkedHandle, error) {
	prepared.mu.Lock()
	prepared.releaseCalls++
	release := prepared.release
	prepared.mu.Unlock()
	signalOnce(prepared.releaseStarted, &prepared.releaseStartedOnce)
	if release != nil {
		return release(ctx)
	}
	return nil, nil
}

func (prepared *fakeNativeParkPrepared) AbortAndVerify(ctx context.Context) error {
	prepared.mu.Lock()
	prepared.abortCalls++
	abort := prepared.abort
	prepared.mu.Unlock()
	if abort != nil {
		return abort(ctx)
	}
	return nil
}

func (prepared *fakeNativeParkPrepared) ContainAndVerify(ctx context.Context) error {
	prepared.mu.Lock()
	prepared.containCalls++
	contain := prepared.contain
	prepared.mu.Unlock()
	if contain != nil {
		return contain(ctx)
	}
	return nil
}

func (prepared *fakeNativeParkPrepared) snapshot() fakeNativeParkPreparedSnapshot {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	return fakeNativeParkPreparedSnapshot{
		releaseCalls: prepared.releaseCalls,
		abortCalls:   prepared.abortCalls,
		containCalls: prepared.containCalls,
	}
}

type fakeNativeHeldBackend struct {
	mu sync.Mutex

	witnessAcquiredValue bool
	closeCalls           int
	attachCalls          int
}

func (backend *fakeNativeHeldBackend) retainedID() string {
	return ""
}

func (backend *fakeNativeHeldBackend) retainLeaderUnreaped() bool {
	return false
}

func (backend *fakeNativeHeldBackend) beforeMonitorBind(_ context.Context, group model.GroupRef) (model.GroupRef, error) {
	return group, nil
}

func (backend *fakeNativeHeldBackend) beforeRelease(context.Context, model.GroupRef) error {
	return nil
}

func (backend *fakeNativeHeldBackend) witness() containment.ContinuityWitness {
	return nil
}

func (backend *fakeNativeHeldBackend) witnessAcquired() bool {
	return backend != nil && backend.witnessAcquiredValue
}

func (backend *fakeNativeHeldBackend) retainedObject() containment.RetainedGroupObject {
	return nil
}

func (backend *fakeNativeHeldBackend) attachHandle(*parklaunch.ParkedHandle) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.attachCalls++
}

func (backend *fakeNativeHeldBackend) leaderRetention() *leaderRetention {
	return nil
}

func (backend *fakeNativeHeldBackend) close(context.Context) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.closeCalls++
	return nil
}

func (backend *fakeNativeHeldBackend) snapshotCloseCalls() int {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.closeCalls
}

var _ nativeParkPrepared = (*fakeNativeParkPrepared)(nil)
var _ nativeContainmentBackend = (*fakeNativeHeldBackend)(nil)
