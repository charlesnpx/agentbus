package custodian

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	errHeldLaunchAckLost    = errors.New("release ack lost")
	errHeldLaunchDaemonDied = errors.New("daemon died before release ack")
)

func TestReleaseOutcomeStringAndValidity(t *testing.T) {
	tests := []struct {
		outcome ReleaseOutcome
		want    string
		valid   bool
	}{
		{outcome: ReleaseDefinitelyNotSent, want: "definitely_not_sent", valid: true},
		{outcome: ReleaseAccepted, want: "accepted", valid: true},
		{outcome: ReleaseOutcomeUnknown, want: "unknown", valid: true},
		{outcome: ReleaseOutcome(99), want: "", valid: false},
	}
	for _, tt := range tests {
		if got := tt.outcome.String(); got != tt.want {
			t.Fatalf("ReleaseOutcome(%d).String() = %q, want %q", tt.outcome, got, tt.want)
		}
		if got := tt.outcome.Valid(); got != tt.valid {
			t.Fatalf("ReleaseOutcome(%d).Valid() = %v, want %v", tt.outcome, got, tt.valid)
		}
	}
}

func TestHeldLaunchActiveCustodyCountsPreparedAndReleasing(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	allowSend := make(chan struct{})
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseAccepted
	effects.releaseRunning = newHeldFakeRunning(t, group, command.ExitObservation{Exited: true, Code: 0}, model.QuiescenceNaturalExit)
	effects.sendStarted = make(chan struct{})
	effects.allowSend = allowSend

	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)
	if got := launch.State(); got != HeldLaunchStatePrepared {
		t.Fatalf("initial state = %s, want %s", got, HeldLaunchStatePrepared)
	}
	if got := launch.ActiveCustodyCount(); got != 1 {
		t.Fatalf("ActiveCustodyCount() while prepared = %d, want 1", got)
	}

	done := make(chan releaseResult, 1)
	go func() {
		running, outcome, err := launch.Release(ctx)
		done <- releaseResult{running: running, outcome: outcome, err: err}
	}()
	waitClosed(t, effects.sendStarted, "release send start")
	if got := launch.State(); got != HeldLaunchStateReleasing {
		t.Fatalf("state while release effect is blocked = %s, want %s", got, HeldLaunchStateReleasing)
	}
	if got := launch.ActiveCustodyCount(); got != 1 {
		t.Fatalf("ActiveCustodyCount() while releasing = %d, want 1", got)
	}

	close(allowSend)
	result := <-done
	if result.err != nil || result.outcome != ReleaseAccepted || result.running == nil {
		t.Fatalf("Release() = (%v, %s, %v), want running accepted nil", result.running, result.outcome, result.err)
	}
	if got := launch.State(); got != HeldLaunchStateRunning {
		t.Fatalf("state after accepted release = %s, want %s", got, HeldLaunchStateRunning)
	}
}

func TestHeldLaunchRaceReleaseVsReleaseExactlyOneSends(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	allowSend := make(chan struct{})
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseAccepted
	effects.releaseRunning = newHeldFakeRunning(t, group, command.ExitObservation{Exited: true, Code: 0}, model.QuiescenceNaturalExit)
	effects.sendStarted = make(chan struct{})
	effects.allowSend = allowSend
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	results := make(chan releaseResult, 2)
	go func() {
		running, outcome, err := launch.Release(ctx)
		results <- releaseResult{running: running, outcome: outcome, err: err}
	}()
	waitClosed(t, effects.sendStarted, "first release send start")
	go func() {
		running, outcome, err := launch.Release(ctx)
		results <- releaseResult{running: running, outcome: outcome, err: err}
	}()

	close(allowSend)
	first := <-results
	second := <-results
	assertReleaseRaceResult(t, []releaseResult{first, second}, 1, 1)
	if got := effects.snapshot().frameWrites; got != 1 {
		t.Fatalf("frame writes = %d, want 1", got)
	}
}

func TestHeldLaunchRaceReleaseVsAbortBeforeSendAbortWins(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	allowAbort := make(chan struct{})
	effects := newHeldLaunchFakeEffects(t, group)
	effects.abortStarted = make(chan struct{})
	effects.allowAbort = allowAbort
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	abortDone := make(chan abortResult, 1)
	go func() {
		verified, err := launch.AbortAndVerify(ctx)
		abortDone <- abortResult{verified: verified, err: err}
	}()
	waitClosed(t, effects.abortStarted, "abort start")

	releaseDone := make(chan releaseResult, 1)
	go func() {
		running, outcome, err := launch.Release(ctx)
		releaseDone <- releaseResult{running: running, outcome: outcome, err: err}
	}()

	if got := effects.snapshot().frameWrites; got != 0 {
		t.Fatalf("frame writes before abort finishes = %d, want 0", got)
	}
	close(allowAbort)
	abort := <-abortDone
	release := <-releaseDone
	if abort.err != nil {
		t.Fatalf("AbortAndVerify() error = %v, want nil", abort.err)
	}
	if release.outcome != ReleaseDefinitelyNotSent || !errors.Is(release.err, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("Release() after abort = (%s, %v), want definitely_not_sent already-consumed", release.outcome, release.err)
	}
	snapshot := effects.snapshot()
	if snapshot.abortCalls != 1 || snapshot.sendCalls != 0 || snapshot.frameWrites != 0 {
		t.Fatalf("calls = abort:%d send:%d writes:%d, want abort:1 send:0 writes:0", snapshot.abortCalls, snapshot.sendCalls, snapshot.frameWrites)
	}
}

func TestHeldLaunchRaceReleaseVsAbortAfterSendContains(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	allowSend := make(chan struct{})
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseOutcomeUnknown
	effects.releaseErr = errHeldLaunchAckLost
	effects.sendStarted = make(chan struct{})
	effects.allowSend = allowSend
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	releaseDone := make(chan releaseResult, 1)
	go func() {
		running, outcome, err := launch.Release(ctx)
		releaseDone <- releaseResult{running: running, outcome: outcome, err: err}
	}()
	waitClosed(t, effects.sendStarted, "release frame write")
	if got := launch.State(); got != HeldLaunchStateReleasing {
		t.Fatalf("state after frame write = %s, want %s", got, HeldLaunchStateReleasing)
	}

	abortDone := make(chan abortResult, 1)
	go func() {
		verified, err := launch.AbortAndVerify(ctx)
		abortDone <- abortResult{verified: verified, err: err}
	}()
	close(allowSend)

	release := <-releaseDone
	abort := <-abortDone
	if release.outcome != ReleaseOutcomeUnknown || !errors.Is(release.err, errHeldLaunchAckLost) {
		t.Fatalf("Release() = (%s, %v), want unknown ack-lost", release.outcome, release.err)
	}
	if !errors.Is(abort.err, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("AbortAndVerify() after unknown release error = %v, want already-consumed", abort.err)
	}
	snapshot := effects.snapshot()
	if snapshot.frameWrites != 1 || snapshot.containCalls != 1 || snapshot.containCauses[0] != QuiescenceCauseContain {
		t.Fatalf("writes/contains = %d/%d %#v, want one write and contain cause", snapshot.frameWrites, snapshot.containCalls, snapshot.containCauses)
	}
}

func TestHeldLaunchRaceReleaseVsCloseAfterSendContains(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	allowSend := make(chan struct{})
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseOutcomeUnknown
	effects.releaseErr = errHeldLaunchAckLost
	effects.sendStarted = make(chan struct{})
	effects.allowSend = allowSend
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	releaseDone := make(chan releaseResult, 1)
	go func() {
		running, outcome, err := launch.Release(ctx)
		releaseDone <- releaseResult{running: running, outcome: outcome, err: err}
	}()
	waitClosed(t, effects.sendStarted, "release frame write")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- launch.Close(ctx)
	}()
	close(allowSend)

	release := <-releaseDone
	closeErr := <-closeDone
	if release.outcome != ReleaseOutcomeUnknown || !errors.Is(release.err, errHeldLaunchAckLost) {
		t.Fatalf("Release() = (%s, %v), want unknown ack-lost", release.outcome, release.err)
	}
	if !errors.Is(closeErr, ErrHeldLaunchCloseRefused) || !errors.Is(closeErr, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("Close() after unknown release error = %v, want close-refused already-consumed", closeErr)
	}
	snapshot := effects.snapshot()
	if snapshot.frameWrites != 1 || snapshot.containCalls != 1 || snapshot.abortCalls != 0 {
		t.Fatalf("calls = writes:%d contains:%d aborts:%d, want writes:1 contains:1 aborts:0", snapshot.frameWrites, snapshot.containCalls, snapshot.abortCalls)
	}
}

func TestHeldLaunchReleaseContextCanceledBeforeFrameWritePermitsAbort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseAccepted
	effects.releaseRunning = newHeldFakeRunning(t, group, command.ExitObservation{Exited: true, Code: 0}, model.QuiescenceNaturalExit)
	launch := prepareHeldLaunchForTest(t, context.Background(), spec, effects)

	running, outcome, err := launch.Release(ctx)
	if running != nil || outcome != ReleaseDefinitelyNotSent || !errors.Is(err, context.Canceled) {
		t.Fatalf("Release(canceled) = (%v, %s, %v), want nil definitely_not_sent context.Canceled", running, outcome, err)
	}
	if got := effects.snapshot().frameWrites; got != 0 {
		t.Fatalf("frame writes after canceled release = %d, want 0", got)
	}
	if _, err := launch.AbortAndVerify(context.Background()); err != nil {
		t.Fatalf("AbortAndVerify() after definitely_not_sent = %v, want nil", err)
	}
	if got := launch.State(); got != HeldLaunchStateFinalized {
		t.Fatalf("state after abort = %s, want %s", got, HeldLaunchStateFinalized)
	}
}

func TestHeldLaunchFrameWriteBeganAckLostUnknownContainsAndNeverResends(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseOutcomeUnknown
	effects.releaseErr = errHeldLaunchAckLost
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	running, outcome, err := launch.Release(ctx)
	if running != nil || outcome != ReleaseOutcomeUnknown || !errors.Is(err, errHeldLaunchAckLost) {
		t.Fatalf("Release() = (%v, %s, %v), want nil unknown ack-lost", running, outcome, err)
	}
	running, outcome, err = launch.Release(ctx)
	if running != nil || outcome != ReleaseDefinitelyNotSent || !errors.Is(err, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("second Release() = (%v, %s, %v), want nil definitely_not_sent already-consumed", running, outcome, err)
	}
	snapshot := effects.snapshot()
	if snapshot.frameWrites != 1 || snapshot.containCalls != 1 || snapshot.sendCalls != 1 {
		t.Fatalf("calls = writes:%d contains:%d sends:%d, want 1/1/1", snapshot.frameWrites, snapshot.containCalls, snapshot.sendCalls)
	}
}

func TestHeldLaunchWorkerAckedThenExecFailedAcceptedWaitsAndVerifies(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	effects := newHeldLaunchFakeEffects(t, group)
	exit := command.ExitObservation{Exited: true, Code: 127}
	running := newHeldFakeRunning(t, group, exit, model.QuiescenceNaturalExit)
	effects.releaseOutcome = ReleaseAccepted
	effects.releaseRunning = running
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	gotRunning, outcome, err := launch.Release(ctx)
	if err != nil || outcome != ReleaseAccepted || gotRunning == nil {
		t.Fatalf("Release() = (%v, %s, %v), want running accepted nil", gotRunning, outcome, err)
	}
	gotExit, verified, err := gotRunning.WaitAndVerify(ctx)
	if err != nil {
		t.Fatalf("WaitAndVerify() error = %v, want nil", err)
	}
	if gotExit != exit {
		t.Fatalf("exit = %#v, want %#v", gotExit, exit)
	}
	physical, err := running.verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatalf("VerifyQuiescence() error = %v, want nil", err)
	}
	if !physical.Group.Equal(group) || physical.Method != model.QuiescenceNaturalExit {
		t.Fatalf("quiescence = (%v, %s), want group natural_exit", physical.Group, physical.Method)
	}
}

func TestHeldLaunchDaemonDiesWhilePreparedAbortsAndRecoversByGroupRef(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)

	unboundEffects := newHeldLaunchFakeEffects(t, group)
	unbound := prepareHeldLaunchForTest(t, ctx, spec, unboundEffects)
	if _, err := unbound.HandleControlLoss(ctx, false); err != nil {
		t.Fatalf("unbound HandleControlLoss() error = %v, want nil", err)
	}
	if snapshot := unboundEffects.snapshot(); snapshot.abortCalls != 1 || snapshot.containCalls != 0 {
		t.Fatalf("unbound calls = abort:%d contain:%d, want abort:1 contain:0", snapshot.abortCalls, snapshot.containCalls)
	}

	boundEffects := newHeldLaunchFakeEffects(t, group)
	bound := prepareHeldLaunchForTest(t, ctx, spec, boundEffects)
	if _, err := bound.HandleControlLoss(ctx, true); err != nil {
		t.Fatalf("bound HandleControlLoss() error = %v, want nil", err)
	}
	snapshot := boundEffects.snapshot()
	if snapshot.containCalls != 1 || snapshot.containCauses[0] != QuiescenceCauseRecovery || !snapshot.containRefs[0].Equal(group) {
		t.Fatalf("bound recovery = calls:%d causes:%#v refs:%#v, want recovery by durable group", snapshot.containCalls, snapshot.containCauses, snapshot.containRefs)
	}
}

func TestHeldLaunchDaemonDiesAfterGrantBeforeAckContainsAndNeverResends(t *testing.T) {
	ctx := context.Background()
	spec := heldLaunchTestSpec(t)
	group := heldLaunchTestGroup(t, spec.LaunchKey)
	effects := newHeldLaunchFakeEffects(t, group)
	effects.releaseOutcome = ReleaseOutcomeUnknown
	effects.releaseErr = errHeldLaunchDaemonDied
	launch := prepareHeldLaunchForTest(t, ctx, spec, effects)

	running, outcome, err := launch.Release(ctx)
	if running != nil || outcome != ReleaseOutcomeUnknown || !errors.Is(err, errHeldLaunchDaemonDied) {
		t.Fatalf("Release() with daemon death = (%v, %s, %v), want nil unknown daemon-died", running, outcome, err)
	}
	running, outcome, err = launch.Release(ctx)
	if running != nil || outcome != ReleaseDefinitelyNotSent || !errors.Is(err, ErrHeldLaunchAlreadyConsumed) {
		t.Fatalf("second Release() = (%v, %s, %v), want nil definitely_not_sent already-consumed", running, outcome, err)
	}
	snapshot := effects.snapshot()
	if snapshot.frameWrites != 1 || snapshot.containCalls != 1 || snapshot.sendCalls != 1 {
		t.Fatalf("calls = writes:%d contains:%d sends:%d, want 1/1/1", snapshot.frameWrites, snapshot.containCalls, snapshot.sendCalls)
	}
}

type releaseResult struct {
	running RunningProcess
	outcome ReleaseOutcome
	err     error
}

type abortResult struct {
	verified VerifiedQuiescence
	err      error
}

func assertReleaseRaceResult(t *testing.T, results []releaseResult, wantAccepted, wantConsumed int) {
	t.Helper()
	var accepted int
	var consumed int
	for _, result := range results {
		if result.outcome == ReleaseAccepted && result.err == nil && result.running != nil {
			accepted++
		}
		if result.outcome == ReleaseDefinitelyNotSent && errors.Is(result.err, ErrHeldLaunchAlreadyConsumed) {
			consumed++
		}
	}
	if accepted != wantAccepted || consumed != wantConsumed {
		t.Fatalf("release results = %#v, want accepted:%d consumed:%d", results, wantAccepted, wantConsumed)
	}
}

func prepareHeldLaunchForTest(t *testing.T, ctx context.Context, spec PrepareSpec, effects *heldLaunchFakeEffects) *HeldLaunchCore {
	t.Helper()
	launch, err := PrepareHeldLaunch(ctx, spec, effects)
	if err != nil {
		t.Fatalf("PrepareHeldLaunch() error = %v", err)
	}
	return launch
}

type heldLaunchFakeEffects struct {
	mu     sync.Mutex
	ref    model.GroupRef
	issuer AttestationIssuer

	releaseOutcome ReleaseOutcome
	releaseErr     error
	releaseRunning RunningProcess

	sendStarted     chan struct{}
	sendStartedOnce sync.Once
	allowSend       <-chan struct{}

	abortStarted     chan struct{}
	abortStartedOnce sync.Once
	allowAbort       <-chan struct{}

	prepareCalls   int
	sendCalls      int
	frameWrites    int
	abortCalls     int
	containCalls   int
	releaseSecrets []model.ReleaseSecret
	releaseRefs    []model.GroupRef
	abortRefs      []model.GroupRef
	containRefs    []model.GroupRef
	containCauses  []QuiescenceCause
}

type heldLaunchFakeSnapshot struct {
	prepareCalls   int
	sendCalls      int
	frameWrites    int
	abortCalls     int
	containCalls   int
	releaseSecrets []model.ReleaseSecret
	releaseRefs    []model.GroupRef
	abortRefs      []model.GroupRef
	containRefs    []model.GroupRef
	containCauses  []QuiescenceCause
}

func newHeldLaunchFakeEffects(t *testing.T, ref model.GroupRef) *heldLaunchFakeEffects {
	t.Helper()
	issuer, _ := NewAttestationChannel()
	if err := ref.Validate(); err != nil {
		t.Fatalf("test group invalid: %v", err)
	}
	return &heldLaunchFakeEffects{
		ref:            ref,
		issuer:         issuer,
		releaseOutcome: ReleaseDefinitelyNotSent,
	}
}

func (effects *heldLaunchFakeEffects) Prepare(_ context.Context, spec PrepareSpec) (model.GroupRef, error) {
	effects.mu.Lock()
	defer effects.mu.Unlock()
	effects.prepareCalls++
	if spec.ReleaseSecret == "" {
		return model.GroupRef{}, ErrInvalidHeldLaunch
	}
	return effects.ref, nil
}

func (effects *heldLaunchFakeEffects) SendRelease(ctx context.Context, spec PrepareSpec, ref model.GroupRef) (RunningProcess, ReleaseOutcome, error) {
	effects.mu.Lock()
	effects.sendCalls++
	effects.releaseSecrets = append(effects.releaseSecrets, spec.ReleaseSecret)
	effects.releaseRefs = append(effects.releaseRefs, ref)
	effects.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, ReleaseDefinitelyNotSent, err
	}

	effects.mu.Lock()
	effects.frameWrites++
	effects.mu.Unlock()
	signalOnce(effects.sendStarted, &effects.sendStartedOnce)
	waitIfSet(ctx, effects.allowSend)
	return effects.releaseRunning, effects.releaseOutcome, effects.releaseErr
}

func (effects *heldLaunchFakeEffects) AbortAndVerify(ctx context.Context, ref model.GroupRef) (VerifiedQuiescence, error) {
	effects.mu.Lock()
	effects.abortCalls++
	effects.abortRefs = append(effects.abortRefs, ref)
	effects.mu.Unlock()
	signalOnce(effects.abortStarted, &effects.abortStartedOnce)
	waitIfSet(ctx, effects.allowAbort)
	return effects.issuer.AttestQuiescence(PhysicalQuiescence{Group: ref, Method: model.QuiescenceAlreadyAbsent})
}

func (effects *heldLaunchFakeEffects) ContainAndVerify(_ context.Context, ref model.GroupRef, cause QuiescenceCause) (VerifiedQuiescence, error) {
	effects.mu.Lock()
	effects.containCalls++
	effects.containRefs = append(effects.containRefs, ref)
	effects.containCauses = append(effects.containCauses, cause)
	effects.mu.Unlock()
	return effects.issuer.AttestQuiescence(PhysicalQuiescence{Group: ref, Method: model.QuiescenceTermKill})
}

func (effects *heldLaunchFakeEffects) snapshot() heldLaunchFakeSnapshot {
	effects.mu.Lock()
	defer effects.mu.Unlock()
	return heldLaunchFakeSnapshot{
		prepareCalls:   effects.prepareCalls,
		sendCalls:      effects.sendCalls,
		frameWrites:    effects.frameWrites,
		abortCalls:     effects.abortCalls,
		containCalls:   effects.containCalls,
		releaseSecrets: append([]model.ReleaseSecret(nil), effects.releaseSecrets...),
		releaseRefs:    append([]model.GroupRef(nil), effects.releaseRefs...),
		abortRefs:      append([]model.GroupRef(nil), effects.abortRefs...),
		containRefs:    append([]model.GroupRef(nil), effects.containRefs...),
		containCauses:  append([]QuiescenceCause(nil), effects.containCauses...),
	}
}

type heldFakeRunning struct {
	ref      model.GroupRef
	exit     command.ExitObservation
	verified VerifiedQuiescence
	verifier AttestationVerifier
}

func newHeldFakeRunning(t *testing.T, ref model.GroupRef, exit command.ExitObservation, method model.QuiescenceMethod) *heldFakeRunning {
	t.Helper()
	issuer, verifier := NewAttestationChannel()
	verified, err := issuer.AttestQuiescence(PhysicalQuiescence{Group: ref, Method: method})
	if err != nil {
		t.Fatalf("attest running quiescence: %v", err)
	}
	return &heldFakeRunning{ref: ref, exit: exit, verified: verified, verifier: verifier}
}

func (running *heldFakeRunning) runningProcess() {}

func (running *heldFakeRunning) Ref() model.GroupRef {
	return running.ref
}

func (running *heldFakeRunning) Stdin() io.WriteCloser {
	return nopWriteCloser{}
}

func (running *heldFakeRunning) Stdout() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}

func (running *heldFakeRunning) Stderr() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}

func (running *heldFakeRunning) WaitAndVerify(context.Context) (command.ExitObservation, VerifiedQuiescence, error) {
	return running.exit, running.verified, nil
}

func (running *heldFakeRunning) ContainAndVerify(context.Context, QuiescenceCause) (VerifiedQuiescence, error) {
	return running.verified, nil
}

type nopWriteCloser struct{}

func (nopWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (nopWriteCloser) Close() error {
	return nil
}

func heldLaunchTestSpec(t *testing.T) PrepareSpec {
	t.Helper()
	attempt, err := model.NewAttemptRef("job-held-launch", "attempt-held-launch", 1)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := model.NewReleaseSecret("release-secret-held-launch")
	if err != nil {
		t.Fatal(err)
	}
	return PrepareSpec{
		Exec:          command.ExecSpec{Argv: []string{"/bin/fake-agent"}},
		LaunchKey:     model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		ReleaseSecret: secret,
	}
}

func heldLaunchTestGroup(t *testing.T, key model.LaunchKey) model.GroupRef {
	t.Helper()
	group := model.GroupRef{
		Version:             1,
		CustodyID:           model.CustodyID("custody-held-launch"),
		Launch:              key,
		HostBootID:          "boot-held-launch",
		PIDNamespaceState:   model.PIDNamespaceNotApplicable,
		RetainedDomainState: model.RetainedDomainNotApplicable,
		PGID:                4242,
		Leader:              model.ProcessIdentity{PID: 4242, HighResStartToken: "leader-start-held-launch"},
		Monitor:             model.ProcessIdentity{PID: 4243, HighResStartToken: "monitor-start-held-launch"},
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("group invalid: %v", err)
	}
	return group
}

func signalOnce(ch chan struct{}, once *sync.Once) {
	if ch != nil {
		once.Do(func() {
			close(ch)
		})
	}
}

func waitIfSet(ctx context.Context, ch <-chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}
