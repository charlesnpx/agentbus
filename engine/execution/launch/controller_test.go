package launch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

func TestLaunchControllerHappyPathOrdering(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "happy")
	h.authority.beforeRecordRelease = func() error {
		select {
		case <-h.running.waitStarted:
			return nil
		case <-time.After(time.Second):
			return errors.New("wait did not start before record_release")
		}
	}
	h.authority.afterRecordRelease = h.running.allowWait

	result, err := h.controller.Run(ctx, h.request(nil))
	if err != nil {
		t.Fatal(err)
	}
	if !result.ReleaseRecorded {
		t.Fatal("release was not recorded")
	}
	if result.Contained {
		t.Fatal("happy path was contained")
	}
	if h.prepared.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", h.prepared.releaseCalls)
	}
	want := []string{"prepare", "bind_group", "allocate_grant", "release", "wait_start", "record_release", "wait_return", "record_quiescence"}
	if got := h.events.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
}

func TestLaunchControllerHeldStartCancelIntentPreventsFailClosedWait(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "held-start-cancel")
	intent := &ContainmentIntent{}
	postRelease := make(chan struct{})
	allowRecordRelease := make(chan struct{})
	h.authority.beforeRecordRelease = func() error {
		close(postRelease)
		select {
		case <-allowRecordRelease:
			return nil
		case <-time.After(time.Second):
			return errors.New("record release hold timed out")
		}
	}
	h.running.waitErr = errors.New("signal: killed")

	type startResult struct {
		process *Process
		err     error
	}
	started := make(chan startResult, 1)
	go func() {
		request := h.request(nil)
		request.ContainmentIntent = intent
		process, err := h.controller.Start(ctx, request)
		started <- startResult{process: process, err: err}
	}()

	select {
	case <-postRelease:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach post-release hold")
	}
	select {
	case <-h.running.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("eager wait did not start")
	}

	intent.MarkContaining()
	h.running.allowWait()
	select {
	case result := <-started:
		t.Fatalf("Start returned before registration hold was released: process=%v err=%v", result.process, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowRecordRelease)

	var result startResult
	select {
	case result = <-started:
	case <-time.After(time.Second):
		t.Fatal("Start did not return after registration hold release")
	}
	if result.err != nil {
		t.Fatalf("Start error = %v", result.err)
	}
	final, err := result.process.FinalResult(ctx)
	if err != nil {
		t.Fatalf("FinalResult error = %v, want cancel containment without fail-closed", err)
	}
	if !final.Contained {
		t.Fatalf("final result contained = false, want true")
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
}

func TestLaunchControllerWaitErrorWithoutContainmentIntentFailCloses(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "external-wait-error")
	h.running.waitErr = errors.New("signal: killed")
	h.authority.afterRecordRelease = h.running.allowWait

	process, err := h.controller.Start(ctx, h.request(nil))
	if err != nil {
		t.Fatal(err)
	}
	_, err = process.FinalResult(ctx)
	if !errors.Is(err, ErrFailClosed) {
		t.Fatalf("FinalResult error = %v, want ErrFailClosed", err)
	}
	if h.authority.failStops != 1 {
		t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
	}
}

func TestLaunchControllerContainAndVerifyUsesCustodianPort(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "contain-entrypoint")

	verified, cleanup, err := h.controller.ContainAndVerifyWithCleanup(ctx, h.launch, h.group, custodian.QuiescenceCauseContain)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup.Err != nil {
		t.Fatalf("cleanup error = %v, want nil", cleanup.Err)
	}

	if h.custodian.containCalls != 1 {
		t.Fatalf("custodian contain calls = %d, want 1", h.custodian.containCalls)
	}
	if !h.custodian.containedGroup.Equal(h.group) {
		t.Fatalf("contained group = %+v, want %+v", h.custodian.containedGroup, h.group)
	}
	if h.custodian.containCause != custodian.QuiescenceCauseContain {
		t.Fatalf("contain cause = %q, want %q", h.custodian.containCause, custodian.QuiescenceCauseContain)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
	physical, err := h.verifier.VerifyQuiescence(verified)
	if err != nil {
		t.Fatal(err)
	}
	if !physical.Group.Equal(h.group) || physical.Method != model.QuiescenceTermKill {
		t.Fatalf("verified quiescence = %+v, want term-kill for launch group", physical)
	}
}

func TestLaunchControllerFailpointsFailClosed(t *testing.T) {
	tests := []struct {
		name                  string
		point                 Failpoint
		wantReleaseCalls      int
		wantRecordRelease     int
		wantRecordQuiescence  int
		wantContainCalls      int
		wantAbortCalls        int
		wantFailStops         int
		wantAttestationsAtMax int
	}{
		{name: "after prepare", point: FailAfterPrepare, wantAbortCalls: 1, wantAttestationsAtMax: 1},
		{name: "after bind group", point: FailAfterBindGroup, wantAbortCalls: 1, wantRecordQuiescence: 1, wantAttestationsAtMax: 1},
		{name: "after allocate grant", point: FailAfterAllocateGrant, wantContainCalls: 1, wantFailStops: 1, wantAttestationsAtMax: 1},
		{name: "after release", point: FailAfterRelease, wantReleaseCalls: 1, wantContainCalls: 1, wantFailStops: 1, wantAttestationsAtMax: 1},
		{name: "after record release", point: FailAfterRecordRelease, wantReleaseCalls: 1, wantRecordRelease: 1, wantRecordQuiescence: 1, wantContainCalls: 1, wantFailStops: 1, wantAttestationsAtMax: 1},
		{name: "after wait", point: FailAfterWait, wantReleaseCalls: 1, wantRecordRelease: 1, wantRecordQuiescence: 1, wantContainCalls: 1, wantFailStops: 1, wantAttestationsAtMax: 1},
		{name: "after record quiescence", point: FailAfterRecordQuiescence, wantReleaseCalls: 1, wantRecordRelease: 1, wantRecordQuiescence: 1, wantContainCalls: 1, wantFailStops: 1, wantAttestationsAtMax: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "failpoint-"+string(tt.point))
			h.authority.afterRecordRelease = h.running.allowWait
			injector := &FailureInjector{Target: tt.point}

			_, err := h.controller.Run(context.Background(), h.request(injector))
			if err == nil {
				t.Fatal("Run returned nil error for injected failpoint")
			}
			if !injector.Hit {
				t.Fatalf("failpoint %s was not hit", tt.point)
			}
			if h.prepared.releaseCalls != tt.wantReleaseCalls {
				t.Fatalf("release calls = %d, want %d", h.prepared.releaseCalls, tt.wantReleaseCalls)
			}
			if h.authority.recordReleaseCalls != tt.wantRecordRelease {
				t.Fatalf("record release calls = %d, want %d", h.authority.recordReleaseCalls, tt.wantRecordRelease)
			}
			if h.authority.recordQuiescenceCalls != tt.wantRecordQuiescence {
				t.Fatalf("record quiescence calls = %d, want %d", h.authority.recordQuiescenceCalls, tt.wantRecordQuiescence)
			}
			if totalContains := h.running.containCalls + h.custodian.containCalls; totalContains != tt.wantContainCalls {
				t.Fatalf("contain calls running=%d custodian=%d total=%d, want %d", h.running.containCalls, h.custodian.containCalls, totalContains, tt.wantContainCalls)
			}
			if h.prepared.abortCalls != tt.wantAbortCalls {
				t.Fatalf("abort calls = %d, want %d", h.prepared.abortCalls, tt.wantAbortCalls)
			}
			if h.authority.failStops != tt.wantFailStops {
				t.Fatalf("fail stops = %d, want %d", h.authority.failStops, tt.wantFailStops)
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > tt.wantAttestationsAtMax {
				t.Fatalf("attestations = %d, want <= %d", got, tt.wantAttestationsAtMax)
			}
		})
	}
}

func TestLaunchControllerCommitOutcomeUnknownContainsAndFailStops(t *testing.T) {
	tests := []struct {
		name                 string
		configure            func(*harness)
		wantReleaseCalls     int
		wantRecordRelease    int
		wantRecordQuiescence int
	}{
		{
			name: "bind group unknown",
			configure: func(h *harness) {
				h.authority.bindOutcome = CommitOutcomeUnknown
				h.authority.bindErr = errors.New("bind ambiguous")
			},
		},
		{
			name: "allocate grant unknown",
			configure: func(h *harness) {
				h.authority.grantOutcome = CommitOutcomeUnknown
				h.authority.grantErr = errors.New("grant ambiguous")
			},
		},
		{
			name: "record release unknown",
			configure: func(h *harness) {
				h.authority.recordReleaseOutcome = CommitOutcomeUnknown
				h.authority.recordReleaseErr = errors.New("release record ambiguous")
			},
			wantReleaseCalls:  1,
			wantRecordRelease: 1,
		},
		{
			name: "record quiescence unknown",
			configure: func(h *harness) {
				h.authority.recordQuiescenceOutcome = CommitOutcomeUnknown
				h.authority.recordQuiescenceErr = errors.New("quiescence ambiguous")
				h.authority.afterRecordRelease = h.running.allowWait
			},
			wantReleaseCalls:     1,
			wantRecordRelease:    1,
			wantRecordQuiescence: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "unknown-"+tt.name)
			tt.configure(h)

			_, err := h.controller.Run(context.Background(), h.request(nil))
			if err == nil {
				t.Fatal("Run returned nil error for unknown durability")
			}
			if !errors.Is(err, ErrDurabilityUnknown) {
				t.Fatalf("Run error = %v, want ErrDurabilityUnknown", err)
			}
			if h.authority.failStops != 1 {
				t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
			}
			if h.prepared.releaseCalls != tt.wantReleaseCalls {
				t.Fatalf("release calls = %d, want %d", h.prepared.releaseCalls, tt.wantReleaseCalls)
			}
			if h.authority.recordReleaseCalls != tt.wantRecordRelease {
				t.Fatalf("record release calls = %d, want %d", h.authority.recordReleaseCalls, tt.wantRecordRelease)
			}
			if h.authority.recordQuiescenceCalls != tt.wantRecordQuiescence {
				t.Fatalf("record quiescence calls = %d, want %d", h.authority.recordQuiescenceCalls, tt.wantRecordQuiescence)
			}
			if totalContains := h.running.containCalls + h.custodian.containCalls; totalContains == 0 {
				t.Fatal("containment was not attempted")
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > 1 {
				t.Fatalf("attestations = %d, want at most one", got)
			}
		})
	}
}

func TestLaunchControllerWaitAndVerifyErrorContainsAndFailStops(t *testing.T) {
	waitErr := errors.New("wait lifecycle failed")
	h := newHarness(t, "wait-error")
	h.running.waitErr = waitErr
	h.authority.afterRecordRelease = h.running.allowWait

	result, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for wait failure")
	}
	if !errors.Is(err, waitErr) {
		t.Fatalf("Run error = %v, want wait error", err)
	}
	if !errors.Is(err, ErrFailClosed) {
		t.Fatalf("Run error = %v, want ErrFailClosed", err)
	}
	if !result.Contained {
		t.Fatal("wait failure result was not marked contained after successful containment")
	}
	if h.running.containCalls != 1 {
		t.Fatalf("running contain calls = %d, want 1", h.running.containCalls)
	}
	if h.authority.failStops != 1 {
		t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
	if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > 1 {
		t.Fatalf("attestations = %d, want at most one", got)
	}
}

func TestLaunchControllerWaitAndVerifyErrorReportsContainmentFailure(t *testing.T) {
	waitErr := errors.New("wait lifecycle failed")
	containErr := errors.New("contain failed")
	h := newHarness(t, "wait-contain-error")
	h.running.waitErr = waitErr
	h.running.containErr = containErr
	h.authority.afterRecordRelease = h.running.allowWait

	result, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for wait and containment failure")
	}
	if !errors.Is(err, waitErr) {
		t.Fatalf("Run error = %v, want wait error", err)
	}
	if !errors.Is(err, containErr) {
		t.Fatalf("Run error = %v, want containment error", err)
	}
	if !errors.Is(err, ErrFailClosed) {
		t.Fatalf("Run error = %v, want ErrFailClosed", err)
	}
	if result.Contained {
		t.Fatal("failed containment was reported as contained")
	}
	if h.running.containCalls != 1 {
		t.Fatalf("running contain calls = %d, want 1", h.running.containCalls)
	}
	if h.authority.failStops != 1 {
		t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
	}
	if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > 1 {
		t.Fatalf("attestations = %d, want at most one", got)
	}
}

func TestLaunchControllerWaitCleanupUnresolvedDoesNotFailStop(t *testing.T) {
	unresolved := &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonAbsenceDeadlineExceeded,
		Decision: model.SignalDirectly,
	}
	h := newHarness(t, "wait-unresolved")
	h.running.waitErr = unresolved
	h.running.containErr = unresolved
	h.authority.afterRecordRelease = h.running.allowWait

	result, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for wait unresolved cleanup")
	}
	if !custodian.IsCleanupUnresolved(err) {
		t.Fatalf("Run error = %v, want CleanupUnresolvedError", err)
	}
	if errors.Is(err, ErrFailClosed) {
		t.Fatalf("Run error = %v, want no fail-closed marker", err)
	}
	if result.Contained {
		t.Fatal("unresolved containment was reported as contained")
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
}

func TestLaunchControllerRecordQuiescenceFailuresContainAndFailStop(t *testing.T) {
	committedErr := errors.New("quiescence committed but observer failed")
	tests := []struct {
		name    string
		outcome DurabilityOutcome
		err     error
		wantErr error
	}{
		{name: "definitely not committed", outcome: DefinitelyNotCommitted, wantErr: ErrDurabilityNotCommitted},
		{name: "committed with error", outcome: CommittedAndAnchored, err: committedErr, wantErr: committedErr},
		{name: "unknown", outcome: CommitOutcomeUnknown, err: errors.New("quiescence ambiguous"), wantErr: ErrDurabilityUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "record-quiescence-"+tt.name)
			h.authority.afterRecordRelease = h.running.allowWait
			h.authority.recordQuiescenceOutcome = tt.outcome
			h.authority.recordQuiescenceErr = tt.err

			result, err := h.controller.Run(context.Background(), h.request(nil))
			if err == nil {
				t.Fatal("Run returned nil error for record quiescence failure")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run error = %v, want %v", err, tt.wantErr)
			}
			if !errors.Is(err, ErrFailClosed) {
				t.Fatalf("Run error = %v, want ErrFailClosed", err)
			}
			if !result.Contained {
				t.Fatal("record quiescence failure result was not contained")
			}
			if h.authority.recordQuiescenceCalls != 1 {
				t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
			}
			if h.running.containCalls != 1 {
				t.Fatalf("running contain calls = %d, want 1", h.running.containCalls)
			}
			if h.authority.failStops != 1 {
				t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > 1 {
				t.Fatalf("attestations = %d, want at most one", got)
			}
		})
	}
}

func TestLaunchControllerPreGrantSafeAbortRetiresPreparedAndRecordsQuiescence(t *testing.T) {
	bindErr := errors.New("bind committed but observer failed")
	tests := []struct {
		name      string
		configure func(*harness)
		wantErr   error
	}{
		{
			name: "allocate grant definitely not committed after durable bind",
			configure: func(h *harness) {
				h.authority.grantOutcome = DefinitelyNotCommitted
			},
			wantErr: ErrDurabilityNotCommitted,
		},
		{
			name: "bind group committed with error",
			configure: func(h *harness) {
				h.authority.bindErr = bindErr
			},
			wantErr: bindErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "pre-grant-"+tt.name)
			tt.configure(h)

			_, err := h.controller.Run(context.Background(), h.request(nil))
			if err == nil {
				t.Fatal("Run returned nil error for pre-grant safe abort case")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run error = %v, want %v", err, tt.wantErr)
			}
			if h.prepared.abortCalls != 1 {
				t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
			}
			if h.authority.recordQuiescenceCalls != 1 {
				t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
			}
			if h.prepared.releaseCalls != 0 {
				t.Fatalf("release calls = %d, want 0", h.prepared.releaseCalls)
			}
			if totalContains := h.running.containCalls + h.custodian.containCalls; totalContains != 0 {
				t.Fatalf("contain calls running=%d custodian=%d total=%d, want 0", h.running.containCalls, h.custodian.containCalls, totalContains)
			}
			if h.authority.failStops != 0 {
				t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got > 1 {
				t.Fatalf("attestations = %d, want at most one", got)
			}
		})
	}
}

func TestLaunchControllerPreGrantAbortErrorFallsBackToContainAndRecordsQuiescence(t *testing.T) {
	abortErr := errors.New("abort verification failed")
	h := newHarness(t, "pre-grant-abort-fallback")
	h.authority.grantOutcome = DefinitelyNotCommitted
	h.prepared.abortErr = abortErr

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for pre-grant abort fallback case")
	}
	if !errors.Is(err, ErrDurabilityNotCommitted) {
		t.Fatalf("Run error = %v, want ErrDurabilityNotCommitted", err)
	}
	if !errors.Is(err, abortErr) {
		t.Fatalf("Run error = %v, want abort error", err)
	}
	if h.prepared.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
	}
	if h.custodian.containCalls != 1 {
		t.Fatalf("custodian contain calls = %d, want 1", h.custodian.containCalls)
	}
	if !h.custodian.containedGroup.Equal(h.group) {
		t.Fatalf("contained group = %#v, want %#v", h.custodian.containedGroup, h.group)
	}
	if h.custodian.containCause != custodian.QuiescenceCauseContain {
		t.Fatalf("contain cause = %q, want %q", h.custodian.containCause, custodian.QuiescenceCauseContain)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.prepared.releaseCalls != 0 {
		t.Fatalf("release calls = %d, want 0", h.prepared.releaseCalls)
	}
	if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got != 1 {
		t.Fatalf("attestations = %d, want 1", got)
	}
}

func TestLaunchControllerPreGrantAbortAndContainErrorsFailStop(t *testing.T) {
	abortErr := errors.New("abort verification failed")
	containErr := errors.New("containment verification failed")
	h := newHarness(t, "pre-grant-abort-contain-error")
	h.authority.grantOutcome = DefinitelyNotCommitted
	h.prepared.abortErr = abortErr
	h.custodian.containErr = containErr

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for abort and contain failure")
	}
	if !errors.Is(err, abortErr) {
		t.Fatalf("Run error = %v, want abort error", err)
	}
	if !errors.Is(err, containErr) {
		t.Fatalf("Run error = %v, want contain error", err)
	}
	if !errors.Is(err, ErrFailClosed) {
		t.Fatalf("Run error = %v, want ErrFailClosed", err)
	}
	if h.prepared.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
	}
	if h.custodian.containCalls != 1 {
		t.Fatalf("custodian contain calls = %d, want 1", h.custodian.containCalls)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
	if h.authority.failStops != 1 {
		t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
	}
	if h.prepared.releaseCalls != 0 {
		t.Fatalf("release calls = %d, want 0", h.prepared.releaseCalls)
	}
	if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got != 0 {
		t.Fatalf("attestations = %d, want 0", got)
	}
}

func TestLaunchControllerPreGrantAbortUnresolvedAfterDurableBindDoesNotFailStop(t *testing.T) {
	tests := []struct {
		name string
		err  func(model.GroupRef) error
	}{
		{
			name: "cleanup unresolved",
			err: func(model.GroupRef) error {
				return &custodian.CleanupUnresolvedError{
					Reason:   containment.ReasonProbeUnprovable,
					Decision: model.Unprovable,
				}
			},
		},
		{
			name: "direct retained object reacquire unresolved",
			err: func(group model.GroupRef) error {
				return custodian.RetainedObjectReacquireUnresolvedError{
					Group: group,
					Cause: errors.New("retained object disappeared before absence proof"),
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "pre-grant-abort-unresolved-"+tt.name)
			unresolved := tt.err(h.group)
			h.authority.grantOutcome = DefinitelyNotCommitted
			h.prepared.abortErr = unresolved
			h.custodian.containErr = unresolved

			_, err := h.controller.Run(context.Background(), h.request(nil))
			if err == nil {
				t.Fatal("Run returned nil error for unresolved pre-grant abort")
			}
			if !errors.Is(err, ErrDurabilityNotCommitted) {
				t.Fatalf("Run error = %v, want ErrDurabilityNotCommitted", err)
			}
			if !physicalCleanupUnresolved(err) {
				t.Fatalf("Run error = %v, want typed physical cleanup uncertainty", err)
			}
			if errors.Is(err, ErrFailClosed) {
				t.Fatalf("Run error = %v, want no fail-closed marker", err)
			}
			if h.prepared.abortCalls != 1 {
				t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
			}
			if h.custodian.containCalls != 1 {
				t.Fatalf("custodian contain calls = %d, want 1", h.custodian.containCalls)
			}
			if h.authority.recordQuiescenceCalls != 0 {
				t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
			}
			if h.authority.failStops != 0 {
				t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
			}
			if h.prepared.releaseCalls != 0 {
				t.Fatalf("release calls = %d, want 0", h.prepared.releaseCalls)
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got != 0 {
				t.Fatalf("attestations = %d, want 0", got)
			}
		})
	}
}

func TestLaunchControllerPreGrantRecordQuiescenceFailuresFailStop(t *testing.T) {
	committedErr := errors.New("quiescence observer failed")
	tests := []struct {
		name    string
		outcome DurabilityOutcome
		err     error
		wantErr error
	}{
		{name: "definitely not committed", outcome: DefinitelyNotCommitted, wantErr: ErrDurabilityNotCommitted},
		{name: "unknown", outcome: CommitOutcomeUnknown, wantErr: ErrDurabilityUnknown},
		{name: "committed with error", outcome: CommittedAndAnchored, err: committedErr, wantErr: committedErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "pre-grant-quiescence-"+tt.name)
			h.authority.grantOutcome = DefinitelyNotCommitted
			h.authority.recordQuiescenceOutcome = tt.outcome
			h.authority.recordQuiescenceErr = tt.err

			_, err := h.controller.Run(context.Background(), h.request(nil))
			if err == nil {
				t.Fatal("Run returned nil error for pre-grant record quiescence failure")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Run error = %v, want %v", err, tt.wantErr)
			}
			if !errors.Is(err, ErrFailClosed) {
				t.Fatalf("Run error = %v, want ErrFailClosed", err)
			}
			if h.prepared.abortCalls != 1 {
				t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
			}
			if h.custodian.containCalls != 0 {
				t.Fatalf("custodian contain calls = %d, want 0", h.custodian.containCalls)
			}
			if h.authority.recordQuiescenceCalls != 1 {
				t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
			}
			if h.authority.failStops != 1 {
				t.Fatalf("fail stops = %d, want 1", h.authority.failStops)
			}
			if h.prepared.releaseCalls != 0 {
				t.Fatalf("release calls = %d, want 0", h.prepared.releaseCalls)
			}
			if got := h.running.attestations + h.prepared.attestations + h.custodian.attestations; got != 1 {
				t.Fatalf("attestations = %d, want 1", got)
			}
		})
	}
}

func TestLaunchProcessDoneAndFinalResultExposeTerminalResult(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t, "done-final")

	process, err := h.controller.Start(ctx, h.request(nil))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.running.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("wait did not start eagerly")
	}
	select {
	case <-process.Done():
		t.Fatal("process reported done before wait was allowed")
	default:
	}

	h.running.allowWait()
	select {
	case <-process.Done():
	case <-time.After(time.Second):
		t.Fatal("process did not report done after wait completed")
	}
	result, err := process.FinalResult(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ReleaseRecorded {
		t.Fatal("final result did not record release")
	}
	if result.Contained {
		t.Fatal("happy final result was contained")
	}
}

func TestLaunchControllerEagerWaitReportsContainedAndNaturalResults(t *testing.T) {
	tests := []struct {
		name             string
		waitContains     bool
		wantContained    bool
		wantContainCalls int
		wantMethod       model.QuiescenceMethod
	}{
		{
			name:             "residual group contained by wait",
			waitContains:     true,
			wantContained:    true,
			wantContainCalls: 1,
			wantMethod:       model.QuiescenceTermKill,
		},
		{
			name:       "natural exit",
			wantMethod: model.QuiescenceNaturalExit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, "eager-wait-"+tt.name)
			h.running.waitContains = tt.waitContains
			h.authority.afterRecordRelease = h.running.allowWait

			result, err := h.controller.Run(context.Background(), h.request(nil))
			if err != nil {
				t.Fatal(err)
			}
			if result.Contained != tt.wantContained {
				t.Fatalf("contained = %t, want %t", result.Contained, tt.wantContained)
			}
			if h.running.containCalls != tt.wantContainCalls {
				t.Fatalf("running contain calls = %d, want %d", h.running.containCalls, tt.wantContainCalls)
			}
			if h.authority.failStops != 0 {
				t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
			}
			payload, err := h.verifier.VerifyQuiescence(result.Verified)
			if err != nil {
				t.Fatal(err)
			}
			if payload.Method != tt.wantMethod {
				t.Fatalf("quiescence method = %s, want %s", payload.Method, tt.wantMethod)
			}
		})
	}
}

func TestLaunchControllerRecordsQuiescenceBeforeSurfacingCleanupFailure(t *testing.T) {
	h := newHarness(t, "wait-cleanup")
	cleanupErr := errors.New("retained remove failed")
	h.running.waitCleanup = cleanupErr
	h.authority.afterRecordRelease = h.running.allowWait

	result, err := h.controller.Run(context.Background(), h.request(nil))
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("Run error = %v, want cleanup failure", err)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
	if h.running.containCalls != 0 {
		t.Fatalf("running contain calls = %d, want 0 cleanup retry", h.running.containCalls)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	payload, verifyErr := h.verifier.VerifyQuiescence(result.Verified)
	if verifyErr != nil {
		t.Fatalf("VerifyQuiescence() error = %v", verifyErr)
	}
	if payload.Method != model.QuiescenceNaturalExit {
		t.Fatalf("quiescence method = %s, want %s", payload.Method, model.QuiescenceNaturalExit)
	}
}

func TestLaunchControllerReleaseErrorContainsWithoutRetry(t *testing.T) {
	h := newHarness(t, "release-error")
	h.prepared.releaseOutcome = custodian.ReleaseOutcomeUnknown
	h.prepared.releaseErr = errors.New("release channel lost")

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for release failure")
	}
	if !errors.Is(err, ErrReleaseUncertain) {
		t.Fatalf("Run error = %v, want ErrReleaseUncertain", err)
	}
	if h.prepared.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", h.prepared.releaseCalls)
	}
	if h.custodian.containCalls != 1 {
		t.Fatalf("custodian contain calls = %d, want 1", h.custodian.containCalls)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
}

func TestLaunchControllerReleaseUnknownCleanupUnresolvedDoesNotFailStop(t *testing.T) {
	h := newHarness(t, "release-unknown-unresolved")
	h.prepared.releaseOutcome = custodian.ReleaseOutcomeUnknown
	h.prepared.releaseErr = errors.New("release channel lost")
	h.custodian.containErr = &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonProbeUnprovable,
		Decision: model.Unprovable,
	}

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for release-unknown unresolved cleanup")
	}
	if !errors.Is(err, ErrReleaseUncertain) {
		t.Fatalf("Run error = %v, want ErrReleaseUncertain", err)
	}
	if !custodian.IsCleanupUnresolved(err) {
		t.Fatalf("Run error = %v, want CleanupUnresolvedError", err)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.authority.releaseOutcomeFact != model.LaunchReleaseSentUnknown {
		t.Fatalf("release outcome fact = %s, want %s", h.authority.releaseOutcomeFact, model.LaunchReleaseSentUnknown)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
}

func TestLaunchControllerReleaseUnknownRetainedObjectUnresolvedDoesNotFailStop(t *testing.T) {
	h := newHarness(t, "release-unknown-retained-unresolved")
	h.prepared.releaseOutcome = custodian.ReleaseOutcomeUnknown
	h.prepared.releaseErr = errors.New("release channel lost")
	h.custodian.containErr = &custodian.CleanupUnresolvedError{
		Reason:   containment.ReasonProbeUnprovable,
		Decision: model.Unprovable,
		Cause: custodian.RetainedObjectReacquireUnresolvedError{
			Group: h.group,
			Cause: errors.New("retained object disappeared before absence proof"),
		},
	}

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for release-unknown retained-object unresolved cleanup")
	}
	if !errors.Is(err, ErrReleaseUncertain) {
		t.Fatalf("Run error = %v, want ErrReleaseUncertain", err)
	}
	if !custodian.IsCleanupUnresolved(err) {
		t.Fatalf("Run error = %v, want CleanupUnresolvedError", err)
	}
	if !errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
		t.Fatalf("Run error = %v, want ErrRetainedObjectReacquireUnresolved", err)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.authority.releaseOutcomeFact != model.LaunchReleaseSentUnknown {
		t.Fatalf("release outcome fact = %s, want %s", h.authority.releaseOutcomeFact, model.LaunchReleaseSentUnknown)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
}

func TestLaunchControllerReleaseUnknownDirectRetainedObjectUnresolvedDoesNotFailStop(t *testing.T) {
	h := newHarness(t, "release-unknown-direct-retained-unresolved")
	h.prepared.releaseOutcome = custodian.ReleaseOutcomeUnknown
	h.prepared.releaseErr = errors.New("release channel lost")
	h.custodian.containErr = custodian.RetainedObjectReacquireUnresolvedError{
		Group: h.group,
		Cause: errors.New("retained object disappeared before absence proof"),
	}

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for release-unknown direct retained-object unresolved cleanup")
	}
	if !errors.Is(err, ErrReleaseUncertain) {
		t.Fatalf("Run error = %v, want ErrReleaseUncertain", err)
	}
	if !errors.Is(err, custodian.ErrRetainedObjectReacquireUnresolved) {
		t.Fatalf("Run error = %v, want ErrRetainedObjectReacquireUnresolved", err)
	}
	if errors.Is(err, ErrFailClosed) {
		t.Fatalf("Run error = %v, want no fail-closed marker", err)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
	if h.authority.releaseOutcomeFact != model.LaunchReleaseSentUnknown {
		t.Fatalf("release outcome fact = %s, want %s", h.authority.releaseOutcomeFact, model.LaunchReleaseSentUnknown)
	}
	if h.authority.recordQuiescenceCalls != 0 {
		t.Fatalf("record quiescence calls = %d, want 0", h.authority.recordQuiescenceCalls)
	}
}

func TestLaunchControllerReleaseDefinitelyNotSentAbortsWithoutRetry(t *testing.T) {
	h := newHarness(t, "release-not-sent")
	h.prepared.releaseOutcome = custodian.ReleaseDefinitelyNotSent
	h.prepared.releaseErr = errors.New("release pipe closed before write")

	_, err := h.controller.Run(context.Background(), h.request(nil))
	if err == nil {
		t.Fatal("Run returned nil error for definitely-not-sent release")
	}
	if !errors.Is(err, ErrReleaseUncertain) {
		t.Fatalf("Run error = %v, want ErrReleaseUncertain", err)
	}
	if h.prepared.releaseCalls != 1 {
		t.Fatalf("release calls = %d, want 1", h.prepared.releaseCalls)
	}
	if h.prepared.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", h.prepared.abortCalls)
	}
	if h.custodian.containCalls != 0 {
		t.Fatalf("custodian contain calls = %d, want 0", h.custodian.containCalls)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
	if h.authority.failStops != 0 {
		t.Fatalf("fail stops = %d, want 0", h.authority.failStops)
	}
}

func TestLaunchControllerConcurrentWaitAndCancelHasOneFinalAttestation(t *testing.T) {
	h := newHarness(t, "concurrent")
	process, err := h.controller.Start(context.Background(), h.request(nil))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.running.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("wait did not start eagerly")
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- process.Interrupt(context.Background())
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := process.Wait(context.Background())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent wait/cancel error = %v", err)
		}
	}
	if h.running.containCalls != 1 {
		t.Fatalf("running contain calls = %d, want 1", h.running.containCalls)
	}
	if h.running.attestations != 1 {
		t.Fatalf("attestations = %d, want 1", h.running.attestations)
	}
	if h.authority.recordQuiescenceCalls != 1 {
		t.Fatalf("record quiescence calls = %d, want 1", h.authority.recordQuiescenceCalls)
	}
}

type harness struct {
	events     *eventLog
	controller *LaunchController
	authority  *fakeAuthority
	custodian  *fakeCustodian
	prepared   *fakePrepared
	running    *fakeRunning
	launch     LaunchContext
	group      model.GroupRef
	grant      model.LaunchGrant
	verifier   custodian.AttestationVerifier
}

func newHarness(t *testing.T, name string) *harness {
	t.Helper()
	events := &eventLog{}
	issuer, verifier := custodian.NewAttestationChannel()
	launch := LaunchContext{
		JobID:   model.JobID("job-" + sanitizeName(name)),
		Attempt: model.AttemptRef{JobID: model.JobID("job-" + sanitizeName(name)), AttemptID: model.AttemptID("attempt-" + sanitizeName(name)), Epoch: 1},
		Ordinal: model.LaunchOrdinalOne,
	}
	group := testGroup(launch, sanitizeName(name))
	grant := model.LaunchGrant{
		Attempt:   launch.Attempt,
		Ordinal:   launch.Ordinal,
		Nonce:     model.LaunchNonce("grant-" + sanitizeName(name)),
		GrantedBy: model.BootRef{BootID: model.BootID("boot-" + sanitizeName(name)), OwnerID: model.OwnerID("owner-" + sanitizeName(name))},
	}
	running := &fakeRunning{
		events:      events,
		group:       group,
		issuer:      issuer,
		waitStarted: make(chan struct{}),
		allowWaitCh: make(chan struct{}),
		stdin:       nopWriteCloser{bytes.NewBuffer(nil)},
		stdout:      io.NopCloser(bytes.NewBuffer(nil)),
		stderr:      io.NopCloser(bytes.NewBuffer(nil)),
	}
	prepared := &fakePrepared{events: events, group: group, running: running, issuer: issuer}
	cust := &fakeCustodian{events: events, prepared: prepared, issuer: issuer}
	auth := &fakeAuthority{
		events:                  events,
		grant:                   grant,
		bindOutcome:             CommittedAndAnchored,
		grantOutcome:            CommittedAndAnchored,
		recordReleaseOutcome:    CommittedAndAnchored,
		recordQuiescenceOutcome: CommittedAndAnchored,
	}
	controller, err := New(auth, cust)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		events:     events,
		controller: controller,
		authority:  auth,
		custodian:  cust,
		prepared:   prepared,
		running:    running,
		launch:     launch,
		group:      group,
		grant:      grant,
		verifier:   verifier,
	}
}

func (h *harness) request(injector *FailureInjector) LaunchRequest {
	return LaunchRequest{
		Context:  h.launch,
		Exec:     command.ExecSpec{Argv: []string{"/bin/fake"}},
		Failures: injector,
	}
}

func testGroup(launch LaunchContext, name string) model.GroupRef {
	return model.GroupRef{
		Version:             1,
		CustodyID:           model.CustodyID("custody-" + name),
		Launch:              launch.Key(),
		HostBootID:          "host-boot-" + name,
		PIDNamespaceState:   model.PIDNamespaceNotApplicable,
		RetainedDomainID:    "retained-domain-" + name,
		RetainedDomainState: model.RetainedDomainKnown,
		PGID:                2001,
		Leader:              model.ProcessIdentity{PID: 2001, HighResStartToken: "leader-" + name},
		Monitor:             model.ProcessIdentity{PID: 3001, HighResStartToken: "monitor-" + name},
		RetainedID:          "retained-" + name,
	}
}

func sanitizeName(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			out = append(out, c)
			continue
		}
		out = append(out, '-')
	}
	return string(out)
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (log *eventLog) add(event string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = append(log.events, event)
}

func (log *eventLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

type fakeAuthority struct {
	events *eventLog
	grant  model.LaunchGrant

	bindOutcome             DurabilityOutcome
	bindErr                 error
	grantOutcome            DurabilityOutcome
	grantErr                error
	recordReleaseOutcomeErr error
	releaseOutcomeFact      model.LaunchReleaseOutcome
	recordReleaseOutcome    DurabilityOutcome
	recordReleaseErr        error
	recordQuiescenceOutcome DurabilityOutcome
	recordQuiescenceErr     error

	beforeRecordRelease func() error
	afterRecordRelease  func()

	recordReleaseCalls    int
	recordQuiescenceCalls int
	failStops             int
}

func (authority *fakeAuthority) BindGroup(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.GroupRef) (DurabilityOutcome, error) {
	authority.events.add("bind_group")
	return authority.bindOutcome, authority.bindErr
}

func (authority *fakeAuthority) AllocateGrant(context.Context, model.AttemptRef, model.LaunchOrdinal) (model.LaunchGrant, DurabilityOutcome, error) {
	authority.events.add("allocate_grant")
	return authority.grant, authority.grantOutcome, authority.grantErr
}

func (authority *fakeAuthority) RecordReleaseOutcome(_ context.Context, _ model.JobID, _ model.AttemptRef, _ model.LaunchOrdinal, outcome model.LaunchReleaseOutcome) (DurabilityOutcome, error) {
	authority.releaseOutcomeFact = outcome
	authority.events.add("record_release_outcome")
	return authority.recordReleaseOutcome, authority.recordReleaseOutcomeErr
}

func (authority *fakeAuthority) RecordRelease(context.Context, model.JobID, model.AttemptRef, model.LaunchOrdinal, model.ChildIdentity, model.Evidence) (DurabilityOutcome, error) {
	if authority.beforeRecordRelease != nil {
		if err := authority.beforeRecordRelease(); err != nil {
			return DefinitelyNotCommitted, err
		}
	}
	authority.recordReleaseCalls++
	authority.events.add("record_release")
	if authority.afterRecordRelease != nil {
		authority.afterRecordRelease()
	}
	return authority.recordReleaseOutcome, authority.recordReleaseErr
}

func (authority *fakeAuthority) RecordQuiescence(context.Context, model.JobID, model.LaunchOrdinal, custodian.VerifiedQuiescence) (DurabilityOutcome, error) {
	authority.recordQuiescenceCalls++
	authority.events.add("record_quiescence")
	return authority.recordQuiescenceOutcome, authority.recordQuiescenceErr
}

func (authority *fakeAuthority) FailStop(context.Context, error) error {
	authority.failStops++
	authority.events.add("fail_stop")
	return nil
}

type fakeCustodian struct {
	events         *eventLog
	prepared       *fakePrepared
	issuer         custodian.AttestationIssuer
	containErr     error
	cleanupErr     error
	containCalls   int
	containedGroup model.GroupRef
	containCause   custodian.QuiescenceCause
	attestations   int
}

func (cust *fakeCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	cust.events.add("prepare")
	return cust.prepared, nil
}

func (cust *fakeCustodian) ContainAndVerify(_ context.Context, group model.GroupRef, cause custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	cust.containCalls++
	cust.containedGroup = group
	cust.containCause = cause
	cust.events.add("custodian_contain")
	if cust.containErr != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, cust.containErr
	}
	cust.attestations++
	verified, err := cust.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: model.QuiescenceTermKill})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: cust.cleanupErr}, nil
}

type fakePrepared struct {
	events         *eventLog
	group          model.GroupRef
	running        *fakeRunning
	issuer         custodian.AttestationIssuer
	releaseErr     error
	releaseOutcome custodian.ReleaseOutcome
	abortErr       error
	abortCleanup   error
	releaseCalls   int
	abortCalls     int
	attestations   int
}

func (prepared *fakePrepared) Ref() model.GroupRef {
	return prepared.group
}

func (prepared *fakePrepared) Release(context.Context) (RunningProcess, custodian.ReleaseOutcome, error) {
	prepared.releaseCalls++
	prepared.events.add("release")
	outcome := prepared.releaseOutcome
	if outcome == 0 {
		outcome = custodian.ReleaseAccepted
	}
	if prepared.releaseErr != nil {
		return nil, outcome, prepared.releaseErr
	}
	if outcome != custodian.ReleaseAccepted {
		return nil, outcome, nil
	}
	return prepared.running, outcome, nil
}

func (prepared *fakePrepared) AbortAndVerify(context.Context) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	prepared.abortCalls++
	prepared.events.add("abort")
	if prepared.abortErr != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, prepared.abortErr
	}
	prepared.attestations++
	verified, err := prepared.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: prepared.group, Method: model.QuiescenceAlreadyAbsent})
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: prepared.abortCleanup}, nil
}

type fakeRunning struct {
	events      *eventLog
	group       model.GroupRef
	issuer      custodian.AttestationIssuer
	waitStarted chan struct{}
	allowWaitCh chan struct{}
	waitOnce    sync.Once
	allowOnce   sync.Once

	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	mu             sync.Mutex
	waitErr        error
	waitCleanup    error
	waitContains   bool
	waitContained  bool
	containErr     error
	containCleanup error
	containCalls   int
	attestations   int
	verified       custodian.VerifiedQuiescence
}

func (running *fakeRunning) Ref() model.GroupRef {
	return running.group
}

func (running *fakeRunning) Stdin() io.WriteCloser {
	return running.stdin
}

func (running *fakeRunning) Stdout() io.ReadCloser {
	return running.stdout
}

func (running *fakeRunning) Stderr() io.ReadCloser {
	return running.stderr
}

func (running *fakeRunning) WaitAndVerify(ctx context.Context) (command.ExitObservation, custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	running.events.add("wait_start")
	running.waitOnce.Do(func() { close(running.waitStarted) })
	select {
	case <-running.allowWaitCh:
		running.events.add("wait_return")
		if running.waitErr != nil {
			return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, running.waitErr
		}
		if running.waitContains {
			verified, cleanup, err := running.ContainAndVerify(ctx, custodian.QuiescenceCauseWait)
			return command.ExitObservation{Exited: true, Code: 0}, verified, cleanup, err
		}
		verified, err := running.attest(model.QuiescenceNaturalExit)
		if err != nil {
			return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
		}
		return command.ExitObservation{Exited: true, Code: 0}, verified, custodian.CleanupStatus{Err: running.waitCleanup}, nil
	case <-ctx.Done():
		return command.ExitObservation{}, custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, ctx.Err()
	}
}

func (running *fakeRunning) ContainAndVerify(context.Context, custodian.QuiescenceCause) (custodian.VerifiedQuiescence, custodian.CleanupStatus, error) {
	running.mu.Lock()
	running.containCalls++
	running.waitContained = true
	running.mu.Unlock()
	running.events.add("running_contain")
	if running.containErr != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, running.containErr
	}
	verified, err := running.attest(model.QuiescenceTermKill)
	if err != nil {
		return custodian.VerifiedQuiescence{}, custodian.CleanupStatus{}, err
	}
	return verified, custodian.CleanupStatus{Err: running.containCleanup}, nil
}

func (running *fakeRunning) WaitContained() bool {
	running.mu.Lock()
	defer running.mu.Unlock()
	return running.waitContained
}

func (running *fakeRunning) allowWait() {
	running.allowOnce.Do(func() { close(running.allowWaitCh) })
}

func (running *fakeRunning) attest(method model.QuiescenceMethod) (custodian.VerifiedQuiescence, error) {
	running.mu.Lock()
	if running.attestations != 0 {
		verified := running.verified
		running.mu.Unlock()
		return verified, nil
	}
	running.attestations++
	running.mu.Unlock()
	verified, err := running.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: running.group, Method: method})
	running.mu.Lock()
	running.verified = verified
	running.mu.Unlock()
	return verified, err
}

type nopWriteCloser struct {
	*bytes.Buffer
}

func (writer nopWriteCloser) Close() error {
	return nil
}
