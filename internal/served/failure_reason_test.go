package served

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/launch"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestClassifyTerminalFailure(t *testing.T) {
	t.Parallel()
	const providerCapacityMessage = "Selected model is at capacity. Please try a different model."
	tests := []struct {
		name                  string
		origin                terminalFailureOrigin
		err                   error
		agentbusRequestedStop bool
		want                  engine.FailureClass
		wantReasonContains    string
	}{
		{
			name:   "backend not started",
			origin: terminalFailureBackendNotStarted,
			err:    errors.New("runner was not admitted"),
			want:   engine.FailureClassBackendNotStarted,
		},
		{
			name:   "pre-launch overload remains not started",
			origin: terminalFailureBackendNotStarted,
			err:    fmt.Errorf("provider refused before launch: %w", engine.ErrProviderOverloaded),
			want:   engine.FailureClassBackendNotStarted,
		},
		{
			name:   "backend error",
			origin: terminalFailureBackendRan,
			err:    errors.New("backend exited 1"),
			want:   engine.FailureClassBackendError,
		},
		{
			name:   "oversized backend transport frame",
			origin: terminalFailureBackendRan,
			err:    fmt.Errorf("transport: %w", engine.ErrTransportFrameTooLarge),
			want:   engine.FailureClassTransportFrameTooLarge,
		},
		{
			name:               "provider overload sentinel preserves provider message",
			origin:             terminalFailureBackendRan,
			err:                fmt.Errorf("codex app-server provider overload: %s: %w", providerCapacityMessage, engine.ErrProviderOverloaded),
			want:               engine.FailureClassProviderOverloaded,
			wantReasonContains: providerCapacityMessage,
		},
		{
			name:   "unrequested codex interruption",
			origin: terminalFailureBackendRan,
			err:    fmt.Errorf("codex app-server turn interrupted before completion: %w", engine.ErrTurnInterrupted),
			want:   engine.FailureClassBackendInterrupted,
		},
		{
			name:   "interruption takes precedence over provider overload",
			origin: terminalFailureBackendRan,
			err:    errors.Join(engine.ErrTurnInterrupted, engine.ErrProviderOverloaded),
			want:   engine.FailureClassBackendInterrupted,
		},
		{
			name:   "backend text cannot spoof interruption",
			origin: terminalFailureBackendRan,
			err:    errors.New("ordinary backend failure: turn interrupted before completion"),
			want:   engine.FailureClassBackendError,
		},
		{
			name:   "unrequested context interruption",
			origin: terminalFailureBackendRan,
			err:    context.Canceled,
			want:   engine.FailureClassBackendInterrupted,
		},
		{
			name:                  "requested context cancellation is not backend interruption",
			origin:                terminalFailureBackendRan,
			err:                   context.Canceled,
			agentbusRequestedStop: true,
			want:                  engine.FailureClassInternalError,
		},
		{
			name:   "result finalization error",
			origin: terminalFailureFinalization,
			err:    errors.New("write result: disk full"),
			want:   engine.FailureClassFinalizationError,
		},
		{
			name:   "internal error",
			origin: terminalFailureInternal,
			err:    errors.New("policy validation failed"),
			want:   engine.FailureClassInternalError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTerminalFailure(tt.origin, tt.err, tt.agentbusRequestedStop); got != tt.want {
				t.Fatalf("classifyTerminalFailure(%v, %v, requested=%t) = %q, want %q", tt.origin, tt.err, tt.agentbusRequestedStop, got, tt.want)
			}
			if tt.wantReasonContains != "" {
				failure := terminalFailureFor(tt.origin, tt.err, tt.agentbusRequestedStop)
				if !strings.Contains(failure.reason, tt.wantReasonContains) {
					t.Fatalf("failure reason = %q, want provider message %q", failure.reason, tt.wantReasonContains)
				}
			}
		})
	}
}

func TestAuthorityFailureMetadataOnlyForFailureOrInterruptedTerminalStates(t *testing.T) {
	projection := model.JobProjection{
		FailureClass:  engine.FailureClassBackendInterrupted,
		FailureReason: "retained for an interrupted backend turn",
	}
	for _, public := range model.AllPublicStates() {
		projection.Public = public
		gotReason, gotClass := authorityFailureMetadata(projection)
		wantExposed := public == model.PublicFailed || public == model.PublicInterrupted || public == model.PublicQuarantined
		if wantExposed {
			if gotReason != projection.FailureReason || gotClass != projection.FailureClass {
				t.Fatalf("failure metadata for %s = (%q, %q), want (%q, %q)", public, gotReason, gotClass, projection.FailureReason, projection.FailureClass)
			}
			continue
		}
		if gotReason != "" || gotClass != "" {
			t.Fatalf("failure metadata for %s = (%q, %q), want empty", public, gotReason, gotClass)
		}
	}
}

type turnWithRunnerFailureSession struct {
	err error
}

func (s turnWithRunnerFailureSession) ID() string { return "turn-with-runner-failure" }

func (turnWithRunnerFailureSession) Turn(context.Context, engine.TurnInput) (<-chan engine.Event, error) {
	return nil, errors.New("direct turn must not be used")
}

func (s turnWithRunnerFailureSession) TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error) {
	return nil, s.err
}

func (turnWithRunnerFailureSession) Interrupt(context.Context) error { return nil }

func TestAdmissionTurnEventsClassifiesLaunchProvenance(t *testing.T) {
	t.Parallel()
	server, _, _ := newUnstartedTestServer(t, newFakeBackend("fake"))
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "failure-provenance")

	server.admissionStateMu.RLock()
	submission := server.admissionSubmission
	server.admissionStateMu.RUnlock()
	base := jobRun{admissionLaunch: admissionLaunchBinding{
		coordinator:       submission,
		jobID:             accepted.Record.JobID,
		attempt:           accepted.Record.Attempt.Ref,
		containmentIntent: &launch.ContainmentIntent{},
	}}

	tests := []struct {
		name string
		run  jobRun
		want terminalFailureOrigin
	}{
		{
			name: "pre-launch session contract failure",
			run: func() jobRun {
				run := base
				run.session = &nonOrdinalSession{id: "pre-launch"}
				return run
			}(),
			want: terminalFailureBackendNotStarted,
		},
		{
			name: "launch runner failure is conservative",
			run: func() jobRun {
				run := base
				run.session = turnWithRunnerFailureSession{err: errors.New("must not reach turn")}
				run.admissionLaunch.jobID = ""
				return run
			}(),
			want: terminalFailureBackendRan,
		},
		{
			name: "turn with runner failure is post-launch",
			run: func() jobRun {
				run := base
				run.session = turnWithRunnerFailureSession{err: errors.New("turn setup failed")}
				return run
			}(),
			want: terminalFailureBackendRan,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.admissionTurnEvents(context.Background(), tt.run, engine.TurnInput{}, model.LaunchOrdinalOne)
			if err == nil {
				t.Fatal("admissionTurnEvents error = nil")
			}
			if got := terminalFailureOriginFor(err, terminalFailureInternal); got != tt.want {
				t.Fatalf("terminal failure origin = %v, want %v (error: %v)", got, tt.want, err)
			}
		})
	}
}

func TestRunAttemptClassifiesFinalAttemptTimingRecordFailureAsBackendNotStarted(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, _ := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	accepted := acceptIdentifiedAuthorityWork(t, server, "final-attempt-record-failure")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := server.runAttempt(ctx, jobRun{
		jobID:               accepted.Record.JobID.String(),
		admissionControlled: true,
	}, "must not launch", false, model.LaunchOrdinalOne)
	if err == nil {
		t.Fatal("runAttempt error = nil, want canceled timing-record failure")
	}
	origin := terminalFailureOriginFor(err, terminalFailureInternal)
	if got := classifyTerminalFailure(origin, err, false); got != engine.FailureClassBackendNotStarted {
		t.Fatalf("failure class = %q, want %q (error: %v)", got, engine.FailureClassBackendNotStarted, err)
	}
	if got := backend.count.Load(); got != 0 {
		t.Fatalf("backend starts = %d, want 0", got)
	}
}

func TestFailedJobExposesSanitizedFailureMetadata(t *testing.T) {
	t.Parallel()
	hostile := "backend failed\n" + strings.Repeat("界", admissionProbeReasonMaxRunes) + "\x00tail"
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventTerminalError, Text: hostile}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-failure-metadata",
		RequestID:    "request-failure-metadata",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "fail",
		},
	})
	waitAdmissionSafetyTerminal(t, server, job.JobID)

	wantReason := sanitizeAdmissionProbeReason(hostile)
	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 {
		t.Fatalf("job.status jobs = %+v, want one job", status.Jobs)
	}
	if got := status.Jobs[0].FailureClass; got != engine.FailureClassBackendError {
		t.Fatalf("job.status failure class = %q, want %q", got, engine.FailureClassBackendError)
	}
	if got := status.Jobs[0].FailureReason; got != wantReason {
		t.Fatalf("job.status failure reason = %q, want %q", got, wantReason)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if got := result.FailureClass; got != engine.FailureClassBackendError {
		t.Fatalf("job.result failure class = %q, want %q", got, engine.FailureClassBackendError)
	}
	if got := result.FailureReason; got != wantReason {
		t.Fatalf("job.result failure reason = %q, want %q", got, wantReason)
	}
	if got := utf8.RuneCountInString(result.FailureReason); got > admissionProbeReasonMaxRunes {
		t.Fatalf("job.result failure reason runes = %d, want <= %d", got, admissionProbeReasonMaxRunes)
	}
	if strings.ContainsAny(result.FailureReason, "\n\r\x00") {
		t.Fatalf("job.result failure reason retained control characters: %q", result.FailureReason)
	}
}

func TestTransportFrameFailurePersistsDroppedFrameMetadata(t *testing.T) {
	t.Parallel()
	drops := engine.TransportFrameDrops{
		Count:          1,
		Bytes:          33 * 1024 * 1024,
		RedactedPrefix: "method=turn/completed",
	}
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{
			{Type: engine.EventWarning, Text: "discarded backend transport frame", Metadata: drops.EventMetadata()},
			{Type: engine.EventTerminalError, Text: engine.ErrTransportFrameTooLarge.Error(), Err: engine.ErrTransportFrameTooLarge},
		}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-transport-frame",
		RequestID:    "request-transport-frame",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Prompt:  "fail",
		},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if got := record.FailureClass; got != engine.FailureClassTransportFrameTooLarge {
		t.Fatalf("durable failure class = %q, want %q; reason=%q", got, engine.FailureClassTransportFrameTooLarge, record.FailureReason)
	}
	if record.TransportFrameDrops == nil || *record.TransportFrameDrops != drops {
		t.Fatalf("durable transport frame drops = %#v, want %#v", record.TransportFrameDrops, drops)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.TransportFrameDrops == nil || *result.TransportFrameDrops != drops {
		t.Fatalf("job.result transport frame drops = %#v, want %#v", result.TransportFrameDrops, drops)
	}
}

func TestInterruptedJobExposesFailureMetadata(t *testing.T) {
	t.Parallel()
	interruption := fmt.Errorf("backend turn stopped unexpectedly: %w", context.Canceled)
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventTerminalError, Text: interruption.Error(), Err: interruption}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-interrupted-metadata",
		RequestID:    "request-interrupted-metadata",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "interrupt",
		},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Cancel != nil {
		t.Fatalf("interrupted record cancel = %+v, want no requested cancel", record.Cancel)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeInterrupted {
		t.Fatalf("terminal record = %+v, want interrupted", record.Terminal)
	}
	if got := record.FailureClass; got != engine.FailureClassBackendInterrupted {
		t.Fatalf("durable failure class = %q, want %q", got, engine.FailureClassBackendInterrupted)
	}
	if got := record.FailureReason; got != interruption.Error() {
		t.Fatalf("durable failure reason = %q, want %q", got, interruption.Error())
	}

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 {
		t.Fatalf("job.status jobs = %+v, want one job", status.Jobs)
	}
	if got := status.Jobs[0].State; got != engine.StateInterrupted {
		t.Fatalf("job.status state = %q, want %q", got, engine.StateInterrupted)
	}
	if got := status.Jobs[0].FailureClass; got != engine.FailureClassBackendInterrupted {
		t.Fatalf("job.status failure class = %q, want %q", got, engine.FailureClassBackendInterrupted)
	}
	if got := status.Jobs[0].FailureReason; got != interruption.Error() {
		t.Fatalf("job.status failure reason = %q, want %q", got, interruption.Error())
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if got := result.State; got != engine.StateInterrupted {
		t.Fatalf("job.result state = %q, want %q", got, engine.StateInterrupted)
	}
	if got := result.FailureClass; got != engine.FailureClassBackendInterrupted {
		t.Fatalf("job.result failure class = %q, want %q", got, engine.FailureClassBackendInterrupted)
	}
	if got := result.FailureReason; got != interruption.Error() {
		t.Fatalf("job.result failure reason = %q, want %q", got, interruption.Error())
	}
}

func TestBackendTerminalTextCannotSpoofInterruptedFailureClass(t *testing.T) {
	t.Parallel()
	const backendText = "ordinary backend failure: turn interrupted before completion"
	backend := newFakeBackend("fake")
	backend.events = func(string, bool) []engine.Event {
		return []engine.Event{{Type: engine.EventTerminalError, Text: backendText}}
	}
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-text-spoof",
		RequestID:    "request-text-spoof",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "fail",
		},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if got := record.FailureClass; got != engine.FailureClassBackendError {
		t.Fatalf("failure class = %q, want %q", got, engine.FailureClassBackendError)
	}
}

func TestNonFailureTerminalHidesPersistedFailureMetadata(t *testing.T) {
	t.Parallel()
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)
	enableTestAdmission(t, server, newAdmissionFakeLaunchCustodian(t))
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-nonfailure-metadata",
		RequestID:    "request-nonfailure-metadata",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "complete",
		},
	})
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCompleted {
		t.Fatalf("terminal record = %+v, want completed", record.Terminal)
	}
	jobID, err := model.NewJobID(job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	const reason = "retained only for forensics"
	if _, err := server.admissionReady.RecordFailure(context.Background(), jobID, engine.FailureClassBackendError, reason); err != nil {
		t.Fatal(err)
	}
	image, err := server.admissionReady.LoadJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got := image.Safety.Value.FailureClass; got != engine.FailureClassBackendError {
		t.Fatalf("durable failure class = %q, want %q", got, engine.FailureClassBackendError)
	}
	if got := image.Projection.Value.FailureClass; got != "" {
		t.Fatalf("completed projection failure class = %q, want empty", got)
	}
	if got := image.Projection.Value.FailureReason; got != "" {
		t.Fatalf("completed projection failure reason = %q, want empty", got)
	}

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 {
		t.Fatalf("job.status jobs = %+v, want one job", status.Jobs)
	}
	if got := status.Jobs[0].State; got != engine.StateCompleted {
		t.Fatalf("completed job.status state = %q, want %q", got, engine.StateCompleted)
	}
	if got := status.Jobs[0].FailureClass; got != "" {
		t.Fatalf("completed job.status failure class = %q, want empty", got)
	}
	if got := status.Jobs[0].FailureReason; got != "" {
		t.Fatalf("completed job.status failure reason = %q, want empty", got)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if got := result.State; got != engine.StateCompleted {
		t.Fatalf("completed job.result state = %q, want %q", got, engine.StateCompleted)
	}
	if got := result.FailureClass; got != "" {
		t.Fatalf("completed job.result failure class = %q, want empty", got)
	}
	if got := result.FailureReason; got != "" {
		t.Fatalf("completed job.result failure reason = %q, want empty", got)
	}
}

func TestCanceledJobDoesNotExposeFailureMetadata(t *testing.T) {
	t.Parallel()
	holdNaturalExit := make(chan struct{})
	defer close(holdNaturalExit)
	backend := newFakeBackend("fake")
	backend.started = make(chan struct{}, 1)
	server, _, cwd := newUnstartedTestServer(t, backend)
	launcher := newAdmissionFakeLaunchCustodian(t)
	launcher.waitAndVerify = holdNaturalExit
	enableTestAdmission(t, server, launcher)
	job := submitIdentifiedViaScriptedRequest(t, server, protocol.JobSubmitParams{
		WorkspaceKey: "workspace-canceled-metadata",
		RequestID:    "request-canceled-metadata",
		TaskSpec: protocol.TaskSpec{
			Backend: "fake",
			CWD:     cwd,
			Write:   false,
			Prompt:  "cancel",
		},
	})
	waitBackendStarted(t, backend)
	canceled := jobCancelViaHandler(t, server, protocol.JobCancelParams{JobID: job.JobID})
	if canceled.JobID != job.JobID || canceled.State != engine.StateCanceled {
		t.Fatalf("job.cancel = %+v, want canceled job %s", canceled, job.JobID)
	}
	record := waitAdmissionSafetyTerminal(t, server, job.JobID)
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCanceled {
		t.Fatalf("terminal record = %+v, want canceled", record.Terminal)
	}
	if record.FailureClass != "" || record.FailureReason != "" {
		t.Fatalf("canceled durable failure metadata = (%q, %q), want empty", record.FailureReason, record.FailureClass)
	}
	if record.CancellationOrigin != engine.CancellationOriginClientRequest || record.CancellationReason != "client requested cancellation" {
		t.Fatalf("canceled durable cancellation metadata = (%q, %q), want client request", record.CancellationOrigin, record.CancellationReason)
	}

	status := jobStatusViaHandler(t, server, protocol.JobStatusParams{JobID: job.JobID})
	if len(status.Jobs) != 1 || status.Jobs[0].State != engine.StateCanceled {
		t.Fatalf("job.status = %+v, want one canceled job", status)
	}
	if status.Jobs[0].FailureClass != "" || status.Jobs[0].FailureReason != "" {
		t.Fatalf("canceled job.status failure metadata = (%q, %q), want empty", status.Jobs[0].FailureReason, status.Jobs[0].FailureClass)
	}
	if status.Jobs[0].CancellationOrigin != engine.CancellationOriginClientRequest || status.Jobs[0].CancellationReason != "client requested cancellation" {
		t.Fatalf("canceled job.status cancellation metadata = (%q, %q), want client request", status.Jobs[0].CancellationOrigin, status.Jobs[0].CancellationReason)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if result.State != engine.StateCanceled {
		t.Fatalf("job.result state = %q, want %q", result.State, engine.StateCanceled)
	}
	if result.FailureClass != "" || result.FailureReason != "" {
		t.Fatalf("canceled job.result failure metadata = (%q, %q), want empty", result.FailureReason, result.FailureClass)
	}
	if result.CancellationOrigin != engine.CancellationOriginClientRequest || result.CancellationReason != "client requested cancellation" {
		t.Fatalf("canceled job.result cancellation metadata = (%q, %q), want client request", result.CancellationOrigin, result.CancellationReason)
	}
}
