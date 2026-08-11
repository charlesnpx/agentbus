package served

import (
	"context"
	"errors"
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
	tests := []struct {
		name                  string
		origin                terminalFailureOrigin
		err                   error
		agentbusRequestedStop bool
		want                  engine.FailureClass
	}{
		{
			name:   "backend not started",
			origin: terminalFailureBackendNotStarted,
			err:    errors.New("runner was not admitted"),
			want:   engine.FailureClassBackendNotStarted,
		},
		{
			name:   "backend error",
			origin: terminalFailureBackendRan,
			err:    errors.New("backend exited 1"),
			want:   engine.FailureClassBackendError,
		},
		{
			name:   "unrequested codex interruption",
			origin: terminalFailureBackendRan,
			err:    errors.New("codex app-server turn interrupted before completion"),
			want:   engine.FailureClassBackendInterrupted,
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
		})
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
	if got := status.Jobs[0].FailureClass; got != "" {
		t.Fatalf("completed job.status failure class = %q, want empty", got)
	}
	if got := status.Jobs[0].FailureReason; got != "" {
		t.Fatalf("completed job.status failure reason = %q, want empty", got)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: job.JobID})
	if got := result.FailureClass; got != "" {
		t.Fatalf("completed job.result failure class = %q, want empty", got)
	}
	if got := result.FailureReason; got != "" {
		t.Fatalf("completed job.result failure reason = %q, want empty", got)
	}
}
