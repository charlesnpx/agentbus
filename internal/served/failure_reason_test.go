package served

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestClassifyTerminalFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		origin terminalFailureOrigin
		err    error
		want   engine.FailureClass
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
			if got := classifyTerminalFailure(tt.origin, tt.err); got != tt.want {
				t.Fatalf("classifyTerminalFailure(%v, %v) = %q, want %q", tt.origin, tt.err, got, tt.want)
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
