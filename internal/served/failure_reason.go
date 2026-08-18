package served

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type terminalFailureOrigin uint8

const (
	terminalFailureInternal terminalFailureOrigin = iota + 1
	terminalFailureBackendNotStarted
	terminalFailureBackendRan
	terminalFailureFinalization
)

type terminalFailure struct {
	class  engine.FailureClass
	reason string
}

type terminalCancellation struct {
	origin engine.CancellationOrigin
	reason string
}

type backendFailureClassPattern struct {
	class     engine.FailureClass
	fragments []string
}

// backendFailureClassPatterns maps stable provider error fragments to the
// launched-turn classes that need a changed retry input. Matching is
// case-insensitive. Content-policy patterns take precedence over model-
// unavailable patterns when a provider error contains both kinds of fragment:
// the prompt must change before that retry can succeed.
var backendFailureClassPatterns = []backendFailureClassPattern{
	{
		class: engine.FailureClassContentPolicy,
		fragments: []string{
			"flagged for possible",
			"content policy",
			"trusted access for cyber",
		},
	},
	{
		class: engine.FailureClassModelUnavailable,
		fragments: []string{
			"model is not supported",
			"unknown model",
			"model_not_found",
		},
	},
}

// classifiedTerminalError preserves where an error arose while retaining its
// original identity for existing errors.Is and errors.As callers.
type classifiedTerminalError struct {
	origin terminalFailureOrigin
	err    error
}

func (e *classifiedTerminalError) Error() string {
	if e == nil || e.err == nil {
		return "terminal failure"
	}
	return e.err.Error()
}

func (e *classifiedTerminalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func classifyFailureError(origin terminalFailureOrigin, err error) error {
	if err == nil {
		err = errors.New("unknown failure")
	}
	return &classifiedTerminalError{origin: origin, err: err}
}

func terminalFailureOriginFor(err error, fallback terminalFailureOrigin) terminalFailureOrigin {
	var classified *classifiedTerminalError
	if errors.As(err, &classified) && classified != nil && classified.origin != 0 {
		return classified.origin
	}
	return fallback
}

// classifyTerminalFailure makes the one stable class decision for all served
// terminal failure paths. agentbusRequestedStop is explicit intent from the
// run lifecycle; an error alone cannot safely establish that distinction.
func classifyTerminalFailure(origin terminalFailureOrigin, err error, agentbusRequestedStop bool) engine.FailureClass {
	switch origin {
	case terminalFailureBackendNotStarted:
		return engine.FailureClassBackendNotStarted
	case terminalFailureFinalization:
		return engine.FailureClassFinalizationError
	case terminalFailureBackendRan:
		if agentbusRequestedStop {
			// An agentbus-requested cancellation or timeout normally terminalizes
			// separately. If it nevertheless reaches a failed path, none of the
			// existing backend classes honestly describes it; internal_error is
			// safer than claiming an unrequested backend interruption.
			return engine.FailureClassInternalError
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrTurnInterrupted) {
			return engine.FailureClassBackendInterrupted
		}
		if errors.Is(err, engine.ErrTransportFrameTooLarge) {
			return engine.FailureClassTransportFrameTooLarge
		}
		if errors.Is(err, engine.ErrProviderOverloaded) {
			return engine.FailureClassProviderOverloaded
		}
		return classifyBackendFailureText(err)
	default:
		return engine.FailureClassInternalError
	}
}

// classifyBackendFailureText returns a specific class only for a stable
// provider fragment. Unrecognized launched-turn failures retain backend_error.
func classifyBackendFailureText(err error) engine.FailureClass {
	if err == nil {
		return engine.FailureClassBackendError
	}
	text := strings.ToLower(err.Error())
	for _, pattern := range backendFailureClassPatterns {
		for _, fragment := range pattern.fragments {
			if strings.Contains(text, fragment) {
				return pattern.class
			}
		}
	}
	return engine.FailureClassBackendError
}

func terminalFailureFor(origin terminalFailureOrigin, err error, agentbusRequestedStop bool) terminalFailure {
	return terminalFailure{
		class:  classifyTerminalFailure(origin, err, agentbusRequestedStop),
		reason: terminalReasonFor(err, "unknown failure"),
	}
}

func terminalCancellationFor(origin engine.CancellationOrigin, reason string) terminalCancellation {
	if !origin.Valid() {
		origin = engine.CancellationOriginUnattributable
	}
	return terminalCancellation{
		origin: origin,
		reason: terminalReasonFor(errors.New(reason), "canceled without an attributable origin"),
	}
}

func terminalReasonFor(err error, fallback string) string {
	reason := fallback
	if err != nil && strings.TrimSpace(err.Error()) != "" {
		reason = err.Error()
	}
	reason = sanitizeAdmissionProbeReason(reason)
	if strings.TrimSpace(reason) == "" {
		return fallback
	}
	return reason
}

func terminalFailureStopWasRequestedByAgentbus(run jobRun, err error) bool {
	if run.active != nil && run.active.requestedTerminal() != "" {
		// Both client cancellation and graceful shutdown record a requested
		// terminal state on the active job before canceling its context.
		return true
	}
	// Daemon-imposed timeouts normally become timed_out before this classifier
	// runs. Preserve that intent if a wrapped timeout instead reaches a failure
	// path, so it cannot be advertised as an unrequested interruption.
	return run.timeout > 0 && errors.Is(err, context.DeadlineExceeded)
}

func (s *Server) recordFailureMetadata(run jobRun, failure terminalFailure) error {
	if !failure.class.Valid() || strings.TrimSpace(failure.reason) == "" {
		return fmt.Errorf("invalid failure metadata for job %s", run.jobID)
	}
	if !run.admissionControlled {
		// Serve starts strict admission before it accepts requests, and the only
		// production jobRun constructor marks those runs admission-controlled.
		// A non-admission write would advertise a path the daemon does not have.
		return fmt.Errorf("failure metadata for non-admission job %s is unreachable", run.jobID)
	}
	jobID, err := model.NewJobID(run.jobID)
	if err != nil {
		return err
	}
	s.admissionStateMu.RLock()
	ready := s.admissionReady
	available := s.admissionInstance != nil && ready != nil
	s.admissionStateMu.RUnlock()
	if !available {
		return authority.ErrNotReady
	}
	_, err = ready.RecordFailure(context.Background(), jobID, failure.class, failure.reason)
	return err
}
