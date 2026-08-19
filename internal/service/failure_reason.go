//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type terminalFailureOrigin uint8

const (
	terminalFailureInternal terminalFailureOrigin = iota + 1
	terminalFailureBackendNotStarted
	terminalFailureBackendRan
	terminalFailureFinalization
)

type terminalFailure struct {
	class  protocol.FailureClass
	reason string
}

type backendFailureClassPattern struct {
	class     protocol.FailureClass
	fragments []string
}

// backendFailureClassPatterns maps stable provider error fragments to the
// launched-turn classes that need a changed retry input. Matching is
// case-insensitive. Content-policy patterns take precedence over model-
// unavailable patterns when a provider error contains both kinds of fragment:
// the prompt must change before that retry can succeed, so the table order
// below is behavioral and is pinned by a mixed-fragment test.
//
// Fragments are matched against the whole wrapped error text, so each must be
// specific enough not to collide with an unrelated refusal: "content was
// flagged for possible" is used rather than "flagged for possible" so that an
// account-level refusal such as "This account was flagged for possible abuse"
// keeps its backend_error class instead of directing an operator to rewrite a
// prompt that was never the problem.
var backendFailureClassPatterns = []backendFailureClassPattern{
	{
		class: protocol.FailureClassContentPolicy,
		fragments: []string{
			"content was flagged for possible",
			"content policy",
			"trusted access for cyber",
		},
	},
	{
		class: protocol.FailureClassModelUnavailable,
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

// classifyTerminalFailure makes the one stable class decision for all service
// terminal failure paths. agentbusRequestedStop is explicit intent from the
// run lifecycle; an error alone cannot safely establish that distinction.
func classifyTerminalFailure(origin terminalFailureOrigin, err error, agentbusRequestedStop bool) protocol.FailureClass {
	switch origin {
	case terminalFailureBackendNotStarted:
		return protocol.FailureClassBackendUnavailable
	case terminalFailureFinalization:
		return protocol.FailureClassInternal
	case terminalFailureBackendRan:
		if agentbusRequestedStop {
			// An agentbus-requested cancellation or timeout normally terminalizes
			// separately. If it nevertheless reaches a failed path, none of the
			// existing backend classes honestly describes it; internal is safer
			// than claiming an unrequested backend interruption.
			return protocol.FailureClassInternal
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, engine.ErrTurnInterrupted) {
			return protocol.FailureClassInterrupted
		}
		if errors.Is(err, engine.ErrProviderOverloaded) {
			return protocol.FailureClassProviderOverloaded
		}
		return classifyBackendFailureText(err)
	default:
		return protocol.FailureClassInternal
	}
}

// classifyBackendFailureText returns a specific class only for a stable
// provider fragment. Unrecognized launched-turn failures retain backend_error.
func classifyBackendFailureText(err error) protocol.FailureClass {
	if err == nil {
		return protocol.FailureClassBackendError
	}
	text := strings.ToLower(err.Error())
	for _, pattern := range backendFailureClassPatterns {
		for _, fragment := range pattern.fragments {
			if strings.Contains(text, fragment) {
				return pattern.class
			}
		}
	}
	return protocol.FailureClassBackendError
}

func terminalFailureFor(origin terminalFailureOrigin, err error, agentbusRequestedStop bool) terminalFailure {
	return terminalFailure{
		class:  classifyTerminalFailure(origin, err, agentbusRequestedStop),
		reason: terminalReasonFor(err, "unknown failure"),
	}
}

func terminalReasonFor(err error, fallback string) string {
	if err == nil || strings.TrimSpace(err.Error()) == "" {
		return fallback
	}
	return executionFailureReason(err)
}
