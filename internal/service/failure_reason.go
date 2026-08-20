//go:build darwin || linux

package service

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"strings"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

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

// classifyBackendFailureText recognizes failures to create the backend process
// before examining stable provider fragments. Unrecognized launched-turn
// failures retain backend_error.
func classifyBackendFailureText(err error) protocol.FailureClass {
	if err == nil {
		return protocol.FailureClassBackendError
	}
	var execErr *exec.Error
	if errors.As(err, &execErr) && errors.Is(execErr.Err, exec.ErrNotFound) {
		return protocol.FailureClassBackendUnavailable
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) && pathErr.Op == "fork/exec" &&
		(errors.Is(pathErr.Err, fs.ErrNotExist) || errors.Is(pathErr.Err, fs.ErrPermission)) {
		return protocol.FailureClassBackendUnavailable
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
