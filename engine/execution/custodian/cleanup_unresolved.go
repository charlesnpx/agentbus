package custodian

import (
	"context"
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

// CleanupUnresolvedError is the typed, job-local signal that physical absence
// could not be established after the durable launch identity was trusted.
type CleanupUnresolvedError struct {
	Reason   containment.UnprovableReason
	Decision model.ContainmentDecision
	Cause    error
}

func (e *CleanupUnresolvedError) Error() string {
	if e == nil {
		return "cleanup unresolved"
	}
	if e.Cause != nil {
		return fmt.Sprintf("cleanup unresolved: reason=%s decision=%s: %v", e.Reason, e.Decision, e.Cause)
	}
	return fmt.Sprintf("cleanup unresolved: reason=%s decision=%s", e.Reason, e.Decision)
}

func (e *CleanupUnresolvedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func IsCleanupUnresolved(err error) bool {
	return CleanupUnresolved(err) != nil
}

func CleanupUnresolved(err error) *CleanupUnresolvedError {
	var unresolved *CleanupUnresolvedError
	if errors.As(err, &unresolved) {
		return unresolved
	}
	return nil
}

func cleanupUnresolvedFromPhysicalOutcome(outcome PhysicalOutcome) error {
	if !outcome.Unprovable() {
		return nil
	}
	switch outcome.Reason {
	case containment.ReasonContextDone:
		if outcome.Err != nil {
			return outcome.Err
		}
		return context.Canceled
	case containment.ReasonObservationFailed,
		containment.ReasonAuthorizationUnprovable,
		containment.ReasonUnauthorizedWaitExpired,
		containment.ReasonSignalUnprovable,
		containment.ReasonProbeUnprovable,
		containment.ReasonProbeContradictedObserver,
		containment.ReasonAbsenceDeadlineExceeded:
		return &CleanupUnresolvedError{
			Reason:   outcome.Reason,
			Decision: outcome.Decision,
			Cause:    outcome.Err,
		}
	default:
		return nil
	}
}
