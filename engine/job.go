package engine

import (
	"errors"
	"fmt"
	"time"
)

// JobState is the persisted foreground turn/background job state.
type JobState string

const (
	StateQueued                JobState = "queued"
	StateStarting              JobState = "starting"
	StateRunning               JobState = "running"
	StateRetrying              JobState = "retrying"
	StateCompleted             JobState = "completed"
	StateCompletedNoncompliant JobState = "completed_noncompliant"
	StateFailed                JobState = "failed"
	StateTimedOut              JobState = "timed_out"
	StateInterrupted           JobState = "interrupted"
	StateCanceled              JobState = "canceled"
	StateOrphaned              JobState = "orphaned"
	StateReaped                JobState = "reaped"
	StateQuarantined           JobState = "quarantined"
)

// FailureClass is the stable, machine-readable category for a persisted job
// failure. It is intentionally a small closed set so callers can make safe
// retry and operator-routing decisions without parsing FailureReason.
type FailureClass string

// FailureReasonMaxRunes is the maximum length retained by the existing served
// backend-error sanitizer and by durable failure metadata validation.
const FailureReasonMaxRunes = 512

const (
	// FailureClassBackendNotStarted means agentbus could not admit or launch the
	// backend turn, so no backend work was possible.
	FailureClassBackendNotStarted FailureClass = "backend_not_started"
	// FailureClassProviderOverloaded means the provider refused the turn for
	// capacity or overload reasons; no backend work was performed, and the
	// condition is transient and safe to retry.
	FailureClassProviderOverloaded FailureClass = "provider_overloaded"
	// FailureClassBackendError means a launched backend turn returned an error;
	// it may have performed work before doing so.
	FailureClassBackendError FailureClass = "backend_error"
	// FailureClassBackendInterrupted means a backend turn stopped without an
	// interrupt request from agentbus.
	FailureClassBackendInterrupted FailureClass = "backend_interrupted"
	// FailureClassFinalizationError means the backend completed, but agentbus
	// failed while finalizing or publishing its result.
	FailureClassFinalizationError FailureClass = "finalization_error"
	// FailureClassInternalError means no more specific failure category is
	// available.
	FailureClassInternalError FailureClass = "internal_error"
)

// Valid reports whether class is one of the supported persisted failure
// categories. The empty class is allowed only when no failure metadata exists.
func (class FailureClass) Valid() bool {
	switch class {
	case FailureClassBackendNotStarted,
		FailureClassProviderOverloaded,
		FailureClassBackendError,
		FailureClassBackendInterrupted,
		FailureClassFinalizationError,
		FailureClassInternalError:
		return true
	default:
		return false
	}
}

// ProcessRef records enough process identity to detect PID reuse.
type ProcessRef struct {
	PID       int    `json:"pid,omitempty"`
	PGID      int    `json:"pgid,omitempty"`
	StartTime string `json:"startTime,omitempty"`
}

// Lease is a heartbeat lease. Expired is computed at status-read time.
type Lease struct {
	ExpiresAt time.Time `json:"expiresAt"`
	Expired   bool      `json:"expired"`
}

// LogPaths identifies captured backend log files.
type LogPaths struct {
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// ResultInfo describes the authoritative spilled final result.
type ResultInfo struct {
	Text          string `json:"text,omitempty"`
	TextElided    bool   `json:"textElided,omitempty"`
	ResultPath    string `json:"resultPath"`
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
	ModelReported string `json:"modelReported,omitempty"`
}

// JobRecord is the durable job state record stored as JSON.
type JobRecord struct {
	JobID                 string            `json:"jobId"`
	SessionID             string            `json:"sessionId,omitempty"`
	Backend               string            `json:"backend,omitempty"`
	Foreground            bool              `json:"foreground,omitempty"`
	State                 JobState          `json:"state"`
	Tags                  map[string]string `json:"tags,omitempty"`
	CreatedAt             time.Time         `json:"createdAt"`
	StartedAt             time.Time         `json:"startedAt,omitempty"`
	UpdatedAt             time.Time         `json:"updatedAt"`
	HeartbeatAt           time.Time         `json:"heartbeatAt,omitempty"`
	Lease                 Lease             `json:"lease,omitempty"`
	Supervisor            ProcessRef        `json:"supervisor,omitempty"`
	Worker                ProcessRef        `json:"worker,omitempty"`
	BackendSessionID      string            `json:"backendSessionId,omitempty"`
	BackendChildPID       int               `json:"backendChildPid,omitempty"`
	BackendChildStartTime string            `json:"backendChildStartTime,omitempty"`
	ModelReported         string            `json:"modelReported,omitempty"`
	StatePath             string            `json:"statePath,omitempty"`
	LogPaths              LogPaths          `json:"logPaths,omitempty"`
	Result                *ResultInfo       `json:"result,omitempty"`
	LateFinalization      bool              `json:"lateFinalization,omitempty"`
	Policy                *TurnPolicy       `json:"policy,omitempty"`
	ResolvedContract      *ContractSpec     `json:"resolvedContract,omitempty"`
	Contract              *ContractStamp    `json:"contract,omitempty"`
	RetryCount            int               `json:"retryCount,omitempty"`
	Warnings              []string          `json:"warnings,omitempty"`
	QuarantineReason      string            `json:"quarantineReason,omitempty"`
	FailureReason         string            `json:"failureReason,omitempty"`
	FailureClass          FailureClass      `json:"failureClass,omitempty"`
}

// IsTerminal reports whether state is terminal under the public job protocol.
func IsTerminal(state JobState) bool {
	switch state {
	case StateCompleted, StateCompletedNoncompliant, StateFailed, StateTimedOut, StateInterrupted, StateCanceled, StateOrphaned, StateReaped, StateQuarantined:
		return true
	default:
		return false
	}
}

// ExitCodeForState maps a job state to the protocol CLI result exit code.
func ExitCodeForState(state JobState) int {
	switch state {
	case StateCompleted:
		return 0
	case StateCompletedNoncompliant:
		return 3
	case StateFailed:
		return 4
	case StateTimedOut:
		return 5
	case StateInterrupted:
		return 6
	case StateCanceled:
		return 7
	case StateReaped:
		return 8
	case StateQuarantined:
		return 9
	case StateOrphaned:
		// 14 is reserved for terminal orphaned jobs; 2 remains nonterminal,
		// and 10-13 are existing CLI protocol/daemon error codes.
		return 14
	default:
		return 2
	}
}

// LegalTransition reports whether a state change is allowed by protocol v1.
func LegalTransition(from, to JobState, retryCount int) bool {
	if from == to {
		return true
	}
	switch from {
	case StateQueued:
		return to == StateStarting || to == StateInterrupted || to == StateCanceled || to == StateOrphaned
	case StateStarting:
		return to == StateRunning || to == StateFailed || to == StateTimedOut || to == StateInterrupted || to == StateCanceled || to == StateOrphaned
	case StateRunning:
		if to == StateRetrying {
			return retryCount == 0
		}
		return to == StateCompleted || to == StateCompletedNoncompliant || to == StateFailed || to == StateTimedOut || to == StateInterrupted || to == StateCanceled || to == StateOrphaned
	case StateRetrying:
		return to == StateRunning || to == StateCompleted || to == StateCompletedNoncompliant || to == StateFailed || to == StateTimedOut || to == StateInterrupted || to == StateCanceled || to == StateOrphaned
	case StateOrphaned:
		return to == StateCompleted || to == StateCompletedNoncompliant || to == StateFailed || to == StateReaped
	case StateReaped:
		return to == StateCompleted || to == StateCompletedNoncompliant
	default:
		return false
	}
}

// Transition validates and applies a state transition.
func (r *JobRecord) Transition(to JobState, now time.Time) error {
	if r == nil {
		return errors.New("nil job record")
	}
	if !LegalTransition(r.State, to, r.RetryCount) {
		return fmt.Errorf("illegal job state transition %q -> %q", r.State, to)
	}
	if to == StateRetrying && r.State != StateRetrying {
		r.RetryCount++
	}
	r.State = to
	r.UpdatedAt = now
	return nil
}

// StatusRecord returns a copy with computed lease expiry.
func (r JobRecord) StatusRecord(now time.Time) JobRecord {
	if !r.Lease.ExpiresAt.IsZero() {
		r.Lease.Expired = !now.Before(r.Lease.ExpiresAt)
	}
	return r
}
