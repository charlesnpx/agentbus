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
	Text       string `json:"text,omitempty"`
	ResultPath string `json:"resultPath"`
	SHA256     string `json:"sha256"`
	Bytes      int64  `json:"bytes"`
}

// JobRecord is the durable job state record stored as JSON.
type JobRecord struct {
	JobID            string            `json:"jobId"`
	SessionID        string            `json:"sessionId,omitempty"`
	Backend          string            `json:"backend,omitempty"`
	Foreground       bool              `json:"foreground,omitempty"`
	State            JobState          `json:"state"`
	Tags             map[string]string `json:"tags,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	StartedAt        time.Time         `json:"startedAt,omitempty"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	HeartbeatAt      time.Time         `json:"heartbeatAt,omitempty"`
	Lease            Lease             `json:"lease,omitempty"`
	Supervisor       ProcessRef        `json:"supervisor,omitempty"`
	Worker           ProcessRef        `json:"worker,omitempty"`
	BackendSessionID string            `json:"backendSessionId,omitempty"`
	BackendChildPID  int               `json:"backendChildPid,omitempty"`
	StatePath        string            `json:"statePath,omitempty"`
	LogPaths         LogPaths          `json:"logPaths,omitempty"`
	Result           *ResultInfo       `json:"result,omitempty"`
	Policy           *TurnPolicy       `json:"policy,omitempty"`
	ResolvedContract *ContractSpec     `json:"resolvedContract,omitempty"`
	Contract         *ContractStamp    `json:"contract,omitempty"`
	RetryCount       int               `json:"retryCount,omitempty"`
	QuarantineReason string            `json:"quarantineReason,omitempty"`
}

// IsTerminal reports whether state is terminal under protocol v1.
func IsTerminal(state JobState) bool {
	switch state {
	case StateCompleted, StateCompletedNoncompliant, StateFailed, StateTimedOut, StateInterrupted, StateCanceled, StateReaped, StateQuarantined:
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
		return to == StateReaped
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
