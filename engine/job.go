package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
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

// CancellationOrigin is the stable, machine-readable category for a persisted
// job cancellation. It is intentionally a small closed set so callers can
// route operator investigation without parsing CancellationReason.
type CancellationOrigin string

const (
	// FailureClassBackendNotStarted means agentbus could not admit or launch the
	// backend turn, so no backend work was possible.
	FailureClassBackendNotStarted FailureClass = "backend_not_started"
	// FailureClassProviderOverloaded means the provider reported a capacity or
	// overload refusal. It does not establish whether backend work occurred and
	// therefore does not by itself license an automatic retry.
	FailureClassProviderOverloaded FailureClass = "provider_overloaded"
	// FailureClassBackendError means a launched backend turn returned an error;
	// it may have performed work before doing so.
	FailureClassBackendError FailureClass = "backend_error"
	// FailureClassBackendInterrupted means a backend turn stopped without an
	// interrupt request from agentbus.
	FailureClassBackendInterrupted FailureClass = "backend_interrupted"
	// FailureClassTransportFrameTooLarge means agentbus observed a backend
	// transport frame exceed the configured limit.
	FailureClassTransportFrameTooLarge FailureClass = "transport_frame_too_large"
	// FailureClassFinalizationError means the backend completed, but agentbus
	// failed while finalizing or publishing its result.
	FailureClassFinalizationError FailureClass = "finalization_error"
	// FailureClassInternalError means no more specific failure category is
	// available.
	FailureClassInternalError FailureClass = "internal_error"
)

const (
	// CancellationOriginClientRequest means agentbus observed a client request
	// to cancel the job. It does not establish whether backend work occurred.
	CancellationOriginClientRequest CancellationOrigin = "client_request"
	// CancellationOriginDaemonShutdown means agentbus canceled the job while
	// performing graceful daemon shutdown. It does not establish whether backend
	// work occurred.
	CancellationOriginDaemonShutdown CancellationOrigin = "daemon_shutdown"
	// CancellationOriginUnattributable means agentbus recorded a canceled job
	// but did not observe a more specific cause. It does not establish whether
	// backend work occurred.
	CancellationOriginUnattributable CancellationOrigin = "unattributable"
)

// Valid reports whether class is one of the supported persisted failure
// categories. The empty class is allowed only when no failure metadata exists.
func (class FailureClass) Valid() bool {
	switch class {
	case FailureClassBackendNotStarted,
		FailureClassProviderOverloaded,
		FailureClassBackendError,
		FailureClassBackendInterrupted,
		FailureClassTransportFrameTooLarge,
		FailureClassFinalizationError,
		FailureClassInternalError:
		return true
	default:
		return false
	}
}

// TransportFrameDrops records bounded metadata about backend JSONL frames
// discarded because they exceeded the configured transport limit. The prefix
// is a redacted frame-type summary, never backend payload text.
type TransportFrameDrops struct {
	Count          uint64 `json:"count,omitempty"`
	Bytes          uint64 `json:"bytes,omitempty"`
	RedactedPrefix string `json:"redactedPrefix,omitempty"`
}

// Empty reports whether no backend transport frame was discarded.
func (drops TransportFrameDrops) Empty() bool {
	return drops.Count == 0
}

// Merge adds independently observed dropped-frame counters. It saturates on
// overflow because transport diagnostics must remain bounded and non-fatal.
func (drops *TransportFrameDrops) Merge(other TransportFrameDrops) {
	if drops == nil || other.Empty() {
		return
	}
	if math.MaxUint64-drops.Count < other.Count {
		drops.Count = math.MaxUint64
	} else {
		drops.Count += other.Count
	}
	if math.MaxUint64-drops.Bytes < other.Bytes {
		drops.Bytes = math.MaxUint64
	} else {
		drops.Bytes += other.Bytes
	}
	if other.RedactedPrefix != "" {
		drops.RedactedPrefix = other.RedactedPrefix
	}
}

// TransportFrameDropsMetadataKey identifies dropped-frame metadata carried on
// an in-process adapter warning event.
const TransportFrameDropsMetadataKey = "agentbusTransportFrameDrops"

// EventMetadata returns the bounded transport-drop metadata carried with an
// adapter warning event.
func (drops TransportFrameDrops) EventMetadata() map[string]any {
	if drops.Empty() {
		return nil
	}
	return map[string]any{
		TransportFrameDropsMetadataKey: map[string]any{
			"count":          drops.Count,
			"bytes":          drops.Bytes,
			"redactedPrefix": drops.RedactedPrefix,
		},
	}
}

// TransportFrameDropsFromMetadata reads transport-drop metadata from an
// in-process adapter warning event. Invalid metadata is ignored.
func TransportFrameDropsFromMetadata(metadata map[string]any) (TransportFrameDrops, bool) {
	if len(metadata) == 0 {
		return TransportFrameDrops{}, false
	}
	raw, ok := metadata[TransportFrameDropsMetadataKey].(map[string]any)
	if !ok {
		return TransportFrameDrops{}, false
	}
	count, ok := metadataUint64(raw["count"])
	if !ok || count == 0 {
		return TransportFrameDrops{}, false
	}
	bytes, ok := metadataUint64(raw["bytes"])
	if !ok || bytes == 0 {
		return TransportFrameDrops{}, false
	}
	prefix, _ := raw["redactedPrefix"].(string)
	return TransportFrameDrops{Count: count, Bytes: bytes, RedactedPrefix: prefix}, true
}

func metadataUint64(value any) (uint64, bool) {
	switch value := value.(type) {
	case uint64:
		return value, true
	case uint:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case int:
		if value >= 0 {
			return uint64(value), true
		}
	case int64:
		if value >= 0 {
			return uint64(value), true
		}
	case float64:
		if value >= 0 && value <= math.MaxUint64 && value == math.Trunc(value) {
			return uint64(value), true
		}
	case json.Number:
		parsed, err := value.Int64()
		if err == nil && parsed >= 0 {
			return uint64(parsed), true
		}
	}
	return 0, false
}

// Valid reports whether origin is one of the supported persisted cancellation
// categories. The empty origin is allowed only when no cancellation metadata
// exists.
func (origin CancellationOrigin) Valid() bool {
	switch origin {
	case CancellationOriginClientRequest,
		CancellationOriginDaemonShutdown,
		CancellationOriginUnattributable:
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

const (
	// TimeoutSourceClient means the effective timeout came from taskSpec.timeoutMs.
	TimeoutSourceClient = "client"
	// TimeoutSourceDaemonDefault means the effective timeout came from the daemon default.
	TimeoutSourceDaemonDefault = "daemon_default"
)

// TimeoutResolution records the timeout applied to a job in milliseconds.
// Requested is present only when the client supplied taskSpec.timeoutMs;
// Effective is the duration used for each runAttempt invocation; Source
// identifies whether that duration came from the client or the daemon default.
type TimeoutResolution struct {
	Requested *int64 `json:"requested,omitempty"`
	Effective int64  `json:"effective"`
	Source    string `json:"source"`
}

// Valid reports whether resolution is either absent in a legacy record or has
// a complete, internally consistent source shape.
func (resolution TimeoutResolution) Valid() bool {
	if resolution.Effective < 0 {
		return false
	}
	if resolution.Source == "" {
		return resolution.Requested == nil && resolution.Effective == 0
	}
	switch resolution.Source {
	case TimeoutSourceClient:
		return resolution.Requested != nil
	case TimeoutSourceDaemonDefault:
		return resolution.Requested == nil
	default:
		return false
	}
}

// CloneTimeoutResolution returns an independent copy suitable for a durable
// record or wire response.
func CloneTimeoutResolution(resolution *TimeoutResolution) *TimeoutResolution {
	if resolution == nil {
		return nil
	}
	copy := *resolution
	if resolution.Requested != nil {
		requested := *resolution.Requested
		copy.Requested = &requested
	}
	return &copy
}

// JobRecord is the durable job state record stored as JSON.
type JobRecord struct {
	JobID                 string               `json:"jobId"`
	SessionID             string               `json:"sessionId,omitempty"`
	Backend               string               `json:"backend,omitempty"`
	Timeout               *TimeoutResolution   `json:"timeout,omitempty"`
	Foreground            bool                 `json:"foreground,omitempty"`
	State                 JobState             `json:"state"`
	Tags                  map[string]string    `json:"tags,omitempty"`
	CreatedAt             time.Time            `json:"createdAt"`
	StartedAt             time.Time            `json:"startedAt,omitempty"`
	UpdatedAt             time.Time            `json:"updatedAt"`
	HeartbeatAt           time.Time            `json:"heartbeatAt,omitempty"`
	Lease                 Lease                `json:"lease,omitempty"`
	Supervisor            ProcessRef           `json:"supervisor,omitempty"`
	Worker                ProcessRef           `json:"worker,omitempty"`
	BackendSessionID      string               `json:"backendSessionId,omitempty"`
	BackendChildPID       int                  `json:"backendChildPid,omitempty"`
	BackendChildStartTime string               `json:"backendChildStartTime,omitempty"`
	ModelReported         string               `json:"modelReported,omitempty"`
	StatePath             string               `json:"statePath,omitempty"`
	LogPaths              *LogPaths            `json:"logPaths,omitempty"`
	Result                *ResultInfo          `json:"result,omitempty"`
	LateFinalization      bool                 `json:"lateFinalization,omitempty"`
	Policy                *TurnPolicy          `json:"policy,omitempty"`
	ResolvedContract      *ContractSpec        `json:"resolvedContract,omitempty"`
	Contract              *ContractStamp       `json:"contract,omitempty"`
	RetryCount            int                  `json:"retryCount,omitempty"`
	Warnings              []string             `json:"warnings,omitempty"`
	QuarantineReason      string               `json:"quarantineReason,omitempty"`
	FailureReason         string               `json:"failureReason,omitempty"`
	FailureClass          FailureClass         `json:"failureClass,omitempty"`
	TransportFrameDrops   *TransportFrameDrops `json:"transportFrameDrops,omitempty"`
	// ObservedWorkspaceWriteItemCount is the number of workspace-write items
	// reported by the backend while this job ran, not a verified filesystem
	// state. Zero means no write items
	// were observed; it does not guarantee the workspace is clean because the
	// stream may have been truncated or a write may have happened by a route
	// the backend did not report.
	ObservedWorkspaceWriteItemCount uint64             `json:"observedWorkspaceWriteItemCount"`
	CancellationReason              string             `json:"cancellationReason,omitempty"`
	CancellationOrigin              CancellationOrigin `json:"cancellationOrigin,omitempty"`
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
