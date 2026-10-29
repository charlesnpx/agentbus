package protocol

import (
	"encoding/json"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const (
	// Version is the protocol major version served by the simplified daemon.
	Version = 3

	// MethodJobGet retrieves one identified job.
	MethodJobGet = "job.get"
	// MethodJobList retrieves compact job summaries.
	MethodJobList = "job.list"
	// MethodJobTranscript retrieves the captured transcript for one job.
	MethodJobTranscript = "job.transcript"
	// MethodJobSteer submits one in-flight instruction to an active steerable job.
	MethodJobSteer = "job.steer"
)

// The protocol error codes are the only codes protocol.hello, job.submit, job.get,
// job.list, job.transcript, job.cancel, and job.steer may produce.
const (
	ErrorUnauthorized       = "unauthorized"
	ErrorVersionMismatch    = "protocol_version_mismatch"
	ErrorMethodNotFound     = "method_not_found"
	ErrorBackendUnavailable = "backend_unavailable"
	ErrorInvalidTaskSpec    = "invalid_task_spec"
	ErrorQueueFull          = "queue_full"
	ErrorUnknownJob         = "unknown_job"
)

// InputMode determines whether a running task accepts follow-up instructions.
// The empty value is the wire-compatible spelling of fixed; callers should
// omit it when submitting fixed-input work. Unknown values are invalid.
type InputMode string

const (
	// InputModeFixed freezes the submitted prompt for the life of the job.
	InputModeFixed InputMode = "fixed"
	// InputModeSteerable allows bounded follow-up instructions during its active turn.
	InputModeSteerable InputMode = "steerable"
)

// EffectiveInputMode maps an omitted legacy value to the fail-closed default.
func EffectiveInputMode(mode InputMode) InputMode {
	if mode == "" {
		return InputModeFixed
	}
	return mode
}

// PublicState is the stable job state exposed by protocol v3. The daemon
// persists an internal "starting" state to enforce the no-relaunch boundary,
// but projects it on the wire as "running", so it has no constant here.
type PublicState string

const (
	PublicStateQueued    PublicState = "queued"
	PublicStateRunning   PublicState = "running"
	PublicStateCompleted PublicState = "completed"
	PublicStateFailed    PublicState = "failed"
	PublicStateCanceled  PublicState = "canceled"
	PublicStateUnknown   PublicState = "unknown"
)

// IsTerminal reports whether state cannot transition to another public state.
func (state PublicState) IsTerminal() bool {
	switch state {
	case PublicStateCompleted, PublicStateFailed, PublicStateCanceled, PublicStateUnknown:
		return true
	default:
		return false
	}
}

// Valid reports whether state is a public job state.
func (state PublicState) Valid() bool {
	switch state {
	case PublicStateQueued,
		PublicStateRunning,
		PublicStateCompleted,
		PublicStateFailed,
		PublicStateCanceled,
		PublicStateUnknown:
		return true
	default:
		return false
	}
}

// Liveness is the daemon's current verdict for an active job's recorded
// process claim. It deliberately exposes no process identifiers.
type Liveness string

const (
	LivenessAlive   Liveness = "alive"
	LivenessGone    Liveness = "gone"
	LivenessUnknown Liveness = "unknown"
)

// FailureClass is the stable, machine-readable category for a failed job.
type FailureClass string

const (
	FailureClassBackendUnavailable FailureClass = "backend_unavailable"
	FailureClassProviderOverloaded FailureClass = "provider_overloaded"
	FailureClassModelUnavailable   FailureClass = "model_unavailable"
	FailureClassContentPolicy      FailureClass = "content_policy"
	// FailureClassAuthentication records authentication failures. No producer
	// classifies failures this way yet.
	FailureClassAuthentication FailureClass = "authentication"
	FailureClassBackendError   FailureClass = "backend_error"
	FailureClassTimeout        FailureClass = "timeout"
	FailureClassInterrupted    FailureClass = "interrupted"
	FailureClassInternal       FailureClass = "internal"
)

// Valid reports whether class is one of the protocol v3 failure categories.
func (class FailureClass) Valid() bool {
	switch class {
	case FailureClassBackendUnavailable,
		FailureClassProviderOverloaded,
		FailureClassModelUnavailable,
		FailureClassContentPolicy,
		FailureClassAuthentication,
		FailureClassBackendError,
		FailureClassTimeout,
		FailureClassInterrupted,
		FailureClassInternal:
		return true
	default:
		return false
	}
}

// Cleanup describes post-run cleanup certainty. It is an axis orthogonal to
// state: a job may be completed with cleanup=uncertain without losing its
// result.
type Cleanup string

const (
	CleanupClean     Cleanup = "clean"
	CleanupUncertain Cleanup = "uncertain"
)

// Valid reports whether cleanup is one of the protocol v3 cleanup values.
func (cleanup Cleanup) Valid() bool {
	switch cleanup {
	case CleanupClean, CleanupUncertain:
		return true
	default:
		return false
	}
}

// ContractResult records the outcome of output-schema evaluation.
type ContractResult struct {
	SchemaSHA256 string   `json:"schemaSha256"`
	Evaluated    bool     `json:"evaluated"`
	Compliant    bool     `json:"compliant"`
	Attempts     int      `json:"attempts"`
	Violations   []string `json:"violations"`
}

// ContractVerdict is the compact output-schema evaluation result included in
// a listed job.
type ContractVerdict struct {
	Evaluated bool `json:"evaluated"`
	Compliant bool `json:"compliant"`
}

// TaskSpec describes a job submitted through protocol v3.
type TaskSpec struct {
	Backend string `json:"backend"`
	CWD     string `json:"cwd"`
	Prompt  string `json:"prompt"`
	Write   bool   `json:"write"`
	// InputMode is immutable request data and participates in replay hashing.
	// Omission means fixed and, due to omitempty, preserves existing hashes.
	InputMode InputMode `json:"inputMode,omitempty"`
	// ResumeJobID names the prior job whose recorded backend session should be
	// resumed for this new job. It is part of the immutable task specification
	// and therefore participates in identified-replay hashing.
	ResumeJobID string `json:"resumeJobId,omitempty"`
	// RetainSession declares that the submitter intends to continue this
	// conversation after it completes, so the backend session home must not be
	// cleaned up.
	RetainSession bool               `json:"retainSession,omitempty"`
	Model         *string            `json:"model,omitempty"`
	Effort        *string            `json:"effort,omitempty"`
	TimeoutMS     *int64             `json:"timeoutMs,omitempty"`
	OutputSchema  json.RawMessage    `json:"outputSchema,omitempty"`
	Tags          *map[string]string `json:"tags,omitempty"`
}

// JobSteerParams is one caller-identified follow-up instruction. MessageID is
// bounded to 256 bytes and Text to 64 KiB by the daemon.
type JobSteerParams struct {
	JobID     string `json:"jobId"`
	MessageID string `json:"messageId"`
	Text      string `json:"text"`
}

// SteerStatus is the stable result of a steering request.
type SteerStatus string

const (
	SteerStatusAdmitted  SteerStatus = "admitted"
	SteerStatusDelivered SteerStatus = "delivered"
	SteerStatusRejected  SteerStatus = "rejected"
	SteerStatusUnknown   SteerStatus = "unknown"
)

// SteerReason is a stable, machine-readable rejection or uncertainty cause.
type SteerReason string

const (
	SteerReasonNotSteerable           SteerReason = "not_steerable"
	SteerReasonNotRunning             SteerReason = "not_running"
	SteerReasonDuplicateConflict      SteerReason = "duplicate_conflict"
	SteerReasonQueueFull              SteerReason = "queue_full"
	SteerReasonSchemaCorrectionActive SteerReason = "schema_correction_active"
	SteerReasonBackendUnsupported     SteerReason = "backend_unsupported"
	SteerReasonDeliveryUnconfirmed    SteerReason = "delivery_unconfirmed"
)

// JobSteerResult is the immediate receipt from job.steer. An admitted receipt
// is updated asynchronously in job.get once the adapter reports an outcome.
type JobSteerResult struct {
	JobID        string      `json:"jobId"`
	MessageID    string      `json:"messageId"`
	Status       SteerStatus `json:"status"`
	Mechanism    string      `json:"mechanism"`
	Reason       SteerReason `json:"reason"`
	Deduplicated bool        `json:"deduplicated"`
}

// SteerRecord is the durable audit metadata for one message. Text itself is
// never stored; TextSHA256 and TextBytes allow identity checks on retries.
type SteerRecord struct {
	MessageID  string      `json:"messageId"`
	TextSHA256 string      `json:"textSha256"`
	TextBytes  int         `json:"textBytes"`
	Status     SteerStatus `json:"status"`
	Mechanism  string      `json:"mechanism,omitempty"`
	Reason     SteerReason `json:"reason,omitempty"`
	CreatedAt  time.Time   `json:"createdAt"`
	UpdatedAt  time.Time   `json:"updatedAt"`
}

// SteerSummaryWire is the text-free steering projection included in job.get.
type SteerSummaryWire struct {
	MessageID string      `json:"messageId"`
	Status    SteerStatus `json:"status"`
	Mechanism string      `json:"mechanism"`
	Time      time.Time   `json:"time"`
}

// JobSteerRecordUpdate changes the durable outcome for an admitted message.
type JobSteerRecordUpdate struct {
	JobID     string
	MessageID string
	Status    SteerStatus
	Mechanism string
	Reason    SteerReason
}

// JobSubmitParams is the parameter object for job.submit.
type JobSubmitParams struct {
	WorkspaceKey string   `json:"workspaceKey,omitempty"`
	RequestID    string   `json:"requestId,omitempty"`
	TaskSpec     TaskSpec `json:"taskSpec"`
}

// JobSubmitResult is the result object for job.submit.
type JobSubmitResult struct {
	JobID        string                    `json:"jobId"`
	State        PublicState               `json:"state"`
	Deduplicated bool                      `json:"deduplicated"`
	Timeout      *engine.TimeoutResolution `json:"timeout,omitempty"`
}

// JobGetParams is the parameter object for job.get.
type JobGetParams struct {
	JobID string `json:"jobId,omitempty"`
}

// JobListParams is the parameter object for job.list. Every field is optional;
// omitted or empty filters do not restrict results, while limit and cursor
// select an optional page.
type JobListParams struct {
	WorkspaceKey string            `json:"workspaceKey,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	States       []PublicState     `json:"states,omitempty"`
	Limit        int               `json:"limit,omitempty"`
	Cursor       string            `json:"cursor,omitempty"`
}

// JobListResult is the response to job.list. Truncated and NextCursor are set
// only when another page exists.
type JobListResult struct {
	Jobs       []JobSummaryWire `json:"jobs"`
	Truncated  bool             `json:"truncated,omitempty"`
	NextCursor string           `json:"nextCursor,omitempty"`
}

// JobTranscriptParams selects a stateless view of one job's captured
// transcript. JobID is required. Every selector is optional and selectors
// combine with AND.
type JobTranscriptParams struct {
	JobID        string     `json:"jobId"`
	Kinds        []string   `json:"kinds,omitempty"`
	Since        *time.Time `json:"since,omitempty"`
	SinceOrdinal *int       `json:"sinceOrdinal,omitempty"`
	Last         *int       `json:"last,omitempty"`
	Limit        *int       `json:"limit,omitempty"`
}

// TranscriptItem is one normalized, backend-neutral event captured for a
// job. Kind is one of message, reasoning, tool, toolResult, fileChange,
// warning, or error.
type TranscriptItem struct {
	Ordinal   int       `json:"ordinal"`
	At        time.Time `json:"at"`
	Kind      string    `json:"kind"`
	Name      string    `json:"name,omitempty"`
	Text      string    `json:"text,omitempty"`
	Truncated bool      `json:"truncated"`
}

// JobTranscriptResult is the captured transcript digest and selected items
// for one job. Counts, itemCount, firstAt, lastAt, and gap describe the
// captured sidecar rather than only the selected Items.
type JobTranscriptResult struct {
	State     PublicState      `json:"state"`
	Liveness  Liveness         `json:"liveness,omitempty"`
	ItemCount int              `json:"itemCount"`
	Counts    map[string]int   `json:"counts"`
	FirstAt   *time.Time       `json:"firstAt,omitempty"`
	LastAt    *time.Time       `json:"lastAt,omitempty"`
	Items     []TranscriptItem `json:"items"`
	Gap       bool             `json:"gap"`
}

// JobCancelParams is the parameter object for job.cancel.
type JobCancelParams struct {
	JobID string `json:"jobId"`
}

// JobCancelResult is the result object for job.cancel.
type JobCancelResult struct {
	JobID string      `json:"jobId"`
	State PublicState `json:"state"`
}

// HelloResult is the result object for protocol.hello.
type HelloResult struct {
	ProtocolVersion int           `json:"protocolVersion"`
	BackendMetadata []BackendInfo `json:"backends"`
}

// JobRecordWire is the detailed response to job.get when JobID is provided.
type JobRecordWire struct {
	JobID         string                    `json:"jobId"`
	WorkspaceKey  string                    `json:"workspaceKey"`
	RequestID     string                    `json:"requestId"`
	Backend       string                    `json:"backend"`
	InputMode     InputMode                 `json:"inputMode"`
	State         PublicState               `json:"state"`
	Tags          map[string]string         `json:"tags,omitempty"`
	CreatedAt     time.Time                 `json:"createdAt"`
	QueuedAt      time.Time                 `json:"queuedAt"`
	StartedAt     *time.Time                `json:"startedAt,omitempty"`
	FinishedAt    *time.Time                `json:"finishedAt,omitempty"`
	Timeout       *engine.TimeoutResolution `json:"timeout,omitempty"`
	Result        *ResultInfoWire           `json:"result,omitempty"`
	Contract      *ContractResult           `json:"contract,omitempty"`
	Failure       *JobFailureWire           `json:"failure,omitempty"`
	Cleanup       Cleanup                   `json:"cleanup"`
	LogPaths      *LogPathsWire             `json:"logPaths,omitempty"`
	ModelReported string                    `json:"modelReported,omitempty"`
	Execution     *ExecutionPosture         `json:"execution,omitempty"`
	Steers        []SteerSummaryWire        `json:"steers"`
}

// ExecutionPosture records the requested source access and the backend
// enforcement and scratch/cache paths selected when a job starts. It is
// descriptive; it does not negotiate or expand backend capabilities.
type ExecutionPosture struct {
	Intent         string            `json:"intent"`
	SourceWritable bool              `json:"sourceWritable"`
	Scratch        string            `json:"scratch,omitempty"`
	Caches         map[string]string `json:"caches,omitempty"`
	Network        string            `json:"network"`
	GitMetadata    string            `json:"gitMetadata"`
	Enforcement    string            `json:"enforcement"`
}

// JobSummaryWire is the compact item returned when job.list lists jobs. It
// deliberately excludes request-local identifiers, task details, detailed
// results, failure reasons, logs, and process claims.
type JobSummaryWire struct {
	// JobID lets a client select this job for a subsequent detailed lookup.
	JobID string `json:"jobId"`
	// Backend identifies the executor for a listed job.
	Backend string `json:"backend"`
	// State lets a client distinguish pending work from terminal work.
	State PublicState `json:"state"`
	// Tags identifies the submitted labels that can be used to filter a list.
	Tags map[string]string `json:"tags,omitempty"`
	// Cleanup reports post-run cleanup certainty alongside the public state.
	Cleanup Cleanup `json:"cleanup"`
	// CreatedAt reports when the job was created.
	CreatedAt time.Time `json:"createdAt"`
	// QueuedAt reports when the job entered the daemon queue.
	QueuedAt time.Time `json:"queuedAt"`
	// UpdatedAt reports when the job was last updated.
	UpdatedAt time.Time `json:"updatedAt"`
	// ModelReported is the model reported by the backend, when available.
	ModelReported string `json:"modelReported,omitempty"`
	// FailureClass is the compact failure category for a failed job, when set.
	FailureClass FailureClass `json:"failureClass,omitempty"`
	// Contract is the compact output-schema verdict, when evaluation occurred.
	Contract *ContractVerdict `json:"contract,omitempty"`
	// ItemCount is present only while this daemon has an active execution.
	ItemCount *int `json:"itemCount,omitempty"`
	// LastItemAt is present after an active execution has assembled an item.
	LastItemAt *time.Time `json:"lastItemAt,omitempty"`
	// LastActivityAt is present after an active execution observes backend activity.
	LastActivityAt *time.Time `json:"lastActivityAt,omitempty"`
	// Liveness is present only while this daemon has an active execution.
	// It is a verdict, never a process identifier or process claim.
	Liveness Liveness `json:"liveness,omitempty"`
}

// JobFailureWire records the public failure category and reason for a job.
type JobFailureWire struct {
	Class  FailureClass `json:"class"`
	Reason string       `json:"reason"`
}

// ResultInfoWire describes the authoritative spilled final result.
type ResultInfoWire struct {
	Text       string `json:"text,omitempty"`
	ResultPath string `json:"resultPath"`
	SHA256     string `json:"sha256"`
	Bytes      int64  `json:"bytes"`
}

// LogPathsWire identifies captured backend log files and whether each captured
// file ended at the capped-log truncation marker.
type LogPathsWire struct {
	Stdout          string `json:"stdout,omitempty"`
	StdoutTruncated *bool  `json:"stdoutTruncated,omitempty"`
	Stderr          string `json:"stderr,omitempty"`
	StderrTruncated *bool  `json:"stderrTruncated,omitempty"`
}
