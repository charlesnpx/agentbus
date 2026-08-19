package protocol

import (
	"encoding/json"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const (
	// MethodJobGet retrieves one job, or all jobs when jobId is empty.
	MethodJobGet = "job.get"
)

// The V3 error codes are the only codes protocol.hello, job.submit, job.get,
// and job.cancel may produce. Policy-specific name_conflict and quarantined
// are intentionally absent.
const (
	ErrorUnauthorizedV3       = ErrorUnauthorized
	ErrorVersionMismatchV3    = ErrorVersionMismatch
	ErrorMethodNotFoundV3     = ErrorMethodNotFound
	ErrorBackendUnavailableV3 = ErrorBackendUnavailable
	ErrorInvalidTaskSpecV3    = ErrorInvalidTaskSpec
	ErrorUnknownJobV3         = ErrorUnknownJob
)

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

// TaskSpecV3 describes a job submitted through protocol v3.
type TaskSpecV3 struct {
	Backend      string             `json:"backend"`
	CWD          string             `json:"cwd"`
	Prompt       string             `json:"prompt"`
	Write        bool               `json:"write"`
	Model        *string            `json:"model,omitempty"`
	Effort       *string            `json:"effort,omitempty"`
	TimeoutMS    *int64             `json:"timeoutMs,omitempty"`
	OutputSchema json.RawMessage    `json:"outputSchema,omitempty"`
	Tags         *map[string]string `json:"tags,omitempty"`
}

// JobSubmitParamsV3 is the parameter object for job.submit.
type JobSubmitParamsV3 struct {
	WorkspaceKey string     `json:"workspaceKey,omitempty"`
	RequestID    string     `json:"requestId,omitempty"`
	TaskSpec     TaskSpecV3 `json:"taskSpec"`
}

// JobSubmitResultV3 is the result object for job.submit.
type JobSubmitResultV3 struct {
	JobID        string                    `json:"jobId"`
	State        PublicState               `json:"state"`
	Deduplicated bool                      `json:"deduplicated"`
	Timeout      *engine.TimeoutResolution `json:"timeout,omitempty"`
}

// JobGetParams is the parameter object for job.get. A non-empty JobID returns
// a JobRecordWire directly; an empty JobID returns a JobGetListResult.
type JobGetParams struct {
	JobID string `json:"jobId,omitempty"`
}

// JobGetListResult is the response to job.get when JobID is empty.
type JobGetListResult struct {
	Jobs []JobSummaryWire `json:"jobs"`
}

// JobCancelParamsV3 is the parameter object for job.cancel.
type JobCancelParamsV3 struct {
	JobID string `json:"jobId"`
}

// JobCancelResultV3 is the result object for job.cancel.
type JobCancelResultV3 struct {
	JobID string      `json:"jobId"`
	State PublicState `json:"state"`
}

// HelloResultV3 is the result object for protocol.hello. It deliberately has
// no capabilities map: the v2 map had eight keys, three hardcoded false and
// never set anywhere in the tree.
type HelloResultV3 struct {
	ProtocolVersion int           `json:"protocolVersion"`
	Backends        []string      `json:"backends"`
	BackendMetadata []BackendInfo `json:"backendMetadata,omitempty"`
}

// JobRecordWire is the detailed response to job.get when JobID is provided.
type JobRecordWire struct {
	JobID         string                    `json:"jobId"`
	WorkspaceKey  string                    `json:"workspaceKey"`
	RequestID     string                    `json:"requestId"`
	Backend       string                    `json:"backend"`
	State         PublicState               `json:"state"`
	Tags          map[string]string         `json:"tags,omitempty"`
	CreatedAt     time.Time                 `json:"createdAt"`
	StartedAt     *time.Time                `json:"startedAt,omitempty"`
	FinishedAt    *time.Time                `json:"finishedAt,omitempty"`
	Timeout       *engine.TimeoutResolution `json:"timeout,omitempty"`
	Result        *ResultInfoWire           `json:"result,omitempty"`
	Contract      *ContractResult           `json:"contract,omitempty"`
	Failure       *JobFailureWire           `json:"failure,omitempty"`
	Cleanup       Cleanup                   `json:"cleanup"`
	LogPaths      *LogPathsWire             `json:"logPaths,omitempty"`
	ModelReported string                    `json:"modelReported,omitempty"`
}

// JobSummaryWire is the compact item returned when job.get lists jobs. It
// deliberately excludes request-local identifiers, task details, detailed
// results, failure reasons, logs, and process claims.
type JobSummaryWire struct {
	// JobID lets a client select this job for a subsequent detailed lookup.
	JobID string `json:"jobId"`
	// Backend identifies the executor for a listed job.
	Backend string `json:"backend"`
	// State lets a client distinguish pending work from terminal work.
	State PublicState `json:"state"`
	// Cleanup reports post-run cleanup certainty alongside the public state.
	Cleanup Cleanup `json:"cleanup"`
	// CreatedAt reports when the job was created.
	CreatedAt time.Time `json:"createdAt"`
	// UpdatedAt reports when the job was last updated.
	UpdatedAt time.Time `json:"updatedAt"`
	// ModelReported is the model reported by the backend, when available.
	ModelReported string `json:"modelReported,omitempty"`
	// FailureClass is the compact failure category for a failed job, when set.
	FailureClass FailureClass `json:"failureClass,omitempty"`
	// Contract is the compact output-schema verdict, when evaluation occurred.
	Contract *ContractVerdict `json:"contract,omitempty"`
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

// LogPathsWire identifies captured backend log files.
type LogPathsWire struct {
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}
