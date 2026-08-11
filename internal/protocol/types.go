package protocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const (
	// Version is the strict-only protocol major version frozen by ADR-12.
	Version       = 2
	SocketName    = "agentbus.sock"
	TokenFileName = "token"

	MethodHello          = "protocol.hello"
	MethodJobSubmit      = "job.submit"
	MethodJobStatus      = "job.status"
	MethodJobResult      = "job.result"
	MethodJobCancel      = "job.cancel"
	MethodPolicyValidate = "policy.validate"
	MethodPolicyRegister = "policy.register"

	CapabilityAdmissionStrictContainment = "admission.strictContainment"

	ErrorUnauthorized       = "unauthorized"
	ErrorNameConflict       = "name_conflict"
	ErrorVersionMismatch    = "protocol_version_mismatch"
	ErrorMethodNotFound     = "method_not_found"
	ErrorCapabilityMissing  = "capability_missing"
	ErrorBackendUnavailable = "backend_unavailable"
	ErrorTimeout            = "timeout"
	ErrorInterrupted        = "interrupted"
	ErrorQuarantined        = "quarantined"
	ErrorResultTooLarge     = "result_too_large"
	ErrorInvalidTaskSpec    = "invalid_task_spec"
	ErrorUnknownJob         = "unknown_job"
)

const (
	// AdmissionRejectMissingIdentity means strict admission did not receive a workspaceKey and requestId identity.
	AdmissionRejectMissingIdentity string = "missing_identity"
	// AdmissionRejectReplayConflict means the request key is already bound or tombstoned to a different task identity.
	AdmissionRejectReplayConflict string = "replay_conflict"
	// AdmissionRejectRequestExpired means the request key matches an expired tombstone.
	AdmissionRejectRequestExpired string = "request_expired"
	// AdmissionRejectRequestFingerprintUnsupported means the recorded fingerprint algorithm or version cannot be compared.
	AdmissionRejectRequestFingerprintUnsupported string = "request_fingerprint_unsupported"
	// AdmissionRejectUnsupportedBackend means the requested backend is unavailable to strict admission.
	AdmissionRejectUnsupportedBackend string = "unsupported_backend"
	// AdmissionRejectUnfenceableBackend means the requested backend cannot satisfy strict fencing or containment.
	AdmissionRejectUnfenceableBackend string = "unfenceable_backend"
	// AdmissionRejectInvalidStrictConfig means the strict task configuration is malformed or incompatible with strict admission.
	AdmissionRejectInvalidStrictConfig string = "invalid_strict_config"
	// AdmissionRejectUnavailableNativeRuntime means the native runtime support probe failed strict runtime requirements.
	AdmissionRejectUnavailableNativeRuntime string = "unavailable_native_runtime"
	// AdmissionRejectRootCorrupt means the authority root has detected repository, anchor, or integrity corruption.
	AdmissionRejectRootCorrupt string = "root_corrupt"
	// AdmissionRejectRootIdentityMismatch means repository and anchor identities disagree.
	AdmissionRejectRootIdentityMismatch string = "root_identity_mismatch"
	// AdmissionRejectRootFailStopped means the authority root has tripped fail-stop and rejects admission.
	AdmissionRejectRootFailStopped string = "root_fail_stopped"
	// AdmissionRejectRootSealed means the authority root is sealed against service or admission.
	AdmissionRejectRootSealed string = "root_sealed"
	// AdmissionRejectAdmissionClosing means the daemon is closing and rejects new admission.
	AdmissionRejectAdmissionClosing string = "admission_closing"
)

const (
	DefaultTimeout = 30 * time.Minute
	MaxTimeout     = 4 * time.Hour
)

// Request is one JSON-RPC 2.0 request frame before newline framing.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is one JSON-RPC 2.0 response frame before newline framing.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *ErrorObject    `json:"error,omitempty"`
}

// ErrorObject is the JSON-RPC error object. Code remains numeric; Data.Code is stable.
type ErrorObject struct {
	Code    int       `json:"code"`
	Message string    `json:"message"`
	Data    ErrorData `json:"data"`
}

// ErrorData carries the stable protocol error identifier and optional context.
// AdmissionCause carries ADR-12 strict rejection causes.
type ErrorData struct {
	Code                  string                        `json:"code"`
	SessionID             string                        `json:"sessionId,omitempty"`
	JobID                 string                        `json:"jobId,omitempty"`
	Backend               string                        `json:"backend,omitempty"`
	AdmissionCause        string                        `json:"admissionCause,omitempty"`
	RuntimeSupport        *RuntimeSupportAssessmentData `json:"runtimeSupport,omitempty"`
	ServerProtocolVersion int                           `json:"serverProtocolVersion,omitempty"`
}

// RuntimeSupportAssessmentData is the wire-safe form of the strict native
// runtime support assessment carried on admission runtime rejections.
type RuntimeSupportAssessmentData struct {
	Class       string `json:"class"`
	Cause       string `json:"cause,omitempty"`
	Attempts    int    `json:"attempts"`
	CleanupSafe bool   `json:"cleanupSafe"`
}

// RPCError is returned by typed clients when a JSON-RPC error response arrives.
type RPCError struct {
	Object ErrorObject
}

func (e *RPCError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Object.Data.Code == "" {
		return e.Object.Message
	}
	return fmt.Sprintf("%s: %s", e.Object.Data.Code, e.Object.Message)
}

// NewError constructs a protocol error using the implementation-defined JSON-RPC code.
func NewError(stableCode, message string, data ErrorData) *ErrorObject {
	data.Code = stableCode
	code := -32000
	if stableCode == ErrorMethodNotFound {
		code = -32601
	}
	return &ErrorObject{Code: code, Message: message, Data: data}
}

func DefaultCapabilities() map[string]bool {
	return map[string]bool{
		"policy.shape":                  false,
		"policy.jsonSchema":             true,
		"policy.named":                  true,
		"policy.retry":                  true,
		"nativeStructuredOutput.codex":  false,
		"nativeStructuredOutput.claude": false,
		"models.discovery":              true,
		"models.reported":               true,
	}
}

type HelloParams struct {
	ClientProtocolVersion int    `json:"clientProtocolVersion"`
	Token                 string `json:"token"`
}

type HelloResult struct {
	ProtocolVersion int             `json:"protocolVersion"`
	Backends        []string        `json:"backends"`
	BackendMetadata []BackendInfo   `json:"backendMetadata,omitempty"`
	Capabilities    map[string]bool `json:"capabilities"`
}

type BackendInfo struct {
	Backend string   `json:"backend"`
	Models  []string `json:"models"`
	Efforts []string `json:"efforts"`
}

type TaskSpec struct {
	Backend   string             `json:"backend"`
	CWD       string             `json:"cwd"`
	Write     bool               `json:"write"`
	Model     string             `json:"model,omitempty"`
	Effort    string             `json:"effort,omitempty"`
	Prompt    string             `json:"prompt"`
	Policy    *engine.TurnPolicy `json:"policy,omitempty"`
	Tags      map[string]string  `json:"tags,omitempty"`
	TimeoutMs *int64             `json:"timeoutMs,omitempty"`
}

type JobSubmitParams struct {
	WorkspaceKey string   `json:"workspaceKey,omitempty"`
	RequestID    string   `json:"requestId,omitempty"`
	TaskSpec     TaskSpec `json:"taskSpec"`
}

type JobSubmitResult struct {
	JobID        string          `json:"jobId"`
	State        engine.JobState `json:"state"`
	Deduplicated bool            `json:"deduplicated,omitempty"`
}

type JobStatusParams struct {
	JobID string `json:"jobId,omitempty"`
	All   bool   `json:"all,omitempty"`
}

type JobStatusResult struct {
	Jobs []JobStatus `json:"jobs"`
}

type JobStatus struct {
	JobID              string            `json:"jobId"`
	SessionID          string            `json:"sessionId,omitempty"`
	Backend            string            `json:"backend,omitempty"`
	State              engine.JobState   `json:"state"`
	CleanupDisposition string            `json:"cleanupDisposition,omitempty"`
	LateFinalization   bool              `json:"lateFinalization,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
	StartedAt          *time.Time        `json:"startedAt,omitempty"`
	UpdatedAt          *time.Time        `json:"updatedAt,omitempty"`
	HeartbeatAt        *time.Time        `json:"heartbeatAt,omitempty"`
	// FinalAttemptStartedAt is the start of the final contract attempt, not
	// whole-job elapsed time. A retry replaces this value with its own start.
	FinalAttemptStartedAt *time.Time `json:"finalAttemptStartedAt,omitempty"`
	// FinalAttemptEndedAt is when that same final attempt reached terminal.
	FinalAttemptEndedAt   *time.Time          `json:"finalAttemptEndedAt,omitempty"`
	Lease                 *engine.Lease       `json:"lease,omitempty"`
	WorkerPID             int                 `json:"workerPid,omitempty"`
	WorkerStartTime       string              `json:"workerStartTime,omitempty"`
	BackendChildPID       int                 `json:"backendChildPid,omitempty"`
	BackendChildStartTime string              `json:"backendChildStartTime,omitempty"`
	StatePath             string              `json:"statePath,omitempty"`
	LogPaths              engine.LogPaths     `json:"logPaths,omitempty"`
	ModelReported         string              `json:"modelReported,omitempty"`
	Warnings              []string            `json:"warnings,omitempty"`
	FailureReason         string              `json:"failureReason,omitempty"`
	FailureClass          engine.FailureClass `json:"failureClass,omitempty"`
}

type JobResultParams struct {
	JobID string `json:"jobId"`
}

type JobResult struct {
	JobID              string                `json:"jobId"`
	SessionID          string                `json:"sessionId,omitempty"`
	State              engine.JobState       `json:"state"`
	CleanupDisposition string                `json:"cleanupDisposition,omitempty"`
	LateFinalization   bool                  `json:"lateFinalization,omitempty"`
	Result             *engine.ResultInfo    `json:"result,omitempty"`
	ModelReported      string                `json:"modelReported,omitempty"`
	Contract           *engine.ContractStamp `json:"contract,omitempty"`
	// FinalAttemptStartedAt is the start of the final contract attempt, not
	// whole-job elapsed time. A retry replaces this value with its own start.
	FinalAttemptStartedAt *time.Time `json:"finalAttemptStartedAt,omitempty"`
	// FinalAttemptEndedAt is when that same final attempt reached terminal.
	FinalAttemptEndedAt *time.Time          `json:"finalAttemptEndedAt,omitempty"`
	FailureReason       string              `json:"failureReason,omitempty"`
	FailureClass        engine.FailureClass `json:"failureClass,omitempty"`
}

type JobCancelParams struct {
	JobID string `json:"jobId"`
}

type JobCancelResult struct {
	JobID string          `json:"jobId"`
	State engine.JobState `json:"state"`
}

type PolicyValidateParams struct {
	Text     string              `json:"text"`
	Contract engine.ContractSpec `json:"contract"`
}

type PolicyValidateResult = engine.ValidationResult

type PolicyRegisterParams struct {
	Name string              `json:"name"`
	Spec engine.ContractSpec `json:"spec"`
}

type PolicyRegisterResult struct {
	Name           string `json:"name"`
	ContractSHA256 string `json:"contractSha256"`
	Registered     bool   `json:"registered"`
}
