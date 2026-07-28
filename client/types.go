package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams

// BackendInfo describes the models and effort levels advertised by a backend.
type BackendInfo struct {
	Name    string   `json:"backend"`
	Models  []string `json:"models"`
	Efforts []string `json:"efforts"`
}

// HelloResult describes the server and its negotiated client capabilities.
type HelloResult struct {
	ProtocolVersion int             `json:"protocolVersion"`
	Backends        []string        `json:"backends"`
	BackendMetadata []BackendInfo   `json:"backendMetadata,omitempty"`
	Capabilities    map[string]bool `json:"capabilities"`
}
type TaskSpec = protocol.TaskSpec
type JobSubmitParams = protocol.JobSubmitParams
type JobSubmitResult = protocol.JobSubmitResult
type JobStatusParams = protocol.JobStatusParams
type JobStatusResult = protocol.JobStatusResult
type JobStatus = protocol.JobStatus
type JobResultParams = protocol.JobResultParams
type JobResult = protocol.JobResult
type JobCancelParams = protocol.JobCancelParams
type JobCancelResult = protocol.JobCancelResult
type PolicyValidateParams = protocol.PolicyValidateParams
type PolicyValidateResult = protocol.PolicyValidateResult
type PolicyRegisterParams = protocol.PolicyRegisterParams
type PolicyRegisterResult = protocol.PolicyRegisterResult
type RPCError = protocol.RPCError
