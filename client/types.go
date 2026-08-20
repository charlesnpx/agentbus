package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams
type BackendInfo = protocol.BackendInfo
type HelloResult = protocol.HelloResult
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
type JobGetParams = protocol.JobGetParams
type JobGetResult = protocol.JobRecordWire
type JobGetListResult = protocol.JobGetListResult
type JobSummaryWire = protocol.JobSummaryWire
type PublicState = protocol.PublicState
type FailureClass = protocol.FailureClass
type Cleanup = protocol.Cleanup
type ContractResult = protocol.ContractResult
type ContractVerdict = protocol.ContractVerdict
type PolicyValidateParams = protocol.PolicyValidateParams
type PolicyValidateResult = protocol.PolicyValidateResult
type PolicyRegisterParams = protocol.PolicyRegisterParams
type PolicyRegisterResult = protocol.PolicyRegisterResult
type RPCError = protocol.RPCError
