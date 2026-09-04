package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams
type BackendInfo = protocol.BackendInfo
type HelloResult = protocol.HelloResult
type TaskSpec = protocol.TaskSpec
type JobSubmitParams = protocol.JobSubmitParams
type JobSubmitResult = protocol.JobSubmitResult
type JobCancelParams = protocol.JobCancelParams
type JobCancelResult = protocol.JobCancelResult
type JobGetParams = protocol.JobGetParams
type JobGetResult = protocol.JobRecordWire
type JobListParams = protocol.JobListParams
type JobListResult = protocol.JobListResult
type JobSummaryWire = protocol.JobSummaryWire
type PublicState = protocol.PublicState
type Liveness = protocol.Liveness
type FailureClass = protocol.FailureClass
type Cleanup = protocol.Cleanup
type ContractResult = protocol.ContractResult
type ContractVerdict = protocol.ContractVerdict
type RPCError = protocol.RPCError
