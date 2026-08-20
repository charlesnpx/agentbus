package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams
type BackendInfo = protocol.BackendInfo
type HelloResult = protocol.HelloResultV3
type TaskSpec = protocol.TaskSpecV3
type JobSubmitParams = protocol.JobSubmitParamsV3
type JobSubmitResult = protocol.JobSubmitResultV3
type JobCancelParams = protocol.JobCancelParamsV3
type JobCancelResult = protocol.JobCancelResultV3
type JobGetParams = protocol.JobGetParams
type JobGetResult = protocol.JobRecordWire
type JobGetListResult = protocol.JobGetListResult
type JobSummaryWire = protocol.JobSummaryWire
type PublicState = protocol.PublicState
type FailureClass = protocol.FailureClass
type Cleanup = protocol.Cleanup
type ContractResult = protocol.ContractResult
type ContractVerdict = protocol.ContractVerdict
type RPCError = protocol.RPCError
