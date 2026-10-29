package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams
type BackendInfo = protocol.BackendInfo
type BackendInspectParams = protocol.BackendInspectParams
type BackendInspection = protocol.BackendInspectionWire
type BackendInspectResult = protocol.BackendInspectResult
type HelloResult = protocol.HelloResult

// TaskSpec is the client-facing alias of protocol.TaskSpec, including its
// optional RetainSession declaration.
type TaskSpec = protocol.TaskSpec
type InputMode = protocol.InputMode
type JobSubmitParams = protocol.JobSubmitParams
type JobSubmitResult = protocol.JobSubmitResult
type JobSteerParams = protocol.JobSteerParams
type JobSteerResult = protocol.JobSteerResult
type SteerStatus = protocol.SteerStatus
type SteerSummaryWire = protocol.SteerSummaryWire
type JobCancelParams = protocol.JobCancelParams
type JobCancelResult = protocol.JobCancelResult
type JobGetParams = protocol.JobGetParams
type JobGetResult = protocol.JobRecordWire
type ExecutionPosture = protocol.ExecutionPosture
type JobListParams = protocol.JobListParams
type JobListResult = protocol.JobListResult
type JobSummaryWire = protocol.JobSummaryWire
type JobTranscriptParams = protocol.JobTranscriptParams
type JobTranscriptResult = protocol.JobTranscriptResult
type ArtifactsPruneParams = protocol.ArtifactsPruneParams
type ArtifactsPruneResult = protocol.ArtifactsPruneResult
type PruneItem = protocol.PruneItem
type PruneSkip = protocol.PruneSkip
type TranscriptItem = protocol.TranscriptItem
type PublicState = protocol.PublicState
type Liveness = protocol.Liveness
type FailureClass = protocol.FailureClass
type Cleanup = protocol.Cleanup
type ContractResult = protocol.ContractResult
type ContractVerdict = protocol.ContractVerdict
type SteerReason = protocol.SteerReason
type RPCError = protocol.RPCError
