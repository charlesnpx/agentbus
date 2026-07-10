package client

import "github.com/charlesnpx/agentbus/internal/protocol"

type HelloParams = protocol.HelloParams
type HelloResult = protocol.HelloResult
type SessionStartParams = protocol.SessionStartParams
type SessionStartResult = protocol.SessionStartResult
type SessionResumeParams = protocol.SessionResumeParams
type SessionListParams = protocol.SessionListParams
type SessionListResult = protocol.SessionListResult
type SessionInfo = protocol.SessionInfo
type TurnStartParams = protocol.TurnStartParams
type TurnStartResult = protocol.TurnStartResult
type TurnEventParams = protocol.TurnEventParams
type TurnResultParams = protocol.TurnResultParams
type TurnInterruptParams = protocol.TurnInterruptParams
type TurnInterruptResult = protocol.TurnInterruptResult
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

// TurnNotification is one streamed foreground notification for a turn.
type TurnNotification struct {
	Method string
	Event  *TurnEventParams
	Result *TurnResultParams
}
