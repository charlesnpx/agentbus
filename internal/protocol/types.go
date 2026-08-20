package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	SocketName    = "agentbus.sock"
	TokenFileName = "token"

	MethodHello     = "protocol.hello"
	MethodJobSubmit = "job.submit"
	MethodJobCancel = "job.cancel"
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
type ErrorData struct {
	Code                  string `json:"code"`
	SessionID             string `json:"sessionId,omitempty"`
	JobID                 string `json:"jobId,omitempty"`
	Backend               string `json:"backend,omitempty"`
	ServerProtocolVersion int    `json:"serverProtocolVersion,omitempty"`
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

type HelloParams struct {
	ClientProtocolVersion int    `json:"clientProtocolVersion"`
	Token                 string `json:"token"`
}

type BackendInfo struct {
	Name    string   `json:"backend"`
	Models  []string `json:"models"`
	Efforts []string `json:"efforts"`
}
