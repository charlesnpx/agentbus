package engine

import (
	"context"
	"time"
)

// Backend is the public engine adapter surface for a backend CLI.
type Backend interface {
	Name() string
	Preflight(ctx context.Context) (Health, error)
	Start(ctx context.Context, opts SessionOpts) (Session, error)
	Resume(ctx context.Context, id string, opts SessionOpts) (Session, error)
}

// Session is one resumable backend conversation.
type Session interface {
	ID() string
	Turn(ctx context.Context, input TurnInput) (<-chan Event, error)
	Interrupt(ctx context.Context) error
}

// Health is the non-network preflight result for a backend.
type Health struct {
	Backend      string
	BinaryPath   string
	Version      string
	StreamSchema string
	Minimum      string
	Warning      string
}

// SessionOpts configures a backend session default.
type SessionOpts struct {
	CWD        string
	EnvOverlay map[string]string
	// WriteEnvOverlay is merged after EnvOverlay only for write turns.
	WriteEnvOverlay map[string]string
	// WriteSandboxRoot is added to workspace-write sandbox policies for write
	// turns only. It is ignored for read-only and trusted turns.
	WriteSandboxRoot string
	Write            bool
	Model            string
	Effort           string
	Timeout          time.Duration
}

// TurnInput is the effective input for one backend turn.
type TurnInput struct {
	Prompt         string
	Write          bool
	Timeout        time.Duration
	LogPaths       LogPaths
	OnProcessStart func(ProcessRef, int)
}

// Event is an agentbus streaming event emitted by an adapter.
type Event struct {
	Type          string         `json:"type"`
	Name          string         `json:"name,omitempty"`
	Text          string         `json:"text"`
	Truncated     bool           `json:"truncated"`
	ModelReported string         `json:"modelReported,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	// ObservedWorkspaceWriteItem reports one write-bearing item observed by
	// the backend. It is in-process metadata only: callers persist its count,
	// never the item, paths, or contents.
	ObservedWorkspaceWriteItem bool                  `json:"-"`
	TurnFinal                  *TurnFinalObservation `json:"-"`
	RawText                    string                `json:"-"`
	// Err preserves an in-process, adapter-confirmed terminal condition. It is
	// deliberately never serialized because text from a backend is not a typed
	// status signal.
	Err error `json:"-"`
}

// TurnFinalObservation is the embedded-only retirement observation emitted at
// the end of a duplex turn.
type TurnFinalObservation struct {
	BackendSessionID string
	ReturnCodeKnown  bool
	ReturnCode       int
	Signal           string
	TimedOut         bool
	Canceled         bool
	ExecutionFailed  bool
	CleanupFailed    bool
}

const (
	EventAgentText     = "AgentText"
	EventToolUse       = "ToolUse"
	EventWarning       = "Warning"
	EventModelReported = "ModelReported"
	EventResultMessage = "ResultMessage"
	EventTerminalError = "TerminalError"
	EventTurnFinal     = "TurnFinal"
	EventProgress      = "Progress"
)

type ModelDiscovery struct {
	Models        []string
	Efforts       []string
	Source        string
	FetchedAt     string
	ClientVersion string
	Warnings      []string
}

type BackendMetadataProvider interface {
	BackendMetadata(context.Context) BackendMetadata
}

type BackendMetadata struct {
	Name    string
	Models  []string
	Efforts []string
}
