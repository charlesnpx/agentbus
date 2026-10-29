package engine

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrSteerUnsupported means the session's adapter cannot steer this turn.
	ErrSteerUnsupported = errors.New("steering unsupported")
	// ErrSteerNotActive means there is no active provider turn to steer.
	ErrSteerNotActive = errors.New("no active turn to steer")
)

const (
	// SteerMechanismNative identifies provider-native steering of the current turn.
	SteerMechanismNative = "native"
	// SteerMechanismInterruptContinue identifies an interrupt followed by a
	// continuation in the same provider conversation.
	SteerMechanismInterruptContinue = "interrupt_continue"
)

// Backend is the public engine adapter surface for a backend CLI.
type Backend interface {
	Name() string
	Preflight(ctx context.Context) (Health, error)
	Start(ctx context.Context, opts SessionOpts) (Session, error)
	Resume(ctx context.Context, id string, opts SessionOpts) (Session, error)
}

// BackendInspection describes the local executable configured for a backend.
// It does not describe account status or provider model availability.
type BackendInspection struct {
	Executable string
}

// BackendInspectionProvider exposes the executable configured on an adapter.
type BackendInspectionProvider interface {
	BackendInspection() BackendInspection
}

// Session is one resumable backend conversation.
type Session interface {
	ID() string
	Turn(ctx context.Context, input TurnInput) (<-chan Event, error)
	Interrupt(ctx context.Context) error
}

// Steerer is an optional extension implemented by sessions that can accept a
// follow-up instruction during their currently running Turn. Turn's event
// channel must stay open while steering and continue with events from any
// resumed segment. The logical Turn emits exactly one final result and one
// TurnFinal observation. An internal interruption used to steer must never be
// exposed as cancellation or a terminal error, and a partial result from the
// interrupted segment must never be reported as the logical turn's final
// result. Implementations must not change the turn's deadline, permissions,
// model, effort, or schema. Sessions without this interface remain valid and
// are treated as not supporting steering.
type Steerer interface {
	Steer(ctx context.Context, msg SteerMessage) (SteerDelivery, error)
}

// SteerMessage is one caller-identified follow-up instruction. Text is passed
// only to the active adapter and is not added to the original TurnInput.
type SteerMessage struct {
	ID   string
	Text string
}

// SteerDelivery reports the adapter's delivery mechanism and whether the
// provider acknowledged the instruction. Confirmed=false means the message
// was written but provider acceptance was not confirmed; callers must retain
// that uncertainty and must not redeliver automatically.
type SteerDelivery struct {
	Mechanism string
	Confirmed bool
}

// Health is the non-network preflight result for a backend.
type Health struct {
	Backend    string
	BinaryPath string
	Version    string
	// StreamSchema is the stream protocol this adapter speaks. It is an
	// adapter declaration, not a capability verified from the binary by
	// Preflight.
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
	// LocalIPC selects the Codex local-IPC permission profile for this job.
	LocalIPC bool
	// LocalIPCRoot is the private short temp directory granted for local IPC.
	LocalIPCRoot string
	Write        bool
	Model        string
	Effort       string
	Timeout      time.Duration
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
	Type           string         `json:"type"`
	Name           string         `json:"name,omitempty"`
	Text           string         `json:"text"`
	Truncated      bool           `json:"truncated"`
	ModelReported  string         `json:"modelReported,omitempty"`
	EffortReported string         `json:"effortReported,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
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
	EventToolResult    = "ToolResult"
	EventReasoning     = "Reasoning"
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
