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
}

// SessionOpts configures a backend session default.
type SessionOpts struct {
	CWD     string
	Write   bool
	Model   string
	Effort  string
	Timeout time.Duration
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
	Type      string         `json:"type"`
	Name      string         `json:"name,omitempty"`
	Text      string         `json:"text"`
	Truncated bool           `json:"truncated"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	RawText   string         `json:"-"`
}

const (
	EventAgentText = "AgentText"
	EventToolUse   = "ToolUse"
	EventWarning   = "Warning"
)

// SetupProbeCache is written by setup after a live stream probe and read by
// adapter Preflight without running another backend turn.
type SetupProbeCache struct {
	Backends []BackendSetupProbe `json:"backends"`
}

// BackendSetupProbe records the setup-time facts required by Preflight.
type BackendSetupProbe struct {
	Backend          string   `json:"backend"`
	BinaryPath       string   `json:"binaryPath"`
	Version          string   `json:"version"`
	StreamSchema     string   `json:"streamSchema"`
	ConfigMode       ModeInfo `json:"configMode"`
	SandboxModes     []string `json:"sandboxModes"`
	JSONEventsProbed bool     `json:"jsonEventsProbed"`
}

// ModeInfo describes write/read-only configuration loading.
type ModeInfo struct {
	Write    string `json:"write"`
	ReadOnly string `json:"readOnly"`
}
