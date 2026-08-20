package codexcli

import (
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion = "0.143.0"
	StreamSchema            = "codex-appserver-v1"
)

// WritePolicy controls the sandbox policy used for write-enabled Codex turns.
// Its zero value preserves the existing workspace-write, offline behavior.
type WritePolicy uint8

const (
	// WritePolicyWorkspaceOffline permits writes in the workspace without network access.
	WritePolicyWorkspaceOffline WritePolicy = iota
	// WritePolicyWorkspaceNetwork permits writes in the workspace with network access.
	WritePolicyWorkspaceNetwork
	// WritePolicyTrusted uses the app-server's unrestricted sandbox policy.
	WritePolicyTrusted
)

type Options struct {
	Binary           string
	SupportedModels  []string
	SupportedEfforts []string
	WritePolicy      WritePolicy
}

func New(opts Options) engine.Backend {
	efforts := opts.SupportedEfforts
	if len(efforts) == 0 {
		efforts = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}
	}
	driver := newAppServerDriver(opts.Binary, opts.WritePolicy)
	return &cliadapter.Backend{
		NameValue:      "codex",
		Binary:         opts.Binary,
		MinimumVersion: MinimumKnownGoodVersion,
		StreamSchema:   StreamSchema,
		AllowedModels:  cliadapter.StringSet(opts.SupportedModels...),
		AllowedEfforts: cliadapter.StringSet(efforts...),
		Driver:         driver,
	}
}
