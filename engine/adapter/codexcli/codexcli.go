package codexcli

import (
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion   = "0.143.0"
	StreamSchema              = "codex-appserver-v1"
	localIPCPermissionProfile = "agentbus_local_ipc"
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
	return &backend{
		Backend: &cliadapter.Backend{
			NameValue:      "codex",
			Binary:         opts.Binary,
			MinimumVersion: MinimumKnownGoodVersion,
			StreamSchema:   StreamSchema,
			AllowedModels:  cliadapter.StringSet(opts.SupportedModels...),
			AllowedEfforts: cliadapter.StringSet(efforts...),
			Driver:         driver,
		},
		writePolicy: opts.WritePolicy,
	}
}

type backend struct {
	*cliadapter.Backend
	writePolicy WritePolicy
}

// CodexSandboxRequired tells the service whether this job relies on Codex's
// OS sandbox. Read-only and local-IPC turns always use the sandbox profile;
// only explicitly trusted write turns omit the sandbox-start check.
func (b *backend) CodexSandboxRequired(write, localIPC bool) bool {
	return !write || localIPC || b.writePolicy != WritePolicyTrusted
}
