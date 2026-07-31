package codexcli

import (
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion = "0.143.0"
	StreamSchema            = "codex-appserver-v1"
)

type Options struct {
	Binary           string
	CachePath        string
	SupportedModels  []string
	SupportedEfforts []string
}

func New(opts Options) engine.Backend {
	efforts := opts.SupportedEfforts
	if len(efforts) == 0 {
		efforts = []string{"none", "minimal", "low", "medium", "high", "xhigh"}
	}
	driver := newAppServerDriver(opts.Binary)
	return &cliadapter.Backend{
		NameValue:      "codex",
		Binary:         opts.Binary,
		MinimumVersion: MinimumKnownGoodVersion,
		CachePath:      opts.CachePath,
		StreamSchema:   StreamSchema,
		AllowedModels:  cliadapter.StringSet(opts.SupportedModels...),
		AllowedEfforts: cliadapter.StringSet(efforts...),
		Driver:         driver,
		SetupQualify:   driver.SetupQualify,
	}
}
