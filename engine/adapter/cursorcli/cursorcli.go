package cursorcli

import (
	"regexp"
	"strings"

	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion = "2026.08.04"
	StreamSchema            = "cursor-acp-v1"
)

// Options configures the Cursor CLI backend.
type Options struct {
	Binary string
}

// New constructs the Cursor ACP backend.
func New(opts Options) *cliadapter.Backend {
	driver := newACPDriver(opts.Binary)
	return &cliadapter.Backend{
		NameValue:        "cursor",
		Binary:           opts.Binary,
		MinimumVersion:   MinimumKnownGoodVersion,
		StreamSchema:     StreamSchema,
		Driver:           driver,
		VersionTransform: normalizeCursorVersion,
	}
}

var cursorVersionPattern = regexp.MustCompile(`\b[0-9]{4}\.[0-9]{2}\.[0-9]{2}(?:-[A-Za-z0-9._-]+)?\b`)

func normalizeCursorVersion(value string) string {
	if version := cursorVersionPattern.FindString(value); version != "" {
		return strings.SplitN(version, "-", 2)[0]
	}
	return strings.TrimSpace(value)
}
