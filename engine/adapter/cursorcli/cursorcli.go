package cursorcli

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
	"github.com/charlesnpx/agentbus/engine/command"
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
		Discover:         discoverModels,
	}
}

var cursorVersionPattern = regexp.MustCompile(`\b[0-9]{4}\.[0-9]{2}\.[0-9]{2}(?:-[A-Za-z0-9._-]+)?\b`)

func normalizeCursorVersion(value string) string {
	if version := cursorVersionPattern.FindString(value); version != "" {
		return strings.SplitN(version, "-", 2)[0]
	}
	return strings.TrimSpace(value)
}

func discoverModels(ctx context.Context, runner command.ProbeRunner, binary string) (*engine.ModelDiscovery, error) {
	if runner == nil {
		return nil, fmt.Errorf("probe runner is required")
	}

	var lastErr error
	for _, args := range [][]string{{binary, "models"}, {binary, "--list-models"}} {
		result, err := runner.Run(ctx, command.ProbeSpec{Argv: args})
		if err != nil {
			lastErr = err
			continue
		}
		return &engine.ModelDiscovery{
			Models:    parseAvailableModels(string(result.Stdout) + "\n" + string(result.Stderr)),
			Source:    "cursor models",
			FetchedAt: time.Now().UTC().Format(time.RFC3339),
		}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("cursor model discovery did not run")
	}
	return nil, lastErr
}

func parseAvailableModels(output string) []string {
	seen := make(map[string]struct{})
	var models []string
	inModels := false
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(strings.ToLower(trimmed), "available models") {
			inModels = true
			continue
		}
		if !inModels || !strings.Contains(trimmed, " - ") {
			continue
		}
		id := strings.TrimLeft(strings.TrimSpace(strings.SplitN(trimmed, " - ", 2)[0]), "*- ")
		if fields := strings.Fields(id); len(fields) > 0 {
			id = fields[0]
		}
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		models = append(models, id)
	}
	return models
}
