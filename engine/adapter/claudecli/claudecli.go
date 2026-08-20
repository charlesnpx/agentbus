package claudecli

import (
	"context"
	"fmt"
	"regexp"
	"sort"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
	"github.com/charlesnpx/agentbus/engine/command"
)

const (
	MinimumKnownGoodVersion = "2.1.205"
	StreamSchema            = "claude-streamjson-v2"
)

var readOnlyAllowedTools = []string{
	"Read",
	"Grep",
	"Glob",
	"Bash(git diff*)",
	"Bash(git log*)",
	"Bash(git show*)",
	"Bash(git status*)",
	"Bash(cat*)",
	"Bash(rg*)",
	"Bash(grep*)",
	"Bash(ls*)",
	"Bash(head*)",
	"Bash(tail*)",
	"Bash(wc*)",
}

var readOnlyDeniedTools = []string{
	"Edit",
	"Write",
	"NotebookEdit",
	"mcp__*",
	"Bash(*&&*)",
	"Bash(*&*)",
	"Bash(*;*)",
	"Bash(*|*)",
	"Bash(*$(*)",
	"Bash(*`*)",
	"Bash(*<(*)",
	"Bash(*>*)",
	"Bash(*>>*)",
	"Bash(sed -i*)",
	"Bash(tee*)",
	"Bash(find*)",
	"Bash(rm*)",
	"Bash(mv*)",
	"Bash(cp*)",
	"Bash(git -c*)",
	"Bash(git --config-env*)",
	"Bash(git --paginate*)",
	"Bash(git -p*)",
	"Bash(git *--help*)",
	"Bash(*--output*)",
	"Bash(*--ext-diff*)",
	"Bash(*--textconv*)",
	"Bash(*--pre*)",
	"Bash(*--hostname-bin*)",
	"Bash(*--search-zip*)",
	"Bash(* -z*)",
	"Bash(git commit*)",
	"Bash(git push*)",
	"Bash(git checkout*)",
	"Bash(chmod*)",
	"Bash(curl*)",
	"Bash(wget*)",
}

type Options struct {
	Binary           string
	SupportedModels  []string
	SupportedEfforts []string
}

func New(opts Options) engine.Backend {
	efforts := opts.SupportedEfforts
	if len(efforts) == 0 {
		efforts = []string{"low", "medium", "high", "max"}
	}
	driver := newStreamJSONDriver(opts.Binary)
	return &cliadapter.Backend{
		NameValue:      "claude",
		Binary:         opts.Binary,
		MinimumVersion: MinimumKnownGoodVersion,
		StreamSchema:   StreamSchema,
		AllowedModels:  cliadapter.StringSet(opts.SupportedModels...),
		AllowedEfforts: cliadapter.StringSet(efforts...),
		Driver:         driver,
		Discover:       discoverModels,
	}
}

func discoverModels(ctx context.Context, runner command.ProbeRunner, binary string) (*engine.ModelDiscovery, error) {
	if runner == nil {
		return nil, fmt.Errorf("probe runner is required")
	}
	result, err := runner.Run(ctx, command.ProbeSpec{Argv: []string{binary, "--help"}})
	if err != nil {
		return nil, err
	}
	text := string(result.Stdout) + string(result.Stderr)
	efforts := valuesFromGroup(text, `(?m)--effort[^\n]*\n?[^\n]*\(([^)]+)\)`)
	models := valuesFromGroup(text, `(?m)--model[^\n]*\n(?:[^\n]*\n){0,4}?[^\n]*\((?:e\.g\.\s*)?([^)]+)\)`)
	if len(models) == 0 && len(efforts) == 0 {
		return nil, fmt.Errorf("claude --help model discovery parser found no model or effort listings")
	}
	return &engine.ModelDiscovery{Models: models, Efforts: efforts, Source: "claude --help"}, nil
}

func valuesFromGroup(text, pattern string) []string {
	match := regexp.MustCompile(pattern).FindStringSubmatch(text)
	if len(match) < 2 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, value := range regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9._-]*`).FindAllString(match[1], -1) {
		if value != "e.g" && value != "or" {
			seen[value] = struct{}{}
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}
