package claudecli

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion = "2.1.205"
	StreamSchema            = "claude-stream-json-v1"
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
	CachePath        string
	SupportedModels  []string
	SupportedEfforts []string
}

func New(opts Options) engine.Backend {
	efforts := opts.SupportedEfforts
	if len(efforts) == 0 {
		efforts = []string{"low", "medium", "high", "max"}
	}
	return &cliadapter.Backend{
		NameValue:      "claude",
		Binary:         opts.Binary,
		MinimumVersion: MinimumKnownGoodVersion,
		CachePath:      opts.CachePath,
		StreamSchema:   StreamSchema,
		AllowedModels:  cliadapter.StringSet(opts.SupportedModels...),
		AllowedEfforts: cliadapter.StringSet(efforts...),
		BuildArgs:      buildArgs,
		Parse:          parseEvent,
		Discover:       discoverModels,
	}
}

func discoverModels(ctx context.Context, binary string) (*engine.ModelDiscovery, error) {
	out, err := exec.CommandContext(ctx, binary, "--help").CombinedOutput()
	if err != nil {
		return nil, err
	}
	text := string(out)
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

func buildArgs(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error) {
	args := []string{"--print", "--output-format", "stream-json"}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	if opts.Effort != "" {
		args = append(args, "--effort", opts.Effort)
	}
	if input.Write {
		args = append(args, "--dangerously-skip-permissions")
	} else {
		args = append(args,
			"--bare",
			"--strict-mcp-config",
			"--mcp-config", "{}",
			"--permission-mode", "dontAsk",
			"--allowedTools", strings.Join(readOnlyAllowedTools, ","),
			"--disallowedTools", strings.Join(readOnlyDeniedTools, ","),
		)
	}
	if resumeID != "" {
		args = append(args, "--resume", resumeID)
	}
	return args, nil
}

func parseEvent(obj map[string]any) ([]engine.Event, string, error) {
	id := firstString(obj, "session_id", "sessionId", "uuid")
	typ := strings.ToLower(firstString(obj, "type"))
	switch typ {
	case "system":
		return nil, id, nil
	case "assistant":
		return parseAssistant(obj, id)
	case "result":
		if text := firstString(obj, "result"); text != "" {
			return []engine.Event{{Type: engine.EventResultMessage, Text: text, Metadata: obj}}, id, nil
		}
	case "user":
		return nil, id, nil
	case "error", "warning":
		return []engine.Event{{Type: engine.EventWarning, Text: textFrom(obj), Metadata: obj}}, id, nil
	}
	if text := textFrom(obj); text != "" && typ != "" {
		return []engine.Event{{Type: engine.EventAgentText, Text: text, Metadata: obj}}, id, nil
	}
	return nil, id, nil
}

func parseAssistant(obj map[string]any, id string) ([]engine.Event, string, error) {
	msg, _ := obj["message"].(map[string]any)
	content, ok := msg["content"].([]any)
	if !ok {
		if text := textFrom(obj); text != "" {
			return []engine.Event{{Type: engine.EventAgentText, Text: text, Metadata: obj}}, id, nil
		}
		return nil, id, nil
	}
	var events []engine.Event
	for _, item := range content {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		switch firstString(block, "type") {
		case "text":
			if text := firstString(block, "text"); text != "" {
				events = append(events, engine.Event{Type: engine.EventAgentText, Text: text, Metadata: obj})
			}
		case "tool_use":
			name := firstString(block, "name")
			text := firstString(block, "text")
			if text == "" {
				b, _ := json.Marshal(block["input"])
				text = string(b)
			}
			if text == "" {
				text = name
			}
			events = append(events, engine.Event{Type: engine.EventToolUse, Name: name, Text: text, Metadata: obj})
		}
	}
	return events, id, nil
}

func textFrom(obj map[string]any) string {
	for _, key := range []string{"text", "message", "content", "result", "error"} {
		if s := firstString(obj, key); s != "" {
			return s
		}
	}
	return fmt.Sprint(obj)
}

func firstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := obj[key]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}
