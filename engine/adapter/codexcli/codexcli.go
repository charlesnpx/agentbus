package codexcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
)

const (
	MinimumKnownGoodVersion = "0.143.0"
	StreamSchema            = "codex-json-v1"
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
	return &cliadapter.Backend{
		NameValue:      "codex",
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

func discoverModels(ctx context.Context, _ string) (*engine.ModelDiscovery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := modelsCachePath()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read models cache %s: %w", path, err)
	}
	var cache modelsCache
	if err := json.Unmarshal(raw, &cache); err != nil {
		return nil, fmt.Errorf("parse models cache %s: %w", path, err)
	}

	discovery := &engine.ModelDiscovery{
		Source:        "models_cache",
		FetchedAt:     cache.FetchedAt,
		ClientVersion: cache.ClientVersion,
	}
	seenEfforts := make(map[string]struct{})
	unknownEfforts := make([]string, 0)
	for _, model := range cache.Models {
		if model.Visibility != "list" {
			continue
		}
		slug := strings.TrimSpace(model.Slug)
		if slug == "" {
			discovery.Warnings = append(discovery.Warnings, fmt.Sprintf("codex models cache %s: skipped a list-visible entry with an empty slug", path))
			continue
		}
		discovery.Models = append(discovery.Models, slug)
		for _, level := range model.SupportedReasoningLevels {
			effort := strings.TrimSpace(level.Effort)
			if effort == "" {
				continue
			}
			if _, seen := seenEfforts[effort]; seen {
				continue
			}
			seenEfforts[effort] = struct{}{}
			if _, known := canonicalEffortRank[effort]; !known {
				unknownEfforts = append(unknownEfforts, effort)
			}
		}
	}
	for _, effort := range canonicalEffortOrder {
		if _, seen := seenEfforts[effort]; seen {
			discovery.Efforts = append(discovery.Efforts, effort)
		}
	}
	discovery.Efforts = append(discovery.Efforts, unknownEfforts...)

	if cache.FetchedAt != "" {
		fetchedAt, err := time.Parse(time.RFC3339, cache.FetchedAt)
		if err != nil {
			discovery.Warnings = append(discovery.Warnings, fmt.Sprintf("codex models cache fetched_at %q is not RFC 3339", cache.FetchedAt))
		} else if fetchedAt.Before(time.Now().UTC().Add(-7 * 24 * time.Hour)) {
			discovery.Warnings = append(discovery.Warnings, fmt.Sprintf("codex models cache is stale: fetched_at %q is older than 7 days", cache.FetchedAt))
		}
	}
	return discovery, nil
}

type modelsCache struct {
	FetchedAt     string       `json:"fetched_at"`
	ClientVersion string       `json:"client_version"`
	Models        []cacheModel `json:"models"`
}

type cacheModel struct {
	Slug                     string           `json:"slug"`
	Visibility               string           `json:"visibility"`
	SupportedReasoningLevels []reasoningLevel `json:"supported_reasoning_levels"`
}

type reasoningLevel struct {
	Effort string `json:"effort"`
}

var canonicalEffortOrder = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"}

var canonicalEffortRank = func() map[string]int {
	ranks := make(map[string]int, len(canonicalEffortOrder))
	for i, effort := range canonicalEffortOrder {
		ranks[effort] = i
	}
	return ranks
}()

func modelsCachePath() (string, error) {
	if home := os.Getenv("CODEX_HOME"); home != "" {
		return filepath.Join(home, "models_cache.json"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve CODEX_HOME: %w", err)
	}
	return filepath.Join(home, ".codex", "models_cache.json"), nil
}

func buildArgs(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error) {
	args := []string{"exec", "--json"}
	if !isGitRepository(opts.CWD) {
		args = append(args, "--skip-git-repo-check")
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	sandbox := "read-only"
	if input.Write {
		sandbox = "workspace-write"
	}
	args = append(args, "--sandbox", sandbox)
	if !input.Write {
		args = append(args, "--ignore-user-config")
	}
	if opts.Effort != "" {
		args = append(args, "--config", fmt.Sprintf("model_reasoning_effort=%q", opts.Effort))
	}
	if resumeID != "" {
		args = append(args, "resume", resumeID, "-")
	} else {
		args = append(args, "-")
	}
	return args, nil
}

// isGitRepository reports whether cwd is inside a Git worktree without
// invoking Git. Errors resolving or inspecting the directory fail closed so a
// trust check is only bypassed for a directory proved not to be a repository.
func isGitRepository(cwd string) bool {
	resolved, err := resolvedCWD(cwd)
	if err != nil {
		return true
	}
	for {
		_, err := os.Lstat(filepath.Join(resolved, ".git"))
		if err == nil {
			return true
		}
		if !errors.Is(err, os.ErrNotExist) {
			return true
		}
		parent := filepath.Dir(resolved)
		if parent == resolved {
			return false
		}
		resolved = parent
	}
}

func resolvedCWD(cwd string) (string, error) {
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

func parseEvent(obj map[string]any) ([]engine.Event, string, error) {
	id := firstString(obj, "thread_id", "session_id", "sessionId", "conversation_id", "conversationId", "id")
	typ := strings.ToLower(firstString(obj, "type", "event"))
	switch typ {
	case "thread.started", "session_configured":
		return modelReportedEvent(obj, id), id, nil
	case "task_started", "turn.started", "turn_started", "item.updated", "token_count":
		return nil, id, nil
	case "item.completed":
		return parseItemCompleted(obj, id)
	case "turn.completed":
		text := firstString(obj, "last_agent_message", "lastAgentMessage", "result", "message", "text", "content", "output")
		return []engine.Event{{Type: engine.EventResultMessage, Text: text, Metadata: obj}}, id, nil
	case "agent_message", "agent_message_content_delta", "agentmessage", "agenttext", "agent_text", "assistant_text", "message", "assistant_message":
		return agentTextEvent(obj, id)
	case "task_complete", "turn_complete", "taskcomplete", "result":
		if text := firstString(obj, "last_agent_message", "lastAgentMessage", "result", "message", "text", "content", "output"); text != "" {
			return []engine.Event{{Type: engine.EventResultMessage, Text: text, Metadata: obj}}, id, nil
		}
	case "item_completed":
		return parseItemCompleted(obj, id)
	case "tooluse", "tool_use", "tool_call", "exec_command", "function_call", "local_shell_call", "exec_command_begin", "exec_command_end", "mcp_tool_call_begin", "mcp_tool_call_end":
		return toolUseEvent(obj, id)
	case "warning", "error":
		if text := textFrom(obj); text != "" {
			return []engine.Event{{Type: engine.EventWarning, Text: text, Metadata: obj}}, id, nil
		}
	}
	if text := textFrom(obj); text != "" && looksAssistant(obj) {
		return []engine.Event{{Type: engine.EventAgentText, Text: text, Metadata: obj}}, id, nil
	}
	return nil, id, nil
}

func modelReportedEvent(obj map[string]any, id string) []engine.Event {
	model := strings.TrimSpace(firstString(obj, "model"))
	if model == "" {
		return nil
	}
	return []engine.Event{{Type: engine.EventModelReported, ModelReported: model, Metadata: obj}}
}

func parseItemCompleted(obj map[string]any, id string) ([]engine.Event, string, error) {
	item, ok := firstMap(obj, "item", "payload", "response_item")
	if !ok {
		return nil, id, nil
	}
	itemType := strings.ToLower(firstString(item, "type"))
	switch itemType {
	case "agent_message", "agentmessage", "assistant_message", "message":
		return agentTextEvent(item, id)
	default:
		return toolUseEvent(item, id)
	}
}

func agentTextEvent(obj map[string]any, id string) ([]engine.Event, string, error) {
	if text := textFrom(obj); text != "" {
		return []engine.Event{{Type: engine.EventAgentText, Text: text, Metadata: obj}}, id, nil
	}
	return nil, id, nil
}

func toolUseEvent(obj map[string]any, id string) ([]engine.Event, string, error) {
	name := firstString(obj, "name", "tool", "tool_name", "command", "namespace")
	text := firstString(obj, "command", "arguments", "aggregated_output", "formatted_output", "output", "text")
	if text == "" {
		text = textFrom(obj)
	}
	if text == "" {
		text = name
	}
	return []engine.Event{{Type: engine.EventToolUse, Name: name, Text: text, Metadata: obj}}, id, nil
}

func looksAssistant(obj map[string]any) bool {
	return strings.EqualFold(firstString(obj, "role"), "assistant")
}

func textFrom(obj map[string]any) string {
	for _, key := range []string{"text", "message", "last_agent_message", "lastAgentMessage", "content", "delta", "output", "output_text", "input_text", "result", "error"} {
		if s := firstString(obj, key); s != "" {
			return s
		}
	}
	if nested, ok := firstMap(obj, "msg", "item", "payload", "response_item"); ok {
		return textFrom(nested)
	}
	return ""
}

func firstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := obj[key]; ok {
			switch x := v.(type) {
			case string:
				return x
			case map[string]any:
				if s := textFrom(x); s != "" {
					return s
				}
			case []any:
				var parts []string
				for _, item := range x {
					switch y := item.(type) {
					case string:
						if y != "" {
							parts = append(parts, y)
						}
					case map[string]any:
						if s := textFrom(y); s != "" {
							parts = append(parts, s)
						}
					}
				}
				return strings.Join(parts, "")
			}
		}
	}
	return ""
}

func firstMap(obj map[string]any, keys ...string) (map[string]any, bool) {
	for _, key := range keys {
		if v, ok := obj[key].(map[string]any); ok {
			return v, true
		}
	}
	return nil, false
}
