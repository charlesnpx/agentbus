package codexcli

import (
	"fmt"
	"strings"

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
	}
}

func buildArgs(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error) {
	args := []string{"exec", "--json"}
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
	}
	return args, nil
}

func parseEvent(obj map[string]any) ([]engine.Event, string, error) {
	id := firstString(obj, "session_id", "sessionId", "conversation_id", "conversationId", "id")
	typ := strings.ToLower(firstString(obj, "type", "event"))
	switch typ {
	case "task_started", "turn_started", "session_configured", "token_count":
		return nil, id, nil
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

func parseItemCompleted(obj map[string]any, id string) ([]engine.Event, string, error) {
	item, ok := firstMap(obj, "item", "payload", "response_item")
	if !ok {
		return nil, id, nil
	}
	itemType := strings.ToLower(firstString(item, "type"))
	switch itemType {
	case "agent_message", "agentmessage", "assistant_message", "message":
		return agentTextEvent(item, id)
	case "local_shell_call", "exec_command", "tool_call", "function_call", "custom_tool_call", "mcp_tool_call":
		return toolUseEvent(item, id)
	default:
		return nil, id, nil
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
