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
	args := []string{"exec"}
	if resumeID != "" {
		args = append(args, "resume", resumeID)
	}
	args = append(args, "--json")
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
	return args, nil
}

func parseEvent(obj map[string]any) ([]engine.Event, string, error) {
	id := firstString(obj, "session_id", "sessionId", "conversation_id", "conversationId", "id")
	typ := strings.ToLower(firstString(obj, "type", "event"))
	switch typ {
	case "agenttext", "agent_text", "assistant_text", "message", "assistant_message":
		if text := textFrom(obj); text != "" {
			return []engine.Event{{Type: engine.EventAgentText, Text: text, Metadata: obj}}, id, nil
		}
	case "tooluse", "tool_use", "tool_call", "exec_command", "function_call":
		name := firstString(obj, "name", "tool", "tool_name")
		text := textFrom(obj)
		if text == "" {
			text = name
		}
		return []engine.Event{{Type: engine.EventToolUse, Name: name, Text: text, Metadata: obj}}, id, nil
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

func looksAssistant(obj map[string]any) bool {
	return strings.EqualFold(firstString(obj, "role"), "assistant")
}

func textFrom(obj map[string]any) string {
	for _, key := range []string{"text", "message", "content", "delta", "output"} {
		if s := firstString(obj, key); s != "" {
			return s
		}
	}
	if msg, ok := obj["msg"].(map[string]any); ok {
		return textFrom(msg)
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
			}
		}
	}
	return ""
}
