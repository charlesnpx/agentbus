package cursorcli

import (
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

const correctionFixture = "The preceding final result did not satisfy the required JSON Schema. Return a\n" +
	"replacement final result that satisfies it. This is the one permitted\n" +
	"correction attempt. It is read-only: make no further changes. Do not edit\n" +
	"files, write data, invoke tools, or alter the workspace or external systems.\n\n" +
	"Schema:\n{\"type\":\"object\"}\n\nSchema violations:\ninvalid"

func TestIntentPolicyInitialResumeAndCorrection(t *testing.T) {
	prefixes := map[engine.Intent]string{
		engine.IntentInspect: "Inspect the task and report your findings. Do not edit project source or configuration. You may run commands that produce ordinary build and test artifacts, use caches and temporary files, and start local services needed for the task. Write requested evidence artifacts only where directed.",
		engine.IntentEdit:    "Make the changes requested by the task. Keep source, configuration, and other changes within the task's scope.",
	}
	for _, intent := range []engine.Intent{engine.IntentInspect, engine.IntentEdit} {
		t.Run(string(intent), func(t *testing.T) {
			inputs := []engine.TurnInput{{Intent: intent, Prompt: "initial"}, {Intent: intent, Prompt: "resume"}, {Intent: engine.IntentInspect, Prompt: correctionFixture}}
			var peers []acpPeerFunc
			for i, input := range inputs {
				peers = append(peers, func(t *testing.T, proc *fakeACPProcess, _ command.ExecSpec) {
					peer := newACPPeer(t, proc)
					peer.handshake(true)
					method := "session/new"
					if i > 0 {
						method = "session/load"
					}
					start := peer.expectRequest(method)
					peer.respond(start, acpSessionResult("session", ""))
					mode := peer.expectRequest("session/set_mode")
					if got := nestedString(mode, "params", "modeId"); got != "agent" {
						t.Fatalf("mode: %q", got)
					}
					peer.respond(mode, map[string]any{})
					prompt := peer.expectRequest("session/prompt")
					if got := acpPromptText(prompt); got != prefixes[input.Intent]+"\n\n"+input.Prompt {
						t.Fatalf("prompt: %q", got)
					}
					peer.serverRequest("permission", "session/request_permission", map[string]any{"options": []any{map[string]any{"optionId": "allow", "kind": "allow_once"}}})
					if got := nestedString(peer.expectResponse("permission"), "result", "outcome", "optionId"); got != "allow" {
						t.Fatalf("permission: %q", got)
					}
					peer.notify("session/update", map[string]any{"sessionId": "session", "sessionUpdate": "agent_message_chunk", "content": map[string]any{"text": "done"}})
					peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
				})
			}
			runner := newFakeACPRunner(t, peers...)
			session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
			for _, input := range inputs {
				events, err := turnWithFakeRunner(t, session, input, runner)
				if err != nil {
					t.Fatal(err)
				}
				_ = collectEvents(t, events, 2*time.Second)
			}
		})
	}
}
