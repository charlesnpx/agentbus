package claudecli

import (
	"slices"
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
			var peers []claudePeerFunc
			for i, input := range inputs {
				peers = append(peers, func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
					if !slices.Contains(spec.Argv, "--dangerously-skip-permissions") || slices.Contains(spec.Argv, "--allowedTools") || slices.Contains(spec.Argv, "--disallowedTools") || slices.Contains(spec.Argv, "--strict-mcp-config") {
						t.Fatalf("Claude argv: %#v", spec.Argv)
					}
					if i > 0 {
						assertArgValue(t, spec.Argv, "--resume", "session")
					}
					peer := newClaudePeer(t, proc)
					peer.handshake()
					frame := peer.expectUser(input.Prompt)
					if got := nestedString(frame, "message", "content"); got != prefixes[input.Intent]+"\n\n"+input.Prompt {
						t.Fatalf("user prompt: %q", got)
					}
					peer.emitSystem("session", "model")
					peer.emitResult("success", false, "done")
				})
			}
			runner := newFakeClaudeRunner(t, peers...)
			session := startFakeClaudeSession(t, engine.SessionOpts{})
			for _, input := range inputs {
				events, err := turnWithRunner(t, session, input, runner)
				if err != nil {
					t.Fatal(err)
				}
				_ = collectEventsWithTimeout(t, events, 2*time.Second)
			}
		})
	}
}
