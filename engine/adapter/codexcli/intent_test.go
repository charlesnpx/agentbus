package codexcli

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
			var peers []appServerPeerFunc
			for i, input := range inputs {
				peers = append(peers, func(t *testing.T, proc *fakeAppServerProcess, _ command.ExecSpec) {
					peer := newAppServerPeer(t, proc)
					peer.handshake()
					method := "thread/start"
					if i > 0 {
						method = "thread/resume"
					}
					thread := peer.expectRequest(method)
					params := thread["params"].(map[string]any)
					if params["sandbox"] != "danger-full-access" || params["approvalPolicy"] != "never" {
						t.Fatalf("%s policy: %#v", method, params)
					}
					peer.respond(thread, threadResult("thread"))
					turn := peer.expectRequest("turn/start")
					turnParams := turn["params"].(map[string]any)
					policy := turnParams["sandboxPolicy"].(map[string]any)
					items := turnParams["input"].([]any)
					text := items[0].(map[string]any)["text"]
					if policy["type"] != "dangerFullAccess" || turnParams["approvalPolicy"] != "never" || text != prefixes[input.Intent]+"\n\n"+input.Prompt {
						t.Fatalf("turn params: %#v", turnParams)
					}
					peer.respond(turn, turnResult("turn"))
					peer.notify("turn/completed", completedParams("thread", "turn", "completed", ""))
				})
			}
			runner := newFakeAppServerRunner(t, peers...)
			session := startFakeCodexSession(t, engine.SessionOpts{})
			for _, input := range inputs {
				original := input.Prompt
				events, err := turnWithRunner(t, session, input, runner)
				if err != nil {
					t.Fatal(err)
				}
				_ = collectEventsWithTimeout(t, events, 2*time.Second)
				if input.Prompt != original {
					t.Fatal("caller prompt changed")
				}
			}
		})
	}
}
