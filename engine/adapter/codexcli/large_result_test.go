package codexcli

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestCodexLargeFinalResultsPreserveBytes(t *testing.T) {
	for _, size := range []int{4097, 65537, 262144, 262145} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			want := strings.Repeat("x", size-5) + " \n雪"
			runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, _ command.ExecSpec) {
				peer := newAppServerPeer(t, proc)
				peer.handshake()
				thread := peer.expectRequest("thread/start")
				peer.respond(thread, threadResult("thread-large"))
				turn := peer.expectRequest("turn/start")
				peer.respond(turn, turnResult("turn-large"))
				peer.notify("item/agentMessage/delta", map[string]any{"itemId": "result", "delta": want})
				peer.notify("turn/completed", completedParams("thread-large", "turn-large", "completed", ""))
			})
			session := startFakeCodexSession(t, engine.SessionOpts{})
			events, err := turnWithRunner(t, session, engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			assertLargeResultOutput(t, collectEventsWithTimeout(t, events, 10*time.Second), want)
		})
	}
}

func assertLargeResultOutput(t *testing.T, events []engine.Event, want string) {
	t.Helper()
	var result *engine.Event
	for i := range events {
		if events[i].Type == engine.EventResultMessage {
			result = &events[i]
		}
	}
	if result == nil || result.RawText != want || len(result.Text) > engine.DefaultEventTextCap {
		t.Fatalf("result missing or truncated: got=%d want=%d", func() int {
			if result == nil {
				return -1
			}
			return len(result.RawText)
		}(), len(want))
	}
}
