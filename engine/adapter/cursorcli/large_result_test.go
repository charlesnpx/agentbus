package cursorcli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestCursorLargeFinalResultsPreserveBytes(t *testing.T) {
	for _, size := range []int{4097, 65537, 262144, 262145} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			want := strings.Repeat("x", size-5) + " \n雪"
			runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, _ command.ExecSpec) {
				peer := newACPPeer(t, proc)
				peer.handshake(false)
				session := peer.expectRequest("session/new")
				peer.respond(session, acpSessionResult("session-large", "model"))
				mode := peer.expectRequest("session/set_mode")
				peer.respond(mode, map[string]any{})
				prompt := peer.expectRequest("session/prompt")
				peer.notify("session/update", map[string]any{"sessionId": "session-large", "sessionUpdate": "agent_message_chunk", "content": map[string]any{"text": want}})
				peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
			})
			session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
			events, err := turnWithFakeRunner(t, session, engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			assertLargeResultArtifact(t, collectEvents(t, events, 10*time.Second), want)
		})
	}
}

func assertLargeResultArtifact(t *testing.T, events []engine.Event, want string) {
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
	info, err := engine.WriteResultForLayout(engine.WorkspaceLayout{Results: t.TempDir()}, "job_large", []byte(result.RawText), engine.DefaultInlineResultCap)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(info.ResultPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(want))
	if string(raw) != want || info.Bytes != int64(len(want)) || info.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("artifact mismatch: bytes=%d digest=%s", info.Bytes, info.SHA256)
	}
	if info.TextElided != (len(want) >= engine.DefaultInlineResultCap) {
		t.Fatalf("inline elision=%v at %d bytes", info.TextElided, len(want))
	}
}
