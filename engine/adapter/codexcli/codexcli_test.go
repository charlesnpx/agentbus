package codexcli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestStaticDefaultEffortsAcceptMaxAndUltra(t *testing.T) {
	for _, effort := range []string{"max", "ultra"} {
		t.Run(effort, func(t *testing.T) {
			backend := New(Options{Binary: "fake-codex"})
			if _, err := backend.Start(context.Background(), engine.SessionOpts{Effort: effort}); err != nil {
				t.Fatalf("Start() with static default effort %q: %v", effort, err)
			}
		})
	}
}

func TestAppServerInitializeHandshakePrecedesTurnAndFailureIsTerminal(t *testing.T) {
	t.Run("handshake order", func(t *testing.T) {
		cwd := t.TempDir()
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			init := peer.expectRequest("initialize")
			peer.respond(init, initializeResult())
			peer.expectNotification("initialized")
			thread := peer.expectRequest("thread/start")
			peer.respond(thread, threadResult("thread-1"))
			turn := peer.expectRequest("turn/start")
			peer.respond(turn, turnResult("turn-1"))
			peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
		})

		session := startFakeCodexSession(t, engine.SessionOpts{CWD: cwd, Model: "gpt-5", Effort: "high"})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
		if err != nil {
			t.Fatal(err)
		}
		_ = collectEventsWithTimeout(t, events, 2*time.Second)
		spec := runner.lastSpec()
		if strings.Join(spec.Argv, "\x00") != "fake-codex\x00app-server" {
			t.Fatalf("argv = %#v", spec.Argv)
		}
		if spec.Dir != cwd {
			t.Fatalf("dir = %q, want %q", spec.Dir, cwd)
		}
	})

	t.Run("initialize failure", func(t *testing.T) {
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			init := peer.expectRequest("initialize")
			peer.respondError(init, "initialize failed")
		})

		session := startFakeCodexSession(t, engine.SessionOpts{})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
		if err != nil {
			t.Fatal(err)
		}
		got := collectEventsWithTimeout(t, events, 2*time.Second)
		if !containsEvent(got, engine.EventTerminalError, "initialize failed") {
			t.Fatalf("events = %#v, want terminal initialize error", got)
		}
	})
}

func TestAppServerOversizedTerminalFrameFailsWithTransportClass(t *testing.T) {
	t.Setenv("AGENTBUS_BACKEND_JSON_LINE_BYTES", "512")
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		init := peer.expectRequest("initialize")
		peer.respond(init, initializeResult())
		peer.expectNotification("initialized")
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		payload := `{"method":"turn/completed","params":{"status":"completed","padding":"` + strings.Repeat("x", 1024) + `"}}` + "\n"
		if _, err := io.WriteString(proc.stdoutW, payload); err != nil {
			t.Fatal(err)
		}
		// A later valid completion must not race ahead of the discarded terminal
		// frame and turn this transport failure into a false success.
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	found := false
	dropsRecorded := false
	for _, event := range got {
		if event.Type == engine.EventWarning {
			if drops, ok := engine.TransportFrameDropsFromMetadata(event.Metadata); ok && drops.Count == 1 && drops.Bytes > 512 {
				dropsRecorded = true
			}
		}
		if event.Type == engine.EventTerminalError && errors.Is(event.Err, engine.ErrTransportFrameTooLarge) {
			found = true
		}
	}
	if !found {
		t.Fatalf("events = %#v, want transport frame terminal error", got)
	}
	if !dropsRecorded {
		t.Fatalf("events = %#v, want dropped-frame metadata before terminal error", got)
	}
}

func TestAppServerSkipsOversizedFileChangeFrame(t *testing.T) {
	t.Setenv("AGENTBUS_BACKEND_JSON_LINE_BYTES", "512")
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		init := peer.expectRequest("initialize")
		peer.respond(init, initializeResult())
		peer.expectNotification("initialized")
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		payload := `{"method":"item/completed","params":{"item":{"type":"fileChange","diff":"` + strings.Repeat("x", 1024) + `"}}}` + "\n"
		if _, err := io.WriteString(proc.stdoutW, payload); err != nil {
			t.Fatal(err)
		}
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if containsEvent(got, engine.EventTerminalError, "transport") {
		t.Fatalf("events = %#v, want oversized fileChange skipped", got)
	}
	if resultText(got) != "" {
		t.Fatalf("result text = %q, want empty completed turn", resultText(got))
	}
	foundDrops := false
	for _, event := range got {
		if drops, ok := engine.TransportFrameDropsFromMetadata(event.Metadata); ok && drops.Count == 1 && drops.Bytes > 512 && drops.RedactedPrefix == "method=item/completed" {
			foundDrops = true
		}
	}
	if !foundDrops {
		t.Fatalf("events = %#v, want persisted dropped-frame warning", got)
	}
}

func TestAppServerOversizedDuplicateDiscriminatorFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{
			name:    "top-level method",
			payload: `{"method":"warning","padding":"` + strings.Repeat("x", 1024) + `","method":"turn/completed","params":{"status":"completed"}}` + "\n",
		},
		{
			name:    "nested item type",
			payload: `{"method":"item/completed","params":{"item":{"type":"fileChange","diff":"` + strings.Repeat("x", 1024) + `","type":"agentMessage"}}}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("AGENTBUS_BACKEND_JSON_LINE_BYTES", "512")
			runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
				peer := newAppServerPeer(t, proc)
				init := peer.expectRequest("initialize")
				peer.respond(init, initializeResult())
				peer.expectNotification("initialized")
				thread := peer.expectRequest("thread/start")
				peer.respond(thread, threadResult("thread-1"))
				turn := peer.expectRequest("turn/start")
				peer.respond(turn, turnResult("turn-1"))
				if _, err := io.WriteString(proc.stdoutW, test.payload); err != nil {
					t.Fatal(err)
				}
				// A later completion makes a skip observable as a false success.
				peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
			})

			session := startFakeCodexSession(t, engine.SessionOpts{})
			events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			got := collectEventsWithTimeout(t, events, 2*time.Second)
			if !containsTransportFrameError(got) {
				t.Fatalf("events = %#v, want duplicate discriminator transport error", got)
			}
		})
	}
}

func containsTransportFrameError(events []engine.Event) bool {
	for _, event := range events {
		if event.Type == engine.EventTerminalError && errors.Is(event.Err, engine.ErrTransportFrameTooLarge) {
			return true
		}
	}
	return false
}

func TestAppServerTurnSandboxPolicyFollowsWritePolicy(t *testing.T) {
	cwd := t.TempDir()
	tests := []struct {
		name        string
		writePolicy WritePolicy
		write       bool
		want        map[string]any
	}{
		{
			name:  "zero value write remains workspace offline",
			write: true,
			want: map[string]any{
				"type":          "workspaceWrite",
				"networkAccess": false,
				"writableRoots": []any{cwd},
			},
		},
		{
			name:        "workspace network write enables network",
			writePolicy: WritePolicyWorkspaceNetwork,
			write:       true,
			want: map[string]any{
				"type":          "workspaceWrite",
				"networkAccess": true,
				"writableRoots": []any{cwd},
			},
		},
		{
			name:        "trusted write uses unrestricted policy",
			writePolicy: WritePolicyTrusted,
			write:       true,
			want: map[string]any{
				"type": "dangerFullAccess",
			},
		},
		{
			name:  "zero value read is read only offline",
			write: false,
			want: map[string]any{
				"type":          "readOnly",
				"networkAccess": false,
			},
		},
		{
			name:        "workspace network read is read only offline",
			writePolicy: WritePolicyWorkspaceNetwork,
			write:       false,
			want: map[string]any{
				"type":          "readOnly",
				"networkAccess": false,
			},
		},
		{
			name:        "trusted read is read only offline",
			writePolicy: WritePolicyTrusted,
			write:       false,
			want: map[string]any{
				"type":          "readOnly",
				"networkAccess": false,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
				peer := newAppServerPeer(t, proc)
				peer.handshake()
				thread := peer.expectRequest("thread/start")
				peer.respond(thread, threadResult("thread-1"))
				turn := peer.expectRequest("turn/start")
				params, ok := turn["params"].(map[string]any)
				if !ok {
					t.Fatalf("turn/start params = %#v", turn["params"])
				}
				got, ok := params["sandboxPolicy"].(map[string]any)
				if !ok {
					t.Fatalf("turn/start sandboxPolicy = %#v", params["sandboxPolicy"])
				}
				if !reflect.DeepEqual(got, test.want) {
					t.Fatalf("turn/start sandboxPolicy = %#v, want %#v", got, test.want)
				}
				peer.respond(turn, turnResult("turn-1"))
				peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
			})

			session := startFakeCodexSessionWithOptions(t, Options{
				Binary:      "fake-codex",
				WritePolicy: test.writePolicy,
			}, engine.SessionOpts{CWD: cwd})
			events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello", Write: test.write}, runner)
			if err != nil {
				t.Fatal(err)
			}
			_ = collectEventsWithTimeout(t, events, 2*time.Second)
		})
	}
}

func TestAppServerThreadsStartThenResumeWithReturnedID(t *testing.T) {
	runner := newFakeAppServerRunner(t,
		func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			thread := peer.expectRequest("thread/start")
			if _, ok := thread["params"].(map[string]any)["threadId"]; ok {
				t.Fatalf("thread/start params = %#v, did not want threadId", thread["params"])
			}
			peer.respond(thread, threadResult("thread-1"))
			turn := peer.expectRequest("turn/start")
			assertParam(t, turn, "threadId", "thread-1")
			peer.respond(turn, turnResult("turn-1"))
			peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
		},
		func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			thread := peer.expectRequest("thread/resume")
			assertParam(t, thread, "threadId", "thread-1")
			peer.respond(thread, threadResult("thread-1"))
			turn := peer.expectRequest("turn/start")
			assertParam(t, turn, "threadId", "thread-1")
			peer.respond(turn, turnResult("turn-2"))
			peer.notify("turn/completed", completedParams("thread-1", "turn-2", "completed", ""))
		},
	)

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	if session.ID() != "thread-1" {
		t.Fatalf("session id after first turn = %q, want thread-1", session.ID())
	}

	events, err = turnWithRunner(t, session, engine.TurnInput{Prompt: "second"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	if session.ID() != "thread-1" {
		t.Fatalf("session id after resume = %q, want thread-1", session.ID())
	}
}

func TestAppServerCompletedTurnUsesLastCompletedAgentMessageAsResult(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1", "delta": "streamed "})
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-1", "type": "agentMessage", "text": "streamed "}))
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-2", "type": "agentMessage", "text": "final answer"}))
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "final answer" {
		t.Fatalf("events = %#v, want authoritative final answer result", got)
	}
	if resultRawText(got) != "final answer" {
		t.Fatalf("events = %#v, want authoritative final answer raw result", got)
	}
	if agentText := agentTextTexts(got); !reflect.DeepEqual(agentText, []string{"streamed ", "final answer"}) {
		t.Fatalf("agent text = %#v, want streamed delta once and completed text without prior delta once", agentText)
	}
}

func TestAppServerCompletedTurnUsesCompletedTextAfterMismatchedAgentDelta(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1", "delta": "streamed "})
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-1", "type": "agentMessage", "text": "draft"}))
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "draft" {
		t.Fatalf("events = %#v, want authoritative completed result", got)
	}
	if resultRawText(got) != "draft" {
		t.Fatalf("events = %#v, want authoritative completed raw result", got)
	}
	if agentText := agentTextTexts(got); !reflect.DeepEqual(agentText, []string{"streamed "}) {
		t.Fatalf("agent text = %#v, want streamed delta only", agentText)
	}
}

func TestAppServerCompletedTurnUsesAccumulatedAgentMessageDeltasAsResult(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1", "delta": "Hello, "})
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-1", "delta": "world"})
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "Hello, world" {
		t.Fatalf("events = %#v, want accumulated delta result", got)
	}
	if resultRawText(got) != "Hello, world" {
		t.Fatalf("events = %#v, want accumulated delta raw result", got)
	}
	if agentText := agentTextTexts(got); !reflect.DeepEqual(agentText, []string{"Hello, ", "world"}) {
		t.Fatalf("agent text = %#v, want accumulated delta chunks", agentText)
	}
}

func TestAppServerCompletedTurnUsesLastDeltaAgentMessageItemAsResult(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-a", "delta": "Check"})
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-a", "delta": "ing..."})
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-b", "delta": "Done"})
		peer.notify("item/agentMessage/delta", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "item-b", "delta": "."})
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "Done." {
		t.Fatalf("events = %#v, want last delta agent message item result", got)
	}
}

func TestAppServerTaskCompleteUsesLastAgentMessageAsResult(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("task_complete", map[string]any{
			"turn_id":            "turn-1",
			"last_agent_message": "done",
			"error":              nil,
		})
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "done" {
		t.Fatalf("events = %#v, want task_complete last agent message result", got)
	}
	if resultRawText(got) != "done" {
		t.Fatalf("events = %#v, want task_complete last agent message raw result", got)
	}
	if session.ID() != "thread-1" {
		t.Fatalf("session id after task_complete = %q, want thread-1", session.ID())
	}
}

func TestAppServerTerminalTurnStatuses(t *testing.T) {
	tests := []struct {
		name         string
		status       string
		errorText    string
		errorInfo    string
		want         string
		interrupted  bool
		overloaded   bool
		taskComplete bool
	}{
		{name: "failed", status: "failed", errorText: "model failed", want: "model failed"},
		{
			name:         "camel-case structured server overloaded task completion",
			errorText:    "Selected model is at capacity. Please try a different model.",
			errorInfo:    "serverOverloaded",
			want:         "Selected model is at capacity. Please try a different model.",
			overloaded:   true,
			taskComplete: true,
		},
		{
			name:       "message-only overloaded completion",
			errorText:  "Selected model is at capacity. Please try a different model.",
			want:       "Selected model is at capacity. Please try a different model.",
			overloaded: true,
		},
		{
			name:       "unrecognized structured code falls through to capacity message",
			errorText:  "Selected model is at capacity. Please try a different model.",
			errorInfo:  "someOtherCode",
			want:       "Selected model is at capacity. Please try a different model.",
			overloaded: true,
		},
		{
			name:      "unrecognized structured code leaves unrelated message alone",
			errorText: "model failed",
			errorInfo: "someOtherCode",
			want:      "model failed",
		},
		{name: "unrequested interrupted", status: "interrupted", want: "interrupted", interrupted: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
				peer := newAppServerPeer(t, proc)
				peer.handshake()
				thread := peer.expectRequest("thread/start")
				peer.respond(thread, threadResult("thread-1"))
				turn := peer.expectRequest("turn/start")
				peer.respond(turn, turnResult("turn-1"))
				method := "turn/completed"
				params := completedParamsWithErrorInfo("thread-1", "turn-1", test.status, test.errorText, test.errorInfo)
				if test.taskComplete {
					method = "task_complete"
					params = taskCompleteParams("turn-1", test.errorText, test.errorInfo)
				}
				peer.notify(method, params)
			})

			session := startFakeCodexSession(t, engine.SessionOpts{})
			events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			got := collectEventsWithTimeout(t, events, 2*time.Second)
			if !containsEvent(got, engine.EventTerminalError, test.want) {
				t.Fatalf("events = %#v, want terminal error containing %q", got, test.want)
			}
			for _, event := range got {
				if event.Type != engine.EventTerminalError {
					continue
				}
				if test.interrupted && !errors.Is(event.Err, engine.ErrTurnInterrupted) {
					t.Fatalf("terminal event error = %v, want ErrTurnInterrupted", event.Err)
				}
				if gotOverload := errors.Is(event.Err, engine.ErrProviderOverloaded); gotOverload != test.overloaded {
					t.Fatalf("terminal event error = %v, provider overloaded = %t, want %t", event.Err, gotOverload, test.overloaded)
				}
			}
			if resultText(got) != "" {
				t.Fatalf("events = %#v, did not want fabricated success result", got)
			}
		})
	}
}

func TestAppServerProviderOverloadIgnoresTerminalItemInventory(t *testing.T) {
	const capacityMessage = "Selected model is at capacity. Please try a different model."
	tests := []struct {
		name          string
		notifyItems   []map[string]any
		configureTurn func(map[string]any)
	}{
		{
			name: "items absent",
			configureTurn: func(turn map[string]any) {
				delete(turn, "items")
			},
		},
		{
			name: "items empty",
		},
		{
			name: "items null",
			configureTurn: func(turn map[string]any) {
				turn["items"] = nil
			},
		},
		{
			name: "unknown and malformed terminal items",
			configureTurn: func(turn map[string]any) {
				turn["items"] = []any{
					map[string]any{"id": "unknown-1", "type": "newItemKind"},
					map[string]any{"type": "agentMessage"},
					"not an item object",
				}
			},
		},
		{
			name: "observed tool notification",
			notifyItems: []map[string]any{
				{"id": "command-1", "type": "commandExecution", "command": "echo wrote"},
				{"id": "file-1", "type": "fileChange", "changes": "wrote a file"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
				peer := newAppServerPeer(t, proc)
				peer.handshake()
				thread := peer.expectRequest("thread/start")
				peer.respond(thread, threadResult("thread-1"))
				turn := peer.expectRequest("turn/start")
				peer.respond(turn, turnResult("turn-1"))
				for index, item := range test.notifyItems {
					method := "item/started"
					if index > 0 {
						method = "item/completed"
					}
					peer.notify(method, itemParams("thread-1", "turn-1", item))
				}
				params := completedParamsWithErrorInfo("thread-1", "turn-1", "failed", capacityMessage, "server_overloaded")
				if test.configureTurn != nil {
					turn, ok := params["turn"].(map[string]any)
					if !ok {
						t.Fatal("completed params missing turn")
					}
					test.configureTurn(turn)
				}
				peer.notify("turn/completed", params)
			})

			session := startFakeCodexSession(t, engine.SessionOpts{})
			events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			got := collectEventsWithTimeout(t, events, 2*time.Second)
			if !containsEvent(got, engine.EventTerminalError, capacityMessage) {
				t.Fatalf("events = %#v, want terminal error containing %q", got, capacityMessage)
			}
			for _, event := range got {
				if event.Type == engine.EventTerminalError && !errors.Is(event.Err, engine.ErrProviderOverloaded) {
					t.Fatalf("terminal event error = %v, want ErrProviderOverloaded", event.Err)
				}
			}
		})
	}
}

func TestAppServerCompletedFileChangeReportsObservedWorkspaceWriteItem(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("item/started", itemParams("thread-1", "turn-1", map[string]any{"id": "change-started", "type": "fileChange"}))
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "command", "type": "commandExecution"}))
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "change-completed", "type": "fileChange"}))
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	var observed int
	for _, event := range got {
		if event.ObservedWorkspaceWriteItem {
			observed++
		}
	}
	if observed != 1 {
		t.Fatalf("observed workspace-write events = %d, want 1; events = %#v", observed, got)
	}
}

func TestAppServerAnswersServerRequestsAndContinues(t *testing.T) {
	t.Run("known approval and elicitation requests", func(t *testing.T) {
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			thread := peer.expectRequest("thread/start")
			peer.respond(thread, threadResult("thread-1"))
			turn := peer.expectRequest("turn/start")
			peer.respond(turn, turnResult("turn-1"))

			peer.serverRequest("srv-approval", "item/commandExecution/requestApproval", map[string]any{"threadId": "thread-1", "turnId": "turn-1", "itemId": "cmd-1"})
			approval := peer.expectResponse("srv-approval")
			if got := nestedString(approval, "result", "decision"); got != "decline" {
				t.Fatalf("approval response = %#v, want decision decline", approval)
			}

			peer.serverRequest("srv-elicitation", "mcpServer/elicitation/request", map[string]any{"threadId": "thread-1", "serverName": "mcp", "mode": "form", "message": "question"})
			elicitation := peer.expectResponse("srv-elicitation")
			if got := nestedString(elicitation, "result", "action"); got != "decline" {
				t.Fatalf("elicitation response = %#v, want action decline", elicitation)
			}

			peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-1", "type": "agentMessage", "text": "done"}))
			peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
		})

		session := startFakeCodexSession(t, engine.SessionOpts{})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
		if err != nil {
			t.Fatal(err)
		}
		got := collectEventsWithTimeout(t, events, 2*time.Second)
		if resultText(got) != "done" {
			t.Fatalf("events = %#v, want completed turn after answered requests", got)
		}
	})

	t.Run("unsupported server request", func(t *testing.T) {
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			thread := peer.expectRequest("thread/start")
			peer.respond(thread, threadResult("thread-1"))
			turn := peer.expectRequest("turn/start")
			peer.respond(turn, turnResult("turn-1"))

			peer.serverRequest("srv-unsupported", "unknown/request", map[string]any{})
			response := peer.expectResponse("srv-unsupported")
			if got := nestedString(response, "error", "message"); !strings.Contains(got, "unsupported server request") {
				t.Fatalf("unsupported response = %#v", response)
			}
			peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-1", "type": "agentMessage", "text": "done"}))
			peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
		})

		session := startFakeCodexSession(t, engine.SessionOpts{})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
		if err != nil {
			t.Fatal(err)
		}
		got := collectEventsWithTimeout(t, events, 2*time.Second)
		if resultText(got) != "done" {
			t.Fatalf("events = %#v, want completion after unsupported request error response", got)
		}
	})
}

func TestAppServerUnknownNotificationIsIgnored(t *testing.T) {
	runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
		peer := newAppServerPeer(t, proc)
		peer.handshake()
		thread := peer.expectRequest("thread/start")
		peer.respond(thread, threadResult("thread-1"))
		turn := peer.expectRequest("turn/start")
		peer.respond(turn, turnResult("turn-1"))
		peer.notify("new/notification", map[string]any{"unexpected": true})
		peer.notify("item/completed", itemParams("thread-1", "turn-1", map[string]any{"id": "item-1", "type": "agentMessage", "text": "still done"}))
		peer.notify("turn/completed", completedParams("thread-1", "turn-1", "completed", ""))
	})

	session := startFakeCodexSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if resultText(got) != "still done" {
		t.Fatalf("events = %#v, want completion despite unknown notification", got)
	}
}

func TestAppServerSetupQualifyDiscoversModelsAndRequiresOne(t *testing.T) {
	t.Run("discovers model list", func(t *testing.T) {
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			req := peer.expectRequest("model/list")
			peer.respond(req, map[string]any{
				"data": []any{
					map[string]any{
						"model": "gpt-5.4",
						"supportedReasoningEfforts": []any{
							map[string]any{"reasoningEffort": "high"},
							map[string]any{"reasoningEffort": "low"},
						},
					},
					map[string]any{
						"id":                        "id-only-model",
						"supportedReasoningEfforts": []any{"xhigh"},
					},
				},
			})
		})
		driver := newAppServerDriver("fake-codex", WritePolicyWorkspaceOffline)
		discovery, err := driver.SetupQualify(context.Background(), runner, engine.SessionOpts{CWD: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(discovery.Models, ","); got != "gpt-5.4,id-only-model" {
			t.Fatalf("models = %q", got)
		}
		if got := strings.Join(discovery.Efforts, ","); got != "low,high,xhigh" {
			t.Fatalf("efforts = %q", got)
		}
		if discovery.Source != "app-server" || discovery.FetchedAt == "" {
			t.Fatalf("discovery provenance = %#v", discovery)
		}
	})

	t.Run("zero models fails", func(t *testing.T) {
		runner := newFakeAppServerRunner(t, func(t *testing.T, proc *fakeAppServerProcess, spec command.ExecSpec) {
			peer := newAppServerPeer(t, proc)
			peer.handshake()
			req := peer.expectRequest("model/list")
			peer.respond(req, map[string]any{"data": []any{}})
		})
		driver := newAppServerDriver("fake-codex", WritePolicyWorkspaceOffline)
		discovery, err := driver.SetupQualify(context.Background(), runner, engine.SessionOpts{CWD: t.TempDir()})
		if err == nil || !strings.Contains(err.Error(), "no usable models") || len(discovery.Models) != 0 {
			t.Fatalf("discovery=%#v err=%v, want zero-model qualification failure", discovery, err)
		}
	})
}

func TestCodexPreflightUsesAppServerStreamSchema(t *testing.T) {
	fake := fakeVersionCodex(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	health, err := backend.Preflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.StreamSchema != "codex-appserver-v1" {
		t.Fatalf("health = %#v", health)
	}
}

func startFakeCodexSession(t *testing.T, opts engine.SessionOpts) engine.Session {
	t.Helper()
	return startFakeCodexSessionWithOptions(t, Options{Binary: "fake-codex"}, opts)
}

func startFakeCodexSessionWithOptions(t *testing.T, adapterOpts Options, opts engine.SessionOpts) engine.Session {
	t.Helper()
	backend := New(adapterOpts)
	session, err := backend.Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func turnWithRunner(t *testing.T, session engine.Session, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	t.Helper()
	turner, ok := session.(interface {
		TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
	})
	if !ok {
		t.Fatal("session does not support TurnWithRunner")
	}
	return turner.TurnWithRunner(context.Background(), input, runner)
}

type appServerPeerFunc func(*testing.T, *fakeAppServerProcess, command.ExecSpec)

type fakeAppServerRunner struct {
	t *testing.T

	mu    sync.Mutex
	peers []appServerPeerFunc
	specs []command.ExecSpec
}

func newFakeAppServerRunner(t *testing.T, peers ...appServerPeerFunc) *fakeAppServerRunner {
	t.Helper()
	return &fakeAppServerRunner{t: t, peers: append([]appServerPeerFunc(nil), peers...)}
}

func (r *fakeAppServerRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.mu.Lock()
	if len(r.peers) == 0 {
		r.mu.Unlock()
		return nil, errors.New("unexpected fake app-server start")
	}
	peer := r.peers[0]
	r.peers = r.peers[1:]
	r.specs = append(r.specs, spec)
	r.mu.Unlock()

	proc := newFakeAppServerProcess()
	go func() {
		defer proc.closeOutputs()
		defer proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
		peer(r.t, proc, spec)
	}()
	return proc, nil
}

func (r *fakeAppServerRunner) lastSpec() command.ExecSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.specs) == 0 {
		return command.ExecSpec{}
	}
	return r.specs[len(r.specs)-1]
}

type fakeAppServerProcess struct {
	stdinR  *io.PipeReader
	stdinW  *trackedPipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	waitCh      chan struct{}
	finishOnce  sync.Once
	outputsOnce sync.Once
	exit        command.ExitObservation
	waitErr     error
	interrupts  atomic.Int32
	stdinClosed atomic.Bool
}

func newFakeAppServerProcess() *fakeAppServerProcess {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	proc := &fakeAppServerProcess{
		stdinR:  stdinR,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
		waitCh:  make(chan struct{}),
	}
	proc.stdinW = &trackedPipeWriter{PipeWriter: stdinW, closed: &proc.stdinClosed}
	return proc
}

func (p *fakeAppServerProcess) Stdin() io.WriteCloser {
	return p.stdinW
}

func (p *fakeAppServerProcess) Stdout() io.ReadCloser {
	return p.stdoutR
}

func (p *fakeAppServerProcess) Stderr() io.ReadCloser {
	return p.stderrR
}

func (p *fakeAppServerProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-p.waitCh:
		return p.exit, p.waitErr
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (p *fakeAppServerProcess) Interrupt(context.Context) error {
	p.interrupts.Add(1)
	return nil
}

func (p *fakeAppServerProcess) finish(exit command.ExitObservation, err error) {
	p.finishOnce.Do(func() {
		p.exit = exit
		p.waitErr = err
		close(p.waitCh)
	})
}

func (p *fakeAppServerProcess) closeOutputs() {
	p.outputsOnce.Do(func() {
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
	})
}

type trackedPipeWriter struct {
	*io.PipeWriter
	closed *atomic.Bool
}

func (w *trackedPipeWriter) Close() error {
	w.closed.Store(true)
	return w.PipeWriter.Close()
}

type appServerPeer struct {
	t       *testing.T
	proc    *fakeAppServerProcess
	scanner *bufio.Scanner
}

func newAppServerPeer(t *testing.T, proc *fakeAppServerProcess) *appServerPeer {
	t.Helper()
	return &appServerPeer{
		t:       t,
		proc:    proc,
		scanner: bufio.NewScanner(proc.stdinR),
	}
}

func (p *appServerPeer) handshake() {
	init := p.expectRequest("initialize")
	p.respond(init, initializeResult())
	p.expectNotification("initialized")
}

func (p *appServerPeer) expectRequest(method string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "method"); got != method {
		p.t.Fatalf("client method = %q, want %q in frame %#v", got, method, frame)
	}
	if _, ok := frame["id"]; !ok {
		p.t.Fatalf("%s frame missing id: %#v", method, frame)
	}
	return frame
}

func (p *appServerPeer) expectNotification(method string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "method"); got != method {
		p.t.Fatalf("client notification method = %q, want %q in frame %#v", got, method, frame)
	}
	if _, ok := frame["id"]; ok {
		p.t.Fatalf("%s notification unexpectedly has id: %#v", method, frame)
	}
	return frame
}

func (p *appServerPeer) expectResponse(id string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := requestIDKey(frame["id"]); got != id {
		p.t.Fatalf("response id = %q, want %q in frame %#v", got, id, frame)
	}
	if _, hasMethod := frame["method"]; hasMethod {
		p.t.Fatalf("response has method: %#v", frame)
	}
	return frame
}

func (p *appServerPeer) respond(request map[string]any, result any) {
	p.write(map[string]any{"id": request["id"], "result": result})
}

func (p *appServerPeer) respondError(request map[string]any, message string) {
	p.write(map[string]any{
		"id": request["id"],
		"error": map[string]any{
			"code":    -32000,
			"message": message,
		},
	})
}

func (p *appServerPeer) notify(method string, params map[string]any) {
	p.write(map[string]any{"method": method, "params": params})
}

func (p *appServerPeer) serverRequest(id, method string, params map[string]any) {
	p.write(map[string]any{"id": id, "method": method, "params": params})
}

func (p *appServerPeer) readFrame() map[string]any {
	p.t.Helper()
	if !p.scanner.Scan() {
		p.t.Fatalf("missing client frame: %v", p.scanner.Err())
	}
	var frame map[string]any
	if err := json.Unmarshal(p.scanner.Bytes(), &frame); err != nil {
		p.t.Fatalf("decode client frame %q: %v", p.scanner.Text(), err)
	}
	return frame
}

func (p *appServerPeer) write(v any) {
	p.t.Helper()
	payload, err := json.Marshal(v)
	if err != nil {
		p.t.Fatal(err)
	}
	payload = append(payload, '\n')
	if _, err := p.proc.stdoutW.Write(payload); err != nil {
		p.t.Fatalf("write server frame: %v", err)
	}
}

func initializeResult() map[string]any {
	return map[string]any{
		"codexHome":      "/tmp/codex-home",
		"platformFamily": "unix",
		"platformOs":     "macos",
		"userAgent":      "codex-cli/0.143.0",
	}
}

func threadResult(id string) map[string]any {
	return map[string]any{
		"thread": map[string]any{
			"id":            id,
			"sessionId":     id,
			"cwd":           "/tmp/work",
			"createdAt":     float64(1),
			"updatedAt":     float64(1),
			"cliVersion":    "0.143.0",
			"modelProvider": "openai",
			"preview":       "",
			"source":        "app-server",
			"status":        "running",
			"turns":         []any{},
			"ephemeral":     false,
		},
	}
}

func turnResult(id string) map[string]any {
	return map[string]any{
		"turn": map[string]any{
			"id":     id,
			"items":  []any{},
			"status": "inProgress",
		},
	}
}

func completedParams(threadID, turnID, status, errorText string) map[string]any {
	return completedParamsWithErrorInfo(threadID, turnID, status, errorText, "")
}

func completedParamsWithErrorInfo(threadID, turnID, status, errorText, errorInfo string) map[string]any {
	turn := map[string]any{
		"id":     turnID,
		"items":  []any{},
		"status": status,
	}
	if errorText != "" {
		err := map[string]any{"message": errorText}
		if errorInfo != "" {
			err["codexErrorInfo"] = errorInfo
		}
		turn["error"] = err
	}
	return map[string]any{"threadId": threadID, "turn": turn}
}

func taskCompleteParams(turnID, errorText, errorInfo string) map[string]any {
	err := map[string]any{"message": errorText}
	if errorInfo != "" {
		err["codexErrorInfo"] = errorInfo
	}
	return map[string]any{
		"turn_id":            turnID,
		"last_agent_message": nil,
		"error":              err,
	}
}

func itemParams(threadID, turnID string, item map[string]any) map[string]any {
	return map[string]any{
		"threadId": threadID,
		"turnId":   turnID,
		"item":     item,
	}
}

func assertParam(t *testing.T, frame map[string]any, key, want string) {
	t.Helper()
	params, ok := frame["params"].(map[string]any)
	if !ok {
		t.Fatalf("frame params = %#v", frame["params"])
	}
	if got := firstString(params, key); got != want {
		t.Fatalf("params[%s] = %q, want %q in %#v", key, got, want, params)
	}
}

func nestedString(obj map[string]any, path ...string) string {
	current := obj
	for i, key := range path {
		if i == len(path)-1 {
			return firstString(current, key)
		}
		next, _ := current[key].(map[string]any)
		current = next
	}
	return ""
}

func collectEventsWithTimeout(t *testing.T, ch <-chan engine.Event, timeout time.Duration) []engine.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var out []engine.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timer.C:
			t.Fatalf("timed out collecting events after %s; collected %#v", timeout, out)
		}
	}
}

func containsEvent(events []engine.Event, typ, sub string) bool {
	for _, ev := range events {
		if ev.Type == typ && strings.Contains(ev.Text, sub) {
			return true
		}
	}
	return false
}

func agentTextTexts(events []engine.Event) []string {
	var texts []string
	for _, ev := range events {
		if ev.Type == engine.EventAgentText {
			texts = append(texts, ev.Text)
		}
	}
	return texts
}

func resultText(events []engine.Event) string {
	var out string
	for _, ev := range events {
		if ev.Type == engine.EventResultMessage {
			out = ev.Text
		}
	}
	return out
}

func resultRawText(events []engine.Event) string {
	var out string
	for _, ev := range events {
		if ev.Type == engine.EventResultMessage {
			out = ev.RawText
		}
	}
	return out
}

type fakeVersionCLI struct {
	bin   string
	cache string
}

func fakeVersionCodex(t *testing.T) fakeVersionCLI {
	t.Helper()
	dir := t.TempDir()
	fake := fakeVersionCLI{
		bin:   filepath.Join(dir, "fakecodex"),
		cache: filepath.Join(dir, "setup-probes.json"),
	}
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'codex-cli 0.143.0\n'
  exit 0
fi
exit 1
`
	if err := os.WriteFile(fake.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	writeCache(t, fake.cache, "codex", fake.bin, MinimumKnownGoodVersion, StreamSchema)
	return fake
}

func writeCache(t *testing.T, path, backend, bin, version, schema string) {
	t.Helper()
	raw, err := json.Marshal(engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion, Backends: []engine.BackendSetupProbe{{
		Backend:          backend,
		BinaryPath:       bin,
		Version:          version,
		StreamSchema:     schema,
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
