package cursorcli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestACPFreshWritableTurn(t *testing.T) {
	if _, err := newACPDriver("fake-cursor").ExecSpec("", engine.SessionOpts{Effort: "high"}, engine.TurnInput{}); err == nil || err.Error() != "cursor backend does not expose a supported effort control" {
		t.Fatalf("effort error = %v", err)
	}

	cwd := t.TempDir()
	modelID := "resolved-model[context=272k,reasoning=medium,fast=false]"
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		if got := nestedString(newSession, "params", "cwd"); !filepath.IsAbs(got) || got != cwd {
			t.Fatalf("session/new cwd = %q, want absolute %q", got, cwd)
		}
		peer.respond(newSession, acpSessionResult("session-write", modelID))

		setModel := peer.expectRequest("session/set_model")
		if got := nestedString(setModel, "params", "sessionId"); got != "session-write" {
			t.Fatalf("set_model sessionId = %q, want session-write", got)
		}
		if got := nestedString(setModel, "params", "modelId"); got != modelID {
			t.Fatalf("modelId = %q, want %q", got, modelID)
		}
		peer.respond(setModel, map[string]any{})

		setMode := peer.expectRequest("session/set_mode")
		if got := nestedString(setMode, "params", "modeId"); got != "agent" {
			t.Fatalf("modeId = %q, want agent", got)
		}
		peer.respond(setMode, map[string]any{})

		prompt := peer.expectRequest("session/prompt")
		peer.serverRequest("permission-write", "session/request_permission", map[string]any{
			"options": []any{
				map[string]any{"optionId": "allow-this-once", "kind": "allow_once"},
				map[string]any{"optionId": "reject-this-once", "kind": "reject_once"},
			},
		})
		permission := peer.expectResponse("permission-write")
		if got := nestedString(permission, "result", "outcome", "optionId"); got != "allow-this-once" {
			t.Fatalf("permission response = %#v, want offered allow_once option", permission)
		}

		peer.notify("session/update", map[string]any{
			"sessionId":     "session-write",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "Hello, "},
		})
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-write",
			"sessionUpdate": "agent_thought_chunk",
			"content":       map[string]any{"text": "do not expose"},
		})
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-write",
			"sessionUpdate": "tool_call",
			"toolCallId":    "tool\n1",
			"title":         "Inspect workspace",
			"kind":          "read",
			"status":        "in_progress",
			"rawInput":      map[string]any{"secret": "must-not-leak"},
		})
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-write",
			"sessionUpdate": "tool_call_update",
			"toolCallId":    "tool\n1",
			"title":         "Inspect workspace",
			"kind":          "read",
			"status":        "completed",
			"content": []any{map[string]any{
				"type":    "diff",
				"path":    "ignored.txt",
				"oldText": "old",
				"newText": "new",
			}},
		})
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-write",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "world"},
		})
		peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: cwd, Model: modelID})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	if spec := runner.lastSpec(); !slices.Equal(spec.Argv, []string{"fake-cursor", "acp"}) || spec.Dir != cwd || spec.Env == nil {
		t.Fatalf("exec argv=%#v dir=%q envSet=%v", spec.Argv, spec.Dir, spec.Env != nil)
	}
	if session.ID() != "session-write" {
		t.Fatalf("session id = %q, want session-write", session.ID())
	}
	if results := eventsOfType(got, engine.EventResultMessage); len(results) != 1 || results[0].Text != "Hello, world" {
		t.Fatalf("result events = %#v, want one concatenated result", results)
	}
	if models := eventsOfType(got, engine.EventModelReported); len(models) != 1 || models[0].ModelReported != modelID {
		t.Fatalf("model events = %#v", models)
	}
	tools := eventsOfType(got, engine.EventToolUse)
	if len(tools) != 1 {
		t.Fatalf("tool events = %#v, want one completed lifecycle", tools)
	}
	tool := tools[0]
	if !tool.ObservedWorkspaceWriteItem || tool.Text != "" {
		t.Fatalf("tool event = %#v, want a text-free workspace-write observation", tool)
	}
	metadata, err := json.Marshal(tool.Metadata)
	if err != nil || strings.Contains(string(metadata), "rawInput") || strings.Contains(string(metadata), "must-not-leak") || strings.Contains(string(metadata), "ignored.txt") || strings.Contains(string(metadata), "old") || strings.Contains(string(metadata), "new") {
		t.Fatalf("tool metadata leaked private tool content: %s (%v)", metadata, err)
	}
	progress := eventsOfType(got, engine.EventProgress)
	if len(progress) != 1 || progress[0].Text != "" || progress[0].Metadata != nil {
		t.Fatalf("progress events = %#v, want one empty lifecycle progress event", progress)
	}
	runner.assertRetired(t)
	if process := runner.lastProcess(); process == nil || !process.stdinClosed.Load() {
		t.Fatal("writable ACP process did not retire after stdin was closed")
	}
}

func TestACPRequestedBareModelSelection(t *testing.T) {
	currentModel := "gpt-5.6-terra[context=272k,reasoning=medium,fast=false]"
	requestedModel := "grok-4.6"
	resolvedModel := "grok-4.6[effort=high,fast=true]"
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-model", currentModel,
			map[string]any{"modelId": "default[]", "name": "Auto"},
			map[string]any{"modelId": resolvedModel, "name": requestedModel},
			map[string]any{"modelId": "composer-2.5[fast=true]", "name": "composer-2.5"},
		))

		setModel := peer.expectRequest("session/set_model")
		if got := nestedString(setModel, "params", "modelId"); got != resolvedModel {
			t.Fatalf("modelId = %q, want %q", got, resolvedModel)
		}
		peer.respond(setModel, map[string]any{})

		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")
		peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir(), Model: requestedModel})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	if models := eventsOfType(got, engine.EventModelReported); len(models) != 1 || models[0].ModelReported != resolvedModel {
		t.Fatalf("model events = %#v, want %q", models, resolvedModel)
	}
	runner.assertRetired(t)
}

func TestACPUnavailableModelStopsBeforePrompt(t *testing.T) {
	requestedModel := "missing-model"
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-unavailable", "current-model",
			map[string]any{"modelId": "grok-4.6[effort=high,fast=true]", "name": "grok-4.6"},
		))
		setModel := peer.expectRequest("session/set_model")
		if got := nestedString(setModel, "params", "modelId"); got != requestedModel {
			t.Fatalf("modelId = %q, want unresolved %q", got, requestedModel)
		}
		peer.write(map[string]any{
			"jsonrpc": "2.0",
			"id":      setModel["id"],
			"error":   map[string]any{"code": -32602, "message": "Invalid model value: " + requestedModel},
		})
		peer.expectStdinClose()
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir(), Model: requestedModel})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	terminal := eventsOfType(got, engine.EventTerminalError)
	if len(terminal) != 1 || !strings.Contains(terminal[0].Text, "could not select Cursor model: Invalid model value: "+requestedModel) {
		t.Fatalf("events = %#v, want Cursor model rejection for %q", got, requestedModel)
	}
	runner.assertRetired(t)
}

func TestACPNoRequestedModelReportsCurrentModel(t *testing.T) {
	currentModel := "gpt-5.6-terra[context=272k,reasoning=medium,fast=false]"
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-default", currentModel))

		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")
		peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	if models := eventsOfType(got, engine.EventModelReported); len(models) != 1 || models[0].ModelReported != currentModel {
		t.Fatalf("model events = %#v, want %q", models, currentModel)
	}
	runner.assertRetired(t)
}

func TestACPTurnFlushesToolCallWithoutTerminalUpdate(t *testing.T) {
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-unterminated", "unterminated-model"))

		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-unterminated",
			"sessionUpdate": "tool_call",
			"toolCallId":    "tool-no-terminal",
			"title":         "Inspect config",
			"kind":          "read",
			"status":        "in_progress",
		})
		peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	tools := eventsOfType(got, engine.EventToolUse)
	if len(tools) != 1 || tools[0].Name != "Inspect config" || tools[0].Text != "Inspect config (in_progress)" || tools[0].ObservedWorkspaceWriteItem {
		t.Fatalf("tool events = %#v, want one flushed non-write tool observation", tools)
	}
	progress := eventsOfType(got, engine.EventProgress)
	if len(progress) != 1 {
		t.Fatalf("progress events = %#v, want one lifecycle progress event", progress)
	}
	runner.assertRetired(t)
}

func TestACPLoadedReadOnlyTurnCancellation(t *testing.T) {
	cwd := t.TempDir()
	promptPending := make(chan struct{})
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(true)
		load := peer.expectRequest("session/load")
		if got := nestedString(load, "params", "sessionId"); got != "session-load" {
			t.Fatalf("session/load id = %q", got)
		}
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-load",
			"sessionUpdate": "user_message_chunk",
			"content":       map[string]any{"text": "replay user"},
		})
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-load",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "REPLAY MUST NOT SURFACE"},
		})
		peer.respond(load, acpSessionResult("session-load", "loaded-model"))

		setMode := peer.expectRequest("session/set_mode")
		if got := nestedString(setMode, "params", "modeId"); got != "plan" {
			t.Fatalf("modeId = %q, want plan", got)
		}
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")

		peer.serverRequest("permission-read", "session/request_permission", map[string]any{
			"options": []any{
				map[string]any{"optionId": "allow-once", "kind": "allow_once"},
				map[string]any{"optionId": "reject-once", "kind": "reject_once"},
			},
		})
		permission := peer.expectResponse("permission-read")
		if got := nestedString(permission, "result", "outcome", "optionId"); got != "reject-once" {
			t.Fatalf("read-only permission response = %#v", permission)
		}
		close(promptPending)

		cancel := peer.expectNotification("session/cancel")
		if got := nestedString(cancel, "params", "sessionId"); got != "session-load" {
			t.Fatalf("cancel session = %q", got)
		}
		peer.serverRequest("permission-cancelled", "session/request_permission", map[string]any{
			"options": []any{map[string]any{"optionId": "reject-after-cancel", "kind": "reject_once"}},
		})
		cancelled := peer.expectResponse("permission-cancelled")
		if got := nestedString(cancelled, "result", "outcome", "outcome"); got != "cancelled" {
			t.Fatalf("cancelled permission response = %#v", cancelled)
		}
		peer.respond(prompt, map[string]any{"stopReason": "cancelled"})
	})

	backend := New(Options{Binary: "fake-cursor"})
	session, err := backend.Resume(context.Background(), "session-load", engine.SessionOpts{CWD: cwd})
	if err != nil {
		t.Fatal(err)
	}
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "review", Write: false}, runner)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-promptPending:
	case <-time.After(2 * time.Second):
		t.Fatal("ACP prompt did not reach the cancellation point")
	}
	interruptCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := session.Interrupt(interruptCtx); err != nil {
		t.Fatalf("interrupt = %v", err)
	}
	got := collectEvents(t, events, 2*time.Second)
	for _, event := range got {
		if strings.Contains(event.Text, "REPLAY MUST NOT SURFACE") {
			t.Fatalf("replayed load update leaked into events: %#v", got)
		}
		if event.Type == engine.EventTerminalError {
			t.Fatalf("cancelled turn was terminal: %#v", got)
		}
	}
	if results := eventsOfType(got, engine.EventResultMessage); len(results) != 1 || results[0].Text != "" {
		t.Fatalf("cancelled prompt result events = %#v, want one empty result", results)
	}
	runner.assertRetired(t)
	if process := runner.lastProcess(); process == nil || !process.stdinClosed.Load() {
		t.Fatal("read-only ACP process did not retire after cancellation")
	}
}

func TestACPSteerInterruptContinuesInOrder(t *testing.T) {
	cwd := t.TempDir()
	promptReady := make(chan struct{})
	firstContinuationReady := make(chan struct{})
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-steer", "steer-model"))
		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})

		prompt := peer.expectRequest("session/prompt")
		if got := nestedString(prompt, "params", "sessionId"); got != "session-steer" || acpPromptText(prompt) != "initial" {
			t.Fatalf("initial prompt = %#v", prompt)
		}
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-steer",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "discarded first segment"},
		})
		close(promptReady)

		cancel := peer.expectNotification("session/cancel")
		if got := firstString(cancel, "jsonrpc"); got != "2.0" {
			t.Fatalf("cancel jsonrpc = %q, want 2.0", got)
		}
		if got := nestedString(cancel, "params", "sessionId"); got != "session-steer" {
			t.Fatalf("first cancel sessionId = %q", got)
		}
		peer.serverRequest("permission-after-cancel", "session/request_permission", map[string]any{
			"options": []any{map[string]any{"optionId": "reject-once", "kind": "reject_once"}},
		})
		permission := peer.expectResponse("permission-after-cancel")
		if got := firstString(permission, "jsonrpc"); got != "2.0" {
			t.Fatalf("permission response jsonrpc = %q, want 2.0", got)
		}
		if got := nestedString(permission, "result", "outcome", "outcome"); got != "cancelled" {
			t.Fatalf("permission response = %#v, want cancelled outcome", permission)
		}
		peer.respond(prompt, map[string]any{"stopReason": "cancelled"})

		firstContinuation := peer.expectRequest("session/prompt")
		if got := nestedString(firstContinuation, "params", "sessionId"); got != "session-steer" || acpPromptText(firstContinuation) != "first direction" {
			t.Fatalf("first continuation = %#v", firstContinuation)
		}
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-steer",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "discarded second segment"},
		})
		close(firstContinuationReady)

		secondCancel := peer.expectNotification("session/cancel")
		if got := firstString(secondCancel, "jsonrpc"); got != "2.0" {
			t.Fatalf("second cancel jsonrpc = %q, want 2.0", got)
		}
		if got := nestedString(secondCancel, "params", "sessionId"); got != "session-steer" {
			t.Fatalf("second cancel sessionId = %q", got)
		}
		peer.respond(firstContinuation, map[string]any{"stopReason": "cancelled"})

		secondContinuation := peer.expectRequest("session/prompt")
		if got := nestedString(secondContinuation, "params", "sessionId"); got != "session-steer" || acpPromptText(secondContinuation) != "second direction" {
			t.Fatalf("second continuation = %#v", secondContinuation)
		}
		peer.notify("session/update", map[string]any{
			"sessionId":     "session-steer",
			"sessionUpdate": "agent_message_chunk",
			"content":       map[string]any{"text": "final segment"},
		})
		peer.respond(secondContinuation, map[string]any{"stopReason": "end_turn"})
		peer.expectStdinClose()
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: cwd})
	steerer, ok := session.(engine.Steerer)
	if !ok {
		t.Fatal("Cursor session does not implement engine.Steerer")
	}
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "initial", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-promptReady:
	case <-time.After(2 * time.Second):
		t.Fatal("initial ACP prompt was not written")
	}

	steerCtx, cancelSteer := context.WithTimeout(context.Background(), 2*time.Second)
	first, err := steerer.Steer(steerCtx, engine.SteerMessage{ID: "steer-1", Text: "first direction"})
	cancelSteer()
	if err != nil || first.Mechanism != engine.SteerMechanismInterruptContinue || !first.Confirmed {
		t.Fatalf("first steer delivery = %#v, err=%v", first, err)
	}
	select {
	case <-firstContinuationReady:
	case <-time.After(2 * time.Second):
		t.Fatal("first continuation prompt was not written")
	}
	steerCtx, cancelSteer = context.WithTimeout(context.Background(), 2*time.Second)
	second, err := steerer.Steer(steerCtx, engine.SteerMessage{ID: "steer-2", Text: "second direction"})
	cancelSteer()
	if err != nil || second.Mechanism != engine.SteerMechanismInterruptContinue || !second.Confirmed {
		t.Fatalf("second steer delivery = %#v, err=%v", second, err)
	}

	got := collectEvents(t, events, 2*time.Second)
	results := eventsOfType(got, engine.EventResultMessage)
	if len(results) != 1 || results[0].Text != "final segment" {
		t.Fatalf("result events = %#v, want only the final resumed segment", results)
	}
	if finals := eventsOfType(got, engine.EventTurnFinal); len(finals) != 1 {
		t.Fatalf("TurnFinal events = %#v, want exactly one", finals)
	}
	if got, err := steerer.Steer(context.Background(), engine.SteerMessage{ID: "too-late", Text: "late"}); !errors.Is(err, engine.ErrSteerNotActive) {
		t.Fatalf("steer after completion = %#v, %v; want ErrSteerNotActive", got, err)
	}
	runner.assertRetired(t)
}

func TestACPCancelDropsPendingSteer(t *testing.T) {
	promptReady := make(chan struct{})
	cancelSeen := make(chan struct{})
	permissionAnswered := make(chan struct{})
	finishCancelledPrompt := make(chan struct{})
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-cancel-steer", "steer-model"))
		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")
		close(promptReady)
		peer.expectNotification("session/cancel")
		close(cancelSeen)
		peer.serverRequest("permission-pending", "session/request_permission", map[string]any{
			"options": []any{map[string]any{"optionId": "allow-once", "kind": "allow_once"}},
		})
		permission := peer.expectResponse("permission-pending")
		if got := nestedString(permission, "result", "outcome", "outcome"); got != "cancelled" {
			t.Fatalf("permission response = %#v, want cancelled outcome", permission)
		}
		close(permissionAnswered)
		<-finishCancelledPrompt
		peer.respond(prompt, map[string]any{"stopReason": "cancelled"})
		peer.expectStdinClose()
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
	steerer := session.(engine.Steerer)
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	defer cancelTurn()
	events, err := turnWithFakeRunnerContext(t, turnCtx, session, engine.TurnInput{Prompt: "initial", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	<-promptReady
	steerDone := make(chan error, 1)
	go func() {
		_, err := steerer.Steer(context.Background(), engine.SteerMessage{ID: "pending", Text: "must be dropped"})
		steerDone <- err
	}()
	select {
	case <-cancelSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("steer did not send ACP session/cancel")
	}
	select {
	case <-permissionAnswered:
	case <-time.After(2 * time.Second):
		t.Fatal("pending permission was not answered")
	}
	interruptCtx, cancelInterrupt := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelInterrupt()
	interruptDone := make(chan error, 1)
	go func() { interruptDone <- session.Interrupt(interruptCtx) }()
	select {
	case err := <-steerDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending steer error = %v, want cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending steer did not observe job cancellation")
	}
	cancelTurn()
	close(finishCancelledPrompt)
	select {
	case err := <-interruptDone:
		if err != nil {
			t.Fatalf("job cancellation interrupt = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("job cancellation interrupt did not settle")
	}
	got := collectEvents(t, events, 2*time.Second)
	finals := eventsOfType(got, engine.EventTurnFinal)
	if len(finals) != 1 || finals[0].TurnFinal == nil || !finals[0].TurnFinal.Canceled {
		t.Fatalf("TurnFinal events = %#v, want one cancelled turn final", finals)
	}
	if results := eventsOfType(got, engine.EventResultMessage); len(results) > 1 || (len(results) == 1 && results[0].Text != "") {
		t.Fatalf("cancelled steer result events = %#v, want no partial segment result", results)
	}
	runner.assertRetired(t)
}

func TestACPSteerDoesNotContinueIfCancelledPromptDoesNotDrain(t *testing.T) {
	promptReady := make(chan struct{})
	cancelSeen := make(chan struct{})
	finishCancelledPrompt := make(chan struct{})
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		peer.respond(newSession, acpSessionResult("session-undrained", "steer-model"))
		setMode := peer.expectRequest("session/set_mode")
		peer.respond(setMode, map[string]any{})
		prompt := peer.expectRequest("session/prompt")
		close(promptReady)
		peer.expectNotification("session/cancel")
		close(cancelSeen)
		<-finishCancelledPrompt
		peer.respond(prompt, map[string]any{"stopReason": "cancelled"})
		peer.expectStdinClose()
	})

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
	steerer := session.(engine.Steerer)
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "initial", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	<-promptReady
	steerCtx, cancelSteer := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = steerer.Steer(steerCtx, engine.SteerMessage{ID: "undrained", Text: "must not be sent"})
	cancelSteer()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("undrained steer error = %v, want context deadline", err)
	}
	select {
	case <-cancelSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("steer did not send ACP session/cancel")
	}
	close(finishCancelledPrompt)
	got := collectEvents(t, events, 2*time.Second)
	if terminal := eventsOfType(got, engine.EventTerminalError); len(terminal) != 1 || !errors.Is(terminal[0].Err, context.DeadlineExceeded) {
		t.Fatalf("undrained turn events = %#v, want one deadline terminal error", got)
	}
	if results := eventsOfType(got, engine.EventResultMessage); len(results) != 0 {
		t.Fatalf("undrained cancelled prompt produced results: %#v", results)
	}
	runner.assertRetired(t)
}

func acpPromptText(frame map[string]any) string {
	params, _ := frame["params"].(map[string]any)
	prompt, _ := params["prompt"].([]any)
	if len(prompt) != 1 {
		return ""
	}
	item, _ := prompt[0].(map[string]any)
	if firstString(item, "type") != "text" {
		return ""
	}
	return firstString(item, "text")
}

func TestACPNegativeTerminalProtocolBranches(t *testing.T) {
	tests := []struct {
		name         string
		run          func(*testing.T, *acpPeer)
		terminalText string
		wantResult   bool
		interrupted  bool
	}{
		{
			name: "set mode error prevents prompt",
			run: func(t *testing.T, peer *acpPeer) {
				setMode := peer.expectRequest("session/set_mode")
				peer.write(map[string]any{
					"jsonrpc": "2.0",
					"id":      setMode["id"],
					"error":   map[string]any{"code": -32000, "message": "set mode rejected"},
				})
				peer.expectStdinClose()
			},
			terminalText: "could not verify Cursor mode: set mode rejected",
		},
		{
			name: "error data message is included",
			run: func(t *testing.T, peer *acpPeer) {
				setMode := peer.expectRequest("session/set_mode")
				peer.write(map[string]any{
					"jsonrpc": "2.0",
					"id":      setMode["id"],
					"error": map[string]any{
						"code":    -32602,
						"message": "Invalid params",
						"data":    map[string]any{"message": "Invalid model value: definitely-not-a-model"},
					},
				})
				peer.expectStdinClose()
			},
			terminalText: "could not verify Cursor mode: Invalid params: Invalid model value: definitely-not-a-model",
		},
		{
			name: "unsupported reverse request is method not found",
			run: func(t *testing.T, peer *acpPeer) {
				setMode := peer.expectRequest("session/set_mode")
				peer.respond(setMode, map[string]any{})
				prompt := peer.expectRequest("session/prompt")
				peer.serverRequest("unsupported-reverse", "cursor/ask_question", map[string]any{
					"options": []any{map[string]any{"optionId": "allow-once", "kind": "allow_once"}},
				})
				response := peer.expectResponse("unsupported-reverse")
				rpcError, _ := response["error"].(map[string]any)
				if got := protocolVersion(rpcError["code"]); got != -32601 {
					t.Fatalf("unsupported reverse response = %#v, want error code -32601", response)
				}
				peer.respond(prompt, map[string]any{"stopReason": "end_turn"})
				peer.expectStdinClose()
			},
			wantResult: true,
		},
		{
			name: "unrequested prompt cancellation is terminal",
			run: func(t *testing.T, peer *acpPeer) {
				setMode := peer.expectRequest("session/set_mode")
				peer.respond(setMode, map[string]any{})
				prompt := peer.expectRequest("session/prompt")
				peer.respond(prompt, map[string]any{"stopReason": "cancelled"})
				peer.expectStdinClose()
			},
			terminalText: "cursor ACP prompt was cancelled without a requested interrupt",
			interrupted:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
				peer := newACPPeer(t, proc)
				peer.handshake(false)
				newSession := peer.expectRequest("session/new")
				peer.respond(newSession, acpSessionResult("session-negative", "negative-model"))
				test.run(t, peer)
			})

			session := startFakeCursorSession(t, engine.SessionOpts{CWD: t.TempDir()})
			events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
			if err != nil {
				t.Fatal(err)
			}
			got := collectEvents(t, events, 2*time.Second)
			terminal := eventsOfType(got, engine.EventTerminalError)
			if test.terminalText != "" {
				if len(terminal) != 1 || !strings.Contains(terminal[0].Text, test.terminalText) {
					t.Fatalf("events = %#v, want terminal error containing %q", got, test.terminalText)
				}
				if test.interrupted && !errors.Is(terminal[0].Err, engine.ErrTurnInterrupted) {
					t.Fatalf("terminal event error = %v, want ErrTurnInterrupted", terminal[0].Err)
				}
			} else if len(terminal) != 0 {
				t.Fatalf("events = %#v, did not want terminal error", got)
			}
			if test.wantResult && len(eventsOfType(got, engine.EventResultMessage)) != 1 {
				t.Fatalf("events = %#v, want one result", got)
			}
			runner.assertRetired(t)
		})
	}
}

func startFakeCursorSession(t *testing.T, opts engine.SessionOpts) engine.Session {
	t.Helper()
	backend := New(Options{Binary: "fake-cursor"})
	session, err := backend.Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func turnWithFakeRunner(t *testing.T, session engine.Session, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	return turnWithFakeRunnerContext(t, context.Background(), session, input, runner)
}

func turnWithFakeRunnerContext(t *testing.T, ctx context.Context, session engine.Session, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	t.Helper()
	turner, ok := session.(interface {
		TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
	})
	if !ok {
		t.Fatal("session does not support TurnWithRunner")
	}
	return turner.TurnWithRunner(ctx, input, runner)
}

func collectEvents(t *testing.T, events <-chan engine.Event, timeout time.Duration) []engine.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var got []engine.Event
	for {
		select {
		case event, ok := <-events:
			if !ok {
				return got
			}
			got = append(got, event)
		case <-timer.C:
			t.Fatalf("timed out collecting events: %#v", got)
		}
	}
}

func eventsOfType(events []engine.Event, typ string) []engine.Event {
	var result []engine.Event
	for _, event := range events {
		if event.Type == typ {
			result = append(result, event)
		}
	}
	return result
}

type acpPeerFunc func(*testing.T, *fakeACPProcess, command.ExecSpec)

type fakeACPRunner struct {
	t *testing.T

	mu        sync.Mutex
	peers     []acpPeerFunc
	specs     []command.ExecSpec
	processes []*fakeACPProcess
}

func newFakeACPRunner(t *testing.T, peers ...acpPeerFunc) *fakeACPRunner {
	t.Helper()
	return &fakeACPRunner{t: t, peers: append([]acpPeerFunc(nil), peers...)}
}

func (r *fakeACPRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.mu.Lock()
	if len(r.peers) == 0 {
		r.mu.Unlock()
		return nil, errors.New("unexpected fake ACP start")
	}
	peer := r.peers[0]
	r.peers = r.peers[1:]
	r.specs = append(r.specs, spec)
	proc := newFakeACPProcess()
	r.processes = append(r.processes, proc)
	r.mu.Unlock()

	go func() {
		defer proc.closeOutputs()
		defer proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
		peer(r.t, proc, spec)
	}()
	return proc, nil
}

func (r *fakeACPRunner) lastSpec() command.ExecSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.specs) == 0 {
		return command.ExecSpec{}
	}
	return r.specs[len(r.specs)-1]
}

func (r *fakeACPRunner) lastProcess() *fakeACPProcess {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.processes) == 0 {
		return nil
	}
	return r.processes[len(r.processes)-1]
}

func (r *fakeACPRunner) assertRetired(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	processes := append([]*fakeACPProcess(nil), r.processes...)
	r.mu.Unlock()
	for _, process := range processes {
		select {
		case <-process.waitCh:
		case <-time.After(2 * time.Second):
			t.Fatal("fake ACP process did not retire")
		}
	}
}

type fakeACPProcess struct {
	stdinR  *io.PipeReader
	stdinW  *trackedACPPipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	waitCh      chan struct{}
	finishOnce  sync.Once
	outputsOnce sync.Once
	exit        command.ExitObservation
	waitErr     error
	stdinClosed atomic.Bool
}

func newFakeACPProcess() *fakeACPProcess {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	process := &fakeACPProcess{
		stdinR:  stdinR,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
		waitCh:  make(chan struct{}),
	}
	process.stdinW = &trackedACPPipeWriter{PipeWriter: stdinW, closed: &process.stdinClosed}
	return process
}

func (p *fakeACPProcess) Stdin() io.WriteCloser           { return p.stdinW }
func (p *fakeACPProcess) Stdout() io.ReadCloser           { return p.stdoutR }
func (p *fakeACPProcess) Stderr() io.ReadCloser           { return p.stderrR }
func (p *fakeACPProcess) Interrupt(context.Context) error { return nil }

func (p *fakeACPProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-p.waitCh:
		return p.exit, p.waitErr
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (p *fakeACPProcess) finish(exit command.ExitObservation, err error) {
	p.finishOnce.Do(func() {
		p.exit = exit
		p.waitErr = err
		close(p.waitCh)
	})
}

func (p *fakeACPProcess) closeOutputs() {
	p.outputsOnce.Do(func() {
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
	})
}

type trackedACPPipeWriter struct {
	*io.PipeWriter
	closed *atomic.Bool
}

func (w *trackedACPPipeWriter) Close() error {
	w.closed.Store(true)
	return w.PipeWriter.Close()
}

type acpPeer struct {
	t       *testing.T
	proc    *fakeACPProcess
	scanner *bufio.Scanner
}

func newACPPeer(t *testing.T, proc *fakeACPProcess) *acpPeer {
	t.Helper()
	return &acpPeer{t: t, proc: proc, scanner: bufio.NewScanner(proc.stdinR)}
}

func (p *acpPeer) handshake(loadSession bool) {
	p.t.Helper()
	initialize := p.expectRequest("initialize")
	params, _ := initialize["params"].(map[string]any)
	if got := protocolVersion(params["protocolVersion"]); got != 1 {
		p.t.Fatalf("initialize protocol version = %d", got)
	}
	capabilities, _ := params["clientCapabilities"].(map[string]any)
	fs, _ := capabilities["fs"].(map[string]any)
	if read, ok := fs["readTextFile"].(bool); !ok || read {
		p.t.Fatalf("initialize readTextFile = %#v, want false", fs["readTextFile"])
	}
	if write, ok := fs["writeTextFile"].(bool); !ok || write {
		p.t.Fatalf("initialize writeTextFile = %#v, want false", fs["writeTextFile"])
	}
	if terminal, ok := capabilities["terminal"].(bool); !ok || terminal {
		p.t.Fatalf("initialize terminal = %#v, want false", capabilities["terminal"])
	}
	if got := nestedString(initialize, "params", "clientInfo", "name"); got != "agentbus" {
		p.t.Fatalf("initialize client name = %q", got)
	}
	if got := nestedString(initialize, "params", "clientInfo", "version"); got != StreamSchema {
		p.t.Fatalf("initialize client version = %q", got)
	}
	p.respond(initialize, map[string]any{
		"protocolVersion":   1,
		"agentCapabilities": map[string]any{"loadSession": loadSession},
	})
	authenticate := p.expectRequest("authenticate")
	if got := nestedString(authenticate, "params", "methodId"); got != "cursor_login" {
		p.t.Fatalf("authenticate method = %q", got)
	}
	p.respond(authenticate, map[string]any{})
}

func (p *acpPeer) expectRequest(method string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "method"); got != method {
		p.t.Fatalf("client method = %q, want %q in %#v", got, method, frame)
	}
	if _, ok := frame["id"]; !ok {
		p.t.Fatalf("request %s lacks id: %#v", method, frame)
	}
	if got := firstString(frame, "jsonrpc"); got != "2.0" {
		p.t.Fatalf("request %s jsonrpc = %q", method, got)
	}
	return frame
}

func (p *acpPeer) expectNotification(method string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "method"); got != method {
		p.t.Fatalf("client notification = %q, want %q in %#v", got, method, frame)
	}
	if _, ok := frame["id"]; ok {
		p.t.Fatalf("notification %s unexpectedly has id: %#v", method, frame)
	}
	return frame
}

func (p *acpPeer) expectResponse(id string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := requestIDKey(frame["id"]); got != id {
		p.t.Fatalf("response id = %q, want %q in %#v", got, id, frame)
	}
	if _, ok := frame["method"]; ok {
		p.t.Fatalf("response unexpectedly has method: %#v", frame)
	}
	return frame
}

func (p *acpPeer) expectStdinClose() {
	p.t.Helper()
	if p.scanner.Scan() {
		p.t.Fatalf("unexpected client frame during setup: %q", p.scanner.Text())
	}
	if err := p.scanner.Err(); err != nil {
		p.t.Fatalf("read setup stdin: %v", err)
	}
	if !p.proc.stdinClosed.Load() {
		p.t.Fatal("setup stdin reached EOF without CloseStdin")
	}
}

func (p *acpPeer) respond(request map[string]any, result any) {
	p.write(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": result})
}

func (p *acpPeer) notify(method string, params map[string]any) {
	p.write(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func (p *acpPeer) serverRequest(id, method string, params map[string]any) {
	p.write(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

func (p *acpPeer) readFrame() map[string]any {
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

func (p *acpPeer) write(value any) {
	p.t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		p.t.Fatal(err)
	}
	payload = append(payload, '\n')
	if _, err := p.proc.stdoutW.Write(payload); err != nil {
		p.t.Fatalf("write server frame: %v", err)
	}
}

func acpSessionResult(sessionID, currentModel string, modelOptions ...map[string]any) map[string]any {
	if len(modelOptions) == 0 {
		modelOptions = []map[string]any{
			{"modelId": currentModel, "name": currentModel},
			{"modelId": "fallback-model", "name": "fallback-model"},
		}
	}
	availableModels := make([]any, len(modelOptions))
	for index, model := range modelOptions {
		availableModels[index] = model
	}
	return map[string]any{
		"sessionId": sessionID,
		"modes": map[string]any{
			"currentModeId": "ask",
			"availableModes": []any{
				map[string]any{"id": "agent"},
				map[string]any{"id": "plan"},
				map[string]any{"id": "ask"},
			},
		},
		"models": map[string]any{
			"currentModelId":  currentModel,
			"availableModels": availableModels,
		},
	}
}

func nestedString(object map[string]any, path ...string) string {
	current := object
	for index, key := range path {
		if index == len(path)-1 {
			return firstString(current, key)
		}
		next, _ := current[key].(map[string]any)
		current = next
	}
	return ""
}
