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
	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		if got := nestedString(newSession, "params", "cwd"); !filepath.IsAbs(got) || got != cwd {
			t.Fatalf("session/new cwd = %q, want absolute %q", got, cwd)
		}
		peer.respond(newSession, acpSessionResult("session-write", "resolved-model[context=272k,reasoning=medium,fast=false]"))

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

	session := startFakeCursorSession(t, engine.SessionOpts{CWD: cwd, Model: "requested-model"})
	events, err := turnWithFakeRunner(t, session, engine.TurnInput{Prompt: "hello", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(t, events, 2*time.Second)
	if spec := runner.lastSpec(); !slices.Equal(spec.Argv, []string{"fake-cursor", "--model", "requested-model", "acp"}) || spec.Dir != cwd || spec.Env == nil {
		t.Fatalf("exec argv=%#v dir=%q envSet=%v", spec.Argv, spec.Dir, spec.Env != nil)
	}
	if session.ID() != "session-write" {
		t.Fatalf("session id = %q, want session-write", session.ID())
	}
	if results := eventsOfType(got, engine.EventResultMessage); len(results) != 1 || results[0].Text != "Hello, world" {
		t.Fatalf("result events = %#v, want one concatenated result", results)
	}
	if models := eventsOfType(got, engine.EventModelReported); len(models) != 1 || models[0].ModelReported != "resolved-model[context=272k,reasoning=medium,fast=false]" {
		t.Fatalf("model events = %#v", models)
	}
	tools := eventsOfType(got, engine.EventToolUse)
	if len(tools) != 2 {
		t.Fatalf("tool events = %#v, want tool call and update", tools)
	}
	for _, event := range tools {
		metadata, err := json.Marshal(event.Metadata)
		if err != nil || strings.Contains(string(metadata), "rawInput") || strings.Contains(string(metadata), "must-not-leak") {
			t.Fatalf("tool metadata leaked raw input: %s (%v)", metadata, err)
		}
	}
	runner.assertRetired(t)
	if process := runner.lastProcess(); process == nil || !process.stdinClosed.Load() {
		t.Fatal("writable ACP process did not retire after stdin was closed")
	}
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

func TestACPSetupQualification(t *testing.T) {
	backend := New(Options{Binary: "fake-cursor"})
	if got := backend.VersionTransform("cursor-agent 2026.08.04-aaa8809"); got != MinimumKnownGoodVersion {
		t.Fatalf("normalized version = %q, want %q", got, MinimumKnownGoodVersion)
	}

	probe := &fakeCursorProbeRunner{
		path: "/qualified/bin/cursor-agent",
		output: "Available models\n" +
			"  cursor-fast - Cursor Fast\n" +
			"  cursor-pro - Cursor Pro\n",
	}
	discoverer := interface {
		DiscoverModels(context.Context, command.ProbeRunner) (*engine.ModelDiscovery, error)
	}(backend)
	discovered, err := discoverer.DiscoverModels(context.Background(), probe)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(discovered.Models, []string{"cursor-fast", "cursor-pro"}) || len(probe.specs) != 1 || !slices.Equal(probe.specs[0].Argv, []string{"/qualified/bin/cursor-agent", "models"}) {
		t.Fatalf("CLI model discovery = %#v, probe specs = %#v", discovered, probe.specs)
	}

	runner := newFakeACPRunner(t, func(t *testing.T, proc *fakeACPProcess, spec command.ExecSpec) {
		peer := newACPPeer(t, proc)
		peer.handshake(false)
		newSession := peer.expectRequest("session/new")
		if got := nestedString(newSession, "params", "cwd"); !filepath.IsAbs(got) {
			t.Fatalf("qualification cwd = %q, want absolute temporary directory", got)
		}
		peer.respond(newSession, acpSessionResult("qualification-session", "qualified-model"))
		setMode := peer.expectRequest("session/set_mode")
		if got := nestedString(setMode, "params", "modeId"); got != "plan" {
			t.Fatalf("qualification mode = %q, want plan", got)
		}
		peer.respond(setMode, map[string]any{})
		peer.expectStdinClose()
	})

	driver := newACPDriver("fake-cursor")
	qualified, err := driver.SetupQualify(context.Background(), runner, engine.SessionOpts{CWD: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(qualified.Models, []string{"qualified-model", "fallback-model"}) || qualified.Source != "cursor ACP session/new" {
		t.Fatalf("ACP qualification discovery = %#v", qualified)
	}
	spec := runner.lastSpec()
	if !slices.Equal(spec.Argv, []string{"fake-cursor", "acp"}) || spec.Env == nil {
		t.Fatalf("qualification exec argv=%#v envSet=%v", spec.Argv, spec.Env != nil)
	}
	serialized, err := json.Marshal(qualified)
	if err != nil || strings.Contains(string(serialized), "credential") || strings.Contains(strings.Join(spec.Argv, "\x00"), "credential") {
		t.Fatalf("qualification leaked credentials: discovery=%s argv=%#v err=%v", serialized, spec.Argv, err)
	}
	runner.assertRetired(t)
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
	t.Helper()
	turner, ok := session.(interface {
		TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
	})
	if !ok {
		t.Fatal("session does not support TurnWithRunner")
	}
	return turner.TurnWithRunner(context.Background(), input, runner)
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

type fakeCursorProbeRunner struct {
	path   string
	output string
	specs  []command.ProbeSpec
}

func (r *fakeCursorProbeRunner) LookPath(string) (string, error) {
	return r.path, nil
}

func (r *fakeCursorProbeRunner) Run(_ context.Context, spec command.ProbeSpec) (command.ProbeResult, error) {
	r.specs = append(r.specs, spec)
	if len(spec.Argv) > 1 && spec.Argv[1] == "models" {
		return command.ProbeResult{Stdout: []byte(r.output)}, nil
	}
	return command.ProbeResult{}, errors.New("unexpected probe command")
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

func acpSessionResult(sessionID, currentModel string) map[string]any {
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
			"currentModelId": currentModel,
			"availableModels": []any{
				map[string]any{"modelId": currentModel},
				map[string]any{"modelId": "fallback-model"},
			},
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
