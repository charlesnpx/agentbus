package claudecli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/cliadapter"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestClaudeStreamJSONArgvAndPermissionProfiles(t *testing.T) {
	cwd := t.TempDir()
	runner := newFakeClaudeRunner(t,
		func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			peer.handshake()
			peer.expectUser("inspect")
			peer.emitSystem("session-readonly", "claude-sonnet")
			peer.emitResult("success", false, "readonly done")
		},
		func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			peer.handshake()
			peer.expectUser("repair")
			peer.emitSystem("session-write", "claude-sonnet")
			peer.emitResult("success", false, "write done")
		},
	)

	session := startFakeClaudeSession(t, engine.SessionOpts{CWD: cwd, Model: "sonnet", Effort: "max"})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "inspect"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	readOnlySpec := runner.specAt(0)
	if readOnlySpec.Dir != cwd {
		t.Fatalf("dir = %q, want %q", readOnlySpec.Dir, cwd)
	}
	assertContainsArg(t, readOnlySpec.Argv, "-p")
	assertArgValue(t, readOnlySpec.Argv, "--input-format", "stream-json")
	assertArgValue(t, readOnlySpec.Argv, "--output-format", "stream-json")
	assertContainsArg(t, readOnlySpec.Argv, "--verbose")
	assertArgValue(t, readOnlySpec.Argv, "--model", "sonnet")
	assertArgValue(t, readOnlySpec.Argv, "--effort", "max")
	assertArgValue(t, readOnlySpec.Argv, "--allowedTools", strings.Join(readOnlyAllowedTools, ","))
	assertArgValue(t, readOnlySpec.Argv, "--disallowedTools", strings.Join(readOnlyDeniedTools, ","))
	assertContainsArg(t, readOnlySpec.Argv, "--strict-mcp-config")
	assertArgValue(t, readOnlySpec.Argv, "--mcp-config", `{"mcpServers":{}}`)
	assertArgValue(t, readOnlySpec.Argv, "--permission-mode", "dontAsk")
	assertNotContainsArg(t, readOnlySpec.Argv, "--cwd")
	assertNotContainsArg(t, readOnlySpec.Argv, "--replay-user-messages")
	assertNotContainsArg(t, readOnlySpec.Argv, "--include-partial-messages")
	assertNotContainsArg(t, readOnlySpec.Argv, "--dangerously-skip-permissions")

	session = startFakeClaudeSession(t, engine.SessionOpts{CWD: cwd})
	events, err = turnWithRunner(t, session, engine.TurnInput{Prompt: "repair", Write: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	writeSpec := runner.specAt(1)
	assertContainsArg(t, writeSpec.Argv, "--dangerously-skip-permissions")
	assertNotContainsArg(t, writeSpec.Argv, "--allowedTools")
	assertNotContainsArg(t, writeSpec.Argv, "--disallowedTools")
	assertNotContainsArg(t, writeSpec.Argv, "--permission-mode")
}

func TestClaudeInitializeOrderingAndTimeoutFallback(t *testing.T) {
	t.Run("awaits initialize response before user message", func(t *testing.T) {
		runner := newFakeClaudeRunner(t, func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			init := peer.expectControlRequest("initialize")
			next := peer.startReadFrame()
			select {
			case frame := <-next:
				t.Fatalf("client sent frame before initialize response: %#v", frame.obj)
			case <-time.After(25 * time.Millisecond):
			}
			peer.respondControlSuccess(init, map[string]any{"ok": true})
			user := peer.receiveFrame(next, 2*time.Second)
			peer.assertUser(user, "hello")
			peer.emitSystem("session-init", "claude-sonnet")
			peer.emitResult("success", false, "done")
		})

		session := startFakeClaudeSession(t, engine.SessionOpts{})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
		if err != nil {
			t.Fatal(err)
		}
		_ = collectEventsWithTimeout(t, events, 2*time.Second)
	})

	t.Run("proceeds when initialize response is absent", func(t *testing.T) {
		runner := newFakeClaudeRunner(t, func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			peer.expectControlRequest("initialize")
			peer.expectUser("hello")
			peer.emitSystem("session-init-timeout", "claude-sonnet")
			peer.emitResult("success", false, "done")
		})

		session := startFakeClaudeSessionWithInitializeTimeout(t, 15*time.Millisecond, engine.SessionOpts{})
		events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
		if err != nil {
			t.Fatal(err)
		}
		got := collectEventsWithTimeout(t, events, 2*time.Second)
		if !containsEvent(got, engine.EventResultMessage, "done") {
			t.Fatalf("events = %#v, want result after initialize timeout", got)
		}
	})
}

func TestClaudeResultStatusMapping(t *testing.T) {
	tests := []struct {
		name       string
		subtype    string
		isError    bool
		result     any
		wantType   string
		wantText   string
		wantResult bool
	}{
		{name: "success", subtype: "success", result: "final answer", wantType: engine.EventResultMessage, wantText: "final answer", wantResult: true},
		{name: "is_error", subtype: "success", isError: true, result: map[string]any{"message": "tool failed"}, wantType: engine.EventTerminalError, wantText: "tool failed"},
		{name: "error subtype", subtype: "error_during_execution", result: "execution failed", wantType: engine.EventTerminalError, wantText: "execution failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := newFakeClaudeRunner(t, func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
				peer := newClaudePeer(t, proc)
				peer.handshake()
				peer.expectUser("hello")
				peer.emitSystem("session-result", "claude-sonnet")
				peer.emitResult(test.subtype, test.isError, test.result)
			})

			session := startFakeClaudeSession(t, engine.SessionOpts{})
			events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "hello"}, runner)
			if err != nil {
				t.Fatal(err)
			}
			got := collectEventsWithTimeout(t, events, 2*time.Second)
			if !containsEvent(got, test.wantType, test.wantText) {
				t.Fatalf("events = %#v, want %s containing %q", got, test.wantType, test.wantText)
			}
			if !test.wantResult && resultText(got) != "" {
				t.Fatalf("events = %#v, did not want fabricated success result", got)
			}
		})
	}
}

func TestClaudeSessionIDCapturedAndResumed(t *testing.T) {
	runner := newFakeClaudeRunner(t,
		func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			peer.handshake()
			peer.expectUser("first")
			peer.emitSystem("claude-session-1", "claude-sonnet")
			peer.emitResult("success", false, "first done")
		},
		func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
			peer := newClaudePeer(t, proc)
			peer.handshake()
			peer.expectUser("second")
			peer.emitSystem("claude-session-1", "claude-sonnet")
			peer.emitResult("success", false, "second done")
		},
	)

	session := startFakeClaudeSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	if session.ID() != "claude-session-1" {
		t.Fatalf("session id = %q, want claude-session-1", session.ID())
	}

	events, err = turnWithRunner(t, session, engine.TurnInput{Prompt: "second"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	assertArgValue(t, runner.specAt(1).Argv, "--resume", "claude-session-1")
}

func TestClaudeAnswersControlRequestsAndMapsAssistantEvents(t *testing.T) {
	runner := newFakeClaudeRunner(t, func(t *testing.T, proc *fakeClaudeProcess, spec command.ExecSpec) {
		peer := newClaudePeer(t, proc)
		peer.handshake()
		peer.expectUser("use tool")
		peer.emitSystem("session-control", "claude-sonnet")

		peer.emitControlRequest("tool-req", "can_use_tool")
		toolResponse := peer.expectControlResponse("tool-req")
		if got := nestedString(toolResponse, "response", "response", "behavior"); got != "allow" {
			t.Fatalf("can_use_tool response = %#v, want behavior allow", toolResponse)
		}

		peer.emitControlRequest("unknown-req", "future_subtype")
		unknownResponse := peer.expectControlResponse("unknown-req")
		if got := nestedString(unknownResponse, "response", "subtype"); got != "success" {
			t.Fatalf("unknown control response = %#v, want success", unknownResponse)
		}

		peer.emitAssistant("assistant text", "Bash", map[string]any{"command": "git status"})
		peer.emitResult("success", false, "done")
	})

	session := startFakeClaudeSession(t, engine.SessionOpts{})
	events, err := turnWithRunner(t, session, engine.TurnInput{Prompt: "use tool"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if !containsModel(got, "claude-sonnet") || !containsEvent(got, engine.EventAgentText, "assistant text") || !containsToolUse(got, "Bash", "git status") {
		t.Fatalf("events = %#v, want model, assistant text, and tool use", got)
	}
	if resultText(got) != "done" {
		t.Fatalf("events = %#v, want result done", got)
	}
}

func TestClaudeDiscoveryParsesFakeHelp(t *testing.T) {
	discovery, err := New(Options{Binary: "fake-claude"}).(interface {
		DiscoverModels(context.Context, command.ProbeRunner) (*engine.ModelDiscovery, error)
	}).DiscoverModels(context.Background(), fakeProbeRunner{
		help: strings.Join([]string{
			"  --effort <level> Effort level",
			"    (low, medium, high, xhigh, max)",
			"  --model <model> Model",
			"    (e.g. 'fable', 'opus', or 'sonnet')",
		}, "\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(discovery.Models, ",") != "fable,opus,sonnet" || strings.Join(discovery.Efforts, ",") != "high,low,max,medium,xhigh" {
		t.Fatalf("discovery = %#v", discovery)
	}
}

func TestClaudeDiscoveryReportsHelpFailures(t *testing.T) {
	_, err := discoverModels(context.Background(), fakeProbeRunner{err: errors.New("help exploded")}, "fake-claude")
	if err == nil || !strings.Contains(err.Error(), "help exploded") {
		t.Fatalf("err = %v", err)
	}
	_, err = discoverModels(context.Background(), fakeProbeRunner{help: "generic help"}, "fake-claude")
	if err == nil || !strings.Contains(err.Error(), "parser found no model or effort") {
		t.Fatalf("err = %v", err)
	}
}

func startFakeClaudeSession(t *testing.T, opts engine.SessionOpts) engine.Session {
	t.Helper()
	backend := New(Options{Binary: "fake-claude"})
	session, err := backend.Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func startFakeClaudeSessionWithInitializeTimeout(t *testing.T, timeout time.Duration, opts engine.SessionOpts) engine.Session {
	t.Helper()
	backend := New(Options{Binary: "fake-claude"})
	cliBackend, ok := backend.(*cliadapter.Backend)
	if !ok {
		t.Fatal("backend is not cliadapter.Backend")
	}
	driver, ok := cliBackend.Driver.(*streamJSONDriver)
	if !ok {
		t.Fatal("backend driver is not streamJSONDriver")
	}
	driver.initializeTimeout = timeout
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

type fakeProbeRunner struct {
	help string
	err  error
}

func (r fakeProbeRunner) LookPath(file string) (string, error) {
	return file, nil
}

func (r fakeProbeRunner) Run(context.Context, command.ProbeSpec) (command.ProbeResult, error) {
	if r.err != nil {
		return command.ProbeResult{}, r.err
	}
	return command.ProbeResult{Stdout: []byte(r.help)}, nil
}

type claudePeerFunc func(*testing.T, *fakeClaudeProcess, command.ExecSpec)

type fakeClaudeRunner struct {
	t *testing.T

	mu    sync.Mutex
	peers []claudePeerFunc
	specs []command.ExecSpec
}

func newFakeClaudeRunner(t *testing.T, peers ...claudePeerFunc) *fakeClaudeRunner {
	t.Helper()
	return &fakeClaudeRunner{t: t, peers: append([]claudePeerFunc(nil), peers...)}
}

func (r *fakeClaudeRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.mu.Lock()
	if len(r.peers) == 0 {
		r.mu.Unlock()
		return nil, errors.New("unexpected fake claude start")
	}
	peer := r.peers[0]
	r.peers = r.peers[1:]
	r.specs = append(r.specs, spec)
	r.mu.Unlock()

	proc := newFakeClaudeProcess()
	go func() {
		defer proc.closeOutputs()
		defer proc.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
		peer(r.t, proc, spec)
	}()
	return proc, nil
}

func (r *fakeClaudeRunner) specAt(i int) command.ExecSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.specs) {
		r.t.Fatalf("missing exec spec %d; have %d", i, len(r.specs))
	}
	return r.specs[i]
}

type fakeClaudeProcess struct {
	stdinR  *io.PipeReader
	stdinW  *io.PipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	waitCh      chan struct{}
	finishOnce  sync.Once
	outputsOnce sync.Once
	exit        command.ExitObservation
	waitErr     error
}

func newFakeClaudeProcess() *fakeClaudeProcess {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &fakeClaudeProcess{
		stdinR:  stdinR,
		stdinW:  stdinW,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
		waitCh:  make(chan struct{}),
	}
}

func (p *fakeClaudeProcess) Stdin() io.WriteCloser {
	return p.stdinW
}

func (p *fakeClaudeProcess) Stdout() io.ReadCloser {
	return p.stdoutR
}

func (p *fakeClaudeProcess) Stderr() io.ReadCloser {
	return p.stderrR
}

func (p *fakeClaudeProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-p.waitCh:
		return p.exit, p.waitErr
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (p *fakeClaudeProcess) Interrupt(context.Context) error {
	p.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	return nil
}

func (p *fakeClaudeProcess) finish(exit command.ExitObservation, err error) {
	p.finishOnce.Do(func() {
		p.exit = exit
		p.waitErr = err
		close(p.waitCh)
	})
}

func (p *fakeClaudeProcess) closeOutputs() {
	p.outputsOnce.Do(func() {
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
	})
}

type claudePeer struct {
	t       *testing.T
	proc    *fakeClaudeProcess
	scanner *bufio.Scanner
}

type frameResult struct {
	obj map[string]any
	err error
}

func newClaudePeer(t *testing.T, proc *fakeClaudeProcess) *claudePeer {
	t.Helper()
	return &claudePeer{t: t, proc: proc, scanner: bufio.NewScanner(proc.stdinR)}
}

func (p *claudePeer) handshake() {
	p.t.Helper()
	init := p.expectControlRequest("initialize")
	p.respondControlSuccess(init, map[string]any{"ok": true})
}

func (p *claudePeer) expectControlRequest(subtype string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "type"); got != "control_request" {
		p.t.Fatalf("client frame type = %q, want control_request in %#v", got, frame)
	}
	request, ok := firstMap(frame, "request")
	if !ok {
		p.t.Fatalf("control_request missing request: %#v", frame)
	}
	if got := firstString(request, "subtype"); got != subtype {
		p.t.Fatalf("control_request subtype = %q, want %q in %#v", got, subtype, frame)
	}
	if firstString(frame, "request_id") == "" {
		p.t.Fatalf("control_request missing request_id: %#v", frame)
	}
	return frame
}

func (p *claudePeer) expectUser(prompt string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	p.assertUser(frame, prompt)
	return frame
}

func (p *claudePeer) assertUser(frame map[string]any, prompt string) {
	p.t.Helper()
	if got := firstString(frame, "type"); got != "user" {
		p.t.Fatalf("client frame type = %q, want user in %#v", got, frame)
	}
	message, ok := firstMap(frame, "message")
	if !ok {
		p.t.Fatalf("user frame missing message: %#v", frame)
	}
	if got := firstString(message, "role"); got != "user" {
		p.t.Fatalf("message role = %q, want user in %#v", got, frame)
	}
	if got := firstString(message, "content"); got != prompt {
		p.t.Fatalf("message content = %q, want %q in %#v", got, prompt, frame)
	}
}

func (p *claudePeer) expectControlResponse(requestID string) map[string]any {
	p.t.Helper()
	frame := p.readFrame()
	if got := firstString(frame, "type"); got != "control_response" {
		p.t.Fatalf("client frame type = %q, want control_response in %#v", got, frame)
	}
	if got := nestedString(frame, "response", "request_id"); got != requestID {
		p.t.Fatalf("control response request_id = %q, want %q in %#v", got, requestID, frame)
	}
	return frame
}

func (p *claudePeer) respondControlSuccess(request map[string]any, response any) {
	p.write(map[string]any{
		"type": "control_response",
		"response": map[string]any{
			"subtype":    "success",
			"request_id": firstString(request, "request_id"),
			"response":   response,
		},
	})
}

func (p *claudePeer) emitControlRequest(id, subtype string) {
	p.write(map[string]any{
		"type":       "control_request",
		"request_id": id,
		"request": map[string]any{
			"subtype": subtype,
		},
	})
}

func (p *claudePeer) emitSystem(sessionID, model string) {
	p.write(map[string]any{
		"type":       "system",
		"session_id": sessionID,
		"model":      model,
	})
}

func (p *claudePeer) emitAssistant(text, toolName string, input map[string]any) {
	p.write(map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "text", "text": text},
				map[string]any{"type": "tool_use", "name": toolName, "input": input},
			},
		},
	})
}

func (p *claudePeer) emitResult(subtype string, isError bool, result any) {
	p.write(map[string]any{
		"type":     "result",
		"subtype":  subtype,
		"is_error": isError,
		"result":   result,
	})
}

func (p *claudePeer) readFrame() map[string]any {
	p.t.Helper()
	return p.receiveFrame(p.startReadFrame(), 2*time.Second)
}

func (p *claudePeer) startReadFrame() <-chan frameResult {
	p.t.Helper()
	ch := make(chan frameResult, 1)
	go func() {
		if !p.scanner.Scan() {
			ch <- frameResult{err: p.scanner.Err()}
			return
		}
		var frame map[string]any
		if err := json.Unmarshal(p.scanner.Bytes(), &frame); err != nil {
			ch <- frameResult{err: err}
			return
		}
		ch <- frameResult{obj: frame}
	}()
	return ch
}

func (p *claudePeer) receiveFrame(ch <-chan frameResult, timeout time.Duration) map[string]any {
	p.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-ch:
		if result.err != nil {
			p.t.Fatalf("read client frame: %v", result.err)
		}
		if result.obj == nil {
			p.t.Fatal("client stdin closed before expected frame")
		}
		return result.obj
	case <-timer.C:
		p.t.Fatalf("timed out waiting for client frame after %s", timeout)
		return nil
	}
}

func (p *claudePeer) write(v any) {
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

func containsModel(events []engine.Event, model string) bool {
	for _, ev := range events {
		if ev.Type == engine.EventModelReported && ev.ModelReported == model {
			return true
		}
	}
	return false
}

func containsToolUse(events []engine.Event, name, textSub string) bool {
	for _, ev := range events {
		if ev.Type == engine.EventToolUse && ev.Name == name && strings.Contains(ev.Text, textSub) {
			return true
		}
	}
	return false
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

func assertContainsArg(t *testing.T, argv []string, arg string) {
	t.Helper()
	if !hasArg(argv, arg) {
		t.Fatalf("argv = %#v, want %q", argv, arg)
	}
}

func assertNotContainsArg(t *testing.T, argv []string, arg string) {
	t.Helper()
	if hasArg(argv, arg) {
		t.Fatalf("argv = %#v, did not want %q", argv, arg)
	}
}

func assertArgValue(t *testing.T, argv []string, arg, want string) {
	t.Helper()
	i := argIndex(argv, arg)
	if i < 0 || i+1 >= len(argv) {
		t.Fatalf("argv = %#v, missing value for %q", argv, arg)
	}
	if got := argv[i+1]; got != want {
		t.Fatalf("argv value after %q = %q, want %q in %#v", arg, got, want, argv)
	}
}

func hasArg(argv []string, arg string) bool {
	return argIndex(argv, arg) >= 0
}

func argIndex(argv []string, arg string) int {
	for i, got := range argv {
		if got == arg {
			return i
		}
	}
	return -1
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
