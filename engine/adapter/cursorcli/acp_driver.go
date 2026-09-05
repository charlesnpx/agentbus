package cursorcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

const acpRequestTimeout = 15 * time.Second

type acpDriver struct {
	binary         string
	requestTimeout time.Duration
	nextID         atomic.Uint64

	mu     sync.Mutex
	active map[*duplex.Conn]*activeACPTurn
}

type activeACPTurn struct {
	mu                 sync.Mutex
	sessionID          string
	write              bool
	interruptRequested bool
}

func (t *activeACPTurn) sendCancel(conn *duplex.Conn) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	err := conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"method":  "session/cancel",
		"params":  map[string]any{"sessionId": t.sessionID},
	})
	if err == nil {
		t.interruptRequested = true
	}
	return err
}

func (t *activeACPTurn) interruptWasRequested() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.interruptRequested
}

func newACPDriver(binary string) *acpDriver {
	return &acpDriver{
		binary:         binary,
		requestTimeout: acpRequestTimeout,
		active:         make(map[*duplex.Conn]*activeACPTurn),
	}
}

func (d *acpDriver) ExecSpec(_ string, opts engine.SessionOpts, _ engine.TurnInput) (command.ExecSpec, error) {
	if opts.Effort != "" {
		return command.ExecSpec{}, errors.New("cursor backend does not expose a supported effort control")
	}
	argv := []string{d.binary}
	if opts.Model != "" {
		argv = append(argv, "--model", opts.Model)
	}
	argv = append(argv, "acp")
	return command.ExecSpec{Argv: argv, Dir: opts.CWD}, nil
}

func (d *acpDriver) RunTurn(ctx context.Context, conn *duplex.Conn, resumeID string, opts engine.SessionOpts, input engine.TurnInput, emit duplex.EmitFunc) (string, error) {
	rpc := d.newRPC(conn, emit)
	capabilities, err := rpc.initialize(ctx)
	if err != nil {
		return "", fmt.Errorf("cursor ACP initialize: %w", err)
	}
	if err := rpc.authenticate(ctx); err != nil {
		return "", fmt.Errorf("cursor ACP authenticate: %w", err)
	}

	cwd, err := absoluteCWD(opts.CWD)
	if err != nil {
		return "", err
	}
	info, err := rpc.openSession(ctx, capabilities, resumeID, cwd)
	if err != nil {
		return resumeID, err
	}

	active := &activeACPTurn{sessionID: info.sessionID, write: input.Write}
	d.setActive(conn, active)
	defer d.clearActive(conn, active)

	if model := info.currentModel(); model != "" && emit != nil {
		emit(engine.Event{Type: engine.EventModelReported, ModelReported: model})
	}
	if err := rpc.setMode(ctx, info.sessionID, cursorMode(input.Write)); err != nil {
		return info.sessionID, fmt.Errorf("could not verify Cursor mode: %w", err)
	}

	observer := &acpTurnObserver{emit: emit}
	result, err := rpc.request(ctx, "session/prompt", map[string]any{
		"sessionId": info.sessionID,
		"prompt": []any{map[string]any{
			"type": "text",
			"text": input.Prompt,
		}},
	}, observer)
	// A prompt response can arrive without a terminal update for an observed
	// tool call. Flush those calls before the result so their observations are
	// retained even when ACP omits (or uses an unrecognized) terminal status.
	observer.flushToolUses()
	if err != nil {
		return info.sessionID, err
	}

	stopReason := firstString(resultMap(result), "stopReason")
	observer.emitEvent(engine.Event{Type: engine.EventResultMessage, Text: observer.resultText()})
	switch stopReason {
	case "end_turn":
		return info.sessionID, nil
	case "cancelled":
		if active.interruptWasRequested() {
			return info.sessionID, nil
		}
		return info.sessionID, fmt.Errorf("cursor ACP prompt was cancelled without a requested interrupt: %w", engine.ErrTurnInterrupted)
	default:
		return info.sessionID, fmt.Errorf("cursor ACP prompt completed with terminal stopReason %q", stopReason)
	}
}

func (d *acpDriver) Interrupt(ctx context.Context, conn *duplex.Conn) error {
	active := d.activeTurn(conn)
	if active == nil || active.sessionID == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return active.sendCancel(conn)
}

func (d *acpDriver) newRPC(conn *duplex.Conn, emit duplex.EmitFunc) *acpRPC {
	timeout := d.requestTimeout
	if timeout <= 0 {
		timeout = acpRequestTimeout
	}
	return &acpRPC{driver: d, conn: conn, emit: emit, requestTimeout: timeout}
}

func (d *acpDriver) nextRequestID() string {
	return fmt.Sprintf("agentbus-%d", d.nextID.Add(1))
}

func (d *acpDriver) setActive(conn *duplex.Conn, active *activeACPTurn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active == nil {
		d.active = make(map[*duplex.Conn]*activeACPTurn)
	}
	d.active[conn] = active
}

func (d *acpDriver) clearActive(conn *duplex.Conn, active *activeACPTurn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active[conn] == active {
		delete(d.active, conn)
	}
}

func (d *acpDriver) activeTurn(conn *duplex.Conn) *activeACPTurn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active[conn]
}

type acpRPC struct {
	driver         *acpDriver
	conn           *duplex.Conn
	emit           duplex.EmitFunc
	requestTimeout time.Duration
}

type acpAgentCapabilities struct {
	loadSession bool
}

func (c *acpRPC) initialize(ctx context.Context) (acpAgentCapabilities, error) {
	result, err := c.request(ctx, "initialize", map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"fs": map[string]any{
				"readTextFile":  false,
				"writeTextFile": false,
			},
			"terminal": false,
		},
		"clientInfo": map[string]any{
			"name":    "agentbus",
			"version": StreamSchema,
		},
	}, nil)
	if err != nil {
		return acpAgentCapabilities{}, err
	}
	object := resultMap(result)
	if protocolVersion(object["protocolVersion"]) != 1 {
		return acpAgentCapabilities{}, fmt.Errorf("unsupported ACP protocol version %v", object["protocolVersion"])
	}
	capabilities, _ := firstMap(object, "agentCapabilities")
	return acpAgentCapabilities{loadSession: boolValue(capabilities["loadSession"])}, nil
}

func (c *acpRPC) authenticate(ctx context.Context) error {
	_, err := c.request(ctx, "authenticate", map[string]any{"methodId": "cursor_login"}, nil)
	return err
}

func (c *acpRPC) openSession(ctx context.Context, capabilities acpAgentCapabilities, resumeID, cwd string) (acpSessionInfo, error) {
	params := map[string]any{"cwd": cwd, "mcpServers": []any{}}
	method := "session/new"
	if resumeID != "" {
		if !capabilities.loadSession {
			return acpSessionInfo{}, errors.New("Cursor ACP agent does not advertise loadSession")
		}
		method = "session/load"
		params["sessionId"] = resumeID
	}
	result, err := c.request(ctx, method, params, nil)
	if err != nil {
		return acpSessionInfo{}, err
	}
	return parseSessionInfo(result)
}

func (c *acpRPC) setMode(ctx context.Context, sessionID, modeID string) error {
	result, err := c.request(ctx, "session/set_mode", map[string]any{
		"sessionId": sessionID,
		"modeId":    modeID,
	}, nil)
	if err != nil {
		return err
	}
	if _, ok := result.(map[string]any); !ok {
		return errors.New("Cursor mode response was not an object")
	}
	return nil
}

func (c *acpRPC) request(ctx context.Context, method string, params any, observer *acpTurnObserver) (any, error) {
	id := c.driver.nextRequestID()
	if params == nil {
		params = map[string]any{}
	}
	if err := c.conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}); err != nil {
		return nil, err
	}

	requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	for {
		frame, err := c.nextFrame(requestCtx)
		if err != nil {
			return nil, fmt.Errorf("%s response: %w", method, err)
		}
		if handled, err := c.handleServerRequest(frame); handled || err != nil {
			if err != nil {
				return nil, err
			}
			continue
		}
		if isResponseTo(frame.Object, id) {
			return responseResult(frame.Object)
		}
		if isResponse(frame.Object) {
			continue
		}
		if observer != nil {
			observer.handle(frame)
		}
	}
}

func (c *acpRPC) nextFrame(ctx context.Context) (duplex.Frame, error) {
	frames := c.conn.Frames()
	decodeErrs := c.conn.DecodeErrors()
	for frames != nil || decodeErrs != nil {
		select {
		case frame, ok := <-frames:
			if !ok {
				if err := pendingDecodeError(c.conn); err != nil {
					return duplex.Frame{}, err
				}
				frames = nil
				continue
			}
			if frame.Err != nil {
				return duplex.Frame{}, cursorTransportReadError(frame.Err)
			}
			return frame, nil
		case err, ok := <-decodeErrs:
			if !ok {
				decodeErrs = nil
				continue
			}
			if err != nil {
				return duplex.Frame{}, cursorTransportReadError(err)
			}
		case <-ctx.Done():
			return duplex.Frame{}, ctx.Err()
		}
	}
	return duplex.Frame{}, duplex.ErrBackendExitedBeforeTerminal
}

func cursorTransportReadError(err error) error {
	var overlong *duplex.OverlongFrameError
	if errors.As(err, &overlong) {
		return fmt.Errorf("cursor ACP transport: %w: %w", engine.ErrTransportFrameTooLarge, err)
	}
	return err
}

// Cursor ACP v1 implements only the qualified core permission reverse request. Cursor extension requests
// are unsupported until observed during qualification or dogfood; unsupported requests fail explicitly
// with JSON-RPC method-not-found.
func (c *acpRPC) handleServerRequest(frame duplex.Frame) (bool, error) {
	method := firstString(frame.Object, "method")
	if method == "" {
		return false, nil
	}
	id, hasID := frame.Object["id"]
	if !hasID {
		return false, nil
	}
	params := paramsMap(frame.Object)
	if method == "session/request_permission" {
		return true, c.handlePermissionRequest(id, params)
	}
	return true, c.writeError(id, -32601, fmt.Sprintf("unsupported server request: %s", method))
}

func (c *acpRPC) handlePermissionRequest(id any, params map[string]any) error {
	active := c.driver.activeTurn(c.conn)
	if active == nil {
		return c.permissionTerminalError(id, "Cursor permission request arrived without an active session")
	}
	active.mu.Lock()
	defer active.mu.Unlock()
	if active.interruptRequested {
		return c.writeResult(id, map[string]any{
			"outcome": map[string]any{"outcome": "cancelled"},
		})
	}

	wantKind := "allow_once"
	if !active.write {
		wantKind = "reject_once"
	}
	for _, raw := range anySlice(params["options"]) {
		option, ok := raw.(map[string]any)
		if !ok || firstString(option, "kind") != wantKind {
			continue
		}
		optionID, ok := option["optionId"]
		if !ok || strings.TrimSpace(stringValue(optionID)) == "" {
			continue
		}
		return c.writeResult(id, map[string]any{
			"outcome": map[string]any{
				"outcome":  "selected",
				"optionId": optionID,
			},
		})
	}
	return c.permissionTerminalError(id, fmt.Sprintf("Cursor permission request lacks offered %s option", wantKind))
}

func (c *acpRPC) permissionTerminalError(id any, message string) error {
	if err := c.writeError(id, -32602, message); err != nil {
		return errors.Join(errors.New(message), err)
	}
	return errors.New(message)
}

func (c *acpRPC) writeResult(id any, result any) error {
	return c.conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (c *acpRPC) writeError(id any, code int, message string) error {
	return c.conn.WriteJSON(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

type acpSessionInfo struct {
	sessionID string
	models    map[string]any
}

func parseSessionInfo(result any) (acpSessionInfo, error) {
	object, ok := result.(map[string]any)
	if !ok {
		return acpSessionInfo{}, errors.New("Cursor session response was not an object")
	}
	info := acpSessionInfo{
		sessionID: firstString(object, "sessionId"),
	}
	info.models, _ = firstMap(object, "models")
	if info.sessionID == "" {
		return acpSessionInfo{}, errors.New("Cursor session response missing sessionId")
	}
	return info, nil
}

func (i acpSessionInfo) currentModel() string {
	return firstString(i.models, "currentModelId")
}

type acpTurnObserver struct {
	emit               duplex.EmitFunc
	text               strings.Builder
	pendingToolCalls   map[string]*acpToolCall
	pendingToolCallIDs []string
	emittedToolCallIDs map[string]struct{}
}

type acpToolCall struct {
	id                     string
	title                  string
	kind                   string
	status                 string
	workspaceWriteObserved bool
}

func (o *acpTurnObserver) handle(frame duplex.Frame) {
	if firstString(frame.Object, "method") != "session/update" {
		return
	}
	update := sessionUpdate(paramsMap(frame.Object))
	switch firstString(update, "sessionUpdate") {
	case "agent_message_chunk":
		content, _ := firstMap(update, "content")
		if text, _ := content["text"].(string); text != "" {
			o.text.WriteString(text)
			o.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text})
		}
	case "tool_call", "tool_call_update":
		o.handleToolUse(update)
	}
}

func (o *acpTurnObserver) handleToolUse(update map[string]any) {
	toolCallID := firstString(update, "toolCallId")
	if toolCallID == "" {
		// ACP tool-call lifecycles are keyed by toolCallId. A malformed frame
		// without one still proves activity, but cannot safely be correlated into
		// a transcript item.
		o.emitEvent(engine.Event{Type: engine.EventProgress})
		return
	}
	if _, emitted := o.emittedToolCallIDs[toolCallID]; emitted {
		return
	}
	if o.pendingToolCalls == nil {
		o.pendingToolCalls = make(map[string]*acpToolCall)
	}
	toolCall := o.pendingToolCalls[toolCallID]
	if toolCall == nil {
		toolCall = &acpToolCall{id: toolCallID}
		o.pendingToolCalls[toolCallID] = toolCall
		o.pendingToolCallIDs = append(o.pendingToolCallIDs, toolCallID)
	}
	toolCall.absorb(update)

	// The Cursor fixture establishes completed as the terminal status. Keep all
	// other statuses pending until the prompt finishes so an absent or
	// unrecognized terminal frame is still recorded once.
	if toolCall.status == "completed" {
		o.emitToolCall(toolCall)
		delete(o.pendingToolCalls, toolCallID)
		if o.emittedToolCallIDs == nil {
			o.emittedToolCallIDs = make(map[string]struct{})
		}
		o.emittedToolCallIDs[toolCallID] = struct{}{}
		return
	}
	o.emitEvent(engine.Event{Type: engine.EventProgress})
}

func (o *acpTurnObserver) flushToolUses() {
	for _, toolCallID := range o.pendingToolCallIDs {
		toolCall, ok := o.pendingToolCalls[toolCallID]
		if !ok {
			continue
		}
		o.emitToolCall(toolCall)
	}
}

func (c *acpToolCall) absorb(update map[string]any) {
	if title := firstString(update, "title"); title != "" {
		c.title = title
	}
	if kind := firstString(update, "kind"); kind != "" {
		c.kind = kind
	}
	if status := firstString(update, "status"); status != "" {
		c.status = status
	}
	if hasACPDiffContent(update["content"]) {
		c.workspaceWriteObserved = true
	}
}

func (o *acpTurnObserver) emitToolCall(toolCall *acpToolCall) {
	name := toolCall.title
	if name == "" {
		name = toolCall.kind
	}
	if name == "" {
		name = "tool"
	}
	metadata := map[string]any{"toolCallId": toolCall.id}
	if toolCall.kind != "" {
		metadata["kind"] = toolCall.kind
	}
	if toolCall.status != "" {
		metadata["status"] = toolCall.status
	}
	event := engine.Event{
		Type:                       engine.EventToolUse,
		Name:                       name,
		Metadata:                   metadata,
		ObservedWorkspaceWriteItem: toolCall.workspaceWriteObserved,
	}
	if !toolCall.workspaceWriteObserved {
		event.Text = name
		if toolCall.status != "" {
			event.Text += " (" + toolCall.status + ")"
		}
	}
	o.emitEvent(event)
}

func hasACPDiffContent(content any) bool {
	switch content := content.(type) {
	case map[string]any:
		return firstString(content, "type") == "diff"
	case []any:
		for _, item := range content {
			if block, ok := item.(map[string]any); ok && firstString(block, "type") == "diff" {
				return true
			}
		}
	}
	return false
}

func (o *acpTurnObserver) resultText() string {
	return o.text.String()
}

func (o *acpTurnObserver) emitEvent(event engine.Event) {
	if o.emit != nil {
		o.emit(event)
	}
}

func absoluteCWD(cwd string) (string, error) {
	if strings.TrimSpace(cwd) == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	return filepath.Abs(cwd)
}

func cursorMode(write bool) string {
	if write {
		return "agent"
	}
	return "plan"
}

func sessionUpdate(params map[string]any) map[string]any {
	if update, ok := params["update"].(map[string]any); ok {
		return update
	}
	return params
}

func responseResult(object map[string]any) (any, error) {
	if rawError, ok := object["error"]; ok {
		message := "request failed"
		if errObject, ok := rawError.(map[string]any); ok {
			if value := firstString(errObject, "message"); value != "" {
				message = value
			}
		} else if value := strings.TrimSpace(stringValue(rawError)); value != "" {
			message = value
		}
		return nil, errors.New(message)
	}
	result, ok := object["result"]
	if !ok {
		return nil, errors.New("response missing result")
	}
	return result, nil
}

func paramsMap(object map[string]any) map[string]any {
	if params, ok := object["params"].(map[string]any); ok {
		return params
	}
	return object
}

func resultMap(result any) map[string]any {
	object, _ := result.(map[string]any)
	return object
}

func firstMap(object map[string]any, keys ...string) (map[string]any, bool) {
	for _, key := range keys {
		if value, ok := object[key].(map[string]any); ok {
			return value, true
		}
	}
	return nil, false
}

func firstString(object map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := object[key]; ok {
			if text := strings.TrimSpace(stringValue(value)); text != "" {
				return text
			}
		}
	}
	return ""
}

func stringValue(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case float64:
		if value == float64(int64(value)) {
			return strconv.FormatInt(int64(value), 10)
		}
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		return ""
	}
}

func anySlice(value any) []any {
	values, _ := value.([]any)
	return values
}

func boolValue(value any) bool {
	b, _ := value.(bool)
	return b
}

func protocolVersion(value any) int {
	switch value := value.(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		parsed, _ := strconv.Atoi(value.String())
		return parsed
	default:
		return 0
	}
}

func isResponse(object map[string]any) bool {
	_, hasID := object["id"]
	return hasID && firstString(object, "method") == ""
}

func isResponseTo(object map[string]any, id string) bool {
	return isResponse(object) && requestIDKey(object["id"]) == id
}

func requestIDKey(id any) string {
	switch value := id.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case float64:
		if value == float64(int64(value)) {
			return strconv.FormatInt(int64(value), 10)
		}
	}
	return fmt.Sprint(id)
}

func pendingDecodeError(conn *duplex.Conn) error {
	select {
	case err, ok := <-conn.DecodeErrors():
		if ok {
			return err
		}
	default:
	}
	return nil
}
