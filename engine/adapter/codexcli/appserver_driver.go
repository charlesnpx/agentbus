package codexcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

const appServerRequestTimeout = 15 * time.Second

type appServerDriver struct {
	binary         string
	writePolicy    WritePolicy
	requestTimeout time.Duration
	nextID         atomic.Uint64

	mu     sync.Mutex
	active map[*duplex.Conn]*activeAppServerTurn
}

type activeAppServerTurn struct {
	threadID           string
	turnID             string
	interruptRequested atomic.Bool
}

func newAppServerDriver(binary string, writePolicy WritePolicy) *appServerDriver {
	return &appServerDriver{
		binary:         binary,
		writePolicy:    writePolicy,
		requestTimeout: appServerRequestTimeout,
		active:         make(map[*duplex.Conn]*activeAppServerTurn),
	}
}

func (d *appServerDriver) ExecSpec(_ string, opts engine.SessionOpts, _ engine.TurnInput) (command.ExecSpec, error) {
	return command.ExecSpec{
		Argv: []string{d.binaryName(), "app-server"},
		Dir:  opts.CWD,
	}, nil
}

func (d *appServerDriver) RunTurn(ctx context.Context, conn *duplex.Conn, resumeID string, opts engine.SessionOpts, input engine.TurnInput, emit duplex.EmitFunc) (string, error) {
	rpc := d.newRPC(conn, emit)
	if err := rpc.handshake(ctx); err != nil {
		return "", fmt.Errorf("codex app-server initialize: %w", err)
	}

	threadID := resumeID
	var threadResult any
	var err error
	if resumeID == "" {
		threadResult, err = rpc.request(ctx, "thread/start", threadParams(opts, input, ""), nil)
	} else {
		threadResult, err = rpc.request(ctx, "thread/resume", threadParams(opts, input, resumeID), nil)
	}
	if err != nil {
		return "", err
	}
	if id := extractThreadID(threadResult); id != "" {
		threadID = id
	}
	if threadID == "" {
		return "", errors.New("codex app-server thread response missing thread id")
	}

	observer := &turnObserver{emit: emit}
	// A turn can stop after item/started without ever delivering its matching
	// item/completed notification. Flush on every return after turn/start begins,
	// including transport errors and cancellation paths that bypass completion.
	defer observer.flushPendingToolItems()
	turnResult, err := rpc.request(ctx, "turn/start", turnStartParams(threadID, opts, input, d.writePolicy), observer)
	if err != nil {
		return threadID, err
	}
	turnID := extractTurnID(turnResult)
	if turnID == "" {
		return threadID, errors.New("codex app-server turn/start response missing turn id")
	}

	active := &activeAppServerTurn{threadID: threadID, turnID: turnID}
	d.setActive(conn, active)
	defer d.clearActive(conn, active)

	if observer.completion != nil {
		return finishTurnCompletion(threadID, active, observer)
	}
	for {
		frame, err := rpc.nextFrame(ctx)
		if err != nil {
			return threadID, err
		}
		if handled, err := rpc.handleServerRequest(frame); handled || err != nil {
			if err != nil {
				return threadID, err
			}
			continue
		}
		if isResponse(frame.Object) {
			continue
		}
		if observer.handle(frame) {
			return finishTurnCompletion(threadID, active, observer)
		}
		rpc.handleNotification(frame)
	}
}

func (d *appServerDriver) Interrupt(ctx context.Context, conn *duplex.Conn) error {
	active := d.activeTurn(conn)
	if active == nil {
		return nil
	}
	active.interruptRequested.Store(true)
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return conn.WriteJSON(map[string]any{
		"id":     d.nextRequestID(),
		"method": "turn/interrupt",
		"params": map[string]any{
			"threadId": active.threadID,
			"turnId":   active.turnID,
		},
	})
}

func (d *appServerDriver) binaryName() string {
	if strings.TrimSpace(d.binary) == "" {
		return "codex"
	}
	return d.binary
}

func (d *appServerDriver) nextRequestID() string {
	return fmt.Sprintf("agentbus-%d", d.nextID.Add(1))
}

func (d *appServerDriver) newRPC(conn *duplex.Conn, emit duplex.EmitFunc) *appServerRPC {
	timeout := d.requestTimeout
	if timeout <= 0 {
		timeout = appServerRequestTimeout
	}
	return &appServerRPC{driver: d, conn: conn, emit: emit, requestTimeout: timeout}
}

func (d *appServerDriver) setActive(conn *duplex.Conn, active *activeAppServerTurn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active == nil {
		d.active = make(map[*duplex.Conn]*activeAppServerTurn)
	}
	d.active[conn] = active
}

func (d *appServerDriver) clearActive(conn *duplex.Conn, active *activeAppServerTurn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.active[conn] == active {
		delete(d.active, conn)
	}
}

func (d *appServerDriver) activeTurn(conn *duplex.Conn) *activeAppServerTurn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.active[conn]
}

type appServerRPC struct {
	driver         *appServerDriver
	conn           *duplex.Conn
	emit           duplex.EmitFunc
	requestTimeout time.Duration
}

func (c *appServerRPC) handshake(ctx context.Context) error {
	_, err := c.request(ctx, "initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "agentbus",
			"title":   "Agent Bus",
			"version": StreamSchema,
		},
		"capabilities": map[string]any{
			"experimentalApi": true,
		},
	}, nil)
	if err != nil {
		return err
	}
	return c.conn.WriteJSON(map[string]any{"method": "initialized"})
}

func (c *appServerRPC) request(ctx context.Context, method string, params any, observer *turnObserver) (any, error) {
	id := c.driver.nextRequestID()
	if params == nil {
		params = map[string]any{}
	}
	if err := c.conn.WriteJSON(map[string]any{
		"id":     id,
		"method": method,
		"params": params,
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
		if observer != nil && observer.handle(frame) {
			continue
		}
		c.handleNotification(frame)
	}
}

func (c *appServerRPC) nextFrame(ctx context.Context) (duplex.Frame, error) {
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
				var overlong *duplex.OverlongFrameError
				if errors.As(frame.Err, &overlong) && codexOverlongFrameCanBeSkipped(overlong) {
					continue
				}
				return duplex.Frame{}, codexTransportReadError(frame.Err)
			}
			return frame, nil
		case err, ok := <-decodeErrs:
			if !ok {
				decodeErrs = nil
				continue
			}
			if err != nil {
				return duplex.Frame{}, codexTransportReadError(err)
			}
		case <-ctx.Done():
			return duplex.Frame{}, ctx.Err()
		}
	}
	return duplex.Frame{}, duplex.ErrBackendExitedBeforeTerminal
}

func codexTransportReadError(err error) error {
	var overlong *duplex.OverlongFrameError
	if errors.As(err, &overlong) {
		return fmt.Errorf("codex app-server transport: %w: %w", engine.ErrTransportFrameTooLarge, err)
	}
	return err
}

// codexOverlongFrameCanBeSkipped accepts only a prefix that identifies a
// known non-terminal Codex notification. An absent or malformed envelope is
// treated as a transport failure rather than risking a successful turn after
// an unrecognized result-bearing frame was discarded.
func codexOverlongFrameCanBeSkipped(frame *duplex.OverlongFrameError) bool {
	if frame == nil || frame.DuplicateDiscriminator {
		return false
	}
	method, field := duplex.FrameTypeFromPrefix(frame.Prefix)
	if method == "" || field != "method" {
		return false
	}
	switch method {
	case "turn/completed", "task_complete":
		return false
	case "item/started", "warning", "error", "config/warning", "guardian/warning":
		return true
	case "item/completed":
		switch codexCompletedItemTypeFromPrefix(frame.Prefix) {
		case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall":
			return true
		}
		return false
	default:
		return false
	}
}

// codexCompletedItemTypeFromPrefix extracts only the structural
// params.item.type path from a bounded prefix. It stops as soon as that string
// is complete; an incomplete or unexpected path proves nothing and returns an
// empty type so the caller fails the turn rather than losing possible result
// text.
func codexCompletedItemTypeFromPrefix(prefix []byte) string {
	decoder := json.NewDecoder(bytes.NewReader(prefix))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return ""
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return ""
		}
		keyText, ok := key.(string)
		if !ok {
			return ""
		}
		if keyText != "params" {
			if !skipPrefixJSONValue(decoder) {
				return ""
			}
			continue
		}
		return codexItemTypeFromParamsPrefix(decoder)
	}
	return ""
}

func codexItemTypeFromParamsPrefix(decoder *json.Decoder) string {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return ""
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return ""
		}
		keyText, ok := key.(string)
		if !ok {
			return ""
		}
		if keyText != "item" {
			if !skipPrefixJSONValue(decoder) {
				return ""
			}
			continue
		}
		return codexItemTypeFromItemPrefix(decoder)
	}
	return ""
}

func codexItemTypeFromItemPrefix(decoder *json.Decoder) string {
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return ""
	}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return ""
		}
		keyText, ok := key.(string)
		if !ok {
			return ""
		}
		if keyText == "type" {
			var itemType string
			if err := decoder.Decode(&itemType); err != nil {
				return ""
			}
			return itemType
		}
		if !skipPrefixJSONValue(decoder) {
			return ""
		}
	}
	return ""
}

func skipPrefixJSONValue(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return true
	}
	switch delimiter {
	case '{':
		for decoder.More() {
			if _, err := decoder.Token(); err != nil || !skipPrefixJSONValue(decoder) {
				return false
			}
		}
		_, err := decoder.Token()
		return err == nil
	case '[':
		for decoder.More() {
			if !skipPrefixJSONValue(decoder) {
				return false
			}
		}
		_, err := decoder.Token()
		return err == nil
	default:
		return false
	}
}

func (c *appServerRPC) handleServerRequest(frame duplex.Frame) (bool, error) {
	method := firstString(frame.Object, "method")
	if method == "" {
		return false, nil
	}
	id, hasID := frame.Object["id"]
	if !hasID {
		return false, nil
	}
	params := paramsMap(frame.Object)
	if result, ok := declineResponse(method, params); ok {
		return true, c.conn.WriteJSON(map[string]any{"id": id, "result": result})
	}
	return true, c.conn.WriteJSON(map[string]any{
		"id": id,
		"error": map[string]any{
			"code":    -32601,
			"message": fmt.Sprintf("unsupported server request: %s", method),
		},
	})
}

func (c *appServerRPC) handleNotification(frame duplex.Frame) {
	if c.emit == nil {
		return
	}
	method := firstString(frame.Object, "method")
	if method != "warning" && method != "error" && method != "config/warning" && method != "guardian/warning" {
		return
	}
	if text := textFrom(paramsMap(frame.Object)); text != "" {
		c.emit(engine.Event{Type: engine.EventWarning, Text: text, Metadata: frame.Object})
	}
}

type turnObserver struct {
	emit               duplex.EmitFunc
	lastCompletedAgent string
	agentDeltaSeen     map[string]bool
	agentDeltaText     map[string]*strings.Builder
	lastDeltaAgentItem string
	pendingToolItems   map[string]pendingToolItem
	pendingToolItemIDs []string
	emittedToolItemIDs map[string]struct{}
	completion         *turnCompletion
}

type pendingToolItem struct {
	item     map[string]any
	metadata map[string]any
}

type turnCompletion struct {
	threadID  string
	status    string
	error     string
	errorInfo string
}

func (o *turnObserver) handle(frame duplex.Frame) bool {
	method := firstString(frame.Object, "method", "type")
	payload := paramsMap(frame.Object)
	switch method {
	case "item/agentMessage/delta":
		if text := firstString(payload, "delta", "text"); text != "" {
			o.appendAgentDelta(payload, text)
			o.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: frame.Object})
		}
	case "item/started", "item/completed":
		o.handleItem(method, payload, frame.Object)
	case "turn/completed", "task_complete":
		o.complete(payload)
		return true
	}
	return false
}

func (o *turnObserver) handleItem(method string, payload map[string]any, metadata map[string]any) {
	item, ok := firstMap(payload, "item", "payload", "response_item")
	if !ok {
		item = payload
	}
	kind := normalizeKind(firstString(item, "type"))
	switch kind {
	case "agentmessage", "assistantmessage", "message":
		if method == "item/completed" {
			if text := textFrom(item); text != "" {
				o.lastCompletedAgent = text
				if !o.hasAgentDelta(payload) {
					o.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: metadata})
				}
			}
		}
	case "filechange", "commandexecution", "mcptoolcall", "dynamictoolcall":
		itemID := toolItemID(payload, item)
		if method == "item/started" {
			o.recordPendingToolItem(itemID, item, metadata)
			o.emitEvent(engine.Event{Type: engine.EventProgress})
			return
		}
		if !o.completePendingToolItem(itemID) {
			return
		}
		o.emitToolItem(item, metadata)
	}
}

func (o *turnObserver) recordPendingToolItem(itemID string, item map[string]any, metadata map[string]any) {
	// The app-server fixtures identify item lifecycles by params.item.id. Do not
	// collapse id-less frames under a shared fallback: two concurrent tools of the
	// same kind would then be indistinguishable.
	if itemID == "" || o.toolItemWasEmitted(itemID) {
		return
	}
	if o.pendingToolItems == nil {
		o.pendingToolItems = make(map[string]pendingToolItem)
	}
	if _, exists := o.pendingToolItems[itemID]; exists {
		return
	}
	o.pendingToolItems[itemID] = pendingToolItem{item: item, metadata: metadata}
	o.pendingToolItemIDs = append(o.pendingToolItemIDs, itemID)
}

func (o *turnObserver) completePendingToolItem(itemID string) bool {
	if itemID == "" {
		// A completed frame that arrived without a started frame is still a
		// terminal observation, as it was before lifecycle retention.
		return true
	}
	if o.toolItemWasEmitted(itemID) {
		return false
	}
	delete(o.pendingToolItems, itemID)
	o.markToolItemEmitted(itemID)
	return true
}

func (o *turnObserver) flushPendingToolItems() {
	for _, itemID := range o.pendingToolItemIDs {
		pending, ok := o.pendingToolItems[itemID]
		if !ok {
			continue
		}
		delete(o.pendingToolItems, itemID)
		o.markToolItemEmitted(itemID)
		o.emitToolItem(pending.item, pending.metadata)
	}
}

func (o *turnObserver) toolItemWasEmitted(itemID string) bool {
	_, emitted := o.emittedToolItemIDs[itemID]
	return emitted
}

func (o *turnObserver) markToolItemEmitted(itemID string) {
	if o.emittedToolItemIDs == nil {
		o.emittedToolItemIDs = make(map[string]struct{})
	}
	o.emittedToolItemIDs[itemID] = struct{}{}
}

func (o *turnObserver) emitToolItem(item map[string]any, metadata map[string]any) {
	switch normalizeKind(firstString(item, "type")) {
	case "filechange":
		// File-change observations intentionally carry no item text. The service
		// retains their count, never paths or contents.
		o.emitEvent(engine.Event{
			Type:                       engine.EventToolUse,
			Name:                       toolName(item),
			Metadata:                   metadata,
			ObservedWorkspaceWriteItem: true,
		})
	case "commandexecution", "mcptoolcall", "dynamictoolcall":
		o.emitEvent(engine.Event{
			Type:     engine.EventToolUse,
			Name:     toolName(item),
			Text:     toolText(item),
			Metadata: metadata,
		})
	}
}

func (o *turnObserver) complete(payload map[string]any) {
	turn, _ := firstMap(payload, "turn")
	if turn != nil {
		if text := lastAgentMessageFromTurn(turn); text != "" {
			o.lastCompletedAgent = text
		}
	}
	if text := firstString(payload, "last_agent_message", "lastAgentMessage"); text != "" {
		o.lastCompletedAgent = text
	}
	status := firstString(payload, "status")
	if status == "" && turn != nil {
		status = firstString(turn, "status")
	}
	errText := ""
	errInfo := ""
	for _, completion := range []map[string]any{turn, payload} {
		if completion == nil {
			continue
		}
		errObj, ok := firstMap(completion, "error")
		if !ok {
			continue
		}
		if errText == "" {
			errText = textFrom(errObj)
		}
		if errInfo == "" {
			errInfo = firstString(errObj, "codex_error_info", "codexErrorInfo")
		}
	}
	if errText == "" {
		errText = textFrom(payload)
	}
	o.completion = &turnCompletion{
		threadID:  firstString(payload, "threadId", "thread_id"),
		status:    status,
		error:     errText,
		errorInfo: errInfo,
	}
}

func (o *turnObserver) emitEvent(ev engine.Event) {
	if o.emit != nil {
		o.emit(ev)
	}
}

const defaultAgentDeltaItemID = "\x00agent-delta-default"

func (o *turnObserver) hasAgentDelta(payload map[string]any) bool {
	return o.agentDeltaSeen[agentDeltaItemIDOrDefault(payload)]
}

func (o *turnObserver) appendAgentDelta(payload map[string]any, text string) {
	itemID := agentDeltaItemIDOrDefault(payload)
	if o.agentDeltaSeen == nil {
		o.agentDeltaSeen = make(map[string]bool)
	}
	o.agentDeltaSeen[itemID] = true
	if o.agentDeltaText == nil {
		o.agentDeltaText = make(map[string]*strings.Builder)
	}
	builder := o.agentDeltaText[itemID]
	if builder == nil {
		builder = &strings.Builder{}
		o.agentDeltaText[itemID] = builder
	}
	builder.WriteString(text)
	o.lastDeltaAgentItem = itemID
}

func (o *turnObserver) resultText() string {
	if o.lastCompletedAgent != "" {
		return o.lastCompletedAgent
	}
	if o.lastDeltaAgentItem == "" {
		return ""
	}
	if builder := o.agentDeltaText[o.lastDeltaAgentItem]; builder != nil {
		return builder.String()
	}
	return ""
}

func finishTurnCompletion(threadID string, active *activeAppServerTurn, observer *turnObserver) (string, error) {
	// Flush before emitting a result or returning an error so failed, empty, and
	// interrupted completions retain observations in their original turn order.
	observer.flushPendingToolItems()
	completion := observer.completion
	if completion == nil {
		return threadID, nil
	}
	if completion.threadID != "" {
		threadID = completion.threadID
	}
	switch completion.status {
	case "completed":
		observer.emitEvent(engine.Event{Type: engine.EventResultMessage, Text: observer.resultText()})
		return threadID, nil
	case "failed", "":
		msg := strings.TrimSpace(completion.error)
		if completion.status == "" && msg == "" {
			observer.emitEvent(engine.Event{Type: engine.EventResultMessage, Text: observer.resultText()})
			return threadID, nil
		}
		if msg == "" {
			msg = "turn failed"
		}
		if isProviderOverloaded(completion.errorInfo, msg) {
			return threadID, fmt.Errorf("codex app-server provider overload: %s: %w", msg, engine.ErrProviderOverloaded)
		}
		return threadID, fmt.Errorf("codex app-server turn failed: %s", msg)
	case "interrupted":
		if active != nil && active.interruptRequested.Load() {
			return threadID, nil
		}
		return threadID, fmt.Errorf("codex app-server turn interrupted before completion: %w", engine.ErrTurnInterrupted)
	default:
		return threadID, fmt.Errorf("codex app-server turn completed with unsupported status %q", completion.status)
	}
}

// isProviderOverloaded recognizes a confirmed overload code or a capacity message.
// An unrecognized errorInfo is no signal and must fall through to the message
// heuristic; providers can introduce new codes without suppressing it.
//
// The fall-through is narrowed to the human-language phrase. The other two
// fragments are code spellings, which only appear in a message when a code has
// leaked into prose, and provider messages quote caller-supplied identifiers: a
// model-unavailable failure reporting an unknown model named "server_overloaded"
// would otherwise be reported as capacity, telling an operator to retry when the
// model is what must change. When no code is supplied at all, all three
// fragments still apply exactly as before.
func isProviderOverloaded(errorInfo, message string) bool {
	errorInfo = normalizeProviderErrorInfo(errorInfo)
	if errorInfo == "serveroverloaded" {
		return true
	}
	message = strings.ToLower(message)
	if strings.Contains(message, "at capacity") {
		return true
	}
	if errorInfo != "" {
		return false
	}
	return strings.Contains(message, "server overloaded") ||
		strings.Contains(message, "server_overloaded")
}

func normalizeProviderErrorInfo(errorInfo string) string {
	errorInfo = strings.TrimSpace(errorInfo)
	errorInfo = strings.ReplaceAll(errorInfo, "_", "")
	errorInfo = strings.ReplaceAll(errorInfo, "-", "")
	return strings.ToLower(errorInfo)
}

func threadParams(opts engine.SessionOpts, input engine.TurnInput, resumeID string) map[string]any {
	params := map[string]any{
		"approvalPolicy": "never",
		"sandbox":        sandboxMode(input.Write),
	}
	if opts.CWD != "" {
		params["cwd"] = opts.CWD
	}
	if opts.Model != "" {
		params["model"] = opts.Model
	}
	if resumeID != "" {
		params["threadId"] = resumeID
	}
	return params
}

func turnStartParams(threadID string, opts engine.SessionOpts, input engine.TurnInput, writePolicy WritePolicy) map[string]any {
	params := map[string]any{
		"threadId":       threadID,
		"approvalPolicy": "never",
		"sandboxPolicy":  sandboxPolicy(input.Write, opts.CWD, opts.WriteSandboxRoot, writePolicy),
		"input": []map[string]any{{
			"type": "text",
			"text": input.Prompt,
		}},
	}
	if opts.CWD != "" {
		params["cwd"] = opts.CWD
	}
	if opts.Model != "" {
		params["model"] = opts.Model
	}
	if opts.Effort != "" {
		params["effort"] = opts.Effort
	}
	return params
}

func sandboxMode(write bool) string {
	if write {
		return "workspace-write"
	}
	return "read-only"
}

func sandboxPolicy(write bool, cwd, writeSandboxRoot string, writePolicy WritePolicy) map[string]any {
	if !write {
		return map[string]any{
			"type":          "readOnly",
			"networkAccess": false,
		}
	}
	if writePolicy == WritePolicyTrusted {
		return map[string]any{
			"type": "dangerFullAccess",
		}
	}
	policy := map[string]any{
		"type":          "workspaceWrite",
		"networkAccess": writePolicy == WritePolicyWorkspaceNetwork,
	}
	roots := make([]string, 0, 2)
	if root := workspaceRoot(cwd); root != "" {
		roots = append(roots, root)
	}
	if writeSandboxRoot != "" {
		roots = append(roots, writeSandboxRoot)
	}
	if len(roots) > 0 {
		policy["writableRoots"] = roots
	}
	return policy
}

func workspaceRoot(cwd string) string {
	if cwd != "" {
		return cwd
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return ""
}

func declineResponse(method string, params map[string]any) (map[string]any, bool) {
	switch method {
	case "item/commandExecution/requestApproval":
		return map[string]any{"decision": "decline"}, true
	case "item/fileChange/requestApproval":
		return map[string]any{"decision": "decline"}, true
	case "item/permissions/requestApproval":
		return map[string]any{
			"permissions":      map[string]any{},
			"scope":            "turn",
			"strictAutoReview": false,
		}, true
	case "mcpServer/elicitation/request":
		return map[string]any{"action": "decline", "content": nil}, true
	case "tool/requestUserInput", "item/tool/requestUserInput":
		return map[string]any{"answers": declineAnswers(params)}, true
	default:
		return nil, false
	}
}

func declineAnswers(params map[string]any) map[string]any {
	answers := make(map[string]any)
	for _, question := range anySlice(params["questions"]) {
		q, ok := question.(map[string]any)
		if !ok {
			continue
		}
		if id := firstString(q, "id"); id != "" {
			answers[id] = map[string]any{"answers": []string{}}
		}
	}
	return answers
}

func responseResult(obj map[string]any) (any, error) {
	if errObj, ok := firstMap(obj, "error"); ok {
		msg := textFrom(errObj)
		if msg == "" {
			msg = "request failed"
		}
		return nil, errors.New(msg)
	}
	result, ok := obj["result"]
	if !ok {
		return nil, errors.New("response missing result")
	}
	return result, nil
}

func extractThreadID(result any) string {
	obj, ok := result.(map[string]any)
	if !ok {
		return strings.TrimSpace(stringValue(result))
	}
	if id := firstString(obj, "threadId", "thread_id", "id"); id != "" {
		return id
	}
	if thread, ok := firstMap(obj, "thread"); ok {
		return firstString(thread, "id", "threadId", "thread_id")
	}
	return ""
}

func extractTurnID(result any) string {
	obj, ok := result.(map[string]any)
	if !ok {
		return strings.TrimSpace(stringValue(result))
	}
	if id := firstString(obj, "turnId", "turn_id", "id"); id != "" {
		return id
	}
	if turn, ok := firstMap(obj, "turn"); ok {
		return firstString(turn, "id", "turnId", "turn_id")
	}
	return ""
}

func lastAgentMessageFromTurn(turn map[string]any) string {
	var last string
	for _, raw := range anySlice(turn["items"]) {
		item, ok := raw.(map[string]any)
		if !ok || normalizeKind(firstString(item, "type")) != "agentmessage" {
			continue
		}
		if text := textFrom(item); text != "" {
			last = text
		}
	}
	return last
}

func toolName(item map[string]any) string {
	name := firstString(item, "tool", "name", "command", "server", "namespace")
	if server := firstString(item, "server"); server != "" {
		if tool := firstString(item, "tool", "name"); tool != "" {
			name = server + "." + tool
		}
	}
	return name
}

func toolText(item map[string]any) string {
	for _, key := range []string{"command", "aggregatedOutput", "aggregated_output", "result", "text", "arguments", "changes"} {
		if value, ok := item[key]; ok {
			if text := stringValue(value); text != "" {
				return text
			}
		}
	}
	return toolName(item)
}

func paramsMap(obj map[string]any) map[string]any {
	if params, ok := obj["params"].(map[string]any); ok {
		return params
	}
	return obj
}

func agentDeltaItemID(payload map[string]any) string {
	if id := firstString(payload, "itemId", "item_id"); id != "" {
		return id
	}
	if item, ok := firstMap(payload, "item", "payload", "response_item"); ok {
		return firstString(item, "id", "itemId", "item_id")
	}
	return firstString(payload, "id")
}

func toolItemID(payload, item map[string]any) string {
	if itemID := firstString(item, "id", "itemId", "item_id"); itemID != "" {
		return itemID
	}
	return firstString(payload, "itemId", "item_id", "id")
}

func agentDeltaItemIDOrDefault(payload map[string]any) string {
	if itemID := agentDeltaItemID(payload); itemID != "" {
		return itemID
	}
	return defaultAgentDeltaItemID
}

func isResponse(obj map[string]any) bool {
	_, hasID := obj["id"]
	return hasID && firstString(obj, "method") == ""
}

func isResponseTo(obj map[string]any, id string) bool {
	if !isResponse(obj) {
		return false
	}
	return requestIDKey(obj["id"]) == id
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

func firstAny(obj map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := obj[key]; ok {
			return value
		}
	}
	return nil
}

func firstString(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := obj[key]; ok {
			if s := stringValue(value); s != "" {
				return s
			}
		}
	}
	return ""
}

func firstMap(obj map[string]any, keys ...string) (map[string]any, bool) {
	for _, key := range keys {
		if value, ok := obj[key].(map[string]any); ok {
			return value, true
		}
	}
	return nil, false
}

func textFrom(obj map[string]any) string {
	for _, key := range []string{"message", "text", "delta", "error", "content", "result", "output", "aggregatedOutput", "aggregated_output"} {
		if value, ok := obj[key]; ok {
			if s := stringValue(value); s != "" {
				return s
			}
		}
	}
	return ""
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := stringValue(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	case map[string]any:
		if text := textFrom(v); text != "" {
			return text
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(raw)
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

func anySlice(value any) []any {
	if values, ok := value.([]any); ok {
		return values
	}
	return nil
}

func boolValue(value any) bool {
	b, _ := value.(bool)
	return b
}

func normalizeKind(kind string) string {
	kind = strings.ToLower(kind)
	var b strings.Builder
	for _, r := range kind {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
