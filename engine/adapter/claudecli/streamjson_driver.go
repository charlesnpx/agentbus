package claudecli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

const defaultInitializeTimeout = 60 * time.Second

const defaultSteerAckTimeout = 60 * time.Second

const progressEmissionInterval = time.Second

type streamJSONDriver struct {
	binary            string
	initializeTimeout time.Duration
	nextID            atomic.Uint64
	streamsMu         sync.Mutex
	streams           map[*duplex.Conn]*claudeStream
}

type pendingSteer struct {
	requestID string
	message   engine.SteerMessage
	ctx       context.Context
	ack       chan error
	delivered chan error
	result    map[string]any

	acknowledged bool
	abandoned    bool
	failure      error
}

type claudeStream struct {
	driver       *streamJSONDriver
	conn         *duplex.Conn
	emit         duplex.EmitFunc
	pending      []duplex.Frame
	lastProgress time.Time
	mu           sync.Mutex
	steer        *pendingSteer
	canceled     bool
	completed    bool
	finished     bool
	runErr       error
	ready        chan struct{}
	done         chan struct{}
	readyOnce    sync.Once
	doneOnce     sync.Once
	// Claude is assumed to send a completed assistant frame before the next
	// message_start. If it does not, this flag could suppress that later
	// message's completed text after the prior message emitted partial text.
	partialAgentTextEmitted bool
}

func newStreamJSONDriver(binary string) *streamJSONDriver {
	return &streamJSONDriver{
		binary:            binary,
		initializeTimeout: defaultInitializeTimeout,
		streams:           make(map[*duplex.Conn]*claudeStream),
	}
}

func (d *streamJSONDriver) ExecSpec(resumeID string, opts engine.SessionOpts, input engine.TurnInput) (command.ExecSpec, error) {
	args := []string{
		d.binaryName(),
		"-p",
		"--input-format",
		"stream-json",
		"--output-format",
		"stream-json",
		"--include-partial-messages",
		"--verbose",
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	if opts.Effort != "" {
		args = append(args, "--effort", opts.Effort)
	}
	if input.Write {
		args = append(args, "--dangerously-skip-permissions")
	} else {
		args = append(args,
			"--strict-mcp-config",
			"--mcp-config", `{"mcpServers":{}}`,
			"--permission-mode", "dontAsk",
			"--allowedTools", strings.Join(readOnlyAllowedTools, ","),
			"--disallowedTools", strings.Join(readOnlyDeniedTools, ","),
		)
	}
	if resumeID != "" {
		args = append(args, "--resume", resumeID)
	}
	return command.ExecSpec{Argv: args, Dir: opts.CWD}, nil
}

func (d *streamJSONDriver) RunTurn(ctx context.Context, conn *duplex.Conn, resumeID string, _ engine.SessionOpts, input engine.TurnInput, emit duplex.EmitFunc) (sessionID string, runErr error) {
	stream := &claudeStream{
		driver: d,
		conn:   conn,
		emit:   emit,
		ready:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	d.registerStream(stream)
	defer d.unregisterStream(stream)
	defer func() { stream.finish(runErr) }()
	sessionID = resumeID
	if err := stream.initialize(ctx); err != nil {
		return sessionID, err
	}
	if err := stream.writeUserMessage(ctx, input.Prompt); err != nil {
		return sessionID, err
	}
	stream.markReady()
	for {
		frame, err := stream.nextFrame(ctx)
		if err != nil {
			return sessionID, err
		}
		terminal, err := stream.handleFrame(ctx, frame, &sessionID)
		if err != nil {
			return sessionID, err
		}
		if terminal {
			return sessionID, nil
		}
	}
}

func (d *streamJSONDriver) Steer(ctx context.Context, conn *duplex.Conn, msg engine.SteerMessage) (engine.SteerDelivery, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	stream := d.activeStream(conn)
	if stream == nil {
		return engine.SteerDelivery{}, engine.ErrSteerNotActive
	}
	select {
	case <-stream.ready:
	case <-stream.done:
		return engine.SteerDelivery{}, stream.finishedError()
	case <-ctx.Done():
		return engine.SteerDelivery{}, ctx.Err()
	}

	pending := &pendingSteer{
		requestID: d.nextRequestID(),
		message:   msg,
		ctx:       ctx,
		ack:       make(chan error, 1),
		delivered: make(chan error, 1),
	}
	if err := stream.beginSteer(ctx, pending); err != nil {
		return engine.SteerDelivery{}, err
	}

	ackCtx, cancel := context.WithTimeout(ctx, defaultSteerAckTimeout)
	defer cancel()
	select {
	case err := <-pending.ack:
		if err != nil {
			return engine.SteerDelivery{}, err
		}
	case <-stream.done:
		if err, settled := stream.outcomeAfterDone(pending.ack); settled {
			if err != nil {
				return engine.SteerDelivery{}, err
			}
		} else {
			return engine.SteerDelivery{}, err
		}
	case <-ackCtx.Done():
		err := fmt.Errorf("claude interrupt was not acknowledged: %w", ackCtx.Err())
		stream.abandonSteer(pending, err)
		return engine.SteerDelivery{}, err
	}

	select {
	case err := <-pending.delivered:
		if err != nil {
			return engine.SteerDelivery{}, err
		}
		return engine.SteerDelivery{Mechanism: engine.SteerMechanismInterruptContinue, Confirmed: true}, nil
	case <-stream.done:
		if err, settled := stream.outcomeAfterDone(pending.delivered); settled {
			if err != nil {
				return engine.SteerDelivery{}, err
			}
			return engine.SteerDelivery{Mechanism: engine.SteerMechanismInterruptContinue, Confirmed: true}, nil
		} else {
			return engine.SteerDelivery{}, err
		}
	case <-ctx.Done():
		err := ctx.Err()
		stream.abandonSteer(pending, err)
		return engine.SteerDelivery{}, err
	}
}

func (d *streamJSONDriver) registerStream(stream *claudeStream) {
	d.streamsMu.Lock()
	defer d.streamsMu.Unlock()
	if d.streams == nil {
		d.streams = make(map[*duplex.Conn]*claudeStream)
	}
	d.streams[stream.conn] = stream
}

func (d *streamJSONDriver) unregisterStream(stream *claudeStream) {
	d.streamsMu.Lock()
	defer d.streamsMu.Unlock()
	if d.streams[stream.conn] == stream {
		delete(d.streams, stream.conn)
	}
}

func (d *streamJSONDriver) activeStream(conn *duplex.Conn) *claudeStream {
	d.streamsMu.Lock()
	defer d.streamsMu.Unlock()
	return d.streams[conn]
}

func (d *streamJSONDriver) Interrupt(ctx context.Context, conn *duplex.Conn) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if stream := d.activeStream(conn); stream != nil {
		stream.markCanceled()
	}
	return conn.WriteJSON(map[string]any{
		"type":       "control_request",
		"request_id": d.nextRequestID(),
		"request": map[string]any{
			"subtype": "interrupt",
		},
	})
}

func (d *streamJSONDriver) binaryName() string {
	if strings.TrimSpace(d.binary) == "" {
		return "claude"
	}
	return d.binary
}

func (d *streamJSONDriver) nextRequestID() string {
	return fmt.Sprintf("req_%d", d.nextID.Add(1))
}

func (s *claudeStream) initialize(ctx context.Context) error {
	id := s.driver.nextRequestID()
	if err := s.writeControlRequest(ctx, id, "initialize", map[string]any{"hooks": nil}); err != nil {
		return err
	}
	timeout := s.driver.initializeTimeout
	if timeout <= 0 {
		timeout = defaultInitializeTimeout
	}
	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var deferred []duplex.Frame
	defer func() {
		if len(deferred) > 0 {
			s.pending = append(deferred, s.pending...)
		}
	}()
	for {
		frame, err := s.nextFrame(initCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					// The parent turn context is already done (canceled or its
					// own deadline). Surface the parent's actual cause rather
					// than the child initialize deadline, so a turn cancellation
					// is not mislabeled as an init timeout.
					return ctx.Err()
				}
				return fmt.Errorf("claude initialize was not acknowledged within %s", timeout)
			}
			return err
		}
		if isControlResponseTo(frame.Object, id) {
			return controlResponseError(frame.Object)
		}
		handled, err := s.handleControlRequest(ctx, frame.Object)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		deferred = append(deferred, frame)
	}
}

func (s *claudeStream) writeControlRequest(ctx context.Context, id, subtype string, fields map[string]any) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	request := map[string]any{"subtype": subtype}
	for key, value := range fields {
		request[key] = value
	}
	return s.conn.WriteJSON(map[string]any{
		"type":       "control_request",
		"request_id": id,
		"request":    request,
	})
}

func (s *claudeStream) writeUserMessage(ctx context.Context, prompt string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return s.conn.WriteJSON(map[string]any{
		"type": "user",
		"message": map[string]any{
			"role":    "user",
			"content": prompt,
		},
	})
}

func (s *claudeStream) markReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.ready) })
}

func (s *claudeStream) beginSteer(ctx context.Context, pending *pendingSteer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished || s.completed {
		return engine.ErrSteerNotActive
	}
	if s.steer != nil {
		return errors.New("claude steering already pending")
	}
	if err := s.writeControlRequest(ctx, pending.requestID, "interrupt", nil); err != nil {
		return err
	}
	s.steer = pending
	return nil
}

func (s *claudeStream) abandonSteer(pending *pendingSteer, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steer == pending {
		pending.abandoned = true
		pending.failure = err
	}
}

func (s *claudeStream) finish(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return
	}
	s.finished = true
	s.runErr = err
	s.readyOnce.Do(func() { close(s.ready) })
	if pending := s.steer; pending != nil {
		steerErr := err
		if steerErr == nil {
			steerErr = engine.ErrSteerNotActive
		}
		notifySteer(pending.ack, steerErr)
		notifySteer(pending.delivered, steerErr)
		s.steer = nil
	}
	s.doneOnce.Do(func() { close(s.done) })
}

func (s *claudeStream) finishedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runErr != nil {
		return s.runErr
	}
	return engine.ErrSteerNotActive
}

func (s *claudeStream) outcomeAfterDone(outcome <-chan error) (error, bool) {
	select {
	case err := <-outcome:
		return err, true
	default:
		return s.finishedError(), false
	}
}

func (s *claudeStream) markCanceled() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.canceled = true
	if pending := s.steer; pending != nil {
		pending.abandoned = true
		pending.failure = context.Canceled
		notifySteer(pending.ack, context.Canceled)
		notifySteer(pending.delivered, context.Canceled)
	}
}

func notifySteer(ch chan error, err error) {
	select {
	case ch <- err:
	default:
	}
}

func (s *claudeStream) handleSteerControlResponse(ctx context.Context, obj map[string]any) (bool, bool, error) {
	if strings.ToLower(firstString(obj, "type")) != "control_response" {
		return false, false, nil
	}
	s.mu.Lock()
	pending := s.steer
	if pending == nil || !isControlResponseTo(obj, pending.requestID) {
		s.mu.Unlock()
		return false, false, nil
	}
	if err := controlResponseError(obj); err != nil {
		outcomeErr := err
		if pending.abandoned && pending.failure != nil {
			outcomeErr = pending.failure
		}
		heldResult := pending.result
		s.steer = nil
		if heldResult != nil {
			s.completed = true
		}
		notifySteer(pending.ack, outcomeErr)
		notifySteer(pending.delivered, outcomeErr)
		s.mu.Unlock()
		if heldResult == nil {
			return true, false, nil
		}
		return true, true, outcomeErr
	}
	pending.acknowledged = true
	if !pending.abandoned {
		notifySteer(pending.ack, nil)
	}
	heldResult := pending.result
	pending.result = nil
	s.mu.Unlock()
	if heldResult == nil {
		return true, false, nil
	}
	terminal, err := s.handleResult(ctx, heldResult)
	return true, terminal, err
}

func (s *claudeStream) handleResult(ctx context.Context, obj map[string]any) (bool, error) {
	s.mu.Lock()
	pending := s.steer
	if s.canceled {
		s.steer = nil
		s.completed = true
		if pending != nil {
			notifySteer(pending.ack, context.Canceled)
			notifySteer(pending.delivered, context.Canceled)
		}
		s.mu.Unlock()
		return false, context.Canceled
	}
	if pending == nil {
		s.completed = true
		s.mu.Unlock()
		s.emitResult(obj)
		return true, nil
	}
	if pending.abandoned {
		s.steer = nil
		s.completed = true
		err := pending.failure
		if err == nil {
			err = errors.New("claude interrupt was not acknowledged")
		}
		notifySteer(pending.ack, err)
		notifySteer(pending.delivered, err)
		s.mu.Unlock()
		return false, err
	}
	if !pending.acknowledged {
		if !isClaudeSuccessResult(obj) {
			pending.result = obj
			s.mu.Unlock()
			return false, nil
		}
		s.steer = nil
		s.completed = true
		err := errors.New("claude interrupt control request was not acknowledged")
		notifySteer(pending.ack, err)
		notifySteer(pending.delivered, err)
		s.mu.Unlock()
		// The result arrived before a successful interrupt acknowledgement, so
		// the original segment completed normally and remains the logical result.
		s.emitResult(obj)
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		s.steer = nil
		s.completed = true
		notifySteer(pending.delivered, err)
		s.mu.Unlock()
		return false, err
	}
	if err := pending.ctx.Err(); err != nil {
		s.steer = nil
		s.completed = true
		notifySteer(pending.delivered, err)
		s.mu.Unlock()
		return false, err
	}
	if err := s.writeUserMessage(ctx, pending.message.Text); err != nil {
		s.steer = nil
		s.completed = true
		notifySteer(pending.delivered, err)
		s.mu.Unlock()
		return false, err
	}
	s.steer = nil
	s.mu.Unlock()
	notifySteer(pending.delivered, nil)
	return false, nil
}

func isClaudeSuccessResult(obj map[string]any) bool {
	return firstString(obj, "subtype") == "success" && !boolValue(obj["is_error"]) && !boolValue(obj["isError"])
}

func (s *claudeStream) nextFrame(ctx context.Context) (duplex.Frame, error) {
	if len(s.pending) > 0 {
		frame := s.pending[0]
		copy(s.pending, s.pending[1:])
		s.pending = s.pending[:len(s.pending)-1]
		return frame, nil
	}
	frames := s.conn.Frames()
	decodeErrs := s.conn.DecodeErrors()
	for frames != nil || decodeErrs != nil {
		select {
		case frame, ok := <-frames:
			if !ok {
				frames = nil
				continue
			}
			if frame.Err != nil {
				return duplex.Frame{}, frame.Err
			}
			return frame, nil
		case err, ok := <-decodeErrs:
			if !ok {
				decodeErrs = nil
				continue
			}
			if err != nil {
				return duplex.Frame{}, err
			}
		case <-ctx.Done():
			return duplex.Frame{}, ctx.Err()
		}
	}
	return duplex.Frame{}, duplex.ErrBackendExitedBeforeTerminal
}

func (s *claudeStream) handleFrame(ctx context.Context, frame duplex.Frame, sessionID *string) (bool, error) {
	obj := frame.Object
	if id := sessionIDFrom(obj); id != "" {
		*sessionID = id
	}
	frameType := strings.ToLower(firstString(obj, "type"))
	if isClaudeProgressFrameType(frameType) {
		s.emitThinking(obj, obj)
		return false, nil
	}
	switch frameType {
	case "control_request":
		_, err := s.handleControlRequest(ctx, obj)
		return false, err
	case "control_response":
		_, terminal, err := s.handleSteerControlResponse(ctx, obj)
		return terminal, err
	case "user":
		s.emitToolResults(obj)
		return false, nil
	case "system":
		s.emitModelReported(obj)
		return false, nil
	case "stream_event":
		s.emitPartialAssistant(obj)
		return false, nil
	case "assistant":
		s.emitAssistant(obj)
		return false, nil
	case "result":
		return s.handleResult(ctx, obj)
	case "warning":
		if text := textFrom(obj); text != "" {
			s.emitEvent(engine.Event{Type: engine.EventWarning, Text: text, Metadata: obj})
		}
	case "error":
		if text := textFrom(obj); text != "" {
			s.emitEvent(engine.Event{Type: engine.EventTerminalError, Text: text, Metadata: obj})
		}
	default:
		if text := textFrom(obj); text != "" {
			s.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: obj})
		}
	}
	return false, nil
}

func (s *claudeStream) handleControlRequest(ctx context.Context, obj map[string]any) (bool, error) {
	if strings.ToLower(firstString(obj, "type")) != "control_request" {
		return false, nil
	}
	request, _ := firstMap(obj, "request")
	response := map[string]any{}
	switch strings.ToLower(firstString(request, "subtype")) {
	case "can_use_tool":
		response["behavior"] = "allow"
	case "hook_callback":
		response["continue"] = true
	case "mcp_message":
		response["mcp_response"] = map[string]any{}
	}
	select {
	case <-ctx.Done():
		return true, ctx.Err()
	default:
	}
	return true, s.conn.WriteJSON(map[string]any{
		"type": "control_response",
		"response": map[string]any{
			"subtype":    "success",
			"request_id": firstString(obj, "request_id", "requestId", "id"),
			"response":   response,
		},
	})
}

func (s *claudeStream) emitModelReported(obj map[string]any) {
	if model := strings.TrimSpace(firstString(obj, "model")); model != "" {
		s.emitEvent(engine.Event{Type: engine.EventModelReported, ModelReported: model, Metadata: obj})
	}
}

func (s *claudeStream) emitAssistant(obj map[string]any) {
	msg, _ := firstMap(obj, "message")
	hasAgentDelta := s.partialAgentTextEmitted
	s.partialAgentTextEmitted = false
	content := anySlice(msg["content"])
	if len(content) == 0 {
		if !hasAgentDelta {
			if text := textFrom(obj); text != "" {
				s.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: obj})
			}
		}
		return
	}
	for _, item := range content {
		block, ok := item.(map[string]any)
		if !ok {
			continue
		}
		blockType := strings.ToLower(firstString(block, "type"))
		if isClaudeProgressFrameType(blockType) {
			s.emitThinking(block, obj)
			continue
		}
		switch blockType {
		case "text":
			if !hasAgentDelta {
				if text := textValue(block["text"]); text != "" {
					s.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: obj})
				}
			}
		case "tool_use":
			name := firstString(block, "name")
			event := engine.Event{
				Type:                       engine.EventToolUse,
				Name:                       name,
				Metadata:                   obj,
				ObservedWorkspaceWriteItem: isClaudeWorkspaceWriteTool(name),
			}
			if !event.ObservedWorkspaceWriteItem {
				event.Text = textValue(block["text"])
				if event.Text == "" {
					event.Text = textValue(block["input"])
				}
				if event.Text == "" {
					event.Text = name
				}
			}
			s.emitEvent(event)
		}
	}
}

func isClaudeWorkspaceWriteTool(name string) bool {
	switch name {
	case "Edit", "Write", "NotebookEdit":
		return true
	default:
		return false
	}
}

func (s *claudeStream) emitPartialAssistant(obj map[string]any) {
	event, ok := firstMap(obj, "event")
	if !ok {
		return
	}
	switch strings.ToLower(firstString(event, "type")) {
	case "message_start":
		s.partialAgentTextEmitted = false
	case "content_block_delta":
		s.emitPartialAgentText(event, obj)
	}
}

func (s *claudeStream) emitPartialAgentText(event, metadata map[string]any) {
	delta, ok := firstMap(event, "delta")
	if !ok || strings.ToLower(firstString(delta, "type")) != "text_delta" {
		return
	}
	if text := firstString(delta, "text"); text != "" {
		s.partialAgentTextEmitted = true
		s.emitEvent(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: metadata})
	}
}

func (s *claudeStream) emitToolResults(obj map[string]any) {
	message, _ := firstMap(obj, "message")
	for _, item := range anySlice(message["content"]) {
		block, ok := item.(map[string]any)
		if !ok || strings.ToLower(firstString(block, "type")) != "tool_result" {
			continue
		}
		text := textValue(block["content"])
		if text == "" {
			text = textValue(block["text"])
		}
		s.emitEvent(engine.Event{
			Type:     engine.EventToolResult,
			Text:     text,
			Metadata: obj,
		})
	}
}

func (s *claudeStream) emitThinking(source, metadata map[string]any) {
	if text := firstString(source, "thinking", "text", "content"); text != "" {
		s.emitEvent(engine.Event{Type: engine.EventReasoning, Text: text, Metadata: metadata})
	}
	// Preserve the existing rate-limited liveness signal for callers that use
	// Progress independently of textual reasoning (including empty frames).
	s.emitProgress()
}

func (s *claudeStream) emitResult(obj map[string]any) {
	subtype, _ := obj["subtype"].(string)
	isError := boolValue(obj["is_error"]) || boolValue(obj["isError"])
	if !isError && subtype == "success" {
		text := explicitResultText(obj["result"])
		if strings.TrimSpace(text) == "" {
			s.emitEvent(engine.Event{Type: engine.EventTerminalError, Text: missingSuccessResultText(obj), Metadata: obj})
			return
		}
		s.emitEvent(engine.Event{Type: engine.EventResultMessage, Text: text, Metadata: obj})
		return
	}
	text := resultErrorText(obj, strings.TrimSpace(subtype))
	s.emitEvent(engine.Event{Type: engine.EventTerminalError, Text: text, Metadata: obj})
}

func (s *claudeStream) emitEvent(ev engine.Event) {
	if s.emit != nil {
		s.emit(ev)
	}
}

func (s *claudeStream) emitProgress() {
	now := time.Now()
	if !s.lastProgress.IsZero() && now.Sub(s.lastProgress) < progressEmissionInterval {
		return
	}
	s.lastProgress = now
	s.emitEvent(engine.Event{Type: engine.EventProgress})
}

func isClaudeProgressFrameType(frameType string) bool {
	// Keep this list concrete: unknown and system frames must retain their
	// existing handling so they cannot mask a stalled backend.
	switch frameType {
	case "thinking":
		return true
	default:
		return false
	}
}

func isControlResponseTo(obj map[string]any, id string) bool {
	if strings.ToLower(firstString(obj, "type")) != "control_response" {
		return false
	}
	response, _ := firstMap(obj, "response")
	got := firstString(response, "request_id", "requestId")
	if got == "" {
		got = firstString(obj, "request_id", "requestId", "id")
	}
	return got == id
}

func controlResponseError(obj map[string]any) error {
	response, _ := firstMap(obj, "response")
	subtype := strings.ToLower(firstString(response, "subtype"))
	if subtype == "success" {
		return nil
	}
	msg := textValue(response["error"])
	if msg == "" {
		msg = textFrom(response)
	}
	if msg == "" {
		if subtype == "" {
			msg = "claude control response missing success acknowledgement"
		} else {
			msg = "claude control " + subtype
		}
	}
	return errors.New(msg)
}

func sessionIDFrom(obj map[string]any) string {
	return strings.TrimSpace(firstString(obj, "session_id", "sessionId", "uuid"))
}

func resultErrorText(obj map[string]any, subtype string) string {
	if text := errorsArrayText(obj["errors"]); text != "" {
		return text
	}
	for _, key := range []string{"error", "message"} {
		if text := textValue(obj[key]); text != "" {
			return text
		}
	}
	if text := textValue(obj["result"]); text != "" {
		return text
	}
	if subtype != "" {
		return "claude result " + subtype
	}
	return "claude result error"
}

func explicitResultText(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case map[string]any:
		text, _ := v["text"].(string)
		return text
	default:
		return ""
	}
}

func missingSuccessResultText(obj map[string]any) string {
	if text := errorsArrayText(obj["errors"]); text != "" {
		return text
	}
	for _, key := range []string{"error", "message"} {
		if text := textValue(obj[key]); text != "" {
			return text
		}
	}
	return "claude success result missing result text"
}

func errorsArrayText(value any) string {
	var parts []string
	switch values := value.(type) {
	case []any:
		parts = make([]string, 0, len(values))
		for _, item := range values {
			text, ok := item.(string)
			if !ok {
				continue
			}
			if text = strings.TrimSpace(text); text != "" {
				parts = append(parts, text)
			}
		}
	case []string:
		parts = make([]string, 0, len(values))
		for _, text := range values {
			if text = strings.TrimSpace(text); text != "" {
				parts = append(parts, text)
			}
		}
	default:
		return ""
	}
	return strings.Join(parts, "; ")
}

func firstMap(obj map[string]any, keys ...string) (map[string]any, bool) {
	if obj == nil {
		return nil, false
	}
	for _, key := range keys {
		if value, ok := obj[key].(map[string]any); ok {
			return value, true
		}
	}
	return nil, false
}

func firstString(obj map[string]any, keys ...string) string {
	if obj == nil {
		return ""
	}
	for _, key := range keys {
		if value, ok := obj[key]; ok {
			if s := textValue(value); s != "" {
				return s
			}
		}
	}
	return ""
}

func textFrom(obj map[string]any) string {
	for _, key := range []string{"text", "message", "content", "result", "error"} {
		if text := textValue(obj[key]); text != "" {
			return text
		}
	}
	return ""
}

func textValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			if text := textValue(item); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "")
	case map[string]any:
		for _, key := range []string{"text", "message", "content", "result", "error"} {
			if text := textValue(v[key]); text != "" {
				return text
			}
		}
		raw, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(raw)
	case json.Number:
		return v.String()
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return ""
	}
}

func anySlice(value any) []any {
	values, _ := value.([]any)
	return values
}

func boolValue(value any) bool {
	v, _ := value.(bool)
	return v
}
