package duplex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

const (
	// InitialJSONLineBufferBytes matches the existing CLI adapter scanner
	// starting buffer size.
	InitialJSONLineBufferBytes = 64 * 1024
	// MaxJSONLineBytes is the default maximum single JSONL frame accepted from
	// backend stdout. 32 MiB accommodates normal Codex file-change diffs and
	// command-output events while retaining a hard bound on one JSONL frame.
	MaxJSONLineBytes = 32 * 1024 * 1024
	// JSONLineBytesEnv overrides MaxJSONLineBytes for backend stdout frames.
	// It accepts a positive integer byte count; absent, invalid, zero, or
	// negative values use MaxJSONLineBytes so the limit is never disabled.
	JSONLineBytesEnv = "AGENTBUS_BACKEND_JSON_LINE_BYTES"
	// OverlongFramePrefixBytes bounds the retained raw prefix used only for
	// local frame classification. It is not persisted.
	OverlongFramePrefixBytes = 4 * 1024
)

// ErrFrameTooLarge marks a JSONL backend frame that exceeded the configured
// line limit after its remainder was discarded for resynchronization.
var ErrFrameTooLarge = errors.New("backend JSONL frame exceeded configured limit")

// Frame is one decoded JSON object or an in-order recoverable reader condition
// from the process stdout stream.
type Frame struct {
	Raw    json.RawMessage
	Object map[string]any
	// Err reports an in-order recoverable transport condition. It is used for
	// an oversized frame after the reader has discarded that frame through its
	// newline; the following JSONL frame remains readable. Keeping this on the
	// frame stream preserves wire order, so a following terminal frame cannot
	// be accepted before a discarded terminal frame is classified.
	Err error
}

// DecodeError reports a malformed JSONL stdout frame.
type DecodeError struct {
	Line string
	Err  error
}

// OverlongFrameError reports one discarded JSONL backend frame. Prefix is
// bounded, untrusted transport evidence and is intentionally not an error
// message payload.
type OverlongFrameError struct {
	Limit                  int
	Bytes                  uint64
	Prefix                 []byte
	DuplicateDiscriminator bool
}

func (e *OverlongFrameError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %d bytes exceeds %d byte limit", ErrFrameTooLarge, e.Bytes, e.Limit)
}

func (e *OverlongFrameError) Is(target error) bool {
	return target == ErrFrameTooLarge
}

func (e *DecodeError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("malformed duplex stdout: %v", e.Err)
}

func (e *DecodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Conn is the live duplex connection exposed to a driver for one turn.
type Conn struct {
	stdin      io.WriteCloser
	frames     <-chan Frame
	decodeErrs <-chan error
	readerDone <-chan struct{}
	drops      *frameDropTracker
	running    command.RunningCommand
	retirement *retirement

	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

func newConn(stdin io.WriteCloser, stdout io.ReadCloser, stdoutLog io.Writer, running command.RunningCommand, retirement *retirement) *Conn {
	frames := make(chan Frame, 16)
	decodeErrs := make(chan error, 1)
	readerDone := make(chan struct{})
	drops := &frameDropTracker{}
	go readJSONL(stdout, stdoutLog, frames, decodeErrs, readerDone, drops)
	return &Conn{
		stdin:      stdin,
		frames:     frames,
		decodeErrs: decodeErrs,
		readerDone: readerDone,
		drops:      drops,
		running:    running,
		retirement: retirement,
	}
}

// FrameDrops returns the backend JSONL frames discarded by the bounded reader.
func (c *Conn) FrameDrops() engine.TransportFrameDrops {
	if c == nil || c.drops == nil {
		return engine.TransportFrameDrops{}
	}
	return c.drops.snapshot()
}

// WriteJSON writes one JSON value followed by a newline. Calls are serialized so
// concurrent prompts, replies, and interrupt frames cannot interleave.
func (c *Conn) WriteJSON(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.stdin.Write(payload)
	return err
}

// WriteStdin writes raw bytes to process stdin. Calls are serialized with
// WriteJSON and CloseStdin so mixed protocol writes cannot interleave.
func (c *Conn) WriteStdin(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.stdin.Write(p)
}

// CloseStdin half-closes the process stdin. It is safe to call more than once.
func (c *Conn) CloseStdin() error {
	c.closeOnce.Do(func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		c.closeErr = c.stdin.Close()
	})
	return c.closeErr
}

// Frames returns decoded stdout JSON objects.
func (c *Conn) Frames() <-chan Frame {
	return c.frames
}

// DecodeErrors returns fatal stdout JSON decode or reader errors. Recoverable
// oversized-frame errors are delivered in order on Frames.
func (c *Conn) DecodeErrors() <-chan error {
	return c.decodeErrs
}

// Interrupt invokes the underlying process interrupt path.
func (c *Conn) Interrupt(ctx context.Context) error {
	if c == nil || c.running == nil {
		return nil
	}
	return c.running.Interrupt(ctx)
}

// Wait waits for the cached process retirement observation.
func (c *Conn) Wait(ctx context.Context) (command.FinalObservation, error) {
	if c == nil || c.retirement == nil {
		return command.FinalObservation{}, nil
	}
	return c.retirement.wait(ctx)
}

func (c *Conn) waitReader() {
	if c == nil || c.readerDone == nil {
		return
	}
	<-c.readerDone
}

func (c *Conn) drainReader() {
	if c == nil {
		return
	}
	frames := c.Frames()
	decodeErrs := c.DecodeErrors()
	for frames != nil || decodeErrs != nil {
		select {
		case _, ok := <-frames:
			if !ok {
				frames = nil
			}
		case _, ok := <-decodeErrs:
			if !ok {
				decodeErrs = nil
			}
		}
	}
	c.waitReader()
}

func readJSONL(src io.ReadCloser, log io.Writer, frames chan<- Frame, decodeErrs chan<- error, done chan<- struct{}, drops *frameDropTracker) {
	defer close(done)
	defer close(frames)
	defer close(decodeErrs)
	defer func() { _ = src.Close() }()

	var reader io.Reader = src
	if log != nil {
		reader = io.TeeReader(src, log)
	}
	buffered := bufio.NewReaderSize(reader, InitialJSONLineBufferBytes)
	limit := configuredJSONLineBytes()
	for {
		line, overlong, err := readJSONLine(buffered, limit)
		if overlong != nil {
			if drops != nil {
				drops.record(overlong)
			}
			// This must stay on the ordered frame stream rather than decodeErrs.
			// Drivers may receive a valid subsequent frame and a buffered decode
			// error in either select order; that could turn a discarded terminal
			// frame into a false success.
			frames <- Frame{Err: overlong}
			continue
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				decodeErrs <- err
			}
			return
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		raw := append([]byte(nil), line...)
		var obj map[string]any
		if err := json.Unmarshal(raw, &obj); err != nil {
			decodeErrs <- &DecodeError{Line: string(raw), Err: err}
			return
		}
		frames <- Frame{Raw: json.RawMessage(raw), Object: obj}
	}
}

func configuredJSONLineBytes() int {
	raw := strings.TrimSpace(os.Getenv(JSONLineBytesEnv))
	if raw == "" {
		return MaxJSONLineBytes
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 || value > int64(maxInt()) {
		return MaxJSONLineBytes
	}
	return int(value)
}

type frameDropTracker struct {
	mu    sync.Mutex
	drops engine.TransportFrameDrops
}

func (tracker *frameDropTracker) record(frame *OverlongFrameError) {
	if tracker == nil || frame == nil {
		return
	}
	summary := "unclassified"
	if !frame.DuplicateDiscriminator {
		summary = redactedFramePrefix(frame.Prefix)
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.drops.Merge(engine.TransportFrameDrops{
		Count:          1,
		Bytes:          frame.Bytes,
		RedactedPrefix: summary,
	})
}

func (tracker *frameDropTracker) snapshot() engine.TransportFrameDrops {
	if tracker == nil {
		return engine.TransportFrameDrops{}
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.drops
}

func redactedFramePrefix(prefix []byte) string {
	kind, field := FrameTypeFromPrefix(prefix)
	summary := "unclassified"
	switch field {
	case "method":
		switch kind {
		case "turn/completed", "task_complete", "item/started", "warning", "error", "config/warning", "guardian/warning", "item/completed":
			summary = "method=" + kind
		}
	case "type":
		switch kind {
		case "message", "complete", "warning":
			summary = "type=" + kind
		}
	}
	if len(summary) > maxRedactedFrameSummaryBytes {
		return "unclassified"
	}
	return summary
}

const maxRedactedFrameSummaryBytes = 128

// FrameTypeFromPrefix returns a complete top-level method or type string from
// a bounded JSON object prefix. It returns no value for incomplete or malformed
// evidence rather than inferring a frame kind from payload text.
func FrameTypeFromPrefix(prefix []byte) (string, string) {
	index := skipJSONWhitespace(prefix, 0)
	if index >= len(prefix) || prefix[index] != '{' {
		return "", ""
	}
	index++
	for {
		index = skipJSONWhitespace(prefix, index)
		if index >= len(prefix) || prefix[index] == '}' {
			return "", ""
		}
		key, next, ok := readJSONStringPrefix(prefix, index)
		if !ok {
			return "", ""
		}
		index = skipJSONWhitespace(prefix, next)
		if index >= len(prefix) || prefix[index] != ':' {
			return "", ""
		}
		index = skipJSONWhitespace(prefix, index+1)
		if key == "method" || key == "type" {
			value, _, ok := readJSONStringPrefix(prefix, index)
			if !ok {
				return "", ""
			}
			return value, key
		}
		next, ok = skipJSONValuePrefix(prefix, index)
		if !ok {
			return "", ""
		}
		index = skipJSONWhitespace(prefix, next)
		if index >= len(prefix) || prefix[index] == '}' {
			return "", ""
		}
		if prefix[index] != ',' {
			return "", ""
		}
		index++
	}
}

func skipJSONWhitespace(payload []byte, index int) int {
	for index < len(payload) {
		switch payload[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return index
		}
	}
	return index
}

func readJSONStringPrefix(payload []byte, index int) (string, int, bool) {
	if index >= len(payload) || payload[index] != '"' {
		return "", index, false
	}
	start := index
	index++
	escaped := false
	for index < len(payload) {
		value := payload[index]
		if escaped {
			escaped = false
			index++
			continue
		}
		if value == '\\' {
			escaped = true
			index++
			continue
		}
		if value == '"' {
			var decoded string
			if err := json.Unmarshal(payload[start:index+1], &decoded); err != nil {
				return "", index, false
			}
			return decoded, index + 1, true
		}
		if value < ' ' {
			return "", index, false
		}
		index++
	}
	return "", index, false
}

func skipJSONValuePrefix(payload []byte, index int) (int, bool) {
	depth := 0
	inString := false
	escaped := false
	for index < len(payload) {
		value := payload[index]
		if inString {
			if escaped {
				escaped = false
			} else if value == '\\' {
				escaped = true
			} else if value == '"' {
				inString = false
			}
			index++
			continue
		}
		switch value {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			if depth == 0 {
				return index, true
			}
			depth--
		case ',':
			if depth == 0 {
				return index, true
			}
		}
		index++
	}
	return index, false
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

// frameDiscriminatorTracker observes only the object-key positions that can
// authorize an oversized-frame skip. It retains no frame values and does not
// validate the JSON document.
type frameDiscriminatorTracker struct {
	started bool
	depth   int

	inString      bool
	escaped       bool
	unicodeDigits int
	unicodeValue  uint16
	keyContext    frameKeyContext
	key           frameKeyCapture

	rootActive        bool
	rootExpectKey     bool
	rootAwaitingColon bool
	rootValuePending  bool
	rootKey           frameKey

	paramsActive        bool
	paramsDepth         int
	paramsExpectKey     bool
	paramsAwaitingColon bool
	paramsValuePending  bool
	paramsKey           frameKey

	itemActive        bool
	itemDepth         int
	itemExpectKey     bool
	itemAwaitingColon bool

	methodCount   uint8
	typeCount     uint8
	itemTypeCount uint8
}

type frameKeyContext uint8

const (
	frameKeyContextNone frameKeyContext = iota
	frameKeyContextRoot
	frameKeyContextParams
	frameKeyContextItem
)

type frameKey uint8

const (
	frameKeyOther frameKey = iota
	frameKeyParams
	frameKeyItem
)

type frameKeyCapture struct {
	value    [8]byte
	length   int
	overflow bool
}

func (tracker *frameDiscriminatorTracker) consume(payload []byte) {
	for _, value := range payload {
		tracker.consumeByte(value)
	}
}

func (tracker *frameDiscriminatorTracker) consumeByte(value byte) {
	if tracker.inString {
		tracker.consumeStringByte(value)
		return
	}
	if !tracker.started {
		if value == ' ' || value == '\t' || value == '\r' {
			return
		}
		if value != '{' {
			tracker.started = true
			return
		}
		tracker.started = true
		tracker.depth = 1
		tracker.rootActive = true
		tracker.rootExpectKey = true
		return
	}
	if value == ' ' || value == '\t' || value == '\r' {
		return
	}
	if tracker.consumeColon(value) {
		return
	}
	tracker.beginValue(value)
	if value == '"' {
		tracker.inString = true
		tracker.escaped = false
		tracker.unicodeDigits = 0
		tracker.keyContext = tracker.nextKeyContext()
		if tracker.keyContext != frameKeyContextNone {
			tracker.key = frameKeyCapture{}
		}
		return
	}
	switch value {
	case '{', '[':
		tracker.depth++
	case '}', ']':
		tracker.closeContainer()
	case ',':
		tracker.nextObjectKey()
	}
}

func (tracker *frameDiscriminatorTracker) consumeStringByte(value byte) {
	if tracker.unicodeDigits > 0 {
		digit, ok := hexDigit(value)
		if !ok {
			tracker.key.overflow = true
			tracker.unicodeDigits = 0
			return
		}
		tracker.unicodeValue = tracker.unicodeValue<<4 | uint16(digit)
		tracker.unicodeDigits--
		if tracker.unicodeDigits == 0 {
			if tracker.unicodeValue <= 0x7f {
				tracker.appendKeyByte(byte(tracker.unicodeValue))
			} else {
				tracker.key.overflow = true
			}
		}
		return
	}
	if tracker.escaped {
		tracker.escaped = false
		switch value {
		case 'u':
			tracker.unicodeDigits = 4
			tracker.unicodeValue = 0
		case '"', '\\', '/':
			tracker.appendKeyByte(value)
		default:
			tracker.key.overflow = true
		}
		return
	}
	switch value {
	case '\\':
		tracker.escaped = true
	case '"':
		tracker.inString = false
		tracker.completeKey()
	default:
		tracker.appendKeyByte(value)
	}
}

func (tracker *frameDiscriminatorTracker) appendKeyByte(value byte) {
	if tracker.keyContext == frameKeyContextNone {
		return
	}
	if tracker.key.length >= len(tracker.key.value) {
		tracker.key.overflow = true
		return
	}
	tracker.key.value[tracker.key.length] = value
	tracker.key.length++
}

func (tracker *frameDiscriminatorTracker) completeKey() {
	context := tracker.keyContext
	tracker.keyContext = frameKeyContextNone
	if context == frameKeyContextNone {
		return
	}
	switch context {
	case frameKeyContextRoot:
		tracker.rootExpectKey = false
		tracker.rootAwaitingColon = true
		tracker.rootKey = frameKeyOther
		if tracker.key.matches("method") {
			tracker.increment(&tracker.methodCount)
		} else if tracker.key.matches("type") {
			tracker.increment(&tracker.typeCount)
		} else if tracker.key.matches("params") {
			tracker.rootKey = frameKeyParams
		}
	case frameKeyContextParams:
		tracker.paramsExpectKey = false
		tracker.paramsAwaitingColon = true
		tracker.paramsKey = frameKeyOther
		if tracker.key.matches("item") {
			tracker.paramsKey = frameKeyItem
		}
	case frameKeyContextItem:
		tracker.itemExpectKey = false
		tracker.itemAwaitingColon = true
		if tracker.key.matches("type") {
			tracker.increment(&tracker.itemTypeCount)
		}
	}
}

func (tracker *frameDiscriminatorTracker) consumeColon(value byte) bool {
	if value != ':' {
		return false
	}
	switch {
	case tracker.rootActive && tracker.depth == 1 && tracker.rootAwaitingColon:
		tracker.rootAwaitingColon = false
		tracker.rootValuePending = true
		return true
	case tracker.paramsActive && tracker.depth == tracker.paramsDepth && tracker.paramsAwaitingColon:
		tracker.paramsAwaitingColon = false
		tracker.paramsValuePending = true
		return true
	case tracker.itemActive && tracker.depth == tracker.itemDepth && tracker.itemAwaitingColon:
		tracker.itemAwaitingColon = false
		return true
	default:
		return false
	}
}

func (tracker *frameDiscriminatorTracker) beginValue(value byte) {
	if tracker.rootActive && tracker.depth == 1 && tracker.rootValuePending {
		tracker.rootValuePending = false
		if tracker.rootKey == frameKeyParams && value == '{' {
			tracker.paramsActive = true
			tracker.paramsDepth = tracker.depth + 1
			tracker.paramsExpectKey = true
		}
	}
	if tracker.paramsActive && tracker.depth == tracker.paramsDepth && tracker.paramsValuePending {
		tracker.paramsValuePending = false
		if tracker.paramsKey == frameKeyItem && value == '{' {
			tracker.itemActive = true
			tracker.itemDepth = tracker.depth + 1
			tracker.itemExpectKey = true
		}
	}
}

func (tracker *frameDiscriminatorTracker) nextKeyContext() frameKeyContext {
	switch {
	case tracker.rootActive && tracker.depth == 1 && tracker.rootExpectKey:
		return frameKeyContextRoot
	case tracker.paramsActive && tracker.depth == tracker.paramsDepth && tracker.paramsExpectKey:
		return frameKeyContextParams
	case tracker.itemActive && tracker.depth == tracker.itemDepth && tracker.itemExpectKey:
		return frameKeyContextItem
	default:
		return frameKeyContextNone
	}
}

func (tracker *frameDiscriminatorTracker) closeContainer() {
	if tracker.itemActive && tracker.depth == tracker.itemDepth {
		tracker.itemActive = false
	}
	if tracker.paramsActive && tracker.depth == tracker.paramsDepth {
		tracker.paramsActive = false
	}
	if tracker.rootActive && tracker.depth == 1 {
		tracker.rootActive = false
	}
	if tracker.depth > 0 {
		tracker.depth--
	}
}

func (tracker *frameDiscriminatorTracker) nextObjectKey() {
	switch {
	case tracker.rootActive && tracker.depth == 1:
		tracker.rootExpectKey = true
		tracker.rootAwaitingColon = false
		tracker.rootValuePending = false
	case tracker.paramsActive && tracker.depth == tracker.paramsDepth:
		tracker.paramsExpectKey = true
		tracker.paramsAwaitingColon = false
		tracker.paramsValuePending = false
	case tracker.itemActive && tracker.depth == tracker.itemDepth:
		tracker.itemExpectKey = true
		tracker.itemAwaitingColon = false
	}
}

func (tracker *frameDiscriminatorTracker) duplicateDiscriminator() bool {
	return tracker.methodCount > 1 || tracker.typeCount > 1 || tracker.itemTypeCount > 1
}

func (tracker *frameDiscriminatorTracker) increment(count *uint8) {
	if *count < 2 {
		*count++
	}
}

func (capture frameKeyCapture) matches(want string) bool {
	if capture.overflow || capture.length != len(want) {
		return false
	}
	for index := range want {
		if capture.value[index] != want[index] {
			return false
		}
	}
	return true
}

func hexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

// readJSONLine reads one newline-delimited frame. When its payload exceeds
// limit, it discards through the newline, returns a bounded prefix, and leaves
// the reader positioned at the following frame.
func readJSONLine(reader *bufio.Reader, limit int) ([]byte, *OverlongFrameError, error) {
	if limit <= 0 {
		limit = MaxJSONLineBytes
	}
	line := make([]byte, 0, min(limit, InitialJSONLineBufferBytes))
	prefix := make([]byte, 0, min(limit, OverlongFramePrefixBytes))
	discriminators := &frameDiscriminatorTracker{}
	var bytesRead uint64
	overlong := false
	for {
		fragment, err := reader.ReadSlice('\n')
		payload := fragment
		endsLine := len(fragment) > 0 && fragment[len(fragment)-1] == '\n'
		if endsLine {
			payload = fragment[:len(fragment)-1]
		}
		bytesRead += uint64(len(payload))
		if !overlong && len(line)+len(payload) <= limit {
			line = append(line, payload...)
		} else {
			if !overlong {
				discriminators.consume(line)
			}
			overlong = true
			discriminators.consume(payload)
			if len(prefix) < OverlongFramePrefixBytes {
				fromLine := min(len(line), OverlongFramePrefixBytes-len(prefix))
				prefix = append(prefix, line[:fromLine]...)
				remaining := OverlongFramePrefixBytes - len(prefix)
				if remaining > 0 {
					prefix = append(prefix, payload[:min(len(payload), remaining)]...)
				}
			}
		}
		if endsLine {
			break
		}
		if err == nil {
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			if overlong {
				return nil, &OverlongFrameError{Limit: limit, Bytes: bytesRead, Prefix: append([]byte(nil), prefix...), DuplicateDiscriminator: discriminators.duplicateDiscriminator()}, nil
			}
			if len(line) > 0 {
				return line, nil, nil
			}
			return nil, nil, io.EOF
		}
		return nil, nil, err
	}
	if overlong {
		return nil, &OverlongFrameError{Limit: limit, Bytes: bytesRead, Prefix: append([]byte(nil), prefix...), DuplicateDiscriminator: discriminators.duplicateDiscriminator()}, nil
	}
	return line, nil, nil
}
