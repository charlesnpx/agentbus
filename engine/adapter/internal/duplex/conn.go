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
	Limit  int
	Bytes  uint64
	Prefix []byte
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
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.drops.Merge(engine.TransportFrameDrops{
		Count:          1,
		Bytes:          frame.Bytes,
		RedactedPrefix: redactedFramePrefix(frame.Prefix),
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
	if kind != "" {
		return field + "=" + safeFrameType(kind)
	}
	return "unclassified"
}

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

func safeFrameType(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 128 {
		value = value[:128]
	}
	if value == "" {
		return "unclassified"
	}
	for _, runeValue := range value {
		if runeValue < ' ' || runeValue > '~' {
			return "unclassified"
		}
	}
	return value
}

func maxInt() int {
	return int(^uint(0) >> 1)
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
			overlong = true
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
				return nil, &OverlongFrameError{Limit: limit, Bytes: bytesRead, Prefix: append([]byte(nil), prefix...)}, nil
			}
			if len(line) > 0 {
				return line, nil, nil
			}
			return nil, nil, io.EOF
		}
		return nil, nil, err
	}
	if overlong {
		return nil, &OverlongFrameError{Limit: limit, Bytes: bytesRead, Prefix: append([]byte(nil), prefix...)}, nil
	}
	return line, nil, nil
}
