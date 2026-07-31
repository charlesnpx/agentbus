package duplex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/charlesnpx/agentbus/engine/command"
)

const (
	// InitialJSONLineBufferBytes matches the existing CLI adapter scanner
	// starting buffer size.
	InitialJSONLineBufferBytes = 64 * 1024
	// MaxJSONLineBytes is the maximum single JSONL frame accepted from stdout.
	MaxJSONLineBytes = 1024 * 1024
)

// Frame is one decoded JSON object read from the process stdout stream.
type Frame struct {
	Raw    json.RawMessage
	Object map[string]any
}

// DecodeError reports a malformed JSONL stdout frame.
type DecodeError struct {
	Line string
	Err  error
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
	go readJSONL(stdout, stdoutLog, frames, decodeErrs, readerDone)
	return &Conn{
		stdin:      stdin,
		frames:     frames,
		decodeErrs: decodeErrs,
		readerDone: readerDone,
		running:    running,
		retirement: retirement,
	}
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

// DecodeErrors returns stdout JSON decode or scanner errors.
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

func readJSONL(src io.ReadCloser, log io.Writer, frames chan<- Frame, decodeErrs chan<- error, done chan<- struct{}) {
	defer close(done)
	defer close(frames)
	defer close(decodeErrs)
	defer func() { _ = src.Close() }()

	var reader io.Reader = src
	if log != nil {
		reader = io.TeeReader(src, log)
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, InitialJSONLineBufferBytes), MaxJSONLineBytes)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
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
	if err := scanner.Err(); err != nil {
		decodeErrs <- err
	}
}
