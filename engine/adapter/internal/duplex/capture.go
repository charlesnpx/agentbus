package duplex

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

const captureWarningErrorBytes = 256

type captureWarning struct {
	source string
	bytes  int64
	err    string
}

func (warning captureWarning) event() map[string]any {
	return map[string]any{
		"agentbusCaptureWarning": true,
		"source":                 warning.source,
		"bytesCaptured":          warning.bytes,
		"error":                  warning.err,
	}
}

func (warning captureWarning) text() string {
	return fmt.Sprintf("capture warning: raw %s log stopped after %d bytes: %s", warning.source, warning.bytes, warning.err)
}

type captureWarningTracker struct {
	mu      sync.Mutex
	source  string
	written int64
	warning *captureWarning
}

func (tracker *captureWarningTracker) addWritten(count int) {
	if tracker == nil || count <= 0 {
		return
	}
	tracker.mu.Lock()
	tracker.written += int64(count)
	tracker.mu.Unlock()
}

func (tracker *captureWarningTracker) setWritten(count int64) {
	if tracker == nil || count < 0 {
		return
	}
	tracker.mu.Lock()
	if tracker.warning == nil {
		tracker.written = count
	}
	tracker.mu.Unlock()
}

func (tracker *captureWarningTracker) fail(err error) {
	if tracker == nil || err == nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.warning != nil {
		return
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > captureWarningErrorBytes {
		message = message[:captureWarningErrorBytes-3] + "..."
	}
	if message == "" {
		message = "raw log sink failed"
	}
	tracker.warning = &captureWarning{source: tracker.source, bytes: tracker.written, err: message}
}

func (tracker *captureWarningTracker) snapshot() *captureWarning {
	if tracker == nil {
		return nil
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.warning == nil {
		return nil
	}
	warning := *tracker.warning
	return &warning
}

func (tracker *captureWarningTracker) tolerantWriter(dst io.Writer) io.Writer {
	return &tolerantCaptureWriter{dst: dst, tracker: tracker}
}

// tolerantCaptureWriter observes a failed optional raw-log sink but always
// reports successful writes to its upstream reader so provider output keeps
// flowing. It stops touching the sink after its first error.
type tolerantCaptureWriter struct {
	dst      io.Writer
	tracker  *captureWarningTracker
	disabled bool
}

func (writer *tolerantCaptureWriter) Write(payload []byte) (int, error) {
	if writer == nil || writer.dst == nil || writer.disabled {
		return len(payload), nil
	}
	n, err := writer.dst.Write(payload)
	if n < 0 {
		n = 0
		err = errors.New("raw log sink returned a negative write count")
	} else if n > len(payload) {
		n = len(payload)
		err = errors.New("raw log sink returned an invalid write count")
	}
	writer.tracker.addWritten(n)
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		writer.tracker.fail(err)
		writer.disabled = true
	}
	return len(payload), nil
}
