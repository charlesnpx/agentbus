package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	DefaultInlineResultCap = 256 * 1024
	DefaultEventTextCap    = 64 * 1024
	// A measured Codex app-server stream writes 163 KB/min; over the four-hour
	// maximum timeout that is about 39 MB, so 64 MiB leaves headroom while
	// retaining a bounded log.
	DefaultCappedLogCapBytes int64 = 64 * 1024 * 1024
)

// EventText is text prepared for a streaming event.
type EventText struct {
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}

// WriteResultForLayout writes a final result into layout.Results without
// requiring a persistence instance.
func WriteResultForLayout(layout WorkspaceLayout, jobID string, raw []byte, inlineCap int) (ResultInfo, error) {
	path, err := ResultPathForLayout(layout, jobID)
	if err != nil {
		return ResultInfo{}, err
	}
	return writeResultFile(path, raw, inlineCap)
}

// ResultPathForLayout returns the result artifact path for jobID in layout.
func ResultPathForLayout(layout WorkspaceLayout, jobID string) (string, error) {
	return safePathForID(layout.Results, jobID, ".txt")
}

// LogPathsForLayout returns the backend stdout and stderr paths for jobID.
func LogPathsForLayout(layout WorkspaceLayout, jobID string) (LogPaths, error) {
	stdout, err := safePathForID(layout.Logs, jobID, ".stdout.log")
	if err != nil {
		return LogPaths{}, err
	}
	stderr, err := safePathForID(layout.Logs, jobID, ".stderr.log")
	if err != nil {
		return LogPaths{}, err
	}
	return LogPaths{Stdout: stdout, Stderr: stderr}, nil
}

func writeResultFile(path string, raw []byte, inlineCap int) (ResultInfo, error) {
	if inlineCap <= 0 {
		inlineCap = DefaultInlineResultCap
	}
	sum := sha256.Sum256(raw)
	if err := atomicWriteFile(path, raw, 0o600); err != nil {
		return ResultInfo{}, err
	}
	info := ResultInfo{
		ResultPath: path,
		SHA256:     hex.EncodeToString(sum[:]),
		Bytes:      int64(len(raw)),
	}
	if len(raw) < inlineCap {
		info.Text = string(raw)
	} else {
		info.TextElided = true
	}
	return info, nil
}

// TruncateEventText applies the per-event cap and reports whether truncation occurred.
func TruncateEventText(raw []byte, capBytes int) EventText {
	if capBytes <= 0 {
		capBytes = DefaultEventTextCap
	}
	if len(raw) <= capBytes {
		return EventText{Text: string(raw)}
	}
	return EventText{Text: string(raw[:capBytes]), Truncated: true}
}

const (
	eventMetadataStringCap     = 4 * 1024
	eventMetadataKeyCap        = 128
	eventMetadataMaxMapEntries = 32
	eventMetadataMaxArrayItems = 32
	eventMetadataMaxDepth      = 4
)

// SanitizeEventMetadata returns a JSON-safe bounded copy of backend metadata.
func SanitizeEventMetadata(meta map[string]any) map[string]any {
	if len(meta) == 0 {
		return nil
	}
	sanitized, _ := sanitizeEventMetadataValue(meta, 0).(map[string]any)
	if len(sanitized) == 0 {
		return nil
	}
	raw, err := json.Marshal(sanitized)
	if err != nil || len(raw) > DefaultEventTextCap {
		return map[string]any{"truncated": true}
	}
	return sanitized
}

func sanitizeEventMetadataValue(v any, depth int) any {
	if depth >= eventMetadataMaxDepth {
		return "[agentbus: metadata truncated]"
	}
	switch x := v.(type) {
	case string:
		text := TruncateEventText([]byte(x), eventMetadataStringCap)
		if text.Truncated {
			return text.Text + "\n[agentbus: metadata truncated]"
		}
		return x
	case map[string]any:
		out := make(map[string]any, min(len(x), eventMetadataMaxMapEntries))
		n := 0
		for key, value := range x {
			if key == "agentbusRawText" {
				continue
			}
			if n >= eventMetadataMaxMapEntries {
				out["truncated"] = true
				break
			}
			out[capEventMetadataString(key, eventMetadataKeyCap)] = sanitizeEventMetadataValue(value, depth+1)
			n++
		}
		return out
	case []any:
		limit := min(len(x), eventMetadataMaxArrayItems)
		out := make([]any, 0, limit+1)
		for i := 0; i < limit; i++ {
			out = append(out, sanitizeEventMetadataValue(x[i], depth+1))
		}
		if len(x) > limit {
			out = append(out, "[agentbus: metadata truncated]")
		}
		return out
	default:
		return v
	}
}

func capEventMetadataString(s string, capBytes int) string {
	text := TruncateEventText([]byte(s), capBytes)
	return text.Text
}

// NewCappedLogWriter appends to a job's single-owner raw log, preserving
// evidence from prior turns and applying one cumulative cap. Callers must not
// share a path concurrently between sessions.
func NewCappedLogWriter(path string, capBytes int64) (*CappedLogWriter, error) {
	if capBytes <= 0 {
		capBytes = DefaultCappedLogCapBytes
	}
	marker := TruncationMarker()
	if capBytes < int64(len(marker)) {
		return nil, errors.New("raw log cap is smaller than the truncation marker")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*CappedLogWriter, error) {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() {
		return fail(errors.New("raw log is not a regular file"))
	}
	size := info.Size()
	payloadCap := capBytes - int64(len(marker))
	truncated := false
	if size == capBytes {
		tail := make([]byte, len(marker))
		if _, err := f.ReadAt(tail, size-int64(len(marker))); err != nil {
			return fail(err)
		}
		truncated = string(tail) == marker
	}
	if !truncated && size > payloadCap {
		return fail(fmt.Errorf("inconsistent raw log size %d for cap %d", size, capBytes))
	}
	if err := f.Chmod(0o600); err != nil {
		return fail(err)
	}
	written := size
	if truncated {
		written = payloadCap
	}
	return &CappedLogWriter{file: f, cap: capBytes, written: written, truncated: truncated}, nil
}

type cappedLogFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

// CappedLogWriter reserves marker space within the cap.
type CappedLogWriter struct {
	file      cappedLogFile
	cap       int64
	written   int64
	truncated bool
	disabled  bool
}

// Write implements io.Writer. Once a sink fails, it is never written again.
func (w *CappedLogWriter) Write(p []byte) (int, error) {
	if len(p) == 0 || w.truncated {
		return len(p), nil
	}
	if w.disabled {
		return 0, errors.New("raw log sink disabled")
	}
	remaining := w.cap - int64(len(TruncationMarker())) - w.written
	toWrite := len(p)
	if int64(toWrite) > remaining {
		toWrite = int(remaining)
	}
	if toWrite > 0 {
		n, err := w.file.Write(p[:toWrite])
		if n < 0 || n > toWrite {
			w.disabled = true
			return 0, errors.New("invalid raw log write count")
		}
		w.written += int64(n)
		if err == nil && n != toWrite {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.disabled = true
			return n, err
		}
	}
	if toWrite < len(p) {
		marker := []byte(TruncationMarker())
		n, err := w.file.Write(marker)
		if err == nil && n != len(marker) {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.disabled = true
			return toWrite, err
		}
		w.truncated = true
	}
	return len(p), nil
}

// Close fsyncs and closes the log.
func (w *CappedLogWriter) Close() error {
	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

// CapturedBytes reports cumulative provider payload bytes retained by the log.
func (w *CappedLogWriter) CapturedBytes() int64 {
	if w == nil {
		return 0
	}
	return w.written
}

// TruncationMarker is appended once when a capped log reaches its limit.
func TruncationMarker() string {
	return "\n[agentbus: log truncated]\n"
}
