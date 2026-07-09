package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
)

const (
	DefaultInlineResultCap = 256 * 1024
	DefaultEventTextCap    = 64 * 1024
)

// EventText is text prepared for a streaming event.
type EventText struct {
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
}

// WriteResult spills the authoritative final result and returns result metadata.
func (s *Store) WriteResult(jobID string, raw []byte, inlineCap int) (ResultInfo, error) {
	if inlineCap <= 0 {
		inlineCap = DefaultInlineResultCap
	}
	path, err := s.resultPath(jobID)
	if err != nil {
		return ResultInfo{}, err
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

// NewCappedLogWriter opens a protocol-mode capped log file.
func NewCappedLogWriter(path string, capBytes int64) (*CappedLogWriter, error) {
	if capBytes <= 0 {
		capBytes = 10 * 1024 * 1024
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &CappedLogWriter{file: f, cap: capBytes}, nil
}

// CappedLogWriter writes at most cap bytes and appends a truncation marker.
type CappedLogWriter struct {
	file      *os.File
	cap       int64
	written   int64
	truncated bool
}

// Write implements io.Writer.
func (w *CappedLogWriter) Write(p []byte) (int, error) {
	if w.truncated {
		return len(p), nil
	}
	marker := truncationMarker()
	payloadCap := w.cap - int64(len(marker))
	if payloadCap < 0 {
		payloadCap = 0
	}
	remaining := payloadCap - w.written
	if remaining <= 0 {
		if err := w.markTruncated(); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if int64(len(p)) <= remaining {
		n, err := w.file.Write(p)
		w.written += int64(n)
		return n, err
	}
	n, err := w.file.Write(p[:remaining])
	w.written += int64(n)
	if err != nil {
		return n, err
	}
	if err := w.markTruncated(); err != nil {
		return n, err
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

func (w *CappedLogWriter) markTruncated() error {
	if w.truncated {
		return nil
	}
	marker := truncationMarker()
	if int64(len(marker)) > w.cap {
		marker = marker[:int(w.cap)]
	}
	_, err := w.file.WriteString(marker)
	w.truncated = true
	return err
}

func truncationMarker() string {
	return "\n[agentbus: log truncated]\n"
}
