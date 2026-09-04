//go:build darwin || linux

package service

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const (
	transcriptItemTextCap = 4 * 1024
	transcriptItemFileCap = 16 * 1024 * 1024
)

var transcriptItemStopLine = []byte("{\"appendStopped\":true}\n")

type transcriptItemKind string

const (
	transcriptItemMessage    transcriptItemKind = "message"
	transcriptItemTool       transcriptItemKind = "tool"
	transcriptItemFileChange transcriptItemKind = "fileChange"
	transcriptItemWarning    transcriptItemKind = "warning"
	transcriptItemError      transcriptItemKind = "error"
)

// TranscriptItem is one normalized, backend-neutral event captured for a job.
// Its Kind is one of message, tool, fileChange, warning, or error.
type TranscriptItem struct {
	Ordinal   int       `json:"ordinal"`
	At        time.Time `json:"at"`
	Kind      string    `json:"kind"`
	Name      string    `json:"name,omitempty"`
	Text      string    `json:"text,omitempty"`
	Truncated bool      `json:"truncated,omitempty"`
}

// ItemActivity is the in-memory transcript progress for a running job.
// LastActivityAt also advances for contentless progress events; LastItemAt
// changes only when an item is assembled.
type ItemActivity struct {
	ItemCount      int
	LastItemAt     time.Time
	LastActivityAt time.Time
}

// ItemActivity returns transcript progress only while this daemon has an
// active execution for jobID. The state is intentionally not persisted.
func (s *Server) ItemActivity(jobID string) (ItemActivity, bool) {
	run := s.activeExecution(jobID)
	if run == nil {
		return ItemActivity{}, false
	}
	return run.itemActivity(), true
}

func (run *activeExecution) noteTranscriptItem(at time.Time) {
	if run == nil {
		return
	}
	run.mu.Lock()
	run.itemCount++
	run.lastItemAt = at
	run.lastActivityAt = at
	run.mu.Unlock()
}

func (run *activeExecution) noteTranscriptProgress(at time.Time) {
	if run == nil {
		return
	}
	run.mu.Lock()
	run.lastActivityAt = at
	run.mu.Unlock()
}

func (run *activeExecution) itemActivity() ItemActivity {
	if run == nil {
		return ItemActivity{}
	}
	run.mu.Lock()
	activity := ItemActivity{
		ItemCount:      run.itemCount,
		LastItemAt:     run.lastItemAt,
		LastActivityAt: run.lastActivityAt,
	}
	run.mu.Unlock()
	return activity
}

// itemSidecarWriter owns the append-only sidecar for one job. It continues to
// assign logical ordinals after a disk failure or cap so activity remains a
// measure of the live event stream rather than a measure of file bytes.
type itemSidecarWriter struct {
	path       string
	textCap    int
	fileCap    int64
	file       *os.File
	written    int64
	next       int
	stopped    bool
	failed     bool
	diagnostic string
}

func newItemSidecarWriter(path string) *itemSidecarWriter {
	return newItemSidecarWriterWithCaps(path, transcriptItemTextCap, transcriptItemFileCap)
}

func newItemSidecarWriterWithCaps(path string, textCap int, fileCap int64) *itemSidecarWriter {
	if textCap <= 0 {
		textCap = transcriptItemTextCap
	}
	if fileCap <= 0 {
		fileCap = transcriptItemFileCap
	}
	writer := &itemSidecarWriter{path: path, textCap: textCap, fileCap: fileCap}
	writer.open()
	return writer
}

func unavailableItemSidecarWriter(err error) *itemSidecarWriter {
	writer := &itemSidecarWriter{textCap: transcriptItemTextCap, fileCap: transcriptItemFileCap}
	writer.noteFailure("resolve path", err)
	return writer
}

func (writer *itemSidecarWriter) append(kind transcriptItemKind, name, text string, alreadyTruncated bool) TranscriptItem {
	if writer == nil {
		return TranscriptItem{At: time.Now().UTC(), Kind: string(kind)}
	}
	writer.next++
	item := TranscriptItem{
		Ordinal: writer.next,
		At:      time.Now().UTC(),
		Kind:    string(kind),
		Name:    name,
	}
	if kind != transcriptItemFileChange {
		item.Text, item.Truncated = capTranscriptItemText(text, writer.textCap)
		item.Truncated = item.Truncated || alreadyTruncated
	}
	writer.write(item)
	return item
}

func capTranscriptItemText(text string, capBytes int) (string, bool) {
	if capBytes <= 0 {
		capBytes = transcriptItemTextCap
	}
	if len(text) <= capBytes {
		return text, false
	}
	return text[:capBytes], true
}

func (writer *itemSidecarWriter) write(item TranscriptItem) {
	if writer == nil || writer.stopped || writer.failed {
		return
	}
	writer.open()
	if writer.stopped || writer.failed || writer.file == nil {
		return
	}
	line, err := json.Marshal(item)
	if err != nil {
		writer.noteFailure("encode item", err)
		return
	}
	line = append(line, '\n')
	if int64(len(line)) > writer.remainingPayload() {
		writer.markStopped()
		return
	}
	if err := writeAll(writer.file, line); err != nil {
		writer.noteFailure("append item", err)
		return
	}
	writer.written += int64(len(line))
}

func (writer *itemSidecarWriter) open() {
	if writer == nil || writer.file != nil || writer.failed || writer.stopped {
		return
	}
	if strings.TrimSpace(writer.path) == "" {
		writer.noteFailure("open", fmt.Errorf("path is empty"))
		return
	}
	if err := os.MkdirAll(filepath.Dir(writer.path), 0o700); err != nil {
		writer.noteFailure("create parent", err)
		return
	}
	file, err := os.OpenFile(writer.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		writer.noteFailure("open", err)
		return
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		writer.noteFailure("set mode", err)
		return
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		writer.noteFailure("stat", err)
		return
	}
	writer.file = file
	writer.written = info.Size()
}

func (writer *itemSidecarWriter) remainingPayload() int64 {
	if writer == nil {
		return 0
	}
	reserved := int64(len(transcriptItemStopLine))
	if writer.written >= writer.fileCap-reserved {
		return 0
	}
	return writer.fileCap - reserved - writer.written
}

func (writer *itemSidecarWriter) markStopped() {
	if writer == nil || writer.stopped {
		return
	}
	writer.stopped = true
	if writer.failed || writer.file == nil {
		return
	}
	if writer.written+int64(len(transcriptItemStopLine)) > writer.fileCap {
		writer.noteFailure("record append stop", fmt.Errorf("sidecar is already at its %d-byte cap", writer.fileCap))
		return
	}
	if err := writeAll(writer.file, transcriptItemStopLine); err != nil {
		writer.noteFailure("record append stop", err)
		return
	}
	writer.written += int64(len(transcriptItemStopLine))
}

func (writer *itemSidecarWriter) noteFailure(operation string, err error) {
	if writer == nil || writer.failed || err == nil {
		return
	}
	writer.failed = true
	writer.diagnostic = "item sidecar " + operation + ": " + err.Error()
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
}

func (writer *itemSidecarWriter) close() {
	if writer == nil || writer.file == nil {
		return
	}
	file := writer.file
	writer.file = nil
	if err := file.Sync(); err != nil {
		writer.noteFailure("sync", err)
	}
	if err := file.Close(); err != nil {
		writer.noteFailure("close", err)
	}
}

func (writer *itemSidecarWriter) diagnostics() []string {
	if writer == nil || writer.diagnostic == "" {
		return nil
	}
	return []string{writer.diagnostic}
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// itemAssembler turns the normalized engine event stream into transcript
// items without giving adapters a second persistence path.
type itemAssembler struct {
	run    *activeExecution
	writer *itemSidecarWriter

	message          strings.Builder
	messageActive    bool
	messageTruncated bool
}

func newItemAssembler(run *activeExecution, writer *itemSidecarWriter) *itemAssembler {
	return &itemAssembler{run: run, writer: writer}
}

func (assembler *itemAssembler) absorb(event engine.Event, rawText string) {
	if assembler == nil {
		return
	}
	if event.ObservedWorkspaceWriteItem {
		assembler.flushMessage()
		assembler.append(transcriptItemFileChange, event.Name, "", false)
		return
	}
	switch event.Type {
	case engine.EventAgentText:
		assembler.appendAgentText(rawText)
		return
	case engine.EventProgress:
		assembler.noteProgress(time.Now().UTC())
		return
	case engine.EventModelReported:
		return
	}

	switch event.Type {
	case engine.EventResultMessage:
		assembler.flushMessage()
		assembler.append(transcriptItemMessage, "", rawText, false)
	case engine.EventToolUse:
		assembler.flushMessage()
		assembler.append(transcriptItemTool, event.Name, rawText, false)
	case engine.EventWarning:
		assembler.flushMessage()
		assembler.append(transcriptItemWarning, "", rawText, false)
	case engine.EventTerminalError:
		assembler.flushMessage()
		assembler.append(transcriptItemError, "", rawText, false)
	}
}

// EventAgentText has no message-boundary signal in engine.Event. Consecutive
// runs therefore coalesce, and two agent messages without an intervening
// item-producing event merge. Adding that boundary would enlarge the
// convo-relay seam, so this accepted limitation remains until a concrete case
// needs it.
func (assembler *itemAssembler) appendAgentText(text string) {
	if assembler == nil {
		return
	}
	assembler.messageActive = true
	if assembler.messageTruncated || len(text) == 0 {
		return
	}
	capBytes := transcriptItemTextCap
	if assembler.writer != nil {
		capBytes = assembler.writer.textCap
	}
	remaining := capBytes - assembler.message.Len()
	if remaining <= 0 {
		assembler.messageTruncated = true
		return
	}
	if len(text) <= remaining {
		assembler.message.WriteString(text)
		return
	}
	assembler.message.WriteString(text[:remaining])
	assembler.messageTruncated = true
}

func (assembler *itemAssembler) flushMessage() {
	if assembler == nil || !assembler.messageActive {
		return
	}
	assembler.append(transcriptItemMessage, "", assembler.message.String(), assembler.messageTruncated)
	assembler.message.Reset()
	assembler.messageActive = false
	assembler.messageTruncated = false
}

func (assembler *itemAssembler) finishTurn() {
	if assembler != nil {
		assembler.flushMessage()
	}
}

func (assembler *itemAssembler) append(kind transcriptItemKind, name, text string, alreadyTruncated bool) {
	if assembler == nil || assembler.writer == nil {
		return
	}
	item := assembler.writer.append(kind, name, text, alreadyTruncated)
	assembler.run.noteTranscriptItem(item.At)
}

func (assembler *itemAssembler) noteProgress(at time.Time) {
	if assembler != nil {
		assembler.run.noteTranscriptProgress(at)
	}
}

func appendItemSidecarDiagnostics(diagnostics []string, writer *itemSidecarWriter) []string {
	if writer == nil {
		return diagnostics
	}
	writer.close()
	return append(diagnostics, writer.diagnostics()...)
}
