//go:build darwin || linux

package service

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
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

// TranscriptItem is the wire-shaped normalized event captured in a job's
// private sidecar.
type TranscriptItem = protocol.TranscriptItem

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

func (run *activeExecution) noteItemSidecarDiagnostic(diagnostic string) {
	if run == nil || diagnostic == "" {
		return
	}
	run.mu.Lock()
	if run.itemSidecarDiag == "" {
		run.itemSidecarDiag = diagnostic
	}
	run.mu.Unlock()
}

func (run *activeExecution) itemSidecarDiagnostics() []string {
	if run == nil {
		return nil
	}
	run.mu.Lock()
	diagnostic := run.itemSidecarDiag
	run.mu.Unlock()
	if diagnostic == "" {
		return nil
	}
	return []string{diagnostic}
}

// itemSidecarWriter owns the append-only sidecar for one job. It continues to
// assign logical ordinals after a disk failure or cap so activity remains a
// measure of the live event stream rather than a measure of file bytes.
type itemSidecarWriter struct {
	path        string
	textCap     int
	fileCap     int64
	file        *os.File
	written     int64
	next        int
	stopped     bool
	diagnostic  string
	failureSink func(string)
}

func newItemSidecarWriter(path string, textCap int, fileCap int64) *itemSidecarWriter {
	if textCap <= 0 {
		textCap = transcriptItemTextCap
	}
	if fileCap <= 0 {
		fileCap = transcriptItemFileCap
	}
	writer := &itemSidecarWriter{path: path, textCap: textCap, fileCap: fileCap}
	if strings.TrimSpace(writer.path) == "" {
		writer.noteFailure("open", fmt.Errorf("path is empty"))
		return writer
	}
	if err := os.MkdirAll(filepath.Dir(writer.path), 0o700); err != nil {
		writer.noteFailure("create parent", err)
		return writer
	}
	file, err := os.OpenFile(writer.path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		writer.noteFailure("open", err)
		return writer
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		writer.noteFailure("set mode", err)
		return writer
	}
	writer.file = file
	return writer
}

func (writer *itemSidecarWriter) setFailureSink(sink func(string)) {
	if writer == nil {
		return
	}
	writer.failureSink = sink
	if writer.diagnostic != "" && writer.failureSink != nil {
		writer.failureSink(writer.diagnostic)
	}
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
		item.Text, item.Truncated = truncateTranscriptText(text, writer.textCap)
		item.Truncated = item.Truncated || alreadyTruncated
	}
	writer.write(item)
	return item
}

// truncateTranscriptText keeps text at or below capBytes without splitting a
// UTF-8 rune. It is shared by completed items and coalesced agent-text runs.
func truncateTranscriptText(text string, capBytes int) (string, bool) {
	if capBytes <= 0 {
		capBytes = transcriptItemTextCap
	}
	if len(text) <= capBytes {
		return text, false
	}
	end := capBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end], true
}

func (writer *itemSidecarWriter) write(item TranscriptItem) {
	if writer == nil || writer.stopped || writer.file == nil {
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
	if _, err := writer.file.Write(line); err != nil {
		writer.noteFailure("append item", err)
		return
	}
	writer.written += int64(len(line))
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
	if writer.file == nil {
		writer.noteFailure("record append stop", fmt.Errorf("sidecar file is unavailable"))
		return
	}
	if writer.written+int64(len(transcriptItemStopLine)) > writer.fileCap {
		writer.noteFailure("record append stop", fmt.Errorf("sidecar is already at its %d-byte cap", writer.fileCap))
		return
	}
	if _, err := writer.file.Write(transcriptItemStopLine); err != nil {
		writer.noteFailure("record append stop", err)
		return
	}
	writer.written += int64(len(transcriptItemStopLine))
	writer.stopped = true
}

func (writer *itemSidecarWriter) noteFailure(operation string, err error) {
	if writer == nil || writer.diagnostic != "" || err == nil {
		return
	}
	writer.stopped = true
	writer.diagnostic = "item sidecar " + operation + ": " + err.Error()
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	if writer.failureSink != nil {
		writer.failureSink(writer.diagnostic)
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

func itemSidecarPath(stdoutPath string) (string, error) {
	base, ok := strings.CutSuffix(stdoutPath, ".stdout.log")
	if !ok {
		return "", fmt.Errorf("stdout log path %q is missing .stdout.log suffix", stdoutPath)
	}
	return base + ".items.jsonl", nil
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
	if event.Type == engine.EventAgentText {
		assembler.appendAgentText(rawText)
		return
	}
	if event.ObservedWorkspaceWriteItem {
		assembler.flushMessage()
		assembler.append(transcriptItemFileChange, event.Name, "", false)
		return
	}
	switch event.Type {
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
	assembler.noteProgress(time.Now().UTC())
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
	truncated, _ := truncateTranscriptText(text, remaining)
	assembler.message.WriteString(truncated)
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
