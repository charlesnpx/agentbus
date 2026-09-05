//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	transcriptItemTextCap       = 4 * 1024
	transcriptItemFileCap       = 16 * 1024 * 1024
	itemSidecarDiagnosticPrefix = "item sidecar "
)

var (
	transcriptItemStopLine     = []byte("{\"appendStopped\":true}\n")
	transcriptItemCompleteLine = []byte("{\"captureComplete\":true}\n")
	syncItemSidecarDirectory   = func(dir string) error {
		file, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer file.Close()
		return file.Sync()
	}
)

type transcriptItemKind string

const (
	transcriptItemMessage    transcriptItemKind = "message"
	transcriptItemReasoning  transcriptItemKind = "reasoning"
	transcriptItemTool       transcriptItemKind = "tool"
	transcriptItemToolResult transcriptItemKind = "toolResult"
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
	incomplete  bool
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
	writer := &itemSidecarWriter{
		path:    path,
		textCap: textCap,
		fileCap: fileCap,
	}
	if strings.TrimSpace(writer.path) == "" {
		writer.noteFailure("open", fmt.Errorf("path is empty"))
		return writer
	}
	parent := filepath.Dir(writer.path)
	directoriesToSync, err := itemSidecarDirectoriesToSync(parent)
	if err != nil {
		writer.noteFailure("inspect parent", err)
		return writer
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
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
	// Publish the sidecar name before a later receipt can make its contents
	// authoritative. If MkdirAll created any components, their parent entries
	// need the same treatment so the sidecar remains reachable after a crash.
	for _, dir := range directoriesToSync {
		if err := syncItemSidecarDirectory(dir); err != nil {
			writer.noteFailure("sync parent directory", fmt.Errorf("%s: %w", dir, err))
			return writer
		}
	}
	return writer
}

// itemSidecarDirectoriesToSync returns the sidecar parent followed by every
// ancestor through the first pre-existing directory. Syncing that chain after
// creation makes both the new sidecar entry and any MkdirAll-created components
// durable without charging append operations for directory syncs.
func itemSidecarDirectoriesToSync(dir string) ([]string, error) {
	directories := []string{dir}
	for current := dir; ; {
		info, err := os.Stat(current)
		switch {
		case err == nil:
			if !info.IsDir() {
				return nil, fmt.Errorf("sidecar parent %q is not a directory", current)
			}
			return directories, nil
		case !errors.Is(err, os.ErrNotExist):
			return nil, err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil, fmt.Errorf("sidecar parent %q has no existing ancestor", dir)
		}
		directories = append(directories, parent)
		current = parent
	}
}

func (writer *itemSidecarWriter) setFailureSink(sink func(string)) {
	if writer == nil {
		return
	}
	writer.failureSink = sink
	if writer.failureSink != nil {
		for _, diagnostic := range writer.diagnostics() {
			writer.failureSink(diagnostic)
		}
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
	if completion := int64(len(transcriptItemCompleteLine)); completion > reserved {
		reserved = completion
	}
	if writer.written >= writer.fileCap-reserved {
		return 0
	}
	return writer.fileCap - reserved - writer.written
}

func (writer *itemSidecarWriter) writeControlLine(file *os.File, line []byte) error {
	if writer == nil || file == nil {
		return fmt.Errorf("sidecar file is unavailable")
	}
	if writer.written+int64(len(line)) > writer.fileCap {
		return fmt.Errorf("sidecar is already at its %d-byte cap", writer.fileCap)
	}
	written, err := file.Write(line)
	if err != nil {
		return err
	}
	if written != len(line) {
		return io.ErrShortWrite
	}
	writer.written += int64(written)
	return nil
}

func (writer *itemSidecarWriter) markStopped() {
	if writer == nil || writer.stopped {
		return
	}
	if writer.file == nil {
		writer.noteFailure("record append stop", fmt.Errorf("sidecar file is unavailable"))
		return
	}
	if err := writer.writeControlLine(writer.file, transcriptItemStopLine); err != nil {
		writer.noteFailure("record append stop", err)
		return
	}
	writer.stopped = true
	writer.incomplete = true
}

func (writer *itemSidecarWriter) noteFailure(operation string, err error) {
	if writer == nil || writer.diagnostic != "" || err == nil {
		return
	}
	writer.stopped = true
	writer.incomplete = true
	writer.diagnostic = itemSidecarDiagnosticPrefix + operation + ": " + err.Error()
	if writer.file != nil {
		_ = writer.file.Close()
		writer.file = nil
	}
	if writer.failureSink != nil {
		writer.failureSink(writer.diagnostic)
	}
}

// markIncomplete withholds the completion receipt for uncertainty that does
// not itself prevent sidecar I/O, such as a dropped backend frame.
func (writer *itemSidecarWriter) markIncomplete() {
	if writer == nil {
		return
	}
	writer.incomplete = true
}

func (writer *itemSidecarWriter) close() {
	if writer == nil || writer.file == nil {
		return
	}
	file := writer.file
	writer.file = nil
	// The receipt is written only after every item is durable, so a reader that
	// sees it never treats an unsynced item prefix as a completed capture.
	if err := file.Sync(); err != nil {
		writer.noteFailure("sync", err)
		_ = file.Close()
		return
	}
	if writer.incomplete {
		if err := file.Close(); err != nil {
			writer.noteFailure("close", err)
		}
		return
	}
	if err := writer.writeControlLine(file, transcriptItemCompleteLine); err != nil {
		writer.noteFailure("record capture completion", err)
		_ = file.Close()
		return
	}
	if err := file.Sync(); err != nil {
		writer.noteFailure("sync capture completion", err)
		_ = file.Close()
		return
	}
	if err := file.Close(); err != nil {
		writer.noteFailure("close", err)
	}
}

func (writer *itemSidecarWriter) diagnostics() []string {
	if writer == nil {
		return nil
	}
	diagnostics := make([]string, 0, 1)
	if writer.diagnostic != "" {
		diagnostics = append(diagnostics, writer.diagnostic)
	}
	return diagnostics
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
	lastAgentRun     string
	lastAgentRunOK   bool
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
	case engine.EventProgress, engine.EventModelReported:
		assembler.noteProgress(time.Now().UTC())
		return
	}

	switch event.Type {
	case engine.EventResultMessage:
		assembler.appendResultMessage(rawText)
	case engine.EventReasoning:
		assembler.flushMessage()
		assembler.append(transcriptItemReasoning, "", rawText, false)
	case engine.EventToolUse:
		assembler.flushMessage()
		assembler.append(transcriptItemTool, event.Name, rawText, false)
	case engine.EventToolResult:
		assembler.flushMessage()
		assembler.append(transcriptItemToolResult, event.Name, rawText, false)
	case engine.EventWarning:
		if _, dropped := engine.TransportFrameDropsFromMetadata(event.Metadata); dropped {
			assembler.writer.markIncomplete()
		}
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
	if !assembler.messageActive {
		// A new run supersedes any run retained across an intervening event.
		assembler.lastAgentRun = ""
		assembler.lastAgentRunOK = false
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
	truncated, _ := truncateTranscriptText(text, remaining)
	assembler.message.WriteString(truncated)
	assembler.messageTruncated = true
}

func (assembler *itemAssembler) flushMessage() {
	if assembler == nil || !assembler.messageActive {
		return
	}
	text := assembler.message.String()
	assembler.append(transcriptItemMessage, "", text, assembler.messageTruncated)
	assembler.lastAgentRun = text
	assembler.lastAgentRunOK = !assembler.messageTruncated
	assembler.message.Reset()
	assembler.messageActive = false
	assembler.messageTruncated = false
}

// appendResultMessage ends an agent-text run. Exact byte equality, including
// trailing whitespace, makes the most recent untruncated run the result item,
// even if a non-message item already flushed it. Any difference remains a
// second item so content is never silently discarded. A truncated run cannot
// establish equality with its full original text, so its result is retained
// separately.
func (assembler *itemAssembler) appendResultMessage(text string) {
	if assembler == nil {
		return
	}
	matchesAgentRun := false
	if assembler.messageActive {
		matchesAgentRun = !assembler.messageTruncated && assembler.message.String() == text
	} else {
		matchesAgentRun = assembler.lastAgentRunOK && assembler.lastAgentRun == text
	}
	assembler.flushMessage()
	assembler.lastAgentRun = ""
	assembler.lastAgentRunOK = false
	if matchesAgentRun {
		return
	}
	assembler.append(transcriptItemMessage, "", text, false)
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
