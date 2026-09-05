//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func transcriptItemPath(t *testing.T, record jobstore.Record) string {
	t.Helper()
	logs, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	path, err := itemSidecarPath(logs.Stdout)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func readTranscriptItems(t *testing.T, record jobstore.Record) []TranscriptItem {
	t.Helper()
	file, err := os.Open(transcriptItemPath(t, record))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	var items []TranscriptItem
	for {
		var item TranscriptItem
		err := decoder.Decode(&item)
		if err == io.EOF {
			return items
		}
		if err != nil {
			t.Fatal(err)
		}
		if item.Ordinal == 0 {
			t.Fatalf("sidecar contains a non-item record: %+v", item)
		}
		items = append(items, item)
	}
}

func TestTranscriptItemsCoalesceAgentTextRun(t *testing.T) {
	events := make(chan engine.Event)
	agentTextSent := make(chan struct{})
	releaseEvents := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseEvents) }) }
	t.Cleanup(release)

	backend := &executionFakeBackend{name: "items-coalesce"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6101, PGID: 6101, StartTime: "items-coalesce"}, 0)
				go func() {
					events <- engine.Event{Type: engine.EventAgentText, Text: "hel"}
					events <- engine.Event{Type: engine.EventAgentText, Text: "lo"}
					close(agentTextSent)
					<-releaseEvents
					events <- engine.Event{Type: engine.EventToolUse, Name: "shell", Text: "pwd"}
					events <- engine.Event{Type: engine.EventProgress}
					close(events)
				}()
				return events, nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "coalesce", nil)
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(done)
	}()
	select {
	case <-agentTextSent:
	case <-time.After(time.Second):
		t.Fatal("agent-text events did not arrive")
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		activity, ok := server.ItemActivity(record.JobID)
		if ok && !activity.LastActivityAt.IsZero() {
			if activity.ItemCount != 0 || !activity.LastItemAt.IsZero() {
				t.Fatalf("agent-text activity before flush = %+v, want timestamp only", activity)
			}
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("agent-text did not advance activity before the message flush")
		case <-ticker.C:
		}
	}
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("coalesced turn did not finish")
	}

	items := readTranscriptItems(t, record)
	if len(items) != 2 {
		t.Fatalf("item count = %d, want 2: %#v", len(items), items)
	}
	if got := items[0]; got.Ordinal != 1 || got.Kind != string(transcriptItemMessage) || got.Text != "hello" {
		t.Fatalf("message item = %+v, want ordinal 1 concatenated message", got)
	}
	if got := items[1]; got.Ordinal != 2 || got.Kind != string(transcriptItemTool) || got.Name != "shell" || got.Text != "pwd" {
		t.Fatalf("tool item = %+v, want ordinal 2 tool", got)
	}
	info, err := os.Stat(transcriptItemPath(t, record))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("sidecar mode = %o, want 600", got)
	}
	activity, ok := server.ItemActivity(record.JobID)
	if !ok || activity.ItemCount != 2 || activity.LastItemAt.IsZero() || activity.LastActivityAt.IsZero() {
		t.Fatalf("item activity = %+v, present=%t", activity, ok)
	}
	if activity.LastActivityAt.Before(activity.LastItemAt) {
		t.Fatalf("activity time %s precedes last item time %s", activity.LastActivityAt, activity.LastItemAt)
	}
}

func TestTranscriptItemsSuppressWorkspaceWriteText(t *testing.T) {
	backend := &executionFakeBackend{name: "items-file-change"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6102, PGID: 6102, StartTime: "items-file-change"}, 0)
				return executionEvents(engine.Event{
					Type:                       engine.EventToolUse,
					Name:                       "fileChange",
					Text:                       "private path and contents",
					ObservedWorkspaceWriteItem: true,
				}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "file change", nil)
	runExecution(t, server, record)

	items := readTranscriptItems(t, record)
	if len(items) != 1 {
		t.Fatalf("item count = %d, want 1: %#v", len(items), items)
	}
	if got := items[0]; got.Kind != string(transcriptItemFileChange) || got.Name != "fileChange" || got.Text != "" || got.Truncated {
		t.Fatalf("file-change item = %+v, want text-free fileChange", got)
	}
	sidecar, err := os.ReadFile(transcriptItemPath(t, record))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(sidecar, []byte(`"truncated":false`)) {
		t.Fatalf("sidecar = %s, want explicit false truncated member", sidecar)
	}
}

func TestTranscriptItemsCapTextAndDoNotCoalesceReasoningOrToolResults(t *testing.T) {
	prefix := strings.Repeat("x", transcriptItemTextCap-1)
	tooLong := prefix + "€"
	backend := &executionFakeBackend{name: "items-cap"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6103, PGID: 6103, StartTime: "items-cap"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventAgentText, Text: prefix},
					engine.Event{Type: engine.EventAgentText, Text: "€"},
					engine.Event{Type: engine.EventToolUse, Name: "large-output", Text: tooLong},
					engine.Event{Type: "Reasoning", Text: tooLong},
					engine.Event{Type: "Reasoning", Text: tooLong},
					engine.Event{Type: "ToolResult", Name: "large-result-one", Text: tooLong},
					engine.Event{Type: "ToolResult", Name: "large-result-two", Text: tooLong},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "cap", nil)
	runExecution(t, server, record)

	items := readTranscriptItems(t, record)
	want := []struct {
		kind transcriptItemKind
		name string
	}{
		{kind: transcriptItemMessage},
		{kind: transcriptItemTool, name: "large-output"},
		{kind: transcriptItemReasoning},
		{kind: transcriptItemReasoning},
		{kind: transcriptItemToolResult, name: "large-result-one"},
		{kind: transcriptItemToolResult, name: "large-result-two"},
	}
	if len(items) != len(want) {
		t.Fatalf("item count = %d, want %d: %#v", len(items), len(want), items)
	}
	for index, item := range items {
		if item.Kind != string(want[index].kind) || item.Name != want[index].name {
			t.Fatalf("item %d = %+v, want kind %q and name %q", index, item, want[index].kind, want[index].name)
		}
		if len(item.Text) > transcriptItemTextCap || !utf8.ValidString(item.Text) || item.Text != prefix || !item.Truncated {
			t.Fatalf("capped item = %+v, want valid %d-byte-or-less text ending before the partial rune", item, transcriptItemTextCap)
		}
	}
}

func TestItemSidecarWriterStopsAtSmallFileCap(t *testing.T) {
	const fileCap = 256
	path := filepath.Join(t.TempDir(), "items.jsonl")
	writer := newItemSidecarWriter(path, 8, fileCap)
	for range 3 {
		writer.append(transcriptItemTool, "tool", "output", false)
	}
	writer.close()
	if writer.diagnostic != "" {
		t.Fatalf("sidecar diagnostic = %q, want no write failure", writer.diagnostic)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(contents)) > fileCap {
		t.Fatalf("sidecar size = %d, want at most %d", len(contents), fileCap)
	}
	if !bytes.HasSuffix(contents, transcriptItemStopLine) || bytes.Count(contents, transcriptItemStopLine) != 1 {
		t.Fatalf("sidecar = %q, want one append-stopped marker", contents)
	}
}

func TestItemSidecarPathDerivesFromStdoutLog(t *testing.T) {
	stdout := filepath.Join(t.TempDir(), "job_123.stdout.log")
	path, err := itemSidecarPath(stdout)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.TrimSuffix(stdout, ".stdout.log") + ".items.jsonl"; path != want {
		t.Fatalf("sidecar path = %q, want %q", path, want)
	}
	if _, err := itemSidecarPath(strings.TrimSuffix(stdout, ".stdout.log") + ".stderr.log"); err == nil {
		t.Fatal("non-stdout log path derived a sidecar path")
	}
}

func TestTranscriptItemsCorrectionTurnContinuesOrdinals(t *testing.T) {
	const initialResult = `{"wrong":true}`
	const correctedResult = `{"ok":true}`
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	turns := 0
	backend := &executionFakeBackend{name: "items-correction"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				turns++
				switch turns {
				case 1:
					input.OnProcessStart(engine.ProcessRef{PID: 6104, PGID: 6104, StartTime: "items-initial"}, 0)
					return executionEvents(
						engine.Event{Type: engine.EventAgentText, Text: "draft"},
						engine.Event{Type: engine.EventResultMessage, Text: initialResult},
					), nil
				case 2:
					input.OnProcessStart(engine.ProcessRef{PID: 6105, PGID: 6105, StartTime: "items-correction"}, 0)
					return executionEvents(
						engine.Event{Type: engine.EventToolUse, Name: "validate", Text: "retry"},
						engine.Event{Type: engine.EventResultMessage, Text: correctedResult},
					), nil
				default:
					t.Fatalf("turn count = %d, want 2", turns)
					return nil, nil
				}
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "correct", nil, schemaRaw)
	runExecution(t, server, record)

	items := readTranscriptItems(t, record)
	if len(items) != 4 {
		t.Fatalf("item count = %d, want 4: %#v", len(items), items)
	}
	for index, item := range items {
		if item.Ordinal != index+1 {
			t.Fatalf("item %d ordinal = %d, want %d", index, item.Ordinal, index+1)
		}
	}
	if got := items[3]; got.Kind != string(transcriptItemMessage) || got.Text != correctedResult {
		t.Fatalf("last correction item = %+v, want corrected result message", got)
	}
}

func TestTranscriptItemSidecarFailureSurvivesCancellation(t *testing.T) {
	events := make(chan engine.Event)
	turnStarted := make(chan struct{})
	var closeEvents sync.Once
	closeStream := func() { closeEvents.Do(func() { close(events) }) }
	t.Cleanup(closeStream)

	backend := &executionFakeBackend{name: "items-write-failure"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, _ engine.TurnInput) (<-chan engine.Event, error) {
				close(turnStarted)
				return events, nil
			},
			interrupt: func(context.Context) error {
				closeStream()
				return nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "sidecar failure", nil)
	if err := os.MkdirAll(transcriptItemPath(t, record), 0o700); err != nil {
		t.Fatal(err)
	}
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	done := make(chan struct{})
	go func() {
		server.runJob(context.Background(), record, run)
		close(done)
	}()
	select {
	case <-turnStarted:
	case <-time.After(time.Second):
		t.Fatal("turn did not start")
	}
	if diagnostics := run.itemSidecarDiagnostics(); !strings.Contains(strings.Join(diagnostics, "\n"), "item sidecar open") {
		t.Fatalf("active sidecar diagnostics = %#v, want constructor failure", diagnostics)
	}
	canceled := server.handleJobCancel(mustJSON(t, protocol.JobCancelParams{JobID: record.JobID}))
	if canceled.err != nil {
		t.Fatalf("job.cancel error = %#v", canceled.err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled turn did not finish")
	}

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != protocol.PublicStateCanceled {
		t.Fatalf("terminal record = %+v, want canceled result", terminal)
	}
	if !strings.Contains(strings.Join(terminal.Diagnostics, "\n"), "item sidecar open") {
		t.Fatalf("diagnostics = %#v, want sidecar failure", terminal.Diagnostics)
	}
}
