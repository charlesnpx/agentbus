//go:build darwin || linux

package service

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func transcriptItemPath(t *testing.T, record jobstore.Record) string {
	t.Helper()
	path, err := engine.ItemPathForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
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
	backend := &executionFakeBackend{name: "items-coalesce"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6101, PGID: 6101, StartTime: "items-coalesce"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventAgentText, Text: "hel"},
					engine.Event{Type: engine.EventAgentText, Text: "lo"},
					engine.Event{Type: engine.EventToolUse, Name: "shell", Text: "pwd"},
					engine.Event{Type: engine.EventProgress},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "coalesce", nil)
	run := newActiveExecution(record.JobID, backend)
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: run}
	server.executionMu.Unlock()
	server.runJob(context.Background(), record, run)

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
}

func TestTranscriptItemsCapText(t *testing.T) {
	backend := &executionFakeBackend{name: "items-cap"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6103, PGID: 6103, StartTime: "items-cap"}, 0)
				return executionEvents(engine.Event{
					Type: engine.EventToolUse,
					Name: "large-output",
					Text: strings.Repeat("x", transcriptItemTextCap+1),
				}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "cap", nil)
	runExecution(t, server, record)

	items := readTranscriptItems(t, record)
	if len(items) != 1 {
		t.Fatalf("item count = %d, want 1: %#v", len(items), items)
	}
	if got := items[0]; len(got.Text) != transcriptItemTextCap || !got.Truncated {
		t.Fatalf("capped item = %+v, want %d-byte text with truncation", got, transcriptItemTextCap)
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

func TestTranscriptItemSidecarFailureDoesNotChangeOutcome(t *testing.T) {
	backend := &executionFakeBackend{name: "items-write-failure"}
	backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 6106, PGID: 6106, StartTime: "items-write-failure"}, 0)
				return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: "authoritative"}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "sidecar failure", nil)
	if err := os.MkdirAll(transcriptItemPath(t, record), 0o700); err != nil {
		t.Fatal(err)
	}
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != protocol.PublicStateCompleted || terminal.ResultText != "authoritative" {
		t.Fatalf("terminal record = %+v, want completed authoritative result", terminal)
	}
	if !strings.Contains(strings.Join(terminal.Diagnostics, "\n"), "item sidecar open") {
		t.Fatalf("diagnostics = %#v, want sidecar failure", terminal.Diagnostics)
	}
}
