//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
		var line json.RawMessage
		err := decoder.Decode(&line)
		if err == io.EOF {
			return items
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(line, transcriptItemStopLine[:len(transcriptItemStopLine)-1]) ||
			bytes.Equal(line, transcriptItemCompleteLine[:len(transcriptItemCompleteLine)-1]) {
			continue
		}
		var item TranscriptItem
		if err := json.Unmarshal(line, &item); err != nil {
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
	if result := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID}); result.Gap {
		t.Fatalf("clean file-change transcript = %#v, want gap false", result)
	}

	t.Run("dropped completed frame marks gap", func(t *testing.T) {
		drops := engine.TransportFrameDrops{Count: 1, Bytes: 1024, RedactedPrefix: "method=item/completed"}
		backend := &executionFakeBackend{name: "items-dropped-completed-frame"}
		backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
			return &executionFakeSession{
				turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
					input.OnProcessStart(engine.ProcessRef{PID: 6107, PGID: 6107, StartTime: "items-dropped-completed-frame"}, 0)
					return executionEvents(engine.Event{
						Type:     engine.EventWarning,
						Text:     "discarded completed item frame",
						Metadata: drops.EventMetadata(),
					}), nil
				},
			}, nil
		}
		server := newExecutionServer(t, backend)
		record := queuedExecutionRecord(t, server, backend.Name(), "dropped completed frame", nil)
		runExecution(t, server, record)

		result := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID})
		if !result.Gap || result.ItemCount != 1 || result.Counts["warning"] != 1 || result.Counts["tool"] != 0 || result.Counts["fileChange"] != 0 {
			t.Fatalf("dropped completed-frame transcript = %#v, want warning-only incomplete prefix", result)
		}
	})
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
					engine.Event{Type: engine.EventReasoning, Text: tooLong},
					engine.Event{Type: engine.EventReasoning, Text: tooLong},
					engine.Event{Type: engine.EventToolResult, Text: tooLong},
					engine.Event{Type: engine.EventToolResult, Text: tooLong},
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
		{kind: transcriptItemToolResult},
		{kind: transcriptItemToolResult},
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

func TestItemSidecarWriterSyncsCreatedDirectories(t *testing.T) {
	t.Run("publishes sidecar and newly created ancestors once", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "artifacts", "logs", "items.jsonl")
		originalSync := syncItemSidecarDirectory
		t.Cleanup(func() { syncItemSidecarDirectory = originalSync })

		synced := make([]string, 0, 3)
		syncItemSidecarDirectory = func(dir string) error {
			if len(synced) == 0 {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("sidecar did not exist before its directory was synced: %v", err)
				}
			}
			synced = append(synced, dir)
			return nil
		}

		writer := newItemSidecarWriter(path, transcriptItemTextCap, transcriptItemFileCap)
		if writer.file == nil || writer.diagnostic != "" {
			t.Fatalf("new writer = %+v, want open sidecar without a diagnostic", writer)
		}
		want := []string{filepath.Dir(path), filepath.Dir(filepath.Dir(path)), root}
		if len(synced) != len(want) {
			t.Fatalf("synced directories = %v, want %v", synced, want)
		}
		for index, directory := range want {
			if synced[index] != directory {
				t.Fatalf("synced directory %d = %q, want %q", index, synced[index], directory)
			}
		}

		writer.append(transcriptItemTool, "tool", "output", false)
		writer.append(transcriptItemTool, "tool", "more output", false)
		writer.close()
		if writer.diagnostic != "" {
			t.Fatalf("writer diagnostic = %q, want no failure", writer.diagnostic)
		}
		if len(synced) != len(want) {
			t.Fatalf("append or close added directory syncs: got %v, want %v", synced, want)
		}
	})

	t.Run("directory sync failure withholds the receipt", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "artifacts", "logs", "items.jsonl")
		originalSync := syncItemSidecarDirectory
		t.Cleanup(func() { syncItemSidecarDirectory = originalSync })
		syncCalls := 0
		syncItemSidecarDirectory = func(string) error {
			syncCalls++
			return errors.New("injected directory sync failure")
		}

		writer := newItemSidecarWriter(path, transcriptItemTextCap, transcriptItemFileCap)
		if syncCalls != 1 || writer.file != nil || !strings.HasPrefix(writer.diagnostic, itemSidecarDiagnosticPrefix+"sync parent directory:") {
			t.Fatalf("writer after directory sync failure = %+v, sync calls = %d", writer, syncCalls)
		}
		writer.close()
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(contents, transcriptItemCompleteLine) {
			t.Fatalf("sidecar = %q, want no completion receipt after directory sync failure", contents)
		}
	})
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

func TestTranscriptItemsResultMessageTerminatesAgentTextRun(t *testing.T) {
	const initialResult = `{"wrong":true}`
	const correctedResult = `{"ok":true}`
	schemaRaw := json.RawMessage(`{"required":["ok"],"type":"object"}`)
	type expectedItem struct {
		kind transcriptItemKind
		name string
		text string
	}
	tests := []struct {
		name             string
		initialEvents    []engine.Event
		correctionEvents []engine.Event
		withSchema       bool
		want             []expectedItem
	}{
		{
			name: "matching agent text and result share one item",
			initialEvents: []engine.Event{
				{Type: engine.EventAgentText, Text: "tagged "},
				{Type: engine.EventAgentText, Text: "ok"},
				{Type: engine.EventResultMessage, Text: "tagged ok"},
			},
			want: []expectedItem{{kind: transcriptItemMessage, text: "tagged ok"}},
		},
		{
			name: "result without agent text becomes an item",
			initialEvents: []engine.Event{
				{Type: engine.EventResultMessage, Text: "result only"},
			},
			want: []expectedItem{{kind: transcriptItemMessage, text: "result only"}},
		},
		{
			name: "matching agent text, warning, and result produce one message and one warning",
			initialEvents: []engine.Event{
				{Type: engine.EventAgentText, Text: "done"},
				{Type: engine.EventWarning, Text: "notice"},
				{Type: engine.EventResultMessage, Text: "done"},
			},
			want: []expectedItem{
				{kind: transcriptItemMessage, text: "done"},
				{kind: transcriptItemWarning, text: "notice"},
			},
		},
		{
			name: "workspace-write tool text is suppressed",
			initialEvents: []engine.Event{
				{
					Type:                       engine.EventToolUse,
					Name:                       "fileChange",
					Text:                       "private path and contents",
					ObservedWorkspaceWriteItem: true,
				},
			},
			want: []expectedItem{{kind: transcriptItemFileChange, name: "fileChange"}},
		},
		{
			name: "different agent text and result remain separate across correction",
			initialEvents: []engine.Event{
				{Type: engine.EventAgentText, Text: "draft"},
				{Type: engine.EventResultMessage, Text: initialResult},
			},
			correctionEvents: []engine.Event{
				{Type: engine.EventToolUse, Name: "validate", Text: "retry"},
				{Type: engine.EventResultMessage, Text: correctedResult},
			},
			withSchema: true,
			want: []expectedItem{
				{kind: transcriptItemMessage, text: "draft"},
				{kind: transcriptItemMessage, text: initialResult},
				{kind: transcriptItemTool, name: "validate", text: "retry"},
				{kind: transcriptItemMessage, text: correctedResult},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			turns := 0
			backend := &executionFakeBackend{name: "items-result-message"}
			backend.start = func(_ context.Context, _ engine.SessionOpts) (engine.Session, error) {
				return &executionFakeSession{
					turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						turns++
						input.OnProcessStart(engine.ProcessRef{PID: 6103 + turns, PGID: 6103 + turns, StartTime: "items-result-message"}, 0)
						switch turns {
						case 1:
							return executionEvents(tt.initialEvents...), nil
						case 2:
							if !tt.withSchema {
								t.Fatalf("turn count = %d, want 1", turns)
							}
							return executionEvents(tt.correctionEvents...), nil
						default:
							t.Fatalf("turn count = %d, want at most 2", turns)
							return nil, nil
						}
					},
				}, nil
			}
			server := newExecutionServer(t, backend)
			var record jobstore.Record
			if tt.withSchema {
				record = queuedExecutionRecordWithSchema(t, server, backend.Name(), "correct", nil, schemaRaw)
			} else {
				record = queuedExecutionRecord(t, server, backend.Name(), "result message", nil)
			}
			runExecution(t, server, record)

			items := readTranscriptItems(t, record)
			if len(items) != len(tt.want) {
				t.Fatalf("item count = %d, want %d: %#v", len(items), len(tt.want), items)
			}
			for index, item := range items {
				want := tt.want[index]
				if item.Ordinal != index+1 || item.Kind != string(want.kind) || item.Name != want.name || item.Text != want.text {
					t.Fatalf("item %d = %+v, want ordinal=%d kind=%s name=%q text=%q", index, item, index+1, want.kind, want.name, want.text)
				}
			}
		})
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

func TestCanceledTranscriptReportsGapWhenSidecarSyncFailsAfterTerminal(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	record := transcriptTestRecord(t, server, "gap-sync-failure-after-cancel")
	itemPath := transcriptItemPath(t, record)
	writer := newItemSidecarWriter(itemPath, transcriptItemTextCap, transcriptItemFileCap)
	writer.append(transcriptItemTool, "lost", "captured before sync failed", false)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:   protocol.PublicStateCanceled,
		Cleanup: protocol.CleanupClean,
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.State != protocol.PublicStateCanceled || hasItemSidecarFailure(terminal.Diagnostics) {
		t.Fatalf("terminal record before deferred sync = %#v, want canceled without sidecar diagnostic", terminal)
	}

	if writer.file == nil {
		t.Fatal("sidecar writer has no file")
	}
	// A closed *os.File makes writer.close's direct Sync call fail predictably.
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}
	writer.close()
	if !strings.HasPrefix(writer.diagnostic, itemSidecarDiagnosticPrefix+"sync:") {
		t.Fatalf("sidecar diagnostics = %#v, want real sync failure", writer.diagnostics())
	}
	terminal, err = store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if hasItemSidecarFailure(terminal.Diagnostics) {
		t.Fatalf("terminal record after deferred sync = %#v, want immutable pre-sync diagnostics", terminal)
	}
	sidecar, err := os.ReadFile(itemPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sidecar, transcriptItemStopLine) || bytes.Contains(sidecar, transcriptItemCompleteLine) {
		t.Fatalf("sidecar = %q, want no terminal control record for the sync failure", sidecar)
	}

	result := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID})
	if result.State != protocol.PublicStateCanceled || result.ItemCount != 1 || !result.Gap {
		t.Fatalf("canceled transcript = %#v, want captured item with gap", result)
	}
}

func TestRestartedTranscriptReportsGapAfterUnpersistedSidecarFailure(t *testing.T) {
	root := t.TempDir()
	events := make(chan engine.Event, 1)
	events <- engine.Event{Type: engine.EventToolUse, Name: "lost", Text: "captured before the crash"}
	turnStarted := make(chan struct{})
	var closeEvents sync.Once
	closeStream := func() { closeEvents.Do(func() { close(events) }) }
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		closeStream()
	})

	backend := &executionFakeBackend{name: "items-restart-gap"}
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
	first := newTestServer(t, root, Config{Backends: []engine.Backend{backend}})
	record := queuedExecutionRecord(t, first, backend.Name(), "restart after sidecar failure", nil)
	itemPath := transcriptItemPath(t, record)
	if err := os.MkdirAll(filepath.Dir(itemPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(itemPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	run := newActiveExecution(record.JobID, backend)
	done := make(chan struct{})
	go func() {
		first.runJob(parent, record, run)
		close(done)
	}()
	select {
	case <-turnStarted:
	case <-time.After(time.Second):
		t.Fatal("turn did not start")
	}

	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for run.itemActivity().ItemCount != 1 {
		select {
		case <-deadline.C:
			t.Fatal("lost transcript item was not observed")
		case <-ticker.C:
		}
	}
	if !hasItemSidecarFailure(run.itemSidecarDiagnostics()) {
		t.Fatalf("active sidecar diagnostics = %#v, want production sidecar failure", run.itemSidecarDiagnostics())
	}

	store, err := first.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if !started.Starting || hasItemSidecarFailure(started.Diagnostics) {
		t.Fatalf("pre-crash record = %#v, want starting without a durable sidecar diagnostic", started)
	}

	// Closing the store without taking a terminal path models the abrupt daemon
	// exit between the in-memory failure and terminal persistence.
	first.closeJobStore()
	restarted := newTestServer(t, root, Config{})
	store, err = restarted.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.reconcileRecoveredJobs(store); err != nil {
		t.Fatal(err)
	}
	result := transcriptResultForTest(t, restarted, protocol.JobTranscriptParams{JobID: record.JobID})
	if result.State != protocol.PublicStateUnknown || result.ItemCount != 0 || !result.Gap {
		t.Fatalf("restarted transcript = %#v, want unknown empty transcript with gap", result)
	}

	if _, err := run.requestCancellation(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("interrupted pre-crash turn did not finish")
	}
}
