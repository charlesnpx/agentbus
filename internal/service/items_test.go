//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/engine/command"
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

type runnerBackedCodexSession struct {
	session engine.Session
	runner  command.Runner
}

func (session *runnerBackedCodexSession) ID() string {
	return session.session.ID()
}

func (session *runnerBackedCodexSession) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	turner, ok := session.session.(interface {
		TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
	})
	if !ok {
		return nil, errors.New("Codex session does not support injected command runners")
	}
	return turner.TurnWithRunner(ctx, input, session.runner)
}

func (session *runnerBackedCodexSession) Interrupt(ctx context.Context) error {
	return session.session.Interrupt(ctx)
}

func scriptedCodexBackend(t *testing.T, runner command.Runner) *executionFakeBackend {
	t.Helper()
	backend := &executionFakeBackend{name: "scripted-codex"}
	backend.start = func(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
		session, err := codexcli.New(codexcli.Options{Binary: "fake-codex"}).Start(ctx, opts)
		if err != nil {
			return nil, err
		}
		return &runnerBackedCodexSession{session: session, runner: runner}, nil
	}
	return backend
}

type scriptedCodexRunner struct {
	frames []map[string]any

	mu        sync.Mutex
	started   bool
	scriptErr error
}

func newScriptedCodexRunner(frames ...map[string]any) *scriptedCodexRunner {
	return &scriptedCodexRunner{frames: append([]map[string]any(nil), frames...)}
}

func (runner *scriptedCodexRunner) Start(context.Context, command.ExecSpec) (command.RunningCommand, error) {
	runner.mu.Lock()
	if runner.started {
		runner.mu.Unlock()
		return nil, errors.New("scripted Codex runner started more than once")
	}
	runner.started = true
	runner.mu.Unlock()

	process := newScriptedCodexProcess()
	go runner.serve(process)
	return process, nil
}

func (runner *scriptedCodexRunner) serve(process *scriptedCodexProcess) {
	var scriptErr error
	defer func() {
		_ = process.stdinR.Close()
		_ = process.stdoutW.Close()
		_ = process.stderrW.Close()
		runner.mu.Lock()
		runner.scriptErr = scriptErr
		runner.mu.Unlock()
		process.finish(scriptErr)
	}()

	decoder := json.NewDecoder(process.stdinR)
	for {
		var request map[string]any
		if err := decoder.Decode(&request); err != nil {
			scriptErr = fmt.Errorf("read Codex request: %w", err)
			return
		}
		method, _ := request["method"].(string)
		switch method {
		case "initialize":
			scriptErr = process.writeJSON(map[string]any{"id": request["id"], "result": map[string]any{}})
		case "initialized":
			continue
		case "thread/start":
			scriptErr = process.writeJSON(map[string]any{"id": request["id"], "result": map[string]any{"thread": map[string]any{"id": "thread-1"}}})
		case "turn/start":
			scriptErr = process.writeJSON(map[string]any{"id": request["id"], "result": map[string]any{"turn": map[string]any{"id": "turn-1"}}})
			if scriptErr == nil {
				for _, frame := range runner.frames {
					if scriptErr = process.writeJSON(frame); scriptErr != nil {
						break
					}
				}
			}
			return
		default:
			scriptErr = fmt.Errorf("unexpected Codex request method %q", method)
		}
		if scriptErr != nil {
			return
		}
	}
}

func (runner *scriptedCodexRunner) assert(t *testing.T) {
	t.Helper()
	runner.mu.Lock()
	started := runner.started
	scriptErr := runner.scriptErr
	runner.mu.Unlock()
	if !started {
		t.Fatal("scripted Codex runner was not started")
	}
	if scriptErr != nil {
		t.Fatal(scriptErr)
	}
}

type scriptedCodexProcess struct {
	stdinR  *io.PipeReader
	stdinW  *io.PipeWriter
	stdoutR *io.PipeReader
	stdoutW *io.PipeWriter
	stderrR *io.PipeReader
	stderrW *io.PipeWriter

	done chan struct{}
	once sync.Once
	err  error
}

func newScriptedCodexProcess() *scriptedCodexProcess {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &scriptedCodexProcess{
		stdinR:  stdinR,
		stdinW:  stdinW,
		stdoutR: stdoutR,
		stdoutW: stdoutW,
		stderrR: stderrR,
		stderrW: stderrW,
		done:    make(chan struct{}),
	}
}

func (process *scriptedCodexProcess) Stdin() io.WriteCloser { return process.stdinW }

func (process *scriptedCodexProcess) Stdout() io.ReadCloser { return process.stdoutR }

func (process *scriptedCodexProcess) Stderr() io.ReadCloser { return process.stderrR }

func (process *scriptedCodexProcess) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-process.done:
		return command.ExitObservation{Exited: true, Code: 0}, process.err
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (process *scriptedCodexProcess) Interrupt(context.Context) error { return nil }

func (process *scriptedCodexProcess) ProcessRef() (engine.ProcessRef, int) {
	return engine.ProcessRef{PID: 6104, PGID: 6104, StartTime: "scripted-codex"}, 0
}

func (process *scriptedCodexProcess) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	_, err = process.stdoutW.Write(payload)
	return err
}

func (process *scriptedCodexProcess) finish(err error) {
	process.once.Do(func() {
		process.err = err
		close(process.done)
	})
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

func TestTranscriptItemsCodexWarningAndMatchingResult(t *testing.T) {
	runner := newScriptedCodexRunner(
		map[string]any{
			"method": "item/agentMessage/delta",
			"params": map[string]any{
				"threadId": "thread-1",
				"turnId":   "turn-1",
				"itemId":   "message-1",
				"delta":    "done",
			},
		},
		map[string]any{
			"method": "warning",
			"params": map[string]any{
				"message": "notice",
			},
		},
		map[string]any{
			"method": "turn/completed",
			"params": map[string]any{
				"threadId": "thread-1",
				"turn": map[string]any{
					"id":     "turn-1",
					"items":  []any{},
					"status": "completed",
				},
			},
		},
	)
	backend := scriptedCodexBackend(t, runner)
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "warning sequence", nil)
	runExecution(t, server, record)
	runner.assert(t)

	items := readTranscriptItems(t, record)
	if len(items) != 2 {
		t.Fatalf("item count = %d, want message and one warning: %#v", len(items), items)
	}
	if got := items[0]; got.Ordinal != 1 || got.Kind != string(transcriptItemMessage) || got.Text != "done" || got.Truncated {
		t.Fatalf("item 1 = %+v, want untruncated message done", got)
	}
	if got := items[1]; got.Ordinal != 2 || got.Kind != string(transcriptItemWarning) || got.Text != "notice" || got.Truncated {
		t.Fatalf("item 2 = %+v, want one untruncated warning", got)
	}
}

func TestTranscriptItemsCodexStartedFileChangeSuppressesText(t *testing.T) {
	const sensitiveChange = "diff --git a/private/secret.txt b/private/secret.txt\n+do-not-persist-this"
	runner := newScriptedCodexRunner(
		map[string]any{
			"method": "item/started",
			"params": map[string]any{
				"threadId": "thread-1",
				"turnId":   "turn-1",
				"item": map[string]any{
					"id":      "change-started",
					"type":    "fileChange",
					"path":    "private/secret.txt",
					"changes": sensitiveChange,
				},
			},
		},
		map[string]any{
			"method": "turn/completed",
			"params": map[string]any{
				"threadId": "thread-1",
				"turn": map[string]any{
					"id":     "turn-1",
					"items":  []any{},
					"status": "completed",
				},
			},
		},
	)
	backend := scriptedCodexBackend(t, runner)
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "started file change", nil)
	runExecution(t, server, record)
	runner.assert(t)

	items := readTranscriptItems(t, record)
	var fileChanges []TranscriptItem
	for _, item := range items {
		if item.Kind == string(transcriptItemFileChange) {
			fileChanges = append(fileChanges, item)
		}
	}
	if len(fileChanges) != 1 {
		t.Fatalf("file-change item count = %d, want 1: %#v", len(fileChanges), items)
	}
	if got := fileChanges[0]; got.Ordinal != 1 || got.Text != "" || got.Truncated {
		t.Fatalf("file-change item = %+v, want text-free untruncated fileChange", got)
	}
	sidecar, err := os.ReadFile(transcriptItemPath(t, record))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sidecar, []byte(sensitiveChange)) || bytes.Contains(sidecar, []byte("private/secret.txt")) {
		t.Fatalf("sidecar leaked started file-change data: %s", sidecar)
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
