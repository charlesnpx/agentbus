//go:build darwin || linux

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

type executionFakeBackend struct {
	name  string
	start func(context.Context, engine.SessionOpts) (engine.Session, error)
}

func (backend *executionFakeBackend) Name() string { return backend.name }

func (backend *executionFakeBackend) Preflight(context.Context) (engine.Health, error) {
	return engine.Health{Backend: backend.name}, nil
}

func (backend *executionFakeBackend) Start(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
	if backend.start == nil {
		return nil, errors.New("fake backend Start was not configured")
	}
	return backend.start(ctx, opts)
}

func (backend *executionFakeBackend) Resume(context.Context, string, engine.SessionOpts) (engine.Session, error) {
	return nil, errors.New("fake backend Resume was not configured")
}

type executionFakeSession struct {
	turn      func(context.Context, engine.TurnInput) (<-chan engine.Event, error)
	interrupt func(context.Context) error
}

func (*executionFakeSession) ID() string { return "execution-fake-session" }

func (session *executionFakeSession) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	if session.turn == nil {
		return nil, errors.New("fake session Turn was not configured")
	}
	return session.turn(ctx, input)
}

func (session *executionFakeSession) Interrupt(ctx context.Context) error {
	if session.interrupt == nil {
		return nil
	}
	return session.interrupt(ctx)
}

func executionEvents(events ...engine.Event) <-chan engine.Event {
	stream := make(chan engine.Event, len(events))
	for _, event := range events {
		stream <- event
	}
	close(stream)
	return stream
}

func newExecutionServer(t *testing.T, backend engine.Backend) *Server {
	t.Helper()
	return newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}})
}

func queuedExecutionRecord(t *testing.T, server *Server, backend, prompt string, timeoutMS *int64) jobstore.Record {
	t.Helper()
	spec := protocol.TaskSpecV3{
		Backend:   backend,
		CWD:       t.TempDir(),
		Prompt:    prompt,
		Write:     false,
		TimeoutMS: timeoutMS,
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: "execution-workspace", RequestID: "execution-request"},
		raw,
		func(id string) (jobstore.Record, error) {
			return jobstore.Record{JobID: id, Backend: backend, CWD: spec.CWD, Write: spec.Write}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("new execution record was unexpectedly deduplicated")
	}
	return record
}

func runExecution(t *testing.T, server *Server, record jobstore.Record) {
	t.Helper()
	run := newActiveExecution(record.JobID)
	server.runJob(context.Background(), record, run)
	select {
	case <-run.done:
	case <-time.After(time.Second):
		t.Fatal("execution did not finish")
	}
}

func claimedResultSession(result string) *executionFakeSession {
	return &executionFakeSession{
		turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 4101, PGID: 4101, StartTime: "process-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: result}), nil
		},
	}
}

func TestExecutionNormalCompletionRecordsAuthoritativeText(t *testing.T) {
	backend := &executionFakeBackend{name: "fake"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4100, PGID: 4100, StartTime: "completion-token"}, 0)
				return executionEvents(
					engine.Event{Type: engine.EventAgentText, Text: "non-authoritative"},
					engine.Event{Type: engine.EventResultMessage, Text: "authoritative final text"},
				), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "finish", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != "authoritative final text" {
		t.Fatalf("terminal record = %+v, want completed authoritative result", got)
	}
	spilled, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	if string(spilled) != got.ResultText {
		t.Fatalf("spilled result = %q, want %q", spilled, got.ResultText)
	}
}

func TestExecutionCommitsStartingBeforeBackendStart(t *testing.T) {
	var server *Server
	backend := &executionFakeBackend{name: "ordered"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		store, err := server.ensureJobStore()
		if err != nil {
			t.Fatal(err)
		}
		records, err := store.List()
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || !records[0].Starting || records[0].State != protocol.PublicStateRunning {
			t.Fatalf("record at Backend.Start = %+v, want committed private starting", records)
		}
		return claimedResultSession("ordered"), nil
	}
	server = newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "ordered", nil)
	runExecution(t, server, record)
}

func TestExecutionRecordsClaimInSeparateTransaction(t *testing.T) {
	var server *Server
	backend := &executionFakeBackend{name: "claim"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				store, err := server.ensureJobStore()
				if err != nil {
					t.Fatal(err)
				}
				before, err := store.List()
				if err != nil {
					t.Fatal(err)
				}
				if len(before) != 1 || !before[0].Starting || before[0].ProcessClaim != nil {
					t.Fatalf("record before process claim = %+v, want separate committed starting record", before)
				}
				input.OnProcessStart(engine.ProcessRef{PID: 4102, PGID: 4102, StartTime: "claim-token"}, 0)
				after, err := store.List()
				if err != nil {
					t.Fatal(err)
				}
				if len(after) != 1 || after[0].Starting || after[0].ProcessClaim == nil || after[0].ProcessClaim.StartToken != "claim-token" {
					t.Fatalf("record after process claim = %+v, want separately committed claim", after)
				}
				return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: "claimed"}), nil
			},
		}, nil
	}
	server = newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "claim", nil)
	runExecution(t, server, record)
}

func TestExecutionTimeoutFailsAndInterruptsProcessGroup(t *testing.T) {
	timeoutMS := int64(25)
	stream := make(chan engine.Event)
	var closeOnce sync.Once
	var actionsMu sync.Mutex
	var actions []string
	backend := &executionFakeBackend{name: "timeout"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4103, PGID: 4103, StartTime: "timeout-token"}, 0)
				return stream, nil
			},
			interrupt: func(context.Context) error {
				actionsMu.Lock()
				actions = append(actions, "TERM", "KILL")
				actionsMu.Unlock()
				closeOnce.Do(func() { close(stream) })
				return nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "timeout", &timeoutMS)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassTimeout {
		t.Fatalf("timeout terminal = %+v, want failed timeout", got)
	}
	actionsMu.Lock()
	defer actionsMu.Unlock()
	if strings.Join(actions, ",") != "TERM,KILL" {
		t.Fatalf("cleanup actions = %v, want TERM then KILL", actions)
	}
}

func TestExecutionCancellationRecordsCleanOrUncertainCleanup(t *testing.T) {
	for _, test := range []struct {
		name         string
		interruptErr error
		wantCleanup  protocol.Cleanup
	}{
		{name: "clean", wantCleanup: protocol.CleanupClean},
		{name: "uncertain", interruptErr: errors.New("process group did not settle"), wantCleanup: protocol.CleanupUncertain},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := make(chan engine.Event)
			var closeOnce sync.Once
			turnStarted := make(chan struct{})
			backend := &executionFakeBackend{name: "cancel-" + test.name}
			backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
				return &executionFakeSession{
					turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
						input.OnProcessStart(engine.ProcessRef{PID: 4104, PGID: 4104, StartTime: "cancel-token-" + test.name}, 0)
						close(turnStarted)
						return stream, nil
					},
					interrupt: func(context.Context) error {
						closeOnce.Do(func() { close(stream) })
						return test.interruptErr
					},
				}, nil
			}
			server := newExecutionServer(t, backend)
			server.beginExecutions(context.Background())
			defer server.stopExecutions()
			record := queuedExecutionRecord(t, server, backend.Name(), "cancel", nil)
			server.enqueueQueuedJob(record)
			select {
			case <-turnStarted:
			case <-time.After(time.Second):
				t.Fatal("turn did not start")
			}
			got, err := server.cancelJob(context.Background(), record.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != protocol.PublicStateCanceled || got.Cleanup != test.wantCleanup {
				t.Fatalf("canceled record = %+v, want canceled cleanup=%q", got, test.wantCleanup)
			}
		})
	}
}

func TestExecutionSpillsLargeTerminalResultWithDigestAndElision(t *testing.T) {
	text := strings.Repeat("x", engine.DefaultInlineResultCap+1)
	backend := &executionFakeBackend{name: "spill"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return claimedResultSession(text), nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "spill", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	spilled, err := os.ReadFile(got.Artifacts.Result)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(text))
	if got.State != protocol.PublicStateCompleted || got.ResultText != "" || len(spilled) != len(text) || hex.EncodeToString(sum[:]) != hex.EncodeToString(sha256Sum(spilled)) {
		t.Fatalf("spilled terminal = state=%s inline=%d bytes=%d, want complete elided matching result", got.State, len(got.ResultText), len(spilled))
	}
}

func sha256Sum(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func TestExecutionProviderOverloadedUsesTypedFailureClass(t *testing.T) {
	backend := &executionFakeBackend{name: "overloaded"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{
			turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
				input.OnProcessStart(engine.ProcessRef{PID: 4105, PGID: 4105, StartTime: "overload-token"}, 0)
				return executionEvents(engine.Event{Type: engine.EventTerminalError, Text: "provider capacity text must not control classification", Err: engine.ErrProviderOverloaded}), nil
			},
		}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "overloaded", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateFailed || got.FailureClass != protocol.FailureClassProviderOverloaded {
		t.Fatalf("overload terminal = %+v, want provider_overloaded", got)
	}
}

func TestExecutionSecondTerminalWriteDoesNotOverwriteFirst(t *testing.T) {
	backend := &executionFakeBackend{name: "first-terminal"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return claimedResultSession("first result"), nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecord(t, server, backend.Name(), "first", nil)
	runExecution(t, server, record)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:         protocol.PublicStateFailed,
		Cleanup:       protocol.CleanupUncertain,
		FailureClass:  protocol.FailureClassInternal,
		FailureReason: "later result",
	})
	if !errors.Is(err, jobstore.ErrTerminal) {
		t.Fatalf("second terminal write error = %v, want ErrTerminal", err)
	}
	got, err := store.Get(record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.PublicStateCompleted || got.ResultText != "first result" {
		t.Fatalf("record after second terminal write = %+v, want first completed result", got)
	}
}

func TestExecutionServiceHasNoEngineExecutionImport(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(wd)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(wd, entry.Name())
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			path, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(path, "/engine/execution") {
				t.Fatalf("%s imports forbidden %q", entry.Name(), path)
			}
		}
	}
}
