//go:build darwin || linux

package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestHelloMismatchPrecedesMethodDecoding(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Release: "0.18.0"})
	conn := &connection{server: server}
	hello := server.handle(conn, protocol.Request{JSONRPC: "2.0", Method: protocol.MethodHello, Params: json.RawMessage(`{"clientProtocolVersion":3,"clientRelease":"old-client","token":"test-token"}`)})
	if hello.err == nil || hello.err.Data.Code != protocol.ErrorVersionMismatch || hello.err.Data.ServerProtocolVersion != 4 || hello.err.Data.ClientProtocolVersion != 3 || hello.err.Data.ClientRelease != "old-client" || hello.err.Data.DaemonRelease != "0.18.0" || hello.err.Data.DaemonExecutable == "" {
		t.Fatalf("mismatch: %+v", hello.err)
	}
	request := protocol.Request{JSONRPC: "2.0", Method: protocol.MethodJobSubmit, Params: json.RawMessage(`{"invalid":true}`)}
	if out := server.handle(conn, request); out.err == nil || out.err.Data.Code != protocol.ErrorUnauthorized {
		t.Fatalf("before successful hello: %+v", out)
	}
	ok := server.handle(conn, protocol.Request{JSONRPC: "2.0", Method: protocol.MethodHello, Params: json.RawMessage(`{"clientProtocolVersion":4,"token":"test-token"}`)})
	if ok.err != nil {
		t.Fatalf("matching hello: %+v", ok.err)
	}
	if out := server.handle(conn, request); out.err == nil || out.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("invalid task after hello: %+v", out)
	}
}

func TestOwnerMismatchDoesNotChangeTokenOrRootMode(t *testing.T) {
	original := t.TempDir()
	server := newTestServer(t, original, Config{})
	server.closeJobStore()
	data, err := os.ReadFile(filepath.Join(original, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "jobs.db"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(root, "token")
	if err := os.WriteFile(token, []byte("keep-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{StateRoot: root}); err == nil || !strings.Contains(err.Error(), "ownership") {
		t.Fatalf("copied root accepted: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("refusal changed root mode: %v %v", info, err)
	}
	got, err := os.ReadFile(token)
	if err != nil || !bytes.Equal(got, []byte("keep-token")) {
		t.Fatalf("refusal changed token: %q %v", got, err)
	}
}

func TestProtocol4RejectsInvalidTaskWithoutBinding(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	cwd := t.TempDir()
	cases := []struct{ name, fields string }{
		{"missing intent", `"prompt":"p"`},
		{"empty intent", `"prompt":"p","intent":""`},
		{"unknown intent", `"prompt":"p","intent":"verify"`},
		{"null intent", `"prompt":"p","intent":null`},
		{"removed write", `"prompt":"p","intent":"edit","write":true`},
		{"removed verify", `"prompt":"p","intent":"inspect","verify":true`},
		{"removed localIPC", `"prompt":"p","intent":"edit","localIPC":true`},
		{"null timeout", `"prompt":"p","intent":"edit","timeoutMs":null`},
		{"fractional timeout", `"prompt":"p","intent":"edit","timeoutMs":1.5`},
		{"null model", `"prompt":"p","intent":"edit","model":null`},
		{"control in model", `"prompt":"p","intent":"edit","model":"a\nb"`},
		{"blank effort", `"prompt":"p","intent":"edit","effort":"  "`},
		{"null tag value", `"prompt":"p","intent":"edit","tags":{"a":null}`},
		{"wrong tag type", `"prompt":"p","intent":"edit","tags":{"a":3}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := json.RawMessage(fmt.Sprintf(`{"workspaceKey":"w","requestId":%q,"taskSpec":{"backend":"codex","cwd":%q,%s}}`, tc.name, cwd, tc.fields))
			// Identity tokens reject whitespace; use a stable valid request ID.
			raw = json.RawMessage(strings.ReplaceAll(string(raw), tc.name, strings.ReplaceAll(tc.name, " ", "-")))
			outcome := server.handleJobSubmit(raw)
			if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec {
				t.Fatalf("outcome = %#v", outcome)
			}
		})
	}
	invalidUTF8 := []byte(fmt.Sprintf(`{"workspaceKey":"w","requestId":"utf8","taskSpec":{"backend":"codex","cwd":%q,"intent":"inspect","prompt":"`, cwd))
	invalidUTF8 = append(invalidUTF8, 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}}`)...)
	if outcome := server.handleJobSubmit(invalidUTF8); outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec {
		t.Fatalf("invalid UTF-8 outcome = %#v", outcome)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.List()
	if err != nil || len(records) != 0 {
		t.Fatalf("invalid submissions created records: %+v %v", records, err)
	}
}

func TestRecoveryRejectsMismatchedOrUnreadableScope(t *testing.T) {
	cases := []struct {
		name       string
		change     func(*jobstore.ProcessScope)
		unreadable bool
	}{
		{"boot", func(s *jobstore.ProcessScope) { s.BootID = "other-boot" }, false},
		{"device", func(s *jobstore.ProcessScope) { s.RootDevice = "3" }, false},
		{"inode", func(s *jobstore.ProcessScope) { s.RootInode = "3" }, false},
		{"missing boot", func(s *jobstore.ProcessScope) { s.BootID = "" }, false},
		{"invalid inode", func(s *jobstore.ProcessScope) { s.RootInode = "not-decimal" }, false},
		{"unreadable", nil, true},
	}
	if runtime.GOOS == "linux" {
		cases = append(cases, struct {
			name       string
			change     func(*jobstore.ProcessScope)
			unreadable bool
		}{"namespace", func(s *jobstore.ProcessScope) { s.PIDNamespace = "pid:[99]" }, false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newOrphanReaperFixture()
			fixture.groupLive = false
			server := &Server{processTable: fixture, processGroups: fixture, processGroupGoneFn: fixture.processGroupGone}
			scope := testProcessScope()
			server.processScopeFn = func() (jobstore.ProcessScope, error) {
				if tc.unreadable {
					return jobstore.ProcessScope{}, errors.New("scope unavailable")
				}
				return scope, nil
			}
			claim := fixture.claim()
			if tc.change != nil {
				tc.change(&claim.Scope)
			}
			cleanup, diagnostics := server.terminateRecordedProcessClaim(&claim)
			if cleanup != protocol.CleanupUncertain || len(diagnostics) == 0 || len(fixture.signals) != 0 {
				t.Fatalf("cleanup=%s diagnostics=%v signals=%v", cleanup, diagnostics, fixture.signals)
			}
		})
	}
}

func TestInvocationSelectionPrecedenceAndReplay(t *testing.T) {
	var selected engine.SessionOpts
	backend := &executionFakeBackend{name: "selection"}
	backend.start = func(_ context.Context, opts engine.SessionOpts) (engine.Session, error) {
		selected = opts
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			input.OnProcessStart(engine.ProcessRef{PID: 5252, PGID: 5252, StartTime: "selection-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: "done"}), nil
		}}, nil
	}
	server := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{backend}, BackendDefaults: map[string]BackendDefaults{"selection": {Model: "default-model", Effort: "default-effort"}}})
	requested := "task-model"
	params := submissionParams("w", "selection", backend.Name(), t.TempDir(), "original prompt")
	params.TaskSpec.Model = &requested
	submitted := submitResultForTest(t, submitForTest(t, server, params))
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.Get(submitted.JobID)
	if err != nil || queued.Selection != nil {
		t.Fatalf("queued selection: %+v %v", queued, err)
	}
	runExecution(t, server, queued)
	stored, err := store.Get(submitted.JobID)
	if err != nil || stored.Selection == nil || stored.Selection.Model != requested || stored.Selection.Effort != "default-effort" || selected.Model != requested || selected.Effort != "default-effort" {
		t.Fatalf("selection=%+v options=%+v err=%v", stored.Selection, selected, err)
	}
	server.backendDefaults[backend.Name()] = BackendDefaults{Model: "changed", Effort: "changed"}
	replay := submitResultForTest(t, submitForTest(t, server, params))
	if !replay.Deduplicated || replay.JobID != submitted.JobID {
		t.Fatalf("replay: %+v", replay)
	}
	replayed, err := store.Get(replay.JobID)
	if err != nil || replayed.Selection == nil || *replayed.Selection != *stored.Selection {
		t.Fatalf("replayed selection: %+v %v", replayed.Selection, err)
	}
}

func TestScopeIsRecheckedBeforeEscalation(t *testing.T) {
	fixture := newOrphanReaperFixture()
	checks := 0
	server := &Server{processTable: fixture, processGroups: fixture, processGroupGrace: time.Nanosecond}
	server.processGroupGoneFn = func(int) (bool, error) { return false, nil }
	server.processScopeFn = func() (jobstore.ProcessScope, error) {
		checks++
		scope := testProcessScope()
		if checks >= 3 {
			scope.BootID = "changed-boot"
		}
		return scope, nil
	}
	claim := fixture.claim()
	cleanup, diagnostics := server.terminateRecordedProcessClaim(&claim)
	if cleanup != protocol.CleanupUncertain || len(diagnostics) == 0 || len(fixture.signals) != 1 || fixture.signals[0].signal != syscall.SIGTERM {
		t.Fatalf("cleanup=%s diagnostics=%v signals=%v", cleanup, diagnostics, fixture.signals)
	}
}

func TestUncertainInitialCleanupForbidsCorrection(t *testing.T) {
	turns := 0
	backend := &executionFakeBackend{name: "uncertain-correction"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) {
		return &executionFakeSession{turn: func(_ context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
			turns++
			input.OnProcessStart(engine.ProcessRef{PID: 5353, PGID: 5353, StartTime: "initial-token"}, 0)
			return executionEvents(engine.Event{Type: engine.EventResultMessage, Text: `{"bad":true}`}, engine.Event{Type: engine.EventTurnFinal, TurnFinal: &engine.TurnFinalObservation{CleanupFailed: true}}), nil
		}}, nil
	}
	server := newExecutionServer(t, backend)
	record := queuedExecutionRecordWithSchema(t, server, backend.Name(), "task", nil, json.RawMessage(`{"required":["ok"]}`))
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil || turns != 1 || got.State != protocol.PublicStateCompleted || got.Cleanup != protocol.CleanupUncertain || got.ResultText != `{"bad":true}` || got.Contract.Attempts != 1 || got.Contract.Compliant || !strings.Contains(strings.Join(got.Diagnostics, " "), "schema correction unavailable") {
		t.Fatalf("turns=%d record=%+v err=%v", turns, got, err)
	}
}

func TestUnreadableScopePreventsLaunch(t *testing.T) {
	starts := 0
	backend := &executionFakeBackend{name: "scope-unavailable"}
	backend.start = func(context.Context, engine.SessionOpts) (engine.Session, error) { starts++; return nil, nil }
	server := newExecutionServer(t, backend)
	server.processScopeFn = func() (jobstore.ProcessScope, error) { return jobstore.ProcessScope{}, errors.New("unreadable scope") }
	record := queuedExecutionRecord(t, server, backend.Name(), "task", nil)
	runExecution(t, server, record)
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(record.JobID)
	if err != nil || starts != 0 || got.State != protocol.PublicStateFailed || got.Selection != nil || got.Starting || !strings.Contains(got.FailureReason, "scope") {
		t.Fatalf("starts=%d record=%+v err=%v", starts, got, err)
	}
}
