package served

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestTimedOutAdmissionRecoversPartialResultFromTranscript(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-timeout")
	writePartialTranscript(t, paths.Stdout, strings.Join([]string{
		`{"method":"item/completed","params":{"item":{"type":"reasoning","text":"ignore this reasoning"}}}`,
		`{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"first recovered message"}}}`,
		`{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"second recovered message"}}}`,
	}, "\n")+"\n")

	if err := server.completeAdmissionRun(run, engine.StateTimedOut, "", nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Result == nil || !record.Result.Result.Partial || record.Result.Result.PartialReason != model.PartialResultReasonTimeout {
		t.Fatalf("result record = %+v, want timeout partial result", record.Result)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeTimedOut || record.Terminal.Result == nil || !record.Terminal.Result.Partial || record.Terminal.Result.PartialReason != model.PartialResultReasonTimeout {
		t.Fatalf("terminal record = %+v, want timed out terminal with partial result", record.Terminal)
	}
	raw, err := os.ReadFile(record.Result.Result.Path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.HasPrefix(text, partialResultTimeoutHeader) {
		t.Fatalf("partial artifact header = %q, want prefix %q", text, partialResultTimeoutHeader)
	}
	first := strings.Index(text, "first recovered message")
	second := strings.Index(text, "second recovered message")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("partial artifact = %q, want recovered messages in order", text)
	}
	if strings.Contains(text, "ignore this reasoning") {
		t.Fatalf("partial artifact = %q, unexpectedly contains reasoning", text)
	}

	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: accepted.Record.JobID.String()})
	if result.Result == nil || !result.Result.Partial || result.Result.PartialReason != model.PartialResultReasonTimeout {
		t.Fatalf("job.result = %+v, want timeout partial marker", result)
	}
}

func TestInterruptedAdmissionRecoversPartialResultFromTranscript(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-interrupted")
	writePartialTranscript(t, paths.Stdout, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"interrupted recovered message"}}}`+"\n")

	if err := server.completeAdmissionRun(run, engine.StateInterrupted, "", nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Result == nil || !record.Result.Result.Partial || record.Result.Result.PartialReason != model.PartialResultReasonInterrupted {
		t.Fatalf("result record = %+v, want interrupted partial result", record.Result)
	}
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeInterrupted || record.Terminal.Result == nil || !record.Terminal.Result.Partial || record.Terminal.Result.PartialReason != model.PartialResultReasonInterrupted {
		t.Fatalf("terminal record = %+v, want interrupted terminal with partial result", record.Terminal)
	}
	raw, err := os.ReadFile(record.Result.Result.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(raw), partialResultInterruptedHeader) {
		t.Fatalf("partial artifact header = %q, want prefix %q", raw, partialResultInterruptedHeader)
	}
}

func TestPartialResultArtifactSkipsTruncatedFinalTranscriptLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "truncated.stdout.log")
	writePartialTranscript(t, path, "{\"method\":\"item/completed\",\"params\":{\"item\":{\"type\":\"agentMessage\",\"text\":\"recovered before truncation\"}}}\n{\"method\":\"item/completed\",\"params\":{\"item\":")

	artifact, recovered, err := partialResultArtifact(path, engine.StateTimedOut)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered || !strings.Contains(string(artifact), "recovered before truncation") {
		t.Fatalf("partial artifact = %q recovered=%t, want prior complete message", artifact, recovered)
	}
}

func TestPartialResultArtifactKeepsBoundedTailAndMarksElision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.stdout.log")
	earlier := strings.Repeat("x", partialResultArtifactMaxBytes)
	const latest = "latest recovered message"
	writePartialTranscript(t, path, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"`+earlier+`"}}}`+"\n"+`{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"`+latest+`"}}}`+"\n")

	artifact, recovered, err := partialResultArtifact(path, engine.StateTimedOut)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Fatal("partial result was not recovered")
	}
	if len(artifact) > partialResultArtifactMaxBytes {
		t.Fatalf("partial artifact bytes = %d, want at most %d", len(artifact), partialResultArtifactMaxBytes)
	}
	if !strings.Contains(string(artifact), partialResultElisionNotice) {
		t.Fatal("partial artifact did not mark elided content")
	}
	if !strings.HasSuffix(string(artifact), latest) {
		t.Fatalf("partial artifact does not retain latest message suffix %q", latest)
	}
}

func TestTimedOutAdmissionWithNoAssistantTextLeavesResultAbsent(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-empty")
	writePartialTranscript(t, paths.Stdout, strings.Join([]string{
		`{"method":"item/completed","params":{"item":{"type":"reasoning","text":"reasoning is not a report"}}}`,
		`{"method":"item/completed","params":{"item":{"type":"commandExecution","text":"tool output is not a report"}}}`,
	}, "\n")+"\n")

	if err := server.completeAdmissionRun(run, engine.StateTimedOut, "", nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Result != nil || record.Terminal == nil || record.Terminal.Result != nil {
		t.Fatalf("terminal record = %+v, want no result artifact", record)
	}
	layout, err := authorityResultLayout(server.stateRoot, record)
	if err != nil {
		t.Fatal(err)
	}
	path, err := engine.ResultPathForLayout(layout, accepted.Record.JobID.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result artifact stat = %v, want absent", err)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: accepted.Record.JobID.String()})
	if result.Result != nil {
		t.Fatalf("job.result = %+v, want no result", result)
	}
}

func TestTimedOutAdmissionPartialResultSynthesisFailureDoesNotFailTerminalization(t *testing.T) {
	server, accepted, run, _ := partialResultAdmissionRun(t, "partial-result-synthesis-failure")
	run.logPaths.Stdout = filepath.Join(t.TempDir(), "missing.stdout.log")

	if err := server.completeAdmissionRun(run, engine.StateTimedOut, "", nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeTimedOut {
		t.Fatalf("terminal record = %+v, want timed out terminal", record.Terminal)
	}
	if record.Result != nil || record.Terminal.Result != nil {
		t.Fatalf("terminal record = %+v, want no partial result after synthesis failure", record)
	}
}

func TestTimedOutAdmissionPartialResultReceiptCommitFailureDoesNotFailTerminalization(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-receipt-commit-failure")
	writePartialTranscript(t, paths.Stdout, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"recoverable transcript excerpt"}}}`+"\n")
	recorder := installRecordingAdmissionAuthorityForTest(t, server)
	failed := false
	recorder.beforeFinalize = func(_ context.Context, _ model.JobID, _ model.AttemptRef, intent model.TerminalIntent) error {
		if intent.PartialResult == nil || failed {
			return nil
		}
		failed = true
		return errors.New("injected partial result receipt commit failure")
	}

	if err := server.completeAdmissionRun(run, engine.StateTimedOut, "", nil); err != nil {
		t.Fatal(err)
	}
	if !failed {
		t.Fatal("partial result receipt commit failure was not injected")
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeTimedOut {
		t.Fatalf("terminal record = %+v, want timed out terminal", record.Terminal)
	}
	if record.Result != nil || record.Terminal.Result != nil {
		t.Fatalf("terminal record = %+v, want unchanged terminal path without partial result", record)
	}
	assertNoPartialResultArtifacts(t, server, accepted.Record)
}

func TestTimedOutAdmissionPartialResultDoesNotReplaceCompetingCompletedResult(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-competing-completed")
	writePartialTranscript(t, paths.Stdout, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"transcript excerpt must not replace final report"}}}`+"\n")
	const finalReport = "competing authoritative completed report"
	setPartialResultBeforePublishHookForTest(t, func() error {
		return server.admissionCoordinator.CompleteWithObservedWorkspaceWriteItemCount(
			context.Background(),
			accepted.Record.JobID,
			model.OutcomeCompleted,
			[]byte(finalReport),
			nil,
			0,
			0,
			nil,
		)
	})

	if err := server.completeAdmissionRun(run, engine.StateTimedOut, "", nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Terminal == nil || record.Terminal.Outcome != model.OutcomeCompleted || record.Result == nil || record.Result.Result.Partial {
		t.Fatalf("terminal record = %+v, want competing completed result", record)
	}
	path, err := engine.ResultPathForLayout(mustAuthorityResultLayout(t, server, accepted.Record), accepted.Record.JobID.String())
	if err != nil {
		t.Fatal(err)
	}
	if record.Result.Result.Path != path {
		t.Fatalf("completed result path = %q, want %q", record.Result.Result.Path, path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != finalReport {
		t.Fatalf("completed artifact = %q, want %q", raw, finalReport)
	}
	assertNoPartialResultArtifacts(t, server, accepted.Record)
}

func TestCompletedAdmissionResultIsNotReplacedByTranscriptRecovery(t *testing.T) {
	server, accepted, run, paths := partialResultAdmissionRun(t, "partial-result-completed")
	writePartialTranscript(t, paths.Stdout, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"transcript text must not replace the final report"}}}`+"\n")
	const finalReport = "authoritative completed report"

	if err := server.completeAdmissionRun(run, engine.StateCompleted, finalReport, nil); err != nil {
		t.Fatal(err)
	}
	record := loadAdmissionSafetyRecord(t, server, accepted.Record.JobID.String())
	if record.Result == nil || record.Result.Result.Partial || record.Result.Result.PartialReason != "" {
		t.Fatalf("result record = %+v, want ordinary completed result", record.Result)
	}
	raw, err := os.ReadFile(record.Result.Result.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != finalReport {
		t.Fatalf("completed artifact = %q, want %q", raw, finalReport)
	}
	result := jobResultViaHandler(t, server, protocol.JobResultParams{JobID: accepted.Record.JobID.String()})
	if result.State != engine.StateCompleted || result.Result == nil || result.Result.Partial || result.Result.PartialReason != "" || result.Result.Text != finalReport {
		t.Fatalf("job.result = %+v, want ordinary completed result", result)
	}
}

func partialResultAdmissionRun(t *testing.T, requestID string) (*Server, authority.AcceptResult, jobRun, engine.LogPaths) {
	t.Helper()
	server, _, cwd := newUnstartedTestServer(t, newFakeBackend("fake"))
	launcher := newAdmissionFakeLaunchCustodian(t)
	enableTestAdmission(t, server, launcher)
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := server.admissionReady.Accept(context.Background(), authority.AcceptRequest{
		RequestKey: model.RequestKey{
			WorkspaceKey: model.WorkspaceKey("workspace/" + requestID),
			RequestID:    model.RequestID(requestID),
		},
		WorkspaceLayoutKey: model.WorkspaceKey(engine.WorkspaceKey(canonicalCWD)),
		TaskIdentity:       model.NewSHA256TaskIdentity([]byte(requestID)),
		Mode:               model.ModeIdentifiedFenced,
		SessionID:          "session-" + requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID := accepted.Record.JobID
	ref := accepted.Record.Attempt.Ref
	ordinal := model.LaunchOrdinalOne
	group := admissionTestGroup(model.LaunchKey{Attempt: ref, Ordinal: ordinal})
	if _, err := server.admissionReady.BindGroup(context.Background(), jobID, ref, ordinal, group); err != nil {
		t.Fatal(err)
	}
	if _, _, err := server.admissionReady.AllocateGrant(context.Background(), ref, ordinal); err != nil {
		t.Fatal(err)
	}
	child, err := model.NewChildIdentity(5203, "partial-result-child-"+requestID)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := model.NewEvidence("released", "partial result test release")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.RecordRelease(context.Background(), jobID, ref, ordinal, child, evidence); err != nil {
		t.Fatal(err)
	}
	verified, err := launcher.issuer.AttestQuiescence(custodian.PhysicalQuiescence{Group: group, Method: model.QuiescenceAlreadyAbsent})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.admissionReady.RecordQuiescence(context.Background(), jobID, ordinal, verified); err != nil {
		t.Fatal(err)
	}
	layout, err := authorityResultLayout(server.stateRoot, accepted.Record)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := engine.LogPathsForLayout(layout, jobID.String())
	if err != nil {
		t.Fatal(err)
	}
	return server, accepted, jobRun{jobID: jobID.String(), logPaths: paths}, paths
}

// TestLegacyPartialResultReclaimedWhenTerminalRecordSaveFails covers the legacy
// (non-admission) finalizer, where the partial artifact is published inside the
// store mutator but the authoritative record save happens after the mutator
// returns. If that save fails, leaving the excerpt behind is exactly what must
// not happen: on a nearly full filesystem its blocks are the ones the record
// write needs, so the optional artifact could block terminalization outright.
func TestLegacyPartialResultReclaimedWhenTerminalRecordSaveFails(t *testing.T) {
	backend := newFakeBackend("fake")
	server, _, cwd := newUnstartedTestServer(t, backend)

	server.mu.Lock()
	store, err := server.storeForCWDLocked(cwd)
	server.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	jobID := server.nextID("job")
	if err := server.createQueuedRecord(store, jobID, "ses_legacy_partial_reclaim", "fake", nil, nil, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, state := range []engine.JobState{engine.StateStarting, engine.StateRunning} {
		if err := server.transitionRecord(store, jobID, state); err != nil {
			t.Fatal(err)
		}
	}

	layout := store.Layout()
	paths, err := engine.LogPathsForLayout(layout, jobID)
	if err != nil {
		t.Fatal(err)
	}
	writePartialTranscript(t, paths.Stdout, `{"method":"item/completed","params":{"item":{"type":"agentMessage","text":"legacy excerpt must not survive a failed save"}}}`+"\n")

	// Make the authoritative record write fail for real rather than through a
	// seam: atomicWriteFile creates its temp file in the records directory, so a
	// read-only records directory fails the save while leaving reads working.
	info, err := os.Stat(layout.Jobs)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(layout.Jobs, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(layout.Jobs, info.Mode().Perm()) })

	err = server.finalizeTerminal(jobRun{jobID: jobID, store: store, logPaths: paths}, engine.StateTimedOut, "", nil)
	if err == nil {
		t.Skip("records directory remained writable; cannot exercise a failed terminal record save here")
	}

	entries, readErr := os.ReadDir(layout.Results)
	if readErr != nil {
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal(readErr)
		}
		return
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".partial-") {
			t.Fatalf("uncommitted legacy partial artifact %q remains after a failed terminal record save", entry.Name())
		}
	}
}

func writePartialTranscript(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func setPartialResultBeforePublishHookForTest(t *testing.T, hook func() error) {
	t.Helper()
	previous := partialResultBeforePublishForTest
	partialResultBeforePublishForTest = hook
	t.Cleanup(func() {
		partialResultBeforePublishForTest = previous
	})
}

func assertNoPartialResultArtifacts(t *testing.T, server *Server, record model.SafetyRecord) {
	t.Helper()
	layout := mustAuthorityResultLayout(t, server, record)
	entries, err := os.ReadDir(layout.Results)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".partial-") {
			t.Fatalf("uncommitted partial artifact %q remains", entry.Name())
		}
	}
}

func mustAuthorityResultLayout(t *testing.T, server *Server, record model.SafetyRecord) engine.WorkspaceLayout {
	t.Helper()
	layout, err := authorityResultLayout(server.stateRoot, record)
	if err != nil {
		t.Fatal(err)
	}
	return layout
}
