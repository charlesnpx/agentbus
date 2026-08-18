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
	layout, err := authorityResultLayout(server.stateRoot, accepted.Record)
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

func writePartialTranscript(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}
