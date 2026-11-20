//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestPruneDryRunApplyEligibilityAndTranscriptMarker(t *testing.T) {
	root := t.TempDir()
	server := newTestServer(t, root, Config{})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}

	completed := newPruneTestJob(t, server, "completed", protocol.PublicStateCompleted, protocol.CleanupClean, false, "")
	failed := newPruneTestJob(t, server, "failed", protocol.PublicStateFailed, protocol.CleanupClean, false, "")
	retained := newPruneTestJob(t, server, "retained", protocol.PublicStateCompleted, protocol.CleanupClean, true, "session-retained")
	resumable := newPruneTestJob(t, server, "resumable", protocol.PublicStateFailed, protocol.CleanupClean, false, "session-resumable")
	uncertain := newPruneTestJob(t, server, "uncertain", protocol.PublicStateCompleted, protocol.CleanupUncertain, false, "")
	queued := newPruneTestJob(t, server, "queued", protocol.PublicStateQueued, protocol.CleanupClean, false, "")
	running := newPruneTestJob(t, server, "running", protocol.PublicStateRunning, protocol.CleanupClean, false, "")
	young := newPruneTestJob(t, server, "young", protocol.PublicStateCompleted, protocol.CleanupClean, false, "", time.Hour)

	completedLogs := writePruneTestLogs(t, completed, map[string]string{
		"stdout": "out",
		"stderr": "error",
		"items":  "item",
	})
	failedLogs := writePruneTestLogs(t, failed, map[string]string{
		"stdout": "failed",
		"stderr": "ferr",
		"items":  "fi",
	})
	eligibleLogs := make([]string, 0)
	for _, record := range []jobstore.Record{retained, resumable, uncertain} {
		eligibleLogs = append(eligibleLogs, writePruneTestLogs(t, record, map[string]string{"stdout": "keep"})...)
	}
	protectedLogs := make([]string, 0)
	for _, record := range []jobstore.Record{queued, running, young} {
		protectedLogs = append(protectedLogs, writePruneTestLogs(t, record, map[string]string{"stdout": "keep"})...)
	}
	resultPath := filepath.Join(root, "artifacts", "results", completed.JobID+".txt")
	if err := os.MkdirAll(filepath.Dir(resultPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, []byte("authoritative result"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := store.MarkStarting(running.JobID); err != nil {
		t.Fatal(err)
	}

	const successfulAge = 7 * 24 * time.Hour
	const failedAge = 30 * 24 * time.Hour
	dryRunOutcome := server.handleArtifactsPrune(mustJSON(t, protocol.ArtifactsPruneParams{}))
	if dryRunOutcome.err != nil {
		t.Fatalf("default prune dry-run error: %+v", dryRunOutcome.err)
	}
	dryRun, ok := dryRunOutcome.result.(protocol.ArtifactsPruneResult)
	if !ok {
		t.Fatalf("default prune dry-run result type = %T", dryRunOutcome.result)
	}
	if !dryRun.DryRun || dryRun.SuccessfulOlderThan != "168h0m0s" || dryRun.FailedOlderThan != "720h0m0s" || len(dryRun.WouldRemove) != 9 || dryRun.WouldRemoveBytes != 36 || len(dryRun.Removed) != 0 {
		t.Fatalf("dry-run selection = %d items, %d bytes, removed=%d: %+v", len(dryRun.WouldRemove), dryRun.WouldRemoveBytes, len(dryRun.Removed), dryRun)
	}
	for _, path := range append(append([]string(nil), completedLogs...), failedLogs...) {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("dry-run changed %s: %v", path, err)
		}
	}
	if record, err := store.Get(completed.JobID); err != nil || record.LogsPruned {
		t.Fatalf("dry-run changed log marker: record=%+v err=%v", record, err)
	}

	var reasonByJob = make(map[string]string)
	for _, skipped := range dryRun.Skipped {
		reasonByJob[skipped.JobID] = skipped.Reason
	}
	for jobID, want := range map[string]string{
		queued.JobID:  "job is active or queued",
		running.JobID: "job is active or queued",
		young.JobID:   "job is younger than the 168h0m0s retention threshold",
	} {
		if reasonByJob[jobID] != want {
			t.Errorf("skip reason for %s = %q, want %q", jobID, reasonByJob[jobID], want)
		}
	}

	applied, err := server.pruneArtifacts(store, false, successfulAge, failedAge)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Removed) != 9 || applied.RemovedBytes != 36 || len(applied.WouldRemove) != 0 {
		t.Fatalf("apply result = %d items, %d bytes, would-remove=%d: %+v", len(applied.Removed), applied.RemovedBytes, len(applied.WouldRemove), applied)
	}
	for _, path := range append(append([]string(nil), completedLogs...), failedLogs...) {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("apply left log %s: %v", path, err)
		}
	}
	for _, path := range eligibleLogs {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("apply left eligible retained or uncertain log %s: %v", path, err)
		}
	}
	for _, path := range protectedLogs {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("apply removed ineligible log %s: %v", path, err)
		}
	}
	if got, err := os.ReadFile(resultPath); err != nil || string(got) != "authoritative result" {
		t.Errorf("result artifact = %q, %v", got, err)
	}
	marked, err := store.Get(completed.JobID)
	if err != nil || !marked.LogsPruned {
		t.Fatalf("job log marker = %+v, %v; want logsPruned", marked, err)
	}
	transcript := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: completed.JobID})
	if !transcript.Pruned || !transcript.Gap || transcript.ItemCount != 0 {
		t.Fatalf("pruned transcript = %+v", transcript)
	}
	if _, err := store.Get(failed.JobID); err != nil {
		t.Fatalf("pruning removed the job record: %v", err)
	}
}

func TestAutomaticPruneSweepVisitsAllRecordsAndBoundsDetails(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	taskSpec, err := json.Marshal(protocol.TaskSpec{Intent: engine.IntentInspect, Backend: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 140 {
		requestID := fmt.Sprintf("automatic-%02d", i)
		if _, _, err := store.SubmitTx(jobstore.RequestKey{WorkspaceKey: "automatic-prune", RequestID: requestID}, taskSpec, func(string) (jobstore.Record, error) {
			return jobstore.Record{Backend: "fake"}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	sweep, err := server.automaticPruneSweep(store)
	if err != nil || len(sweep.Skipped) != maxPruneDetails || sweep.SkippedCount != 140 || !sweep.DetailsTruncated {
		t.Fatalf("sweep skipped=%d count=%d truncated=%t err=%v, want all 140 records and bounded details", len(sweep.Skipped), sweep.SkippedCount, sweep.DetailsTruncated, err)
	}
}

func TestAutomaticPruneSweepRetriesMarkedLogSidecars(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record := newPruneTestJob(t, server, "marked-sidecars", protocol.PublicStateCompleted, protocol.CleanupClean, false, "")
	paths := writePruneTestLogs(t, record, map[string]string{
		"stdout": "out",
		"stderr": "error",
		"items":  "item",
	})
	if _, err := store.MarkLogsPruned(record.JobID); err != nil {
		t.Fatal(err)
	}

	result, err := server.automaticPruneSweep(store)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Removed) != len(paths) {
		t.Fatalf("sweep removed %d sidecars, want %d: %+v", len(result.Removed), len(paths), result)
	}
	for _, path := range paths {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("sweep left marked sidecar %s: %v", path, err)
		}
	}
}

func TestPruneWaitsForTerminalExecutionCaptureToRetire(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record := newPruneTestJob(t, server, "terminal-with-capture", protocol.PublicStateCompleted, protocol.CleanupUncertain, false, "")
	paths := writePruneTestLogs(t, record, map[string]string{"stdout": "still draining"})
	server.executionMu.Lock()
	server.executions = map[string]*activeExecution{record.JobID: {jobID: record.JobID}}
	server.executionMu.Unlock()
	result, err := server.pruneArtifacts(store, false, defaultSuccessfulPruneAge, defaultFailedPruneAge)
	if err != nil || result.SkippedCount != 1 || len(result.Skipped) != 1 || result.Skipped[0].Reason != "job still has an active execution" {
		t.Fatalf("prune while capture active = %#v, %v", result, err)
	}
	if stored, err := store.Get(record.JobID); err != nil || stored.LogsPruned {
		t.Fatalf("active capture was marked pruned: %#v, %v", stored, err)
	}
	if _, err := os.Stat(paths[0]); err != nil {
		t.Fatalf("active capture file was removed: %v", err)
	}
	server.executionMu.Lock()
	delete(server.executions, record.JobID)
	server.executionMu.Unlock()
	result, err = server.pruneArtifacts(store, false, defaultSuccessfulPruneAge, defaultFailedPruneAge)
	if err != nil || result.RemovedCount != 1 {
		t.Fatalf("prune after capture retirement = %#v, %v", result, err)
	}
	stored, err := store.Get(record.JobID)
	if err != nil || !stored.LogsPruned || stored.Cleanup != protocol.CleanupUncertain {
		t.Fatalf("retired capture marker = %#v, %v", stored, err)
	}
}

func newPruneTestJob(t *testing.T, server *Server, requestID string, state protocol.PublicState, cleanup protocol.Cleanup, retain bool, sessionID string, finishedAgo ...time.Duration) jobstore.Record {
	t.Helper()
	cwd := t.TempDir()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	workspaceKey := engine.WorkspaceKey(canonicalCWD)
	spec := protocol.TaskSpec{Backend: "codex", CWD: canonicalCWD, Prompt: "prune test", Intent: engine.IntentEdit, RetainSession: retain}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := store.SubmitTx(jobstore.RequestKey{WorkspaceKey: workspaceKey, RequestID: requestID}, raw, func(id string) (jobstore.Record, error) {
		return jobstore.Record{
			JobID:            id,
			WorkspaceKey:     workspaceKey,
			RequestID:        requestID,
			Backend:          spec.Backend,
			BackendSessionID: sessionID,
			RetainSession:    retain,
			CWD:              canonicalCWD,
			Intent:           spec.Intent,
			Cleanup:          cleanup,
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if state == protocol.PublicStateRunning {
		return record
	}
	if state == protocol.PublicStateQueued {
		return record
	}
	failureClass := protocol.FailureClass("")
	if state == protocol.PublicStateFailed {
		failureClass = protocol.FailureClassBackendError
	}
	age := 40 * 24 * time.Hour
	if len(finishedAgo) > 0 {
		age = finishedAgo[0]
	}
	finished := time.Now().UTC().Add(-age)
	terminal, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:            state,
		Cleanup:          cleanup,
		BackendSessionID: sessionID,
		FailureClass:     failureClass,
		FinishedAt:       finished,
	})
	if err != nil {
		t.Fatal(err)
	}
	return terminal
}

func writePruneTestLogs(t *testing.T, record jobstore.Record, content map[string]string) []string {
	t.Helper()
	paths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	items, err := itemSidecarPath(paths.Stdout)
	if err != nil {
		t.Fatal(err)
	}
	all := map[string]string{"stdout": paths.Stdout, "stderr": paths.Stderr, "items": items}
	var written []string
	for kind, value := range content {
		path := all[kind]
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
		written = append(written, path)
	}
	return written
}
