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
	protectedLogs := make([]string, 0)
	for _, record := range []jobstore.Record{retained, resumable, uncertain, queued, running, young} {
		protectedLogs = append(protectedLogs, writePruneTestLogs(t, record, map[string]string{"stdout": "keep"})...)
	}
	completedHome, outsideFile := writePruneTestCodexArtifacts(t, server, completed)
	if _, err := os.Stat(completedHome); err != nil {
		t.Fatal(err)
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
	if !dryRun.DryRun || dryRun.SuccessfulOlderThan != "168h0m0s" || dryRun.FailedOlderThan != "720h0m0s" || len(dryRun.WouldRemove) != 8 || dryRun.WouldRemoveBytes != 33 || len(dryRun.Removed) != 0 {
		t.Fatalf("dry-run selection = %d items, %d bytes, removed=%d: %+v", len(dryRun.WouldRemove), dryRun.WouldRemoveBytes, len(dryRun.Removed), dryRun)
	}
	for _, path := range append(append([]string(nil), completedLogs...), failedLogs...) {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("dry-run changed %s: %v", path, err)
		}
	}
	if _, err := os.Stat(filepath.Join(completedHome, "cache")); err != nil {
		t.Fatalf("dry-run removed cache: %v", err)
	}
	if record, err := store.Get(completed.JobID); err != nil || record.LogsPruned {
		t.Fatalf("dry-run changed log marker: record=%+v err=%v", record, err)
	}

	var reasonByJob = make(map[string]string)
	for _, skipped := range dryRun.Skipped {
		reasonByJob[skipped.JobID] = skipped.Reason
	}
	for jobID, want := range map[string]string{
		retained.JobID:  "session is retained",
		resumable.JobID: "backend session is resumable",
		uncertain.JobID: "cleanup is uncertain",
		queued.JobID:    "job is active or queued",
		running.JobID:   "job is active or queued",
		young.JobID:     "job is younger than the 168h0m0s retention threshold",
	} {
		if reasonByJob[jobID] != want {
			t.Errorf("skip reason for %s = %q, want %q", jobID, reasonByJob[jobID], want)
		}
	}

	applied, err := server.pruneArtifacts(store, false, successfulAge, failedAge)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.Removed) != 8 || applied.RemovedBytes != 33 || len(applied.WouldRemove) != 0 {
		t.Fatalf("apply result = %d items, %d bytes, would-remove=%d: %+v", len(applied.Removed), applied.RemovedBytes, len(applied.WouldRemove), applied)
	}
	for _, path := range append(append([]string(nil), completedLogs...), failedLogs...) {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("apply left log %s: %v", path, err)
		}
	}
	for _, path := range protectedLogs {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("apply removed ineligible log %s: %v", path, err)
		}
	}
	for _, name := range []string{"cache", "tmp"} {
		if _, err := os.Stat(filepath.Join(completedHome, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("apply left %s: %v", name, err)
		}
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Errorf("prune followed an owned-tree symlink: outside file stat = %v", err)
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

func TestAutomaticPruneSweepBoundsRecordWork(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	taskSpec, err := json.Marshal(protocol.TaskSpec{Backend: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 70 {
		requestID := fmt.Sprintf("automatic-%02d", i)
		if _, _, err := store.SubmitTx(jobstore.RequestKey{WorkspaceKey: "automatic-prune", RequestID: requestID}, taskSpec, func(string) (jobstore.Record, error) {
			return jobstore.Record{Backend: "fake"}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	firstSweep, err := server.automaticPruneSweep(store)
	if err != nil || len(firstSweep.Skipped) != autoPruneMaxJobsPerSweep || server.pruneApplyCursor == "" {
		t.Fatalf("first sweep skipped=%d cursor=%q err=%v", len(firstSweep.Skipped), server.pruneApplyCursor, err)
	}
	lastSweep, err := server.automaticPruneSweep(store)
	if err != nil || len(lastSweep.Skipped) != 6 || server.pruneApplyCursor != "" {
		t.Fatalf("last sweep skipped=%d cursor=%q err=%v", len(lastSweep.Skipped), server.pruneApplyCursor, err)
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

func newPruneTestJob(t *testing.T, server *Server, requestID string, state protocol.PublicState, cleanup protocol.Cleanup, retain bool, sessionID string, finishedAgo ...time.Duration) jobstore.Record {
	t.Helper()
	cwd := t.TempDir()
	canonicalCWD, err := engine.CanonicalWorkspace(cwd)
	if err != nil {
		t.Fatal(err)
	}
	workspaceKey := engine.WorkspaceKey(canonicalCWD)
	spec := protocol.TaskSpec{Backend: "codex", CWD: canonicalCWD, Prompt: "prune test", Write: true, RetainSession: retain}
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
			Write:            spec.Write,
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

func writePruneTestCodexArtifacts(t *testing.T, server *Server, record jobstore.Record) (homePath, outsideFile string) {
	t.Helper()
	layout, err := engine.LayoutForWorkspace(server.stateRoot, record.CWD)
	if err != nil {
		t.Fatal(err)
	}
	home, err := createManagedCodexHome(layout, record.JobID)
	if err != nil {
		t.Fatal(err)
	}
	cache, managed, err := prepareCodexJobCache(layout, record.JobID, home)
	if err != nil {
		home.close()
		t.Fatal(err)
	}
	defer managed.close()
	cacheFile := filepath.Join(cache.goMod, "module-cache-entry")
	tmpFile := filepath.Join(cache.tmp, "scratch")
	if err := os.WriteFile(cacheFile, []byte("cache!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmpFile, []byte("tmp"), 0o600); err != nil {
		t.Fatal(err)
	}
	outsideFile = filepath.Join(t.TempDir(), "outside-data")
	if err := os.WriteFile(outsideFile, []byte("must survive"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outsideFile), filepath.Join(cache.goBuild, "outside-link")); err != nil {
		t.Fatal(err)
	}
	return home.path, outsideFile
}
