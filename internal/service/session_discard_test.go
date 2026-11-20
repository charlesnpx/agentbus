//go:build darwin || linux

package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func ownedLineageFixture(t *testing.T) (*Server, *jobstore.Store, jobstore.Record, string) {
	t.Helper()
	s := newTestServer(t, t.TempDir(), Config{Backends: []engine.Backend{helloBackend{name: "codex"}}})
	params := submissionParams("lineage-root", "root", "codex", t.TempDir(), "root task")
	params.TaskSpec.RetainSession = true
	submitted := submitResultForTest(t, submitForTest(t, s, params))
	store, err := s.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	path, home, err := s.prepareCodexHome(codexLayout(s.stateRoot), submitted.JobID)
	if err != nil {
		t.Fatal(err)
	}
	home.close()
	if err := os.WriteFile(filepath.Join(path, "provider-data"), []byte("keep until discard"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := engine.WriteResultForLayout(engine.WorkspaceLayout{Results: filepath.Join(s.stateRoot, "artifacts", "results")}, submitted.JobID, []byte("root result"), engine.DefaultInlineResultCap)
	if err != nil {
		t.Fatal(err)
	}
	root, err := store.MarkTerminal(submitted.JobID, jobstore.TerminalUpdate{State: protocol.PublicStateCompleted, Cleanup: protocol.CleanupClean, BackendSessionID: "thread-1", ResultText: info.Text, ResultPath: info.ResultPath, ResultSHA256: info.SHA256, ResultBytes: info.Bytes})
	if err != nil {
		t.Fatal(err)
	}
	return s, store, root, path
}

func TestDiscardRevokesWholeLineageBeforeRemovalAndPreservesReplay(t *testing.T) {
	s, store, root, home := ownedLineageFixture(t)
	childParams := submissionParams("lineage-child", "child", "codex", t.TempDir(), "continue")
	childParams.TaskSpec.ResumeJobID = root.JobID
	childParams.TaskSpec.RetainSession = true
	child := submitResultForTest(t, submitForTest(t, s, childParams))
	blocked := submissionParams("lineage-other", "other", "codex", t.TempDir(), "another continuation")
	blocked.TaskSpec.ResumeJobID = root.JobID
	if outcome := submitForTest(t, s, blocked); outcome.err == nil || outcome.err.Data.JobID != child.JobID {
		t.Fatalf("queued descendant did not block sibling: %#v", outcome.err)
	}
	if outcome := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID})); outcome.err == nil || outcome.err.Data.Reason != protocol.SessionDiscardActiveLineage || outcome.err.Data.JobID != child.JobID {
		t.Fatalf("queued descendant did not block disposal: %#v", outcome.err)
	}
	childRecord, err := store.MarkTerminal(child.JobID, jobstore.TerminalUpdate{State: protocol.PublicStateFailed, Cleanup: protocol.CleanupClean, FailureClass: protocol.FailureClassBackendError, BackendSessionID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	rootBefore, err := store.Get(root.JobID)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	s.beforeDiscardRemoval = func() { close(entered); <-release }
	out := make(chan requestOutcome, 1)
	go func() { out <- s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: child.JobID})) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("disposal did not reach post-commit barrier")
	}
	blocked.TaskSpec.ResumeJobID = child.JobID
	if outcome := submitForTest(t, s, blocked); outcome.err == nil || outcome.err.Data.Reason != protocol.SessionDiscarded {
		close(release)
		t.Fatalf("resume after commit = %#v, want discarded", outcome.err)
	}
	if replay := submitResultForTest(t, submitForTest(t, s, childParams)); !replay.Deduplicated || replay.JobID != child.JobID {
		close(release)
		t.Fatalf("exact replay = %#v", replay)
	}
	if _, err := os.Stat(filepath.Join(home, "provider-data")); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	outcome := <-out
	if outcome.err != nil {
		t.Fatalf("discard: %#v", outcome.err)
	}
	result := outcome.result.(protocol.SessionDiscardResult)
	if result.RootJobID != root.JobID || result.SessionDiscardedAt.IsZero() || result.HomeRemoval != protocol.HomeRemovalRemoved {
		t.Fatalf("discard result: %#v", result)
	}
	if _, err := os.Lstat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("home still exists: %v", err)
	}
	rootAfter, err := store.Get(root.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if rootAfter.SessionDiscardedAt == nil {
		t.Fatal("root marker missing")
	}
	rootAfter.SessionDiscardedAt = nil
	if !reflect.DeepEqual(rootBefore, rootAfter) {
		t.Fatalf("disposal changed root history: before=%#v after=%#v", rootBefore, rootAfter)
	}
	childAfter, err := store.Get(child.JobID)
	if err != nil || !reflect.DeepEqual(childRecord, childAfter) {
		t.Fatalf("disposal changed child: %#v %v", childAfter, err)
	}
	wire, err := s.jobRecordWire(childAfter)
	if err != nil || wire.SessionDiscardedAt == nil || !wire.SessionDiscardedAt.Equal(result.SessionDiscardedAt) {
		t.Fatalf("descendant projection = %#v, %v", wire.SessionDiscardedAt, err)
	}
	s.beforeDiscardRemoval = nil
	retry := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID}))
	if retry.err != nil {
		t.Fatalf("retry: %#v", retry.err)
	}
	retried := retry.result.(protocol.SessionDiscardResult)
	if retried.HomeRemoval != protocol.HomeRemovalAlreadyAbsent || !retried.SessionDiscardedAt.Equal(result.SessionDiscardedAt) {
		t.Fatalf("retry result = %#v", retried)
	}
}

func TestDiscardFailureAndSymlinkAreSafeToRetry(t *testing.T) {
	s, store, root, home := ownedLineageFixture(t)
	s.discardSessionCommit = func(*jobstore.Store, string) (jobstore.Record, error) {
		return jobstore.Record{}, errors.New("injected commit failure")
	}
	outcome := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID}))
	if outcome.err == nil {
		t.Fatal("injected commit failure accepted")
	}
	if got, err := store.Get(root.JobID); err != nil || got.SessionDiscardedAt != nil {
		t.Fatalf("failed commit changed marker: %#v %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(home, "provider-data")); err != nil {
		t.Fatal(err)
	}
	s.discardSessionCommit = nil
	moved := home + "-moved"
	if err := os.Rename(home, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(moved, home); err != nil {
		t.Fatal(err)
	}
	outcome = s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID}))
	if outcome.err != nil {
		t.Fatal(outcome.err)
	}
	failed := outcome.result.(protocol.SessionDiscardResult)
	if failed.HomeRemoval != protocol.HomeRemovalFailed || failed.Diagnostic == "" {
		t.Fatalf("symlink discard = %#v", failed)
	}
	if _, err := os.Stat(filepath.Join(moved, "provider-data")); err != nil {
		t.Fatalf("symlink redirected deletion: %v", err)
	}
	if err := os.Remove(home); err != nil {
		t.Fatal(err)
	}
	retry := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID}))
	if retry.err != nil {
		t.Fatal(retry.err)
	}
	again := retry.result.(protocol.SessionDiscardResult)
	if again.HomeRemoval != protocol.HomeRemovalAlreadyAbsent || !again.SessionDiscardedAt.Equal(failed.SessionDiscardedAt) {
		t.Fatalf("retry = %#v", again)
	}
}

func TestLineageUncertainAndMissingHomeRefuseResumeAndDiscard(t *testing.T) {
	s, store, root, home := ownedLineageFixture(t)
	childParams := submissionParams("lineage-uncertain", "child", "codex", t.TempDir(), "continue")
	childParams.TaskSpec.ResumeJobID = root.JobID
	child := submitResultForTest(t, submitForTest(t, s, childParams))
	_, err := store.MarkTerminal(child.JobID, jobstore.TerminalUpdate{State: protocol.PublicStateUnknown, Cleanup: protocol.CleanupUncertain, BackendSessionID: "thread-1"})
	if err != nil {
		t.Fatal(err)
	}
	resume := submissionParams("lineage-uncertain", "other", "codex", t.TempDir(), "continue")
	resume.TaskSpec.ResumeJobID = root.JobID
	if outcome := submitForTest(t, s, resume); outcome.err == nil || outcome.err.Data.JobID != child.JobID || !strings.Contains(outcome.err.Message, "cleanup_uncertain") {
		t.Fatalf("resume = %#v", outcome.err)
	}
	if outcome := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID})); outcome.err == nil || outcome.err.Data.Reason != protocol.SessionDiscardCleanupUncertain {
		t.Fatalf("discard = %#v", outcome.err)
	}
	if _, err := os.Stat(home); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDiscardSerializesRemoval(t *testing.T) {
	s, _, root, home := ownedLineageFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	s.beforeDiscardRemoval = func() {
		if first {
			first = false
			close(entered)
			<-release
		}
	}
	results := make(chan requestOutcome, 2)
	params := mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID})
	go func() { results <- s.handleSessionDiscard(params) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first discard did not reach removal")
	}
	go func() { results <- s.handleSessionDiscard(params) }()
	close(release)
	one, two := <-results, <-results
	if one.err != nil || two.err != nil {
		t.Fatalf("concurrent discard errors = %#v, %#v", one.err, two.err)
	}
	a, b := one.result.(protocol.SessionDiscardResult), two.result.(protocol.SessionDiscardResult)
	removals := map[string]int{a.HomeRemoval: 1}
	removals[b.HomeRemoval]++
	if removals[protocol.HomeRemovalRemoved] != 1 || removals[protocol.HomeRemovalAlreadyAbsent] != 1 || !a.SessionDiscardedAt.Equal(b.SessionDiscardedAt) {
		t.Fatalf("concurrent discard results = %#v, %#v", a, b)
	}
	if _, err := os.Lstat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discarded home remains: %v", err)
	}
}

func TestResumeRefusesMissingOwnedHome(t *testing.T) {
	s, _, root, home := ownedLineageFixture(t)
	if err := os.RemoveAll(home); err != nil {
		t.Fatal(err)
	}
	params := submissionParams("lineage-missing", "resume", "codex", t.TempDir(), "continue")
	params.TaskSpec.ResumeJobID = root.JobID
	outcome := submitForTest(t, s, params)
	if outcome.err == nil || outcome.err.Data.Code != protocol.ErrorInvalidTaskSpec || outcome.err.Data.JobID != root.JobID {
		t.Fatalf("resume with missing home = %#v", outcome.err)
	}
	if _, err := os.Lstat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resume recreated missing home: %v", err)
	}
}

func TestDiscardRejectsReplacementAfterHomeIsOpened(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprintf("symlink=%t", symlink), func(t *testing.T) {
			s, _, root, home := ownedLineageFixture(t)
			other := t.TempDir()
			marker := filepath.Join(other, "must-survive")
			if err := os.WriteFile(marker, []byte("other conversation"), 0o600); err != nil {
				t.Fatal(err)
			}
			s.afterDiscardHomeOpen = func() {
				if err := os.Rename(home, home+"-moved"); err != nil {
					t.Fatal(err)
				}
				if symlink {
					if err := os.Symlink(other, home); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Rename(other, home); err != nil {
					t.Fatal(err)
				} else {
					marker = filepath.Join(home, "must-survive")
				}
			}
			outcome := s.handleSessionDiscard(mustJSON(t, protocol.SessionDiscardParams{JobID: root.JobID}))
			if outcome.err != nil || outcome.result.(protocol.SessionDiscardResult).HomeRemoval != protocol.HomeRemovalFailed {
				t.Fatalf("discard replacement = %#v", outcome)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("replacement contents were removed: %v", err)
			}
		})
	}
}
