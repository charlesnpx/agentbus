//go:build darwin || linux

package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
	"golang.org/x/sys/unix"
)

// lineageSnapshot is taken under submissionMu. The scheduler lock makes the
// active set and durable records a conservative, coherent admission fence.
func (s *Server) lineageSnapshot(store *jobstore.Store) (map[string]jobstore.Record, map[string]bool, error) {
	s.executionMu.Lock()
	defer s.executionMu.Unlock()
	records, err := store.List()
	if err != nil {
		return nil, nil, err
	}
	byID := make(map[string]jobstore.Record, len(records))
	for _, record := range records {
		byID[record.JobID] = record
	}
	active := make(map[string]bool, len(s.executions))
	for id := range s.executions {
		active[id] = true
	}
	return byID, active, nil
}

// lineageRoot follows immutable resume links and fails closed on malformed,
// missing, cyclic, or mixed-backend ownership chains.
func lineageRoot(records map[string]jobstore.Record, id string) (jobstore.Record, map[string]bool, error) {
	seen := make(map[string]bool)
	backend := ""
	for {
		if id == "" || seen[id] {
			return jobstore.Record{}, seen, errors.New("resume lineage contains a missing or cyclic link")
		}
		seen[id] = true
		record, ok := records[id]
		if !ok {
			return jobstore.Record{}, seen, fmt.Errorf("resume lineage job %q is missing", id)
		}
		if backend == "" {
			backend = record.Backend
		} else if record.Backend != backend {
			return jobstore.Record{}, seen, errors.New("resume lineage has mismatched backends")
		}
		spec, err := taskSpecFromRecord(record)
		if err != nil {
			return jobstore.Record{}, seen, fmt.Errorf("invalid resume lineage job %q: %w", id, err)
		}
		if spec.ResumeJobID == "" {
			return record, seen, nil
		}
		id = spec.ResumeJobID
	}
}

func lineageMembers(records map[string]jobstore.Record, id string) (jobstore.Record, []jobstore.Record, error) {
	root, _, err := lineageRoot(records, id)
	if err != nil {
		return jobstore.Record{}, nil, err
	}
	members := make([]jobstore.Record, 0)
	for memberID, record := range records {
		candidate, visited, err := lineageRoot(records, memberID)
		if err != nil {
			if visited[id] || visited[root.JobID] {
				return jobstore.Record{}, nil, err
			}
			continue
		}
		if candidate.JobID == root.JobID {
			members = append(members, record)
		}
	}
	return root, members, nil
}

func lineageBlock(members []jobstore.Record, active map[string]bool) (string, string) {
	for _, member := range members {
		if !member.State.IsTerminal() || member.Starting || active[member.JobID] {
			return member.JobID, protocol.SessionDiscardActiveLineage
		}
	}
	for _, member := range members {
		if member.Cleanup != protocol.CleanupClean {
			return member.JobID, protocol.SessionDiscardCleanupUncertain
		}
	}
	return "", ""
}

func (s *Server) validateResumeLineage(spec protocol.TaskSpec, records map[string]jobstore.Record, active map[string]bool) error {
	root, members, err := lineageMembers(records, spec.ResumeJobID)
	if err != nil {
		return &resumeTargetError{jobID: spec.ResumeJobID, cause: err}
	}
	if root.SessionDiscardedAt != nil {
		return &resumeTargetError{jobID: root.JobID, reason: protocol.SessionDiscarded, cause: errors.New("conversation was discarded")}
	}
	if id, reason := lineageBlock(members, active); id != "" {
		return &resumeTargetError{jobID: id, cause: fmt.Errorf("lineage is unavailable: %s", reason)}
	}
	if spec.Backend == "codex" {
		layout := codexLayout(s.stateRoot)
		home, err := openExistingManagedCodexHome(layout, root.JobID)
		if err != nil {
			return &resumeTargetError{jobID: root.JobID, cause: fmt.Errorf("owned Codex home is unavailable: %w", err)}
		}
		home.close()
	}
	return nil
}

func codexLayout(root string) engine.WorkspaceLayout {
	return engine.WorkspaceLayout{Root: root, Namespace: filepath.Join(root, "sessions"), Codex: filepath.Join(root, "sessions", "codex")}
}

func (s *Server) handleSessionDiscard(raw json.RawMessage) requestOutcome {
	var params protocol.SessionDiscardParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if strings.TrimSpace(params.JobID) == "" {
		return invalidParams(errors.New("jobId is required"))
	}
	s.discardMu.Lock()
	defer s.discardMu.Unlock()
	s.submissionMu.Lock()
	store, err := s.ensureJobStore()
	if err != nil {
		s.submissionMu.Unlock()
		return jobStoreUnavailable("open job store", err)
	}
	records, active, err := s.lineageSnapshot(store)
	if err != nil {
		s.submissionMu.Unlock()
		return jobStoreUnavailable("read session lineage", err)
	}
	target, ok := records[params.JobID]
	if !ok {
		s.submissionMu.Unlock()
		return requestOutcome{err: unknownJobError(params.JobID)}
	}
	refuse := func(reason, jobID, message string) requestOutcome {
		return requestOutcome{err: protocol.NewError(protocol.ErrorSessionDiscardRefused, message, protocol.ErrorData{JobID: jobID, Reason: reason})}
	}
	if target.Backend != "codex" {
		s.submissionMu.Unlock()
		return refuse(protocol.SessionDiscardUnsupportedBackend, target.JobID, "session disposal is supported only for owned Codex conversations")
	}
	root, members, err := lineageMembers(records, target.JobID)
	if err != nil {
		s.submissionMu.Unlock()
		return refuse(protocol.SessionDiscardInvalidLineage, target.JobID, err.Error())
	}
	if id, reason := lineageBlock(members, active); id != "" {
		s.submissionMu.Unlock()
		return refuse(reason, id, "conversation has active work or uncertain cleanup")
	}
	commit := store.DiscardSession
	if s.discardSessionCommit != nil {
		commit = func(id string) (jobstore.Record, error) { return s.discardSessionCommit(store, id) }
	}
	root, err = commit(root.JobID)
	s.submissionMu.Unlock()
	if err != nil {
		return jobStoreUnavailable("commit session disposal", err)
	}
	if s.beforeDiscardRemoval != nil {
		s.beforeDiscardRemoval()
	}
	result := protocol.SessionDiscardResult{RootJobID: root.JobID, SessionDiscardedAt: *root.SessionDiscardedAt}
	home, err := openExistingManagedCodexHome(codexLayout(s.stateRoot), root.JobID)
	if errors.Is(err, unix.ENOENT) {
		result.HomeRemoval = protocol.HomeRemovalAlreadyAbsent
		return requestOutcome{result: result}
	}
	if err == nil {
		if s.afterDiscardHomeOpen != nil {
			s.afterDiscardHomeOpen()
		}
		err = removeManagedCodexHome(home)
		home.close()
	}
	if err != nil {
		result.HomeRemoval = protocol.HomeRemovalFailed
		result.Diagnostic = boundProviderValue(err.Error(), 512)
	} else {
		result.HomeRemoval = protocol.HomeRemovalRemoved
	}
	return requestOutcome{result: result}
}
