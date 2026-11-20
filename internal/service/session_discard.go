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

// Admission and disposal hold submissionMu to prevent new lineage members.
// Read durable records first: a job observed nonterminal stays a blocker,
// and one observed terminal cannot be launched again. The later active-set
// snapshot also blocks terminal jobs whose local execution has not retired.
func (s *Server) lineageSnapshot(store *jobstore.Store) (map[string]jobstore.Record, map[string]bool, error) {
	records, err := store.List()
	if err != nil {
		return nil, nil, err
	}
	s.executionMu.Lock()
	active := make(map[string]bool, len(s.executions))
	for id := range s.executions {
		active[id] = true
	}
	s.executionMu.Unlock()
	byID := make(map[string]jobstore.Record, len(records))
	for _, record := range records {
		byID[record.JobID] = record
	}
	return byID, active, nil
}

func lineageMembers(records map[string]jobstore.Record, id string) (jobstore.Record, []jobstore.Record, error) {
	return lineageMembersDecoded(records, id, taskSpecFromRecord)
}

// Resolve each chain once per snapshot, including invalid chains. A backend
// mismatch records the node reached by the invalid edge: if it is the target
// or its root, that malformed descendant must still block the conversation.
func lineageMembersDecoded(records map[string]jobstore.Record, id string, decode func(jobstore.Record) (protocol.TaskSpec, error)) (jobstore.Record, []jobstore.Record, error) {
	type resolution struct {
		root     string
		mismatch string
		err      error
	}
	resolved := make(map[string]resolution, len(records))
	visiting := make(map[string]bool)
	resolve := func(start string) resolution {
		var path []string
		current := start
		var result resolution
		for {
			if known, ok := resolved[current]; ok {
				result = known
				break
			}
			if current == "" || visiting[current] {
				result.err = errors.New("resume lineage contains a missing or cyclic link")
				break
			}
			record, ok := records[current]
			if !ok {
				result.err = fmt.Errorf("resume lineage job %q is missing", current)
				break
			}
			visiting[current] = true
			path = append(path, current)
			spec, err := decode(record)
			if err != nil {
				result.err = fmt.Errorf("invalid resume lineage job %q: %w", current, err)
				break
			}
			if spec.ResumeJobID == "" {
				result.root = current
				break
			}
			if parent, ok := records[spec.ResumeJobID]; ok && parent.Backend != record.Backend {
				result.err = errors.New("resume lineage has mismatched backends")
				result.mismatch = spec.ResumeJobID
				break
			}
			current = spec.ResumeJobID
		}
		for _, member := range path {
			resolved[member] = result
			delete(visiting, member)
		}
		return result
	}
	target := resolve(id)
	if target.err != nil {
		return jobstore.Record{}, nil, target.err
	}
	root := records[target.root]
	members := make([]jobstore.Record, 0)
	for memberID, record := range records {
		candidate := resolve(memberID)
		if candidate.err != nil {
			if candidate.mismatch == id || candidate.mismatch == root.JobID {
				return jobstore.Record{}, nil, candidate.err
			}
			continue
		}
		if candidate.root == root.JobID {
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

func (s *Server) validateResumeLineage(spec protocol.TaskSpec, root jobstore.Record, members []jobstore.Record, active map[string]bool) error {
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

func (s *Server) reobserveLineageCleanup(store *jobstore.Store, members []jobstore.Record, active map[string]bool) error {
	if _, reason := lineageBlock(members, active); reason == protocol.SessionDiscardActiveLineage {
		return nil
	}
	for i, member := range members {
		if member.Cleanup != protocol.CleanupUncertain {
			continue
		}
		updated, err := s.reobserveTerminalCleanup(store, member)
		if err != nil {
			return err
		}
		members[i] = updated
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
	if err := s.reobserveLineageCleanup(store, members, active); err != nil {
		s.submissionMu.Unlock()
		return jobStoreUnavailable("reobserve session cleanup", err)
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
