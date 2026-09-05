//go:build darwin || linux

package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	defaultTranscriptMessageTail = 4
	maxTranscriptSidecarLine     = transcriptItemFileCap + 1
)

var transcriptKinds = map[string]struct{}{
	string(transcriptItemMessage):    {},
	string(transcriptItemReasoning):  {},
	string(transcriptItemTool):       {},
	string(transcriptItemToolResult): {},
	string(transcriptItemFileChange): {},
	string(transcriptItemWarning):    {},
	string(transcriptItemError):      {},
}

// handleJobTranscript returns a digest of the captured sidecar and the items
// selected with stateless filters. A missing sidecar is an empty transcript:
// it is expected for jobs that failed before their first turn and older jobs.
func (s *Server) handleJobTranscript(raw json.RawMessage) requestOutcome {
	var params protocol.JobTranscriptParams
	if err := decodeStrict(raw, &params); err != nil {
		return invalidParams(err)
	}
	if err := validateJobTranscriptParams(params); err != nil {
		return invalidParams(err)
	}

	store, err := s.ensureJobStore()
	if err != nil {
		return jobStoreUnavailable("open job store", err)
	}
	record, err := store.Get(params.JobID)
	if err != nil {
		if errors.Is(err, jobstore.ErrNotFound) {
			return requestOutcome{err: unknownJobError(params.JobID)}
		}
		return jobStoreUnavailable("get job", err)
	}

	result, err := s.jobTranscript(record, params)
	if err != nil {
		return jobStoreUnavailable("read transcript", err)
	}
	return requestOutcome{result: result}
}

func validateJobTranscriptParams(params protocol.JobTranscriptParams) error {
	if strings.TrimSpace(params.JobID) == "" {
		return errors.New("jobId is required")
	}
	for _, kind := range params.Kinds {
		if _, ok := transcriptKinds[kind]; !ok {
			return fmt.Errorf("kinds contains invalid transcript kind %q", kind)
		}
	}
	for name, value := range map[string]*int{
		"sinceOrdinal": params.SinceOrdinal,
		"last":         params.Last,
		"limit":        params.Limit,
	} {
		if value != nil && *value < 0 {
			return fmt.Errorf("%s cannot be negative", name)
		}
	}
	return nil
}

func (s *Server) jobTranscript(record jobstore.Record, params protocol.JobTranscriptParams) (protocol.JobTranscriptResult, error) {
	path, present, err := transcriptSidecarPath(record)
	if err != nil {
		return protocol.JobTranscriptResult{}, err
	}
	result := emptyJobTranscript(record)
	if present {
		result = readTranscriptSidecar(path, params)
		result.State = projectedState(record)
	}
	failureMarker := hasItemSidecarFailureMarker(path)
	result.Gap = result.Gap ||
		failureMarker ||
		hasItemSidecarFailure(record.Diagnostics) ||
		record.State == protocol.PublicStateUnknown
	if !record.State.IsTerminal() {
		_, active := s.ItemActivity(record.JobID)
		if active {
			result.Liveness = s.exactClaimDiagnostic(record.ProcessClaim).liveness
		}
	}
	return result, nil
}

func emptyJobTranscript(record jobstore.Record) protocol.JobTranscriptResult {
	return protocol.JobTranscriptResult{
		State:     projectedState(record),
		Counts:    newTranscriptCounts(),
		Items:     make([]protocol.TranscriptItem, 0),
		ItemCount: 0,
		Gap:       false,
	}
}

func transcriptSidecarPath(record jobstore.Record) (string, bool, error) {
	if strings.TrimSpace(record.Artifacts.Log) == "" {
		return "", false, nil
	}
	paths, err := engine.LogPathsForLayout(engine.WorkspaceLayout{Logs: filepath.Dir(record.Artifacts.Log)}, record.JobID)
	if err != nil {
		return "", false, fmt.Errorf("resolve backend logs: %w", err)
	}
	path, err := itemSidecarPath(paths.Stdout)
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

func newTranscriptCounts() map[string]int {
	return map[string]int{
		string(transcriptItemMessage):    0,
		string(transcriptItemReasoning):  0,
		string(transcriptItemTool):       0,
		string(transcriptItemToolResult): 0,
		string(transcriptItemFileChange): 0,
		string(transcriptItemWarning):    0,
		string(transcriptItemError):      0,
	}
}

func readTranscriptSidecar(path string, params protocol.JobTranscriptParams) protocol.JobTranscriptResult {
	result := protocol.JobTranscriptResult{
		Counts: newTranscriptCounts(),
		Items:  make([]protocol.TranscriptItem, 0),
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return result
	}
	if err != nil {
		// A failed read still leaves a useful captured prefix. Report its
		// incompleteness instead of hiding it behind an RPC failure.
		result.Gap = true
		return result
	}
	defer file.Close()

	kinds := make(map[string]struct{}, len(params.Kinds))
	for _, kind := range params.Kinds {
		kinds[kind] = struct{}{}
	}
	defaultDigest := len(params.Kinds) == 0 && params.Since == nil && params.SinceOrdinal == nil && params.Last == nil && params.Limit == nil
	last := 0
	if params.Last != nil {
		last = *params.Last
		if params.Limit != nil && *params.Limit < last {
			last = *params.Limit
		}
	}
	var items, messages, errorItems []protocol.TranscriptItem
	finishItems := func() {
		if defaultDigest {
			items = make([]protocol.TranscriptItem, 0, len(messages)+len(errorItems))
			items = append(items, messages...)
			items = append(items, errorItems...)
			sort.Slice(items, func(left, right int) bool { return items[left].Ordinal < items[right].Ordinal })
		} else if items == nil {
			items = make([]protocol.TranscriptItem, 0)
		}
		result.Items = items
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 8*1024), maxTranscriptSidecarLine)
	for scanner.Scan() {
		line := scanner.Bytes()
		if bytes.Equal(line, transcriptItemStopLine[:len(transcriptItemStopLine)-1]) {
			result.Gap = true
			continue
		}

		var item protocol.TranscriptItem
		if err := json.Unmarshal(line, &item); err != nil {
			finishItems()
			result.Gap = true
			return result
		}
		result.ItemCount++
		result.Counts[item.Kind]++
		setTranscriptBounds(&result, item)

		if len(kinds) > 0 {
			if _, ok := kinds[item.Kind]; !ok {
				continue
			}
		}
		if params.Since != nil && !item.At.After(*params.Since) {
			continue
		}
		if params.SinceOrdinal != nil && item.Ordinal <= *params.SinceOrdinal {
			continue
		}

		if defaultDigest {
			switch item.Kind {
			case string(transcriptItemMessage):
				messages = appendTranscriptTail(messages, item, defaultTranscriptMessageTail)
			case string(transcriptItemError):
				// Errors are typically rare and are the exception to the small
				// message tail: preserve every captured error in the digest.
				errorItems = append(errorItems, item)
			}
			continue
		}
		if params.Last != nil {
			items = appendTranscriptTail(items, item, last)
			continue
		}
		if params.Limit != nil && len(items) >= *params.Limit {
			continue
		}
		items = append(items, item)
	}
	if err := scanner.Err(); err != nil {
		finishItems()
		result.Gap = true
		return result
	}
	finishItems()
	return result
}

func setTranscriptBounds(result *protocol.JobTranscriptResult, item protocol.TranscriptItem) {
	if result == nil {
		return
	}
	if result.FirstAt == nil || item.At.Before(*result.FirstAt) {
		at := item.At
		result.FirstAt = &at
	}
	if result.LastAt == nil || item.At.After(*result.LastAt) {
		at := item.At
		result.LastAt = &at
	}
}

func hasItemSidecarFailure(diagnostics []string) bool {
	for _, diagnostic := range diagnostics {
		if strings.HasPrefix(diagnostic, itemSidecarDiagnosticPrefix) {
			return true
		}
	}
	return false
}

func hasItemSidecarFailureMarker(sidecarPath string) bool {
	if strings.TrimSpace(sidecarPath) == "" {
		return false
	}
	_, err := os.Stat(itemSidecarFailurePath(sidecarPath))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	// If the marker cannot be inspected, continuity cannot be established.
	return true
}

func appendTranscriptTail(items []protocol.TranscriptItem, item protocol.TranscriptItem, size int) []protocol.TranscriptItem {
	if size <= 0 {
		return items[:0]
	}
	if len(items) < size {
		return append(items, item)
	}
	copy(items, items[1:])
	items[len(items)-1] = item
	return items
}
