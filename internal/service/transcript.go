//go:build darwin || linux

package service

import (
	"bufio"
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
	string(transcriptItemTool):       {},
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
		result, err = readTranscriptSidecar(path, params)
		if err != nil {
			return protocol.JobTranscriptResult{}, err
		}
		result.State = projectedState(record)
	}
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
		string(transcriptItemTool):       0,
		string(transcriptItemFileChange): 0,
		string(transcriptItemWarning):    0,
		string(transcriptItemError):      0,
	}
}

func readTranscriptSidecar(path string, params protocol.JobTranscriptParams) (protocol.JobTranscriptResult, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return protocol.JobTranscriptResult{
			Counts: newTranscriptCounts(),
			Items:  make([]protocol.TranscriptItem, 0),
		}, nil
	}
	if err != nil {
		return protocol.JobTranscriptResult{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return protocol.JobTranscriptResult{}, err
	}
	// The digest deliberately retains every error item. Enforce the writer's
	// durable file cap here too, so an unexpected artifact cannot make that
	// bounded policy consume unbounded memory.
	if info.Size() > transcriptItemFileCap {
		return protocol.JobTranscriptResult{}, fmt.Errorf("transcript sidecar exceeds its %d-byte cap", transcriptItemFileCap)
	}

	result := protocol.JobTranscriptResult{
		Counts: newTranscriptCounts(),
		Items:  make([]protocol.TranscriptItem, 0),
	}
	selector := newTranscriptSelector(params)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 8*1024), maxTranscriptSidecarLine)
	for scanner.Scan() {
		line := scanner.Bytes()
		stopped, err := transcriptAppendStopped(line)
		if err != nil {
			return protocol.JobTranscriptResult{}, fmt.Errorf("decode sidecar entry: %w", err)
		}
		if stopped {
			result.Gap = true
			continue
		}

		var item protocol.TranscriptItem
		if err := json.Unmarshal(line, &item); err != nil {
			return protocol.JobTranscriptResult{}, fmt.Errorf("decode transcript item: %w", err)
		}
		if err := validateTranscriptItem(item); err != nil {
			return protocol.JobTranscriptResult{}, err
		}
		result.ItemCount++
		result.Counts[item.Kind]++
		setTranscriptBounds(&result, item)
		if selector.matches(item) {
			selector.add(item)
		}
	}
	if err := scanner.Err(); err != nil {
		return protocol.JobTranscriptResult{}, fmt.Errorf("scan transcript sidecar: %w", err)
	}
	result.Items = selector.selectedItems()
	return result, nil
}

func transcriptAppendStopped(line []byte) (bool, error) {
	var control struct {
		AppendStopped *bool `json:"appendStopped"`
	}
	if err := json.Unmarshal(line, &control); err != nil {
		return false, err
	}
	if control.AppendStopped == nil {
		return false, nil
	}
	if !*control.AppendStopped {
		return false, errors.New("appendStopped control entry must be true")
	}
	return true, nil
}

func validateTranscriptItem(item protocol.TranscriptItem) error {
	if item.Ordinal <= 0 {
		return errors.New("transcript item has no ordinal")
	}
	if item.At.IsZero() {
		return errors.New("transcript item has no timestamp")
	}
	if _, ok := transcriptKinds[item.Kind]; !ok {
		return fmt.Errorf("transcript item has invalid kind %q", item.Kind)
	}
	return nil
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

type transcriptSelector struct {
	params        protocol.JobTranscriptParams
	kinds         map[string]struct{}
	defaultDigest bool
	items         []protocol.TranscriptItem
	messages      []protocol.TranscriptItem
	errors        []protocol.TranscriptItem
	last          int
}

func newTranscriptSelector(params protocol.JobTranscriptParams) *transcriptSelector {
	selector := &transcriptSelector{
		params:        params,
		kinds:         make(map[string]struct{}, len(params.Kinds)),
		defaultDigest: len(params.Kinds) == 0 && params.Since == nil && params.SinceOrdinal == nil && params.Last == nil && params.Limit == nil,
	}
	for _, kind := range params.Kinds {
		selector.kinds[kind] = struct{}{}
	}
	if params.Last != nil {
		selector.last = *params.Last
		if params.Limit != nil && *params.Limit < selector.last {
			selector.last = *params.Limit
		}
	}
	return selector
}

func (selector *transcriptSelector) matches(item protocol.TranscriptItem) bool {
	if selector == nil {
		return false
	}
	if len(selector.kinds) > 0 {
		if _, ok := selector.kinds[item.Kind]; !ok {
			return false
		}
	}
	if selector.params.Since != nil && !item.At.After(*selector.params.Since) {
		return false
	}
	if selector.params.SinceOrdinal != nil && item.Ordinal <= *selector.params.SinceOrdinal {
		return false
	}
	return true
}

func (selector *transcriptSelector) add(item protocol.TranscriptItem) {
	if selector == nil {
		return
	}
	if selector.defaultDigest {
		switch item.Kind {
		case string(transcriptItemMessage):
			selector.messages = appendTranscriptTail(selector.messages, item, defaultTranscriptMessageTail)
		case string(transcriptItemError):
			// Errors are typically rare and are the exception to the small
			// message tail: preserve every captured error in the digest.
			selector.errors = append(selector.errors, item)
		}
		return
	}
	if selector.params.Last != nil {
		selector.items = appendTranscriptTail(selector.items, item, selector.last)
		return
	}
	if selector.params.Limit != nil && len(selector.items) >= *selector.params.Limit {
		return
	}
	selector.items = append(selector.items, item)
}

func (selector *transcriptSelector) selectedItems() []protocol.TranscriptItem {
	if selector == nil {
		return make([]protocol.TranscriptItem, 0)
	}
	if !selector.defaultDigest {
		if selector.items == nil {
			return make([]protocol.TranscriptItem, 0)
		}
		return selector.items
	}
	items := make([]protocol.TranscriptItem, 0, len(selector.messages)+len(selector.errors))
	items = append(items, selector.messages...)
	items = append(items, selector.errors...)
	sort.Slice(items, func(left, right int) bool { return items[left].Ordinal < items[right].Ordinal })
	return items
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
