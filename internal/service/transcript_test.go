//go:build darwin || linux

package service

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/jobstore"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestJobTranscriptDefaultDigestSurvivesTerminalState(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	record := transcriptTestRecord(t, server, "digest")
	start := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	items := make([]protocol.TranscriptItem, 0, 13)
	for ordinal := 1; ordinal <= 6; ordinal++ {
		items = append(items, transcriptTestItem(ordinal, start.Add(time.Duration(ordinal)*time.Second), "tool"))
	}
	for ordinal := 7; ordinal <= 12; ordinal++ {
		items = append(items, transcriptTestItem(ordinal, start.Add(time.Duration(ordinal)*time.Second), "message"))
	}
	items = append(items, transcriptTestItem(13, start.Add(13*time.Second), "error"))
	writeTranscriptSidecar(t, record, items, false)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCompleted,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: start.Add(14 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	result := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID})
	if result.State != protocol.PublicStateCompleted || result.ItemCount != len(items) || result.Gap {
		t.Fatalf("terminal transcript summary = %#v", result)
	}
	wantCounts := map[string]int{
		"message":    6,
		"reasoning":  0,
		"tool":       6,
		"toolResult": 0,
		"fileChange": 0,
		"warning":    0,
		"error":      1,
	}
	for kind, want := range wantCounts {
		if got, ok := result.Counts[kind]; !ok || got != want {
			t.Fatalf("digest count for %q = %d, present=%t, want %d: %#v", kind, got, ok, want, result.Counts)
		}
	}
	if result.FirstAt == nil || !result.FirstAt.Equal(items[0].At) || result.LastAt == nil || !result.LastAt.Equal(items[len(items)-1].At) {
		t.Fatalf("digest bounds = first:%v last:%v", result.FirstAt, result.LastAt)
	}
	if got, want := transcriptOrdinals(result.Items), []int{9, 10, 11, 12, 13}; !reflect.DeepEqual(got, want) {
		t.Fatalf("default digest items = %v, want %v", got, want)
	}
}

func TestJobTranscriptFiltersNarrowAndCombine(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	record := transcriptTestRecord(t, server, "filters")
	start := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	items := []protocol.TranscriptItem{
		transcriptTestItem(1, start, "message"),
		transcriptTestItem(2, start.Add(time.Second), "tool"),
		transcriptTestItem(3, start.Add(2*time.Second), "reasoning"),
		transcriptTestItem(4, start.Add(3*time.Second), "toolResult"),
		transcriptTestItem(5, start.Add(4*time.Second), "message"),
		transcriptTestItem(6, start.Add(5*time.Second), "warning"),
		transcriptTestItem(7, start.Add(6*time.Second), "message"),
		transcriptTestItem(8, start.Add(7*time.Second), "error"),
	}
	writeTranscriptSidecar(t, record, items, false)

	intPointer := func(value int) *int { return &value }
	timePointer := func(value time.Time) *time.Time { return &value }
	for _, test := range []struct {
		name   string
		params protocol.JobTranscriptParams
		want   []int
	}{
		{name: "kinds", params: protocol.JobTranscriptParams{Kinds: []string{"message"}}, want: []int{1, 5, 7}},
		{name: "reasoning kind", params: protocol.JobTranscriptParams{Kinds: []string{"reasoning"}}, want: []int{3}},
		{name: "tool-result kind", params: protocol.JobTranscriptParams{Kinds: []string{"toolResult"}}, want: []int{4}},
		{name: "since", params: protocol.JobTranscriptParams{Since: timePointer(items[1].At)}, want: []int{3, 4, 5, 6, 7, 8}},
		{name: "since ordinal", params: protocol.JobTranscriptParams{SinceOrdinal: intPointer(3)}, want: []int{4, 5, 6, 7, 8}},
		{name: "last", params: protocol.JobTranscriptParams{Last: intPointer(2)}, want: []int{7, 8}},
		{name: "limit", params: protocol.JobTranscriptParams{Limit: intPointer(2)}, want: []int{1, 2}},
		{name: "combined", params: protocol.JobTranscriptParams{
			Kinds:        []string{"message"},
			Since:        timePointer(items[1].At),
			SinceOrdinal: intPointer(2),
			Last:         intPointer(1),
		}, want: []int{7}},
	} {
		test.params.JobID = record.JobID
		result := transcriptResultForTest(t, server, test.params)
		if got := transcriptOrdinals(result.Items); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%s item ordinals = %v, want %v", test.name, got, test.want)
		}
		wantCounts := map[string]int{
			"message":    3,
			"reasoning":  1,
			"tool":       1,
			"toolResult": 1,
			"fileChange": 0,
			"warning":    1,
			"error":      1,
		}
		if result.ItemCount != len(items) || !reflect.DeepEqual(result.Counts, wantCounts) {
			t.Fatalf("%s metadata unexpectedly narrowed = %#v", test.name, result)
		}
	}
}

func TestJobTranscriptReportsGapAndMissingSidecar(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	item := transcriptTestItem(1, time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC), "message")
	normal := transcriptTestRecord(t, server, "gap-normal")
	writeTranscriptSidecar(t, normal, []protocol.TranscriptItem{item}, false)
	gapped := transcriptTestRecord(t, server, "gap-present")
	writeTranscriptSidecar(t, gapped, []protocol.TranscriptItem{item}, true)
	missing := transcriptTestRecord(t, server, "gap-missing")
	unopenable := transcriptTestRecord(t, server, "gap-unopenable")
	unopenablePath, present, err := transcriptSidecarPath(unopenable)
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("unopenable test record has no transcript sidecar path")
	}
	if err := os.MkdirAll(filepath.Dir(unopenablePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(unopenablePath, unopenablePath); err != nil {
		t.Fatal(err)
	}

	appendBytes := func(record jobstore.Record, payload []byte) {
		t.Helper()
		path, present, err := transcriptSidecarPath(record)
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Fatal("test record has no transcript sidecar path")
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(payload); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	decodeFailure := transcriptTestRecord(t, server, "gap-decode-failure")
	writeTranscriptSidecar(t, decodeFailure, []protocol.TranscriptItem{item}, false)
	appendBytes(decodeFailure, []byte(`{"ordinal":`))
	scanFailure := transcriptTestRecord(t, server, "gap-scan-failure")
	writeTranscriptSidecar(t, scanFailure, []protocol.TranscriptItem{item}, false)
	appendBytes(scanFailure, append(bytes.Repeat([]byte("x"), maxTranscriptSidecarLine+1), '\n'))

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	decodeFailure, err = store.MarkTerminal(decodeFailure.JobID, jobstore.TerminalUpdate{
		State:      protocol.PublicStateCompleted,
		Cleanup:    protocol.CleanupClean,
		FinishedAt: item.At.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name          string
		record        jobstore.Record
		params        protocol.JobTranscriptParams
		wantGap       bool
		wantItemCount int
		wantOrdinals  []int
		wantTerminal  bool
	}{
		{name: "clear", record: normal, wantGap: false, wantItemCount: 1, wantOrdinals: []int{1}},
		{name: "present", record: gapped, wantGap: true, wantItemCount: 1, wantOrdinals: []int{1}},
		{name: "missing", record: missing, wantGap: false, wantItemCount: 0, wantOrdinals: []int{}},
		{name: "unopenable", record: unopenable, wantGap: true, wantItemCount: 0, wantOrdinals: []int{}},
		{name: "decode failure keeps prefix", record: decodeFailure, params: protocol.JobTranscriptParams{Kinds: []string{"message"}}, wantGap: true, wantItemCount: 1, wantOrdinals: []int{1}, wantTerminal: true},
		{name: "scan failure keeps prefix", record: scanFailure, wantGap: true, wantItemCount: 1, wantOrdinals: []int{1}},
	} {
		test.params.JobID = test.record.JobID
		result := transcriptResultForTest(t, server, test.params)
		if result.Gap != test.wantGap || result.ItemCount != test.wantItemCount {
			t.Fatalf("%s transcript = %#v", test.name, result)
		}
		if got := transcriptOrdinals(result.Items); !reflect.DeepEqual(got, test.wantOrdinals) {
			t.Fatalf("%s item ordinals = %v, want %v", test.name, got, test.wantOrdinals)
		}
		wantCounts := newTranscriptCounts()
		wantCounts["message"] = test.wantItemCount
		if !reflect.DeepEqual(result.Counts, wantCounts) {
			t.Fatalf("%s counts = %#v, want %#v", test.name, result.Counts, wantCounts)
		}
		if test.wantItemCount == 0 && (result.FirstAt != nil || result.LastAt != nil) {
			t.Fatalf("%s transcript bounds = first:%v last:%v, want none", test.name, result.FirstAt, result.LastAt)
		}
		if test.wantTerminal && result.State != protocol.PublicStateCompleted {
			t.Fatalf("%s transcript state = %q, want completed", test.name, result.State)
		}
	}
}

func TestJobTranscriptReportsGapForDurableSidecarFailure(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	item := transcriptTestItem(1, time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC), "message")
	record := transcriptTestRecord(t, server, "gap-sidecar-failure")
	writeTranscriptSidecar(t, record, []protocol.TranscriptItem{item}, false)

	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkTerminal(record.JobID, jobstore.TerminalUpdate{
		State:       protocol.PublicStateCompleted,
		Cleanup:     protocol.CleanupClean,
		Diagnostics: []string{itemSidecarDiagnosticPrefix + "sync: test failure"},
		FinishedAt:  item.At.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}

	result := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID})
	if !result.Gap || result.ItemCount != 1 {
		t.Fatalf("sidecar failure transcript = %#v, want one item and gap", result)
	}
}

func TestJobTranscriptOrdinalRoundTrip(t *testing.T) {
	server := newTestServer(t, t.TempDir(), Config{})
	record := transcriptTestRecord(t, server, "ordinal")
	start := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	items := []protocol.TranscriptItem{
		transcriptTestItem(41, start, "message"),
		transcriptTestItem(42, start.Add(time.Second), "message"),
	}
	writeTranscriptSidecar(t, record, items, false)

	initial := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID, Kinds: []string{"message"}})
	if got, want := transcriptOrdinals(initial.Items), []int{41, 42}; !reflect.DeepEqual(got, want) {
		t.Fatalf("initial ordinals = %v, want %v", got, want)
	}
	since := initial.Items[0].Ordinal
	continued := transcriptResultForTest(t, server, protocol.JobTranscriptParams{JobID: record.JobID, SinceOrdinal: &since})
	if got, want := transcriptOrdinals(continued.Items), []int{42}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ordinal round trip = %v, want %v", got, want)
	}
}

func transcriptTestRecord(t *testing.T, server *Server, requestID string) jobstore.Record {
	t.Helper()
	store, err := server.ensureJobStore()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(protocol.TaskSpec{Backend: "fake", CWD: t.TempDir(), Prompt: "transcript", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	record, deduplicated, err := store.SubmitTx(
		jobstore.RequestKey{WorkspaceKey: "transcript-workspace", RequestID: requestID},
		raw,
		func(id string) (jobstore.Record, error) {
			return jobstore.Record{JobID: id, Backend: "fake", CWD: t.TempDir(), Write: false}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if deduplicated {
		t.Fatal("transcript test record was unexpectedly deduplicated")
	}
	return record
}

func transcriptTestItem(ordinal int, at time.Time, kind string) protocol.TranscriptItem {
	return protocol.TranscriptItem{Ordinal: ordinal, At: at, Kind: kind, Text: kind}
}

func writeTranscriptSidecar(t *testing.T, record jobstore.Record, items []protocol.TranscriptItem, gap bool) {
	t.Helper()
	path, present, err := transcriptSidecarPath(record)
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("test record has no transcript sidecar path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, item := range items {
		if err := encoder.Encode(item); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if gap {
		if err := encoder.Encode(struct {
			AppendStopped bool `json:"appendStopped"`
		}{AppendStopped: true}); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func transcriptResultForTest(t *testing.T, server *Server, params protocol.JobTranscriptParams) protocol.JobTranscriptResult {
	t.Helper()
	outcome := server.handleJobTranscript(mustJSON(t, params))
	if outcome.err != nil {
		t.Fatalf("job.transcript error = %#v", outcome.err)
	}
	result, ok := outcome.result.(protocol.JobTranscriptResult)
	if !ok {
		t.Fatalf("job.transcript result = %T, want protocol.JobTranscriptResult", outcome.result)
	}
	return result
}

func transcriptOrdinals(items []protocol.TranscriptItem) []int {
	ordinals := make([]int, len(items))
	for index, item := range items {
		ordinals[index] = item.Ordinal
	}
	return ordinals
}
