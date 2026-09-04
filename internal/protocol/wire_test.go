package protocol

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestWireGoldenJSONRoundTrip(t *testing.T) {
	requested := int64(1234)
	model := "gpt-5"
	effort := "high"
	empty := ""
	tags := map[string]string{"team": "core"}
	emptyTags := map[string]string{}
	createdAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	startedAt := time.Date(2025, time.January, 2, 3, 5, 5, 0, time.UTC)
	finishedAt := time.Date(2025, time.January, 2, 3, 6, 5, 0, time.UTC)
	updatedAt := time.Date(2025, time.January, 2, 3, 7, 5, 0, time.UTC)
	lastItemAt := time.Date(2025, time.January, 2, 3, 7, 6, 0, time.UTC)
	itemCount := 2
	timeout := &engine.TimeoutResolution{
		Requested: &requested,
		Effective: 2345,
		Source:    engine.TimeoutSourceClient,
	}
	contract := &ContractResult{
		SchemaSHA256: "schema-sha",
		Evaluated:    true,
		Compliant:    true,
		Attempts:     2,
		Violations:   []string{"/answer"},
	}
	result := &ResultInfoWire{
		Text:       "answer",
		ResultPath: "/results/job-1",
		SHA256:     "result-sha",
		Bytes:      42,
	}
	stdoutTruncated := false
	stderrTruncated := false
	logPaths := &LogPathsWire{
		Stdout:          "/logs/job-1.out",
		StdoutTruncated: &stdoutTruncated,
		Stderr:          "/logs/job-1.err",
		StderrTruncated: &stderrTruncated,
	}
	summaryContract := &ContractVerdict{Evaluated: true, Compliant: false}
	record := JobRecordWire{
		JobID:         "job-1",
		WorkspaceKey:  "workspace-1",
		RequestID:     "request-1",
		Backend:       "codex",
		State:         PublicStateCompleted,
		Tags:          map[string]string{"team": "core"},
		CreatedAt:     createdAt,
		StartedAt:     &startedAt,
		FinishedAt:    &finishedAt,
		Timeout:       timeout,
		Result:        result,
		Contract:      contract,
		Failure:       &JobFailureWire{Class: FailureClassBackendError, Reason: "backend stopped"},
		Cleanup:       CleanupUncertain,
		LogPaths:      logPaths,
		ModelReported: "gpt-5",
	}
	summary := JobSummaryWire{
		JobID:         "job-1",
		Backend:       "codex",
		State:         PublicStateFailed,
		Tags:          map[string]string{"team": "core"},
		Cleanup:       CleanupUncertain,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
		ModelReported: "gpt-5",
		FailureClass:  FailureClassBackendError,
		Contract:      summaryContract,
		ItemCount:     &itemCount,
		LastItemAt:    &lastItemAt,
		Liveness:      LivenessAlive,
	}
	taskSpec := TaskSpec{
		Backend:      "codex",
		CWD:          "/workspace",
		Prompt:       "do work",
		Write:        true,
		Model:        &model,
		Effort:       &effort,
		TimeoutMS:    &requested,
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Tags:         &tags,
	}

	const recordJSON = `{"jobId":"job-1","workspaceKey":"workspace-1","requestId":"request-1","backend":"codex","state":"completed","tags":{"team":"core"},"createdAt":"2025-01-02T03:04:05Z","startedAt":"2025-01-02T03:05:05Z","finishedAt":"2025-01-02T03:06:05Z","timeout":{"requested":1234,"effective":2345,"source":"client"},"result":{"text":"answer","resultPath":"/results/job-1","sha256":"result-sha","bytes":42},"contract":{"schemaSha256":"schema-sha","evaluated":true,"compliant":true,"attempts":2,"violations":["/answer"]},"failure":{"class":"backend_error","reason":"backend stopped"},"cleanup":"uncertain","logPaths":{"stdout":"/logs/job-1.out","stdoutTruncated":false,"stderr":"/logs/job-1.err","stderrTruncated":false},"modelReported":"gpt-5"}`
	const summaryJSON = `{"jobId":"job-1","backend":"codex","state":"failed","tags":{"team":"core"},"cleanup":"uncertain","createdAt":"2025-01-02T03:04:05Z","updatedAt":"2025-01-02T03:07:05Z","modelReported":"gpt-5","failureClass":"backend_error","contract":{"evaluated":true,"compliant":false},"itemCount":2,"lastItemAt":"2025-01-02T03:07:06Z","liveness":"alive"}`

	tests := []struct {
		name  string
		value any
		new   func() any
		want  string
	}{
		{
			name:  "PublicState",
			value: PublicStateCompleted,
			new:   func() any { return new(PublicState) },
			want:  `"completed"`,
		},
		{
			name:  "FailureClass",
			value: FailureClassBackendUnavailable,
			new:   func() any { return new(FailureClass) },
			want:  `"backend_unavailable"`,
		},
		{
			name:  "Cleanup",
			value: CleanupUncertain,
			new:   func() any { return new(Cleanup) },
			want:  `"uncertain"`,
		},
		{
			name:  "ContractResult",
			value: *contract,
			new:   func() any { return new(ContractResult) },
			want:  `{"schemaSha256":"schema-sha","evaluated":true,"compliant":true,"attempts":2,"violations":["/answer"]}`,
		},
		{
			name: "TaskSpec supplied-empty optional fields",
			value: TaskSpec{
				Backend: "codex",
				CWD:     "/workspace",
				Prompt:  "do work",
				Write:   true,
				Model:   &empty,
				Effort:  &empty,
				Tags:    &emptyTags,
			},
			new:  func() any { return new(TaskSpec) },
			want: `{"backend":"codex","cwd":"/workspace","prompt":"do work","write":true,"model":"","effort":"","tags":{}}`,
		},
		{
			name: "TaskSpec absent optional fields",
			value: TaskSpec{
				Backend: "codex",
				CWD:     "/workspace",
				Prompt:  "do work",
				Write:   true,
			},
			new:  func() any { return new(TaskSpec) },
			want: `{"backend":"codex","cwd":"/workspace","prompt":"do work","write":true}`,
		},
		{
			name:  "JobSubmitParams",
			value: JobSubmitParams{WorkspaceKey: "workspace-1", RequestID: "request-1", TaskSpec: taskSpec},
			new:   func() any { return new(JobSubmitParams) },
			want:  `{"workspaceKey":"workspace-1","requestId":"request-1","taskSpec":{"backend":"codex","cwd":"/workspace","prompt":"do work","write":true,"model":"gpt-5","effort":"high","timeoutMs":1234,"outputSchema":{"type":"object"},"tags":{"team":"core"}}}`,
		},
		{
			name:  "JobSubmitResult",
			value: JobSubmitResult{JobID: "job-1", State: PublicStateQueued, Deduplicated: true, Timeout: timeout},
			new:   func() any { return new(JobSubmitResult) },
			want:  `{"jobId":"job-1","state":"queued","deduplicated":true,"timeout":{"requested":1234,"effective":2345,"source":"client"}}`,
		},
		{
			name:  "JobGetParams",
			value: JobGetParams{JobID: "job-1"},
			new:   func() any { return new(JobGetParams) },
			want:  `{"jobId":"job-1"}`,
		},
		{
			name:  "JobGet single response",
			value: record,
			new:   func() any { return new(JobRecordWire) },
			want:  recordJSON,
		},
		{
			name:  "JobListParams",
			value: JobListParams{WorkspaceKey: "workspace-1", Tags: tags, States: []PublicState{PublicStateQueued, PublicStateRunning}},
			new:   func() any { return new(JobListParams) },
			want:  `{"workspaceKey":"workspace-1","tags":{"team":"core"},"states":["queued","running"]}`,
		},
		{
			name:  "JobList response",
			value: JobListResult{Jobs: []JobSummaryWire{summary}},
			new:   func() any { return new(JobListResult) },
			want:  `{"jobs":[` + summaryJSON + `]}`,
		},
		{
			name:  "JobCancelParams",
			value: JobCancelParams{JobID: "job-1"},
			new:   func() any { return new(JobCancelParams) },
			want:  `{"jobId":"job-1"}`,
		},
		{
			name:  "JobCancelResult",
			value: JobCancelResult{JobID: "job-1", State: PublicStateCanceled},
			new:   func() any { return new(JobCancelResult) },
			want:  `{"jobId":"job-1","state":"canceled"}`,
		},
		{
			name: "HelloResult",
			value: HelloResult{
				ProtocolVersion: 3,
				BackendMetadata: []BackendInfo{{Name: "codex", Models: []string{"gpt-5"}, Efforts: []string{"high"}}},
			},
			new:  func() any { return new(HelloResult) },
			want: `{"protocolVersion":3,"backends":[{"backend":"codex","models":["gpt-5"],"efforts":["high"]}]}`,
		},
		{
			name:  "JobFailureWire",
			value: JobFailureWire{Class: FailureClassBackendError, Reason: "backend stopped"},
			new:   func() any { return new(JobFailureWire) },
			want:  `{"class":"backend_error","reason":"backend stopped"}`,
		},
		{
			name:  "ResultInfoWire",
			value: *result,
			new:   func() any { return new(ResultInfoWire) },
			want:  `{"text":"answer","resultPath":"/results/job-1","sha256":"result-sha","bytes":42}`,
		},
		{
			name:  "LogPathsWire",
			value: *logPaths,
			new:   func() any { return new(LogPathsWire) },
			want:  `{"stdout":"/logs/job-1.out","stdoutTruncated":false,"stderr":"/logs/job-1.err","stderrTruncated":false}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			assertJSONEquivalent(t, got, []byte(test.want))

			decoded := test.new()
			if err := json.Unmarshal(got, decoded); err != nil {
				t.Fatal(err)
			}
			if got := reflect.ValueOf(decoded).Elem().Interface(); !reflect.DeepEqual(got, test.value) {
				t.Fatalf("round trip = %#v, want %#v", got, test.value)
			}

			valueType := reflect.TypeOf(test.value)
			if valueType.Kind() == reflect.Struct {
				assertZeroValueOmitsOmitEmpty(t, reflect.New(valueType).Elem().Interface())
			}
		})
	}
}

func TestJobRecordWireFieldAllowList(t *testing.T) {
	want := map[string]struct{}{
		"JobID": {}, "WorkspaceKey": {}, "RequestID": {}, "Backend": {},
		"State": {}, "Tags": {}, "CreatedAt": {}, "StartedAt": {},
		"FinishedAt": {}, "Timeout": {}, "Result": {}, "Contract": {},
		"Failure": {}, "Cleanup": {}, "LogPaths": {}, "ModelReported": {},
	}

	recordType := reflect.TypeOf(JobRecordWire{})
	got := make(map[string]struct{}, recordType.NumField())
	for i := 0; i < recordType.NumField(); i++ {
		got[recordType.Field(i).Name] = struct{}{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JobRecordWire field names = %v, want %v", got, want)
	}
}

func TestJobSummaryWireFieldAllowList(t *testing.T) {
	want := map[string]struct{}{
		"JobID": {}, "Backend": {}, "State": {}, "Cleanup": {}, "CreatedAt": {},
		"UpdatedAt": {}, "ModelReported": {}, "FailureClass": {}, "Contract": {},
		"Tags": {}, "ItemCount": {}, "LastItemAt": {}, "Liveness": {},
	}

	summaryType := reflect.TypeOf(JobSummaryWire{})
	got := make(map[string]struct{}, summaryType.NumField())
	for i := 0; i < summaryType.NumField(); i++ {
		got[summaryType.Field(i).Name] = struct{}{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JobSummaryWire field names = %v, want %v", got, want)
	}
}

func TestWireEnumMethods(t *testing.T) {
	t.Run("PublicState.IsTerminal", func(t *testing.T) {
		tests := []struct {
			state PublicState
			want  bool
		}{
			{PublicStateQueued, false},
			{PublicStateRunning, false},
			{PublicStateCompleted, true},
			{PublicStateFailed, true},
			{PublicStateCanceled, true},
			{PublicStateUnknown, true},
			{PublicState("invalid"), false},
		}
		for _, test := range tests {
			if got := test.state.IsTerminal(); got != test.want {
				t.Errorf("%q IsTerminal() = %t, want %t", test.state, got, test.want)
			}
		}
	})

	t.Run("PublicState.Valid", func(t *testing.T) {
		for _, state := range []PublicState{
			PublicStateQueued,
			PublicStateRunning,
			PublicStateCompleted,
			PublicStateFailed,
			PublicStateCanceled,
			PublicStateUnknown,
		} {
			if !state.Valid() {
				t.Errorf("%q Valid() = false, want true", state)
			}
		}
		if PublicState("invalid").Valid() {
			t.Error("invalid PublicState Valid() = true, want false")
		}
	})

	t.Run("Liveness.Valid", func(t *testing.T) {
		for _, liveness := range []Liveness{LivenessAlive, LivenessGone, LivenessUnknown} {
			if !liveness.Valid() {
				t.Errorf("%q Valid() = false, want true", liveness)
			}
		}
		if Liveness("invalid").Valid() {
			t.Error("invalid Liveness Valid() = true, want false")
		}
	})

	t.Run("FailureClass.Valid", func(t *testing.T) {
		tests := []struct {
			class FailureClass
			want  bool
		}{
			{FailureClassBackendUnavailable, true},
			{FailureClassProviderOverloaded, true},
			{FailureClassModelUnavailable, true},
			{FailureClassContentPolicy, true},
			{FailureClassAuthentication, true},
			{FailureClassBackendError, true},
			{FailureClassTimeout, true},
			{FailureClassInterrupted, true},
			{FailureClassInternal, true},
			{FailureClass("invalid"), false},
		}
		for _, test := range tests {
			if got := test.class.Valid(); got != test.want {
				t.Errorf("%q Valid() = %t, want %t", test.class, got, test.want)
			}
		}
	})

	t.Run("Cleanup.Valid", func(t *testing.T) {
		tests := []struct {
			cleanup Cleanup
			want    bool
		}{
			{CleanupClean, true},
			{CleanupUncertain, true},
			{Cleanup("invalid"), false},
		}
		for _, test := range tests {
			if got := test.cleanup.Valid(); got != test.want {
				t.Errorf("%q Valid() = %t, want %t", test.cleanup, got, test.want)
			}
		}
	})
}

func assertJSONEquivalent(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode actual JSON: %v", err)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON value = %#v, want %#v", gotValue, wantValue)
	}
}

func assertZeroValueOmitsOmitEmpty(t *testing.T, value any) {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}

	valueType := reflect.TypeOf(value)
	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		name, options, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" || !strings.Contains(","+options+",", ",omitempty,") {
			continue
		}
		if _, ok := fields[name]; ok {
			t.Errorf("zero %s includes omitempty field %q in %s", valueType, name, encoded)
		}
	}
}
