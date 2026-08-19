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
	createdAt := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	startedAt := time.Date(2025, time.January, 2, 3, 5, 5, 0, time.UTC)
	finishedAt := time.Date(2025, time.January, 2, 3, 6, 5, 0, time.UTC)
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
		Text:          "answer",
		TextElided:    true,
		ResultPath:    "/results/job-1",
		SHA256:        "result-sha",
		Bytes:         42,
		ModelReported: "gpt-5",
	}
	logPaths := &LogPathsWire{Stdout: "/logs/job-1.out", Stderr: "/logs/job-1.err"}
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
		JobID:     "job-1",
		State:     PublicStateCompleted,
		Backend:   "codex",
		Cleanup:   CleanupUncertain,
		CreatedAt: createdAt,
	}
	taskSpec := TaskSpecV3{
		Backend:      "codex",
		CWD:          "/workspace",
		Prompt:       "do work",
		Write:        true,
		Model:        "gpt-5",
		Effort:       "high",
		TimeoutMS:    &requested,
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Tags:         map[string]string{"team": "core"},
	}

	const recordJSON = `{"jobId":"job-1","workspaceKey":"workspace-1","requestId":"request-1","backend":"codex","state":"completed","tags":{"team":"core"},"createdAt":"2025-01-02T03:04:05Z","startedAt":"2025-01-02T03:05:05Z","finishedAt":"2025-01-02T03:06:05Z","timeout":{"requested":1234,"effective":2345,"source":"client"},"result":{"text":"answer","textElided":true,"resultPath":"/results/job-1","sha256":"result-sha","bytes":42,"modelReported":"gpt-5"},"contract":{"schemaSha256":"schema-sha","evaluated":true,"compliant":true,"attempts":2,"violations":["/answer"]},"failure":{"class":"backend_error","reason":"backend stopped"},"cleanup":"uncertain","logPaths":{"stdout":"/logs/job-1.out","stderr":"/logs/job-1.err"},"modelReported":"gpt-5"}`
	const summaryJSON = `{"jobId":"job-1","state":"completed","backend":"codex","cleanup":"uncertain","createdAt":"2025-01-02T03:04:05Z"}`

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
			name:  "TaskSpecV3",
			value: taskSpec,
			new:   func() any { return new(TaskSpecV3) },
			want:  `{"backend":"codex","cwd":"/workspace","prompt":"do work","write":true,"model":"gpt-5","effort":"high","timeoutMs":1234,"outputSchema":{"type":"object"},"tags":{"team":"core"}}`,
		},
		{
			name:  "JobSubmitParamsV3",
			value: JobSubmitParamsV3{WorkspaceKey: "workspace-1", RequestID: "request-1", TaskSpec: taskSpec},
			new:   func() any { return new(JobSubmitParamsV3) },
			want:  `{"workspaceKey":"workspace-1","requestId":"request-1","taskSpec":{"backend":"codex","cwd":"/workspace","prompt":"do work","write":true,"model":"gpt-5","effort":"high","timeoutMs":1234,"outputSchema":{"type":"object"},"tags":{"team":"core"}}}`,
		},
		{
			name:  "JobSubmitResultV3",
			value: JobSubmitResultV3{JobID: "job-1", State: PublicStateQueued, Deduplicated: true, Timeout: timeout},
			new:   func() any { return new(JobSubmitResultV3) },
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
			name:  "JobGet list response",
			value: JobGetListResult{Jobs: []JobSummaryWire{summary}},
			new:   func() any { return new(JobGetListResult) },
			want:  `{"jobs":[` + summaryJSON + `]}`,
		},
		{
			name:  "JobCancelParamsV3",
			value: JobCancelParamsV3{JobID: "job-1"},
			new:   func() any { return new(JobCancelParamsV3) },
			want:  `{"jobId":"job-1"}`,
		},
		{
			name:  "JobCancelResultV3",
			value: JobCancelResultV3{JobID: "job-1", State: PublicStateCanceled},
			new:   func() any { return new(JobCancelResultV3) },
			want:  `{"jobId":"job-1","state":"canceled"}`,
		},
		{
			name: "HelloResultV3",
			value: HelloResultV3{
				ProtocolVersion: 3,
				Backends:        []string{"codex"},
				BackendMetadata: []BackendInfo{{Backend: "codex", Models: []string{"gpt-5"}, Efforts: []string{"high"}}},
			},
			new:  func() any { return new(HelloResultV3) },
			want: `{"protocolVersion":3,"backends":["codex"],"backendMetadata":[{"backend":"codex","models":["gpt-5"],"efforts":["high"]}]}`,
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
			want:  `{"text":"answer","textElided":true,"resultPath":"/results/job-1","sha256":"result-sha","bytes":42,"modelReported":"gpt-5"}`,
		},
		{
			name:  "LogPathsWire",
			value: *logPaths,
			new:   func() any { return new(LogPathsWire) },
			want:  `{"stdout":"/logs/job-1.out","stderr":"/logs/job-1.err"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("JSON = %s, want %s", got, test.want)
			}

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
		"JobID": {}, "State": {}, "Backend": {}, "Cleanup": {}, "CreatedAt": {},
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
