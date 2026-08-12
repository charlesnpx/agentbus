package protocol

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestDefaultCapabilitiesAndStructuredError(t *testing.T) {
	t.Parallel()
	caps := DefaultCapabilities()
	for _, name := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude", "models.discovery", "models.reported"} {
		if _, ok := caps[name]; !ok {
			t.Fatalf("missing capability %s in %+v", name, caps)
		}
	}
	if caps["policy.shape"] {
		t.Fatalf("policy.shape advertised as enabled: %+v", caps)
	}
	if _, ok := caps["jobs.requestId"]; ok {
		t.Fatalf("jobs.requestId capability is advertised: %+v", caps)
	}
	err := NewError(ErrorVersionMismatch, "protocol major version mismatch", ErrorData{ServerProtocolVersion: Version})
	if err.Code == 0 || err.Data.Code != ErrorVersionMismatch || err.Data.ServerProtocolVersion != Version {
		t.Fatalf("error = %+v", err)
	}
}

func TestHelloAdditiveFieldsAreIgnoredByLegacyClients(t *testing.T) {
	raw := []byte(`{"protocolVersion":1,"backends":["codex"],"backendMetadata":[{"backend":"codex","models":["gpt-5"],"efforts":["high"]}],"capabilities":{"models.discovery":true}}`)
	var legacy struct {
		ProtocolVersion int             `json:"protocolVersion"`
		Backends        []string        `json:"backends"`
		Capabilities    map[string]bool `json:"capabilities"`
	}
	if err := json.Unmarshal(raw, &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.ProtocolVersion != 1 || len(legacy.Backends) != 1 || !legacy.Capabilities["models.discovery"] {
		t.Fatalf("legacy=%+v", legacy)
	}
}

func TestJobSubmitRequestIdentityFieldsAreAdditive(t *testing.T) {
	raw, err := json.Marshal(JobSubmitParams{
		WorkspaceKey: "workspace-a",
		RequestID:    "request-a",
		TaskSpec:     TaskSpec{Backend: "fake", CWD: "/tmp/work", Write: false, Prompt: "run"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded JobSubmitParams
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.WorkspaceKey != "workspace-a" || decoded.RequestID != "request-a" || decoded.TaskSpec.Prompt != "run" {
		t.Fatalf("decoded = %+v", decoded)
	}

	legacy, err := json.Marshal(JobSubmitResult{JobID: "job-1", State: "queued"})
	if err != nil {
		t.Fatal(err)
	}
	if string(legacy) != `{"jobId":"job-1","state":"queued"}` {
		t.Fatalf("legacy result JSON = %s", legacy)
	}
	deduped, err := json.Marshal(JobSubmitResult{JobID: "job-1", State: "queued", Deduplicated: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(deduped) != `{"jobId":"job-1","state":"queued","deduplicated":true}` {
		t.Fatalf("deduplicated result JSON = %s", deduped)
	}
}

func TestJobTimeoutResolutionFieldsAreAdditive(t *testing.T) {
	requested := int64(45_000)
	timeout := &engine.TimeoutResolution{
		Requested: &requested,
		Effective: requested,
		Source:    engine.TimeoutSourceClient,
	}
	for name, value := range map[string]any{
		"submit": JobSubmitResult{JobID: "job-1", State: engine.StateQueued, Timeout: timeout},
		"status": JobStatus{JobID: "job-1", State: engine.StateCompleted, Timeout: timeout},
		"result": JobResult{JobID: "job-1", State: engine.StateCompleted, Timeout: timeout},
	} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatalf("%s decode fields: %v", name, err)
		}
		timeoutRaw, ok := fields["timeout"]
		if !ok {
			t.Fatalf("%s JSON = %s, missing timeout", name, raw)
		}
		var got engine.TimeoutResolution
		if err := json.Unmarshal(timeoutRaw, &got); err != nil {
			t.Fatalf("%s decode timeout: %v", name, err)
		}
		if got.Requested == nil || *got.Requested != requested || got.Effective != requested || got.Source != engine.TimeoutSourceClient {
			t.Fatalf("%s timeout = %+v, want requested/effective/source client", name, got)
		}
	}
}

func TestJobStatusAndResultCleanupDispositionFieldsAreAdditive(t *testing.T) {
	status, err := json.Marshal(JobStatus{JobID: "job-1", State: engine.StateCompleted, CleanupDisposition: "verified_absent"})
	if err != nil {
		t.Fatal(err)
	}
	var statusFields map[string]json.RawMessage
	if err := json.Unmarshal(status, &statusFields); err != nil {
		t.Fatal(err)
	}
	if got := string(statusFields["cleanupDisposition"]); got != `"verified_absent"` {
		t.Fatalf("status cleanupDisposition = %s in %s", got, status)
	}

	result, err := json.Marshal(JobResult{JobID: "job-1", State: engine.StateCompleted, CleanupDisposition: "unresolved"})
	if err != nil {
		t.Fatal(err)
	}
	var resultFields map[string]json.RawMessage
	if err := json.Unmarshal(result, &resultFields); err != nil {
		t.Fatal(err)
	}
	if got := string(resultFields["cleanupDisposition"]); got != `"unresolved"` {
		t.Fatalf("result cleanupDisposition = %s in %s", got, result)
	}
}

func TestJobStatusAndResultCancellationMetadataFieldsAreAdditive(t *testing.T) {
	status, err := json.Marshal(JobStatus{
		JobID:              "job-1",
		State:              engine.StateCanceled,
		CancellationOrigin: engine.CancellationOriginClientRequest,
		CancellationReason: "client requested cancellation",
	})
	if err != nil {
		t.Fatal(err)
	}
	var statusFields map[string]json.RawMessage
	if err := json.Unmarshal(status, &statusFields); err != nil {
		t.Fatal(err)
	}
	if got := string(statusFields["cancellationOrigin"]); got != `"client_request"` {
		t.Fatalf("status cancellationOrigin = %s in %s", got, status)
	}
	if got := string(statusFields["cancellationReason"]); got != `"client requested cancellation"` {
		t.Fatalf("status cancellationReason = %s in %s", got, status)
	}

	result, err := json.Marshal(JobResult{
		JobID:              "job-1",
		State:              engine.StateCanceled,
		CancellationOrigin: engine.CancellationOriginDaemonShutdown,
		CancellationReason: "daemon shutdown requested cancellation",
	})
	if err != nil {
		t.Fatal(err)
	}
	var resultFields map[string]json.RawMessage
	if err := json.Unmarshal(result, &resultFields); err != nil {
		t.Fatal(err)
	}
	if got := string(resultFields["cancellationOrigin"]); got != `"daemon_shutdown"` {
		t.Fatalf("result cancellationOrigin = %s in %s", got, result)
	}
	if got := string(resultFields["cancellationReason"]); got != `"daemon shutdown requested cancellation"` {
		t.Fatalf("result cancellationReason = %s in %s", got, result)
	}
}

func TestJobStatusAndResultFinalAttemptTimingFieldsAreAdditive(t *testing.T) {
	startedAt := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(7 * time.Second)
	status, err := json.Marshal(JobStatus{
		JobID:                 "job-1",
		State:                 engine.StateCompleted,
		FinalAttemptStartedAt: &startedAt,
		FinalAttemptEndedAt:   &endedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	var statusFields map[string]json.RawMessage
	if err := json.Unmarshal(status, &statusFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := statusFields["finalAttemptStartedAt"]; !ok {
		t.Fatalf("status JSON = %s, missing finalAttemptStartedAt", status)
	}
	if _, ok := statusFields["finalAttemptEndedAt"]; !ok {
		t.Fatalf("status JSON = %s, missing finalAttemptEndedAt", status)
	}

	result, err := json.Marshal(JobResult{
		JobID:                 "job-1",
		State:                 engine.StateCompleted,
		FinalAttemptStartedAt: &startedAt,
		FinalAttemptEndedAt:   &endedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	var resultFields map[string]json.RawMessage
	if err := json.Unmarshal(result, &resultFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := resultFields["finalAttemptStartedAt"]; !ok {
		t.Fatalf("result JSON = %s, missing finalAttemptStartedAt", result)
	}
	if _, ok := resultFields["finalAttemptEndedAt"]; !ok {
		t.Fatalf("result JSON = %s, missing finalAttemptEndedAt", result)
	}

	legacy, err := json.Marshal(JobStatus{JobID: "job-1", State: engine.StateCompleted})
	if err != nil {
		t.Fatal(err)
	}
	statusFields = nil
	if err := json.Unmarshal(legacy, &statusFields); err != nil {
		t.Fatal(err)
	}
	if _, ok := statusFields["finalAttemptStartedAt"]; ok {
		t.Fatalf("legacy status JSON = %s, unexpectedly has finalAttemptStartedAt", legacy)
	}
	if _, ok := statusFields["finalAttemptEndedAt"]; ok {
		t.Fatalf("legacy status JSON = %s, unexpectedly has finalAttemptEndedAt", legacy)
	}
}

func TestJobStatusOmitsEmptyLogPaths(t *testing.T) {
	status, err := json.Marshal(JobStatus{JobID: "job-1", State: engine.StateCompleted})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(status, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["logPaths"]; ok {
		t.Fatalf("status JSON = %s, unexpectedly contains empty logPaths", status)
	}

	paths := &engine.LogPaths{Stdout: "/state/logs/job-1.stdout.log"}
	status, err = json.Marshal(JobStatus{JobID: "job-1", State: engine.StateFailed, LogPaths: paths})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(status, &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["logPaths"]; !ok {
		t.Fatalf("status JSON = %s, missing populated logPaths", status)
	}
}
