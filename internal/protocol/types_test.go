package protocol

import (
	"encoding/json"
	"testing"
)

func TestDefaultCapabilitiesAndStructuredError(t *testing.T) {
	t.Parallel()
	caps := DefaultCapabilities()
	for _, name := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude", "models.discovery", "models.reported"} {
		if _, ok := caps[name]; !ok {
			t.Fatalf("missing capability %s in %+v", name, caps)
		}
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
