package protocol

import (
	"encoding/json"
	"testing"
)

func TestDefaultCapabilitiesAndStructuredError(t *testing.T) {
	t.Parallel()
	caps := DefaultCapabilities()
	for _, name := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude", "models.discovery"} {
		if _, ok := caps[name]; !ok {
			t.Fatalf("missing capability %s in %+v", name, caps)
		}
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
