package protocol

import "testing"

func TestDefaultCapabilitiesAndStructuredError(t *testing.T) {
	t.Parallel()
	caps := DefaultCapabilities()
	for _, name := range []string{"policy.shape", "policy.jsonSchema", "policy.named", "policy.retry", "nativeStructuredOutput.codex", "nativeStructuredOutput.claude"} {
		if _, ok := caps[name]; !ok {
			t.Fatalf("missing capability %s in %+v", name, caps)
		}
	}
	err := NewError(ErrorVersionMismatch, "protocol major version mismatch", ErrorData{ServerProtocolVersion: Version})
	if err.Code == 0 || err.Data.Code != ErrorVersionMismatch || err.Data.ServerProtocolVersion != Version {
		t.Fatalf("error = %+v", err)
	}
}
