package model

import (
	"errors"
	"testing"
)

func TestIncompatibleExecutionCapabilitiesErrorCarriesCapabilities(t *testing.T) {
	caps := ExecutionCapabilities{ExternalRunner: true}
	err := IncompatibleExecutionCapabilitiesError{
		Capabilities: caps,
		Reason:       "strict identified admission requires an in-process backend runner",
	}

	if !errors.Is(err, ErrIncompatibleExecutionCapabilities) {
		t.Fatalf("error = %v, want ErrIncompatibleExecutionCapabilities", err)
	}
	if err.Capabilities != caps {
		t.Fatalf("capabilities = %#v, want %#v", err.Capabilities, caps)
	}
	if err.Error() != "incompatible execution capabilities: strict identified admission requires an in-process backend runner" {
		t.Fatalf("error string = %q", err.Error())
	}
}
