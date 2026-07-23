package agentbusserve

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/served"
)

func TestProductionServedConfigSelectsNativeStrictRuntime(t *testing.T) {
	cfg := productionServedConfig(Config{})
	support := cfg.Runtime.Support()
	if runtime.GOOS == "darwin" {
		if support.Reason == nil || !errors.Is(support.Reason, custodian.ErrNativeRuntimeUnsupported) {
			t.Fatalf("runtime support = %+v, want native runtime unsupported diagnostic", support)
		}
		return
	}
	if support.Reason != nil && errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) {
		t.Fatalf("runtime support = %+v, want native strict runtime rather than generic unavailable runtime", support)
	}
}

func TestProductionStrictServeFailsTypedOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform startup diagnostic")
	}
	err := Serve(context.Background(), Config{
		StateRoot:   t.TempDir(),
		CWD:         t.TempDir(),
		IdleTimeout: -1,
	})
	var diagnostic served.AdmissionSupportDiagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Serve error = %T %v, want AdmissionSupportDiagnostic", err, err)
	}
	if !errors.Is(err, served.ErrAdmissionStrictSupportUnavailable) {
		t.Fatalf("Serve error = %v, want ErrAdmissionStrictSupportUnavailable", err)
	}
	if diagnostic.Assessment.Class == custodian.SupportAvailable {
		t.Fatalf("diagnostic assessment = %+v, want unavailable strict support", diagnostic.Assessment)
	}
}
