package agentbusserve

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/served"
)

func TestProductionServedConfigDefaultsToUnavailableRuntime(t *testing.T) {
	cfg := productionServedConfig(Config{})
	support := cfg.Runtime.Support()
	if !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) || support.VerifiedContainment || support.ParkedExec {
		t.Fatalf("runtime support = %+v, want production unavailable runtime", support)
	}
	if cfg.StrictAdmissionRequested {
		t.Fatal("production config preserved strict admission request")
	}
}

func TestProductionServedConfigStrictRequestSelectsNativeStrictRuntime(t *testing.T) {
	cfg := productionServedConfig(Config{StrictAdmissionRequested: true})
	if !cfg.StrictAdmissionRequested {
		t.Fatal("production config cleared explicit strict admission request")
	}
	if runtime.GOOS == "darwin" {
		support := cfg.Runtime.Support()
		if support.Reason == nil || !errors.Is(support.Reason, custodian.ErrNativeRuntimeUnsupported) {
			t.Fatalf("runtime support = %+v, want native runtime unsupported diagnostic", support)
		}
	}
}

func TestProductionStrictServeFailsTypedOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("darwin unsupported-platform startup diagnostic")
	}
	err := Serve(context.Background(), Config{
		StateRoot:                t.TempDir(),
		CWD:                      t.TempDir(),
		StrictAdmissionRequested: true,
		IdleTimeout:              -1,
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
