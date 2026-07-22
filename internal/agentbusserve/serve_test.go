package agentbusserve

import (
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
)

func TestProductionServedConfigInjectsUnavailableRuntime(t *testing.T) {
	cfg := productionServedConfig(Config{StrictAdmissionRequested: true})
	support := cfg.Runtime.Support()
	if !errors.Is(support.Reason, custodian.ErrSupervisorUnavailable) || support.VerifiedContainment || support.ParkedExec {
		t.Fatalf("runtime support = %+v, want production unavailable runtime", support)
	}
	if cfg.StrictAdmissionRequested {
		t.Fatal("production config preserved strict admission request")
	}
}
