//go:build darwin || linux

package containment

import (
	"context"
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestRealSignalerRefusesUnsafePGIDForSignals(t *testing.T) {
	tests := []struct {
		name   string
		pgid   int
		signal Signal
	}{
		{name: "pgid_0_term", pgid: 0, signal: SignalTerminate},
		{name: "pgid_0_kill", pgid: 0, signal: SignalKill},
		{name: "pgid_1_term", pgid: 1, signal: SignalTerminate},
		{name: "pgid_1_kill", pgid: 1, signal: SignalKill},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := unsafePGIDTarget(t, tt.pgid)

			result, err := RealSignaler{}.SignalGroup(context.Background(), target, tt.signal)

			if result != SignalUnprovable {
				t.Fatalf("SignalGroup result = %v, want %v", result, SignalUnprovable)
			}
			if !errors.Is(err, ErrUnsafeSignalTarget) {
				t.Fatalf("SignalGroup error = %v, want ErrUnsafeSignalTarget", err)
			}
		})
	}
}

func TestRealSignalerRefusesUnsafePGIDForProbe(t *testing.T) {
	for _, tt := range []struct {
		name string
		pgid int
	}{
		{name: "pgid_0", pgid: 0},
		{name: "pgid_1", pgid: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			target := unsafePGIDTarget(t, tt.pgid)

			result, err := RealSignaler{}.ProbeGroup(context.Background(), target)

			if result != ProbeUnprovable {
				t.Fatalf("ProbeGroup result = %v, want %v", result, ProbeUnprovable)
			}
			if !errors.Is(err, ErrUnsafeSignalTarget) {
				t.Fatalf("ProbeGroup error = %v, want ErrUnsafeSignalTarget", err)
			}
		})
	}
}

func unsafePGIDTarget(t *testing.T, pgid int) model.GroupRef {
	t.Helper()
	target := testGroupRef(t)
	target.PGID = pgid
	target.Leader.PID = pgid
	return target
}
