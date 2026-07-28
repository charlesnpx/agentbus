package authority

import (
	"errors"
	"fmt"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

func TestSafetyLatchRepositoryCorruptionScope(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantTrip bool
	}{
		{name: "meta", err: repository.CorruptRecordError("meta", "authority", "checksum"), wantTrip: true},
		{name: "binding", err: repository.CorruptRecordError("binding", "workspace/request", "checksum"), wantTrip: true},
		{name: "safety", err: repository.CorruptRecordError("safety", "job-1", "checksum"), wantTrip: true},
		{name: "tombstone", err: repository.CorruptRecordError("tombstone", "workspace/request", "checksum"), wantTrip: true},
		{name: "terminal", err: repository.CorruptRecordError("terminal", "job-1", "checksum"), wantTrip: true},
		{name: "binding index", err: repository.CorruptRecordError("binding_index", "job-1", "mismatch"), wantTrip: true},
		{name: "structural untyped", err: fmt.Errorf("%w: bbolt structural corruption", repository.ErrCorruptRecord), wantTrip: true},
		{name: "projection only", err: repository.CorruptRecordError("projection", "job-1", "checksum"), wantTrip: false},
		{name: "quarantine only", err: repository.CorruptRecordError("quarantine", "job-1", "checksum"), wantTrip: false},
		{name: "non-corrupt", err: errors.New("physical cleanup unresolved"), wantTrip: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			latch := NewSafetyLatch()
			tripSafetyLatchOnRepositoryCorruption(latch, tt.err)

			reason := latch.Reason()
			if gotTrip := reason != nil; gotTrip != tt.wantTrip {
				t.Fatalf("latch tripped = %t, want %t; reason=%v", gotTrip, tt.wantTrip, reason)
			}
			if tt.wantTrip && !errors.Is(reason, repository.ErrCorruptRecord) {
				t.Fatalf("latch reason = %v, want ErrCorruptRecord", reason)
			}
		})
	}
}
