//go:build darwin

package cgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestDarwinNewFailsClosed(t *testing.T) {
	manager, err := New("")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	support := manager.Probe(context.Background())
	if support.Supported || support.RuntimeProbePassed || support.Reason == nil || !errors.Is(support.Reason, ErrUnsupported) {
		t.Fatalf("Probe() support = %#v, want unsupported", support)
	}
	if capability, err := manager.AcquireRetainedGroup(context.Background(), model.GroupRef{}, time.Now()); capability != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("AcquireRetainedGroup() = %T, %v; want nil ErrUnsupported", capability, err)
	}
}
