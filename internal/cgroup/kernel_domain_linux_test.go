//go:build linux

package cgroup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/procgroup"
)

func TestLinuxRootIdentityKernelDomainMatchesProcgroupCurrentDomain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	manager, err := New("")
	if err != nil {
		t.Fatalf("New(\"\") error = %v", err)
	}
	defer manager.Close()

	root, err := manager.fs.RootIdentity(ctx)
	if err != nil {
		if errors.Is(err, ErrUnsupported) {
			t.Skipf("cgroup root identity unavailable: %v", err)
		}
		t.Fatalf("RootIdentity() error = %v", err)
	}
	cgroupDomain, err := root.kernelDomain()
	if err != nil {
		t.Fatalf("RootIdentity.kernelDomain() error = %v", err)
	}
	procgroupDomain, err := procgroup.CurrentKernelDomain()
	if err != nil {
		t.Fatalf("procgroup.CurrentKernelDomain() error = %v", err)
	}

	cgroupKernelOnly := cgroupDomain
	cgroupKernelOnly.RetainedDomainID = ""
	cgroupKernelOnly.RetainedDomainState = model.RetainedDomainNotApplicable
	if !cgroupKernelOnly.ProvablySame(procgroupDomain) {
		t.Fatalf("kernel domains differ: cgroup=%+v procgroup=%+v", cgroupKernelOnly, procgroupDomain)
	}
	if cgroupDomain.RetainedDomainState != model.RetainedDomainKnown {
		t.Fatalf("cgroup retained domain state = %v, want %v", cgroupDomain.RetainedDomainState, model.RetainedDomainKnown)
	}
	if procgroupDomain.RetainedDomainState != model.RetainedDomainNotApplicable {
		t.Fatalf("procgroup retained domain state = %v, want %v", procgroupDomain.RetainedDomainState, model.RetainedDomainNotApplicable)
	}
}
