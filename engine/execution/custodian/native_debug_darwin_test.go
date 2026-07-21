//go:build darwin

package custodian

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func TestDarwinLeaderRetentionAcquiredDuringMonitorBind(t *testing.T) {
	group := testPhysicalQuiescence().Group
	acquireCalls := 0
	backend := &leaderNativeContainmentBackend{
		factory: func(g model.GroupRef) (*leaderRetention, error) {
			acquireCalls++
			if !g.Equal(group) {
				t.Fatalf("retention factory group = %+v, want %+v", g, group)
			}
			return &leaderRetention{group: g, heldSince: time.Now()}, nil
		},
	}
	launchContainment := &nativeLaunchContainment{}

	bound, err := backend.beforeMonitorBind(context.Background(), group)
	if err != nil {
		t.Fatalf("beforeMonitorBind() error = %v", err)
	}
	if !bound.Equal(group) {
		t.Fatalf("beforeMonitorBind() group = %+v, want %+v", bound, group)
	}
	if !backend.witnessAcquired() || backend.witness() == nil {
		t.Fatal("beforeMonitorBind() did not acquire leader-retention witness")
	}
	if witness := backend.witness(); witness != nil {
		launchContainment.setWitness(witness)
	}
	launchContainment.mu.RLock()
	syncedWitness := launchContainment.witness
	launchContainment.mu.RUnlock()
	if syncedWitness == nil || syncedWitness != backend.witness() {
		t.Fatalf("launch containment witness = %T, want acquired leader retention", syncedWitness)
	}
	if err := backend.beforeRelease(context.Background(), bound); err != nil {
		t.Fatalf("beforeRelease() error = %v", err)
	}
	if acquireCalls != 1 {
		t.Fatalf("retention acquisitions = %d, want exactly one during beforeMonitorBind", acquireCalls)
	}
}

func TestDarwinLeaderRetentionFailureStopsBeforeMonitorBind(t *testing.T) {
	acquireErr := errors.New("retention acquisition failed")
	backend := &leaderNativeContainmentBackend{
		factory: func(model.GroupRef) (*leaderRetention, error) {
			return nil, acquireErr
		},
	}

	if _, err := backend.beforeMonitorBind(context.Background(), testPhysicalQuiescence().Group); !errors.Is(err, acquireErr) {
		t.Fatalf("beforeMonitorBind() error = %v, want acquisition failure", err)
	}
	if backend.witnessAcquired() || backend.witness() != nil {
		t.Fatal("failed beforeMonitorBind() published a leader-retention witness")
	}
	if err := backend.beforeRelease(context.Background(), testPhysicalQuiescence().Group); !errors.Is(err, ErrNativeCustodianUnavailable) {
		t.Fatalf("beforeRelease() after failed bind error = %v, want unavailable assertion", err)
	}
}

func debugGroupMembers(group model.GroupRef) string {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", group.PGID)
	if err != nil {
		return err.Error()
	}
	var parts []string
	for _, process := range processes {
		parts = append(parts, fmt.Sprintf("pid=%d stat=%d", process.Proc.P_pid, process.Proc.P_stat))
	}
	return strings.Join(parts, ",")
}
