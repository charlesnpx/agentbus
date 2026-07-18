//go:build darwin

package custodian

import (
	"context"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

type leaderNativeContainmentBackend struct {
	factory   func(model.GroupRef) (*leaderRetention, error)
	retention *leaderRetention
}

func newNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian) (nativeContainmentBackend, error) {
	if custodian.options.newRetainedGroup != nil {
		return newRetainedNativeContainmentBackend(ctx, custodian, custodian.options.newRetainedGroup)
	}
	factory := custodian.options.newLeaderRetention
	if factory == nil {
		factory = newLeaderRetentionForGroup
	}
	return &leaderNativeContainmentBackend{factory: factory}, nil
}

func (backend *leaderNativeContainmentBackend) retainedID() string {
	return ""
}

func (backend *leaderNativeContainmentBackend) retainLeaderUnreaped() bool {
	return true
}

func (backend *leaderNativeContainmentBackend) beforeMonitorBind(_ context.Context, group model.GroupRef) (model.GroupRef, error) {
	return group, nil
}

func (backend *leaderNativeContainmentBackend) beforeRelease(_ context.Context, group model.GroupRef) error {
	retention, err := backend.factory(group)
	if err != nil {
		return err
	}
	backend.retention = retention
	return nil
}

func (backend *leaderNativeContainmentBackend) witness() containment.ContinuityWitness {
	if backend == nil {
		return nil
	}
	return backend.retention
}

func (backend *leaderNativeContainmentBackend) witnessAcquired() bool {
	return backend != nil && backend.retention != nil
}

func (backend *leaderNativeContainmentBackend) retainedObject() containment.RetainedGroupObject {
	return nil
}

func (backend *leaderNativeContainmentBackend) attachHandle(handle *parklaunch.ParkedHandle) {
	if backend == nil || backend.retention == nil {
		return
	}
	backend.retention.attachHandle(handle)
}

func (backend *leaderNativeContainmentBackend) leaderRetention() *leaderRetention {
	if backend == nil {
		return nil
	}
	return backend.retention
}

func (backend *leaderNativeContainmentBackend) close(context.Context) error {
	if backend == nil || backend.retention == nil {
		return nil
	}
	return backend.retention.close()
}

func platformRealContainment(_ context.Context, real RealContainment, _ model.GroupRef) (RealContainment, error) {
	return real, nil
}

func platformBindContainmentTarget(_ context.Context, real RealContainment, group model.GroupRef) (parklaunch.Containment, error) {
	retention, err := newLeaderRetentionForGroup(group)
	if err != nil {
		return nil, err
	}
	return RealContainment{Params: real.Params, Witness: retention}, nil
}

func probeNativeRuntimePlatform(options NativeOptions) error {
	return probeNativeLeaderContainment(options)
}
