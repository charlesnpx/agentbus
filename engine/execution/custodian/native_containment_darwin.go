//go:build darwin

package custodian

import (
	"context"
	"fmt"

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
		return newRetainedNativeContainmentBackend(ctx, custodian, platformRetainedGroupFactory(custodian.options.newRetainedGroup))
	}
	factory := custodian.options.newLeaderRetention
	if factory == nil {
		factory = newLeaderRetentionForGroup
	}
	return &leaderNativeContainmentBackend{factory: factory}, nil
}

func platformRetainedGroupFactory(factory func() (containment.RetainedGroupObject, error)) func() (containment.RetainedGroupObject, error) {
	return factory
}

func (backend *leaderNativeContainmentBackend) retainedID() string {
	return ""
}

func (backend *leaderNativeContainmentBackend) retainLeaderUnreaped() bool {
	return true
}

func (backend *leaderNativeContainmentBackend) beforeMonitorBind(_ context.Context, group model.GroupRef) (model.GroupRef, error) {
	if backend == nil || backend.factory == nil {
		return model.GroupRef{}, fmt.Errorf("%w: leader containment backend is nil", ErrNativeCustodianUnavailable)
	}
	backend.retention = nil
	retention, err := backend.factory(group)
	if err != nil {
		return model.GroupRef{}, err
	}
	backend.retention = retention
	return group, nil
}

func (backend *leaderNativeContainmentBackend) beforeRelease(_ context.Context, group model.GroupRef) error {
	if backend == nil || backend.retention == nil {
		return fmt.Errorf("%w: leader retention was not acquired before release", ErrNativeCustodianUnavailable)
	}
	if !backend.retention.group.Equal(group) {
		return fmt.Errorf("%w: leader retention group mismatch before release", ErrNativeCustodianUnavailable)
	}
	return nil
}

func (backend *leaderNativeContainmentBackend) witness() containment.ContinuityWitness {
	if backend == nil || backend.retention == nil {
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
