//go:build linux

package custodian

import (
	"context"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

func newNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian) (nativeContainmentBackend, error) {
	factory := custodian.options.newRetainedGroup
	if factory == nil {
		factory = func() (containment.RetainedGroupObject, error) {
			return cgroup.New("")
		}
	}
	return newRetainedNativeContainmentBackend(ctx, custodian, factory)
}

func platformRealContainment(ctx context.Context, real RealContainment, group model.GroupRef) (RealContainment, error) {
	_ = ctx
	required, err := model.ContainmentRequiresRetainedObject(group)
	if err != nil {
		return RealContainment{}, err
	}
	if !required || real.RetainedObject != nil {
		return real, nil
	}
	manager, err := cgroup.New("")
	if err != nil {
		return RealContainment{}, err
	}
	real.RetainedObject = manager
	return real, nil
}

func platformBindContainmentTarget(ctx context.Context, real RealContainment, group model.GroupRef) (parklaunch.Containment, error) {
	return platformRealContainment(ctx, real, group)
}
