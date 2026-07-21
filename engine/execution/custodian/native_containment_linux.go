//go:build linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

func newNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian) (nativeContainmentBackend, error) {
	return newRetainedNativeContainmentBackend(ctx, custodian, platformRetainedGroupFactory(custodian.options.newRetainedGroup))
}

func platformRetainedGroupFactory(factory func() (containment.RetainedGroupObject, error)) func() (containment.RetainedGroupObject, error) {
	if factory != nil {
		return factory
	}
	return func() (containment.RetainedGroupObject, error) {
		return cgroup.New("")
	}
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
	required, err := model.ContainmentRequiresRetainedObject(group)
	if err != nil {
		return nil, err
	}
	if !required || real.RetainedObject != nil {
		return real, nil
	}
	manager, err := cgroup.New("")
	if err != nil {
		return nil, err
	}
	capability, err := manager.AcquireRetainedGroupWithoutRootLease(ctx, group, time.Now())
	if err != nil {
		_ = manager.Close()
		return nil, err
	}
	bound := RealContainment{
		Params:         real.Params,
		RetainedObject: boundRetainedGroupObject{capability: capability},
	}
	if witness, ok := capability.(containment.ContinuityWitness); ok {
		bound.Witness = witness
	}
	return bound, nil
}

func prepareNativeRuntimePlatformOptions(options NativeOptions) (NativeOptions, func() error, error) {
	if options.newRetainedGroup != nil {
		return options, nil, nil
	}
	manager, err := cgroup.New("")
	if err != nil {
		return options, nil, err
	}
	options.newRetainedGroup = func() (containment.RetainedGroupObject, error) {
		return manager, nil
	}
	return options, manager.Close, nil
}

func nativeRuntimePlatformSelfTestEnabled() bool {
	return true
}

func nativeRuntimePlatformUnsupportedCause() error {
	return nil
}

func nativeRuntimePlatformUnsupportedError(err error) bool {
	return errors.Is(err, cgroup.ErrUnsupported)
}

type boundRetainedGroupObject struct {
	capability containment.RetainedGroupCapability
}

func (object boundRetainedGroupObject) AcquireRetainedGroup(_ context.Context, target model.GroupRef, _ time.Time) (containment.RetainedGroupCapability, error) {
	if object.capability == nil {
		return nil, fmt.Errorf("%w: bound retained capability is nil", ErrNativeCustodianUnavailable)
	}
	identity := object.capability.Identity()
	if err := identity.KernelDomainID.Validate(); err != nil {
		return nil, err
	}
	if identity.RetainedID != target.RetainedID || !identity.KernelDomainID.ProvablySame(target.KernelDomain()) {
		return nil, fmt.Errorf("%w: bound retained capability identity mismatch", ErrNativeCustodianUnavailable)
	}
	return object.capability, nil
}
