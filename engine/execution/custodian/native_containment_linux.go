//go:build linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/cgroup"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

func newNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian) (nativeContainmentBackend, error) {
	backend, err := newRetainedNativeContainmentBackend(ctx, custodian, platformRetainedGroupFactory(custodian.options.newRetainedGroup))
	if err == nil {
		return backend, nil
	}
	if !retainedContainmentFallbackAllowed(err) {
		return nil, err
	}
	return newLeaderNativeContainmentBackend(custodian.options.newLeaderRetention), nil
}

func retainedContainmentFallbackAllowed(err error) bool {
	if err == nil || errors.Is(err, cgroup.ErrRootLeaseUnavailable) {
		return false
	}
	return errors.Is(err, cgroup.ErrUnsupported) || errors.Is(err, ErrRetainedContainmentUnavailable)
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
	if !required {
		return real, nil
	}
	if real.RetainedObject == nil {
		return nil, fmt.Errorf("%w: retained monitor leaf capability is missing", ErrNativeCustodianUnavailable)
	}
	capability, err := real.RetainedObject.AcquireRetainedGroup(ctx, group, time.Now())
	if err != nil {
		return nil, err
	}
	bound := RealContainment{
		Params:                      real.Params,
		Witness:                     real.Witness,
		RetainedObject:              boundRetainedGroupObject{capability: capability},
		TolerateUnleasedCleanupSkip: real.TolerateUnleasedCleanupSkip,
	}
	if witness, ok := capability.(containment.ContinuityWitness); ok {
		bound.Witness = witness
	}
	return bound, nil
}

func prepareNativeRuntimePlatformOptions(options NativeOptions) (NativeOptions, func() error, error) {
	return options, nil, nil
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

func nativeRuntimePlatformSelfTestQuiescenceMethod() model.QuiescenceMethod {
	return model.QuiescenceTermKill
}

type leaderNativeContainmentBackend struct {
	factory   func(model.GroupRef) (*leaderRetention, error)
	retention *leaderRetention
}

func newLeaderNativeContainmentBackend(factory func(model.GroupRef) (*leaderRetention, error)) *leaderNativeContainmentBackend {
	if factory == nil {
		factory = newLeaderRetentionForGroup
	}
	return &leaderNativeContainmentBackend{factory: factory}
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

func (backend *leaderNativeContainmentBackend) monitorLeafFile(context.Context) (*os.File, error) {
	return nil, nil
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
