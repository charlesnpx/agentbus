//go:build darwin || linux

package custodian

import (
	"context"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

type retainedGroupPlacementCapability interface {
	containment.RetainedGroupCapability
	containment.ContinuityWitness
	PlacePID(context.Context, int) error
	Remove(context.Context) error
}

type retainedNativeContainmentBackend struct {
	manager    containment.RetainedGroupObject
	capability retainedGroupPlacementCapability
	identity   containment.RetainedGroupIdentity
	bound      bool
}

func newRetainedNativeContainmentBackend(ctx context.Context, custodian *NativeCustodian, factory func() (containment.RetainedGroupObject, error)) (nativeContainmentBackend, error) {
	manager, err := custodian.sharedRetainedGroup(factory)
	if err != nil {
		return nil, fmt.Errorf("%w: create retained-object manager: %v", ErrNativeCustodianUnavailable, err)
	}
	capability, err := manager.AcquireRetainedGroup(ctx, model.GroupRef{}, time.Now())
	if err != nil {
		return nil, fmt.Errorf("%w: acquire retained object: %v", ErrNativeCustodianUnavailable, err)
	}
	placement, ok := capability.(retainedGroupPlacementCapability)
	if !ok {
		_ = capability.Release()
		return nil, fmt.Errorf("%w: retained object does not support launch placement", ErrNativeCustodianUnavailable)
	}
	identity := placement.Identity()
	if err := identity.KernelDomainID.Validate(); err != nil {
		_ = placement.Release()
		return nil, fmt.Errorf("%w: retained identity: %v", ErrNativeCustodianUnavailable, err)
	}
	if identity.RetainedID == "" || identity.KernelDomainID.RetainedDomainState != model.RetainedDomainKnown {
		_ = placement.Release()
		return nil, fmt.Errorf("%w: retained identity is incomplete", ErrNativeCustodianUnavailable)
	}
	return &retainedNativeContainmentBackend{
		manager:    manager,
		capability: placement,
		identity:   identity,
	}, nil
}

func (backend *retainedNativeContainmentBackend) retainedID() string {
	if backend == nil {
		return ""
	}
	return backend.identity.RetainedID
}

func (backend *retainedNativeContainmentBackend) retainLeaderUnreaped() bool {
	return false
}

func (backend *retainedNativeContainmentBackend) beforeMonitorBind(ctx context.Context, group model.GroupRef) (model.GroupRef, error) {
	if backend == nil || backend.capability == nil {
		return model.GroupRef{}, fmt.Errorf("%w: retained containment backend is nil", ErrNativeCustodianUnavailable)
	}
	if group.RetainedID != backend.identity.RetainedID {
		return model.GroupRef{}, fmt.Errorf("%w: launch retained id %q does not match retained id %q", ErrNativeCustodianUnavailable, group.RetainedID, backend.identity.RetainedID)
	}
	if err := backend.capability.PlacePID(ctx, group.Leader.PID); err != nil {
		return model.GroupRef{}, fmt.Errorf("place parked worker in retained group: %w", err)
	}
	bound := group
	bound.HostBootID = backend.identity.KernelDomainID.HostBootID
	bound.PIDNamespaceID = backend.identity.KernelDomainID.PIDNamespaceID
	bound.PIDNamespaceState = backend.identity.KernelDomainID.PIDNamespaceState
	bound.RetainedDomainID = backend.identity.KernelDomainID.RetainedDomainID
	bound.RetainedDomainState = backend.identity.KernelDomainID.RetainedDomainState
	bound.RetainedID = backend.identity.RetainedID
	if err := bound.Validate(); err != nil {
		return model.GroupRef{}, err
	}
	backend.bound = true
	return bound, nil
}

func (backend *retainedNativeContainmentBackend) beforeRelease(context.Context, model.GroupRef) error {
	if backend == nil || !backend.bound {
		return fmt.Errorf("%w: retained object was not bound before release", ErrNativeCustodianUnavailable)
	}
	return nil
}

func (backend *retainedNativeContainmentBackend) witness() containment.ContinuityWitness {
	if backend == nil || !backend.bound {
		return nil
	}
	return backend.capability
}

func (backend *retainedNativeContainmentBackend) witnessAcquired() bool {
	return backend != nil && backend.bound && backend.capability != nil
}

func (backend *retainedNativeContainmentBackend) retainedObject() containment.RetainedGroupObject {
	if backend == nil {
		return nil
	}
	return backend.manager
}

func (backend *retainedNativeContainmentBackend) attachHandle(*parklaunch.ParkedHandle) {}

func (backend *retainedNativeContainmentBackend) leaderRetention() *leaderRetention {
	return nil
}

func (backend *retainedNativeContainmentBackend) close(ctx context.Context) error {
	if backend == nil || backend.capability == nil {
		return nil
	}
	if membership, err := backend.capability.Membership(ctx); err == nil && membership == containment.RetainedMembershipEmpty {
		_ = backend.capability.Remove(ctx)
	}
	return backend.capability.Release()
}
