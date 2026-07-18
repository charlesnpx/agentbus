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

func probeNativeRuntimePlatform(options NativeOptions) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := probeNativeCgroupRuntime(ctx, nativeCgroupProbeConfig{options: options})
	return err
}

type nativeCgroupProbeConfig struct {
	options           NativeOptions
	startHelper       func(context.Context) (*nativeCgroupProbeHelper, error)
	beforeContainment func(context.Context, *nativeCgroupProbeHelper) error
}

func probeNativeCgroupRuntime(ctx context.Context, config nativeCgroupProbeConfig) (outcome PhysicalOutcome, err error) {
	manager, err := cgroup.New("")
	if err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: cgroup manager probe: %v", ErrNativeCustodianUnavailable, err)
	}
	defer func() {
		if closeErr := manager.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close cgroup manager probe: %v", ErrNativeCustodianUnavailable, closeErr))
		}
	}()
	if support := manager.Probe(ctx); !support.Strict() {
		if support.Reason != nil {
			return PhysicalOutcome{}, fmt.Errorf("%w: cgroup support probe: %v", ErrNativeCustodianUnavailable, support.Reason)
		}
		return PhysicalOutcome{}, fmt.Errorf("%w: cgroup support probe failed: supported=%t runtimeProbePassed=%t degraded=%t", ErrNativeCustodianUnavailable, support.Supported, support.RuntimeProbePassed, support.Degraded)
	}

	probeCustodian := &NativeCustodian{
		options:   config.options,
		running:   make(map[string]*NativeRunningProcess),
		finalized: make(map[string]*NativeRunningProcess),
	}
	backend, err := newRetainedNativeContainmentBackend(ctx, probeCustodian, func() (containment.RetainedGroupObject, error) {
		return manager, nil
	})
	if err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: cgroup containment backend probe: %v", ErrNativeCustodianUnavailable, err)
	}
	defer func() {
		if closeErr := backend.close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: close cgroup containment backend probe: %v", ErrNativeCustodianUnavailable, closeErr))
		}
	}()

	startHelper := config.startHelper
	if startHelper == nil {
		startHelper = startNativeCgroupProbeHelper
	}
	helper, err := startHelper(ctx)
	if err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: start cgroup containment probe helper: %v", ErrNativeCustodianUnavailable, err)
	}
	defer helper.cleanup()

	group, err := probeGroupRef(ctx, helper.pid())
	if err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: identify cgroup containment probe helper: %v", ErrNativeCustodianUnavailable, err)
	}
	group.RetainedID = backend.retainedID()
	bound, err := backend.beforeMonitorBind(ctx, group)
	if err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: bind cgroup containment probe helper: %v", ErrNativeCustodianUnavailable, err)
	}
	if err := backend.beforeRelease(ctx, bound); err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: release cgroup containment probe helper: %v", ErrNativeCustodianUnavailable, err)
	}
	if !backend.witnessAcquired() {
		return PhysicalOutcome{}, fmt.Errorf("%w: cgroup containment probe witness was not acquired", ErrNativeCustodianUnavailable)
	}
	if config.beforeContainment != nil {
		if err := config.beforeContainment(ctx, helper); err != nil {
			return PhysicalOutcome{}, fmt.Errorf("%w: cgroup containment probe setup hook: %v", ErrNativeCustodianUnavailable, err)
		}
	}
	if err := verifyNativeCgroupProbeLive(ctx, backend.retainedObject(), bound, helper); err != nil {
		return PhysicalOutcome{}, fmt.Errorf("%w: verify cgroup containment probe live membership: %v", ErrNativeCustodianUnavailable, err)
	}

	outcome = containPhysical(ctx, bound, nativeProbeContainmentParams(config.options.ContainmentParams), backend.witness(), backend.retainedObject())
	if !outcome.Absent() {
		if outcome.Err != nil {
			return outcome, fmt.Errorf("%w: cgroup containment probe %s: %v", ErrNativeCustodianUnavailable, outcome.Reason, outcome.Err)
		}
		return outcome, fmt.Errorf("%w: cgroup containment probe outcome=%s reason=%s", ErrNativeCustodianUnavailable, outcome.Kind, outcome.Reason)
	}
	if err := helper.wait(ctx, 2*time.Second); err != nil {
		return outcome, fmt.Errorf("%w: wait cgroup containment probe helper: %v", ErrNativeCustodianUnavailable, err)
	}
	if err := helper.requireGone(ctx); err != nil {
		return outcome, fmt.Errorf("%w: verify cgroup containment probe helper gone: %v", ErrNativeCustodianUnavailable, err)
	}
	if err := verifyNativeCgroupProbeAbsent(ctx, backend.retainedObject(), bound); err != nil {
		return outcome, fmt.Errorf("%w: verify cgroup containment probe absence: %v", ErrNativeCustodianUnavailable, err)
	}
	return outcome, nil
}

func nativeProbeContainmentParams(params containment.Params) containment.Params {
	if params.GracePeriod == 0 {
		params.GracePeriod = 20 * time.Millisecond
	}
	if params.PollInterval == 0 {
		params.PollInterval = 20 * time.Millisecond
	}
	if params.PollTimeout == 0 {
		params.PollTimeout = 2 * time.Second
	}
	return params
}

func verifyNativeCgroupProbeLive(ctx context.Context, retainedObject containment.RetainedGroupObject, group model.GroupRef, helper *nativeCgroupProbeHelper) error {
	if err := helper.requireRunning(ctx); err != nil {
		return err
	}
	if retainedObject == nil {
		return fmt.Errorf("retained object is nil")
	}
	capability, err := retainedObject.AcquireRetainedGroup(ctx, group, time.Now())
	if err != nil {
		return err
	}
	defer capability.Release()
	membership, err := capability.Membership(ctx)
	if err != nil {
		return err
	}
	if membership != containment.RetainedMembershipPresent {
		return fmt.Errorf("retained membership = %v, want present", membership)
	}
	held, err := capability.StillHeld(ctx)
	if err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("retained capability is no longer held")
	}
	return helper.requireRunning(ctx)
}

func verifyNativeCgroupProbeAbsent(ctx context.Context, retainedObject containment.RetainedGroupObject, group model.GroupRef) error {
	if retainedObject == nil {
		return fmt.Errorf("retained object is nil")
	}
	capability, err := retainedObject.AcquireRetainedGroup(ctx, group, time.Now())
	if err != nil {
		return err
	}
	defer capability.Release()
	membership, err := capability.Membership(ctx)
	if err != nil {
		return err
	}
	if membership != containment.RetainedMembershipEmpty {
		return fmt.Errorf("retained membership = %v, want empty", membership)
	}
	held, err := capability.StillHeld(ctx)
	if err != nil {
		return err
	}
	if !held {
		return fmt.Errorf("retained capability is no longer held")
	}
	return nil
}
