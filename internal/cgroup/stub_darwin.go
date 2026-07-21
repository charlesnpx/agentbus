//go:build darwin

package cgroup

import (
	"context"
	"fmt"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/containment"
)

func New(string) (*Manager, error) {
	return newManagerWithFS(unsupportedFS{}, managerOptions{}), nil
}

func NewInheritedRetainedGroupObjectFromLeafFD(int) containment.RetainedGroupObject {
	return unsupportedInheritedLeafObject{}
}

type unsupportedFS struct{}

type unsupportedInheritedLeafObject struct{}

func (unsupportedInheritedLeafObject) AcquireRetainedGroup(context.Context, model.GroupRef, time.Time) (containment.RetainedGroupCapability, error) {
	return nil, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) RootIdentity(context.Context) (RootIdentity, error) {
	return RootIdentity{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) CreateChild(context.Context, string) (cgroupObject, error) {
	return nil, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) Open(context.Context, string) (cgroupObject, error) {
	return nil, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) releaseRootLease() error {
	return nil
}

func (unsupportedFS) Verify(context.Context, cgroupObject) (bool, error) {
	return false, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ProbeFeatures(context.Context, cgroupObject) (CgroupFeatures, error) {
	return CgroupFeatures{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteProcs(context.Context, cgroupObject, int) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadProcs(context.Context, cgroupObject) ([]int, error) {
	return nil, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadEvents(context.Context, cgroupObject) (Events, error) {
	return Events{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteKill(context.Context, cgroupObject) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteFreeze(context.Context, cgroupObject, FreezeState) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadFreeze(context.Context, cgroupObject) (FreezeState, error) {
	return FreezeUnknown, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) Remove(context.Context, cgroupObject) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}
