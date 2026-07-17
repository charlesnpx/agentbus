//go:build darwin

package cgroup

import (
	"context"
	"fmt"
)

func New(string) (*Manager, error) {
	return newManagerWithFS(unsupportedFS{}, managerOptions{}), nil
}

type unsupportedFS struct{}

func (unsupportedFS) RootIdentity(context.Context) (RootIdentity, error) {
	return RootIdentity{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) CreateChild(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) Open(context.Context, string) (ObjectIdentity, error) {
	return ObjectIdentity{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteProcs(context.Context, string, int) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadProcs(context.Context, string) ([]int, error) {
	return nil, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadEvents(context.Context, string) (Events, error) {
	return Events{}, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteKill(context.Context, string) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) WriteFreeze(context.Context, string, FreezeState) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) ReadFreeze(context.Context, string) (FreezeState, error) {
	return FreezeUnknown, fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}

func (unsupportedFS) Remove(context.Context, string) error {
	return fmt.Errorf("%w: cgroup v2 is unavailable on darwin", ErrUnsupported)
}
