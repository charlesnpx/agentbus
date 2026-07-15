//go:build darwin || linux

package containment

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

type RealSignaler struct{}

var ErrUnsafeSignalTarget = errors.New("unsafe process group signal target")

func (RealSignaler) SignalGroup(ctx context.Context, target model.GroupRef, signal Signal) (SignalResult, error) {
	if err := ctx.Err(); err != nil {
		return SignalUnprovable, err
	}
	nativeTarget, err := signalTarget(target)
	if err != nil {
		return SignalUnprovable, err
	}
	native, err := nativeSignal(signal)
	if err != nil {
		return SignalUnprovable, err
	}
	if err := unix.Kill(nativeTarget, native); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return SignalTargetAbsent, nil
		}
		return SignalUnprovable, err
	}
	return SignalDelivered, nil
}

func (RealSignaler) ProbeGroup(ctx context.Context, target model.GroupRef) (ProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return ProbeUnprovable, err
	}
	nativeTarget, err := signalTarget(target)
	if err != nil {
		return ProbeUnprovable, err
	}
	if err := unix.Kill(nativeTarget, syscall.Signal(0)); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return ProbeAbsent, nil
		}
		return ProbeUnprovable, err
	}
	return ProbeLive, nil
}

func signalTarget(target model.GroupRef) (int, error) {
	if target.PGID <= 1 {
		return 0, fmt.Errorf("%w: pgid must be greater than 1", ErrUnsafeSignalTarget)
	}
	if err := target.Validate(); err != nil {
		return 0, err
	}
	nativeTarget := -target.PGID
	if nativeTarget >= 0 || nativeTarget == -1 {
		return 0, fmt.Errorf("%w: pgid %d resolves to target %d", ErrUnsafeSignalTarget, target.PGID, nativeTarget)
	}
	return nativeTarget, nil
}

func nativeSignal(signal Signal) (syscall.Signal, error) {
	switch signal {
	case SignalTerminate:
		return unix.SIGTERM, nil
	case SignalKill:
		return unix.SIGKILL, nil
	default:
		return 0, fmt.Errorf("unknown signal %d", signal)
	}
}
