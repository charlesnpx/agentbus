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

func (RealSignaler) SignalGroup(ctx context.Context, target model.GroupRef, signal Signal) (SignalResult, error) {
	if err := ctx.Err(); err != nil {
		return SignalUnprovable, err
	}
	if err := target.Validate(); err != nil {
		return SignalUnprovable, err
	}
	native, err := nativeSignal(signal)
	if err != nil {
		return SignalUnprovable, err
	}
	if err := unix.Kill(-target.PGID, native); err != nil {
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
	if err := target.Validate(); err != nil {
		return ProbeUnprovable, err
	}
	if err := unix.Kill(-target.PGID, syscall.Signal(0)); err != nil {
		if errors.Is(err, unix.ESRCH) {
			return ProbeAbsent, nil
		}
		return ProbeUnprovable, err
	}
	return ProbeLive, nil
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
