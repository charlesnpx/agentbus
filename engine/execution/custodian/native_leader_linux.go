//go:build linux

package custodian

import (
	"context"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type nativeLeaderPlatformHandle struct {
	fd     int
	closed bool
}

// openNativeLeaderPlatformHandle opens a pidfd for exit notification only. A
// pidfd held by a non-parent does not keep the numeric PID/PGID unrecycled after
// the real parent reaps the process.
func openNativeLeaderPlatformHandle(pid int) (nativeLeaderPlatformHandle, error) {
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nativeLeaderPlatformHandle{}, err
	}
	return nativeLeaderPlatformHandle{fd: fd}, nil
}

func (handle *nativeLeaderPlatformHandle) held() bool {
	if handle == nil {
		return false
	}
	return handle.fd >= 0 && !handle.closed
}

func (handle *nativeLeaderPlatformHandle) clone() (nativeLeaderPlatformHandle, error) {
	if handle == nil || !handle.held() {
		return nativeLeaderPlatformHandle{}, ErrNativeCustodianUnavailable
	}
	fd, err := unix.Dup(handle.fd)
	if err != nil {
		return nativeLeaderPlatformHandle{}, err
	}
	return nativeLeaderPlatformHandle{fd: fd}, nil
}

func (handle *nativeLeaderPlatformHandle) waitExited(ctx context.Context) error {
	if handle == nil || !handle.held() {
		return ErrNativeCustodianUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fds := []unix.PollFd{{
		Fd:     int32(handle.fd),
		Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR,
	}}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := unix.Poll(fds, int((50 * time.Millisecond).Milliseconds()))
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
}

func (handle *nativeLeaderPlatformHandle) close() error {
	if handle == nil {
		return nil
	}
	if handle.closed || handle.fd < 0 {
		handle.closed = true
		return nil
	}
	fd := handle.fd
	handle.fd = -1
	handle.closed = true
	return unix.Close(fd)
}

func probeNativeLeaderPlatform() error {
	handle, err := openNativeLeaderPlatformHandle(os.Getpid())
	if err != nil {
		return err
	}
	return handle.close()
}
