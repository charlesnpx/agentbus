//go:build linux

package custodian

import "golang.org/x/sys/unix"

type nativeLeaderPlatformHandle struct {
	fd     int
	closed bool
}

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
