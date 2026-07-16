//go:build darwin

package custodian

import (
	"context"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const darwinProcessStateZombie = int8(5)

type nativeLeaderPlatformHandle struct {
	kq     int
	pid    int
	closed bool
}

// openNativeLeaderPlatformHandle registers a kqueue NOTE_EXIT watcher for exit
// notification only. A kqueue held by a non-parent does not keep the numeric
// PID/PGID unrecycled after the real parent reaps the process.
func openNativeLeaderPlatformHandle(pid int) (nativeLeaderPlatformHandle, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nativeLeaderPlatformHandle{}, err
	}
	change := unix.Kevent_t{
		Ident:  uint64(pid),
		Filter: unix.EVFILT_PROC,
		Flags:  unix.EV_ADD | unix.EV_ENABLE | unix.EV_CLEAR,
		Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(kq, []unix.Kevent_t{change}, nil, nil); err != nil {
		_ = unix.Close(kq)
		return nativeLeaderPlatformHandle{}, err
	}
	return nativeLeaderPlatformHandle{kq: kq, pid: pid}, nil
}

func (handle *nativeLeaderPlatformHandle) held() bool {
	if handle == nil {
		return false
	}
	return handle.kq >= 0 && !handle.closed
}

func (handle *nativeLeaderPlatformHandle) clone() (nativeLeaderPlatformHandle, error) {
	if handle == nil || !handle.held() {
		return nativeLeaderPlatformHandle{}, ErrNativeCustodianUnavailable
	}
	kq, err := unix.Dup(handle.kq)
	if err != nil {
		return nativeLeaderPlatformHandle{}, err
	}
	return nativeLeaderPlatformHandle{kq: kq, pid: handle.pid}, nil
}

func (handle *nativeLeaderPlatformHandle) waitExited(ctx context.Context) error {
	if handle == nil || !handle.held() {
		return ErrNativeCustodianUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	events := make([]unix.Kevent_t, 1)
	timeout := unix.NsecToTimespec((50 * time.Millisecond).Nanoseconds())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		exited, err := darwinProcessExited(handle.pid)
		if err != nil {
			return err
		}
		if exited {
			return nil
		}
		n, err := unix.Kevent(handle.kq, nil, events, &timeout)
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

func darwinProcessExited(pid int) (bool, error) {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return false, err
	}
	if len(processes) == 0 {
		return true, nil
	}
	if len(processes) > 1 {
		return false, fmt.Errorf("kern.proc.pid returned %d entries for pid %d", len(processes), pid)
	}
	return processes[0].Proc.P_stat == darwinProcessStateZombie, nil
}

func (handle *nativeLeaderPlatformHandle) close() error {
	if handle == nil {
		return nil
	}
	if handle.closed || handle.kq < 0 {
		handle.closed = true
		return nil
	}
	kq := handle.kq
	handle.kq = -1
	handle.closed = true
	return unix.Close(kq)
}

func probeNativeLeaderPlatform() error {
	handle, err := openNativeLeaderPlatformHandle(os.Getpid())
	if err != nil {
		return err
	}
	return handle.close()
}
