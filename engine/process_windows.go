//go:build windows

package engine

import (
	"errors"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

const windowsStillActive uint32 = 259

// SignalProcessGroup is a no-op on Windows, which has no POSIX process groups.
func (NativeProcessGroupSignaler) SignalProcessGroup(_ int, _ syscall.Signal) error {
	return nil
}

// Lookup returns process liveness and a FILETIME-based start-time token on Windows.
func (NativeProcessTable) Lookup(pid int) (ProcessInfo, bool, error) {
	if pid <= 0 {
		return ProcessInfo{}, false, nil
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
			return ProcessInfo{}, false, nil
		}
		return ProcessInfo{}, false, err
	}
	defer windows.CloseHandle(handle)

	var exitCode uint32
	if err := windows.GetExitCodeProcess(handle, &exitCode); err != nil {
		return ProcessInfo{}, false, err
	}
	if exitCode != windowsStillActive {
		return ProcessInfo{}, false, nil
	}

	var creationTime, exitTime, kernelTime, userTime windows.Filetime
	if err := windows.GetProcessTimes(handle, &creationTime, &exitTime, &kernelTime, &userTime); err != nil {
		return ProcessInfo{}, false, err
	}
	startTime := uint64(creationTime.HighDateTime)<<32 | uint64(creationTime.LowDateTime)
	return ProcessInfo{PID: pid, StartTime: strconv.FormatUint(startTime, 10)}, true, nil
}
