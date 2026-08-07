//go:build windows

package engine

import (
	"errors"
	"syscall"

	"golang.org/x/sys/windows"
)

// SignalProcessGroup is a no-op on Windows, which has no POSIX process groups.
func (NativeProcessGroupSignaler) SignalProcessGroup(_ int, _ syscall.Signal) error {
	return nil
}

// Lookup returns process liveness on Windows. This minimal probe deliberately
// reports no platform start-time token.
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
	return ProcessInfo{PID: pid}, true, nil
}
