//go:build unix

package engine

import "syscall"

// SignalProcessGroup sends signal to pgid. Missing process groups are already
// dead for cancellation purposes and are treated as success.
func (NativeProcessGroupSignaler) SignalProcessGroup(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return nil
	}
	err := syscall.Kill(-pgid, signal)
	if errorsIsProcessMissing(err) {
		return nil
	}
	return err
}

// Lookup returns process liveness and a platform start-time token.
func (NativeProcessTable) Lookup(pid int) (ProcessInfo, bool, error) {
	if pid <= 0 {
		return ProcessInfo{}, false, nil
	}
	if err := syscall.Kill(pid, 0); err != nil {
		if errorsIsProcessMissing(err) {
			return ProcessInfo{}, false, nil
		}
		return ProcessInfo{}, false, err
	}
	return nativeProcessInfo(pid)
}

func errorsIsProcessMissing(err error) bool {
	return err == syscall.ESRCH
}
