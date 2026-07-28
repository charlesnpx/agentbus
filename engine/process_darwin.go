//go:build darwin

package engine

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func nativeProcessInfo(pid int) (ProcessInfo, bool, error) {
	proc, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return ProcessInfo{}, false, err
	}
	if proc == nil {
		return ProcessInfo{}, false, nil
	}
	token := darwinStartTimeToken(proc.Proc.P_starttime)
	if token == "" {
		return ProcessInfo{}, false, nil
	}
	return ProcessInfo{PID: pid, StartTime: token}, true, nil
}

func darwinStartTimeToken(start unix.Timeval) string {
	if start.Sec == 0 && start.Usec == 0 {
		return ""
	}
	return fmt.Sprintf("%d.%06d", start.Sec, start.Usec)
}
