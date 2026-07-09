package engine

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// ProcessInfo is the observed identity of an operating-system process.
type ProcessInfo struct {
	PID       int
	StartTime string
}

// ProcessTable abstracts process liveness and start-time reads for tests.
type ProcessTable interface {
	Lookup(pid int) (ProcessInfo, bool, error)
}

// NativeProcessTable reads process information from the host OS.
type NativeProcessTable struct{}

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
	switch runtime.GOOS {
	case "linux":
		b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			return ProcessInfo{}, false, err
		}
		fields := strings.Fields(string(b))
		if len(fields) < 22 {
			return ProcessInfo{}, false, nil
		}
		return ProcessInfo{PID: pid, StartTime: fields[21]}, true, nil
	case "darwin":
		out, err := exec.Command("ps", "-p", strconv.Itoa(pid), "-o", "lstart=").Output()
		if err != nil {
			return ProcessInfo{}, false, err
		}
		return ProcessInfo{PID: pid, StartTime: strings.TrimSpace(string(out))}, true, nil
	default:
		return ProcessInfo{PID: pid}, true, nil
	}
}

func errorsIsProcessMissing(err error) bool {
	return err == syscall.ESRCH
}
