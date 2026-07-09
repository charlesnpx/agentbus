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

// ProcessGroupSignaler abstracts process-group signals for tests.
type ProcessGroupSignaler interface {
	SignalProcessGroup(pgid int, signal syscall.Signal) error
}

// NativeProcessTable reads process information from the host OS.
type NativeProcessTable struct{}

// NativeProcessGroupSignaler sends signals to host OS process groups.
type NativeProcessGroupSignaler struct{}

// SignalProcessGroup sends signal to pgid. Missing process groups are already
// dead for cancellation purposes and are treated as success.
func (NativeProcessGroupSignaler) SignalProcessGroup(pgid int, signal syscall.Signal) error {
	if pgid <= 0 || runtime.GOOS == "windows" {
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
	switch runtime.GOOS {
	case "linux":
		b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			return ProcessInfo{}, false, err
		}
		startTime, ok := linuxProcStatStartTime(string(b))
		if !ok {
			return ProcessInfo{}, false, nil
		}
		return ProcessInfo{PID: pid, StartTime: startTime}, true, nil
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

func linuxProcStatStartTime(stat string) (string, bool) {
	endComm := strings.LastIndex(stat, ") ")
	if endComm < 0 {
		return "", false
	}
	fieldsAfterComm := strings.Fields(stat[endComm+2:])
	if len(fieldsAfterComm) < 20 {
		return "", false
	}
	return fieldsAfterComm[19], true
}
