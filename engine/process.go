package engine

import (
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
