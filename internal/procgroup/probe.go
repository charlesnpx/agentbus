package procgroup

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

func probeProcessGroupExistence(pgid int, kill func(pid int, signal syscall.Signal) error) groupExistenceProbeResult {
	if pgid <= 1 {
		return groupExistenceIndeterminate
	}
	return mapGroupKillErr(kill(-pgid, 0))
}

func mapGroupKillErr(err error) groupExistenceProbeResult {
	switch {
	case err == nil:
		return groupExistenceExists
	case errors.Is(err, unix.ESRCH):
		return groupExistenceDefinitelyAbsent
	case errors.Is(err, unix.EPERM):
		return groupExistenceExists
	default:
		return groupExistenceIndeterminate
	}
}
