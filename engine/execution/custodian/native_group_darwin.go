//go:build darwin

package custodian

import (
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func groupHasNoMembersExceptLeader(group model.GroupRef) (bool, error) {
	if err := group.Validate(); err != nil {
		return false, err
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", group.PGID)
	if err != nil {
		return false, err
	}
	for _, process := range processes {
		pid := int(process.Proc.P_pid)
		if pid <= 0 {
			return false, fmt.Errorf("kern.proc.pgrp returned invalid pid %d", pid)
		}
		if pid == group.Leader.PID {
			continue
		}
		return false, nil
	}
	return true, nil
}
