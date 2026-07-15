//go:build darwin

package custodian

import (
	"fmt"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func debugGroupMembers(group model.GroupRef) string {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", group.PGID)
	if err != nil {
		return err.Error()
	}
	var parts []string
	for _, process := range processes {
		parts = append(parts, fmt.Sprintf("pid=%d stat=%d", process.Proc.P_pid, process.Proc.P_stat))
	}
	return strings.Join(parts, ",")
}
