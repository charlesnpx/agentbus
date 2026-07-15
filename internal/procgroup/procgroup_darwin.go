//go:build darwin

package procgroup

import (
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func nativeHostBootID() (string, error) {
	boottime, err := unix.SysctlTimeval("kern.boottime")
	if err == nil && boottime == nil {
		return "", fmt.Errorf("kern.boottime returned nil")
	}
	if err == nil && boottime.Sec <= 0 {
		return "", fmt.Errorf("kern.boottime invalid value %d.%06d", boottime.Sec, boottime.Usec)
	}
	if err == nil {
		return formatDarwinTimeToken("darwin-boottime", boottime.Sec, boottime.Usec), nil
	}
	if !errors.Is(err, unix.EPERM) {
		return "", err
	}
	pidOne, fallbackErr := readDarwinProcess(1)
	if fallbackErr != nil {
		return "", fmt.Errorf("kern.boottime: %w; pid1 start fallback: %v", err, fallbackErr)
	}
	return "darwin-pid1-start-" + pidOne.StartToken.String(), nil
}

func nativePIDNamespaceID() (string, error) {
	return "", nil
}

func (nativeKernelReader) CurrentKernelDomain() (model.KernelDomainID, error) {
	hostBootID, err := nativeHostBootID()
	if err != nil {
		return model.KernelDomainID{}, err
	}
	return model.NewKernelDomainIDWithoutPIDNamespace(hostBootID)
}

func (nativeKernelReader) ReadProcess(pid int) (processSnapshot, error) {
	return readDarwinProcess(pid)
}

func (nativeKernelReader) ProcessesInGroup(pgid int) ([]processSnapshot, error) {
	if pgid <= 0 {
		return nil, fmt.Errorf("pgid must be positive")
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pgid)
	if err != nil {
		return nil, err
	}
	members := make([]processSnapshot, 0, len(processes))
	for i := range processes {
		snapshot, err := darwinProcessSnapshot(processes[i])
		if err != nil {
			return nil, err
		}
		members = append(members, snapshot)
	}
	return members, nil
}

func readDarwinProcess(pid int) (processSnapshot, error) {
	if pid <= 0 {
		return processSnapshot{}, fmt.Errorf("pid must be positive")
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return processSnapshot{}, err
	}
	if len(processes) == 0 {
		return processSnapshot{}, ErrProcessMissing
	}
	if len(processes) > 1 {
		return processSnapshot{}, fmt.Errorf("kern.proc.pid returned %d entries for pid %d", len(processes), pid)
	}
	snapshot, err := darwinProcessSnapshot(processes[0])
	if err != nil {
		return processSnapshot{}, err
	}
	if snapshot.PID != pid {
		return processSnapshot{}, fmt.Errorf("kinfo pid mismatch: got %d want %d", snapshot.PID, pid)
	}
	return snapshot, nil
}

func darwinProcessSnapshot(process unix.KinfoProc) (processSnapshot, error) {
	pid := int(process.Proc.P_pid)
	if pid <= 0 {
		return processSnapshot{}, fmt.Errorf("kinfo process has invalid pid %d", pid)
	}
	pgid := int(process.Eproc.Pgid)
	if pgid <= 0 {
		return processSnapshot{}, fmt.Errorf("kinfo process %d has invalid pgid %d", pid, pgid)
	}
	start := process.Proc.P_starttime
	if start.Sec <= 0 {
		return processSnapshot{}, fmt.Errorf("kinfo process %d has invalid start time %d.%d", pid, start.Sec, start.Usec)
	}
	snapshot := processSnapshot{
		PID:        pid,
		PGID:       pgid,
		StartToken: StartToken(formatDarwinTimeToken("", start.Sec, start.Usec)),
	}
	if err := snapshot.validate(); err != nil {
		return processSnapshot{}, err
	}
	return snapshot, nil
}

func formatDarwinTimeToken(prefix string, sec int64, usec int32) string {
	if prefix == "" {
		return fmt.Sprintf("%d.%06d", sec, usec)
	}
	return fmt.Sprintf("%s-%d.%06d", prefix, sec, usec)
}
