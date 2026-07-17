//go:build darwin

package procgroup

import (
	"errors"
	"fmt"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

const darwinProcessStateZombie = int8(5)

func nativeHostBootID() (string, error) {
	bootSessionUUID, err := darwinBootSessionUUID()
	if err == nil {
		return bootSessionUUID, nil
	}
	if !darwinBootSessionUUIDUnavailable(err) {
		return "", err
	}

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

func darwinBootSessionUUID() (string, error) {
	bootSessionUUID, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", err
	}
	bootSessionUUID = strings.TrimSpace(bootSessionUUID)
	if bootSessionUUID == "" {
		return "", fmt.Errorf("kern.bootsessionuuid is empty")
	}
	return "darwin-bootsessionuuid-" + bootSessionUUID, nil
}

func darwinBootSessionUUIDUnavailable(err error) bool {
	return errors.Is(err, unix.ENOENT) ||
		errors.Is(err, unix.EPERM) ||
		errors.Is(err, unix.ENOTSUP) ||
		errors.Is(err, unix.EOPNOTSUPP)
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
	startToken, err := darwinProcessStartToken(process)
	if err != nil {
		return processSnapshot{}, fmt.Errorf("kinfo process %d: %w", pid, err)
	}
	snapshot := processSnapshot{
		PID:        pid,
		PGID:       pgid,
		StartToken: startToken,
		RunState:   darwinProcessRunState(process),
	}
	if err := snapshot.validate(); err != nil {
		return processSnapshot{}, err
	}
	return snapshot, nil
}

func darwinProcessRunState(process unix.KinfoProc) ProcessRunState {
	if process.Proc.P_stat == darwinProcessStateZombie {
		return ProcessRunStateZombie
	}
	return ProcessRunStateRunning
}

// darwinProcessStartToken combines stable kinfo_proc fields to narrow PID reuse
// collisions beyond Darwin's microsecond p_starttime. Residual bound: an
// in-same-instant PID reuse with identical start time, real uid, and parent pid
// remains theoretically possible on Darwin, and Linux start-clock tokens have an
// analogous collision bound; the S3B custodian binds stronger identity.
func darwinProcessStartToken(process unix.KinfoProc) (StartToken, error) {
	start := process.Proc.P_starttime
	if start.Sec <= 0 {
		return "", fmt.Errorf("invalid start time %d.%d", start.Sec, start.Usec)
	}
	if start.Usec < 0 || start.Usec > 999999 {
		return "", fmt.Errorf("invalid start time usec %d", start.Usec)
	}
	ppid := process.Eproc.Ppid
	if ppid < 0 {
		return "", fmt.Errorf("invalid parent pid %d", ppid)
	}
	return StartToken(formatDarwinStartToken(start.Sec, start.Usec, process.Eproc.Pcred.P_ruid, ppid)), nil
}

func formatDarwinStartToken(sec int64, usec int32, realUID uint32, ppid int32) string {
	return fmt.Sprintf("%d.%06d-uid-%d-ppid-%d", sec, usec, realUID, ppid)
}

func formatDarwinTimeToken(prefix string, sec int64, usec int32) string {
	if prefix == "" {
		return fmt.Sprintf("%d.%06d", sec, usec)
	}
	return fmt.Sprintf("%s-%d.%06d", prefix, sec, usec)
}
