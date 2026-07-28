//go:build linux

package procgroup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func nativeHostBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return model.CanonicalHostBootID(string(data))
}

func nativePIDNamespaceID() (string, error) {
	namespaceID, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return "", err
	}
	return model.CanonicalPIDNamespaceID(namespaceID)
}

func (nativeKernelReader) CurrentKernelDomain() (model.KernelDomainID, error) {
	hostBootID, err := nativeHostBootID()
	if err != nil {
		return model.KernelDomainID{}, err
	}
	pidNamespaceID, err := nativePIDNamespaceID()
	if err != nil {
		return model.KernelDomainID{}, err
	}
	return model.NewKernelDomainID(hostBootID, pidNamespaceID)
}

func (nativeKernelReader) ReadProcess(pid int) (processSnapshot, error) {
	return readLinuxProcess(pid)
}

func (nativeKernelReader) ProcessesInGroup(pgid int) ([]processSnapshot, error) {
	if pgid <= 0 {
		return nil, fmt.Errorf("pgid must be positive")
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	return linuxProcessesInGroup(pgid, entries, readLinuxProcess)
}

func (nativeKernelReader) GroupExistenceProbe(pgid int) groupExistenceProbeResult {
	return probeProcessGroupExistence(pgid, unix.Kill)
}

func linuxProcessesInGroup(pgid int, entries []os.DirEntry, readProcess func(int) (processSnapshot, error)) ([]processSnapshot, error) {
	var members []processSnapshot
	for _, entry := range entries {
		if !entry.Type().IsRegular() && !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		snapshot, err := readProcess(pid)
		if errors.Is(err, ErrProcessMissing) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if snapshot.PGID == pgid {
			members = append(members, snapshot)
		}
	}
	return members, nil
}

func readLinuxProcess(pid int) (processSnapshot, error) {
	if pid <= 0 {
		return processSnapshot{}, fmt.Errorf("pid must be positive")
	}
	path := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return processSnapshot{}, ErrProcessMissing
		}
		return processSnapshot{}, err
	}
	snapshot, err := parseLinuxProcStat(string(data))
	if err != nil {
		return processSnapshot{}, err
	}
	if snapshot.PID != pid {
		return processSnapshot{}, fmt.Errorf("proc stat pid mismatch: got %d want %d", snapshot.PID, pid)
	}
	return snapshot, nil
}
