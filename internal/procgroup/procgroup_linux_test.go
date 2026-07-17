//go:build linux

package procgroup

import (
	"errors"
	"io/fs"
	"os"
	"testing"
)

type fakeLinuxProcDirEntry struct {
	name string
	mode fs.FileMode
	dir  bool
}

func (entry fakeLinuxProcDirEntry) Name() string {
	return entry.name
}

func (entry fakeLinuxProcDirEntry) IsDir() bool {
	return entry.dir
}

func (entry fakeLinuxProcDirEntry) Type() fs.FileMode {
	return entry.mode
}

func (entry fakeLinuxProcDirEntry) Info() (fs.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func TestLinuxProcessesInGroupSkipsVanishedPID(t *testing.T) {
	entries := []os.DirEntry{
		fakeLinuxProcDirEntry{name: "101", dir: true},
		fakeLinuxProcDirEntry{name: "102", dir: true},
		fakeLinuxProcDirEntry{name: "self", dir: true},
	}
	members, err := linuxProcessesInGroup(20, entries, func(pid int) (processSnapshot, error) {
		switch pid {
		case 101:
			return processSnapshot{}, ErrProcessMissing
		case 102:
			return processSnapshot{PID: 102, PGID: 20, StartToken: "start-102", RunState: ProcessRunStateRunning}, nil
		default:
			t.Fatalf("readProcess(%d) called for unexpected pid", pid)
			return processSnapshot{}, nil
		}
	})
	if err != nil {
		t.Fatalf("linuxProcessesInGroup() error = %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("linuxProcessesInGroup() returned %d members, want 1", len(members))
	}
	if members[0].PID != 102 {
		t.Fatalf("member PID = %d, want 102", members[0].PID)
	}
}

func TestLinuxProcessesInGroupReturnsNonMissingReadError(t *testing.T) {
	readErr := errors.New("parse failed")
	entries := []os.DirEntry{fakeLinuxProcDirEntry{name: "101", dir: true}}

	_, err := linuxProcessesInGroup(20, entries, func(int) (processSnapshot, error) {
		return processSnapshot{}, readErr
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("linuxProcessesInGroup() error = %v, want %v", err, readErr)
	}
}
