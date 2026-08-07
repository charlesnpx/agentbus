//go:build windows

package engine

import (
	"os"

	"golang.org/x/sys/windows"
)

const jobLockRegionLength = 1

func lockJobFile(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.LockFileEx(
		windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK,
		0,
		jobLockRegionLength,
		0,
		&overlapped,
	)
}

func unlockJobFile(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(
		windows.Handle(f.Fd()),
		0,
		jobLockRegionLength,
		0,
		&overlapped,
	)
}
