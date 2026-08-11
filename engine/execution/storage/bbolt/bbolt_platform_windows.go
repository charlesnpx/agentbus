//go:build windows

package bbolt

import (
	"fmt"
	"io"
	"os"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"golang.org/x/sys/windows"
)

// Windows does not expose Unix mmap/syscall.Stat_t APIs. Read the validated
// range through the opened handle instead, and use the volume serial number
// plus file index as the same stable handle identity check used by bbolt.
func openBoltPreflightData(path string, size int64, expectedIdentity FileIdentity) ([]byte, func(), error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight open read source for %s: %v", repository.ErrCorruptRecord, path, err)
	}
	defer file.Close()

	if expectedIdentity != (FileIdentity{}) {
		openedIdentity, err := fileIdentityFromFile(file)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight stat read source for %s: %v", repository.ErrCorruptRecord, path, err)
		}
		if openedIdentity != expectedIdentity {
			return nil, nil, FileIdentityMismatchError{Path: path, Expected: expectedIdentity, Opened: openedIdentity}
		}
	}

	data := make([]byte, int(size))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight read %d bytes for %s: %v", repository.ErrCorruptRecord, size, path, err)
	}
	return data, func() {}, nil
}

func fileIdentityFromFile(file *os.File) (FileIdentity, error) {
	if file == nil {
		return FileIdentity{}, fmt.Errorf("bbolt file is nil")
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return FileIdentity{}, err
	}
	return FileIdentity{
		Dev: uint64(information.VolumeSerialNumber),
		Ino: uint64(information.FileIndexHigh)<<32 | uint64(information.FileIndexLow),
	}, nil
}

func fileIdentityFromPath(path string, _ os.FileInfo) (FileIdentity, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileIdentity{}, err
	}
	defer file.Close()
	return fileIdentityFromFile(file)
}
