//go:build windows

package bbolt

import (
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
	"golang.org/x/sys/windows"
)

// Windows does not expose Unix mmap/syscall.Stat_t APIs. Map the validated
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

	if size == 0 {
		return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight mmap %d bytes for %s: zero-length mapping", repository.ErrCorruptRecord, size, path)
	}

	mappingSize := uint64(size)
	mapping, err := windows.CreateFileMapping(
		windows.Handle(file.Fd()),
		nil,
		windows.PAGE_READONLY,
		uint32(mappingSize>>32),
		uint32(mappingSize),
		nil,
	)
	if err != nil {
		if mapping != 0 {
			_ = windows.CloseHandle(mapping)
		}
		return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight mmap %d bytes for %s: %v", repository.ErrCorruptRecord, size, path, err)
	}
	address, err := windows.MapViewOfFile(mapping, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		_ = windows.CloseHandle(mapping)
		return nil, nil, fmt.Errorf("%w: bbolt structural graph preflight mmap %d bytes for %s: %v", repository.ErrCorruptRecord, size, path, err)
	}

	data := unsafe.Slice((*byte)(unsafe.Pointer(address)), int(size))
	var releaseOnce sync.Once
	return data, func() {
		releaseOnce.Do(func() {
			_ = windows.UnmapViewOfFile(address)
			_ = windows.CloseHandle(mapping)
		})
	}, nil
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
