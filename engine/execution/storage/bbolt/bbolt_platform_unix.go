//go:build !windows

package bbolt

import (
	"fmt"
	"os"
	"syscall"

	"github.com/charlesnpx/agentbus/engine/execution/repository"
)

func openBoltPreflightData(path string, size int64, expectedIdentity FileIdentity) ([]byte, func(), error) {
	data, err := mmapBoltPreflightData(path, size, expectedIdentity)
	if err != nil {
		return nil, nil, err
	}
	return data, func() {
		_ = syscall.Munmap(data)
	}, nil
}

func mmapBoltPreflightData(path string, size int64, expectedIdentity FileIdentity) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: bbolt structural graph preflight open mmap source for %s: %v", repository.ErrCorruptRecord, path, err)
	}
	defer file.Close()

	if expectedIdentity != (FileIdentity{}) {
		openedIdentity, err := fileIdentityFromFile(file)
		if err != nil {
			return nil, fmt.Errorf("%w: bbolt structural graph preflight stat mmap source for %s: %v", repository.ErrCorruptRecord, path, err)
		}
		if openedIdentity != expectedIdentity {
			return nil, FileIdentityMismatchError{Path: path, Expected: expectedIdentity, Opened: openedIdentity}
		}
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("%w: bbolt structural graph preflight mmap %d bytes for %s: %v", repository.ErrCorruptRecord, size, path, err)
	}
	return data, nil
}

func fileIdentityFromFile(file *os.File) (FileIdentity, error) {
	info, err := file.Stat()
	if err != nil {
		return FileIdentity{}, err
	}
	return fileIdentityFromFileInfo(info)
}

func fileIdentityFromPath(_ string, info os.FileInfo) (FileIdentity, error) {
	return fileIdentityFromFileInfo(info)
}

func fileIdentityFromFileInfo(info os.FileInfo) (FileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return FileIdentity{}, fmt.Errorf("unexpected bbolt stat type %T", info.Sys())
	}
	return FileIdentity{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino)}, nil
}
