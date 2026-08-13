//go:build unix

package authority

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func readSmallRegularAdmissionDaemonPIDFile(path string) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, os.ErrInvalid
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxAdmissionDaemonPIDFileBytes {
		return nil, os.ErrInvalid
	}
	return io.ReadAll(io.LimitReader(file, maxAdmissionDaemonPIDFileBytes+1))
}
