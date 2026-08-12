//go:build windows

package authority

import (
	"io"
	"os"
)

func readSmallRegularAdmissionDaemonPIDFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
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
