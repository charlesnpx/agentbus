//go:build darwin

package served

import "syscall"

func unixSocketStreamType() (int, bool) {
	return syscall.SOCK_STREAM, false
}
