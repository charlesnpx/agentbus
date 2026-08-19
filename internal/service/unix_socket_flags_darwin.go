//go:build darwin

package service

import "syscall"

func unixSocketStreamType() (int, bool) {
	return syscall.SOCK_STREAM, false
}
