//go:build linux

package service

import "syscall"

func unixSocketStreamType() (int, bool) {
	return syscall.SOCK_STREAM | syscall.SOCK_CLOEXEC, true
}
