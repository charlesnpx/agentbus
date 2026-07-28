//go:build linux

package agentbusserve

import (
	"os"

	"golang.org/x/sys/unix"
)

func dupToStderr(file *os.File) error {
	return unix.Dup3(int(file.Fd()), int(os.Stderr.Fd()), 0)
}
