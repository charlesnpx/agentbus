//go:build darwin

package agentbusserve

import (
	"os"

	"golang.org/x/sys/unix"
)

func dupToStderr(file *os.File) error {
	return unix.Dup2(int(file.Fd()), int(os.Stderr.Fd()))
}
