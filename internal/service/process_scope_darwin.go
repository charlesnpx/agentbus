//go:build darwin

package service

import (
	"fmt"
	"golang.org/x/sys/unix"
	"strings"
)

func platformProcessScope() (string, string, error) {
	boot, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", "", fmt.Errorf("read kern.bootsessionuuid: %w", err)
	}
	return strings.TrimSpace(boot), "", nil
}
