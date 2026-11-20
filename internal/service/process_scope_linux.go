//go:build linux

package service

import (
	"fmt"
	"os"
	"strings"
)

func platformProcessScope() (string, string, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", "", fmt.Errorf("read boot identity: %w", err)
	}
	namespace, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return "", "", fmt.Errorf("read PID namespace: %w", err)
	}
	if namespace == "" {
		return "", "", fmt.Errorf("empty PID namespace identity")
	}
	return strings.TrimSpace(string(boot)), namespace, nil
}
