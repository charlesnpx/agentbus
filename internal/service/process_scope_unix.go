//go:build darwin || linux

package service

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/internal/jobstore"
	"golang.org/x/sys/unix"
)

// currentProcessScope reads the boot and root-directory identity anew before
// each launch or signal. A replaced directory or a reboot invalidates a claim.
func (s *Server) currentProcessScope() (jobstore.ProcessScope, error) {
	if s == nil {
		return jobstore.ProcessScope{}, fmt.Errorf("service is unavailable")
	}
	if s.processScopeFn != nil {
		scope, err := s.processScopeFn()
		if err == nil {
			err = scope.Validate()
		}
		return scope, err
	}
	fd, err := unix.Open(s.stateRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return jobstore.ProcessScope{}, fmt.Errorf("open state root: %w", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return jobstore.ProcessScope{}, fmt.Errorf("identify state root: %w", err)
	}
	boot, namespace, err := platformProcessScope()
	if err != nil {
		return jobstore.ProcessScope{}, err
	}
	if boot == "" {
		return jobstore.ProcessScope{}, fmt.Errorf("empty boot identity")
	}
	scope := jobstore.ProcessScope{BootID: boot, RootDevice: strconv.FormatUint(uint64(stat.Dev), 10), RootInode: strconv.FormatUint(uint64(stat.Ino), 10), PIDNamespace: namespace}
	return scope, scope.Validate()
}

func validSelectionValue(value string) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
