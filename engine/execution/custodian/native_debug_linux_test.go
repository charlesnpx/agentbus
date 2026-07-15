//go:build linux

package custodian

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func debugGroupMembers(group model.GroupRef) string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err.Error()
	}
	var parts []string
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		state, pgid, err := linuxProcessStateGroup(pid)
		if err == nil && pgid == group.PGID {
			parts = append(parts, fmt.Sprintf("pid=%d stat=%s", pid, state))
		}
	}
	return strings.Join(parts, ",")
}
