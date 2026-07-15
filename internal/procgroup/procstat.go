package procgroup

import (
	"fmt"
	"strconv"
	"strings"
)

func parseLinuxProcStat(stat string) (processSnapshot, error) {
	stat = strings.TrimSpace(stat)
	rightParen := strings.LastIndex(stat, ")")
	if rightParen < 0 {
		return processSnapshot{}, fmt.Errorf("proc stat missing command terminator")
	}
	leftParen := strings.Index(stat[:rightParen+1], "(")
	if leftParen < 0 {
		return processSnapshot{}, fmt.Errorf("proc stat missing command start")
	}
	pidField := strings.TrimSpace(stat[:leftParen])
	pid, err := strconv.Atoi(pidField)
	if err != nil || pid <= 0 {
		return processSnapshot{}, fmt.Errorf("proc stat invalid pid %q", pidField)
	}
	fields := strings.Fields(stat[rightParen+1:])
	if len(fields) < 20 {
		return processSnapshot{}, fmt.Errorf("proc stat too short: got %d fields after command", len(fields))
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil || pgid <= 0 {
		return processSnapshot{}, fmt.Errorf("proc stat invalid pgid %q", fields[2])
	}
	startTime := fields[19]
	if _, err := strconv.ParseUint(startTime, 10, 64); err != nil {
		return processSnapshot{}, fmt.Errorf("proc stat invalid starttime %q", startTime)
	}
	snapshot := processSnapshot{PID: pid, PGID: pgid, StartToken: StartToken(startTime)}
	if err := snapshot.validate(); err != nil {
		return processSnapshot{}, err
	}
	return snapshot, nil
}
