//go:build linux

package engine

import (
	"os"
	"strconv"
)

func nativeProcessInfo(pid int) (ProcessInfo, bool, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ProcessInfo{}, false, err
	}
	startTime, ok := linuxProcStatStartTime(string(b))
	if !ok {
		return ProcessInfo{}, false, nil
	}
	return ProcessInfo{PID: pid, StartTime: startTime}, true, nil
}
