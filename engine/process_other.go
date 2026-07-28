//go:build !darwin && !linux

package engine

func nativeProcessInfo(pid int) (ProcessInfo, bool, error) {
	return ProcessInfo{PID: pid}, true, nil
}
