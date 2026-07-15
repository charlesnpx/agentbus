//go:build darwin

package custodian

type nativeLeaderPlatformHandle struct {
	closed bool
}

func openNativeLeaderPlatformHandle(int) (nativeLeaderPlatformHandle, error) {
	return nativeLeaderPlatformHandle{}, nil
}

func (handle *nativeLeaderPlatformHandle) held() bool {
	if handle == nil {
		return false
	}
	return !handle.closed
}

func (handle *nativeLeaderPlatformHandle) close() error {
	if handle == nil {
		return nil
	}
	handle.closed = true
	return nil
}
