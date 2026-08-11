//go:build windows

package parklaunch

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestStartMonitorProcessUnsupportedPlatformClosesInheritedLeaf(t *testing.T) {
	leaf, err := os.CreateTemp(t.TempDir(), "monitor-leaf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = leaf.Close()
	})

	monitor, err := StartMonitorProcess(context.Background(), MonitorProcessSpec{
		Command:       CommandSpec{Path: "unused"},
		InheritedLeaf: leaf,
	})
	if monitor != nil || !errors.Is(err, ErrPlatformUnsupported) {
		t.Fatalf("StartMonitorProcess(unsupported platform) = (%v, %v), want nil ErrPlatformUnsupported", monitor, err)
	}
	if _, err := leaf.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("inherited leaf Stat() after ownership return error = %v, want os.ErrClosed", err)
	}
}
