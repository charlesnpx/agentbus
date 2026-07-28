//go:build darwin

package engine

import (
	"os"
	"testing"
)

func TestNativeProcessTableDarwinStartTokenNonEmptyAndStable(t *testing.T) {
	table := NativeProcessTable{}
	first, alive, err := table.Lookup(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if !alive {
		t.Fatal("current process reported not alive")
	}
	if first.StartTime == "" {
		t.Fatal("darwin start token is empty")
	}
	second, alive, err := table.Lookup(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if !alive {
		t.Fatal("current process reported not alive on second lookup")
	}
	if second.StartTime != first.StartTime {
		t.Fatalf("darwin start token changed: first=%q second=%q", first.StartTime, second.StartTime)
	}
}
