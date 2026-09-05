package engine

import "testing"

func TestLinuxProcStatStartTimeWithSpacesInCommand(t *testing.T) {
	stat := "123 (name with spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987654 20"
	startTime, ok := linuxProcStatStartTime(stat)
	if !ok {
		t.Fatal("linuxProcStatStartTime() reported no start time")
	}
	if startTime != "987654" {
		t.Fatalf("linuxProcStatStartTime() = %q, want %q", startTime, "987654")
	}
}
