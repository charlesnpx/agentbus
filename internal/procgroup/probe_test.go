package procgroup

import (
	"errors"
	"strconv"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestMapGroupKillErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want groupExistenceProbeResult
	}{
		{
			name: "nil means exists",
			err:  nil,
			want: groupExistenceExists,
		},
		{
			name: "esrch means absent",
			err:  unix.ESRCH,
			want: groupExistenceDefinitelyAbsent,
		},
		{
			name: "eperm means exists",
			err:  unix.EPERM,
			want: groupExistenceExists,
		},
		{
			name: "eintr means indeterminate",
			err:  unix.EINTR,
			want: groupExistenceIndeterminate,
		},
		{
			name: "other error means indeterminate",
			err:  errors.New("unexpected kill failure"),
			want: groupExistenceIndeterminate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapGroupKillErr(tt.err); got != tt.want {
				t.Fatalf("mapGroupKillErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestProbeProcessGroupExistenceGuardsBroadcastSelectors(t *testing.T) {
	for _, pgid := range []int{-1, 0, 1} {
		t.Run("pgid "+strconv.Itoa(pgid), func(t *testing.T) {
			got := probeProcessGroupExistence(pgid, func(pid int, signal syscall.Signal) error {
				t.Fatalf("kill(%d, %d) called for pgid %d", pid, signal, pgid)
				return nil
			})
			if got != groupExistenceIndeterminate {
				t.Fatalf("probeProcessGroupExistence(%d) = %v, want %v", pgid, got, groupExistenceIndeterminate)
			}
		})
	}
}

func TestProbeProcessGroupExistenceUsesNegativePGIDForRealGroups(t *testing.T) {
	var gotPID int
	var gotSignal syscall.Signal
	got := probeProcessGroupExistence(2, func(pid int, signal syscall.Signal) error {
		gotPID = pid
		gotSignal = signal
		return unix.ESRCH
	})
	if got != groupExistenceDefinitelyAbsent {
		t.Fatalf("probeProcessGroupExistence(2) = %v, want %v", got, groupExistenceDefinitelyAbsent)
	}
	if gotPID != -2 || gotSignal != 0 {
		t.Fatalf("kill called with (%d, %d), want (-2, 0)", gotPID, gotSignal)
	}
}
