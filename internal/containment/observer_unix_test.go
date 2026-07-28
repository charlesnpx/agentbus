//go:build darwin || linux

package containment

import (
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestStableGroupLeaderObservationRequiresRereadBeforeAbsent(t *testing.T) {
	reads := []groupLeaderObservation{
		{Group: model.GroupAbsent, Leader: model.ProcessIdentityMissing},
		{Group: model.GroupLive, Leader: model.ProcessIdentityMatching},
	}

	got := stableGroupLeaderObservation(scriptedGroupLeaderReads(t, reads))

	want := groupLeaderObservation{Group: model.GroupExistenceUnknown, Leader: model.ProcessIdentityUnknown}
	if got != want {
		t.Fatalf("observation = %#v, want %#v", got, want)
	}
}

func TestStableGroupLeaderObservationReportsStableAbsent(t *testing.T) {
	reads := []groupLeaderObservation{
		{Group: model.GroupAbsent, Leader: model.ProcessIdentityMissing},
		{Group: model.GroupAbsent, Leader: model.ProcessIdentityMissing},
	}

	calls := 0
	got := stableGroupLeaderObservation(func() groupLeaderObservation {
		calls++
		if calls > len(reads) {
			t.Fatalf("unexpected read %d", calls)
		}
		return reads[calls-1]
	})

	want := groupLeaderObservation{Group: model.GroupAbsent, Leader: model.ProcessIdentityMissing}
	if got != want {
		t.Fatalf("observation = %#v, want %#v", got, want)
	}
	if calls != 2 {
		t.Fatalf("read calls = %d, want 2", calls)
	}
}

func TestStableGroupLeaderObservationRejectsAbsentMatchingLeader(t *testing.T) {
	got := stableGroupLeaderObservation(func() groupLeaderObservation {
		return groupLeaderObservation{Group: model.GroupAbsent, Leader: model.ProcessIdentityMatching}
	})

	want := groupLeaderObservation{Group: model.GroupExistenceUnknown, Leader: model.ProcessIdentityUnknown}
	if got != want {
		t.Fatalf("observation = %#v, want %#v", got, want)
	}
}

func scriptedGroupLeaderReads(t *testing.T, reads []groupLeaderObservation) func() groupLeaderObservation {
	t.Helper()
	calls := 0
	return func() groupLeaderObservation {
		calls++
		if calls > len(reads) {
			t.Fatalf("unexpected read %d", calls)
		}
		return reads[calls-1]
	}
}
