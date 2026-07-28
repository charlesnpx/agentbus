package engine

import (
	"errors"
	"testing"
	"time"
)

func TestProcessRefAliveRejectsEmptyStartTokens(t *testing.T) {
	store := &Store{processes: fakeProcessTable{entries: map[int]ProcessInfo{
		101: {PID: 101, StartTime: "observed-start"},
		102: {PID: 102},
	}}}

	if alive, err := store.processRefAlive(ProcessRef{PID: 101}); alive || !errors.Is(err, ErrProcessIdentityUnverifiable) {
		t.Fatalf("empty stored token alive=%v err=%v, want ErrProcessIdentityUnverifiable", alive, err)
	}
	if alive, err := store.processRefAlive(ProcessRef{PID: 102, StartTime: "stored-start"}); alive || !errors.Is(err, ErrProcessIdentityUnverifiable) {
		t.Fatalf("empty observed token alive=%v err=%v, want ErrProcessIdentityUnverifiable", alive, err)
	}
}

func TestCancelRunningWithEmptyStoredTokenRejectsBeforeSignal(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	signaler := &recordingProcessGroupSignaler{}
	waiter := &recordingWaiter{}
	store := newTestStoreWithOptions(t, base, fakeProcessTable{entries: map[int]ProcessInfo{
		201: {PID: 201, StartTime: "worker-start"},
	}}, testStoreOptions{
		processGroups: signaler,
		cancelWaiter:  waiter,
		cancelGrace:   time.Second,
	})
	record := &JobRecord{
		JobID:     "job_cancel_empty_identity",
		State:     StateRunning,
		UpdatedAt: base,
		Lease:     Lease{ExpiresAt: base.Add(time.Minute)},
		Worker:    ProcessRef{PID: 201, PGID: 301},
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Cancel(record.JobID); !errors.Is(err, ErrProcessIdentityUnverifiable) {
		t.Fatalf("Cancel err = %v, want ErrProcessIdentityUnverifiable", err)
	}
	assertProcessGroupSignals(t, signaler.signals, nil)
	assertWaits(t, waiter.waits, nil)
}
