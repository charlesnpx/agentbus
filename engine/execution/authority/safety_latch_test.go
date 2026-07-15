package authority

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSafetyLatchFirstTripWins(t *testing.T) {
	latch := NewSafetyLatch()
	first := errors.New("first")
	second := errors.New("second")

	latch.Trip(first)
	latch.Trip(second)

	if got := latch.Reason(); !errors.Is(got, first) {
		t.Fatalf("Reason() = %v, want first error", got)
	}
	if got := latch.Reason(); errors.Is(got, second) {
		t.Fatalf("Reason() = %v, second trip overwrote first", got)
	}
}

func TestSafetyLatchDoneClosesOnce(t *testing.T) {
	latch := NewSafetyLatch()
	done := latch.Done()

	select {
	case <-done:
		t.Fatal("Done closed before Trip")
	default:
	}

	latch.Trip(errors.New("fatal"))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done did not close after Trip")
	}

	latch.Trip(errors.New("ignored"))
	select {
	case <-done:
	default:
		t.Fatal("Done was not closed after repeated Trip")
	}
}

func TestSafetyLatchZeroValue(t *testing.T) {
	var latch SafetyLatch
	reason := errors.New("zero value")

	done := latch.Done()
	latch.Trip(reason)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("zero value Done did not close after Trip")
	}
	if got := latch.Reason(); !errors.Is(got, reason) {
		t.Fatalf("Reason() = %v, want zero value reason", got)
	}
}

func TestSafetyLatchConcurrentTripsAreSafe(t *testing.T) {
	latch := NewSafetyLatch()
	reasons := make([]error, 128)
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := range reasons {
		reasons[i] = fmt.Errorf("reason-%03d", i)
		wg.Add(1)
		go func(reason error) {
			defer wg.Done()
			<-start
			latch.Trip(reason)
		}(reasons[i])
	}

	close(start)
	wg.Wait()

	got := latch.Reason()
	if got == nil {
		t.Fatal("Reason() is nil after concurrent Trips")
	}
	var matched bool
	for _, reason := range reasons {
		if errors.Is(got, reason) {
			matched = true
			break
		}
	}
	if !matched {
		t.Fatalf("Reason() = %v, want one of submitted reasons", got)
	}

	select {
	case <-latch.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close after concurrent Trips")
	}
}
