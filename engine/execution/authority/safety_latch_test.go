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

func TestSafetyLatchReasonConcurrentWithTripIsRaceFree(t *testing.T) {
	latch := NewSafetyLatch()
	first := errors.New("first")
	second := errors.New("second")
	const readers = 64

	var readersWG sync.WaitGroup
	var tripWG sync.WaitGroup
	start := make(chan struct{})
	stop := make(chan struct{})

	for range readers {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}

				_ = latch.Reason()
				select {
				case <-latch.Done():
				default:
				}
			}
		}()
	}

	tripWG.Add(1)
	go func() {
		defer tripWG.Done()
		<-start
		latch.Trip(first)
	}()

	close(start)
	tripWG.Wait()
	latch.Trip(second)
	close(stop)
	readersWG.Wait()

	if got := latch.Reason(); !errors.Is(got, first) {
		t.Fatalf("Reason() = %v, want first error", got)
	}
	select {
	case <-latch.Done():
	case <-time.After(time.Second):
		t.Fatal("Done did not close after concurrent Reason and Trip")
	}
}
