package served

import (
	"errors"
	"sync"
	"testing"
)

func TestSafetyLatchTripIsFirstWinsAndIdempotent(t *testing.T) {
	t.Parallel()
	latch := NewSafetyLatch()
	first := errors.New("first fail-stop")
	second := errors.New("second fail-stop")
	latch.Trip(first)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			latch.Trip(second)
		}()
	}
	wg.Wait()

	select {
	case <-latch.Done():
	default:
		t.Fatal("latch done channel is not closed after Trip")
	}
	if got := latch.Reason(); got != first {
		t.Fatalf("latch reason = %v, want first reason %v", got, first)
	}

	latch.Trip(second)
	if got := latch.Reason(); got != first {
		t.Fatalf("latch reason after second Trip = %v, want first reason %v", got, first)
	}
}
