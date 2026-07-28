package served

import (
	"errors"
	"fmt"
	"sync"
)

var ErrSafetyFailStopped = errors.New("served safety fail-stop")

type SafetyFailStopError struct {
	Reason error
}

func (e SafetyFailStopError) Error() string {
	if e.Reason == nil {
		return ErrSafetyFailStopped.Error()
	}
	return fmt.Sprintf("%s: %v", ErrSafetyFailStopped, e.Reason)
}

func (e SafetyFailStopError) Unwrap() error {
	return e.Reason
}

func (e SafetyFailStopError) Is(target error) bool {
	return target == ErrSafetyFailStopped
}

// SafetyLatch is a concurrent, first-wins fail-stop latch.
type SafetyLatch struct {
	mu      sync.Mutex
	done    chan struct{}
	tripped bool
	reason  error
}

func NewSafetyLatch() *SafetyLatch {
	return &SafetyLatch{done: make(chan struct{})}
}

func (l *SafetyLatch) Trip(reason error) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done == nil {
		l.done = make(chan struct{})
	}
	if l.tripped {
		return
	}
	if reason == nil {
		reason = ErrSafetyFailStopped
	}
	l.tripped = true
	l.reason = reason
	close(l.done)
}

func (l *SafetyLatch) Done() <-chan struct{} {
	if l == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done == nil {
		l.done = make(chan struct{})
	}
	return l.done
}

func (l *SafetyLatch) Reason() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reason
}

func safetyFailStopReason(reason string) error {
	if reason == "" {
		return ErrSafetyFailStopped
	}
	return errors.New(reason)
}
