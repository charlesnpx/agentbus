package authority

import "sync"

// SafetyLatch is a one-way in-memory fail-stop signal. The zero value is ready
// for use; the first Trip closes Done and records the reason.
type SafetyLatch struct {
	mu      sync.Mutex
	done    chan struct{}
	tripped bool
	reason  error
}

func NewSafetyLatch() *SafetyLatch {
	return &SafetyLatch{done: make(chan struct{})}
}

func (l *SafetyLatch) Trip(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.tripped {
		return
	}
	l.tripped = true
	l.reason = err
	close(l.doneLocked())
}

func (l *SafetyLatch) Done() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.doneLocked()
}

func (l *SafetyLatch) Reason() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reason
}

func (l *SafetyLatch) doneLocked() chan struct{} {
	if l.done == nil {
		l.done = make(chan struct{})
	}
	return l.done
}
