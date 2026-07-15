package containment

import (
	"context"
	"time"
)

type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) Sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
