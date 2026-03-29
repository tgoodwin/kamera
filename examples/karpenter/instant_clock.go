package main

import (
	"time"

	"github.com/tgoodwin/kamera/pkg/simclock"
	"k8s.io/utils/clock"
)

// scaledClock wraps clock.RealClock but scales all durations down by a
// constant factor. This preserves the relative timing semantics (trigger
// vs timeout races in the batcher) while reducing wall-clock cost.
// Now()/Since() return simulated time from simclock for determinism.
type scaledClock struct {
	scale float64
}

func newScaledClock(scale float64) *scaledClock {
	return &scaledClock{scale: scale}
}

func (c *scaledClock) Now() time.Time                  { return simclock.Now() }
func (c *scaledClock) Since(t time.Time) time.Duration { return simclock.Now().Sub(t) }

func (c *scaledClock) After(d time.Duration) <-chan time.Time {
	return time.After(c.scaled(d))
}

func (c *scaledClock) NewTimer(d time.Duration) clock.Timer {
	return &realTimerWrapper{Timer: time.NewTimer(c.scaled(d))}
}

func (c *scaledClock) Sleep(d time.Duration) {
	time.Sleep(c.scaled(d))
}

func (c *scaledClock) Tick(d time.Duration) <-chan time.Time {
	return time.Tick(c.scaled(d))
}

func (c *scaledClock) scaled(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	s := time.Duration(float64(d) * c.scale)
	if s <= 0 {
		s = time.Microsecond
	}
	return s
}

// realTimerWrapper adapts time.Timer to clock.Timer.
type realTimerWrapper struct {
	*time.Timer
}

func (t *realTimerWrapper) C() <-chan time.Time { return t.Timer.C }
