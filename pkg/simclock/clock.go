package simclock

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

var (
	_ clock.Clock                         = (*DeterministicClock)(nil)
	_ clock.PassiveClock                  = (*DeterministicClock)(nil)
	_ clock.WithTicker                    = (*DeterministicClock)(nil)
	_ clock.WithDelayedExecution          = (*DeterministicClock)(nil)
	_ clock.WithTickerAndDelayedExecution = (*DeterministicClock)(nil)
)

// DeterministicClock implements clock.Clock based on the package-level simclock state.
type DeterministicClock struct{}

func (DeterministicClock) Now() time.Time                  { return Now() }
func (DeterministicClock) Since(t time.Time) time.Duration { return Now().Sub(t) }
func (DeterministicClock) Sleep(time.Duration)             {}
func (DeterministicClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- Now().Add(d)
	return ch
}
func (DeterministicClock) Tick(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- Now()
	return ch
}
func (DeterministicClock) NewTimer(d time.Duration) clock.Timer {
	return newDeterministicTimer(Now().Add(d))
}
func (DeterministicClock) NewTicker(d time.Duration) clock.Ticker {
	return newDeterministicTicker(d)
}
func (d DeterministicClock) AfterFunc(_ time.Duration, f func()) clock.Timer {
	f()
	return d.NewTimer(0)
}

var (
	_ clock.Timer = (*deterministicTimer)(nil)
)

type deterministicTimer struct {
	mu       sync.Mutex
	stopped  bool
	ch       chan time.Time
	deadline time.Time
}

func newDeterministicTimer(deadline time.Time) *deterministicTimer {
	ch := make(chan time.Time, 1)
	ch <- deadline
	return &deterministicTimer{ch: ch, deadline: deadline}
}

func (t *deterministicTimer) C() <-chan time.Time {
	return t.ch
}

func (t *deterministicTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.stopped = true
	return true
}

func (t *deterministicTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deadline = Now().Add(d)
	t.stopped = false
	select {
	case t.ch <- t.deadline:
	default:
	}
	return true
}

var _ clock.Ticker = (*deterministicTicker)(nil)

type deterministicTicker struct {
	interval time.Duration
	ch       chan time.Time
}

func newDeterministicTicker(interval time.Duration) *deterministicTicker {
	ch := make(chan time.Time, 1)
	ch <- Now()
	return &deterministicTicker{interval: interval, ch: ch}
}

func (t *deterministicTicker) C() <-chan time.Time {
	return t.ch
}

func (t *deterministicTicker) Stop() {}
