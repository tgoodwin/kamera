package simclock

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	tickerSeq      atomic.Int64
	tickerRegistry = make(map[int64]*Ticker)
	tickerMu       sync.Mutex
)

// Ticker delivers ticks on its channel when simulated depth advances far enough.
// It mirrors time.Ticker semantics (C field, Stop, Reset) but is driven by SetDepth.
type Ticker struct {
	C <-chan time.Time // receive-only channel exposed to users

	id            int64
	interval      time.Duration
	intervalSteps int64

	mu        sync.Mutex
	nextDepth int64
	stopped   bool
	ch        chan time.Time // underlying bidirectional channel for sending
}

// NewTicker returns a Ticker whose channel ticks each time the depth advances by d.
// The first tick arrives after the first full interval.
// Panics if d is not a positive multiple of the simclock step.
func NewTicker(d time.Duration) *Ticker {
	return newTicker(d)
}

func newTicker(d time.Duration) *Ticker {
	steps := validateInterval(d)
	id := tickerSeq.Add(1)
	ch := make(chan time.Time, 1)
	t := &Ticker{
		C:             ch, // expose as receive-only
		ch:            ch, // keep bidirectional for internal use
		id:            id,
		interval:      d,
		intervalSteps: steps,
		nextDepth:     currentDepth.Load() + steps,
	}
	registerTicker(t)
	return t
}

// Stop halts the ticker and removes it from the registry.
func (t *Ticker) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	t.stopped = true
	t.mu.Unlock()
	deregisterTicker(t.id)
}

// Reset updates the ticker interval and schedules the next tick relative to the current depth.
// Panics if d is not a positive multiple of the simclock step.
func (t *Ticker) Reset(d time.Duration) {
	steps := validateInterval(d)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.interval = d
	t.intervalSteps = steps
	t.nextDepth = currentDepth.Load() + steps
	if t.stopped {
		t.stopped = false
		registerTicker(t)
	}
}

func (t *Ticker) tickIfDue(depth int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}

	for depth >= t.nextDepth {
		select {
		case t.ch <- depthToTime(t.nextDepth):
		default:
		}
		t.nextDepth += t.intervalSteps
	}
}

func registerTicker(t *Ticker) {
	tickerMu.Lock()
	tickerRegistry[t.id] = t
	tickerMu.Unlock()
}

func deregisterTicker(id int64) {
	tickerMu.Lock()
	delete(tickerRegistry, id)
	tickerMu.Unlock()
}

func advanceTickers(depth int64) {
	tickerMu.Lock()
	tickers := make([]*Ticker, 0, len(tickerRegistry))
	for _, t := range tickerRegistry {
		tickers = append(tickers, t)
	}
	tickerMu.Unlock()

	for _, t := range tickers {
		t.tickIfDue(depth)
	}
}

func depthToTime(depth int64) time.Time {
	return base.Add(time.Duration(depth) * step)
}

func validateInterval(d time.Duration) int64 {
	if d <= 0 || d < step || d%step != 0 {
		panic("simclock: ticker interval must be a positive multiple of the step")
	}
	return int64(d / step)
}
