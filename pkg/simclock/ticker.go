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

	// tickerCallbacks maps ticker ID to callback function to be invoked synchronously when ticker fires
	tickerCallbacks = make(map[int64]func())
	callbacksMu     sync.Mutex
)

// Ticker delivers ticks on its channel when simulated depth advances far enough.
// It mirrors time.Ticker semantics (C field, Stop, Reset) but is driven by SetDepth.
// Unlike time.Ticker, this ticker is designed to work with branching state exploration:
// it fires based purely on whether the current depth is at a valid tick boundary,
// so each branch gets its own ticker fires regardless of what other branches have done.
type Ticker struct {
	C <-chan time.Time // receive-only channel exposed to users

	id            int64
	interval      time.Duration
	intervalSteps int64
	startDepth    int64 // depth when ticker was created (for computing tick boundaries)

	mu      sync.Mutex
	stopped bool
	ch      chan time.Time // underlying bidirectional channel for sending
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
	ch := make(chan time.Time, 1) // buffered channel (size 1) to allow one tick to be queued
	current := currentDepth.Load()
	t := &Ticker{
		C:             ch, // expose as receive-only
		ch:            ch, // keep bidirectional for internal use
		id:            id,
		interval:      d,
		intervalSteps: steps,
		startDepth:    current, // record when ticker was created
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

// Reset updates the ticker interval and resets the start depth to current depth.
// Panics if d is not a positive multiple of the simclock step.
func (t *Ticker) Reset(d time.Duration) {
	steps := validateInterval(d)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.interval = d
	t.intervalSteps = steps
	t.startDepth = currentDepth.Load() // reset start depth
	if t.stopped {
		t.stopped = false
		registerTicker(t)
	}
}

func (t *Ticker) tickIfDue(prevDepth, depth int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return
	}

	// Compute whether this depth advancement crossed at least one tick boundary.
	// Tick boundaries are at: startDepth + intervalSteps, startDepth + 2*intervalSteps, ...
	// (First tick is after one full interval, not at startDepth itself)
	if depth <= t.startDepth {
		return
	}

	offsetPrev := prevDepth - t.startDepth
	if offsetPrev < 0 {
		offsetPrev = 0
	}
	prevIntervals := offsetPrev / t.intervalSteps
	currentIntervals := (depth - t.startDepth) / t.intervalSteps

	// If we haven't crossed a new interval boundary since the last depth, do nothing.
	if currentIntervals <= prevIntervals {
		return
	}

	// Fire at the first boundary after the previous depth; later boundaries are dropped
	tickDepth := t.startDepth + (prevIntervals+1)*t.intervalSteps

	// Invoke synchronous callback if registered (this happens BEFORE sending to channel)
	// This allows deterministic synchronous execution of ticker callbacks
	callbacksMu.Lock()
	callback := tickerCallbacks[t.id]
	callbacksMu.Unlock()
	if callback != nil {
		callback()
	}

	// Send to channel (for goroutines waiting on ticker.C)
	// This is non-blocking to avoid deadlocks if no one is reading
	select {
	case t.ch <- depthToTime(tickDepth):
	default:
		// Channel full, tick dropped
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

	// Also remove callback when ticker is deregistered
	callbacksMu.Lock()
	delete(tickerCallbacks, id)
	callbacksMu.Unlock()
}

// RegisterTickerCallback registers a callback function to be invoked synchronously
// when the specified ticker fires. This allows deterministic synchronous execution
// of ticker callbacks in simulation contexts where goroutine scheduling is unreliable.
// The callback is invoked during SetDepth/advanceTickers, before the tick is sent to the channel.
func RegisterTickerCallback(ticker *Ticker, callback func()) {
	callbacksMu.Lock()
	defer callbacksMu.Unlock()
	tickerCallbacks[ticker.id] = callback
}

func advanceTickers(prevDepth, depth int64) {
	tickerMu.Lock()
	tickers := make([]*Ticker, 0, len(tickerRegistry))
	for _, t := range tickerRegistry {
		tickers = append(tickers, t)
	}
	tickerMu.Unlock()

	for _, t := range tickers {
		t.tickIfDue(prevDepth, depth)
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
