package simclock

import (
	"fmt"
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
	ch := make(chan time.Time, 1) // buffered channel (size 1) to allow one tick to be queued
	current := currentDepth.Load()
	nextDepth := current + steps
	t := &Ticker{
		C:             ch, // expose as receive-only
		ch:            ch, // keep bidirectional for internal use
		id:            id,
		interval:      d,
		intervalSteps: steps,
		nextDepth:     nextDepth,
	}
	registerTicker(t)
	// Debug: log ticker creation
	fmt.Printf("🔔 TICKER-CREATE: id=%d, interval=%v, steps=%d, currentDepth=%d, nextDepth=%d\n", id, d, steps, current, nextDepth)
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
	fmt.Printf("🔔 TICKER-STOP: id=%d\n", t.id)
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
		fmt.Printf("🔔 TICKER-FIRE: id=%d, depth=%d, nextDepth=%d, intervalSteps=%d\n", t.id, depth, t.nextDepth, t.intervalSteps)

		// Invoke synchronous callback if registered (this happens BEFORE sending to channel)
		// This allows deterministic synchronous execution of ticker callbacks
		callbacksMu.Lock()
		callback := tickerCallbacks[t.id]
		callbacksMu.Unlock()
		if callback != nil {
			fmt.Printf("🔔 TICKER-CALLBACK: id=%d, depth=%d, invoking callback synchronously\n", t.id, depth)
			callback()
		}

		// Send to channel (for goroutines waiting on ticker.C)
		// This is non-blocking to avoid deadlocks if no one is reading
		select {
		case t.ch <- depthToTime(t.nextDepth):
			fmt.Printf("🔔 TICKER-SENT: id=%d, depth=%d\n", t.id, depth)
		default:
			fmt.Printf("🔔 TICKER-DROPPED: id=%d, depth=%d (channel full)\n", t.id, depth)
		}

		t.nextDepth += t.intervalSteps
	}
}

func registerTicker(t *Ticker) {
	tickerMu.Lock()
	tickerRegistry[t.id] = t
	tickerMu.Unlock()
	fmt.Printf("🔔 TICKER-REGISTER: id=%d, total_registered=%d\n", t.id, len(tickerRegistry))
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
	fmt.Printf("🔔 TICKER-CALLBACK-REGISTER: id=%d\n", ticker.id)
}

func advanceTickers(depth int64) {
	tickerMu.Lock()
	tickers := make([]*Ticker, 0, len(tickerRegistry))
	for _, t := range tickerRegistry {
		tickers = append(tickers, t)
	}
	tickerMu.Unlock()

	fmt.Printf("🔔 ADVANCE-TICKERS: depth=%d, registered_tickers=%d\n", depth, len(tickers))
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
