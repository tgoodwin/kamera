package simclock

import (
	"sync/atomic"
	"time"
)

var (
	base = time.Unix(0, 0)
	step = time.Hour

	currentDepth atomic.Int64
)

// Now returns a deterministic time based on the current depth.
// Default: Unix epoch + depth * 1h.
func Now() time.Time {
	d := currentDepth.Load()
	return base.Add(time.Duration(d) * step)
}

// SetDepth sets the current depth and returns a restore func to reset to the previous depth.
func SetDepth(depth int) func() {
	prev := currentDepth.Swap(int64(depth))
	return func() {
		currentDepth.Store(prev)
	}
}

// WithDepth runs f with the depth set to the provided value, restoring afterward.
func WithDepth(depth int, f func()) {
	restore := SetDepth(depth)
	defer restore()
	f()
}

// Configure allows overriding the base time and step size used by Now.
func Configure(b time.Time, s time.Duration) {
	base = b
	step = s
}
