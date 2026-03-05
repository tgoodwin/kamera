package simclock

import (
	"sync/atomic"
	"time"
)

var (
	base = time.Unix(0, 0)
	step = time.Second

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
	newDepth := int64(depth)
	prev := currentDepth.Swap(newDepth)
	if newDepth > prev {
		advanceTickers(prev, newDepth)
	}
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

// StepDuration returns the configured simulated time increment per depth step.
func StepDuration() time.Duration {
	return step
}

// StepsForDuration converts a duration to simulated depth steps using ceiling semantics.
// Durations <= 0 return 0.
func StepsForDuration(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := StepDuration()
	if s <= 0 {
		return 0
	}
	steps := d / s
	if d%s != 0 {
		steps++
	}
	return int(steps)
}
