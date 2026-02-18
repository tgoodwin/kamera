package simclock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTickerTicksWithDepthAdvancement(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ticker := NewTicker(2 * time.Second)
	defer ticker.Stop()

	SetDepth(1)
	select {
	case <-ticker.C:
		t.Fatalf("unexpected tick at depth 1")
	default:
	}

	SetDepth(2)
	select {
	case ts := <-ticker.C:
		require.Equal(t, depthToTime(2), ts)
	default:
		t.Fatalf("expected tick at depth 2")
	}
}

func TestTickerDropsExtraTicks(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ticker := NewTicker(2 * time.Second)
	defer ticker.Stop()

	SetDepth(5) // ticks due at 2 and 4, but only one should be queued
	select {
	case ts := <-ticker.C:
		require.Equal(t, depthToTime(2), ts)
	default:
		t.Fatalf("expected tick after depth jump")
	}

	select {
	case <-ticker.C:
		t.Fatalf("unexpected second tick while channel still full")
	default:
	}

	SetDepth(6) // next tick scheduled for depth 6
	select {
	case ts := <-ticker.C:
		require.Equal(t, depthToTime(6), ts)
	default:
		t.Fatalf("expected tick at depth 6 after drain")
	}
}

func TestTickerStopPreventsTicks(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ticker := NewTicker(time.Second)
	ticker.Stop()

	SetDepth(3)
	select {
	case <-ticker.C:
		t.Fatalf("ticker should be stopped")
	default:
	}
}

func TestTickerResetReschedules(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ticker := NewTicker(3 * time.Second)
	defer ticker.Stop()

	SetDepth(3)
	require.Equal(t, depthToTime(3), <-ticker.C)

	ticker.Reset(2 * time.Second)

	SetDepth(4)
	select {
	case <-ticker.C:
		t.Fatalf("tick should not fire until depth 5")
	default:
	}

	SetDepth(5)
	require.Equal(t, depthToTime(5), <-ticker.C)
}

func TestTickerRejectsSubStep(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	require.Panics(t, func() {
		NewTicker(500 * time.Millisecond)
	})
}

func TestNewK8sTickerAdapter(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ticker := NewK8sTicker(time.Second)
	t.Cleanup(ticker.Stop)

	SetDepth(1)
	select {
	case ts := <-ticker.C():
		require.Equal(t, depthToTime(1), ts)
	default:
		t.Fatalf("expected tick at depth 1 via k8s ticker adapter")
	}
}

func TestTickerPanicsOnInvalidIntervalSteps(t *testing.T) {
	restore := SetDepth(0)
	defer restore()

	ch := make(chan time.Time, 1)
	ticker := &Ticker{
		C:             ch,
		ch:            ch,
		id:            999999,
		interval:      time.Second,
		intervalSteps: 0,
		startDepth:    0,
	}
	registerTicker(ticker)
	defer deregisterTicker(ticker.id)

	require.Panics(t, func() {
		SetDepth(1)
	})
}
