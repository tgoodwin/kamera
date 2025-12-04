package tracecheck

import (
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// globalAsyncEnqueueCollector is a singleton collector that captures async enqueues from tickers.
// It's reset automatically after each Get() call, so callers don't need to manage cleanup.
var globalAsyncEnqueueCollector = &AsyncEnqueueCollector{}

// GetGlobalAsyncEnqueueCollector returns the singleton async enqueue collector.
// This collector is automatically cleared after each Get() call, so it's safe to use
// across multiple reconcile steps without manual cleanup.
func GetGlobalAsyncEnqueueCollector() *AsyncEnqueueCollector {
	return globalAsyncEnqueueCollector
}

// AsyncEnqueueCollector stores enqueued reconcile requests captured during reconcile.
// It's thread-safe because Add may be called from a different goroutine.
type AsyncEnqueueCollector struct {
	mu       sync.Mutex
	enqueues []PendingReconcile
}

// Add records an enqueued reconcile request with its associated reconciler ID
func (ec *AsyncEnqueueCollector) Add(reconcilerID string, key types.NamespacedName) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.enqueues = append(ec.enqueues, PendingReconcile{
		ReconcilerID: reconcilerID,
		Request: reconcile.Request{
			NamespacedName: key,
		},
		Source: SourceAsyncEnqueue,
	})
}

// Get returns a copy of all captured enqueues as PendingReconcile entries and clears the collector.
// This ensures the collector is ready for the next reconcile step without manual cleanup.
func (ec *AsyncEnqueueCollector) Get() []PendingReconcile {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	result := append([]PendingReconcile{}, ec.enqueues...)
	ec.enqueues = ec.enqueues[:0] // Clear after returning
	return result
}

// Clear removes all captured enqueues (useful for testing)
func (ec *AsyncEnqueueCollector) Clear() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.enqueues = ec.enqueues[:0]
}
