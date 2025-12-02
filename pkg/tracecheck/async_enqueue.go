package tracecheck

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// asyncEnqueueCollectorKey is the context key for storing AsyncEnqueueCollector
type asyncEnqueueCollectorKey struct{}

// WithAsyncEnqueueCollector adds an AsyncEnqueueCollector to the context
func WithAsyncEnqueueCollector(ctx context.Context, collector *AsyncEnqueueCollector) context.Context {
	return context.WithValue(ctx, asyncEnqueueCollectorKey{}, collector)
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
	})
}

// Get returns a copy of all captured enqueues as PendingReconcile entries
func (ec *AsyncEnqueueCollector) Get() []PendingReconcile {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return append([]PendingReconcile{}, ec.enqueues...)
}

// Clear removes all captured enqueues (useful for testing)
func (ec *AsyncEnqueueCollector) Clear() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.enqueues = ec.enqueues[:0]
}

// GetAsyncEnqueueCollector retrieves the AsyncEnqueueCollector from context, or returns nil if not present
func GetAsyncEnqueueCollector(ctx context.Context) *AsyncEnqueueCollector {
	if collector, ok := ctx.Value(asyncEnqueueCollectorKey{}).(*AsyncEnqueueCollector); ok {
		return collector
	}
	return nil
}
