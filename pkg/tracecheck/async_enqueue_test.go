package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
)

func TestAsyncEnqueueCollector_Add(t *testing.T) {
	collector := &AsyncEnqueueCollector{}

	key1 := types.NamespacedName{Namespace: "default", Name: "foo"}
	key2 := types.NamespacedName{Namespace: "default", Name: "bar"}

	collector.Add("Reconciler1", key1)
	collector.Add("Reconciler2", key2)

	enqueues := collector.Get()
	assert.Len(t, enqueues, 2)
	assert.Equal(t, "Reconciler1", enqueues[0].ReconcilerID)
	assert.Equal(t, key1, enqueues[0].Request.NamespacedName)
	assert.Equal(t, "Reconciler2", enqueues[1].ReconcilerID)
	assert.Equal(t, key2, enqueues[1].Request.NamespacedName)
}

func TestAsyncEnqueueCollector_Get(t *testing.T) {
	collector := &AsyncEnqueueCollector{}

	// Get on empty collector should return empty slice
	enqueues := collector.Get()
	assert.NotNil(t, enqueues)
	assert.Len(t, enqueues, 0)

	// Add some enqueues
	key1 := types.NamespacedName{Namespace: "default", Name: "foo"}
	collector.Add("Reconciler1", key1)

	// Get should return a copy and clear the collector
	enqueues1 := collector.Get()
	assert.Len(t, enqueues1, 1)

	// Second Get should return empty because collector was cleared
	enqueues2 := collector.Get()
	assert.Len(t, enqueues2, 0)
	assert.NotSame(t, enqueues1, enqueues2) // Different slices
}

func TestAsyncEnqueueCollector_Clear(t *testing.T) {
	collector := &AsyncEnqueueCollector{}

	key1 := types.NamespacedName{Namespace: "default", Name: "foo"}
	key2 := types.NamespacedName{Namespace: "default", Name: "bar"}

	collector.Add("Reconciler1", key1)
	collector.Add("Reconciler2", key2)

	assert.Len(t, collector.Get(), 2)

	collector.Clear()

	assert.Len(t, collector.Get(), 0)
}

func TestAsyncEnqueueCollector_ThreadSafety(t *testing.T) {
	collector := &AsyncEnqueueCollector{}

	// Add enqueues from multiple goroutines
	done := make(chan bool)
	numGoroutines := 10
	enqueuesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < enqueuesPerGoroutine; j++ {
				key := types.NamespacedName{
					Namespace: "default",
					Name:      types.NamespacedName{Name: "foo"}.Name,
				}
				collector.Add("Reconciler1", key)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Should have all enqueues
	enqueues := collector.Get()
	assert.Len(t, enqueues, numGoroutines*enqueuesPerGoroutine)
}

func TestGetGlobalAsyncEnqueueCollector(t *testing.T) {
	// Should return the same singleton instance
	collector1 := GetGlobalAsyncEnqueueCollector()
	collector2 := GetGlobalAsyncEnqueueCollector()
	assert.NotNil(t, collector1)
	assert.Same(t, collector1, collector2)
}
