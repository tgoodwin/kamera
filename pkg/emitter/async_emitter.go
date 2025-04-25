package emitter

import (
	"context"
	"fmt"
	"sync"

	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// QueueItem represents an item in the async queue
type QueueItem struct {
	ItemType string // "operation" or "record"
	Event    *event.Event
	Record   snapshot.Record
}

// AsyncEmitter is an emitter that buffers events in a channel to be processed asynchronously
type AsyncEmitter struct {
	underlyingEmitter Emitter
	queue             chan QueueItem
	wg                sync.WaitGroup
	shutdown          chan struct{}
	shutdownOnce      sync.Once
}

// NewAsyncEmitter creates a new AsyncEmitter with the specified buffer size
func NewAsyncEmitter(underlying Emitter, bufferSize int) *AsyncEmitter {
	ae := &AsyncEmitter{
		underlyingEmitter: underlying,
		queue:             make(chan QueueItem, bufferSize),
		shutdown:          make(chan struct{}),
	}

	// Start the consumer goroutine
	ae.wg.Add(1)
	go ae.processQueue()

	return ae
}

// processQueue processes items from the queue until shutdown is signaled
func (ae *AsyncEmitter) processQueue() {
	defer ae.wg.Done()

	// Use background context for processing queued items
	backgroundCtx := context.Background()

	for {
		select {
		case item := <-ae.queue:
			// Process the item
			if item.ItemType == "operation" {
				ae.underlyingEmitter.LogOperation(backgroundCtx, item.Event)
			} else if item.ItemType == "record" {
				ae.underlyingEmitter.LogObjectVersion(backgroundCtx, item.Record)
			}
		case <-ae.shutdown:
			// Drain the queue before exiting
			ae.drainQueue()
			return
		}
	}
}

// drainQueue processes all remaining items in the queue
func (ae *AsyncEmitter) drainQueue() {
	// Use background context for processing remaining queued items
	backgroundCtx := context.Background()

	// Keep processing until the queue is empty
	for {
		select {
		case item := <-ae.queue:
			if item.ItemType == "operation" {
				ae.underlyingEmitter.LogOperation(backgroundCtx, item.Event)
			} else if item.ItemType == "record" {
				ae.underlyingEmitter.LogObjectVersion(backgroundCtx, item.Record)
			}
		default:
			// Queue is empty
			return
		}
	}
}

// LogOperation queues an operation event, blocking if the queue is full
func (ae *AsyncEmitter) LogOperation(ctx context.Context, e *event.Event) {
	select {
	case ae.queue <- QueueItem{
		ItemType: "operation",
		Event:    e,
	}:
		// Item queued successfully
	case <-ctx.Done():
		// Context canceled, log a warning
		fmt.Println("WARNING: Context canceled while trying to queue operation event")
	}
}

// LogObjectVersion queues an object version record, blocking if the queue is full
func (ae *AsyncEmitter) LogObjectVersion(ctx context.Context, r snapshot.Record) {
	select {
	case ae.queue <- QueueItem{
		ItemType: "record",
		Record:   r,
	}:
		// Item queued successfully
	case <-ctx.Done():
		// Context canceled, log a warning
		fmt.Println("WARNING: Context canceled while trying to queue object version record")
	}
}

// Shutdown gracefully shuts down the AsyncEmitter, ensuring all queued events are processed
// It's safe to call this method multiple times
func (ae *AsyncEmitter) Shutdown() {
	ae.shutdownOnce.Do(func() {
		close(ae.shutdown)
		ae.wg.Wait()
	})
}
