package emitter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/mocks"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

func TestAsyncEmitterBasicProcessing(t *testing.T) {
	// Set up gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock emitter using gomock
	mock := mocks.NewMockEmitter(ctrl)
	async := NewAsyncEmitter(mock, 10)
	ctx := context.Background()

	// Create test event and record
	testEvent := &event.Event{ID: "test-event-1"}
	testRecord := snapshot.Record{ObjectID: "test-object-1"}

	// Set expectations for the mock
	mock.EXPECT().LogOperation(gomock.Any(), testEvent).Times(1)
	mock.EXPECT().LogObjectVersion(gomock.Any(), testRecord).Times(1)

	// Log via the async emitter
	async.LogOperation(ctx, testEvent)
	async.LogObjectVersion(ctx, testRecord)

	// Give the async emitter time to process
	time.Sleep(100 * time.Millisecond)

	// Clean up
	async.Shutdown()
}

func TestAsyncEmitterShutdown(t *testing.T) {
	// Set up gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock emitter using gomock
	mock := mocks.NewMockEmitter(ctrl)
	async := NewAsyncEmitter(mock, 5)
	ctx := context.Background()

	// Create test events
	events := make([]*event.Event, 10)
	for i := 0; i < 10; i++ {
		events[i] = &event.Event{ID: "test-event-" + string(rune('0'+i))}
	}

	// Set expectations - all events should be processed
	for _, event := range events {
		mock.EXPECT().LogOperation(gomock.Any(), event).Times(1)
	}

	// Send events
	for _, event := range events {
		async.LogOperation(ctx, event)
	}

	// Initiate shutdown which should process all events
	async.Shutdown()

	// Verify we can call Shutdown multiple times without issue
	async.Shutdown()
}

func TestAsyncEmitterBlocking(t *testing.T) {
	// Set up gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a delayed mock that simulates slow processing
	mock := mocks.NewMockEmitter(ctrl)

	// Set up the mock to delay on each call
	mock.EXPECT().
		LogOperation(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, e *event.Event) {
			// Simulate processing delay
			time.Sleep(50 * time.Millisecond)
		}).
		AnyTimes()

	// Create async emitter with small buffer
	async := NewAsyncEmitter(mock, 2)
	ctx := context.Background()

	// Create test events
	events := make([]*event.Event, 10)
	for i := 0; i < len(events); i++ {
		events[i] = &event.Event{ID: "test-event-" + string(rune('0'+i))}
	}

	// We'll use a WaitGroup to track when all sends complete
	var wg sync.WaitGroup
	wg.Add(len(events))

	// Start a goroutine that sends all events
	go func() {
		for _, event := range events {
			async.LogOperation(ctx, event)
			wg.Done()
		}
	}()

	// Set a timeout for the test
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for either the sends to complete or a timeout
	select {
	case <-done:
		// All sends completed successfully
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out - blocking behavior not working correctly")
	}

	// Shutdown to ensure all queued events are processed
	async.Shutdown()
}

func TestAsyncEmitterContextCancellation(t *testing.T) {
	// Set up gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock emitter
	mock := mocks.NewMockEmitter(ctrl)
	async := NewAsyncEmitter(mock, 1)

	// Create a context we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Set up the mock to delay on each call
	mock.EXPECT().
		LogOperation(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, e *event.Event) {
			time.Sleep(500 * time.Millisecond)
		}).
		AnyTimes()

	// Fill the buffer
	async.LogOperation(ctx, &event.Event{ID: "test-event-1"})

	// Cancel the context
	cancel()

	// Try to send another event with the canceled context
	// This should return quickly due to context cancellation
	start := time.Now()
	async.LogOperation(ctx, &event.Event{ID: "test-event-2"})
	duration := time.Since(start)

	// Should return almost immediately, not block for long
	assert.Less(t, duration, 100*time.Millisecond,
		"LogOperation with canceled context should return quickly")

	// Clean up
	async.Shutdown()
}
