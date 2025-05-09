package emitter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/tgoodwin/sleeve/mocks"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/tag"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestAsyncEmitterBasicProcessing(t *testing.T) {
	// Set up gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create mock emitter using gomock
	mock := mocks.NewMockEmitter(ctrl)
	async := NewAsyncEmitter(mock, 10, 10)
	ctx := context.Background()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			Labels: map[string]string{
				tag.ChangeID:     "test-change-id",
				tag.TraceyRootID: "test-root-id",
			},
		},
	}

	// Set expectations for the mock
	mock.EXPECT().LogOperation(gomock.Any(), gomock.Any()).Times(1)
	mock.EXPECT().LogObjectVersion(gomock.Any(), gomock.Any(), "test-controller").Times(1)

	// Log via the async emitter
	async.Emit(ctx, pod, event.CREATE, "test-controller", "test-reconcile", "test-root-id")

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
	async := NewAsyncEmitter(mock, 5, 5)
	ctx := context.Background()

	// Create test events
	pods := make([]*corev1.Pod, 10)
	for i := 0; i < 10; i++ {
		pods[i] = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "test-pod-" + string(rune('0'+i)),
				Labels: map[string]string{
					tag.ChangeID:     "test-change-id-" + string(rune('0'+i)),
					tag.TraceyRootID: "test-root-id-" + string(rune('0'+i)),
				},
			},
		}
	}

	// Set expectations - all events should be processed
	for range pods {
		mock.EXPECT().LogOperation(gomock.Any(), gomock.Any()).Times(1)
		mock.EXPECT().LogObjectVersion(gomock.Any(), gomock.Any(), "test-controller").Times(1)
	}

	for _, p := range pods {
		async.Emit(ctx, p, event.CREATE, "test-controller", "test-reconcile", "test-root-id")
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
	async := NewAsyncEmitter(mock, 2, 1)
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
