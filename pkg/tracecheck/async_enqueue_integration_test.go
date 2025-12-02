package tracecheck

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// tickerDeciders simulates a component that uses a ticker to enqueue reconciles
type tickerDeciders struct {
	tickerInterval time.Duration
	watchCallback  func(types.NamespacedName)
	ticker         *simclock.Ticker
	mu             sync.Mutex
	stopped        bool
}

func (t *tickerDeciders) Watch(callback func(types.NamespacedName)) {
	t.mu.Lock()
	t.watchCallback = callback
	t.mu.Unlock()

	// Create a ticker that fires every tickerInterval
	t.ticker = simclock.NewTicker(t.tickerInterval)
	// Note: We don't start a goroutine here. Instead, we check for ticks synchronously
	// via checkTicks() which is called after SetDepth advances. This ensures fully
	// synchronous behavior in logical time.
}

// checkTicks synchronously checks for pending ticks and calls the callback if any are available.
// This should be called after SetDepth advances to ensure ticker callbacks run synchronously.
func (t *tickerDeciders) checkTicks() {
	t.mu.Lock()
	cb := t.watchCallback
	stopped := t.stopped
	ticker := t.ticker
	t.mu.Unlock()

	if stopped || ticker == nil || cb == nil {
		return
	}

	// Check for pending ticks synchronously (non-blocking)
	// Since ticker fires synchronously when SetDepth advances, we can check the channel
	// immediately after depth advances to see if a tick occurred.
	for {
		select {
		case <-ticker.C():
			// Ticker fired - call the callback synchronously
			key := types.NamespacedName{Namespace: "default", Name: "ticker-resource"}
			cb(key)
			// Continue checking in case multiple ticks are queued (shouldn't happen with buffered channel size 1, but be safe)
		default:
			// No more ticks available
			return
		}
	}
}

func (t *tickerDeciders) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
	if t.ticker != nil {
		t.ticker.Stop()
	}
}

// AlwaysRequeueStrategy is a strategy that always requeues to keep exploration going
type AlwaysRequeueStrategy struct {
	recorder replay.EffectRecorder
}

func (s *AlwaysRequeueStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (s *AlwaysRequeueStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	// Always requeue to keep the exploration going
	return reconcile.Result{Requeue: true}, nil
}

// TickerBasedStrategy is a strategy that sets up a ticker to enqueue reconciles
type TickerBasedStrategy struct {
	recorder    replay.EffectRecorder
	deciders    *tickerDeciders
	tickerSetup sync.Once
}

func (s *TickerBasedStrategy) PrepareState(ctx context.Context, state []runtime.Object) (context.Context, func(), error) {
	// Get the async enqueue collector from context
	collector := GetAsyncEnqueueCollector(ctx)
	if collector == nil {
		// This is expected in some test scenarios, but in real usage it should be present
		return ctx, func() {}, nil
	}

	// Set up the ticker once
	s.tickerSetup.Do(func() {
		s.deciders = &tickerDeciders{
			tickerInterval: 2 * time.Second, // Fire every 2 depth steps
		}
		s.deciders.Watch(func(key types.NamespacedName) {
			// Capture the enqueue with the correct reconciler ID
			collector.Add("TickerBased", key)
		})
	})

	return ctx, func() {}, nil
}

func (s *TickerBasedStrategy) ReconcileAtState(ctx context.Context, name types.NamespacedName) (reconcile.Result, error) {
	// This controller doesn't do much - the ticker drives the enqueues
	return reconcile.Result{}, nil
}

// TestAsyncEnqueueCollector_IntegrationWithTicker verifies that ticker-fired enqueues
// are properly captured by the AsyncEnqueueCollector and appear in pending reconciles.
func TestAsyncEnqueueCollector_IntegrationWithTicker(t *testing.T) {
	// Reset simclock to a known state
	restore := simclock.SetDepth(0)
	defer restore()

	// Create a scheme
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create an explorer builder
	builder := NewExplorerBuilder(scheme)

	// Set up the AlwaysRequeue strategy to keep exploration going
	builder.WithCustomStrategy("AlwaysRequeue", func(r replay.EffectRecorder) Strategy {
		return &AlwaysRequeueStrategy{recorder: r}
	})

	// Set up the TickerBased strategy
	tickerStrategy := &TickerBasedStrategy{}
	builder.WithCustomStrategy("TickerBased", func(r replay.EffectRecorder) Strategy {
		tickerStrategy.recorder = r
		return tickerStrategy
	})

	// Assign reconcilers to kinds
	builder.AssignReconcilerToKind("AlwaysRequeue", "core/Pod")
	builder.AssignReconcilerToKind("TickerBased", "core/Service")

	// Build the explorer
	explorer, err := builder.Build("test")
	if err != nil {
		t.Fatalf("failed to build explorer: %v", err)
	}

	// Create initial state with a Pod that will trigger AlwaysRequeue
	// This keeps the exploration going so we can observe ticker fires
	stateBuilder := builder.NewStateEventBuilder()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	initialState := stateBuilder.AddTopLevelObject(pod, "AlwaysRequeue")

	// Set up the ticker BEFORE we start taking reconcile steps
	// The ticker needs to be registered in the global ticker registry so it fires when depth advances
	// We'll create a collector that will be used during reconcile steps
	testCollector := &AsyncEnqueueCollector{}

	// Set up the ticker directly (similar to what PrepareState would do)
	// This ensures the ticker is registered before depth advances
	// Ensure we're at depth 0 when creating the ticker
	restoreDepth := simclock.SetDepth(0)
	defer restoreDepth()

	tickerStrategy.deciders = &tickerDeciders{
		tickerInterval: 2 * time.Second, // Fire every 2 depth steps
	}
	tickerStrategy.deciders.Watch(func(key types.NamespacedName) {
		// Capture the enqueue with the correct reconciler ID
		// Note: In real usage, this would be called with the collector from context
		// For testing, we'll use our test collector
		testCollector.Add("TickerBased", key)
	})

	// Register cleanup to stop the ticker when test completes
	t.Cleanup(func() {
		if tickerStrategy.deciders != nil {
			tickerStrategy.deciders.Stop()
		}
	})

	t.Logf("Ticker set up and registered. Will fire at depths 2, 4, 6, etc.")

	// Set max depth and mode
	explorer.config.MaxDepth = 10
	explorer.config.mode = DepthFirst

	// Track depth progression and ticker-fired enqueues
	type stepResult struct {
		stepNum            int
		beforeDepth        int
		afterDepth         int
		tickerBasedPending []PendingReconcile
		allPending         []PendingReconcile
	}

	var stepResults []stepResult
	currentState := initialState

	// Verify initial state depth
	if initialState.depth != 0 {
		t.Fatalf("Expected initial state depth to be 0, got %d", initialState.depth)
	}

	// Track when ticker fires and enqueues appear
	// Enqueues should appear at the same depth as the ticker fire (2, 4, 6, etc.)
	tickerBasedSeenAtDepths := make(map[int]bool)
	tickerBasedFirstSeenAt := make(map[int]int) // depth -> step number where first seen

	// Take reconcile steps and observe depth progression and ticker fires
	for i := 0; i < 8; i++ {
		if len(currentState.PendingReconciles) == 0 {
			break
		}

		beforeDepth := currentState.depth

		// Take the first pending reconcile
		pendingReconcile := currentState.PendingReconciles[0]

		// Create a context for this reconcile step with the test collector
		ctx := context.Background()
		ctx = log.IntoContext(ctx, log.Log)
		ctx = WithAsyncEnqueueCollector(ctx, testCollector)

		// Take a reconcile step (this may fire tickers when SetDepth is called)
		// The key insight: SetDepth only advances tickers if depth > prevDepth.
		// takeReconcileStep calls SetDepth(state.depth), but state.depth is the OLD depth (beforeDepth).
		// We want the ticker to fire when we advance TO the NEW depth (beforeDepth+1).
		// Strategy: Set global depth to beforeDepth, then advance to beforeDepth+1 to trigger ticker fires.
		// Check for ticks synchronously immediately after depth advances, then call takeReconcileStep.
		// Set global depth to beforeDepth
		prevGlobalDepth := simclock.SetDepth(beforeDepth)
		// Advance to beforeDepth+1 to trigger ticker fires synchronously
		advanceRestore := simclock.SetDepth(beforeDepth + 1)

		// Check for ticks synchronously RIGHT AFTER depth advances
		// This ensures the callback runs immediately and the enqueue is captured before takeReconcileStep reads the collector
		// The ticker fires at depths 2, 4, 6, etc. (every 2 steps starting from depth 2)
		// So we should check when beforeDepth+1 is 2, 4, 6, etc.
		if tickerStrategy.deciders != nil {
			tickerStrategy.deciders.checkTicks()
		}

		newState, _, err := explorer.takeReconcileStep(ctx, currentState, pendingReconcile)
		if err != nil {
			t.Fatalf("error taking reconcile step at depth %d: %v", currentState.depth, err)
		}

		// Restore the advance (sets depth back to beforeDepth)
		advanceRestore()
		// Restore the original global depth
		prevGlobalDepth()

		// Manually increment depth (since we're calling takeReconcileStep directly, not through the main loop)
		// The main exploration loop sets depth at line 580, but we're bypassing that
		newState.depth = beforeDepth + 1

		// Check for TickerBased reconciles in the new state's pending reconciles
		// These come from the collector that captured ticker-fired enqueues
		var tickerBasedPending []PendingReconcile
		for _, pr := range newState.PendingReconciles {
			if pr.ReconcilerID == "TickerBased" {
				tickerBasedPending = append(tickerBasedPending, pr)
			}
		}

		// Track when ticker fires by checking if we captured a new enqueue this step
		// The ticker fires when we advance to depths 2, 4, 6, 8, etc., and the enqueue
		// is captured at that same depth (beforeDepth+1 where beforeDepth+1 is 2, 4, 6, 8, etc.)
		// So if beforeDepth+1 is 2, 4, 6, or 8, and we captured a new enqueue, the ticker fired at that depth
		if len(tickerBasedPending) > 0 {
			currentDepth := beforeDepth + 1
			// Check if this is a ticker fire depth (every 2 steps starting from 2)
			if currentDepth >= 2 && currentDepth%2 == 0 {
				// The ticker fired at depth currentDepth, and we see the enqueue in pending reconciles
				// Record this as the enqueue appearing at the ticker fire depth
				if !tickerBasedSeenAtDepths[currentDepth] {
					tickerBasedSeenAtDepths[currentDepth] = true
					tickerBasedFirstSeenAt[currentDepth] = i
					t.Logf("✓ Ticker fired at depth %d, enqueue captured and appears in pending reconciles", currentDepth)
				}
			}
		}

		stepResults = append(stepResults, stepResult{
			stepNum:            i,
			beforeDepth:        beforeDepth,
			afterDepth:         newState.depth,
			tickerBasedPending: tickerBasedPending,
			allPending:         append([]PendingReconcile{}, newState.PendingReconciles...),
		})

		t.Logf("Step %d: depth %d -> %d, pending: %d (TickerBased: %d)", i, beforeDepth, newState.depth, len(newState.PendingReconciles), len(tickerBasedPending))

		currentState = newState
	}

	// Verify depth progression
	if len(stepResults) == 0 {
		t.Fatalf("No steps were taken")
	}

	// Verify that depth incremented properly
	expectedFinalDepth := len(stepResults)
	actualFinalDepth := stepResults[len(stepResults)-1].afterDepth
	if actualFinalDepth != expectedFinalDepth {
		t.Errorf("Expected final depth to be %d (number of steps), but got %d", expectedFinalDepth, actualFinalDepth)
	}

	// Verify ticker fires at expected depths
	// The ticker fires at depths 2, 4, 6, 8, etc. (every 2 seconds = every 2 depth steps)
	// When SetDepth is called with depth 2, the ticker fires synchronously
	// The enqueue is captured synchronously when checkTicks() is called after depth advances
	// Since we call checkTicks() before takeReconcileStep, the enqueue is captured and appears
	// in the same step's pending reconciles. So if the ticker fires at depth 2, the enqueue appears at depth 2.
	// Enqueues should appear at the SAME depth as the ticker fire, not the next depth.
	// For 8 steps (depths 0-8), we expect ticker fires at depths 2, 4, 6, 8
	expectedEnqueueDepths := []int{2, 4, 6, 8} // Expected depths where enqueues appear (same as ticker fire depths)
	// Note: tickerBasedSeenAtDepths is populated in the loop above when we detect ticker fires
	// This ensures we track enqueues at the ticker fire depths, not when they first appear in pending reconciles

	// Convert map to slice of actual depths where we saw enqueues
	actualEnqueueDepths := make([]int, 0, len(tickerBasedSeenAtDepths))
	for depth := range tickerBasedSeenAtDepths {
		actualEnqueueDepths = append(actualEnqueueDepths, depth)
	}

	// Assert that we saw enqueues at exactly the expected depths (no more, no less)
	require.ElementsMatch(t, expectedEnqueueDepths, actualEnqueueDepths,
		"Ticker-fired enqueues should appear at exactly depths 2, 4, 6, 8 (one for each ticker fire)")

	// Verify we have exactly 4 enqueues (one for each ticker fire at 2, 4, 6, 8)
	// The ticker fires at depths 2, 4, 6, 8, and we should see exactly 4 enqueues
	// (one at depth 2, one at depth 4, one at depth 6, one at depth 8)
	// Count unique enqueues by checking the collector's cumulative count
	// Note: We're using a shared collector, so this gives us the total across all steps
	totalEnqueues := len(testCollector.Get())
	require.Equal(t, 4, totalEnqueues, "Expected exactly 4 ticker-fired enqueues (one for each fire at depths 2, 4, 6, 8)")

	// Verify depth progression was correct
	if len(stepResults) > 0 {
		firstDepth := stepResults[0].beforeDepth
		lastDepth := stepResults[len(stepResults)-1].afterDepth
		expectedSteps := lastDepth - firstDepth
		if len(stepResults) != expectedSteps {
			t.Errorf("Depth progression mismatch: took %d steps but depth went from %d to %d (expected %d steps)",
				len(stepResults), firstDepth, lastDepth, expectedSteps)
		} else {
			t.Logf("✓ Depth progression verified: %d steps, depth 0 -> %d", len(stepResults), lastDepth)
		}
	}
}
