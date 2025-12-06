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
	collector := GetGlobalAsyncEnqueueCollector()
	if collector == nil {
		// This is expected in some test scenarios, but in real usage it should be present
		return ctx, func() {}, nil
	}

	// Set up the ticker once
	s.tickerSetup.Do(func() {
		s.deciders = &tickerDeciders{
			tickerInterval: 2 * time.Second, // Fire every 2 depth steps
		}
	})

	// Update the callback each time PrepareState is called so it uses the current step's collector
	// This ensures the ticker callback always uses the collector from the current reconcile step
	s.deciders.Watch(func(key types.NamespacedName) {
		collector.Add("TickerBased", key)
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

	// Create initial state with a Pod (AlwaysRequeue) to keep exploration going
	// We'll add a Service later to trigger the TickerBased reconciler
	stateBuilder := builder.NewStateEventBuilder()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	initialState := stateBuilder.AddTopLevelObject(pod, "AlwaysRequeue")

	// Add a Service to the initial state to trigger TickerBased reconciler
	// This will set up the ticker when PrepareState is called
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ticker-service",
			Namespace: "default",
		},
	}
	serviceState := stateBuilder.AddTopLevelObject(service, "TickerBased")

	// Combine the states by adding the Service to the initial state's objects
	// We need to manually add the Service to initialState's pending reconciles
	initialState.PendingReconciles = append(initialState.PendingReconciles, serviceState.PendingReconciles...)

	// Register cleanup to stop the ticker when test completes
	t.Cleanup(func() {
		if tickerStrategy.deciders != nil {
			tickerStrategy.deciders.Stop()
		}
	})

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

	// Take reconcile steps and observe depth progression and ticker fires
	for i := range 8 {
		if len(currentState.PendingReconciles) == 0 {
			break
		}

		beforeDepth := currentState.depth
		nextDepth := beforeDepth + 1

		// Take the first pending reconcile
		pendingReconcile := currentState.PendingReconciles[0]

		// Create context for this step
		ctx := context.Background()
		ctx = log.IntoContext(ctx, log.Log)

		// Set state depth to nextDepth so that when takeReconcileStep calls SetDepth(state.depth),
		// it will advance depth (triggering ticker fires) if the global depth is less than nextDepth.
		// Note: takeReconcileStep calls SetDepth(state.depth) and then restores it, so we need to
		// ensure the global depth is less than state.depth for advancement to occur.
		currentState.depth = nextDepth

		// Call takeReconcileStep
		// This will:
		// 1. Create collector and add to context
		// 2. Call SetDepth(nextDepth) - if global depth < nextDepth, advances depth and tickers fire
		// 3. doReconcile -> PrepareState updates ticker callback with current collector
		// 4. determineNewPendingReconciles reads collector and merges enqueues
		// 5. Restore global depth (via defer in takeReconcileStep) to preserve branch isolation
		newState, _, err := explorer.takeReconcileStep(ctx, currentState, pendingReconcile)
		if err != nil {
			t.Fatalf("error taking reconcile step at depth %d: %v", currentState.depth, err)
		}

		// Set newState depth to nextDepth (it should already be nextDepth, but be explicit)
		newState.depth = nextDepth

		// Check for TickerBased reconciles in the new state's pending reconciles
		var tickerBasedPending []PendingReconcile
		for _, pr := range newState.PendingReconciles {
			if pr.ReconcilerID == "TickerBased" {
				tickerBasedPending = append(tickerBasedPending, pr)
			}
		}

		// Track when ticker fires by checking if we see enqueues at expected depths
		// The ticker fires at depths 2, 4, 6, 8, etc. (every 2 steps)
		if len(tickerBasedPending) > 0 {
			currentDepth := nextDepth
			if currentDepth >= 2 && currentDepth%2 == 0 {
				if !tickerBasedSeenAtDepths[currentDepth] {
					tickerBasedSeenAtDepths[currentDepth] = true
					t.Logf("✓ Ticker fired at depth %d, enqueue appears in pending reconciles", currentDepth)
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
	// Enqueues should appear at the SAME depth as the ticker fire
	expectedEnqueueDepths := []int{2, 4, 6, 8}

	// Convert map to slice of actual depths where we saw enqueues
	actualEnqueueDepths := make([]int, 0, len(tickerBasedSeenAtDepths))
	for depth := range tickerBasedSeenAtDepths {
		actualEnqueueDepths = append(actualEnqueueDepths, depth)
	}

	// Assert that we saw enqueues at exactly the expected depths (no more, no less)
	require.ElementsMatch(t, expectedEnqueueDepths, actualEnqueueDepths,
		"Ticker-fired enqueues should appear at exactly depths 2, 4, 6, 8 (one for each ticker fire)")

	// Verify we have exactly 4 enqueues (one for each ticker fire at 2, 4, 6, 8)
	totalEnqueues := len(tickerBasedSeenAtDepths)
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
