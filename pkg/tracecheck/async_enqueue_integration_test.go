package tracecheck

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	// hook to capture tick times deterministically in tests
	onTick func(time.Time)
}

func (t *tickerDeciders) Watch(callback func(types.NamespacedName)) {
	t.mu.Lock()
	t.watchCallback = callback
	t.mu.Unlock()

	// Create a ticker that fires every tickerInterval (or reuse existing one)
	if t.ticker == nil {
		t.ticker = simclock.NewTicker(t.tickerInterval)
	}

	// Register a synchronous callback to record tick times deterministically.
	if t.onTick != nil {
		simclock.RegisterTickerCallback(t.ticker, func() {
			t.onTick(simclock.Now())
		})
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

type tickerFireTracker struct {
	seen      map[int]bool
	fires     []int
	intervalS int
}

func newTickerFireTracker(intervalSeconds int) *tickerFireTracker {
	return &tickerFireTracker{
		seen:      make(map[int]bool),
		fires:     []int{},
		intervalS: intervalSeconds,
	}
}

func (t *tickerFireTracker) record(ts time.Time) {
	depth := int(ts.Sub(time.Unix(0, 0)) / time.Second)
	boundary := depth - depth%t.intervalS
	if boundary == 0 {
		return
	}
	if t.seen[boundary] {
		return
	}
	t.seen[boundary] = true
	t.fires = append(t.fires, boundary)
}

func newTickerBasedStrategy(tracker *tickerFireTracker) *TickerBasedStrategy {
	deciders := &tickerDeciders{
		tickerInterval: 2 * time.Second,
		onTick:         tracker.record,
		ticker:         simclock.NewTicker(2 * time.Second),
	}

	if deciders.onTick != nil {
		simclock.RegisterTickerCallback(deciders.ticker, func() {
			deciders.onTick(simclock.Now())
		})
	}

	return &TickerBasedStrategy{deciders: deciders}
}

func registerTickerCleanup(t *testing.T, strategy *TickerBasedStrategy) {
	t.Helper()
	t.Cleanup(func() {
		if strategy != nil && strategy.deciders != nil {
			strategy.deciders.Stop()
		}
	})
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
		if s.deciders == nil {
			s.deciders = &tickerDeciders{
				tickerInterval: 2 * time.Second, // Fire every 2 depth steps
			}
		} else if s.deciders.tickerInterval == 0 {
			s.deciders.tickerInterval = 2 * time.Second
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

type stepResult struct {
	stepNum            int
	beforeDepth        int
	afterDepth         int
	tickerBasedPending []PendingReconcile
	allPending         []PendingReconcile
}

func runReconcileStep(t *testing.T, explorer *Explorer, state StateNode, step int) (StateNode, stepResult) {
	t.Helper()

	require.NotEmpty(t, state.PendingReconciles, "step %d has no pending reconciles", step)

	beforeDepth := state.depth
	nextDepth := beforeDepth + 1
	state.depth = nextDepth

	ctx := log.IntoContext(context.Background(), log.Log)

	pr := state.PendingReconciles[0]
	reconcileResult, err := explorer.takeReconcileStep(ctx, state, pr)
	require.NoErrorf(t, err, "error taking reconcile step at depth %d", state.depth)

	stepLogger := log.FromContext(ctx)

	beforeState := maps.Clone(state.Objects())
	beforeSequences := maps.Clone(state.Contents.KindSequences)

	reconcileResult.StateBefore = beforeState
	reconcileResult.KindSeqBefore = beforeSequences

	nextState, nextSequences, nextEvents := explorer.applyEffects(stepLogger, state, reconcileResult)

	newPending := explorer.determineNewPendingReconciles(ctx, state, pr, reconcileResult)

	reconcileResult.StateAfter = nextState
	reconcileResult.KindSeqAfter = nextSequences
	reconcileResult.PendingReconciles = newPending

	newState := StateNode{
		Contents:                 NewStateSnapshot(nextState, nextSequences, nextEvents),
		PendingReconciles:        newPending,
		parent:                   &state,
		action:                   reconcileResult,
		divergenceKey:            state.divergenceKey,
		stuckReconcilerPositions: maps.Clone(state.stuckReconcilerPositions),
		ExecutionHistory:         append(slices.Clone(state.ExecutionHistory), reconcileResult),
		depth:                    nextDepth,
	}

	var tickerBasedPending []PendingReconcile
	for _, pr := range newState.PendingReconciles {
		if pr.ReconcilerID == "TickerBased" {
			tickerBasedPending = append(tickerBasedPending, pr)
		}
	}

	return newState, stepResult{
		stepNum:            step,
		beforeDepth:        beforeDepth,
		afterDepth:         newState.depth,
		tickerBasedPending: tickerBasedPending,
		allPending:         append([]PendingReconcile{}, newState.PendingReconciles...),
	}
}

// TestAsyncEnqueueCollector_IntegrationWithTicker verifies that ticker-fired enqueues
// are properly captured by the AsyncEnqueueCollector and appear in pending reconciles.
func TestAsyncEnqueueCollector_IntegrationWithTicker(t *testing.T) {
	// Reset simclock to a known state
	restore := simclock.SetDepth(0)
	defer restore()

	// Create a scheme
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	// Create an explorer builder
	builder := NewExplorerBuilder(scheme)

	// Set up the AlwaysRequeue strategy to keep exploration going
	builder.WithCustomStrategy("AlwaysRequeue", func(r replay.EffectRecorder) Strategy {
		return &AlwaysRequeueStrategy{recorder: r}
	}).For("core/Pod")

	// Set up the TickerBased strategy with deterministic ticker tracking.
	tickerTracker := newTickerFireTracker(2)
	tickerStrategy := newTickerBasedStrategy(tickerTracker)
	builder.WithCustomStrategy("TickerBased", func(r replay.EffectRecorder) Strategy {
		tickerStrategy.recorder = r
		return tickerStrategy
	}).For("core/Service")

	t.Cleanup(func() {
		tickerStrategy.deciders.Stop()
	})

	// Build the explorer
	explorer, err := builder.Build("test")
	require.NoError(t, err, "failed to build explorer")

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

	// Set max depth
	explorer.Config.MaxDepth = 10

	var stepResults []stepResult
	currentState := initialState

	// Verify initial state depth
	require.Zero(t, initialState.depth, "initial state depth")

	for i := range 8 {
		if len(currentState.PendingReconciles) == 0 {
			break
		}

		var step stepResult
		currentState, step = runReconcileStep(t, explorer, currentState, i)
		stepResults = append(stepResults, step)

		t.Logf("Step %d: depth %d -> %d, pending: %d (TickerBased: %d)", step.stepNum, step.beforeDepth, step.afterDepth, len(step.allPending), len(step.tickerBasedPending))
	}

	// Verify depth progression
	require.NotEmpty(t, stepResults, "no steps were taken")

	// Verify that depth incremented properly
	expectedFinalDepth := len(stepResults)
	actualFinalDepth := stepResults[len(stepResults)-1].afterDepth
	require.Equal(t, expectedFinalDepth, actualFinalDepth, "final depth mismatch")

	// Verify the ticker fired at the expected depths (allowing extra fires if depth resets occur).
	expectedFires := []int{2, 4, 6, 8}
	assert.Equal(t, expectedFires, tickerTracker.fires)

	// Verify depth progression was correct
	if len(stepResults) > 0 {
		firstDepth := stepResults[0].beforeDepth
		lastDepth := stepResults[len(stepResults)-1].afterDepth
		expectedSteps := lastDepth - firstDepth
		require.Equalf(t, expectedSteps, len(stepResults), "Depth progression mismatch: took %d steps but depth went from %d to %d (expected %d steps)", len(stepResults), firstDepth, lastDepth, expectedSteps)
		t.Logf("✓ Depth progression verified: %d steps, depth 0 -> %d", len(stepResults), lastDepth)
	}
}
