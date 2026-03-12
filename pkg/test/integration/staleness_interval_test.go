package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	foov1 "github.com/tgoodwin/kamera/pkg/test/integration/api/v1"
	"github.com/tgoodwin/kamera/pkg/test/integration/controller"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// buildFooBarWithIntervals sets up FooController (FooReconciler, mode-driven
// state machine) and BarController (BarReconciler, flips mode A→B at "A-1")
// with optional staleness intervals. This is the same two-controller scenario
// as TestConvergedStateIdentification, extracted for staleness testing.
func buildFooBarWithIntervals(t *testing.T, intervals []tracecheck.StalenessInterval) (*tracecheck.Explorer, tracecheck.StateNode) {
	t.Helper()

	eb := tracecheck.NewExplorerBuilder(scheme)
	eb.WithMaxDepth(15)
	eb.WithoutOptimizations()

	fooKind := "webapp.discrete.events/Foo"
	eb.WithReconciler("FooController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.FooReconciler{Client: c, Scheme: scheme}
	}).For(fooKind)

	eb.WithReconciler("BarController", func(c ctrlclient.Client) tracecheck.Reconciler {
		return &controller.BarReconciler{Client: c, Scheme: scheme}
	}).For(fooKind)

	setExplorePermuteConfig(eb, map[tracecheck.ReconcilerID]bool{
		"FooController": true,
		"BarController": true,
	})

	if len(intervals) > 0 {
		cfg := eb.Config()
		cfg.Perturbations.StalenessIntervals = intervals
		eb.SetConfig(cfg)
	}

	topLevelObj := &foov1.Foo{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				"tracey-uid":                       "foo",
				"discrete.events/sleeve-object-id": "foo-123",
			},
		},
		TypeMeta: metav1.TypeMeta{
			APIVersion: "webapp.discrete.events/v1",
			Kind:       "Foo",
		},
		Spec: foov1.FooSpec{Mode: "A"},
	}

	sb := eb.NewStateEventBuilder()
	initialState := sb.AddTopLevelObject(topLevelObj, "FooController", "BarController")
	initialState.Contents.KindSequences = canonicalizeKindSequences(initialState.Contents.KindSequences)

	explorer, err := eb.Build()
	require.NoError(t, err)

	return explorer, initialState
}

// collectStalenessInfo gathers all StaleReadInfo entries across all paths
// in the given converged states.
func collectStalenessInfo(states []tracecheck.ResultState) []tracecheck.StaleReadInfo {
	var all []tracecheck.StaleReadInfo
	for _, s := range states {
		for _, path := range s.Paths {
			for _, step := range path {
				all = append(all, step.StalenessInfo...)
			}
		}
	}
	return all
}

// countReconcilerSteps returns how many steps in the execution history belong
// to the given reconciler, across all paths in all converged states.
func countReconcilerSteps(states []tracecheck.ResultState, reconcilerID tracecheck.ReconcilerID) int {
	count := 0
	for _, s := range states {
		for _, path := range s.Paths {
			for _, step := range path {
				if step.ControllerID == reconcilerID {
					count++
				}
			}
		}
	}
	return count
}

func setupStalenessTestLogger(t *testing.T) {
	t.Helper()
	opts := zap.Options{Development: true}
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	tracecheck.SetLogger(ctrl.Log.WithName("tracecheck").V(2))
}

// TestStalenessInterval_Baseline establishes the reference behavior:
// FooController + BarController without intervals produces 2 converged states
// (A-Final and B-Final) because BarController can flip mode at "A-1".
func TestStalenessInterval_Baseline(t *testing.T) {
	setupStalenessTestLogger(t)

	explorer, initialState := buildFooBarWithIntervals(t, nil)
	result := explorer.Explore(context.Background(), initialState)

	require.Len(t, result.ConvergedStates, 2, "baseline should produce A-Final and B-Final")

	foundAFinal, foundBFinal := false, false
	for _, cs := range result.ConvergedStates {
		status, hasAnnotation, ok := summarizeState(cs, explorer.VersionManager())
		require.True(t, ok, "should be able to summarize converged state")
		if status == "A-Final" && !hasAnnotation {
			foundAFinal = true
		}
		if status == "B-Final" && hasAnnotation {
			foundBFinal = true
		}
	}
	assert.True(t, foundAFinal, "expected A-Final converged state")
	assert.True(t, foundBFinal, "expected B-Final converged state from mode flip")

	staleness := collectStalenessInfo(result.ConvergedStates)
	assert.Empty(t, staleness, "no staleness expected without intervals")
}

// TestStalenessInterval_FrozenReconcilerPreventsModeFlip verifies that
// freezing BarController's view of Foo eliminates the B-Final convergence path.
//
// BarController normally flips Foo's mode from A→B when it sees status "A-1".
// With a staleness interval freezing BarController at the initial kind sequence,
// it never observes the "A-1" transition. The mode flip never fires, and only
// A-Final is reachable — regardless of controller ordering (permutation).
func TestStalenessInterval_FrozenReconcilerPreventsModeFlip(t *testing.T) {
	setupStalenessTestLogger(t)

	intervals := []tracecheck.StalenessInterval{{
		ReconcilerID: "BarController",
		Kind:         "webapp.discrete.events/Foo",
		StaleAt:      1,
		CatchUpAt:    8, // wide window covering all transitions
		Lag:          -1, // frozen at initial sequence
	}}

	explorer, initialState := buildFooBarWithIntervals(t, intervals)
	result := explorer.Explore(context.Background(), initialState)

	// With BarController frozen, it never sees "A-1" → no mode flip → only A-Final.
	require.Len(t, result.ConvergedStates, 1, "frozen BarController should prevent B-Final")

	status, hasAnnotation, ok := summarizeState(result.ConvergedStates[0], explorer.VersionManager())
	require.True(t, ok)
	assert.Equal(t, "A-Final", status, "only A-Final should be reachable")
	assert.False(t, hasAnnotation, "no mode-flip-done annotation expected")
}

// TestStalenessInterval_StalenessInfoPopulated verifies that ReconcileResults
// carry StalenessInfo with correct details when a reconciler operates on stale data.
func TestStalenessInterval_StalenessInfoPopulated(t *testing.T) {
	setupStalenessTestLogger(t)

	intervals := []tracecheck.StalenessInterval{{
		ReconcilerID: "BarController",
		Kind:         "webapp.discrete.events/Foo",
		StaleAt:      1,
		CatchUpAt:    8,
		Lag:          -1,
	}}

	explorer, initialState := buildFooBarWithIntervals(t, intervals)
	result := explorer.Explore(context.Background(), initialState)

	staleness := collectStalenessInfo(result.ConvergedStates)
	require.NotEmpty(t, staleness, "expected staleness info for frozen BarController")

	for _, si := range staleness {
		assert.Contains(t, si.Key, "Foo", "staleness should reference Foo objects")
		assert.Greater(t, si.Lag, int64(0), "lag should be positive (controller saw older version)")
		assert.Less(t, si.ObservedRV, si.CurrentRV, "observed RV should be behind current")
	}
}

// TestStalenessInterval_TriggerFilteringReducesBarActivity verifies that
// trigger filtering prevents BarController from being re-triggered by Foo
// changes while it is stale. Compared to the baseline (no intervals),
// BarController should execute fewer steps.
func TestStalenessInterval_TriggerFilteringReducesBarActivity(t *testing.T) {
	setupStalenessTestLogger(t)

	// Baseline: count BarController steps without intervals.
	explorer, initialState := buildFooBarWithIntervals(t, nil)
	baselineResult := explorer.Explore(context.Background(), initialState)
	baselineBarSteps := countReconcilerSteps(baselineResult.ConvergedStates, "BarController")

	// With intervals: BarController is stale on Foo, so trigger filtering
	// prevents re-triggering during the window.
	intervals := []tracecheck.StalenessInterval{{
		ReconcilerID: "BarController",
		Kind:         "webapp.discrete.events/Foo",
		StaleAt:      1,
		CatchUpAt:    8,
		Lag:          -1,
	}}
	explorer2, initialState2 := buildFooBarWithIntervals(t, intervals)
	intervalResult := explorer2.Explore(context.Background(), initialState2)
	intervalBarSteps := countReconcilerSteps(intervalResult.ConvergedStates, "BarController")

	assert.Less(t, intervalBarSteps, baselineBarSteps,
		"BarController should run fewer times when stale (trigger filtering active): got %d vs baseline %d",
		intervalBarSteps, baselineBarSteps)
}

// TestStalenessInterval_WindowOutsideRange verifies that a staleness window
// that never activates (staleAt far beyond actual kind sequences) produces
// the same convergence behavior as the baseline.
func TestStalenessInterval_WindowOutsideRange(t *testing.T) {
	setupStalenessTestLogger(t)

	intervals := []tracecheck.StalenessInterval{{
		ReconcilerID: "BarController",
		Kind:         "webapp.discrete.events/Foo",
		StaleAt:      50,
		CatchUpAt:    100,
		Lag:          -1,
	}}

	explorer, initialState := buildFooBarWithIntervals(t, intervals)
	result := explorer.Explore(context.Background(), initialState)

	// Window never activates — should match baseline: 2 converged states.
	require.Len(t, result.ConvergedStates, 2,
		"inactive window should not affect convergence")

	foundAFinal, foundBFinal := false, false
	for _, cs := range result.ConvergedStates {
		status, _, ok := summarizeState(cs, explorer.VersionManager())
		if ok && status == "A-Final" {
			foundAFinal = true
		}
		if ok && status == "B-Final" {
			foundBFinal = true
		}
	}
	assert.True(t, foundAFinal, "expected A-Final")
	assert.True(t, foundBFinal, "expected B-Final (window inactive)")

	staleness := collectStalenessInfo(result.ConvergedStates)
	assert.Empty(t, staleness, "no staleness when window is outside sequence range")
}
