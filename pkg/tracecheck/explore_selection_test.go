package tracecheck

import (
	"math/rand"
	"testing"

	"github.com/tgoodwin/kamera/pkg/util"
)

func TestSelectPendingReconcileDFSSelectsFirst(t *testing.T) {
	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
		testPending("controller-c", "default", "c"),
	)
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
		},
	}

	got, err := explorer.selectPendingReconcile(state)
	if err != nil {
		t.Fatalf("selectPendingReconcile() error = %v", err)
	}
	if got != state.PendingReconciles[0] {
		t.Fatalf("expected first pending reconcile in DFS mode")
	}
}

func TestSelectPendingReconcileMonteCarloDeterministicBySeed(t *testing.T) {
	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
		testPending("controller-c", "default", "c"),
	)
	mc := MonteCarloConfig{
		Seed:          1337,
		TrialIndex:    7,
		ScenarioGroup: "scenario/input#2",
	}

	expectedRNG := rand.New(rand.NewSource(mc.DerivedSeed()))
	expected1 := expectedRNG.Intn(len(state.PendingReconciles))
	expected2 := expectedRNG.Intn(len(state.PendingReconciles))

	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeMonteCarlo,
			MonteCarlo: mc,
		},
	}

	got1, err := explorer.selectPendingReconcile(state)
	if err != nil {
		t.Fatalf("selectPendingReconcile() first call error = %v", err)
	}
	got2, err := explorer.selectPendingReconcile(state)
	if err != nil {
		t.Fatalf("selectPendingReconcile() second call error = %v", err)
	}

	if got1 != state.PendingReconciles[expected1] {
		t.Fatalf("first monte carlo selection mismatch: expected idx=%d", expected1)
	}
	if got2 != state.PendingReconciles[expected2] {
		t.Fatalf("second monte carlo selection mismatch: expected idx=%d", expected2)
	}
}

func TestSelectPendingReconcileErrorsOnEmptyPending(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{SearchMode: SearchModeDFS},
	}

	if _, err := explorer.selectPendingReconcile(testState()); err == nil {
		t.Fatalf("expected error for empty pending reconcile list")
	}
}

func TestGetPossibleViewsForReconcileMonteCarloSamplesSingleViewDeterministically(t *testing.T) {
	const (
		reconcilerID ReconcilerID = "MockReconciler"
		group                     = "autoscaling.internal.knative.dev"
		kind                      = "PodAutoscaler"
	)
	kindKey := util.CanonicalGroupKind(group, kind)

	state := stateNodeFromEvents(t,
		newTestStateEvent(1, group, kind, "pa-v1"),
		newTestStateEvent(2, group, kind, "pa-v2"),
	)

	staleness := map[ReconcilerID]StalenessConfig{
		reconcilerID: {
			MaxRestarts: 2,
			StaleReadBounds: LookbackLimits{
				kindKey: 2,
			},
		},
	}
	deps := ResourceDeps{
		kindKey: util.NewSet(reconcilerID),
	}

	dfsExplorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				Staleness: staleness,
			},
		},
		dependencies:    deps,
		priorityHandler: &PriorityManager{},
		stats:           NewExploreStats(),
	}
	dfsViews, err := dfsExplorer.getPossibleViewsForReconcile(state, reconcilerID, 0)
	if err != nil {
		t.Fatalf("getPossibleViewsForReconcile() DFS error = %v", err)
	}
	if len(dfsViews) < 2 {
		t.Fatalf("expected DFS to provide multiple views, got %d", len(dfsViews))
	}

	mcConfig := MonteCarloConfig{
		Seed:          2026,
		TrialIndex:    11,
		ScenarioGroup: "scenario/input#0",
	}
	mcExplorerA := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeMonteCarlo,
			MonteCarlo: mcConfig,
			Perturbations: PerturbationConfig{
				Staleness: staleness,
			},
		},
		dependencies:    deps,
		priorityHandler: &PriorityManager{},
		stats:           NewExploreStats(),
	}
	mcViewsA, err := mcExplorerA.getPossibleViewsForReconcile(state, reconcilerID, 0)
	if err != nil {
		t.Fatalf("getPossibleViewsForReconcile() Monte Carlo A error = %v", err)
	}
	if len(mcViewsA) != 1 {
		t.Fatalf("expected Monte Carlo to sample one view, got %d", len(mcViewsA))
	}

	foundInDFS := false
	for _, candidate := range dfsViews {
		if candidate.Hash() == mcViewsA[0].Hash() {
			foundInDFS = true
			break
		}
	}
	if !foundInDFS {
		t.Fatalf("sampled Monte Carlo view must be one of the DFS candidate views")
	}

	mcExplorerB := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeMonteCarlo,
			MonteCarlo: mcConfig,
			Perturbations: PerturbationConfig{
				Staleness: staleness,
			},
		},
		dependencies:    deps,
		priorityHandler: &PriorityManager{},
		stats:           NewExploreStats(),
	}
	mcViewsB, err := mcExplorerB.getPossibleViewsForReconcile(state, reconcilerID, 0)
	if err != nil {
		t.Fatalf("getPossibleViewsForReconcile() Monte Carlo B error = %v", err)
	}
	if len(mcViewsB) != 1 {
		t.Fatalf("expected Monte Carlo to sample one view on second explorer, got %d", len(mcViewsB))
	}
	if mcViewsA[0].Hash() != mcViewsB[0].Hash() {
		t.Fatalf("expected deterministic Monte Carlo stale-view sampling for same seed/config")
	}
}
