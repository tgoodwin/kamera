package tracecheck

import "testing"

func TestInitialStatesToEnqueue_DFSIncludesOrderVariants(t *testing.T) {
	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
					"controller-b": true,
				},
			},
		},
	}

	got := explorer.initialStatesToEnqueue(state)
	if len(got) <= 1 {
		t.Fatalf("expected DFS to include order variants for multi-pending initial state")
	}
}

func TestInitialStatesToEnqueue_MonteCarloSinglePath(t *testing.T) {
	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeMonteCarlo,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
					"controller-b": true,
				},
			},
		},
	}

	got := explorer.initialStatesToEnqueue(state)
	if len(got) != 1 {
		t.Fatalf("expected Monte Carlo initial state enqueue to stay single-path, got %d", len(got))
	}
	if got[0].Hash() != state.Hash() {
		t.Fatalf("expected Monte Carlo initial enqueue to keep original state")
	}
}

func TestEnqueueNextStates_MonteCarloSkipsOrderExpansion(t *testing.T) {
	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	triggered := []PendingReconcile{
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	}

	dfsExplorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
					"controller-b": true,
				},
			},
		},
		stats: NewExploreStats(),
	}
	dfsStack := dfsExplorer.enqueueNextStates(nil, state, triggered, nil, false, false, false)
	if len(dfsStack) <= 1 {
		t.Fatalf("expected DFS enqueueNextStates to include order-expansion variants")
	}

	mcExplorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeMonteCarlo,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
					"controller-b": true,
				},
			},
		},
		stats: NewExploreStats(),
	}
	mcStack := mcExplorer.enqueueNextStates(nil, state, triggered, nil, false, false, false)
	if len(mcStack) != 1 {
		t.Fatalf("expected Monte Carlo enqueueNextStates to avoid order fanout, got %d entries", len(mcStack))
	}
}
