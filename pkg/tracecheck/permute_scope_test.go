package tracecheck

import (
	"strings"
	"testing"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
)

// --- Depth-range gating ---

func TestShouldExpandPendingOrder_DepthRange_InsideRange(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteDepthRange: &PermuteDepthRange{Min: 2, Max: 5},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	state.depth = 3

	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=true for depth 3 within [2,5]")
	}
}

func TestShouldExpandPendingOrder_DepthRange_OutsideRange(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteDepthRange: &PermuteDepthRange{Min: 2, Max: 5},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)

	// Below range
	state.depth = 1
	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=false for depth 1, below range [2,5]")
	}

	// Above range
	state.depth = 6
	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=false for depth 6, above range [2,5]")
	}
}

func TestShouldExpandPendingOrder_DepthRange_Boundaries(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteDepthRange: &PermuteDepthRange{Min: 2, Max: 5},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)

	// Exactly at min boundary
	state.depth = 2
	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=true at min boundary depth=2")
	}

	// Exactly at max boundary
	state.depth = 5
	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=true at max boundary depth=5")
	}
}

func TestShouldExpandPendingOrder_NoDepthRange_Unconstrained(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)

	// Should expand at any depth when no depth range is configured
	for _, d := range []int{0, 5, 100} {
		state.depth = d
		if !explorer.shouldExpandPendingOrder(state) {
			t.Fatalf("expected shouldExpandPendingOrder=true at depth=%d when no depth range configured", d)
		}
	}
}

// --- Event-trigger activation ---

func TestShouldExpandPendingOrder_EventTrigger_NotTriggered(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteAfterEvent: &PermuteEventTrigger{
					OpType: event.CREATE,
					Kind:   "apiextensions.crossplane.io/CompositionRevision",
				},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	state.permuteTriggered = false

	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=false when event trigger not yet activated")
	}
}

func TestShouldExpandPendingOrder_EventTrigger_Triggered(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteAfterEvent: &PermuteEventTrigger{
					OpType: event.CREATE,
					Kind:   "apiextensions.crossplane.io/CompositionRevision",
				},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	state.permuteTriggered = true

	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=true when event trigger is activated")
	}
}

func TestPermuteTriggered_Inheritance(t *testing.T) {
	// Verify that Clone() preserves permuteTriggered
	original := testState(
		testPending("controller-a", "default", "a"),
	)
	original.permuteTriggered = true

	cloned := original.Clone()
	if !cloned.permuteTriggered {
		t.Fatalf("expected Clone() to preserve permuteTriggered=true")
	}

	// Verify that non-triggered also clones correctly
	original2 := testState(
		testPending("controller-a", "default", "a"),
	)
	original2.permuteTriggered = false

	cloned2 := original2.Clone()
	if cloned2.permuteTriggered {
		t.Fatalf("expected Clone() to preserve permuteTriggered=false")
	}
}

// --- Combined AND logic ---

func TestShouldExpandPendingOrder_Combined_DepthAndEvent(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				PermuteDepthRange: &PermuteDepthRange{Min: 2, Max: 5},
				PermuteAfterEvent: &PermuteEventTrigger{
					OpType: event.CREATE,
					Kind:   "apiextensions.crossplane.io/CompositionRevision",
				},
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)

	// Both satisfied: inside range + triggered
	state.depth = 3
	state.permuteTriggered = true
	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected true when both depth in range and event triggered")
	}

	// Depth in range but not triggered
	state.depth = 3
	state.permuteTriggered = false
	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected false when depth in range but event not triggered")
	}

	// Triggered but depth out of range
	state.depth = 10
	state.permuteTriggered = true
	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected false when event triggered but depth out of range")
	}

	// Neither satisfied
	state.depth = 10
	state.permuteTriggered = false
	if explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected false when neither depth in range nor event triggered")
	}
}

// --- Serialize differentiation ---

func TestSerialize_PermuteTriggered_Differentiation(t *testing.T) {
	ov := ObjectVersions{
		snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("hash1"),
	}

	makeState := func(triggered bool) StateNode {
		sn := StateNode{
			Contents:          NewStateSnapshot(ov, KindSequences{"core/Pod": 1}, nil),
			PendingReconciles: []PendingReconcile{testPending("controller-a", "default", "a")},
			permuteTriggered:  triggered,
		}
		return sn
	}

	notTriggered := makeState(false).Serialize()
	triggered := makeState(true).Serialize()

	if notTriggered == triggered {
		t.Fatalf("expected serialize output to differ between triggered and non-triggered states, got same: %s", notTriggered)
	}

	if !strings.Contains(triggered, "|pt:1") {
		t.Fatalf("expected triggered serialization to contain '|pt:1', got: %s", triggered)
	}

	if strings.Contains(notTriggered, "|pt:1") {
		t.Fatalf("expected non-triggered serialization to NOT contain '|pt:1', got: %s", notTriggered)
	}
}

func TestShouldExpandPendingOrder_NoEventTrigger_Unconstrained(t *testing.T) {
	// When no PermuteAfterEvent is configured, permuteTriggered should not matter
	explorer := &Explorer{
		Config: &ExploreConfig{
			SearchMode: SearchModeDFS,
			Perturbations: PerturbationConfig{
				PermuteOrder: map[ReconcilerID]bool{
					"controller-a": true,
				},
				// No PermuteAfterEvent
			},
		},
	}

	state := testState(
		testPending("controller-a", "default", "a"),
		testPending("controller-b", "default", "b"),
	)
	state.permuteTriggered = false

	if !explorer.shouldExpandPendingOrder(state) {
		t.Fatalf("expected shouldExpandPendingOrder=true when no event trigger configured, regardless of permuteTriggered")
	}
}
