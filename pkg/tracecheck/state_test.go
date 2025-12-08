package tracecheck

import (
	"slices"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func Test_ExecutionHistoryFilterNoOps(t *testing.T) {
	eh := ExecutionHistory{
		{
			ControllerID:      "1",
			FrameID:           "1",
			Changes:           Changes{ObjectVersions: ObjectVersions{}},
			Deltas:            map[snapshot.CompositeKey]Delta{},
			PendingReconciles: []PendingReconcile{{ReconcilerID: "A", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "a"}}}},
		},
		{
			ControllerID:      "2",
			FrameID:           "2",
			Changes:           Changes{ObjectVersions: ObjectVersions{}},
			Deltas:            map[snapshot.CompositeKey]Delta{},
			PendingReconciles: []PendingReconcile{{ReconcilerID: "B", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "b"}}}},
		},
		{
			ControllerID:      "3",
			FrameID:           "3",
			Changes:           Changes{ObjectVersions: ObjectVersions{}},
			Deltas:            map[snapshot.CompositeKey]Delta{},
			PendingReconciles: []PendingReconcile{}, // Convergence step - no-op, filtered out
		},
		{
			ControllerID: "4",
			FrameID:      "4",
			Changes: Changes{ObjectVersions: ObjectVersions{
				snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
			}},
			Deltas: map[snapshot.CompositeKey]Delta{
				snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
			},
			PendingReconciles: []PendingReconcile{{ReconcilerID: "C", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "c"}}}},
		},
	}
	filtered := eh.FilterNoOps()
	// Should preserve only step 4 (has effects). Step 3 is a no-op and filtered out.
	// Convergence distinction is handled by UniqueKey() using :converged marker.
	if len(filtered) != 1 {
		t.Errorf("Expected 1 (only step with effects), got %d", len(filtered))
	}
	// Verify step 4 (has effects) is preserved
	if filtered[0].ControllerID != "4" {
		t.Errorf("Expected step with effects (ControllerID=4) to be preserved, got ControllerID=%s", filtered[0].ControllerID)
	}
}

func Test_GetUniquePaths(t *testing.T) {
	testPaths := []ExecutionHistory{
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
		},
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
		},
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
			{
				ControllerID: "1",
				FrameID:      "3",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
		},
	}
	unique := GetUniquePaths(testPaths)
	expected := []ExecutionHistory{
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
			{
				ControllerID: "1",
				FrameID:      "3",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
		},
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas:       map[snapshot.CompositeKey]Delta{},
			},
		},
	}
	if len(unique) != len(expected) {
		t.Fatalf("Expected %d unique paths, got %d", len(expected), len(unique))
	}

	// Verify no-op reconciles are preserved in the returned paths (used for inspection),
	// even though they are ignored for dedupe keys.
	if len(unique[0]) != 2 {
		t.Fatalf("expected first unique path to retain 2 steps (including trailing no-op), got %d", len(unique[0]))
	}
	if len(unique[0][1].Changes.ObjectVersions) != 0 {
		t.Fatalf("expected trailing step in first path to be a no-op, got %d object versions", len(unique[0][1].Changes.ObjectVersions))
	}
	if len(unique[1]) != 3 {
		t.Fatalf("expected second unique path to retain 3 steps (including leading/trailing no-ops), got %d", len(unique[1]))
	}
	if len(unique[1][0].Changes.ObjectVersions) != 0 || len(unique[1][2].Changes.ObjectVersions) != 0 {
		t.Fatalf("expected no-ops at start/end of second path to be preserved")
	}
}

// Test_GetUniquePaths_PreservesConvergenceSteps verifies that paths ending in convergence
// are not deduplicated with non-converged paths, even if they have the same controller sequence.
// Convergence is tracked via the :converged marker in UniqueKey(), not by preserving no-op steps.
func Test_GetUniquePaths_PreservesConvergenceSteps(t *testing.T) {
	// Path 1: A makes a change, then B makes a change and converges (0 pending)
	// Path 2: A makes a change, then B makes a change but doesn't converge (has pending)
	// These should be considered different paths due to the :converged marker
	testPaths := []ExecutionHistory{
		{
			{
				ControllerID: "A",
				FrameID:      "1",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
				PendingReconciles: []PendingReconcile{{ReconcilerID: "B", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "b"}}}},
			},
			{
				ControllerID: "B",
				FrameID:      "2",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Service", "default", "svc1", "1"): snapshot.NewDefaultHash("Hash2"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Service", "default", "svc1", "1"): "delta2",
				},
				PendingReconciles: []PendingReconcile{}, // Converged - 0 pending
			},
		},
		{
			{
				ControllerID: "A",
				FrameID:      "1",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
				PendingReconciles: []PendingReconcile{{ReconcilerID: "B", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "b"}}}},
			},
			{
				ControllerID: "B",
				FrameID:      "3",
				Changes: Changes{ObjectVersions: ObjectVersions{
					snapshot.NewCompositeKey("Service", "default", "svc1", "1"): snapshot.NewDefaultHash("Hash2"),
				}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Service", "default", "svc1", "1"): "delta2",
				},
				PendingReconciles: []PendingReconcile{{ReconcilerID: "C", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "c"}}}}, // Not converged, has pending
			},
		},
	}
	unique := GetUniquePaths(testPaths)
	// Should preserve both paths since one ends in convergence and one doesn't.
	// The deduplication key includes ":converged" marker for the converged path,
	// making them distinct even though both have the same A@1,B@1 structure.
	if len(unique) != 2 {
		t.Errorf("Expected 2 unique paths (one converged, one not), got %d", len(unique))
	}

	// Verify the unique keys are different - one should have :converged suffix
	key1 := unique[0].UniqueKey()
	key2 := unique[1].UniqueKey()
	if key1 == key2 {
		t.Errorf("Expected different unique keys for converged vs non-converged paths, but both had key: %s", key1)
	}
}

func TestExpandStateByReconcileOrder(t *testing.T) {
	type testCase struct {
		name          string
		pending       []PendingReconcile
		wantNumStates int
		wantOrders    [][]string // ordered list of reconcilerIDs, for each result
	}
	makePR := func(reconcilerID ReconcilerID) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: reconcilerID,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: string(reconcilerID)},
			},
			Source: "test",
		}
	}

	cases := []testCase{
		{
			name:          "no pending reconciles (converged)",
			pending:       []PendingReconcile{},
			wantNumStates: 1,
			wantOrders:    [][]string{{}},
		},
		{
			name:          "single pending reconcile",
			pending:       []PendingReconcile{makePR("A")},
			wantNumStates: 1,
			wantOrders:    [][]string{{"A"}},
		},
		{
			name:          "two pending reconciles",
			pending:       []PendingReconcile{makePR("A"), makePR("B")},
			wantNumStates: 2,
			wantOrders: [][]string{
				{"A", "B"},
				{"B", "A"},
			},
		},
		{
			name:          "three pending reconciles",
			pending:       []PendingReconcile{makePR("A"), makePR("B"), makePR("C")},
			wantNumStates: 3,
			wantOrders: [][]string{
				{"A", "B", "C"},
				{"B", "A", "C"},
				{"C", "A", "B"},
			}, // only permuting first item
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := StateNode{PendingReconciles: tc.pending}
			results := expandStateByReconcileOrder(state)
			if len(results) != tc.wantNumStates {
				t.Errorf("Expected %d result states, got %d", tc.wantNumStates, len(results))
			}
			for _, result := range results {
				if !result.Contents.All().Equals(state.Contents.All()) {
					t.Errorf("Expected state contents to be the same")
				}
			}
			var gotOrders [][]string
			for _, st := range results {
				var order []string
				for _, pr := range st.PendingReconciles {
					order = append(order, string(pr.ReconcilerID))
				}
				gotOrders = append(gotOrders, order)
			}

			// Check we have the right number of orderings
			assert.Equal(t, len(tc.wantOrders), len(gotOrders), "Expected %d orderings, got %d", len(tc.wantOrders), len(gotOrders))

			// Check all expected orders are present in result (order doesn't matter)
			for _, wantOrder := range tc.wantOrders {
				found := lo.ContainsBy(gotOrders, func(gotOrder []string) bool {
					return slices.Equal(gotOrder, wantOrder)
				})
				assert.True(t, found, "Result missing wanted ordering: %v (all orders: %v)", wantOrder, gotOrders)
			}
		})
	}
}
