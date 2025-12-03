package tracecheck

import (
	"testing"

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
			PendingReconciles: []PendingReconcile{}, // Convergence step - should be preserved
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
	// Should preserve: step 3 (convergence with 0 pending) and step 4 (has effects)
	if len(filtered) != 2 {
		t.Errorf("Expected 2 (convergence step + step with effects), got %d", len(filtered))
	}
	// Verify step 3 (convergence) is preserved
	if filtered[0].ControllerID != "3" && filtered[1].ControllerID != "3" {
		t.Errorf("Expected convergence step (ControllerID=3) to be preserved, but it was filtered out")
	}
	// Verify step 4 (has effects) is preserved
	if filtered[0].ControllerID != "4" && filtered[1].ControllerID != "4" {
		t.Errorf("Expected step with effects (ControllerID=4) to be preserved, but it was filtered out")
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
		t.Errorf("Expected %d unique paths, got %d", len(expected), len(unique))
	}
}

// Test_GetUniquePaths_PreservesConvergenceSteps verifies that paths ending in convergence
// are not deduplicated with non-converged paths, even if they have the same controller sequence
func Test_GetUniquePaths_PreservesConvergenceSteps(t *testing.T) {
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
				ControllerID:      "B",
				FrameID:           "2",
				Changes:           Changes{ObjectVersions: ObjectVersions{}},
				Deltas:            map[snapshot.CompositeKey]Delta{},
				PendingReconciles: []PendingReconcile{}, // Convergence step - no-op but preserved
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
				PendingReconciles: []PendingReconcile{{ReconcilerID: "C", Request: reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "c"}}}}, // Not converged, has effects
			},
		},
	}
	unique := GetUniquePaths(testPaths)
	// Should preserve both paths since one ends in convergence and one doesn't
	// The deduplication key should include ":converged" marker for the convergence step
	if len(unique) != 2 {
		t.Errorf("Expected 2 unique paths (one converged, one not), got %d", len(unique))
	}
	// Verify the converged path is preserved
	convergedFound := false
	for _, path := range unique {
		if len(path) > 0 && len(path[len(path)-1].PendingReconciles) == 0 {
			convergedFound = true
			break
		}
	}
	if !convergedFound {
		t.Errorf("Expected to find a path ending in convergence (0 pending reconciles), but none found")
	}
}
