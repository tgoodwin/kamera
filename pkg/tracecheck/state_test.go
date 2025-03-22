package tracecheck

import (
	"testing"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func Test_ExecutionHistoryFilterNoOps(t *testing.T) {
	eh := ExecutionHistory{
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
		{
			ControllerID: "3",
			FrameID:      "3",
			Changes:      Changes{ObjectVersions: ObjectVersions{}},
			Deltas:       map[snapshot.CompositeKey]Delta{},
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
		},
	}
	filtered := eh.FilterNoOps()
	if len(filtered) != 1 {
		t.Errorf("Expected 0, got %d", len(filtered))
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
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
		},
		{
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      Changes{ObjectVersions: ObjectVersions{}},
				Deltas: map[snapshot.CompositeKey]Delta{
					snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): "delta",
				},
			},
		},
	}
	if len(unique) != len(expected) {
		t.Errorf("Expected %d, got %d", len(expected), len(unique))
	}
}

func TestHash(t *testing.T) {
	ov := ObjectVersions{
		snapshot.NewCompositeKey("Pod", "default", "pod1", "1"): snapshot.NewDefaultHash("Hash"),
	}
	sn := StateNode{
		Contents: StateSnapshot{
			contents: ov,
		},
		PendingReconciles: []PendingReconcile{
			{ReconcilerID: "controller1"},
		},
	}
	hash := sn.Hash()
	hash2 := sn.Hash()
	if hash != hash2 {
		t.Errorf("Expected %s, got %s", hash, hash2)
	}
}

func TestExpandStateByReconcileOrder(t *testing.T) {
	// Create test utility functions
	createTestReconcile := func(id string, name string) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: id,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: "default",
					Name:      name,
				},
			},
		}
	}

	// Test cases
	tests := []struct {
		name              string
		pendingReconciles []PendingReconcile
		expectedCount     int
		checkFirsts       bool // whether to verify each first element
	}{
		{
			name:              "Empty pending reconciles",
			pendingReconciles: []PendingReconcile{},
			expectedCount:     1, // returns original state
			checkFirsts:       false,
		},
		{
			name: "Single pending reconcile",
			pendingReconciles: []PendingReconcile{
				createTestReconcile("Rec1", "obj1"),
			},
			expectedCount: 1, // returns original state
			checkFirsts:   false,
		},
		{
			name: "Multiple pending reconciles",
			pendingReconciles: []PendingReconcile{
				createTestReconcile("Rec1", "obj1"),
				createTestReconcile("Rec2", "obj2"),
				createTestReconcile("Rec3", "obj3"),
			},
			expectedCount: 3, // one for each reconcile
			checkFirsts:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test state node
			inputState := StateNode{
				ID:                "test-state",
				PendingReconciles: tc.pendingReconciles,
				ExecutionHistory:  make(ExecutionHistory, 0),
				// Other fields can be nil for this test
			}

			// Call the function
			result := expandStateByReconcileOrder(inputState)

			// Check the number of returned states
			if len(result) != tc.expectedCount {
				t.Errorf("Expected %d states, got %d", tc.expectedCount, len(result))
			}

			// For single or empty reconcile cases, just check we got back the original
			if len(tc.pendingReconciles) <= 1 {
				if result[0].ID != inputState.ID {
					t.Errorf("Expected original state ID, got different ID")
				}
				return
			}

			// For multiple reconciles, verify each expanded state
			if tc.checkFirsts {
				// Map to track which reconciles we've seen as first elements
				firstReconciles := make(map[string]bool)

				for _, state := range result {
					// Ensure the ID is different from the original
					if state.ID == inputState.ID {
						t.Errorf("Expected new state ID, got original ID")
					}

					// Ensure the number of pending reconciles is the same
					if len(state.PendingReconciles) != len(tc.pendingReconciles) {
						t.Errorf("Expected %d pending reconciles, got %d",
							len(tc.pendingReconciles), len(state.PendingReconciles))
					}

					// Check if we have the right first element
					firstRecID := state.PendingReconciles[0].ReconcilerID
					firstReconciles[firstRecID] = true

					// Verify the full set of reconciles (order can differ but should contain all originals)
					reconcileMap := make(map[string]bool)
					for _, pr := range state.PendingReconciles {
						reconcileMap[pr.ReconcilerID] = true
					}

					for _, expected := range tc.pendingReconciles {
						if !reconcileMap[expected.ReconcilerID] {
							t.Errorf("Missing expected reconcile %s in result state", expected.ReconcilerID)
						}
					}
				}

				// Verify each input reconcile was first in one of the output states
				for _, expected := range tc.pendingReconciles {
					if !firstReconciles[expected.ReconcilerID] {
						t.Errorf("Reconcile %s was never first in any expanded state", expected.ReconcilerID)
					}
				}
			}
		})
	}
}
