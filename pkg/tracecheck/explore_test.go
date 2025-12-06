package tracecheck

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func Test_getNewPendingReconciles(t *testing.T) {
	newPr := func(id, namespace, name string) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: id,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      name,
				},
			},
		}
	}
	tests := []struct {
		name     string
		curr     []PendingReconcile
		new      []PendingReconcile
		expected []PendingReconcile
	}{
		{
			name: "identical lists deduped",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			new: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			// DFS: new items first, then curr; duplicates removed (first wins)
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
		},
		{
			name: "new items come first in DFS",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			new: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerA", "namespace1", "name2"),
			},
			// DFS: new items first [A/name1, A/name2], then curr [A/name1 (dup), B/name2]
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerA", "namespace1", "name2"),
				newPr("controllerB", "namespace1", "name2"),
			},
		},
		{
			name: "empty new list",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			new:      []PendingReconcile{},
			expected: []PendingReconcile{newPr("controllerA", "namespace1", "name1")},
		},
		{
			name:     "empty curr list",
			curr:     []PendingReconcile{},
			new:      []PendingReconcile{newPr("controllerA", "namespace1", "name1")},
			expected: []PendingReconcile{newPr("controllerA", "namespace1", "name1")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Explorer{config: &ExploreConfig{}}
			actual := e.getNewPendingReconciles(tt.curr, tt.new)
			if !assert.Equal(t, tt.expected, actual) {
				t.Errorf("getNewPendingReconciles() = %v, want %v", actual, tt.expected)
			}
		})
	}
}

func Test_determineNewPendingReconciles(t *testing.T) {
	newPr := func(id, namespace, name string) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: id,
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      name,
				},
			},
		}
	}

	newCompositeKey := func(kind, namespace, name string) snapshot.CompositeKey {
		return snapshot.CompositeKey{
			IdentityKey: snapshot.IdentityKey{
				Kind:     kind,
				ObjectID: name,
			},
			ResourceKey: snapshot.ResourceKey{
				Kind:      kind,
				Namespace: namespace,
				Name:      name,
			},
		}
	}

	tests := []struct {
		name                     string
		curr                     []PendingReconcile
		pendingReconcile         PendingReconcile
		triggered                []PendingReconcile
		reconcilerKindDeps       map[string][]string
		stuckReconcilerPositions map[string]KindSequences
		result                   *ReconcileResult
		expected                 []PendingReconcile
	}{
		{
			name: "no stuck reconcilers, no changes",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered:        nil,
			reconcilerKindDeps: map[string][]string{
				"controllerA": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: nil,
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{},
				Changes: Changes{},
			},
			expected: []PendingReconcile{},
		},
		{
			name: "stuck reconcilers filtered out",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerC", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered:        []PendingReconcile{newPr("controllerB", "namespace1", "name2")},
			reconcilerKindDeps: map[string][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: map[string]KindSequences{
				"controllerB": {
					"Kind1": 1,
				},
			},
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{},
				Changes: Changes{
					ObjectVersions: ObjectVersions{
						newCompositeKey("Kind1", "namespace1", "name2"): {},
					},
				},
			},
			expected: []PendingReconcile{
				newPr("controllerC", "namespace1", "name1"),
			},
		},
		{
			name: "triggered reconciler added",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered: []PendingReconcile{
				newPr("controllerB", "namespace1", "name2"),
			},
			reconcilerKindDeps: map[string][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: nil,
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{},
				Changes: Changes{
					ObjectVersions: ObjectVersions{
						newCompositeKey("Kind1", "namespace1", "name2"): {},
					},
				},
			},
			expected: []PendingReconcile{
				newPr("controllerB", "namespace1", "name2"),
			},
		},
		{
			name: "requeue current reconcile",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered:        nil,
			reconcilerKindDeps: map[string][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: nil,
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{Requeue: true},
				Changes: Changes{},
			},
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
		},
		{
			name: "controller triggered by change it subscribes to and is not stuck on",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered: []PendingReconcile{
				newPr("controllerB", "namespace1", "name2"),
			},
			reconcilerKindDeps: map[string][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: map[string]KindSequences{
				"controllerB": {
					"Kind1": 1,
				},
			},
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{},
				Changes: Changes{
					ObjectVersions: ObjectVersions{
						newCompositeKey("Kind1", "namespace1", "name2"): {},
						newCompositeKey("Kind2", "namespace1", "name2"): {},
					},
				},
			},
			expected: []PendingReconcile{
				newPr("controllerB", "namespace1", "name2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockTriggered := NewMockTriggerHandler(ctrl)
			mockTriggered.EXPECT().GetTriggered(tt.result.Changes).Return(tt.triggered, nil).Times(1)
			// TODO stop re-implementing the function under test...
			for _, trig := range tt.triggered {
				kindDeps := tt.reconcilerKindDeps[trig.ReconcilerID]
				if _, stuck := tt.stuckReconcilerPositions[trig.ReconcilerID]; stuck {
					mockTriggered.EXPECT().KindDepsForReconciler(trig.ReconcilerID).Return(kindDeps, nil).Times(1)
				}
			}
		e := &Explorer{
			triggerManager: mockTriggered,
			config:         &ExploreConfig{},
		}

			state := StateNode{
				PendingReconciles:        tt.curr,
				stuckReconcilerPositions: tt.stuckReconcilerPositions,
			}

			actual := e.determineNewPendingReconciles(context.Background(), state, tt.pendingReconcile, tt.result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
