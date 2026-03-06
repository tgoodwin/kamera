package tracecheck

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func Test_getNewPendingReconciles(t *testing.T) {
	newPr := func(id, namespace, name string) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: ReconcilerID(id),
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
			// existing pending first, then new; duplicates removed (first wins)
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
			// existing pending first [A/name1, B/name2], then new [A/name1 (dup), A/name2]
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
				newPr("controllerA", "namespace1", "name2"),
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
		{
			name: "StateChange takes precedence over Requeue when StateChange comes first",
			curr: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceRequeue,
				},
			},
			new: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
			expected: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
		},
		{
			name: "StateChange takes precedence over Requeue even when Requeue comes first in merged list",
			curr: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
			new: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceRequeue,
				},
			},
			// curr comes first in all[], so order is [StateChange, Requeue]
			// StateChange replaces Requeue because StateChange has priority
			expected: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
		},
		{
			name: "StateChange takes precedence over AsyncEnqueue",
			curr: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceAsyncEnqueue,
				},
			},
			new: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
			expected: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceStateChange,
				},
			},
		},
		{
			name: "Requeue vs AsyncEnqueue - first occurrence wins",
			curr: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceAsyncEnqueue,
				},
			},
			new: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceRequeue,
				},
			},
			expected: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request:      reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "obj"}},
					Source:       SourceAsyncEnqueue, // curr comes before new, so AsyncEnqueue wins
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Explorer{Config: &ExploreConfig{}}
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
			ReconcilerID: ReconcilerID(id),
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
		reconcilerKindDeps       map[ReconcilerID][]string
		stuckReconcilerPositions map[ReconcilerID]KindSequences
		result                   *ReconcileResult
		stateDepth               int
		expected                 []PendingReconcile
	}{
		{
			name: "no stuck reconcilers, no changes",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered:        nil,
			reconcilerKindDeps: map[ReconcilerID][]string{
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
			reconcilerKindDeps: map[ReconcilerID][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: map[ReconcilerID]KindSequences{
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
			reconcilerKindDeps: map[ReconcilerID][]string{
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
			reconcilerKindDeps: map[ReconcilerID][]string{
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
				{
					ReconcilerID: "controllerA",
					Request: reconcile.Request{
						NamespacedName: types.NamespacedName{Namespace: "namespace1", Name: "name1"},
					},
					Source: SourceRequeue,
				},
			},
		},
		{
			name: "requeue-after current reconcile",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
			},
			pendingReconcile: newPr("controllerA", "namespace1", "name1"),
			triggered:        nil,
			reconcilerKindDeps: map[ReconcilerID][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: nil,
			result: &ReconcileResult{
				ctrlRes: reconcile.Result{RequeueAfter: 5 * time.Second},
				Changes: Changes{},
			},
			stateDepth: 3,
			expected: []PendingReconcile{
				{
					ReconcilerID: "controllerA",
					Request: reconcile.Request{
						NamespacedName: types.NamespacedName{Namespace: "namespace1", Name: "name1"},
					},
					Source:         SourceRequeueAfter,
					NotBeforeDepth: 9, // depth=3 + 1 current step + 5 simulated seconds
				},
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
			reconcilerKindDeps: map[ReconcilerID][]string{
				"controllerA": {"Kind1", "Kind2"},
				"controllerB": {"Kind1", "Kind2"},
				"controllerC": {"Kind1", "Kind2"},
			},
			stuckReconcilerPositions: map[ReconcilerID]KindSequences{
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
			simclock.Configure(time.Unix(0, 0), time.Second)

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
				Config:         &ExploreConfig{},
			}

			state := StateNode{
				PendingReconciles:        tt.curr,
				stuckReconcilerPositions: tt.stuckReconcilerPositions,
				depth:                    tt.stateDepth,
			}

			actual := e.determineNewPendingReconciles(context.Background(), state, &tt.pendingReconcile, tt.result)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func Test_determineNewPendingReconciles_RequeueAfterUsesSimclockStepSize(t *testing.T) {
	simclock.Configure(time.Unix(0, 0), 2*time.Second)
	t.Cleanup(func() {
		simclock.Configure(time.Unix(0, 0), time.Second)
	})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockTriggered := NewMockTriggerHandler(ctrl)
	mockTriggered.EXPECT().GetTriggered(Changes{}).Return(nil, nil).Times(1)

	e := &Explorer{
		triggerManager: mockTriggered,
		Config:         &ExploreConfig{},
	}

	consumed := PendingReconcile{
		ReconcilerID: "controllerA",
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: "namespace1", Name: "name1"},
		},
	}
	state := StateNode{
		PendingReconciles: []PendingReconcile{consumed},
		depth:             4,
	}
	result := &ReconcileResult{
		ctrlRes: reconcile.Result{RequeueAfter: 5 * time.Second}, // ceil(5/2)=3 steps
		Changes: Changes{},
	}

	actual := e.determineNewPendingReconciles(context.Background(), state, &consumed, result)
	expected := []PendingReconcile{
		{
			ReconcilerID: "controllerA",
			Request: reconcile.Request{
				NamespacedName: types.NamespacedName{Namespace: "namespace1", Name: "name1"},
			},
			Source:         SourceRequeueAfter,
			NotBeforeDepth: 8, // depth=4 + 1 current step + 3 delay steps
		},
	}
	assert.Equal(t, expected, actual)
}
