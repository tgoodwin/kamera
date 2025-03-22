package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		mode     string
		curr     []PendingReconcile
		new      []PendingReconcile
		expected []PendingReconcile
	}{
		{
			name: "identical lists in queue mode",
			mode: "queue",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			new: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
		},
		{
			name: "dedupe in queue mode",
			mode: "queue",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			new: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerA", "namespace1", "name2"),
			},
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
				newPr("controllerA", "namespace1", "name2"),
			},
		},
		{
			name: "dedupe in stack mode",
			mode: "stack",
			curr: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerB", "namespace1", "name2"),
			},
			new: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerA", "namespace1", "name2"),
			},
			expected: []PendingReconcile{
				newPr("controllerA", "namespace1", "name1"),
				newPr("controllerA", "namespace1", "name2"),
				newPr("controllerB", "namespace1", "name2"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ExploreConfig{mode: tt.mode}
			e := &Explorer{config: c}
			actual := e.getNewPendingReconciles(tt.curr, tt.new)
			if !assert.Equal(t, tt.expected, actual) {
				t.Errorf("getNewPendingReconciles() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
