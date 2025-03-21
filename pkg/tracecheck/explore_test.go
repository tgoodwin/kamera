package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

func Test_serializeState(t *testing.T) {
	type args struct {
		state StateNode
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "deterministic test",
			args: args{
				state: StateNode{
					Contents: StateSnapshot{
						contents: ObjectVersions{
							snapshot.NewCompositeKey("kind1", "namespace1", "name1", "object1"): snapshot.NewDefaultHash("hash1"),
							snapshot.NewCompositeKey("kind1", "namespace1", "name2", "object2"): snapshot.NewDefaultHash("hash2"),
						},
						KindSequences: KindSequences{
							"kind1": 1,
							"kind2": 2,
						},
					},
					PendingReconciles: []PendingReconcile{
						{ReconcilerID: "controller1"},
						{ReconcilerID: "controller2"},
					},
					// Initialize with some test data
				},
			},
			want: "Objects:{object1=hash1,object2=hash2}|PendingReconciles:{controller1,controller2}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serializeState(tt.args.state)
			if got != tt.want {
				t.Errorf("serializeState() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getNewPendingReconciles(t *testing.T) {
	tests := []struct {
		name     string
		curr     []PendingReconcile
		new      []PendingReconcile
		expected []PendingReconcile
	}{
		{
			name: "identical lists",
			curr: []PendingReconcile{
				{ReconcilerID: "controllerC"},
				{ReconcilerID: "controllerB"},
			},
			new: []PendingReconcile{
				{ReconcilerID: "controllerB"},
				{ReconcilerID: "controllerC"},
			},
			expected: []PendingReconcile{
				{ReconcilerID: "controllerC"},
				{ReconcilerID: "controllerB"},
			},
		},
		{
			name: "one new controller",
			curr: []PendingReconcile{
				{ReconcilerID: "controllerC"},
				{ReconcilerID: "controllerB"},
			},
			new: []PendingReconcile{
				{ReconcilerID: "controllerB"},
				{ReconcilerID: "controllerC"},
				{ReconcilerID: "controllerA"},
			},
			expected: []PendingReconcile{
				{ReconcilerID: "controllerC"},
				{ReconcilerID: "controllerB"},
				{ReconcilerID: "controllerA"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getNewPendingReconciles(tt.curr, tt.new)
			if !assert.Equal(t, actual, tt.expected) {
				t.Errorf("getNewPendingReconciles() = %v, want %v", actual, tt.expected)
			}
		})
	}
}
