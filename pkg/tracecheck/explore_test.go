package tracecheck

import (
	"testing"

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
					objects: ObjectVersions{
						snapshot.IdentityKey{
							Kind:     "kind1",
							ObjectID: "object1",
						}: snapshot.NewDefaultHash("hash1"),
						snapshot.IdentityKey{
							Kind:     "kind1",
							ObjectID: "object2",
						}: snapshot.NewDefaultHash("hash2"),
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

// func Test_getNewPendingReconciles(t *testing.T) {
// 	tests := []struct {
// 		name     string
// 		curr     []string
// 		new      []string
// 		expected []string
// 	}{
// 		{
// 			name:     "identical lists",
// 			curr:     []string{"controllerC", "controllerB"},
// 			new:      []string{"controllerB", "controllerC"},
// 			expected: []string{"controllerC", "controllerB"},
// 		},
// 		{
// 			name:     "one new controller",
// 			curr:     []string{"controllerC", "controllerB"},
// 			new:      []string{"controllerB", "controllerC", "controllerA"},
// 			expected: []string{"controllerC", "controllerB", "controllerA"},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			actual := getNewPendingReconciles(tt.curr, tt.new)
// 			if !assert.Equal(t, actual, tt.expected) {
// 				t.Errorf("getNewPendingReconciles() = %v, want %v", actual, tt.expected)
// 			}
// 		})
// 	}
// }
