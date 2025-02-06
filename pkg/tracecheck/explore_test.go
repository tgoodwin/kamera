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
					ObjectVersions: ObjectVersions{
						snapshot.IdentityKey{
							Kind:     "kind1",
							ObjectID: "object1",
						}: "hash1",
						snapshot.IdentityKey{
							Kind:     "kind1",
							ObjectID: "object2",
						}: "hash2",
					},
					PendingReconciles: []string{"controller1", "controller2"},
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
