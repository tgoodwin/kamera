package replay

import (
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestInsertFrame(t *testing.T) {
	type args struct {
		framesBefore []Frame
		toInsert     Frame
		framesAfter  []Frame
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test InsertFrame",
			args: args{
				framesBefore: []Frame{
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
				},
				toInsert: Frame{sequenceID: "0011", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
				framesAfter: []Frame{
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0011", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
				},
			},
		},
		{
			name: "A frame with a sequenceID that is less than the first frame's sequenceID",
			args: args{
				framesBefore: []Frame{
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
				},
				toInsert: Frame{sequenceID: "0009", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
				framesAfter: []Frame{
					{sequenceID: "0009", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
				},
			},
		},
		{
			name: "A frame with a sequenceID that is greater than the last frame's sequenceID",
			args: args{
				framesBefore: []Frame{
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
				},
				toInsert: Frame{sequenceID: "0013", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
				framesAfter: []Frame{
					{sequenceID: "0010", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID1"},
					{sequenceID: "0012", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID2"},
					{sequenceID: "0013", Type: FrameTypeTraced, Req: reconcile.Request{}, TraceyRootID: "traceyRootID3"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			harness := &Harness{
				frames: tt.args.framesBefore,
			}
			harness.insertFrame(tt.args.toInsert)
			for i, f := range harness.frames {
				if f != tt.args.framesAfter[i] {
					t.Errorf("InsertFrame() = %v, want %v", f, tt.args.framesAfter[i])
				}
			}
		})
	}
}
