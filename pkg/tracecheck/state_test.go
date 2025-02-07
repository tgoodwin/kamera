package tracecheck

import (
	"testing"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

func Test_ExecutionHistoryFilterNoOps(t *testing.T) {
	eh := ExecutionHistory{
		{
			ControllerID: "1",
			FrameID:      "1",
			Changes:      ObjectVersions{},
			Deltas:       map[snapshot.IdentityKey]Delta{},
		},
		{
			ControllerID: "2",
			FrameID:      "2",
			Changes:      ObjectVersions{},
			Deltas:       map[snapshot.IdentityKey]Delta{},
		},
		{
			ControllerID: "3",
			FrameID:      "3",
			Changes:      ObjectVersions{},
			Deltas:       map[snapshot.IdentityKey]Delta{},
		},
		{
			ControllerID: "4",
			FrameID:      "4",
			Changes:      ObjectVersions{},
			Deltas: map[snapshot.IdentityKey]Delta{
				{
					Kind:     "Pod",
					ObjectID: "1",
				}: "delta",
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
				Changes:      ObjectVersions{},
				Deltas: map[snapshot.IdentityKey]Delta{
					{
						Kind:     "Pod",
						ObjectID: "1",
					}: "delta",
				},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      ObjectVersions{},
				Deltas:       map[snapshot.IdentityKey]Delta{},
			},
		},
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      ObjectVersions{},
				Deltas:       map[snapshot.IdentityKey]Delta{},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      ObjectVersions{},
				Deltas:       map[snapshot.IdentityKey]Delta{},
			},
		},
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      ObjectVersions{},
				Deltas:       map[snapshot.IdentityKey]Delta{},
			},
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      ObjectVersions{},
				Deltas: map[snapshot.IdentityKey]Delta{
					{
						Kind:     "Pod",
						ObjectID: "1",
					}: "delta",
				},
			},
			{
				ControllerID: "1",
				FrameID:      "3",
				Changes:      ObjectVersions{},
				Deltas:       map[snapshot.IdentityKey]Delta{},
			},
		},
	}
	unique := GetUniquePaths(testPaths)
	expected := []ExecutionHistory{
		{
			{
				ControllerID: "1",
				FrameID:      "1",
				Changes:      ObjectVersions{},
				Deltas: map[snapshot.IdentityKey]Delta{
					{
						Kind:     "Pod",
						ObjectID: "1",
					}: "delta",
				},
			},
		},
		{
			{
				ControllerID: "2",
				FrameID:      "2",
				Changes:      ObjectVersions{},
				Deltas: map[snapshot.IdentityKey]Delta{
					{
						Kind:     "Pod",
						ObjectID: "1",
					}: "delta",
				},
			},
		},
	}
	if len(unique) != len(expected) {
		t.Errorf("Expected %d, got %d", len(expected), len(unique))
	}

}
