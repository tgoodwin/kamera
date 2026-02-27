package tracecheck

import (
	"fmt"
	"testing"

	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
)

func TestGetPossibleViewsForReconcile_ContainsStrictlyStaleView(t *testing.T) {
	const (
		reconcilerID ReconcilerID = "MockReconciler"
		group                     = "autoscaling.internal.knative.dev"
		kind                      = "PodAutoscaler"
	)
	kindKey := util.CanonicalGroupKind(group, kind)

	state := stateNodeFromEvents(t,
		newTestStateEvent(1, group, kind, "pa-v1"),
		newTestStateEvent(2, group, kind, "pa-v2"),
	)
	state.nextUserActionIdx = 3

	explorer := &Explorer{
		Config: &ExploreConfig{
			Perturbations: PerturbationConfig{
				Staleness: map[ReconcilerID]StalenessConfig{
					reconcilerID: {
						MaxRestarts: 1,
						StaleReadBounds: LookbackLimits{
							kindKey: 2,
						},
					},
				},
			},
		},
		dependencies: ResourceDeps{
			kindKey: util.NewSet(reconcilerID),
		},
		priorityHandler: &PriorityManager{},
		stats:           NewExploreStats(),
	}

	views, err := explorer.getPossibleViewsForReconcile(state, reconcilerID, 0)
	if err != nil {
		t.Fatalf("getPossibleViewsForReconcile returned error: %v", err)
	}
	if len(views) < 2 {
		t.Fatalf("expected at least 2 views (current + stale), got %d", len(views))
	}

	currentSeq := state.Contents.KindSequences[kindKey]
	foundStrictlyStale := false
	for _, v := range views {
		if v.nextUserActionIdx != state.nextUserActionIdx {
			t.Fatalf("expected stale view to preserve nextUserActionIdx=%d, got %d", state.nextUserActionIdx, v.nextUserActionIdx)
		}
		if v.Contents.KindSequences[kindKey] < currentSeq {
			foundStrictlyStale = true
			break
		}
	}
	if !foundStrictlyStale {
		t.Fatalf("expected at least one strictly stale view for %s", kindKey)
	}
}

func newTestStateEvent(sequence int64, group, kind, version string) StateEvent {
	key := snapshot.NewCompositeKeyWithGroup(group, kind, "default", "demo", "obj-1")
	return StateEvent{
		Event: &event.Event{
			ID:           fmt.Sprintf("evt-%d", sequence),
			ControllerID: "writer",
			ReconcileID:  "writer-reconcile",
			Timestamp:    fmt.Sprintf("2026-01-01T00:00:%02dZ", sequence),
			Group:        group,
			Kind:         kind,
			APIVersion:   "v1",
			ObjectID:     "obj-1",
		},
		ReconcileID: "writer-reconcile",
		Timestamp:   fmt.Sprintf("2026-01-01T00:00:%02dZ", sequence),
		Sequence:    sequence,
		Effect:      newEffect(key, snapshot.NewDefaultHash(version), event.UPDATE),
	}
}

func stateNodeFromEvents(t *testing.T, events ...StateEvent) StateNode {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("stateNodeFromEvents requires at least one event")
	}
	snapshot := replayEventSequenceToState(events)
	return StateNode{Contents: *snapshot}
}
