package tracecheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func testPending(reconcilerID, namespace, name string) PendingReconcile {
	return PendingReconcile{
		ReconcilerID: ReconcilerID(reconcilerID),
		Request: reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      name,
			},
		},
	}
}

func testState(pending ...PendingReconcile) StateNode {
	return StateNode{
		Contents:          NewStateSnapshot(ObjectVersions{}, KindSequences{}, nil),
		PendingReconciles: pending,
	}
}

func TestEnqueueStatesWithSubtreeCompletion(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newSubtreeTracker()

	state := testState(testPending("controller-a", "default", "obj-a"))

	stack, enqueued := explorer.enqueueStates(nil, tracker, []StateNode{state, state.Clone()}, true)
	assert.True(t, enqueued)
	assert.Len(t, stack, 3)
	assert.True(t, stack[0].isMarker())
	assert.False(t, stack[1].isMarker())
	assert.False(t, stack[2].isMarker())
	assert.Equal(t, state.LogicalKey(), *stack[0].marker)
}

func TestEnqueueStatesSkipsCompletedLogicalState(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newSubtreeTracker()

	state := testState(testPending("controller-a", "default", "obj-a"))
	tracker.markCompleted(state.LogicalKey())

	stack, enqueued := explorer.enqueueStates(nil, tracker, []StateNode{state}, true)
	assert.False(t, enqueued)
	assert.Empty(t, stack)
	assert.Equal(t, 1, explorer.stats.SubtreeCompletionSkips)
}

func TestEnqueueStatesSkipsInProgressDiamondState(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newSubtreeTracker()

	state := testState(testPending("controller-a", "default", "obj-a"))
	tracker.markInProgress(state.LogicalKey())

	stack, enqueued := explorer.enqueueStates(nil, tracker, []StateNode{state}, true)
	assert.False(t, enqueued)
	assert.Empty(t, stack)
	assert.Equal(t, 1, explorer.stats.SubtreeDiamondSkips)
}

func TestEnqueueStatesAllowsInProgressCycleState(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newSubtreeTracker()

	ancestor := testState(testPending("controller-a", "default", "obj-a"))
	cycle := ancestor.Clone()
	cycle.parent = &ancestor
	tracker.markInProgress(ancestor.LogicalKey())

	stack, enqueued := explorer.enqueueStates(nil, tracker, []StateNode{cycle}, true)
	assert.True(t, enqueued)
	assert.Len(t, stack, 1)
	assert.False(t, stack[0].isMarker())
	assert.Equal(t, 0, explorer.stats.SubtreeDiamondSkips)
}

func TestEnqueueStaleViewsWithSubtreeCompletion(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newStaleViewTracker()

	parent := testState(testPending("controller-a", "default", "obj-a"))
	pending := parent.PendingReconciles[0]
	views := []StateNode{
		testState(parent.PendingReconciles...),
		testState(parent.PendingReconciles...),
	}

	stack, enqueued := explorer.enqueueStaleViewStates(nil, tracker, parent, pending, views, true)
	assert.True(t, enqueued)
	assert.Len(t, stack, 3)
	assert.True(t, stack[0].isStaleViewMarker())
	assert.True(t, stack[1].isStaleViewEntry())
	assert.True(t, stack[2].isStaleViewEntry())
	assert.Equal(t, pending, *stack[1].staleViewReconcile)
	assert.Equal(t, pending, *stack[2].staleViewReconcile)
}

func TestEnqueueStaleViewsSkipsCompletedBranch(t *testing.T) {
	explorer := &Explorer{stats: NewExploreStats()}
	tracker := newStaleViewTracker()

	parent := testState(testPending("controller-a", "default", "obj-a"))
	pending := parent.PendingReconciles[0]
	views := []StateNode{
		testState(parent.PendingReconciles...),
		testState(parent.PendingReconciles...),
	}
	tracker.markCompleted(StaleViewBranchKey{
		ParentLogicalKey: parent.LogicalKey(),
		ReconcilerID:     pending.ReconcilerID,
		RequestKey:       pending.Request.NamespacedName.String(),
	})

	stack, enqueued := explorer.enqueueStaleViewStates(nil, tracker, parent, pending, views, true)
	assert.False(t, enqueued)
	assert.Empty(t, stack)
	assert.Equal(t, 1, explorer.stats.StaleViewCompletionSkips)
}
