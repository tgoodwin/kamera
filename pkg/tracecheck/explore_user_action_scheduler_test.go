package tracecheck

import (
	"testing"

	"github.com/tgoodwin/kamera/pkg/event"
)

func TestShouldApplyNextUserAction_QuiescencePolicy(t *testing.T) {
	explorer := &Explorer{
		userController: &UserController{
			reconciler: &userActionReconciler{actions: []UserAction{{ID: "a1", OpType: event.CREATE}}},
		},
	}

	if !explorer.shouldApplyNextUserAction(StateNode{}) {
		t.Fatalf("expected user action to be schedulable when branch is quiescent and action remains")
	}

	nonQuiescent := StateNode{
		PendingReconciles: []PendingReconcile{{ReconcilerID: "ServiceController", Source: SourceStateChange}},
	}
	if explorer.shouldApplyNextUserAction(nonQuiescent) {
		t.Fatalf("expected false when branch has actionable pending reconciles")
	}

	ignorableOnly := StateNode{
		PendingReconciles: []PendingReconcile{{ReconcilerID: "ticker", Source: SourceAsyncEnqueue}},
	}
	if !explorer.shouldApplyNextUserAction(ignorableOnly) {
		t.Fatalf("expected true when only ignorable pending reconciles remain")
	}

	if explorer.shouldApplyNextUserAction(StateNode{nextUserActionIdx: 1}) {
		t.Fatalf("expected false when no remaining user actions for branch index")
	}
}

func TestShouldApplyNextUserAction_NoUserControllerOrActions(t *testing.T) {
	if (&Explorer{}).shouldApplyNextUserAction(StateNode{}) {
		t.Fatalf("expected false when explorer has no user controller")
	}

	explorer := &Explorer{userController: &UserController{}}
	if explorer.shouldApplyNextUserAction(StateNode{}) {
		t.Fatalf("expected false when user controller has no remaining actions")
	}
}

func TestShouldApplyNextUserAction_ReadyDepthScheduling(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			Perturbations: PerturbationConfig{
				UserActionReadyDepths: map[int]int{1: 5},
			},
		},
		userController: &UserController{
			reconciler: &userActionReconciler{
				actions: []UserAction{
					{ID: "a0", OpType: event.CREATE},
					{ID: "a1", OpType: event.UPDATE},
				},
			},
		},
	}

	beforeReadyDepthNonConverged := StateNode{
		depth:             4,
		nextUserActionIdx: 1,
		PendingReconciles: []PendingReconcile{
			{ReconcilerID: "svc", Source: SourceStateChange},
		},
	}
	if explorer.shouldApplyNextUserAction(beforeReadyDepthNonConverged) {
		t.Fatalf("expected false before ready depth when state is non-converged")
	}

	atReadyDepthNonConverged := beforeReadyDepthNonConverged
	atReadyDepthNonConverged.depth = 5
	if !explorer.shouldApplyNextUserAction(atReadyDepthNonConverged) {
		t.Fatalf("expected true at ready depth even when state is non-converged")
	}

	earlyConverged := StateNode{
		depth:             3,
		nextUserActionIdx: 1,
	}
	if explorer.shouldApplyNextUserAction(earlyConverged) {
		t.Fatalf("expected false for converged state below scheduled ready depth")
	}
}

func TestNextReadyDepth_PrefersEarliestScheduledEvent(t *testing.T) {
	explorer := &Explorer{
		Config: &ExploreConfig{
			Perturbations: PerturbationConfig{
				UserActionReadyDepths: map[int]int{1: 5},
			},
		},
		userController: &UserController{
			reconciler: &userActionReconciler{
				actions: []UserAction{
					{ID: "a0", OpType: event.CREATE},
					{ID: "a1", OpType: event.UPDATE},
				},
			},
		},
	}

	state := StateNode{
		depth:             3,
		nextUserActionIdx: 1,
		PendingReconciles: []PendingReconcile{
			{ReconcilerID: "svc", Source: SourceRequeueAfter, ReadyAtDepth: 7},
		},
	}

	nextDepth, ok := explorer.nextReadyDepth(state)
	if !ok {
		t.Fatalf("expected next ready depth to be available")
	}
	if nextDepth != 5 {
		t.Fatalf("expected earliest scheduled depth 5, got %d", nextDepth)
	}
}

func TestIsTerminalConvergedState_RequiresUserActionExhaustion(t *testing.T) {
	explorer := &Explorer{
		userController: &UserController{
			reconciler: &userActionReconciler{
				actions: []UserAction{{ID: "a1", OpType: event.CREATE}},
			},
		},
	}

	if explorer.isTerminalConvergedState(StateNode{}) {
		t.Fatalf("expected non-terminal state when user action remains")
	}

	if !explorer.isTerminalConvergedState(StateNode{nextUserActionIdx: 1}) {
		t.Fatalf("expected terminal when user actions are exhausted and pending is empty")
	}

	if !explorer.isTerminalConvergedState(StateNode{
		nextUserActionIdx: 1,
		PendingReconciles: []PendingReconcile{
			{ReconcilerID: "ticker", Source: SourceAsyncEnqueue},
		},
	}) {
		t.Fatalf("expected terminal when only ignorable pending reconciles remain and user actions are exhausted")
	}

	if explorer.isTerminalConvergedState(StateNode{
		nextUserActionIdx: 1,
		PendingReconciles: []PendingReconcile{
			{ReconcilerID: "svc", Source: SourceStateChange},
		},
	}) {
		t.Fatalf("expected non-terminal state with actionable pending reconciles")
	}
}
