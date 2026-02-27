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
