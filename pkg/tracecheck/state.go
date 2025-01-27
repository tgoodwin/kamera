package tracecheck

import (
	"fmt"

	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// ObjectVersions is a map of object IDs to their version hashes
type ObjectVersions map[snapshot.IdentityKey]snapshot.VersionHash

type ReconcileResult struct {
	ControllerID string
	FrameID      string
	Changes      ObjectVersions // this is just the writeset, not the resulting full state of the world
}

type StateNode struct {
	ObjectVersions ObjectVersions
	// PendingReconciles is a list of controller IDs that are pending reconciliation.
	// In our "game tree", they represent branches that we can explore.
	PendingReconciles []string

	parent *StateNode
	action *ReconcileResult // the action that led to this state

	depth int
	// TODO remove
	proceed bool
}

func (sn StateNode) IsConverged() bool {
	return len(sn.PendingReconciles) == 0
}

func (sn StateNode) Summarize() {
	// TODO
	fmt.Printf("---StateNode Summary: depth %d---\n", sn.depth)
	if sn.parent == nil {
		fmt.Println("Top-Level StateNode")
	}

	// print the controller that created this state
	if sn.action != nil {
		fmt.Println("ControllerID: ", sn.action.ControllerID)
		fmt.Println("FrameID: ", sn.action.FrameID)
		fmt.Println("Num Changes: ", len(sn.action.Changes))
		fmt.Println("Resulting Pending Reconciles: ", sn.PendingReconciles)
	}
}

func (sn StateNode) SummarizeFromRoot() {
	if sn.parent != nil {
		sn.parent.SummarizeFromRoot()
	} else {
		fmt.Println("Root StateNode")
	}
	sn.Summarize()
}
