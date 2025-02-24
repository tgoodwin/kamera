package tracecheck

import (
	"fmt"
	"os"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
)

// ObjectVersions is a map of object IDs to their version hashes
type ObjectVersions map[snapshot.IdentityKey]snapshot.VersionHash

func (ov ObjectVersions) Equals(other ObjectVersions) bool {
	if len(ov) != len(other) {
		return false
	}
	for key, value := range ov {
		if otherValue, exists := other[key]; !exists || otherValue != value {
			return false
		}
	}
	return true
}

func (ov ObjectVersions) Objects() ObjectVersions {
	return ov
}

type Delta string

type ReconcileResult struct {
	ControllerID string
	FrameID      string
	Changes      ObjectVersions // this is just the writeset, not the resulting full state of the world
	Deltas       map[snapshot.IdentityKey]Delta
}

type ExecutionHistory []*ReconcileResult

func (eh ExecutionHistory) SummarizeToFile(file *os.File) error {
	for _, r := range eh {
		_, err := fmt.Fprintf(file, "\t%s:%s - #changes=%d\n", r.ControllerID, r.FrameID, len(r.Deltas))
		if err != nil {
			return err
		}
		for key, d := range r.Deltas {
			_, err := fmt.Fprintf(file, "\t%s: %s\n", key, d)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (eh ExecutionHistory) Summarize() {
	err := eh.SummarizeToFile(os.Stdout)
	if err != nil {
		fmt.Printf("Error summarizing to stdout: %v\n", err)
	}
}

func (eh ExecutionHistory) FilterNoOps() ExecutionHistory {
	var filtered ExecutionHistory
	for _, r := range eh {
		if len(r.Deltas) > 0 {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

func GetUniquePaths(paths []ExecutionHistory) []ExecutionHistory {
	pathsWithoutNoOps := lo.Map(paths, func(path ExecutionHistory, _ int) ExecutionHistory {
		return path.FilterNoOps()
	})
	// filter out empty paths
	pathsWithoutNoOps = lo.Filter(pathsWithoutNoOps, func(path ExecutionHistory, _ int) bool {
		return len(path) > 0
	})
	unique := lo.UniqBy(pathsWithoutNoOps, func(path ExecutionHistory) string {
		return strings.Join(
			lo.Map(path, func(r *ReconcileResult, _ int) string {
				return r.ControllerID
			}), ",",
		)
	})

	return unique
}

type ObservableState interface {
	Objects() ObjectVersions
}

type StateNode struct {
	objects ObservableState
	// PendingReconciles is a list of controller IDs that are pending reconciliation.
	// In our "game tree", they represent branches that we can explore.
	PendingReconciles []string

	parent *StateNode
	action *ReconcileResult // the action that led to this state

	// ExecutionHistory tracks the sequence of reconciles that led to this state
	ExecutionHistory ExecutionHistory

	depth int
}

func (sn StateNode) IsConverged() bool {
	return len(sn.PendingReconciles) == 0
}

func (sn StateNode) Objects() ObjectVersions {
	return sn.objects.Objects()
}

func (sn StateNode) Summarize() {
	// TODO
	fmt.Printf("---------StateNode Summary: depth %d---------\n", sn.depth)
	if sn.parent == nil {
		fmt.Println("Top-Level StateNode")
	}

	// print the controller that created this state
	if sn.action != nil {
		fmt.Println("ControllerID: ", sn.action.ControllerID)
		fmt.Println("Num Changes: ", len(sn.action.Changes))
		fmt.Println("Pending Reconciles: ", sn.PendingReconciles)
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
