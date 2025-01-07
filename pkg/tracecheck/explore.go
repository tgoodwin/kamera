package tracecheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
)

type ObjectKey struct {
	Kind     string // resource type
	ObjectID string // uid
}

// ObjectVersions is a map of object IDs to their version hashes
type ObjectVersions map[ObjectKey]snapshot.VersionHash

type ReconcileResult struct {
	ControllerID string
	Changes      ObjectVersions // this is just the writeset, not the resulting full state of the world
}

type StateNode struct {
	ObjectVersions ObjectVersions
	// PendingReconciles is a list of controller IDs that are pending reconciliation.
	// In our "game tree", they represent branches that we can explore.
	PendingReconciles []string

	parent *StateNode
	action *ReconcileResult // the action that led to this state
}

type reconciler interface {
	doReconcile(readset ObjectVersions) (ObjectVersions, error)
}

type resourceDeps map[string]util.Set[string]

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]reconciler
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies resourceDeps
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
func (e *Explorer) Explore(initialState StateNode) {
	queue := []StateNode{initialState}

	seenStates := make(map[string]bool)

	for len(queue) > 0 {
		currentState := queue[0]
		queue = queue[1:]
		stateKey := serializeState(currentState)
		if seenStates[stateKey] {
			continue
		}
		seenStates[stateKey] = true

		if len(currentState.PendingReconciles) == 0 {
			// TODO evaluate some predicates upon the converged state and then classify the execution
			fmt.Println("Found a converged state: ", currentState)
			continue
		}

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state. We explore each pending reconcile in a breadth-first manner.
		for _, controller := range currentState.PendingReconciles {
			newState := e.takeReconcileStep(currentState, controller)
			queue = append(queue, newState)
		}
	}
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(state StateNode, controllerID string) StateNode {

	// remove the current controller from the pending reconciles list
	newPendingReconciles := lo.Filter(state.PendingReconciles, func(pending string, _ int) bool {
		return pending != controllerID
	})

	// get the state diff after executing the controller.
	// if the state diff is empty, then the controller did not change anything
	versionChanges := e.reconcileAtState(state, controllerID)

	// update the state with the new object versions
	newObjectVersions := make(ObjectVersions)
	for objID, version := range state.ObjectVersions {
		if newVersion, ok := versionChanges[objID]; ok {
			newObjectVersions[objID] = newVersion
		} else {
			newObjectVersions[objID] = version
		}
	}

	action := &ReconcileResult{
		ControllerID: controllerID,
		Changes:      versionChanges,
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(versionChanges)
	newPendingReconciles = append(newPendingReconciles, triggeredReconcilers...)

	return StateNode{
		ObjectVersions:    newObjectVersions,
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            action,
	}
}

func (e *Explorer) reconcileAtState(state StateNode, controllerID string) ObjectVersions {
	reconciler := e.reconcilers[controllerID]
	// get the read set
	readSet := state.ObjectVersions
	// execute the controller
	// convert the write set to object versions
	writeSet, _ := reconciler.doReconcile(readSet)
	return writeSet
}

func (e *Explorer) getTriggeredReconcilers(changes ObjectVersions) []string {
	triggered := make(util.Set[string])
	for objKey := range changes {
		// get the controllers that depend on this object
		triggeredForKind := e.dependencies[objKey.Kind]
		triggered.Union(triggeredForKind)
	}
	// convert the set to a list that is sorted
	triggeredList := triggered.List()
	// sort the list so that we can explore the triggered controllers in a deterministic order
	sort.Strings(triggeredList)
	return triggeredList
}

// serializeState converts a State to a unique string representation for deduplication
func serializeState(state StateNode) string {
	var objectPairs []string
	for objID, version := range state.ObjectVersions {
		objectPairs = append(objectPairs, fmt.Sprintf("%s=%s", objID, version))
	}
	objectsStr := strings.Join(objectPairs, ",")
	reconcilesStr := strings.Join(state.PendingReconciles, ",")
	return fmt.Sprintf("Objects:{%s}|PendingReconciles:{%s}", objectsStr, reconcilesStr)
}
