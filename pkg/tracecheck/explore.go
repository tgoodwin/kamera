package tracecheck

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ObjectVersions is a map of object IDs to their version hashes
type ObjectVersions map[snapshot.IdentityKey]snapshot.VersionHash

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

	depth int
}

type reconciler interface {
	doReconcile(ctx context.Context, readset ObjectVersions) (ObjectVersions, error)
}

type resourceDeps map[string]util.Set[string]

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]reconciler
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies resourceDeps

	maxDepth int
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
func (e *Explorer) Explore(ctx context.Context, initialState StateNode) []StateNode {
	if e.maxDepth == 0 {
		e.maxDepth = 10
	}

	queue := []StateNode{initialState}

	seenStates := make(map[string]bool)

	convergedStates := make([]StateNode, 0)

	for len(queue) > 0 {
		currentState := queue[0]
		queue = queue[1:]
		stateKey := serializeState(currentState)
		if seenStates[stateKey] {
			fmt.Println("Skipping already seen state at depth", currentState.depth)
			continue
		}
		seenStates[stateKey] = true

		if len(currentState.PendingReconciles) == 0 {
			// TODO evaluate some predicates upon the converged state and then classify the execution
			fmt.Println("Found a converged state at depth", currentState.depth)
			convergedStates = append(convergedStates, currentState)
			// time.Sleep(1 * time.Second)
			continue
		}

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state. We explore each pending reconcile in a breadth-first manner.
		for _, controller := range currentState.PendingReconciles {
			newState := e.takeReconcileStep(ctx, currentState, controller)
			newState.depth = currentState.depth + 1
			if newState.depth > e.maxDepth {
				fmt.Println("Reached max depth", e.maxDepth)
			} else {
				queue = append(queue, newState)
			}
		}
	}

	return convergedStates
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, controllerID string) StateNode {
	logger = log.FromContext(ctx)
	// remove the current controller from the pending reconciles list
	newPendingReconciles := lo.Filter(state.PendingReconciles, func(pending string, _ int) bool {
		return pending != controllerID
	})

	// get the state diff after executing the controller.
	// if the state diff is empty, then the controller did not change anything
	versionChanges := e.reconcileAtState(ctx, state, controllerID)

	// update the state with the new object versions
	newObjectVersions := make(ObjectVersions)
	for objID, version := range state.ObjectVersions {
		newObjectVersions[objID] = version
	}
	for objID, newVersion := range versionChanges {
		newObjectVersions[objID] = newVersion
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
	logger.V(0).WithValues("controllerID", controllerID, "pending", newPendingReconciles).Info("--Finished Reconcile--")

	return StateNode{
		ObjectVersions:    newObjectVersions,
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            action,
	}
}

func (e *Explorer) reconcileAtState(ctx context.Context, state StateNode, controllerID string) ObjectVersions {
	reconciler, ok := e.reconcilers[controllerID]
	if !ok {
		panic(fmt.Sprintf("implementation for reconciler %s not found", controllerID))
	}
	// get the read set
	readSet := state.ObjectVersions
	// execute the controller
	// convert the write set to object versions
	writeSet, err := reconciler.doReconcile(ctx, readSet)
	if err != nil {
		panic(fmt.Sprintf("error executing reconcile for %s: %s", controllerID, err))
	}
	return writeSet
}

func (e *Explorer) getTriggeredReconcilers(changes ObjectVersions) []string {
	triggered := make(util.Set[string])
	for objKey := range changes {
		// get the controllers that depend on this object
		triggeredForKind := e.dependencies[objKey.Kind]
		triggered = triggered.Union(triggeredForKind)
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
