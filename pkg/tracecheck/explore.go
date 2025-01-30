package tracecheck

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type reconciler interface {
	doReconcile(ctx context.Context, readset ObjectVersions) (*ReconcileResult, error)
}

type resourceDeps map[string]util.Set[string]

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]reconciler
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies resourceDeps

	maxDepth int
}

type Result struct {
	ConvergedStates []StateNode
	Duration        time.Duration
	AbortedPaths    int
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
func (e *Explorer) exploreDFS(ctx context.Context, initialState StateNode) *Result {
	if e.maxDepth == 0 {
		e.maxDepth = 10
	}

	startTime := time.Now()
	result := &Result{
		ConvergedStates: make([]StateNode, 0),
	}

	stack := []StateNode{initialState}
	seenStates := make(map[string]bool)
	// convergedStates := make([]StateNode, 0)

	for len(stack) > 0 {
		currentState := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		stateKey := serializeState(currentState)
		if seenStates[stateKey] {
			fmt.Println("Skipping already seen state at depth", currentState.depth)
			continue
		}
		seenStates[stateKey] = true

		if len(currentState.PendingReconciles) == 0 {
			// TODO evaluate some predicates upon the converged state and then classify the execution
			fmt.Println("Found a converged state at depth", currentState.depth)
			result.ConvergedStates = append(result.ConvergedStates, currentState)
		}

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state. We explore each pending reconcile in a depth-first manner.
		for _, controller := range currentState.PendingReconciles {
			newState := e.takeReconcileStep(ctx, currentState, controller)
			newState.depth = currentState.depth + 1
			if newState.depth > e.maxDepth {
				result.AbortedPaths += 1
				fmt.Println("Reached max depth", e.maxDepth)
			} else {
				stack = append(stack, newState)
			}
		}
	}
	endTime := time.Now()
	delta := endTime.Sub(startTime)
	result.Duration = delta

	return result
}

func getNext(stackQueue []StateNode, mode string) (StateNode, []StateNode) {
	if mode == "stack" {
		return stackQueue[len(stackQueue)-1], stackQueue[:len(stackQueue)-1]
	} else if mode == "queue" {
		return stackQueue[0], stackQueue[1:]
	}
	panic("Invalid mode")
}

func (e *Explorer) Explore(ctx context.Context, initialState StateNode) *Result {
	return e.exploreBFS(ctx, initialState)
}

func (e *Explorer) exploreBFS(ctx context.Context, initialState StateNode) *Result {
	if e.maxDepth == 0 {
		e.maxDepth = 10
	}

	result := &Result{
		ConvergedStates: make([]StateNode, 0),
	}
	start := time.Now()

	queue := []StateNode{initialState}
	seenStates := make(map[string]bool)

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
			result.ConvergedStates = append(result.ConvergedStates, currentState)
			continue
		}

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state. We explore each pending reconcile in a breadth-first manner.
		for _, controller := range currentState.PendingReconciles {
			newState := e.takeReconcileStep(ctx, currentState, controller)
			newState.depth = currentState.depth + 1
			if newState.depth > e.maxDepth {
				fmt.Println("Reached max depth", e.maxDepth)
				result.AbortedPaths += 1
			} else {
				queue = append(queue, newState)
			}
		}
	}

	result.Duration = time.Since(start)
	return result
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
	reconcileResult := e.reconcileAtState(ctx, state, controllerID)

	proceed := state.proceed
	// update the state with the new object versions
	newObjectVersions := make(ObjectVersions)
	for objID, version := range state.ObjectVersions {
		newObjectVersions[objID] = version
	}
	for objID, newVersion := range reconcileResult.Changes {
		newObjectVersions[objID] = newVersion
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(reconcileResult.Changes)
	newPendingReconciles = append(newPendingReconciles, triggeredReconcilers...)
	logger.V(0).WithValues("controllerID", controllerID, "pending", newPendingReconciles).Info("--Finished Reconcile--")

	return StateNode{
		ObjectVersions:    newObjectVersions,
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            reconcileResult,
		proceed:           proceed,
	}
}

func (e *Explorer) reconcileAtState(ctx context.Context, state StateNode, controllerID string) *ReconcileResult {
	reconciler, ok := e.reconcilers[controllerID]
	if !ok {
		panic(fmt.Sprintf("implementation for reconciler %s not found", controllerID))
	}
	// get the read set
	readSet := state.ObjectVersions
	// execute the controller
	// convert the write set to object versions
	result, err := reconciler.doReconcile(ctx, readSet)
	if err != nil {
		panic(fmt.Sprintf("error executing reconcile for %s: %s", controllerID, err))
	}
	return result
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
	// Sort objectPairs to ensure deterministic order
	sort.Strings(objectPairs)
	objectsStr := strings.Join(objectPairs, ",")
	reconcilesStr := strings.Join(state.PendingReconciles, ",")

	// Sort PendingReconciles to ensure deterministic order
	// sortedPendingReconciles := append([]string{}, state.PendingReconciles...)
	// sort.Strings(sortedPendingReconciles)
	// reconcilesStr := strings.Join(sortedPendingReconciles, ",")

	return fmt.Sprintf("Objects:{%s}|PendingReconciles:{%s}", objectsStr, reconcilesStr)
}
