package tracecheck

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/snapshot"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type reconciler interface {
	doReconcile(ctx context.Context, readset ObjectVersions) (*ReconcileResult, error)
	replayReconcile(ctx context.Context, frame *replay.Frame) (*ReconcileResult, error)
}

type ReconcilerContainer struct {
	*reconcileImpl
	harness *replay.Harness
}

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]ReconcilerContainer
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies ResourceDeps

	knowledgeManager *EventKnowledge

	// config
	maxDepth     int
	hashStrategy snapshot.HashStrategy // how to serialize states for equality comparison
}

type ConvergedState struct {
	ID    string
	State StateNode
	Paths []ExecutionHistory
}

type Result struct {
	ConvergedStates []ConvergedState
	Duration        time.Duration
	AbortedPaths    int
}

func (e *Explorer) shouldExplore(frameID string) bool {
	return strings.HasPrefix(frameID, "")
}

func (e *Explorer) Walk(reconciles []replay.ReconcileEvent) *Result {
	currExecutionHistory := make(ExecutionHistory, 0)

	result := &Result{
		ConvergedStates: make([]ConvergedState, 0),
	}

	var rebuiltState *StateSnapshot
	for _, reconcile := range reconciles {
		reconcilerID := reconcile.ControllerID
		if _, ok := e.reconcilers[reconcilerID]; !ok {
			panic(fmt.Sprintf("reconciler %s not found", reconcilerID))
		}
		harness := e.reconcilers[reconcilerID].harness
		// this just contains the traced reconcile.Request object
		frame, err := harness.FrameForReconcile(reconcile.ReconcileID)
		if err != nil {
			panic(fmt.Sprintf("frame not found for reconcileID %s", reconcile.ReconcileID))
		}
		fmt.Println("\nReplaying ReconcileID", frame.ID, "for reconciler", reconcilerID)

		rebuiltState = e.knowledgeManager.GetStateAtReconcileID(reconcile.ReconcileID)
		fmt.Printf("rebuilt state ahead of reconcile:%s - # objects: %d\n", util.Shorter(reconcile.ReconcileID), len(rebuiltState.contents))
		rebuiltState.contents.Summarize()

		reconciler := e.reconcilers[reconcilerID]
		ctx := replay.WithFrameID(context.Background(), frame.ID)
		res, err := reconciler.replayReconcile(ctx, frame)
		if err != nil {
			logger.Error(err, "replaying reconcile")
			return nil
		}

		changes := res.Changes
		// TODO this is not working due to anonymization
		res.Deltas = reconciler.computeDeltas(rebuiltState.contents, changes)

		fmt.Println("Reconcile result - # changes:", len(changes))
		changes.Summarize()

		resultingState := e.knowledgeManager.GetStateAfterReconcileID(reconcile.ReconcileID)
		fmt.Printf("resulting state after reconcile:%s - # objects: %d\n", util.Shorter(reconcile.ReconcileID), len(resultingState.contents))
		resultingState.contents.Summarize()

		currExecutionHistory = append(currExecutionHistory, res)

		// breaking off to explore mode
		if e.shouldExplore(reconcile.ReconcileID) {
			triggeredByLastChange := e.getTriggeredReconcilers(changes)
			sn := StateNode{
				DivergencePoint: reconcile.ReconcileID,
				// top-level state to explore
				objects:           resultingState.contents,
				PendingReconciles: triggeredByLastChange,
				ExecutionHistory:  slices.Clone(currExecutionHistory),
			}
			subRes := e.Explore(context.Background(), sn)
			result.ConvergedStates = append(result.ConvergedStates, subRes.ConvergedStates...)
			fmt.Println("explored state space, found", len(subRes.ConvergedStates), "converged states")
		}
	}

	currExecutionHistory.Summarize()

	// the end of the trace trivially converges
	traceWalkResult := ConvergedState{
		State: StateNode{objects: rebuiltState.contents, DivergencePoint: "TRACE_START"},
		Paths: []ExecutionHistory{currExecutionHistory},
	}
	result.ConvergedStates = append(result.ConvergedStates, traceWalkResult)
	fmt.Println("len curr execution history", len(currExecutionHistory))

	return result
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
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
		ConvergedStates: make([]ConvergedState, 0),
	}
	start := time.Now()

	seenDepths := make(map[int]bool)
	queue := []StateNode{initialState}

	// executionPathsToState is a map of stateKey -> ExecutionHistory
	// because we want to track which states we've visited but
	// also want to track all the ways a given state can be reached
	executionPathsToState := make(map[string][]ExecutionHistory)

	seenConvergedStates := make(map[string]StateNode)

	// var currentState StateNode
	for len(queue) > 0 {
		currentState := queue[0]
		queue = queue[1:]
		stateKey := serializeState(currentState)

		if _, seen := executionPathsToState[stateKey]; !seen {
			executionPathsToState[stateKey] = make([]ExecutionHistory, 0)
			executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
		} else {
			executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
			// continue
		}

		if len(currentState.PendingReconciles) == 0 {
			seenConvergedStates[stateKey] = currentState
		}

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state. We explore each pending reconcile in a breadth-first manner.
		for _, controller := range currentState.PendingReconciles {
			newState := e.takeReconcileStep(ctx, currentState, controller.ReconcilerID)
			newState.depth = currentState.depth + 1
			if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
				logger.Info("\rexplore reached depth", "depth", newState.depth)
				seenDepths[newState.depth] = true
			}
			if newState.depth > e.maxDepth {
				// fmt.Println("Reached max depth", e.maxDepth)
				result.AbortedPaths += 1
			} else {
				queue = append(queue, newState)
			}
		}
	}

	// Graph search has ended, summarize the results
	result.Duration = time.Since(start)
	for i, stateKey := range lo.Keys(seenConvergedStates) {
		state := seenConvergedStates[stateKey]
		paths := executionPathsToState[stateKey]

		state.DivergencePoint = initialState.DivergencePoint
		convergedState := ConvergedState{
			ID:    fmt.Sprintf("state-%d", i),
			State: state,
			Paths: paths,
		}
		result.ConvergedStates = append(result.ConvergedStates, convergedState)
	}

	return result
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, controllerID string) StateNode {
	logger = log.FromContext(ctx)
	// remove the current controller from the pending reconciles list
	newPendingReconciles := lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
		return pending.ReconcilerID != controllerID
	})

	// get the state diff after executing the controller.
	// if the state diff is empty, then the controller did not change anything
	reconcileResult := e.reconcileAtState(ctx, state, controllerID)

	// update the state with the new object versions
	newObjectVersions := make(ObjectVersions)
	for iKey, version := range state.Objects() {
		newObjectVersions[iKey] = version
	}
	for iKey, newVersion := range reconcileResult.Changes {
		newObjectVersions[iKey] = newVersion
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(reconcileResult.Changes)
	newPendingReconciles = getNewPendingReconciles(newPendingReconciles, triggeredReconcilers)
	logger.V(2).WithValues("controllerID", controllerID, "pending", newPendingReconciles).Info("--Finished Reconcile--")

	// make a copy of the current execution history
	currHistory := append([]*ReconcileResult{}, state.ExecutionHistory...)

	return StateNode{
		objects:           newObjectVersions,
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            reconcileResult,

		ExecutionHistory: append(currHistory, reconcileResult),
	}
}

// TODO figure out if we need to append to the front if using DFS
func getNewPendingReconciles(currPending, triggered []PendingReconcile) []PendingReconcile {
	// Union does not change the order of elements relatively, but it does remove duplicates
	return lo.Union(currPending, triggered)
}

func (e *Explorer) reconcileAtState(ctx context.Context, state StateNode, controllerID string) *ReconcileResult {
	reconciler, ok := e.reconcilers[controllerID]
	if !ok {
		panic(fmt.Sprintf("implementation for reconciler %s not found", controllerID))
	}
	// get the read set
	readSet := state.Objects()
	// execute the controller
	// convert the write set to object versions
	result, err := reconciler.doReconcile(ctx, readSet)
	if err != nil {
		panic(fmt.Sprintf("error executing reconcile for %s: %s", controllerID, err))
	}
	return result
}

func (e *Explorer) getTriggeredReconcilers(changes ObjectVersions) []PendingReconcile {
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
	out := lo.Map(triggeredList, func(s string, _ int) PendingReconcile {
		return PendingReconcile{
			ReconcilerID: s,
			Request:      reconcile.Request{},
		}
	})
	return out
}

// serializeState converts a State to a unique string representation for deduplication
func serializeState(state StateNode) string {
	var objectPairs []string
	for objKey, version := range state.Objects() {
		objectPairs = append(objectPairs, fmt.Sprintf("%s=%s", objKey.ObjectID, version.Value))
	}
	// Sort objectPairs to ensure deterministic order
	sort.Strings(objectPairs)
	objectsStr := strings.Join(objectPairs, ",")
	sortedPendingReconciles := append([]PendingReconcile{}, state.PendingReconciles...)
	sort.Slice(sortedPendingReconciles, func(i, j int) bool {
		return sortedPendingReconciles[i].ReconcilerID < sortedPendingReconciles[j].ReconcilerID
	})
	reconcileStr := lo.Map(sortedPendingReconciles, func(pr PendingReconcile, _ int) string {
		return pr.ReconcilerID
	})
	reconcilesStr := strings.Join(reconcileStr, ",")
	// reconcilesStr := strings.Join(state.PendingReconciles, ",")

	// Sort PendingReconciles to ensure deterministic order
	// sortedPendingReconciles := append([]string{}, state.PendingReconciles...)
	// sort.Strings(sortedPendingReconciles)
	// reconcilesStr := strings.Join(sortedPendingReconciles, ",")

	return fmt.Sprintf("Objects:{%s}|PendingReconciles:{%s}", objectsStr, reconcilesStr)
}
