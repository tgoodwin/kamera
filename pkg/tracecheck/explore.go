package tracecheck

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type reconciler interface {
	doReconcile(ctx context.Context, readset ObjectVersions, req reconcile.Request) (*ReconcileResult, error)
	replayReconcile(ctx context.Context, req reconcile.Request) (*ReconcileResult, error)
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

	triggerManager *TriggerManager

	// config
	maxDepth int
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

func (e *Explorer) shouldExploreDownstream(frameID string) bool {
	return strings.HasPrefix(frameID, "821c")
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
		res, err := reconciler.replayReconcile(ctx, frame.Req)
		if err != nil {
			logger.Error(err, "replaying reconcile")
			return nil
		}

		changes := res.Changes
		// TODO this is not working due to anonymization
		res.Deltas = reconciler.computeDeltas(rebuiltState.contents, changes.objectVersions)

		fmt.Println("Reconcile result - # changes:", len(changes.objectVersions))
		changes.objectVersions.Summarize()
		for _, eff := range changes.effects {
			fmt.Printf("\top: %s, ikey: %s\n", eff.OpType, eff.ObjectKey)
		}

		resultingState := e.knowledgeManager.GetStateAfterReconcileID(reconcile.ReconcileID)
		fmt.Printf("resulting state after reconcile:%s - # objects: %d\n", util.Shorter(reconcile.ReconcileID), len(resultingState.contents))
		resultingState.contents.Summarize()

		currExecutionHistory = append(currExecutionHistory, res)

		// breaking off to explore to find alternative outcomes of the reconcile we just replayed.
		if e.shouldExploreDownstream(reconcile.ReconcileID) {
			fmt.Println("exploring downstream from reconcileID", reconcile.ReconcileID)

			// TODO have the list of effects by the input to getTriggeredReconcilers
			// to properly handle deletes
			triggeredByLastChange := e.getTriggeredReconcilers(changes.objectVersions)
			sn := StateNode{
				DivergencePoint: reconcile.ReconcileID,
				// top-level state to explore
				objects:           *resultingState,
				PendingReconciles: triggeredByLastChange,
				ExecutionHistory:  slices.Clone(currExecutionHistory),
			}
			fmt.Println("exploring state space at reconcileID", reconcile.ReconcileID)
			fmt.Printf("pending reconciles for frame %s: %v\n", res.FrameID, triggeredByLastChange)
			subRes := e.Explore(context.Background(), sn)
			result.ConvergedStates = append(result.ConvergedStates, subRes.ConvergedStates...)
			fmt.Println("explored state space, found", len(subRes.ConvergedStates), "converged states")
		}
	}

	currExecutionHistory.Summarize()

	// the end of the trace trivially converges
	traceWalkResult := ConvergedState{
		State: StateNode{objects: *rebuiltState, DivergencePoint: "TRACE_START"},
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
		for _, pendingReconcile := range currentState.PendingReconciles {

			// possibleViews := getPossibleViews(currentState, pendingReconcile)

			// for each view, create a new branch in exploration

			newState, err := e.takeReconcileStep(ctx, currentState, pendingReconcile)
			if err != nil {
				panic(fmt.Sprintf("error taking reconcile step: %s", err))
			}
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
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, pr PendingReconcile) (StateNode, error) {
	logger = log.FromContext(ctx)
	// remove the current controller from the pending reconciles list
	newPendingReconciles := lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
		return pending.ReconcilerID != pr.ReconcilerID
	})

	// get the state diff after executing the controller.
	// if the state diff is empty, then the controller did not change anything
	reconcileResult, err := e.reconcileAtState(ctx, state.Objects(), pr)
	if err != nil {
		return StateNode{}, err
	}

	newSequences := make(map[string]int64)
	maps.Copy(state.objects.KindSequences, newSequences)

	effects := reconcileResult.Changes.effects

	// update the state with the new object versions
	newObjectVersions := make(ObjectVersions)
	for iKey, version := range state.Objects() {
		newObjectVersions[iKey] = version
	}

	newStateEvents := slices.Clone(state.objects.stateEvents)

	for _, effect := range effects {
		if _, ok := newObjectVersions[effect.ObjectKey]; !ok {
			panic("object not found in state")
		}
		kind := effect.ObjectKey.Kind
		currSeqForKind := newSequences[kind]
		stateEvent := StateEvent{
			// Kind:   effect.ObjectKey.Kind,
			ReconcileID: reconcileResult.FrameID,
			Sequence:    currSeqForKind + 1,
			effect:      effect,
			// TODO handle time info
			Timestamp: "",
		}
		newStateEvents = append(newStateEvents, stateEvent)
	}

	for iKey, newVersion := range reconcileResult.Changes.objectVersions {
		newObjectVersions[iKey] = newVersion

		// increment resourceversion for the kind
		newSequences[iKey.Kind] += 1
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(reconcileResult.Changes.objectVersions)
	newPendingReconciles = getNewPendingReconciles(newPendingReconciles, triggeredReconcilers)
	logger.V(2).WithValues("reconcilerID", pr.ReconcilerID, "pending", newPendingReconciles).Info("--Finished Reconcile--")

	// make a copy of the current execution history
	currHistory := slices.Clone(state.ExecutionHistory)

	return StateNode{
		objects: StateSnapshot{
			contents:      newObjectVersions,
			KindSequences: newSequences,
			stateEvents:   newStateEvents,
		},
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            reconcileResult,

		ExecutionHistory: append(currHistory, reconcileResult),
	}, nil
}

// TODO figure out if we need to append to the front if using DFS
func getNewPendingReconciles(currPending, triggered []PendingReconcile) []PendingReconcile {
	// Union does not change the order of elements relatively, but it does remove duplicates
	return lo.Union(currPending, triggered)
}

func (e *Explorer) reconcileAtState(ctx context.Context, objState ObjectVersions, pr PendingReconcile) (*ReconcileResult, error) {
	reconciler, ok := e.reconcilers[pr.ReconcilerID]
	if !ok {
		return nil, fmt.Errorf("implementation for reconciler %s not found", pr.ReconcilerID)
	}

	if pr.Request.NamespacedName.Name == "" || pr.Request.NamespacedName.Namespace == "" {
		return nil, fmt.Errorf("empty reconcile request: %v", pr.Request)
	}

	// execute the controller
	// convert the write set to object versions
	result, err := reconciler.doReconcile(ctx, objState, pr.Request)
	if err != nil {
		return nil, err
	}
	return result, nil
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
	// out := lo.Map(triggeredList, func(s string, _ int) PendingReconcile {
	// 	return PendingReconcile{
	// 		ReconcilerID: s,
	// 		Request:      reconcile.Request{},
	// 	}
	// })
	alternative := e.triggerManager.MustGetTriggered(changes)
	// if len(alternative) != len(out) {
	// 	fmt.Println("triggered reconcilers mismatch")
	// 	fmt.Println("out", out)
	// 	fmt.Println("alternative", alternative)
	// }
	return alternative
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
	sortedPendingReconciles := slices.Clone(state.PendingReconciles)
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
