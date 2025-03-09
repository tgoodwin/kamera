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
	"github.com/tgoodwin/sleeve/pkg/event"
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
}

// EffectContextManager manages a "current state of the world" context
// for each branch of execution (not shared between branches). This is the state
// that reconcile effects are validated against before being applied.
type EffectContextManager interface {
	PrepareEffectContext(ctx context.Context, ov ObjectVersions) error
	CleanupEffectContext(ctx context.Context)
}

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]ReconcilerContainer
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies ResourceDeps

	knowledgeManager *EventKnowledge

	triggerManager *TriggerManager

	effectContextManager EffectContextManager

	// config
	maxDepth       int
	stalenessDepth int
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
	// TODO make this some actual heursitic. Right now, it's hardcoded against some
	// test data I was prototyping with.
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
		// this just contains the traced reconcile.Request object
		frame := reconcile.Frame
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
		res.Deltas = reconciler.computeDeltas(rebuiltState.contents, changes.ObjectVersions)

		fmt.Println("Reconcile result - # changes:", len(changes.ObjectVersions))
		changes.ObjectVersions.Summarize()
		for _, eff := range changes.Effects {
			fmt.Printf("\top: %s, ikey: %s\n", eff.OpType, eff.Key.IdentityKey)
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
			triggeredByLastChange := e.getTriggeredReconcilers(changes)
			sn := StateNode{
				DivergencePoint: reconcile.ReconcileID,
				// top-level state to explore
				Contents:          *resultingState,
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
		State: StateNode{Contents: *rebuiltState, DivergencePoint: "TRACE_START"},
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
	depthStart := start

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
			possibleViews, err := e.getPossibleViewsForReconcile(currentState, pendingReconcile.ReconcilerID)
			if err != nil {
				panic(fmt.Sprintf("error getting possible views: %s", err))
			}
			for _, possibleStateView := range possibleViews {
				// for each view, create a new branch in exploration
				newState, err := e.takeReconcileStep(ctx, possibleStateView, pendingReconcile)
				if err != nil {
					// if we encounter an error during reconciliation, just abandon this branch
					continue
				}
				newState.depth = currentState.depth + 1
				if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
					// logger.Info("\rexplore reached depth", "depth", newState.depth)
					elapsed := time.Since(depthStart)
					fmt.Printf("Explore reached depth %d, elapsed time: %s\n", newState.depth, elapsed)
					depthStart = time.Now()
					seenDepths[newState.depth] = true
				}
				if newState.depth > e.maxDepth {
					result.AbortedPaths += 1
				} else {
					queue = append(queue, newState)
				}
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

	// defensive validation
	if len(state.Contents.KindSequences) == 0 {
		panic("reconcile step: state has no kind sequences")
	}

	// create a new frameID for this reconcile state transition
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)

	// prepare the "true state of the world" for the controller
	fullState := state.Contents.Objects()
	fmt.Println("full state that we're preparing effect context for at frame: ", frameID)
	for k := range fullState {
		fmt.Println(k)
	}
	fmt.Println("---")
	e.effectContextManager.PrepareEffectContext(ctx, fullState)
	defer e.effectContextManager.CleanupEffectContext(ctx)

	// invoke the controller at its observed state of the world
	observableState := state.Contents.Observe()
	reconcileResult, err := e.reconcileAtState(ctx, observableState, pr)
	if err != nil {
		// return the pre-reconcile state if the controller errored
		return state, err
	}

	newSequences := make(map[string]int64)
	maps.Copy(newSequences, state.Contents.KindSequences)

	effects := reconcileResult.Changes.Effects

	// update the state with the new object versions.
	// note that we are updating the "global state" here,
	// which may be separate from what the controller saw upon reconciling.
	newObjectVersions := make(ObjectVersions)
	maps.Copy(newObjectVersions, state.Objects())

	changeOV := reconcileResult.Changes.ObjectVersions
	for _, effect := range effects {
		if effect.OpType == event.CREATE {
			if _, ok := newObjectVersions[effect.Key.IdentityKey]; ok {
				// the effect validation mechanism should prevent this from happening
				// so panic if it does happen
				panic("create effect object already exists in prev state: " + fmt.Sprintf("%s", effect.Key.IdentityKey))
			} else {
				// key not in state as expected, add it
				newObjectVersions[effect.Key.IdentityKey] = changeOV[effect.Key.IdentityKey]
			}
		}
		if effect.OpType == event.UPDATE || effect.OpType == event.PATCH {
			if _, ok := newObjectVersions[effect.Key.IdentityKey]; !ok {
				// it is possible that a stale read will cause a controller to update an object
				// that no longer exists in the global state. The effect validation mechanism
				// should cause the client operation to 404 and prevent the update effect from
				// going through. If it does go through, we should panic cause something broke.
				panic("update effect object not found in prev state: " + fmt.Sprintf("%s", effect.Key.IdentityKey))
			}
			newObjectVersions[effect.Key.IdentityKey] = changeOV[effect.Key.IdentityKey]
		}

		if effect.OpType == event.DELETE {
			if _, ok := newObjectVersions[effect.Key.IdentityKey]; !ok {
				// TODO this should return a 404. The effect should not have been materialized
				fmt.Println("warning: deleted key absent in state - ", effect.Key.IdentityKey)
				fmt.Println("frameID: ", frameID)
				fmt.Println("true state:")
				for k := range state.Objects() {
					fmt.Println(k)
				}
				fmt.Println("observed state")
				for k := range observableState {
					fmt.Println(k)
				}
				// panic("deleted key absent in state")
			}
			delete(newObjectVersions, effect.Key.IdentityKey)
		}

		// increment resourceversion for the kind
		newSequences[effect.Key.IdentityKey.Kind] += 1
	}

	newStateEvents := slices.Clone(state.Contents.stateEvents)
	for _, effect := range effects {
		kind := effect.Key.IdentityKey.Kind
		stateEvent := StateEvent{
			ReconcileID: reconcileResult.FrameID,
			Sequence:    newSequences[kind],
			effect:      effect,
			// TODO handle time info
			Timestamp: "",
		}
		newStateEvents = append(newStateEvents, stateEvent)
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(reconcileResult.Changes)
	newPendingReconciles = getNewPendingReconciles(newPendingReconciles, triggeredReconcilers)
	logger.V(2).WithValues("reconcilerID", pr.ReconcilerID, "pending", newPendingReconciles).Info("--Finished Reconcile--")
	// fmt.Printf("len change effects: %d, new pending reconciles: %s\n", len(effects), newPendingReconciles)

	// make a copy of the current execution history
	currHistory := slices.Clone(state.ExecutionHistory)

	return StateNode{
		Contents:          NewStateSnapshot(newObjectVersions, newSequences, newStateEvents),
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

func (e *Explorer) getTriggeredReconcilers(changes Changes) []PendingReconcile {
	// triggered := make(util.Set[string])
	// for objKey := range changes {
	// 	// get the controllers that depend on this object
	// 	triggeredForKind := e.dependencies[objKey.Kind]
	// 	triggered = triggered.Union(triggeredForKind)
	// }
	// // convert the set to a list that is sorted
	// triggeredList := triggered.List()
	// // sort the list so that we can explore the triggered controllers in a deterministic order
	// sort.Strings(triggeredList)
	return e.triggerManager.MustGetTriggered(changes)
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

func (e *Explorer) getPossibleViewsForReconcile(currState StateNode, reconcilerID string) ([]StateNode, error) {
	// TODO update to use some staleness depth configuration
	if e.stalenessDepth == 0 {
		return []StateNode{currState}, nil
	}

	currSnapshot := currState.Contents
	all, err := getAllViewsForController(&currSnapshot, reconcilerID, e.dependencies)
	// fmt.Println("produced", len(all), "stale views for", pending.ReconcilerID)
	if err != nil {
		return nil, err
	}

	asStateNodes := lo.Map(all, func(snapshot *StateSnapshot, _ int) StateNode {
		return StateNode{
			Contents:          *snapshot,
			PendingReconciles: slices.Clone(currState.PendingReconciles),
			parent:            currState.parent,
			action:            currState.action,
			ExecutionHistory:  slices.Clone(currState.ExecutionHistory),
		}
	})

	return asStateNodes, nil
}
