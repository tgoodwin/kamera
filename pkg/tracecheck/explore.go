package tracecheck

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/sleeve/pkg/event"
	"github.com/tgoodwin/sleeve/pkg/replay"
	"github.com/tgoodwin/sleeve/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var DefaultMaxDepth = 10

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

type ReconcilerConfig struct {
	Bounds      LookbackLimits
	MaxRestarts int
}

type ExploreMode string

const (
	DepthFirst   ExploreMode = "stack"
	BreadthFirst ExploreMode = "queue"
)

type ExploreConfig struct {
	MaxDepth     int
	useStaleness int

	mode ExploreMode

	debug bool

	// per-kind staleness config for each reconciler
	KindBoundsPerReconciler map[string]ReconcilerConfig
}

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]ReconcilerContainer
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies ResourceDeps

	knowledgeManager *EventKnowledge

	triggerManager *TriggerManager

	effectContextManager EffectContextManager

	config *ExploreConfig

	stats *ExploreStats
}

type ConvergedState struct {
	ID    string
	State StateNode
	Paths []ExecutionHistory
}

type Result struct {
	ConvergedStates []ConvergedState
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
		logger.Info("\nReplaying ReconcileID", frame.ID, "for reconciler", reconcilerID)

		rebuiltState = e.knowledgeManager.GetStateAtReconcileID(reconcile.ReconcileID)
		logger.Info("rebuilt state ahead of reconcile", "ReconcileID", util.Shorter(reconcile.ReconcileID), "objects", len(rebuiltState.contents))
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

		logger.Info("Reconcile result - # changes:", len(changes.ObjectVersions))
		changes.ObjectVersions.Summarize()

		resultingState := e.knowledgeManager.GetStateAfterReconcileID(reconcile.ReconcileID)
		logger.Info("resulting state after reconcile", "ReconcileID", util.Shorter(reconcile.ReconcileID), "objects", len(resultingState.contents))
		resultingState.contents.Summarize()

		currExecutionHistory = append(currExecutionHistory, res)

		// breaking off to explore to find alternative outcomes of the reconcile we just replayed.
		if e.shouldExploreDownstream(reconcile.ReconcileID) {
			logger.Info("exploring downstream from reconcileID", reconcile.ReconcileID)

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
			logger.Info("exploring state space at reconcileID", reconcile.ReconcileID)
			subRes := e.Explore(context.Background(), sn)
			result.ConvergedStates = append(result.ConvergedStates, subRes.ConvergedStates...)
			logger.Info("explored state space", "# converged states", len(subRes.ConvergedStates))
		}
	}

	currExecutionHistory.Summarize()

	// the end of the trace trivially converges
	traceWalkResult := ConvergedState{
		State: StateNode{Contents: *rebuiltState, DivergencePoint: "TRACE_START"},
		Paths: []ExecutionHistory{currExecutionHistory},
	}
	result.ConvergedStates = append(result.ConvergedStates, traceWalkResult)
	logger.Info("len curr execution history", len(currExecutionHistory))

	return result
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
func (e *Explorer) getNext(stackQueue []StateNode) (StateNode, []StateNode) {
	if e.config.mode == DepthFirst {
		return stackQueue[len(stackQueue)-1], stackQueue[:len(stackQueue)-1]
	} else if e.config.mode == BreadthFirst {
		return stackQueue[0], stackQueue[1:]
	}
	panic("Invalid mode")
}

func (e *Explorer) addStateToExplore(stackQueue []StateNode, state StateNode) []StateNode {
	mode := e.config.mode
	if mode == DepthFirst {
		// Add to the end for a stack (matching getNext's pop from end)
		return append(stackQueue, state)
	} else if mode == BreadthFirst {
		// Add to the end for a queue (matching getNext's pop from front)
		return append(stackQueue, state)
	}
	return stackQueue
}

func (e *Explorer) Explore(ctx context.Context, initialState StateNode) *Result {
	logger.Info("starting!")

	e.config.mode = DepthFirst

	res, err := e.explore(ctx, initialState)
	if err != nil {
		panic(err)
	}
	return res
}

func (e *Explorer) explore(ctx context.Context, initialState StateNode) (*Result, error) {
	if e.config.MaxDepth == 0 {
		e.config.MaxDepth = DefaultMaxDepth
	}

	if e.config.debug {
		fmt.Println("initial state")
		initialState.Contents.contents.DumpContents()
		fmt.Println("kind sequences")
		for k, v := range initialState.Contents.KindSequences {
			fmt.Println(k, v)
		}
	}

	result := &Result{
		ConvergedStates: make([]ConvergedState, 0),
	}

	stats := NewExploreStats()
	stats.Start()
	e.stats = stats

	seenDepths := make(map[int]bool)

	var queue []StateNode

	// executionPathsToState is a map of stateKey -> ExecutionHistory
	// because we want to track which states we've visited but
	// also want to track all the ways a given state can be reached
	executionPathsToState := make(map[string][]ExecutionHistory)

	// we dont skip over seen states because we want to track all the ways a state can be reached
	// but we do track the states we've seen
	seenStates := make(map[string]bool)

	seenStatesPendingOrderSensitive := make(map[string]bool)

	// we do track the seen converged states so we can attribute multiple execution paths to them
	seenConvergedStates := make(map[string]StateNode)

	// var currentState StateNode
	var currentState StateNode

	queue = append(queue, initialState)

	for len(queue) > 0 {
		currentState, queue = e.getNext(queue)
		stateKey := currentState.Hash()
		orderKey := currentState.OrderSensitiveHash()
		lineageKey := currentState.LineageHash()

		logger.V(1).Info("visiting node", "depth", currentState.depth, "Lineage", currentState.DetailedLineage())

		// we reconcile on the first pending reconcile for this state,
		// but if there are multiple pending reconciles, we want to explore what would happen
		// if each pending reconcile had gone first. So, we enqueue copies of this state
		// with each pending reconcile as the first one before proceeding. We do this FIRST
		// before taking the reconcile step so that we can explore a branch entirely in a DFS manner.
		// if we wanted to explore in a BFS manner, we would do this after taking the reconcile step.
		if len(currentState.PendingReconciles) > 1 {
			if !seenStates[orderKey] || true {
				expandedStates := expandStateByReconcileOrder(currentState)
				branchHashes := lo.Map(expandedStates, func(sn StateNode, _ int) string {
					return sn.LineageHash()
				})
				logger.V(2).Info("branching for pending reconcile ordering", "branchCount", len(expandedStates), "Branches", branchHashes)
				for _, candidate := range expandedStates {
					// orderHash := candidate.OrderSensitiveHash()
					lineageHash := candidate.LineageHash()
					if _, seenOrder := seenStatesPendingOrderSensitive[lineageHash]; !seenOrder {
						if lineageHash != lineageKey {
							logger.V(2).Info("adding new branch to explore", "TakenKey", lineageKey, "EnqueuedKey", lineageHash)
							queue = e.addStateToExplore(queue, candidate)
						}
						seenStatesPendingOrderSensitive[lineageHash] = true
					} else {
						logger.V(2).Info("already seen branch, not queueing", "OrderKey", lineageHash)
					}
				}
			} else {
				logger.WithValues("StateKey", stateKey, "OrderKey", lineageKey).V(2).Info("already seen, not expanding")
			}
		}

		seenStates[orderKey] = true

		if _, seen := executionPathsToState[stateKey]; !seen {
			executionPathsToState[stateKey] = []ExecutionHistory{currentState.ExecutionHistory}
		} else {
			executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
		}

		if len(currentState.PendingReconciles) == 0 {
			seenConvergedStates[stateKey] = currentState
			// break
			queue = []StateNode{}
			continue
		}

		// process the first one
		pendingReconcile := currentState.PendingReconciles[0]

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state.
		possibleViews, err := e.getPossibleViewsForReconcile(currentState, pendingReconcile.ReconcilerID, currentState.depth)
		if err != nil {
			return nil, errors.Wrap(err, "getting possible views")
		}
		reconcilerID := pendingReconcile.ReconcilerID
		for _, stateView := range possibleViews {
			if e.config.debug {
				logger.WithValues("Reconciler", reconcilerID, "StateKey", stateView.Hash(), "OrderKey", stateView.OrderSensitiveHash(), "Request", pendingReconcile.Request).Info("BEFORE")
				logger.WithValues("Queue", dumpQueue(queue)).Info("Queue")
				stateView.Contents.DumpContents()
				stateView.DumpPending()
			}

			stepLogger := logger.WithValues("Depth", stateView.depth, "ReconcilerID", reconcilerID)
			stepCtx := log.IntoContext(ctx, stepLogger)
			// for each view, create a new branch in exploration
			newState, err := e.takeReconcileStep(stepCtx, stateView, pendingReconcile)
			if err != nil {
				// if we encounter an error during reconciliation, just abandon this branch
				continue

			}
			logger.V(1).WithValues("NewPendingReconciles", newState.PendingReconciles).Info("reconcile step completed")
			if e.config.debug {
				logger.WithValues("Reconciler", reconcilerID, "StateKey", newState.Hash(), "Request", pendingReconcile.Request).Info("AFTER")
				logger.WithValues("Queue", dumpQueue(queue)).Info("Queue")
				newState.Contents.DumpContents()
				newState.DumpPending()
			}

			newState.depth = currentState.depth + 1
			if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
				seenDepths[newState.depth] = true
			}
			if newState.depth > e.config.MaxDepth {
				stats.AbortedPaths++
			} else {
				// enqueue the new state to explore
				queue = e.addStateToExplore(queue, newState)
			}
		}
	}

	// Graph search has ended, summarize the results
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

	fmt.Println("seen states", len(seenStatesPendingOrderSensitive))
	stats.Print()
	return result, nil
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, pr PendingReconcile) (StateNode, error) {
	stepLog := log.FromContext(ctx)
	// remove the current controller from the pending reconciles list
	newPendingReconciles := lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
		return pending != pr
	})

	// defensive validation
	if len(state.Contents.KindSequences) == 0 {
		panic("reconcile step: state has no kind sequences")
	}

	// create a new frameID for this reconcile state transition
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)

	// prepare the "true state of the world" for the controller
	e.effectContextManager.PrepareEffectContext(ctx, state.Contents.All())
	defer e.effectContextManager.CleanupEffectContext(ctx)

	// invoke the controller at its observed state of the world
	observableState := state.Contents.Observable()
	stepLog.Info("about to reconcile")
	reconcileResult, err := e.reconcileAtState(ctx, observableState, pr)
	if err != nil {
		stepLog.WithValues("ReconcilerID", pr.ReconcilerID).Error(err, "error reconciling")
		// return the pre-reconcile state if the controller errored
		return state, err
	}
	stepLog.Info("finished reconcile")

	newSequences := make(KindSequences)
	maps.Copy(newSequences, state.Contents.KindSequences)

	effects := reconcileResult.Changes.Effects
	stepLog.V(1).Info("completed step", "frameID", frameID, "controller", pr.ReconcilerID, "numEffects", len(effects))

	// update the state with the new object versions.
	// note that we are updating the "global state" here,
	// which may be separate from what the controller saw upon reconciling.
	prevState := make(ObjectVersions)
	maps.Copy(prevState, state.Objects())

	changeOV := reconcileResult.Changes.ObjectVersions
	newStateEvents := slices.Clone(state.Contents.stateEvents)
	for _, effect := range effects {
		switch effect.OpType {
		case event.CREATE:
			if _, ok := prevState.HasNamespacedNameForKind(effect.Key.ResourceKey); ok {
				// the effect validation mechanism should prevent a create effect from going through
				// if an object with the same kind/namespace/name already exists, so panic if it does happen
				panic("create effect object already exists in prev state: " + effect.Key.String())
			} else {
				// key not in state as expected, add it
				prevState[effect.Key] = changeOV[effect.Key]
			}
		case event.UPDATE, event.PATCH:
			if _, ok := prevState.HasNamespacedNameForKind(effect.Key.ResourceKey); !ok {
				// it is possible that a stale read will cause a controller to update an object
				// that no longer exists in the global state. The effect validation mechanism
				// should cause the client operation to 404 and prevent the update effect from
				// going through. If it does go through, we should panic cause something broke.
				panic("update effect object not found in prev state: " + effect.Key.String())
			}
			prevState[effect.Key] = changeOV[effect.Key]

		// need to determine how to update state based on preconditions
		case event.MARK_FOR_DELETION:
			if _, ok := prevState[effect.Key]; !ok {
				if _, nsNameExists := prevState.HasNamespacedNameForKind(effect.Key.ResourceKey); !nsNameExists {
					// We should never get here (effect validation should fail if there is no object matching the namespace/name in the state)
					// but if we do, we should panic because something is wrong.
					logger.Info("warning: deleted key absent in state", "effectKey", effect.Key, "frameID", frameID)
					panic("deleted key is not present in prev state. effect validation should have prevented this")
				}
			}
			stepLog.WithValues("Key", effect.Key).V(2).Info("marked object for deletion")
			// the delete effect is valid, so we should add it to the state
			prevState[effect.Key] = changeOV[effect.Key]
			// TODO when properly implementing preconditions, test with this:
			// "go run examples/zookeeper/cmd/main.go --search-depth 2"
			// and then uncomment the code below

			// logger.Info("warning: deleted key absent in state - ", effect.Key)
			// logger.Info("frameID: ", frameID)
			// logger.Info("true state:")
			// for k := range state.Objects() {
			// 	logger.Info(k)
			// }
			// logger.Info("observed state")
			// for k := range observableState {
			// 	logger.Info(k)
			// }
			// panic("deletion effect")
			// delete(prevState, effect.Key)

		case event.REMOVE:
			if _, ok := prevState.HasNamespacedNameForKind(effect.Key.ResourceKey); !ok {
				stepLog.Error(nil, "warning: removed key absent in state", "effectKey", effect.Key, "frameID", frameID)
				panic("removed key is not present in prev state. effect validation should have prevented this")
			}
			stepLog.V(2).Info("removing object from state", "key", effect.Key)
			delete(prevState, effect.Key)
		default:
			// at this part of the code we are only working with write effects
			err := fmt.Errorf("unknown effect type: %s", effect.OpType)
			logger.Error(err, "effect", effect)
			return StateNode{}, err
		}

		// Find the highest Sequence value globally for newStateEvents
		// TODO just store this in the state snapshot
		var highestSequence int64 = 0
		for _, event := range newStateEvents {
			if event.Sequence > highestSequence {
				highestSequence = event.Sequence
			}
		}

		newRV := highestSequence + 1

		// increment resourceversion for the kind
		newSequences[effect.Key.IdentityKey.Kind] = newRV
		stateEvent := StateEvent{
			ReconcileID: reconcileResult.FrameID,
			Sequence:    newRV,
			Effect:      effect,
			// TODO handle time info
			Timestamp: "",
		}
		newStateEvents = append(newStateEvents, stateEvent)
	}

	// get the controllers that depend on the objects that were changed
	// and add them to the pending reconciles list. n.b. this may potentially
	// include the controller that was just executed.
	triggeredReconcilers := e.getTriggeredReconcilers(reconcileResult.Changes)
	newPendingReconciles = e.getNewPendingReconciles(newPendingReconciles, triggeredReconcilers)

	// make a copy of the current execution history
	currHistory := slices.Clone(state.ExecutionHistory)

	sn := StateNode{
		Contents:          newStateSnapshot(prevState, newSequences, newStateEvents),
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            reconcileResult,

		ExecutionHistory: append(currHistory, reconcileResult),
	}
	sn.ID = sn.Hash()
	return sn, nil
}

// TODO figure out if we need to append to the front if using DFS
func (e *Explorer) getNewPendingReconciles(currPending, triggered []PendingReconcile) []PendingReconcile {
	// lo.Union does not change the order of elements relatively, but it does remove duplicates
	switch e.config.mode {
	case DepthFirst:
		// In DFS, we want to explore newly triggered reconciles first (depth-first)
		// So we put triggered at the beginning of the list
		// Remove duplicates while preserving order
		return lo.Union(triggered, currPending)
	case BreadthFirst:
		// In BFS, we want to explore existing pending reconciles before newly triggered ones
		// So we keep the original order - first finish currPending, then do triggered
		return lo.Union(currPending, triggered)
	default:
		panic("invalid mode")
	}
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
	return e.triggerManager.MustGetTriggered(changes)
}

func (e *Explorer) getPossibleViewsForReconcile(currState StateNode, reconcilerID string, currDepth int) ([]StateNode, error) {
	if e.config.useStaleness == 0 || true {
		return []StateNode{currState}, nil
	}

	// TODO explore restarting at every depth
	reconcilerConfig, ok := e.config.KindBoundsPerReconciler[reconcilerID]
	if !ok {
		return []StateNode{currState}, nil
	}
	maxRestarts := reconcilerConfig.MaxRestarts
	currRestarts := e.stats.RestartsPerReconciler[reconcilerID]

	if currRestarts >= maxRestarts {
		return []StateNode{currState}, nil
	}

	currSnapshot := currState.Contents
	config, ok := e.config.KindBoundsPerReconciler[reconcilerID]
	if !ok {
		// no staleness bounds configured for this reconciler, so dont compute stale states
		return []StateNode{currState}, nil
	}
	all, err := getAllViewsForController(&currSnapshot, reconcilerID, e.dependencies, config.Bounds)
	if err != nil {
		return nil, err
	}
	logger.V(1).Info("produced stale views for controller", "ReconcilerID", reconcilerID, "NumViews", len(all))
	e.stats.RestartsPerReconciler[reconcilerID]++

	asStateNodes := lo.Map(all, func(snapshot *StateSnapshot, _ int) StateNode {
		sn := StateNode{
			Contents:          *snapshot,
			PendingReconciles: slices.Clone(currState.PendingReconciles),
			parent:            currState.parent,
			action:            currState.action,
			ExecutionHistory:  slices.Clone(currState.ExecutionHistory),
		}
		sn.ID = sn.Hash()
		return sn
	})

	return asStateNodes, nil
}

func dumpQueue(queue []StateNode) []string {
	queueStr := lo.Map(queue, func(sn StateNode, _ int) string {
		return sn.OrderSensitiveHash()
	})
	return queueStr
}
