package tracecheck

import (
	"context"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	DefaultMaxDepth = 10
)

type reconciler interface {
	doReconcile(ctx context.Context, readset ObjectVersions, req reconcile.Request) (*ReconcileResult, error)
	replayReconcile(ctx context.Context, req reconcile.Request) (*ReconcileResult, error)
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

	breakEarly bool

	mode ExploreMode

	debug bool

	// per-kind staleness config for each reconciler
	KindBoundsPerReconciler map[string]ReconcilerConfig
}

//go:mockgen:generate -destination=./mocks/mock_trigger.go -package=tracecheck -source=./trigger.go TriggerHandler
type TriggerHandler interface {
	GetTriggered(changes Changes) ([]PendingReconcile, error)
	KindDepsForReconciler(reconcilerID string) ([]string, error)
}

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[string]*ReconcilerContainer
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies ResourceDeps

	knowledgeManager *EventKnowledge

	triggerManager TriggerHandler

	effectContextManager EffectContextManager

	priorityHandler PriorityHandler // prioritize possible views to explore

	config *ExploreConfig

	stats *ExploreStats
}

type ResultState struct {
	ID     string
	State  StateNode
	Paths  []ExecutionHistory
	Reason string
}

func (e *Explorer) shouldExploreDownstream(frameID string) bool {
	// TODO make this some actual heursitic. Right now, it's hardcoded against some
	// test data I was prototyping with.
	return strings.HasPrefix(frameID, "821c")
}

func (e *Explorer) Walk(reconciles []replay.ReconcileEvent) *Result {
	currExecutionHistory := make(ExecutionHistory, 0)

	result := &Result{
		ConvergedStates: make([]ResultState, 0),
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
	traceWalkResult := ResultState{
		State:  StateNode{Contents: *rebuiltState, DivergencePoint: "TRACE_START"},
		Paths:  []ExecutionHistory{currExecutionHistory},
		Reason: "trace",
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

	exploreCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	convergedStateChan := make(chan StateNode, 100)
	executionHistoryChan := make(chan StateNode, 100)
	abortedStateChan := make(chan ResultState, 100)
	errChan := make(chan error, 1)

	seenConvergedStates := make(map[StateHash]StateNode)
	executionPathsToState := make(map[StateHash][]ExecutionHistory)

	e.stats = NewExploreStats()
	e.stats.Start()

	go func() {
		err := e.explore(exploreCtx, initialState, convergedStateChan, executionHistoryChan, abortedStateChan)
		if err != nil {
			errChan <- err
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	// Ctrl-C cancels the search
	go func() {
		<-sigs
		fmt.Println("received interrupt signal")
		cancel()
	}()

	summarize := func(res *Result) {
		fmt.Println("### Summary ###")
		e.stats.Print()
		res.Summarize()
	}

	abortedCollected := make([]ResultState, 0)

	for convergedStateChan != nil || executionHistoryChan != nil || abortedStateChan != nil {
		select {
		case convergedState, ok := <-convergedStateChan:
			if !ok {
				convergedStateChan = nil
				continue
			}
			stateKey := convergedState.Hash()
			if _, seen := seenConvergedStates[stateKey]; !seen {
				seenConvergedStates[stateKey] = convergedState
			}
		case state, ok := <-executionHistoryChan:
			if !ok {
				executionHistoryChan = nil
				continue
			}
			stateKey := state.Hash()
			if _, seen := executionPathsToState[stateKey]; !seen {
				executionPathsToState[stateKey] = make([]ExecutionHistory, 0)
			}
			executionPathsToState[stateKey] = append(executionPathsToState[stateKey], state.ExecutionHistory)
		case aborted, ok := <-abortedStateChan:
			if !ok {
				abortedStateChan = nil
				continue
			}
			abortedCollected = append(abortedCollected, aborted)
		}
	}

	// if we broke out early, collect partial results, summarize them, and return
	result := &Result{ConvergedStates: make([]ResultState, 0), AbortedStates: abortedCollected}
	for i, stateKey := range lo.Keys(seenConvergedStates) {
		state := seenConvergedStates[stateKey]
		paths := executionPathsToState[stateKey]
		state.DivergencePoint = initialState.DivergencePoint
		convergedState := ResultState{
			ID:     fmt.Sprintf("state-%d", i),
			State:  state,
			Paths:  paths,
			Reason: "converged",
		}
		result.ConvergedStates = append(result.ConvergedStates, convergedState)
	}
	for i := range result.AbortedStates {
		stateKey := result.AbortedStates[i].State.Hash()
		if paths, ok := executionPathsToState[stateKey]; ok && len(paths) > 0 {
			result.AbortedStates[i].Paths = paths
		}
	}
	summarize(result)
	return result
}

// explore performs a state space exploration starting from the initialState.
// the state space is modeled as a graph where nodes are states and edges are reconcile steps.
// each branch represents a possible execution path through the state space, and leaf nodes are converged states.
func (e *Explorer) explore(
	ctx context.Context,
	initialState StateNode,
	convergedStatesCh chan<- StateNode,
	executionPathsCh chan<- StateNode,
	abortedStatesCh chan<- ResultState,
) error {
	defer func() {
		close(convergedStatesCh)
		close(executionPathsCh)
		close(abortedStatesCh)
	}()

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

	seenDepths := make(map[int]bool)

	var queue []StateNode

	// executionPathsToState is a map of stateKey -> ExecutionHistory
	// because we want to track which states we've visited but
	// also want to track all the ways a given state can be reached
	executionPathsToState := make(map[StateHash][]ExecutionHistory)

	// we dont skip over seen states because we want to track all the ways a state can be reached
	// but we do track the states we've seen
	seenStates := make(map[OrderHash]bool)

	seenStatesPendingOrderSensitive := make(map[string]bool)

	// we do track the seen converged states so we can attribute multiple execution paths to them
	seenConvergedStates := make(map[StateHash]StateNode)

	convergencesByDivergenceKey := make(map[StateHash][]StateHash)

	// var currentState StateNode
	var currentState StateNode

	queue = append(queue, initialState)

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
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
		e.stats.TotalNodeVisits++

		if _, seen := executionPathsToState[stateKey]; !seen {
			executionPathsToState[stateKey] = make([]ExecutionHistory, 0)
			e.stats.UniqueNodeVisits++
		}
		executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
		if cancelled := sendWithCancel(ctx, executionPathsCh, currentState); cancelled {
			return nil
		}

		if len(currentState.PendingReconciles) == 0 {
			logger.WithValues(
				"Depth", currentState.depth,
				"StateKey", currentState.Hash(),
			).Info("arrived at converged state")
			logger.V(2).Info("lineage", "ReconcileLineage", currentState.ReconcileLineage())
			seenConvergedStates[stateKey] = currentState

			// track how many times we've arrived at this state from some common ancestor
			if currentState.divergenceKey != "" {
				if _, seen := convergencesByDivergenceKey[currentState.divergenceKey]; !seen {
					convergencesByDivergenceKey[currentState.divergenceKey] = make([]StateHash, 0)
				}
				convergencesByDivergenceKey[currentState.divergenceKey] = append(convergencesByDivergenceKey[currentState.divergenceKey], stateKey)
			}

			fmt.Println("sending converged state", currentState.Hash())
			if cancelled := sendWithCancel(ctx, convergedStatesCh, currentState); cancelled {
				return nil
			}
			continue
		}

		// Subtree Circuit-Breaker
		moveOnThreshold := 20
		if currentState.divergenceKey != "" {
			convergencesUnderKey := convergencesByDivergenceKey[currentState.divergenceKey]
			repeatedCount := util.MostCommonElementCount(convergencesUnderKey)
			if repeatedCount > moveOnThreshold {
				logger.Info("skipping state with too many convergences", "StateKey", stateKey, "ConvergencesUnderKey", len(convergencesUnderKey))
				continue
			}
		}

		// process the first one
		pendingReconcile := currentState.PendingReconciles[0]

		// Each controller in the pending reconciles list is a potential branch point
		// from the current state.
		possibleViews, err := e.getPossibleViewsForReconcile(currentState, pendingReconcile.ReconcilerID, currentState.depth)
		if err != nil {
			return errors.Wrap(err, "getting possible views")
		}

		prioritizedViews := lo.Filter(possibleViews, func(s StateNode, _ int) bool {
			return s.Contents.Priority != Skip
		})
		logger.V(2).WithValues(
			"PreFilteredCount", len(possibleViews),
			"FilteredCount", len(prioritizedViews),
		).Info("filtered possible views based on priority")
		possibleViews = prioritizedViews

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

			fmt.Println("taking reconcile step", "ReconcilerID", reconcilerID, "Depth", currentState.depth)
			logger.V(0).WithValues(
				"ReconcilerID", reconcilerID,
				"Depth", currentState.depth,
			).Info("Taking reconcile step")

			// for each view, create a new branch in exploration
			newState, err := e.takeReconcileStep(stepCtx, stateView, pendingReconcile)
			if err != nil {
				// if we encounter an error during reconciliation, just abandon this branch
				continue

			}
			logger.V(1).WithValues("Depth", currentState.depth, "NewPendingReconciles", newState.PendingReconciles).Info("reconcile step completed")
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
				logger.WithValues(
					"maxDepth", e.config.MaxDepth,
					"currentDepth", newState.depth,
					"Lineage", newState.ReconcileLineage(),
				).V(1).Info("aborting path due to max depth")
				e.stats.AbortedPaths++
				stateKey := newState.Hash()
				executionPathsToState[stateKey] = append(executionPathsToState[stateKey], newState.ExecutionHistory)
				select {
				case abortedStatesCh <- ResultState{
					ID:     fmt.Sprintf("aborted-%s", stateKey),
					State:  newState,
					Paths:  []ExecutionHistory{newState.ExecutionHistory},
					Reason: fmt.Sprintf("max depth %d", e.config.MaxDepth),
				}:
				case <-ctx.Done():
				}
			} else {
				// enqueue the new state to explore
				queue = e.addStateToExplore(queue, newState)
			}
		}
	}

	return nil
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, pr PendingReconcile) (StateNode, error) {
	stepLog := log.FromContext(ctx)

	// defensive validation
	if len(state.Contents.KindSequences) == 0 {
		panic("reconcile step: state has no kind sequences")
	}

	// create a new frameID for this reconcile state transition
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)

	// prepare the "true state of the world" for the controller's potential actions
	// to be validated against. (e.g. "create error: thing of name X already exists")
	e.effectContextManager.PrepareEffectContext(ctx, state.Contents.All())
	defer e.effectContextManager.CleanupEffectContext(ctx)

	// invoke the controller at its observed state of the world
	observableState := state.ObserveAs(pr.ReconcilerID)
	stepLog.WithValues("ReconcilerID", pr.ReconcilerID, "FrameID", frameID).V(2).Info("about to reconcile")

	reconcileResult, err := e.reconcileAtState(ctx, observableState, pr)
	if err != nil {
		stepLog.WithValues("ReconcilerID", pr.ReconcilerID).Error(err, "error reconciling")
		// return the pre-reconcile state if the controller errored
		return state, err
	}
	stepLog.V(1).WithValues(
		"Result", reconcileResult.ctrlRes,
	).Info("finished reconcile")

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

	newPendingReconciles := e.determineNewPendingReconciles(state, pr, reconcileResult)

	// make a copy of the current execution history
	currHistory := slices.Clone(state.ExecutionHistory)

	child := StateNode{
		Contents:          newStateSnapshot(prevState, newSequences, newStateEvents),
		PendingReconciles: newPendingReconciles,
		parent:            &state,
		action:            reconcileResult,

		// inherit the mode from the parent
		mode: state.mode,

		// inherit divergence point from the parent
		divergenceKey: state.divergenceKey,

		stuckReconcilerPositions: maps.Clone(state.stuckReconcilerPositions),

		ExecutionHistory: append(currHistory, reconcileResult),
	}
	child.ID = string(child.Hash())
	return child, nil
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
	container, ok := e.reconcilers[pr.ReconcilerID]
	if !ok {
		return nil, fmt.Errorf("implementation for reconciler %s not found", pr.ReconcilerID)
	}

	if pr.Request.NamespacedName.Name == "" || pr.Request.NamespacedName.Namespace == "" {
		return nil, fmt.Errorf("empty reconcile request: %v", pr.Request)
	}

	// execute the controller
	// convert the write set to object versions
	result, err := container.doReconcile(ctx, objState, pr.Request)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Explorer) getTriggeredReconcilers(changes Changes) []PendingReconcile {
	res, err := e.triggerManager.GetTriggered(changes)
	if err != nil {
		logger.Error(err, "getting triggered reconciles")
		panic("getting triggered reconciles: " + err.Error())
	}
	return res
}

func (e *Explorer) getPossibleViewsForReconcile(currState StateNode, reconcilerID string, currDepth int) ([]StateNode, error) {
	currSnapshot := currState.Contents
	config, ok := e.config.KindBoundsPerReconciler[reconcilerID]
	if !ok {
		logger.V(2).Info("no staleness bounds configured for reconciler", "ReconcilerID", reconcilerID)
		// no staleness bounds configured for this reconciler, so dont compute stale states
		return []StateNode{currState}, nil
	}
	maxRestarts := config.MaxRestarts
	currRestarts := e.stats.RestartsPerReconciler[reconcilerID]
	if currRestarts >= maxRestarts {
		logger.V(2).Info("max restarts reached for reconciler", "ReconcilerID", reconcilerID, "CurrRestarts", currRestarts, "MaxRestarts", maxRestarts)
		return []StateNode{currState}, nil
	}

	logger.V(2).Info("getting possible views for reconciler", "ReconcilerID", reconcilerID, "CurrDepth", currDepth, "MaxDepth", e.config.MaxDepth)
	possiblePastViews, err := getAllViewsForController(&currSnapshot, reconcilerID, e.dependencies, config.Bounds)
	if err != nil {
		return nil, errors.Wrap(err, "getting possible views")
	}

	possiblePastViews = e.priorityHandler.AssignPriorities(possiblePastViews)
	possiblePastViews = e.priorityHandler.PrioritizeViews(possiblePastViews)

	// When we generate possible stale views for a controller at a certain depth in the execution,
	// we're modeling a controller restarting and reconnecting to a network-partitioned APIServer.
	e.stats.RestartsPerReconciler[reconcilerID]++
	logger.V(1).Info("produced stale views for controller", "ReconcilerID", reconcilerID, "NumViews", len(possiblePastViews))

	divergenceHash := currState.Hash()
	asStateNodes := lo.Map(possiblePastViews, func(staleState *StateSnapshot, _ int) StateNode {
		var stuckPositions map[string]KindSequences
		if currState.stuckReconcilerPositions == nil {
			stuckPositions = make(map[string]KindSequences)
		} else {
			stuckPositions = maps.Clone(currState.stuckReconcilerPositions)
		}

		// after a restart / reconnect, the controller will be stuck at this position
		// in the stale state, but only for the resource types it has staleness configuration
		// for.
		stuckPositionsForReconciler := make(KindSequences)
		for k, v := range staleState.KindSequences {
			if _, exists := config.Bounds[k]; exists {
				stuckPositionsForReconciler[k] = v
			}
		}
		stuckPositions[reconcilerID] = stuckPositionsForReconciler

		sn := StateNode{
			Contents:          *staleState,
			depth:             currDepth,
			PendingReconciles: slices.Clone(currState.PendingReconciles),
			parent:            currState.parent,
			action:            currState.action,
			ExecutionHistory:  slices.Clone(currState.ExecutionHistory),

			// identify the produced node as a "hypothetical" state
			mode:          NodeModeHypothetical,
			divergenceKey: divergenceHash,

			stuckReconcilerPositions: stuckPositions,
		}
		sn.ID = string(sn.Hash())

		if logger.V(2).Enabled() {
			logger.WithValues("StateKey", sn.ID, "OrderKey").V(2).Info("produced stale view")
			sn.Contents.DumpContents()
		}
		return sn
	})

	return asStateNodes, nil
}

func dumpQueue(queue []StateNode) []string {
	queueStr := lo.Map(queue, func(sn StateNode, _ int) string {
		return string(sn.OrderSensitiveHash())
	})
	return queueStr
}

func (e *Explorer) determineNewPendingReconciles(state StateNode, reconcileInput PendingReconcile, result *ReconcileResult) []PendingReconcile {
	//  remove the current reconcile from the pending reconciles list because it has just been processed
	stillPending := lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
		return pending != reconcileInput
	})

	// after processing the reconcile, we need to determine which controllers
	// were triggered by the changes in the state.
	triggeredByChanges := e.getTriggeredReconcilers(result.Changes)

	// for those that would have been triggered but have been configured as "stuck",
	// filter them out of the triggered list if the changes are contained within the
	// kinds their watch streams are "stuck" on.
	if state.stuckReconcilerPositions != nil {
		filtered := lo.Filter(triggeredByChanges, func(pending PendingReconcile, _ int) bool {
			if stuckKinds, stuck := state.stuckReconcilerPositions[pending.ReconcilerID]; stuck {
				resourceDeps, _ := e.triggerManager.KindDepsForReconciler(pending.ReconcilerID)
				couldSeeChange := false
				for changeKey := range result.Changes.ObjectVersions {
					if _, ok := stuckKinds[changeKey.ResourceKey.Kind]; !ok {
						// if the reconciler subscribes to the change's kind and the kind
						// is not in the stuck set, it could see the change
						if subscribes := lo.Contains(resourceDeps, changeKey.ResourceKey.Kind); subscribes {
							couldSeeChange = true
						}
					}
				}
				return couldSeeChange
			} else {
				// if not stuck on anything, pass it through the filter!
				return true
			}
		})
		triggeredByChanges = filtered
	}

	// if the controller returned a response with Requeue = true,
	// we need to requeue the original request, no matter what.
	if result.ctrlRes.Requeue {
		triggeredByChanges = append(triggeredByChanges, reconcileInput)
	}

	return e.getNewPendingReconciles(stillPending, triggeredByChanges)
}

func sendWithCancel[T any](ctx context.Context, ch chan<- T, val T) bool {
	select {
	case <-ctx.Done():
		return true
	case ch <- val:
		return false
	}
}
