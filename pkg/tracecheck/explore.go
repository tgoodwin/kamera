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
	"time"

	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	DefaultMaxDepth = 10
)

// EffectContextManager manages a "current state of the world" context
// for each branch of execution (not shared between branches). This is the state
// that reconcile effects are validated against before being applied.
type EffectContextManager interface {
	PrepareEffectContext(ctx context.Context, ov ObjectVersions) error
	CleanupEffectContext(ctx context.Context)
}

type PerturbationConfig struct {
	StaleReadBounds LookbackLimits
	MaxRestarts     int
}

type ExploreConfig struct {
	maxDepth        int
	recordPerfStats bool
	// per-reconciler perturbation config
	perturbationCfg map[ReconcilerID]PerturbationConfig

	// divergenceCircuitBreakerThreshold limits exploration below certain subtrees
	// if enough paths below that subtree converge to the same state.
	divergenceCircuitBreakerThreshold int
}

//go:mockgen:generate -destination=./mocks/mock_trigger.go -package=tracecheck -source=./trigger.go TriggerHandler
type TriggerHandler interface {
	GetTriggered(changes Changes) ([]PendingReconcile, error)
	KindDepsForReconciler(reconcilerID ReconcilerID) ([]string, error)
}

type Explorer struct {
	// reconciler implementations keyed by ID
	reconcilers map[ReconcilerID]*ReconcilerContainer
	// maps Kinds to a list of reconcilerIDs that depend on them
	dependencies ResourceDeps

	knowledgeManager *EventKnowledge

	triggerManager TriggerHandler

	effectContextManager EffectContextManager

	versionManager VersionManager

	priorityHandler PriorityHandler // prioritize possible views to explore

	config *ExploreConfig

	stats *ExploreStats
}

type ResultState struct {
	ID              string
	State           StateNode
	Paths           []ExecutionHistory
	Reason          string
	Error           string
	FailedReconcile *PendingReconcile
	Resolver        VersionManager
}

type reconcileResultCache struct {
	outputObjectsHash   string             // hash of output objects (from newState.ObjectsHash())
	wasNoOp             bool               // did it produce changes?
	numEffects          int                // number of effects (for history signature)
	triggeredReconciles []PendingReconcile // reconciles triggered by the changes
}

// Explore takes an initial state and explores the state space to find all execution paths
// that end in a converged state.
// getNext pops the next state from the queue using DFS (depth-first) ordering.
func (e *Explorer) getNext(queue []StateNode) (StateNode, []StateNode) {
	return queue[len(queue)-1], queue[:len(queue)-1]
}

// enqueueState adds a state to the exploration queue.
func (e *Explorer) enqueueState(queue []StateNode, state StateNode) []StateNode {
	return append(queue, state)
}

func (e *Explorer) Explore(ctx context.Context, initialState StateNode) *Result {
	logger.Info("starting!")

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
		logger.Info("received interrupt signal")
		cancel()
	}()

	summarize := func(res *Result) {
		logger.V(1).Info("explore summary")
		if e.config != nil && e.config.recordPerfStats {
			e.stats.Print()
		}
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
	rawPaths := 0
	dedupedPaths := 0
	for i, stateKey := range lo.Keys(seenConvergedStates) {
		state := seenConvergedStates[stateKey]
		rawPaths = rawPaths + len(executionPathsToState[stateKey])
		paths := normalizeAndDedupePaths(executionPathsToState[stateKey])
		dedupedPaths = dedupedPaths + len(paths)
		state.DivergencePoint = initialState.DivergencePoint
		convergedState := ResultState{
			ID:       fmt.Sprintf("state-%d", i),
			State:    state,
			Paths:    paths,
			Reason:   "converged",
			Resolver: e.versionManager,
		}
		result.ConvergedStates = append(result.ConvergedStates, convergedState)
	}
	for i := range result.AbortedStates {
		stateKey := result.AbortedStates[i].State.Hash()
		if paths, ok := executionPathsToState[stateKey]; ok && len(paths) > 0 {
			mergedPaths := append(result.AbortedStates[i].Paths, paths...)
			result.AbortedStates[i].Paths = GetUniquePaths(mergedPaths)
		}
	}
	fmt.Printf("paths pre dedupe: %d\n", rawPaths)
	fmt.Printf("paths post dedupe: %d\n", dedupedPaths)
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

	if e.config.maxDepth == 0 {
		e.config.maxDepth = DefaultMaxDepth
	}

	if logger.V(2).Enabled() {
		logger.V(2).Info("initial state")
		initialState.Contents.contents.DumpContents()
		logger.V(2).Info("kind sequences")
		for k, v := range initialState.Contents.KindSequences {
			logger.V(2).Info("kind sequence", "kind", k, "value", v)
		}
	}

	seenDepths := make(map[int]bool)

	// Track explored state hashes keyed by the sequence of state-changing reconciles.
	// This lets us prune branches that only differ by no-op reads.
	visitedStatePaths := make(map[StateHash]map[string]struct{})

	// Track which (objectsHash, reconcilerID, request) combinations produced no changes.
	// Used to skip orderings that put known no-ops first.
	knownNoOps := make(map[string]bool)

	reconcileCache := make(map[string]*reconcileResultCache)

	// Track which (objectsHash, pendingSignature, historyKey) combinations we've committed to explore.
	// This allows prediction-based deduplication using only predictable components.
	exploredLogicalStates := make(map[string]struct{})

	var queue []StateNode

	// executionPathsToState is a map of stateKey -> ExecutionHistory
	// because we want to track which states we've visited but
	// also want to track all the ways a given state can be reached
	executionPathsToState := make(map[StateHash][]ExecutionHistory)

	// we dont skip over seen states because we want to track all the ways a state can be reached
	// but we do track the states we've seen
	seenStates := make(map[OrderHash]bool)

	// Track which logical states (order-insensitive) have already been expanded
	// for reconcile-order permutations. Branching once per StateHash is enough,
	// because expandStateByReconcileOrder enqueues every first-position choice
	// and recursion covers the full permutation space.
	seenBranchingByState := make(map[StateHash]bool)

	// we do track the seen converged states so we can attribute multiple execution paths to them
	seenConvergedStates := make(map[StateHash]StateNode)

	convergencesByDivergenceKey := make(map[StateHash][]StateHash)

	// var currentState StateNode
	var currentState StateNode

	queue = append(queue, initialState)

	initialHash := initialState.Hash()
	initialSignature := initialState.ExecutionHistory.UniqueKey()
	visitedStatePaths[initialHash] = map[string]struct{}{initialSignature: {}}

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		currentState, queue = e.getNext(queue)
		stateKey := currentState.Hash()
		orderKey := currentState.OrderSensitiveHash()
		alreadySeen := seenStates[orderKey]
		if logger.V(1).Enabled() {
			logger.V(1).Info("visiting node", "depth", currentState.depth, "Lineage", currentState.DetailedLineage())
		}

		// We reconcile the first pending reconcile for this state, but if there are
		// multiple pending reconciles, we need to explore every ordering. Expanding
		// once per logical state (StateHash) is sufficient: the expansion enqueues
		// every first-position choice, and recursive exploration will enumerate the
		// full permutation space. Re-reaching the same StateHash with a different
		// ordering does not add new permutations, so we skip re-branching.
		if len(currentState.PendingReconciles) > 1 {
			if !seenBranchingByState[stateKey] {
				expandedStates := expandStateByReconcileOrder(currentState)
				if logger.V(2).Enabled() {
					branchHashes := lo.Map(expandedStates, func(sn StateNode, _ int) string {
						return sn.LineageHash()
					})
					logger.V(2).Info("branching for pending reconcile ordering", "branchCount", len(expandedStates), "Branches", branchHashes)
				}

				// Optimization: skip orderings that put a known no-op reconciler first.
				objectsHash := currentState.ObjectsHash()
				for _, candidate := range expandedStates {
					// Skip the current ordering; it is already being explored.
					if candidate.OrderSensitiveHash() == orderKey {
						continue
					}

					// Check if the first reconciler in this ordering is a known no-op.
					firstPending := candidate.PendingReconciles[0]
					noOpKey := fmt.Sprintf("%s:%s:%s", objectsHash, firstPending.ReconcilerID, firstPending.Request.NamespacedName.String())
					if isNoOp, known := knownNoOps[noOpKey]; known && isNoOp {
						e.stats.SkippedNoOpOrderings++
						continue
					}

					logger.V(2).Info("adding new branch to explore", "StateKey", stateKey, "EnqueuedOrder", candidate.OrderSensitiveHash())
					queue = e.enqueueState(queue, candidate)
				}
				// Mark this logical state as expanded to avoid re-branching.
				seenBranchingByState[stateKey] = true
			} else {
				e.stats.SkippedOrderExpansions++
				logger.WithValues("StateKey", stateKey).V(2).Info("already expanded pending orderings for this state, not branching")
			}
		}

		if alreadySeen {
			e.stats.SkippedNodeVisits++
		} else {
			e.stats.UniqueNodeVisits++
			seenStates[orderKey] = true
		}
		e.stats.TotalNodeVisits++

		// Record depth distribution stats
		e.stats.RecordVisit(currentState.depth, len(currentState.PendingReconciles), len(queue))

		if _, seen := executionPathsToState[stateKey]; !seen {
			executionPathsToState[stateKey] = make([]ExecutionHistory, 0)
		}
		executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
		if cancelled := sendWithCancel(ctx, executionPathsCh, currentState); cancelled {
			return nil
		}

		// A state is considered converged if:
		// 1. There are no pending reconciles, OR
		// 2. All remaining pending reconciles are ignorable for convergence (async enqueues
		//    from tickers, or requeues from poll-based controllers). These don't indicate
		//    state changes, just time-based or polling behavior.
		if len(currentState.PendingReconciles) == 0 || allPendingIgnorableForConvergence(currentState.PendingReconciles) {
			reason := "no pending reconciles"
			if len(currentState.PendingReconciles) > 0 {
				reason = "only async enqueues/requeues remaining"
			}
			if logger.V(1).Enabled() {
				logger.V(1).WithValues(
					"Depth", currentState.depth,
					"StateKey", currentState.Hash(),
					"Reason", reason,
					"RemainingIgnorable", countIgnorableForConvergence(currentState.PendingReconciles),
				).Info("arrived at converged state")
			}
			if logger.V(2).Enabled() {
				logger.V(2).Info("lineage", "ReconcileLineage", currentState.ReconcileLineage())
			}
			seenConvergedStates[stateKey] = currentState

			// track how many times we've arrived at this state from some common ancestor
			if currentState.divergenceKey != "" {
				if _, seen := convergencesByDivergenceKey[currentState.divergenceKey]; !seen {
					convergencesByDivergenceKey[currentState.divergenceKey] = make([]StateHash, 0)
				}
				convergencesByDivergenceKey[currentState.divergenceKey] = append(convergencesByDivergenceKey[currentState.divergenceKey], stateKey)
			}

			if cancelled := sendWithCancel(ctx, convergedStatesCh, currentState); cancelled {
				return nil
			}
			continue
		}

		// Divergence Circuit-Breaker: limit exploration when paths from a divergence point
		// keep converging to the same state.
		if threshold := e.config.divergenceCircuitBreakerThreshold; threshold > 0 && currentState.divergenceKey != "" {
			convergencesUnderKey := convergencesByDivergenceKey[currentState.divergenceKey]
			repeatedCount := util.MostCommonElementCount(convergencesUnderKey)
			if repeatedCount > threshold {
				logger.V(1).Info("skipping state; subtree circuit breaker triggered",
					"StateKey", stateKey,
					"DivergenceKey", currentState.divergenceKey,
					"Threshold", threshold,
					"RepeatedConvergences", repeatedCount)
				continue
			}
		}

		// process the first one
		pendingReconcile := currentState.PendingReconciles[0]

		// Log all pending reconciles for diagnostic purposes
		if logger.V(1).Enabled() {
			pendingIDs := make([]string, len(currentState.PendingReconciles))
			for i, pr := range currentState.PendingReconciles {
				pendingIDs[i] = fmt.Sprintf("%s(%s)", pr.ReconcilerID, pr.Source)
			}
			logger.V(1).WithValues(
				"Depth", currentState.depth,
				"QueueDepth", len(queue),
				"PendingCount", len(currentState.PendingReconciles),
				"Pending", pendingIDs,
				"Processing", pendingReconcile.ReconcilerID,
			).Info("processing reconcile step")
		}

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

		if len(possibleViews) == 0 {
			logger.WithValues(
				"StateKey", stateKey,
				"ReconcilerID", pendingReconcile.ReconcilerID,
				"PendingCount", len(currentState.PendingReconciles),
			).Info("no eligible views for pending reconcile; marking state as aborted")

			e.stats.AbortedPaths++
			executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
			abortReason := fmt.Sprintf("no eligible views for %s", pendingReconcile.ReconcilerID)

			reconcileCopy := pendingReconcile
			select {
			case abortedStatesCh <- ResultState{
				ID:              fmt.Sprintf("aborted-%s", stateKey),
				State:           currentState,
				Paths:           []ExecutionHistory{currentState.ExecutionHistory},
				Reason:          abortReason,
				FailedReconcile: &reconcileCopy,
				Resolver:        e.versionManager,
			}:
			case <-ctx.Done():
				return nil
			}

			// Skip exploring this branch further since there are no viable views.
			continue
		}

		reconcilerID := pendingReconcile.ReconcilerID
		for _, stateView := range possibleViews {
			if logger.V(2).Enabled() {
				logger.V(2).WithValues("Reconciler", reconcilerID, "StateKey", stateView.Hash(), "OrderKey", stateView.OrderSensitiveHash(), "Request", pendingReconcile.Request).Info("BEFORE")
				logger.V(2).WithValues("Queue", dumpQueue(queue)).Info("Queue")
				stateView.Contents.DumpContents()
				stateView.DumpPending()
			}

			stepLogger := logger.WithValues("Depth", stateView.depth, "ReconcilerID", reconcilerID)
			stepCtx := log.IntoContext(ctx, stepLogger)

			// Cache key uses OBJECTS hash only - pending list doesn't affect reconciler behavior
			cacheKey := fmt.Sprintf("%s:%s:%s", stateView.ObjectsHash(), reconcilerID, pendingReconcile.Request.NamespacedName.String())

			// Check cache: can we predict the output state without running the reconcile?
			if e.skipViaCachePrediction(reconcileCache, exploredLogicalStates, cacheKey, stateView, pendingReconcile) {
				stepLogger.V(1).Info("skipping reconcile via cache prediction; would produce duplicate state")
				e.stats.CachePredictedSkips++
				continue
			}

			stepLogger.Info("Taking reconcile step")

			// for each view, create a new branch in exploration
			newState, stepResult, err := e.takeReconcileStep(stepCtx, stateView, pendingReconcile)

			// Track whether this was a no-op (used by ordering optimization)
			wasNoOp := err == nil && stepResult != nil && len(stepResult.Changes.ObjectVersions) == 0 && stepResult.Error == ""
			knownNoOps[cacheKey] = wasNoOp // cacheKey already uses objectsHash
			if wasNoOp {
				e.stats.NoOpReconciles++
			}

			// Update cache with this reconcile's result
			if err == nil && stepResult != nil {
				reconcileCache[cacheKey] = &reconcileResultCache{
					outputObjectsHash:   newState.ObjectsHash(),
					wasNoOp:             wasNoOp,
					numEffects:          len(stepResult.Changes.Effects),
					triggeredReconciles: e.getTriggeredReconcilers(stepResult.Changes),
				}
			}

			if err != nil {
				// if we encounter an error during reconciliation, just abandon this branch
				stepLogger.Error(err, "error taking reconcile step; abandoning branch")
				e.stats.AbortedPaths++
				failurePath := stateView.ExecutionHistory
				if stepResult != nil {
					failurePath = append(slices.Clone(stateView.ExecutionHistory), stepResult)
				}
				stateKey := stateView.Hash()
				executionPathsToState[stateKey] = append(executionPathsToState[stateKey], failurePath)
				reconcileCopy := pendingReconcile
				select {
				case abortedStatesCh <- ResultState{
					ID:              fmt.Sprintf("aborted-%s", stateKey),
					State:           stateView,
					Paths:           []ExecutionHistory{failurePath},
					Reason:          "error",
					Error:           err.Error(),
					FailedReconcile: &reconcileCopy,
					Resolver:        e.versionManager,
				}:
				case <-ctx.Done():
					return nil
				}
				continue
			}
			logger.V(1).WithValues("Depth", currentState.depth, "NewPendingReconciles", newState.PendingReconciles).Info("reconcile step completed")
			if logger.V(2).Enabled() {
				logger.V(2).WithValues("Reconciler", reconcilerID, "StateKey", newState.Hash(), "Request", pendingReconcile.Request).Info("AFTER")
				logger.V(2).WithValues("Queue", dumpQueue(queue)).Info("Queue")
				newState.Contents.DumpContents()
				newState.DumpPending()
			}

			newState.depth = currentState.depth + 1
			if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
				seenDepths[newState.depth] = true
			}

			if newState.depth > e.config.maxDepth {
				if logger.V(1).Enabled() {
					logger.WithValues(
						"maxDepth", e.config.maxDepth,
						"currentDepth", newState.depth,
						"Lineage", newState.ReconcileLineage(),
					).Info("aborting path due to max depth")
				}
				e.stats.AbortedPaths++
				stateKey := newState.Hash()
				executionPathsToState[stateKey] = append(executionPathsToState[stateKey], newState.ExecutionHistory)
				select {
				case abortedStatesCh <- ResultState{
					ID:       fmt.Sprintf("aborted-%s", stateKey),
					State:    newState,
					Paths:    []ExecutionHistory{newState.ExecutionHistory},
					Reason:   fmt.Sprintf("max depth %d", e.config.maxDepth),
					Resolver: e.versionManager,
				}:
				case <-ctx.Done():
				}
				continue
			}

			// Deduplication: Skip exploring paths that reach the same state via equivalent mutations.
			///Skipped
			// Key invariant: Same pending list = Same future possibilities = Safe to skip.
			//
			// stateHash includes both object state AND pending reconciles. Two paths only
			// match when they have identical pending lists. If the pending lists are identical,
			// then the future exploration from both paths would be identical - same controllers
			// to run, same state to observe - so exploring both would be redundant.
			//
			// Importantly, by the time we reach this check, we've already queued all ordering
			// variants for the pending list (via expandStateByReconcileOrder at lines 388-410).
			// Skipping here doesn't mean "we don't care about orderings" - it means "we've
			// already scheduled those orderings to be explored, no need to schedule them again."
			//
			// At intermediate states, different orderings naturally yield different pending
			// lists because whichever reconcile just ran gets removed:
			//
			//   Path A: ...→ Foo@1 → State X, Pending=[Bar]  (Foo removed)
			//   Path B: ...→ Bar@1 → State X, Pending=[Foo]  (Bar removed)
			//
			// Different pending lists → different stateHashes → both fully explored.
			//
			// Pruning typically only occurs at convergence (Pending=[]) where all paths
			// collapse to empty pending lists. The paths that get pruned differ only in
			// no-op orderings, which by definition cannot produce different outcomes.
			stateHash := newState.Hash()
			historySet, alreadyTracked := visitedStatePaths[stateHash]
			if !alreadyTracked {
				historySet = make(map[string]struct{})
				visitedStatePaths[stateHash] = historySet
			}
			normalizedHistory := newState.ExecutionHistory.UniqueKey()
			if _, seenPath := historySet[normalizedHistory]; seenPath {
				logger.V(1).WithValues(
					"StateHash", stateHash,
					"PathSignature", normalizedHistory,
				).Info("skipping duplicate state reached via equivalent mutation history")
				e.stats.SkippedPaths++
				continue
			} else {
				// enqueue the new state to explore
				historySet[normalizedHistory] = struct{}{}

				// Also track in exploredLogicalStates for cache prediction
				pendingStrs := lo.Map(newState.PendingReconciles, func(pr PendingReconcile, _ int) string {
					return pr.String()
				})
				slices.Sort(pendingStrs)
				pendingSignature := strings.Join(pendingStrs, ",")
				logicalStateKey := fmt.Sprintf("%s|%s|%s", newState.ObjectsHash(), pendingSignature, normalizedHistory)
				exploredLogicalStates[logicalStateKey] = struct{}{}

				queue = e.enqueueState(queue, newState)
			}
		}
	}

	return nil
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, pr PendingReconcile) (StateNode, *ReconcileResult, error) {
	stepLog := log.FromContext(ctx)
	startWall := time.Now()
	defer func() {
		if e.stats != nil && e.config != nil && e.config.recordPerfStats {
			e.stats.RecordStep(pr.ReconcilerID, time.Since(startWall))
		}
	}()

	// defensive validation
	if len(state.Contents.KindSequences) == 0 {
		panic("reconcile step: state has no kind sequences")
	}

	// create a new frameID for this reconcile state transition
	frameID := util.UUID()
	ctx = replay.WithFrameID(ctx, frameID)

	// increment simulated time by setting the simulated clock depth to match the depth of this state
	// Tickers that fire during SetDepth will add enqueues to the global collector.
	restoreClock := simclock.SetDepth(state.depth)
	defer restoreClock()

	// prepare the "true state of the world" for the controller's potential actions
	// to be validated against. (e.g. "create error: thing of name X already exists")
	e.effectContextManager.PrepareEffectContext(ctx, state.Contents.All())
	defer e.effectContextManager.CleanupEffectContext(ctx)

	// invoke the controller at its observed state of the world
	observableState := state.ObserveAs(pr.ReconcilerID)
	stepLog.WithValues("ReconcilerID", pr.ReconcilerID, "FrameID", frameID).V(2).Info("about to reconcile")

	reconcileResult, err := e.reconcileAtState(ctx, observableState, pr)
	if err != nil && apierrors.IsAlreadyExists(err) {
		// AlreadyExists errors happen when a stale read causes a controller to try creating
		// an object that already exists. Treat this as a no-op and continue exploring.
		stepLog.WithValues("ReconcilerID", pr.ReconcilerID, "Request", pr.Request).Info("tolerating AlreadyExists error; treating reconcile as no-op")
		reconcileResult = &ReconcileResult{
			ControllerID: pr.ReconcilerID,
			FrameID:      frameID,
			FrameType:    FrameTypeExplore,
			Changes:      Changes{ObjectVersions: make(ObjectVersions)},
			Error:        err.Error(),
		}
		err = nil
	}
	if err != nil {
		// Other errors cause the branch to be abandoned. Return a minimal result for history tracking.
		stepLog.WithValues("ReconcilerID", pr.ReconcilerID).Error(err, "error reconciling")
		failure := &ReconcileResult{
			ControllerID: pr.ReconcilerID,
			FrameID:      frameID,
			FrameType:    FrameTypeExplore,
			Error:        err.Error(),
		}
		return state, failure, err
	}
	stepLog.V(1).WithValues(
		"Result", reconcileResult.ctrlRes,
	).Info("finished reconcile")

	beforeState := make(ObjectVersions)
	maps.Copy(beforeState, state.Objects())
	beforeSequences := make(KindSequences)
	maps.Copy(beforeSequences, state.Contents.KindSequences)

	reconcileResult.StateBefore = beforeState
	reconcileResult.KindSeqBefore = beforeSequences

	newSequences := make(KindSequences)
	maps.Copy(newSequences, state.Contents.KindSequences)
	for key, seq := range newSequences {
		if !strings.Contains(key, "/") {
			stepLog.Error(nil, "kind sequence key lacks group info", "key", key, "sequence", seq)
		}
	}
	effects := reconcileResult.Changes.Effects
	stepLog.V(1).Info("completed step", "frameID", frameID, "controller", pr.ReconcilerID, "numEffects", len(effects))

	// update the state with the new object versions.
	// note that we are updating the "global state" here,
	// which may be separate from what the controller saw upon reconciling.
	prevState := make(ObjectVersions)
	maps.Copy(prevState, state.Objects())

	changeOV := reconcileResult.Changes.ObjectVersions
	newStateEvents := slices.Clone(state.Contents.stateEvents)

	// determine highest sequence once and increment as effects are applied
	var highestSequence int64
	if len(newStateEvents) > 0 {
		for _, event := range newStateEvents {
			if event.Sequence > highestSequence {
				highestSequence = event.Sequence
			}
		}
	}

	for _, effect := range effects {
		existingKey, exists := prevState.HasNamespacedNameForKind(effect.Key.ResourceKey)

		switch effect.OpType {
		case event.CREATE:
			if exists {
				// the effect validation mechanism should prevent a create effect from going through
				// with an 'AlreadyExists' error, so panic if it does happen
				panic("create effect object already exists in prev state: " + effect.Key.String())
			} else {
				// Mimic APIServer behavior: set Generation to 1 on CREATE if not already set
				newObj := e.versionManager.Resolve(changeOV[effect.Key])
				if newObj != nil {
					gen := newObj.GetGeneration()
					if gen == 0 {
						newObj.SetGeneration(1)
						// Update the version hash after modifying Generation
						changeOV[effect.Key] = e.versionManager.Publish(newObj)
					}
				}
				prevState[effect.Key] = changeOV[effect.Key]
			}
		case event.UPDATE, event.PATCH:
			if !exists {
				// it is possible that a stale read will cause a controller to update an object
				// that no longer exists in the global state. The effect validation mechanism
				// should cause the client operation to 404 and prevent the update effect from
				// going through. If it does go through, we should panic cause something broke.
				panic("update effect object not found in prev state: " + effect.Key.String())
			}
			if exists && existingKey != effect.Key {
				delete(prevState, existingKey)
			}
			// Mimic APIServer behavior: increment Generation on spec updates (not status-only updates)
			oldObj := e.versionManager.Resolve(prevState[existingKey])
			newObj := e.versionManager.Resolve(changeOV[effect.Key])
			if oldObj != nil && newObj != nil {
				// Compare specs to determine if Generation should be incremented
				// In Kubernetes, Generation is only incremented when spec changes, not on status-only updates
				// Use a safe check to avoid panics if spec comparison fails
				specChanged, err := snapshot.CheckSpecChanged(oldObj, newObj)
				if err != nil {
					// If we can't determine if spec changed, conservatively increment Generation
					// This is safer than not incrementing it
					stepLog.V(2).WithValues("key", effect.Key, "error", err).Info("error checking spec change, incrementing Generation conservatively")
					specChanged = true
				}
				if specChanged {
					oldGen := oldObj.GetGeneration()
					if oldGen == 0 {
						// If Generation is 0, set it to 1 (shouldn't happen in real K8s, but handle gracefully)
						// This can happen if the state snapshot has objects with Generation=0
						newObj.SetGeneration(1)
						stepLog.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", 1).Info("set Generation to 1 on spec update (was 0)")
					} else {
						newObj.SetGeneration(oldGen + 1)
						stepLog.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", newObj.GetGeneration()).Info("incremented Generation on spec update")
					}
					// Update the version hash after modifying Generation
					changeOV[effect.Key] = e.versionManager.Publish(newObj)
				}
			}
			prevState[effect.Key] = changeOV[effect.Key]

		// need to determine how to update state based on preconditions
		case event.MARK_FOR_DELETION:
			if !exists {
				// We should never get here (effect validation should fail if there is no object matching the namespace/name in the state)
				// but if we do, we should panic because something is wrong.
				logger.Info("warning: deleted key absent in state", "effectKey", effect.Key, "frameID", frameID)
				panic("deleted key is not present in prev state. effect validation should have prevented this")
			}
			stepLog.WithValues("Key", effect.Key).V(2).Info("marked object for deletion")
			if existingKey != effect.Key {
				delete(prevState, existingKey)
			}
			// the delete effect is valid, so we should add it to the state
			prevState[effect.Key] = changeOV[effect.Key]

		case event.REMOVE:
			if !exists {
				stepLog.Error(nil, "warning: removed key absent in state", "effectKey", effect.Key, "frameID", frameID)
				panic("removed key is not present in prev state. effect validation should have prevented this")
			}
			stepLog.V(2).Info("removing object from state", "key", effect.Key)
			delete(prevState, existingKey)
		default:
			// at this part of the code we are only working with write effects
			err := fmt.Errorf("unknown effect type: %s", effect.OpType)
			logger.Error(err, "effect", effect)
			return StateNode{}, nil, err
		}

		highestSequence++
		newRV := highestSequence

		// increment resourceversion for the kind
		newSequences[effect.Key.IdentityKey.CanonicalGroupKind()] = newRV
		stateEvent := StateEvent{
			ReconcileID: reconcileResult.FrameID,
			Sequence:    newRV,
			Effect:      effect,
			// TODO handle time info
			Timestamp: "",
		}
		newStateEvents = append(newStateEvents, stateEvent)
	}

	newPendingReconciles := e.determineNewPendingReconciles(ctx, state, pr, reconcileResult)
	stepLog.V(1).WithValues(
		"Depth", state.depth,
		"Count", len(newPendingReconciles),
		"Items", newPendingReconciles,
	).Info("final pending reconciles after step")

	// make a copy of the current execution history
	currHistory := slices.Clone(state.ExecutionHistory)

	afterState := make(ObjectVersions)
	maps.Copy(afterState, prevState)
	afterSequences := make(KindSequences)
	maps.Copy(afterSequences, newSequences)

	reconcileResult.StateAfter = afterState
	reconcileResult.KindSeqAfter = afterSequences
	reconcileResult.PendingReconciles = newPendingReconciles

	child := StateNode{
		Contents:          NewStateSnapshot(prevState, newSequences, newStateEvents),
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
	return child, child.action, nil
}

func (e *Explorer) getNewPendingReconciles(currPending, triggered []PendingReconcile) []PendingReconcile {
	// In DFS, explore newly triggered reconciles first, then existing pending.
	// When duplicates exist for the same (ReconcilerID + NamespacedName), if any have Source == StateChange,
	// that one takes precedence over Requeue or AsyncEnqueue. Otherwise, first occurrence wins.
	all := append(triggered, currPending...)

	type dedupKey struct {
		ReconcilerID   ReconcilerID
		NamespacedName string
	}

	resultMap := make(map[dedupKey]PendingReconcile, len(all))
	for _, pr := range all {
		key := dedupKey{ReconcilerID: pr.ReconcilerID, NamespacedName: pr.Request.NamespacedName.String()}
		existing, ok := resultMap[key]
		if !ok {
			resultMap[key] = pr
			continue
		}
		// If new is StateChange and existing is not, replace
		if pr.Source == SourceStateChange && existing.Source != SourceStateChange {
			resultMap[key] = pr
		}
		// Otherwise, keep the existing one (first occurrence or StateChange takes precedence)
	}

	// preserve original order from "all", but use the winner from resultMap
	final := make([]PendingReconcile, 0, len(resultMap))
	seen := make(map[dedupKey]struct{}, len(resultMap))
	for _, pr := range all {
		key := dedupKey{ReconcilerID: pr.ReconcilerID, NamespacedName: pr.Request.NamespacedName.String()}
		if _, ok := seen[key]; ok {
			continue
		}
		if winner, exists := resultMap[key]; exists {
			final = append(final, winner) // Add the winner (may differ from pr due to Source precedence)
			seen[key] = struct{}{}
		}
	}
	return final
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

func (e *Explorer) getPossibleViewsForReconcile(currState StateNode, reconcilerID ReconcilerID, currDepth int) ([]StateNode, error) {
	currSnapshot := currState.Contents
	config, ok := e.config.perturbationCfg[reconcilerID]
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

	logger.V(2).Info("getting possible views for reconciler", "ReconcilerID", reconcilerID, "CurrDepth", currDepth, "MaxDepth", e.config.maxDepth)
	possiblePastViews, err := getAllViewsForController(&currSnapshot, reconcilerID, e.dependencies, config.StaleReadBounds)
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
		var stuckPositions map[ReconcilerID]KindSequences
		if currState.stuckReconcilerPositions == nil {
			stuckPositions = make(map[ReconcilerID]KindSequences)
		} else {
			stuckPositions = maps.Clone(currState.stuckReconcilerPositions)
		}

		// after a restart / reconnect, the controller will be stuck at this position
		// in the stale state, but only for the resource types it has staleness configuration
		// for.
		// this feature is for finding / reproducing bugs where the APIServer is run in HA mode and one of the nodes is partitioned.
		// then, a controller connected to the leader crashes, restarts, and reconnects to the partitioned node,
		// effectively "going back in time" to the frozen past state of the partitioned node.
		stuckPositionsForReconciler := make(KindSequences)
		for k, v := range staleState.KindSequences {
			if _, exists := config.StaleReadBounds[k]; exists {
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

func (e *Explorer) determineNewPendingReconciles(ctx context.Context, state StateNode, reconcileInput PendingReconcile, result *ReconcileResult) []PendingReconcile {
	//  remove the current reconcile from the pending reconciles list because it has just been processed
	stillPending := lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
		return pending != reconcileInput
	})

	// Read captured enqueues from the global collector (from Watch callbacks during reconcile).
	// Get() automatically clears the collector after returning, so it's ready for the next step.
	// These are already PendingReconcile entries with the correct reconciler ID.
	stepLog := log.FromContext(ctx)
	capturedPending := GetGlobalAsyncEnqueueCollector().Get()
	if len(capturedPending) > 0 {
		stepLog.V(1).Info("captured async enqueues from tickers",
			"count", len(capturedPending),
			"depth", state.depth,
			"reconciler", reconcileInput.ReconcilerID,
			"enqueues", capturedPending)
	}

	// after processing the reconcile, we need to determine which controllers
	// were triggered by the changes in the state.
	triggeredByChanges := e.getTriggeredReconcilers(result.Changes)

	// Log which reconcilers were triggered for debugging, but only if verbosity at least 1 is enabled.
	if logger.V(1).Enabled() && len(triggeredByChanges) > 0 {
		triggeredIDs := lo.Map(triggeredByChanges, func(pr PendingReconcile, _ int) string {
			return pr.String()
		})
		logger.WithValues(
			"ReconcilerID", reconcileInput.ReconcilerID,
			"TriggeredReconcilers", triggeredIDs,
			"NumChanges", len(result.Changes.ObjectVersions),
		).V(1).Info("reconcilers triggered by changes")
	}

	// for those that would have been triggered but have been configured as "stuck",
	// filter them out of the triggered list if the changes are contained within the
	// kinds their watch streams are "stuck" on.
	if state.stuckReconcilerPositions != nil {
		filtered := lo.Filter(triggeredByChanges, func(pending PendingReconcile, _ int) bool {
			stuckKinds, stuck := state.stuckReconcilerPositions[pending.ReconcilerID]
			if !stuck {
				return true // not stuck on anything, pass through
			}
			resourceDeps, _ := e.triggerManager.KindDepsForReconciler(pending.ReconcilerID)
			for changeKey := range result.Changes.ObjectVersions {
				canonicalKind := util.CanonicalGroupKind(changeKey.ResourceKey.Group, changeKey.ResourceKey.Kind)
				// If not stuck on this kind AND subscribes to it, could see the change
				if _, stuckOnKind := stuckKinds[canonicalKind]; !stuckOnKind {
					if slices.Contains(resourceDeps, canonicalKind) {
						return true
					}
				}
			}
			return false
		})
		triggeredByChanges = filtered
	}

	// if the controller returned a response with Requeue = true,
	// we need to requeue the original request, no matter what.
	if result.ctrlRes.Requeue {
		requeued := PendingReconcile{
			ReconcilerID: reconcileInput.ReconcilerID,
			Request:      reconcileInput.Request,
			Source:       SourceRequeue,
		}
		triggeredByChanges = append(triggeredByChanges, requeued)
	}

	allTriggered := append(triggeredByChanges, capturedPending...)
	return e.getNewPendingReconciles(stillPending, allTriggered)
}

func (e *Explorer) skipViaCachePrediction(
	reconcileCache map[string]*reconcileResultCache,
	exploredLogicalStates map[string]struct{},
	cacheKey string,
	stateView StateNode,
	pendingReconcile PendingReconcile,
) bool {
	// Check cache: can we predict the output state without running the reconcile?
	if cached, ok := reconcileCache[cacheKey]; ok {
		// We've run this (objects, reconciler, request) before.
		// We can predict what the output will be.

		//                     reconcileCache                exploredLogicalStates
		//                     (input → output)              (output → seen?)
		//                           │                              │
		// State A ──────────────────┼──────► Output X ────────────┼──────► Already queued? YES → skip
		// (objects=O, pending=[1,2])│        (objs=O', pend=[2])  │
		//                           │                              │
		// State B ──────────────────┼──────► Output X ────────────┼──────► Already queued? YES → skip
		// (objects=O, pending=[1,3])│        (objs=O', pend=[3])  │
		//                           ▲                              │
		//                      SAME cache key!              DIFFERENT outputs
		//                      (same objects, same R1)      (different pending)

		// Predict output pending: current pending - this reconcile + triggered
		predictedPending := lo.Filter(stateView.PendingReconciles, func(pr PendingReconcile, _ int) bool {
			return pr != pendingReconcile
		})
		predictedPending = e.getNewPendingReconciles(predictedPending, cached.triggeredReconciles)

		// Build pending signature (sorted for determinism)
		pendingStrs := lo.Map(predictedPending, func(pr PendingReconcile, _ int) string {
			return pr.String()
		})
		slices.Sort(pendingStrs)
		pendingSignature := strings.Join(pendingStrs, ",")

		// Predict the history signature after this step
		currentHistory := stateView.ExecutionHistory.UniqueKey()
		var predictedHistory string
		if cached.wasNoOp {
			predictedHistory = currentHistory
		} else {
			stepSig := fmt.Sprintf("%s@%d", pendingReconcile.ReconcilerID, cached.numEffects)
			if currentHistory == "" {
				predictedHistory = stepSig
			} else {
				predictedHistory = fmt.Sprintf("%s,%s", currentHistory, stepSig)
			}
		}

		// Check if we've already committed to exploring this logical state
		logicalStateKey := fmt.Sprintf("%s|%s|%s", cached.outputObjectsHash, pendingSignature, predictedHistory)
		if _, explored := exploredLogicalStates[logicalStateKey]; explored {
			return true
		}
	}
	return false
}

func sendWithCancel[T any](ctx context.Context, ch chan<- T, val T) bool {
	select {
	case <-ctx.Done():
		return true
	case ch <- val:
		return false
	}
}
