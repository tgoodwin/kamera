package tracecheck

import (
	"context"
	"fmt"
	"hash/fnv"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/tgoodwin/kamera/pkg/event"
	"github.com/tgoodwin/kamera/pkg/replay"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/snapshot"
	"github.com/tgoodwin/kamera/pkg/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	PermuteOrder map[ReconcilerID]bool
	Staleness    map[ReconcilerID]StalenessConfig
}

type StalenessConfig struct {
	StaleReadBounds LookbackLimits
	MaxRestarts     int
}

type SearchMode string

const (
	SearchModeDFS        SearchMode = "dfs"
	SearchModeMonteCarlo SearchMode = "monte_carlo"
)

func ParseSearchMode(raw string) (SearchMode, error) {
	mode := SearchMode(strings.TrimSpace(strings.ToLower(raw)))
	switch mode {
	case "", SearchModeDFS:
		return SearchModeDFS, nil
	case SearchModeMonteCarlo:
		return SearchModeMonteCarlo, nil
	default:
		return "", fmt.Errorf("invalid search mode %q", raw)
	}
}

type MonteCarloConfig struct {
	Seed int64
	// Trials controls total single-path runs for a scenario in Monte Carlo mode.
	// It is consumed by runner orchestration.
	Trials int
	// TrialIndex identifies the current run in a scenario trial group.
	TrialIndex int
	// ScenarioGroup is a stable scenario/input grouping key used for seed derivation.
	ScenarioGroup string
}

func (cfg MonteCarloConfig) DerivedSeed() int64 {
	return DeriveMonteCarloSeed(cfg.Seed, cfg.ScenarioGroup, cfg.TrialIndex)
}

func DeriveMonteCarloSeed(baseSeed int64, scenarioGroup string, trialIndex int) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strconv.FormatInt(baseSeed, 10)))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(scenarioGroup))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(strconv.Itoa(trialIndex)))

	// Keep the value non-negative to simplify formatting/interop in attributes.
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

type OptimizationConfig struct {
	EarlyConvergence   bool
	CompletedPathDedup bool
	OrderingPruning    bool
	// OnlyPermuteTriggered limits order permutations to reconcilers triggered by the last step.
	// When true, permutations are limited to triggered reconcilers.
	// When false, permutations can place any eligible pending reconciler first.
	OnlyPermuteTriggered bool
	// DisableNoOpOrderingSkip disables skipping orderings whose first reconcile is a known no-op.
	DisableNoOpOrderingSkip bool
	CachePrediction         bool
	SubtreeCompletion       bool
}

func (opt OptimizationConfig) AnyEnabled() bool {
	// OnlyPermuteTriggered scopes permutation behavior; it does not independently
	// turn on any optimization heuristic.
	return opt.EarlyConvergence || opt.CompletedPathDedup || opt.OrderingPruning || opt.CachePrediction || opt.SubtreeCompletion
}

type ExploreConfig struct {
	MaxDepth        int
	RecordPerfStats bool
	Timeout         time.Duration
	SearchMode      SearchMode
	MonteCarlo      MonteCarloConfig

	// Perturbations configures optional behavioral perturbations (opt-in), such as
	// reconcile ordering permutations and stale-read view injection.
	Perturbations PerturbationConfig

	// divergenceCircuitBreakerThreshold limits exploration below certain subtrees
	// if enough paths below that subtree converge to the same state.
	divergenceCircuitBreakerThreshold int

	// Optimizations configures runing heuristics.
	Optimizations OptimizationConfig
}

// Clone returns a deep copy of the ExploreConfig, including map fields.
func (cfg ExploreConfig) Clone() ExploreConfig {
	out := cfg
	out.Perturbations.PermuteOrder = maps.Clone(cfg.Perturbations.PermuteOrder)
	out.Perturbations.Staleness = make(map[ReconcilerID]StalenessConfig, len(cfg.Perturbations.Staleness))
	for id, st := range cfg.Perturbations.Staleness {
		copied := st
		copied.StaleReadBounds = maps.Clone(st.StaleReadBounds)
		out.Perturbations.Staleness[id] = copied
	}
	return out
}

func (cfg ExploreConfig) OptimizationsEnabled() bool {
	return cfg.Optimizations.AnyEnabled()
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

	userController *UserController

	Config *ExploreConfig

	stats         *ExploreStats
	optimizations *optimizations
}

// VersionManager returns the shared version manager used during exploration.
func (e *Explorer) VersionManager() VersionManager {
	return e.versionManager
}

// Stats returns the stats gathered during exploration.
func (e *Explorer) Stats() *ExploreStats {
	return e.stats
}

func (e *Explorer) shouldApplyNextUserAction(state StateNode) bool {
	if !e.hasRemainingUserAction(state) {
		return false
	}
	// policy here is to apply the next user action after the current state has converged
	return state.IsConverged()
}

func (e *Explorer) hasRemainingUserAction(state StateNode) bool {
	return e != nil && e.userController != nil && e.userController.HasActionAt(state.nextUserActionIdx)
}

func (e *Explorer) isTerminalConvergedState(state StateNode) bool {
	if e.hasRemainingUserAction(state) {
		return false
	}
	return state.IsConverged()
}

// Objects resolves and returns all objects for the provided ResultState, skipping any that cannot be resolved.
func (e *Explorer) Objects(rs ResultState) []*unstructured.Unstructured {
	if e == nil || e.versionManager == nil {
		return nil
	}
	objects := make([]*unstructured.Unstructured, 0, len(rs.State.Objects()))
	for _, version := range rs.State.Objects() {
		obj := e.versionManager.Resolve(version)
		if obj == nil {
			continue
		}
		objects = append(objects, obj.DeepCopy())
	}
	return objects
}

type ResultState struct {
	ID    string
	State StateNode
	Paths []ExecutionHistory
	Error error
}

type cachedReconcileResult struct {
	outputObjectsHash   ContentsHash       // hash of output objects (from newState.ObjectsHash())
	wasNoOp             bool               // did it produce changes?
	numEffects          int                // number of effects (for history signature)
	triggeredReconciles []PendingReconcile // reconciles triggered by the changes
}

// stackEntry represents an entry in the DFS exploration stack.
// It is either a state to process or a completion marker.
type stackEntry struct {
	state  *StateNode       // non-nil if this is a state to process
	marker *LogicalStateKey // non-nil if this is a completion marker

	// Stale view branching support:
	staleViewMarker    *StaleViewBranchKey // stale view completion marker
	staleViewReconcile *PendingReconcile   // if set, this is a stale view ready to reconcile
}

// isMarker returns true if this entry is a completion marker.
func (e stackEntry) isMarker() bool {
	return e.marker != nil
}

// isStaleViewMarker returns true if this entry is a stale view completion marker.
func (e stackEntry) isStaleViewMarker() bool {
	return e.staleViewMarker != nil
}

// isStaleViewEntry returns true if this entry is a stale view ready to reconcile.
func (e stackEntry) isStaleViewEntry() bool {
	return e.staleViewReconcile != nil
}

// StaleViewBranchKey identifies a stale view branching point.
// All stale views for the same (parent state, reconcile) share this key.
type StaleViewBranchKey struct {
	ParentLogicalKey LogicalStateKey
	ReconcilerID     ReconcilerID
	RequestKey       string // NamespacedName.String()
}

// subtreeTracker tracks completion status of logical state subtrees.
// Used to skip re-exploration of subtrees that have already been fully processed.
type subtreeTracker struct {
	// completed contains logical states whose subtrees have been fully explored.
	// When we encounter a completed state, we skip it entirely.
	completed map[LogicalStateKey]struct{}

	// inProgress contains logical states currently being explored (marker pushed but not yet popped).
	// This handles "diamond" convergence where two paths reach the same logical state
	// before either finishes. Without this, we'd push duplicate markers and do redundant work.
	inProgress map[LogicalStateKey]struct{}
}

func newSubtreeTracker() *subtreeTracker {
	return &subtreeTracker{
		completed:  make(map[LogicalStateKey]struct{}),
		inProgress: make(map[LogicalStateKey]struct{}),
	}
}

// isCompleted returns true if the subtree for this logical state has been fully explored.
func (t *subtreeTracker) isCompleted(key LogicalStateKey) bool {
	_, ok := t.completed[key]
	return ok
}

// isInProgress returns true if this logical state is currently being explored.
func (t *subtreeTracker) isInProgress(key LogicalStateKey) bool {
	_, ok := t.inProgress[key]
	return ok
}

// markInProgress marks a logical state as currently being explored.
func (t *subtreeTracker) markInProgress(key LogicalStateKey) {
	t.inProgress[key] = struct{}{}
}

// markCompleted marks a logical state's subtree as fully explored.
func (t *subtreeTracker) markCompleted(key LogicalStateKey) {
	delete(t.inProgress, key)
	t.completed[key] = struct{}{}
}

// staleViewTracker tracks completion status of stale view branches.
// Used to skip re-exploration of stale view branches that have already been fully processed.
type staleViewTracker struct {
	completed  map[StaleViewBranchKey]struct{}
	inProgress map[StaleViewBranchKey]struct{}
}

func newStaleViewTracker() *staleViewTracker {
	return &staleViewTracker{
		completed:  make(map[StaleViewBranchKey]struct{}),
		inProgress: make(map[StaleViewBranchKey]struct{}),
	}
}

func (t *staleViewTracker) isCompleted(key StaleViewBranchKey) bool {
	_, ok := t.completed[key]
	return ok
}

func (t *staleViewTracker) markInProgress(key StaleViewBranchKey) {
	t.inProgress[key] = struct{}{}
}

func (t *staleViewTracker) markCompleted(key StaleViewBranchKey) {
	delete(t.inProgress, key)
	t.completed[key] = struct{}{}
}

// enqueueWithMarker enqueues states with proper marker handling for subtree completion tracking.
// All provided states should be ordering variants of the same logical state.
// Returns the updated stack and true if states were enqueued (not skipped).
func (e *Explorer) enqueueWithMarker(
	stack []stackEntry,
	tracker *subtreeTracker,
	states []StateNode,
) ([]stackEntry, bool) {
	if len(states) == 0 {
		return stack, false
	}

	// All ordering variants share the same logical key
	logicalKey := states[0].LogicalKey()

	// Already fully explored?
	if tracker.isCompleted(logicalKey) {
		e.stats.SubtreeCompletionSkips++
		return stack, false
	}

	// Already being explored? (diamond convergence)
	if tracker.isInProgress(logicalKey) {
		// If the logical key appears in the ancestry of this state, this is a cycle
		// back-edge (not a true diamond). Keep exploring so the branch can still
		// terminate (e.g., max-depth abort) instead of being dropped silently.
		if hasLogicalKeyInAncestry(states, logicalKey) {
			for i := range states {
				stack = append(stack, stackEntry{state: &states[i]})
			}
			return stack, true
		}
		e.stats.SubtreeDiamondSkips++
		return stack, false
	}

	// First encounter: mark in progress and push marker
	tracker.markInProgress(logicalKey)
	stack = append(stack, stackEntry{marker: &logicalKey})

	// Push all ordering variants (they'll pop before the marker)
	for i := range states {
		stack = append(stack, stackEntry{state: &states[i]})
	}

	return stack, true
}

func hasLogicalKeyInAncestry(states []StateNode, key LogicalStateKey) bool {
	for i := range states {
		for parent := states[i].parent; parent != nil; parent = parent.parent {
			if parent.LogicalKey() == key {
				return true
			}
		}
	}
	return false
}

func (e *Explorer) subtreeCompletionEnabled() bool {
	if e.Config == nil {
		return true
	}
	return e.Config.Optimizations.SubtreeCompletion
}

func (e *Explorer) enqueueStates(
	stack []stackEntry,
	tracker *subtreeTracker,
	states []StateNode,
	useSubtreeCompletion bool,
) ([]stackEntry, bool) {
	if !useSubtreeCompletion {
		for i := range states {
			stack = append(stack, stackEntry{state: &states[i]})
		}
		return stack, len(states) > 0
	}
	if len(states) == 0 {
		return stack, false
	}

	groups := make(map[LogicalStateKey][]StateNode, len(states))
	order := make([]LogicalStateKey, 0, len(states))
	for i := range states {
		key := states[i].LogicalKey()
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], states[i])
	}

	enqueued := false
	for _, key := range order {
		var didEnqueue bool
		stack, didEnqueue = e.enqueueWithMarker(stack, tracker, groups[key])
		if didEnqueue {
			enqueued = true
		}
	}

	return stack, enqueued
}

// enqueueStaleViewStates enqueues stale-view variants onto the DFS stack.
// When subtree completion is enabled, stale view branches use their own
// completion markers to avoid re-exploring completed stale subtrees.
func (e *Explorer) enqueueStaleViewStates(
	stack []stackEntry,
	tracker *staleViewTracker,
	parent StateNode,
	pendingReconcile PendingReconcile,
	possibleViews []StateNode,
	useSubtreeCompletion bool,
) ([]stackEntry, bool) {
	if len(possibleViews) == 0 {
		return stack, false
	}

	if useSubtreeCompletion {
		staleKey := StaleViewBranchKey{
			ParentLogicalKey: parent.LogicalKey(),
			ReconcilerID:     pendingReconcile.ReconcilerID,
			RequestKey:       pendingReconcile.Request.NamespacedName.String(),
		}
		if tracker.isCompleted(staleKey) {
			e.stats.StaleViewCompletionSkips++
			logger.V(1).Info("skipping stale view branch - already completed",
				"depth", parent.depth, "staleViewKey", staleKey)
			return stack, false
		}

		tracker.markInProgress(staleKey)
		stack = append(stack, stackEntry{staleViewMarker: &staleKey})
	}

	for i := range possibleViews {
		stack = append(stack, stackEntry{
			state:              &possibleViews[i],
			staleViewReconcile: &pendingReconcile,
		})
	}

	return stack, true
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

	seenConvergedStates := make(map[NodeHash]StateNode)
	executionPathsToState := make(map[NodeHash][]ExecutionHistory)

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
		if e.Config != nil && e.Config.RecordPerfStats {
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
	for i, stateKey := range lo.Keys(seenConvergedStates) {
		state := seenConvergedStates[stateKey]
		paths := dedupePathsByUniqueKey(executionPathsToState[stateKey])
		state.DivergencePoint = initialState.DivergencePoint
		convergedState := ResultState{
			ID:    fmt.Sprintf("state-%d", i),
			State: state,
			Paths: paths,
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

	if logger.V(2).Enabled() {
		logger.V(2).Info("initial state")
		initialState.Contents.contents.DumpContents()
		logger.V(2).Info("kind sequences")
		for k, v := range initialState.Contents.KindSequences {
			logger.V(2).Info("kind sequence", "kind", k, "value", v)
		}
	}

	e.optimizations = newOptimizations(e.Config.Optimizations)

	seenDepths := make(map[int]bool)

	// Subtree completion tracker: tracks which logical states have been fully explored.
	// When we encounter a completed logical state, we skip it entirely.
	// This replaces the old exploredSubtrees map with proper completion tracking via stack markers.
	useSubtreeCompletion := e.subtreeCompletionEnabled()
	var subtreeTracker *subtreeTracker
	var staleViewTracker *staleViewTracker
	if useSubtreeCompletion {
		subtreeTracker = newSubtreeTracker()
		staleViewTracker = newStaleViewTracker()
	}

	var stack []stackEntry

	// executionPathsToState is a map of stateKey -> ExecutionHistory
	// because we want to track which states we've visited but
	// also want to track all the ways a given state can be reached
	executionPathsToState := make(map[NodeHash][]ExecutionHistory)

	// we dont skip over seen states because we want to track all the ways a state can be reached
	// but we do track the states we've seen
	seenStates := make(map[OrderHash]bool)
	seenLogicalStates := make(map[ContentsHash]struct{})
	logicalStatesOut := os.Getenv("KAMERA_LOGICAL_STATES_OUT")
	var logicalStatesFile *os.File
	if logicalStatesOut != "" {
		file, err := os.Create(logicalStatesOut)
		if err != nil {
			logger.Error(err, "failed to create logical states output file", "path", logicalStatesOut)
		} else {
			logicalStatesFile = file
			defer func() {
				if err := logicalStatesFile.Close(); err != nil {
					logger.Error(err, "failed to close logical states output file", "path", logicalStatesOut)
				}
			}()
		}
	}

	logicalStatesLog := os.Getenv("KAMERA_LOGICAL_STATE_LOG")
	var logicalStatesLogFile *os.File
	if logicalStatesLog != "" {
		file, err := os.Create(logicalStatesLog)
		if err != nil {
			logger.Error(err, "failed to create logical state log file", "path", logicalStatesLog)
		} else {
			logicalStatesLogFile = file
			defer func() {
				if err := logicalStatesLogFile.Close(); err != nil {
					logger.Error(err, "failed to close logical state log file", "path", logicalStatesLog)
				}
			}()
		}
	}
	logOrderingPrune := os.Getenv("KAMERA_LOG_ORDERING_PRUNE") != ""
	orderPruneUseOrderHash := os.Getenv("KAMERA_ORDER_PRUNE_ORDER_HASH") != ""

	// we do track the seen converged states so we can attribute multiple execution paths to them
	seenConvergedStates := make(map[NodeHash]StateNode)

	convergencesByDivergenceKey := make(map[NodeHash][]NodeHash)

	// var currentState StateNode
	var currentState StateNode

	// permute the order of the initial state pending reconciles (assume they were all triggered by the initial state)
	// Use enqueueWithMarker to set up proper subtree completion tracking from the start.
	if len(initialState.PendingReconciles) > 1 {
		initialStateVariants := e.expandStateByReconcileOrder(initialState, initialState.PendingReconciles)
		allVariants := append(initialStateVariants, initialState)
		stack, _ = e.enqueueStates(stack, subtreeTracker, allVariants, useSubtreeCompletion)
	} else {
		stack, _ = e.enqueueStates(stack, subtreeTracker, []StateNode{initialState}, useSubtreeCompletion)
	}

	initialHash := initialState.Hash()
	initialSignature := initialState.ExecutionHistory.UniqueKey()
	if e.optimizations != nil {
		e.optimizations.recordInitialPath(initialHash, initialSignature)
	}

	for len(stack) > 0 {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Pop from stack (DFS order)
		entry := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Handle completion markers
		if entry.isMarker() {
			if useSubtreeCompletion {
				subtreeTracker.markCompleted(*entry.marker)
				logger.V(2).Info("subtree completed", "logicalKey", entry.marker)
			}
			continue
		}

		// Handle stale view completion markers
		if entry.isStaleViewMarker() {
			if useSubtreeCompletion {
				staleViewTracker.markCompleted(*entry.staleViewMarker)
				logger.V(2).Info("stale view branch completed", "staleViewKey", entry.staleViewMarker)
			}
			continue
		}

		currentState = *entry.state
		stateKey := currentState.Hash()
		orderKey := currentState.OrderHash()

		// Check if this logical state was completed while we were queued
		// (another path may have finished the entire subtree for the same ordered state)
		if useSubtreeCompletion {
			logicalKey := currentState.LogicalKey()
			if subtreeTracker.isCompleted(logicalKey) {
				e.stats.SubtreeCompletionSkips++
				logger.V(1).Info("skipping state - logical subtree already completed",
					"depth", currentState.depth, "logicalKey", logicalKey)
				continue
			}
		}

		alreadySeen := seenStates[orderKey]
		if logger.V(1).Enabled() {
			logger.V(1).Info("visiting node", "depth", currentState.depth, "Lineage", currentState.DetailedLineage())
		}

		// We reconcile the first pending reconcile for this state, but if there are
		// multiple pending reconciles, we need to explore every ordering. Expanding
		// once per logical state (ContentsHash) is sufficient: the expansion enqueues
		// every first-position choice, and recursive exploration will enumerate the
		// full permutation space. Re-reaching the same ContentsHash with a different
		// ordering does not add new permutations, so we skip re-branching.
		//
		contentsKey := currentState.ContentsHash()

		// Early Convergence Optimization: if ALL pending reconciles are known no-ops,
		// and we've already found a converged path to this logical state, skip entirely.
		// The first path runs through the no-ops to reach actual convergence; subsequent
		// paths can skip since they'd produce the same result.
		if e.optimizations.checkEarlyConvergence(currentState) && !e.hasRemainingUserAction(currentState) {
			if _, alreadyConverged := seenConvergedStates[NodeHash(contentsKey)]; alreadyConverged {
				e.stats.EarlyConvergence++
				logger.V(1).Info("early convergence: all pending are known no-ops and state already converged",
					"depth", currentState.depth, "pendingCount", len(currentState.PendingReconciles))
				continue
			}
		}

		if _, seen := seenLogicalStates[contentsKey]; !seen {
			e.stats.UniqueResourceStates++
			seenLogicalStates[contentsKey] = struct{}{}
			if logicalStatesLogFile != nil {
				pendingIDs := make([]string, len(currentState.PendingReconciles))
				for i, pr := range currentState.PendingReconciles {
					pendingIDs[i] = fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
				}
				_, err := fmt.Fprintf(
					logicalStatesLogFile,
					"%s\tdepth=%d\tobjects=%d\tpending=%s\tstuck=%s\n",
					contentsKey,
					currentState.depth,
					len(currentState.Objects()),
					strings.Join(pendingIDs, ","),
					currentState.stuckPositionsSignature(),
				)
				if err != nil {
					logger.Error(err, "failed to write logical state log")
				}
			}
		}

		if alreadySeen {
			e.stats.AlreadySeenNodeVisits++
		} else {
			e.stats.UniqueNodeVisits++
			seenStates[orderKey] = true
		}
		e.stats.TotalNodeVisits++

		// Record depth distribution stats
		e.stats.RecordVisit(currentState.depth, len(currentState.PendingReconciles), len(stack))

		if _, seen := executionPathsToState[stateKey]; !seen {
			executionPathsToState[stateKey] = make([]ExecutionHistory, 0)
		}
		executionPathsToState[stateKey] = append(executionPathsToState[stateKey], currentState.ExecutionHistory)
		if cancelled := sendWithCancel(ctx, executionPathsCh, currentState); cancelled {
			return nil
		}

		shouldApplyUserAction := e.shouldApplyNextUserAction(currentState)
		if shouldApplyUserAction && logger.V(2).Enabled() {
			logger.V(2).WithValues(
				"Depth", currentState.depth,
				"NextUserActionIdx", currentState.nextUserActionIdx,
			).Info("user action scheduler selected next action")
		}
		if shouldApplyUserAction {
			stateView := currentState
			stepLogger := logger.WithValues(
				"Depth", stateView.depth,
				"# Distinct States", e.stats.UniqueNodeVisits,
				"Total States", e.stats.TotalNodeVisits,
				"Resource States", e.stats.UniqueResourceStates,
			)
			stepCtx := log.IntoContext(ctx, stepLogger)

			stepLogger.WithValues("ActionIdx", stateView.nextUserActionIdx).Info("Taking user action step")
			stepResult, err := e.takeUserActionStep(stepCtx, stateView)
			if err != nil {
				stepLogger.Error(err, "error taking user action step; abandoning branch")
				failurePath := stateView.ExecutionHistory
				if stepResult != nil {
					failurePath = append(slices.Clone(stateView.ExecutionHistory), stepResult)
				}
				if e.emitAbortedState(ctx, abortedStatesCh, stateView, executionPathsToState, failurePath, err) {
					return nil
				}
				continue
			}
			if stepResult == nil {
				stepLogger.Error(nil, "user action step returned nil result")
				if e.emitAbortedState(ctx, abortedStatesCh, stateView, executionPathsToState, stateView.ExecutionHistory, errors.New("nil user action result")) {
					return nil
				}
				continue
			}
			newState, triggeredByStep := e.materializeNextState(stepLogger, stepCtx, stateView, stepResult, nil, stateView.nextUserActionIdx+1)
			newState.depth = stateView.depth + 1
			if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
				seenDepths[newState.depth] = true
			}

			stack = e.enqueueNextStates(
				stack,
				newState,
				triggeredByStep,
				subtreeTracker,
				useSubtreeCompletion,
				logOrderingPrune,
				orderPruneUseOrderHash,
			)
			continue
		}

		// A state is considered converged if:
		// 1. There are no pending reconciles, OR
		// 2. All remaining pending reconciles are ignorable for convergence (async enqueues
		//    from tickers, or requeues from poll-based controllers). These don't indicate
		//    state changes, just time-based or polling behavior.
		//
		// NOTE: If a state has ANY SourceStateChange pending reconciles, it should NOT be
		// considered converged. The allPendingIgnorableForConvergence function returns false
		// if any pending has SourceStateChange.
		if e.isTerminalConvergedState(currentState) {
			convergenceKey := currentState.ConvergenceHash()
			reason := "no pending reconciles"
			if len(currentState.PendingReconciles) > 0 {
				reason = "only async enqueues/requeues remaining"
			}

			// INVARIANT CHECK: No SourceStateChange pending reconciles should be present
			// when marking a state as converged. If this fires, there's a bug.
			for _, pr := range currentState.PendingReconciles {
				if pr.Source == SourceStateChange {
					logger.Error(nil, "BUG: state marked as converged has SourceStateChange pending reconcile",
						"ReconcilerID", pr.ReconcilerID,
						"Request", pr.Request.NamespacedName,
						"Depth", currentState.depth,
						"TotalPending", len(currentState.PendingReconciles),
						"PendingSources", pendingSourcesSummary(currentState.PendingReconciles),
					)
				}
			}

			if logger.V(1).Enabled() {
				logger.V(1).WithValues(
					"Depth", currentState.depth,
					"StateKey", convergenceKey,
					"Reason", reason,
					"RemainingIgnorable", countIgnorableForConvergence(currentState.PendingReconciles),
					"PendingSources", pendingSourcesSummary(currentState.PendingReconciles),
				).Info("arrived at converged state")
			}
			if logger.V(2).Enabled() {
				logger.V(2).Info("lineage", "ReconcileLineage", currentState.ReconcileLineage())
			}

			// Mark this (state, history) as completed - safe to skip future duplicates
			if e.optimizations != nil {
				e.optimizations.markCompleted(stateKey, currentState.ExecutionHistory.UniqueKey())
			}

			seenConvergedStates[convergenceKey] = currentState
			// Also record by ObjectsHash (drops pending reconciles) so early convergence detection can find it
			seenConvergedStates[NodeHash(currentState.ContentsHash())] = currentState
			if convergenceKey != stateKey {
				if _, ok := executionPathsToState[convergenceKey]; !ok {
					executionPathsToState[convergenceKey] = make([]ExecutionHistory, 0)
				}
				executionPathsToState[convergenceKey] = append(executionPathsToState[convergenceKey], currentState.ExecutionHistory)
			}

			// track how many times we've arrived at this state from some common ancestor
			if currentState.divergenceKey != "" {
				if _, seen := convergencesByDivergenceKey[currentState.divergenceKey]; !seen {
					convergencesByDivergenceKey[currentState.divergenceKey] = make([]NodeHash, 0)
				}
				convergencesByDivergenceKey[currentState.divergenceKey] = append(convergencesByDivergenceKey[currentState.divergenceKey], convergenceKey)
			}

			if cancelled := sendWithCancel(ctx, convergedStatesCh, currentState); cancelled {
				return nil
			}
			continue
		}

		// Divergence Circuit-Breaker: limit exploration when paths from a divergence point
		// keep converging to the same state.
		if threshold := e.Config.divergenceCircuitBreakerThreshold; threshold > 0 && currentState.divergenceKey != "" {
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

		// Determine stateView and pendingReconcile for this reconcile step.
		var stateView StateNode
		var pendingReconcile PendingReconcile

		if entry.isStaleViewEntry() {
			// This is a stale view entry - use currentState directly as the view
			stateView = currentState
			pendingReconcile = *entry.staleViewReconcile
		} else {
			// Normal entry - get first pending and possible views
			pendingReconcile = currentState.PendingReconciles[0]

			// Log all pending reconciles for diagnostic purposes
			if logger.V(2).Enabled() {
				pendingIDs := make([]string, len(currentState.PendingReconciles))
				for i, pr := range currentState.PendingReconciles {
					pendingIDs[i] = fmt.Sprintf("%s(%s)", pr.ReconcilerID, pr.Source)
				}
				logger.V(2).WithValues(
					"Depth", currentState.depth,
					"StackDepth", len(stack),
					"PendingCount", len(currentState.PendingReconciles),
					"Pending", pendingIDs,
					"Processing", pendingReconcile.ReconcilerID,
				).Info("processing reconcile step")
			}

			// Diagnostic logging for non-determinism investigation:
			// Log full pending reconcile details to detect if pending list order differs across runs.
			if logger.V(2).Enabled() {
				pendingFull := lo.Map(currentState.PendingReconciles, func(pr PendingReconcile, _ int) string {
					return fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
				})
				logger.V(2).Info("PENDING_LIST_DIAGNOSTIC",
					"depth", currentState.depth,
					"stateHash", stateKey,
					"contentsHash", currentState.ContentsHash(),
					"pendingFull", pendingFull,
				)
			}

			possibleViews, err := e.getPossibleViewsForReconcile(currentState, pendingReconcile.ReconcilerID, currentState.depth)
			if err != nil {
				return errors.Wrap(err, "getting possible views")
			}

			if len(possibleViews) == 0 {
				logger.WithValues(
					"StateKey", stateKey,
					"ReconcilerID", pendingReconcile.ReconcilerID,
					"PendingCount", len(currentState.PendingReconciles),
				).Info("no eligible views for pending reconcile; marking state as aborted")

				abortErr := errors.New(fmt.Sprintf("no eligible views for %s", pendingReconcile.ReconcilerID))
				if e.emitAbortedState(ctx, abortedStatesCh, currentState, executionPathsToState, currentState.ExecutionHistory, abortErr) {
					return nil
				}

				// Skip exploring this branch further since there are no viable views.
				continue
			}

			// If multiple stale views, push all onto stack with marker (stack-based branching)
			if len(possibleViews) > 1 {
				var didEnqueue bool
				stack, didEnqueue = e.enqueueStaleViewStates(
					stack, staleViewTracker, currentState, pendingReconcile, possibleViews, useSubtreeCompletion,
				)
				if !didEnqueue {
					continue
				}
				continue
			}

			// Single view - use it directly
			stateView = possibleViews[0]
		}

		// Process single stateView
		reconcilerID := pendingReconcile.ReconcilerID
		{
			stepLogger := logger.WithValues(
				"Depth", stateView.depth,
				"# Distinct States", e.stats.UniqueNodeVisits,
				"Total States", e.stats.TotalNodeVisits,
				"Resource States", e.stats.UniqueResourceStates,
			)
			stepCtx := log.IntoContext(ctx, stepLogger)

			// key uses OBJECTS hash only - pending list doesn't affect reconciler behavior
			reconcileResKey := fmt.Sprintf("%s:%s:%s", stateView.ContentsHash(), reconcilerID, pendingReconcile.Request.NamespacedName.String())

			// Check cache: can we predict the output state without running the reconcile?
			if e.skipViaCachePrediction(reconcileResKey, stateView, pendingReconcile) {
				stepLogger.V(1).Info("skipping reconcile via cache prediction; would produce duplicate state")
				e.stats.CachePredictedSkips++
				continue
			}

			stepLogger.Info("Taking reconcile step")
			stepResult, err := e.takeReconcileStep(stepCtx, stateView, pendingReconcile)

			// Diagnostic logging for non-determinism investigation:
			// Log the effects order to detect if Knative reconcilers produce effects in different orders across runs.
			if logger.V(2).Enabled() {
				if len(stepResult.Changes.Effects) > 0 {
					effectsOrder := lo.Map(stepResult.Changes.Effects, func(e Effect, _ int) string {
						return fmt.Sprintf("%s:%s", e.OpType, e.Key.String())
					})
					stepLogger.V(2).WithValues(
						"reconciler", pendingReconcile.ReconcilerID,
						"depth", stateView.depth,
						"numEffects", len(stepResult.Changes.Effects),
						"effectsOrder", effectsOrder,
					).Info("EFFECTS_ORDER_DIAGNOSTIC")
				}
			}

			newState, triggeredByStep := e.materializeNextState(
				stepLogger,
				stepCtx,
				stateView,
				stepResult,
				&pendingReconcile,
				stateView.nextUserActionIdx,
			)

			// Track whether this was a no-op (used by ordering optimization)
			wasNoOp := err == nil && stepResult.wasNoOp()
			e.optimizations.recordNoOp(reconcileResKey, wasNoOp) // cacheKey already uses objectsHash
			if wasNoOp {
				e.stats.NoOpReconciles++
			}

			// Update cache with this reconcile's result
			if err == nil && stepResult != nil {
				e.optimizations.setReconcileResult(reconcileResKey, &cachedReconcileResult{
					outputObjectsHash:   newState.ContentsHash(),
					wasNoOp:             wasNoOp,
					numEffects:          len(stepResult.Changes.Effects),
					triggeredReconciles: triggeredByStep,
				})
			}

			if err != nil {
				// if we encounter an error during reconciliation, just abandon this branch
				stepLogger.Error(err, "error taking reconcile step; abandoning branch")
				failurePath := stateView.ExecutionHistory
				if stepResult != nil {
					failurePath = append(slices.Clone(stateView.ExecutionHistory), stepResult)
				}
				if e.emitAbortedState(ctx, abortedStatesCh, stateView, executionPathsToState, failurePath, err) {
					return nil
				}
				continue
			}
			logger.V(1).WithValues("Depth", currentState.depth, "NewPendingReconciles", newState.PendingReconciles).Info("reconcile step completed")

			newState.depth = currentState.depth + 1
			if _, seenDepth := seenDepths[newState.depth]; !seenDepth {
				seenDepths[newState.depth] = true
			}

			if newState.depth > e.Config.MaxDepth {
				if logger.V(1).Enabled() {
					logger.WithValues(
						"maxDepth", e.Config.MaxDepth,
						"currentDepth", newState.depth,
						"Lineage", newState.ReconcileLineage(),
					).Info("aborting path due to max depth")
				}
				if ctxCancelled := e.emitAbortedState(ctx, abortedStatesCh, newState, executionPathsToState, newState.ExecutionHistory, errors.New("max depth reached")); ctxCancelled {
					return nil
				}
				continue
			}

			stack = e.enqueueNextStates(
				stack,
				newState,
				triggeredByStep,
				subtreeTracker,
				useSubtreeCompletion,
				logOrderingPrune,
				orderPruneUseOrderHash,
			)
		}
	}

	if logicalStatesFile != nil {
		keys := make([]string, 0, len(seenLogicalStates))
		for key := range seenLogicalStates {
			keys = append(keys, string(key))
		}
		slices.Sort(keys)
		if _, err := logicalStatesFile.WriteString(strings.Join(keys, "\n") + "\n"); err != nil {
			logger.Error(err, "failed to write logical states output file", "path", logicalStatesOut)
		}
	}

	return nil
}

// enqueueNextStates handles post-step deduplication and pending-order expansion.
func (e *Explorer) enqueueNextStates(
	stack []stackEntry,
	newState StateNode,
	triggeredByStep []PendingReconcile,
	subtreeTracker *subtreeTracker,
	useSubtreeCompletion bool,
	logOrderingPrune bool,
	orderPruneUseOrderHash bool,
) []stackEntry {
	// Deduplication: skip only when this (state, history) has already completed.
	//
	// Key invariant: same pending list means same future possibilities.
	// ContentsHash includes object state + pending reconciles, so two paths only
	// collide when they have identical pending lists, which implies identical future
	// exploration from that point.
	//
	// This is safe with ordering expansion because we queue ordering variants before
	// this dedup would hide anything. At intermediate states, different run orders
	// naturally produce different pending lists (the consumed reconcile differs), so
	// they remain distinct and continue to be explored.
	contentsHash := newState.Hash()
	normalizedHistory := newState.ExecutionHistory.UniqueKey()
	if e.optimizations != nil && e.optimizations.pathCompleted(contentsHash, normalizedHistory) {
		logger.V(1).WithValues(
			"ContentsHash", contentsHash,
			"PathSignature", normalizedHistory,
		).Info("skipping - path already completed exploration")
		e.stats.SkippedPaths++
		return stack
	}

	if e.optimizations != nil {
		e.optimizations.markVisited(contentsHash, normalizedHistory)
		e.optimizations.markLogicalState(
			newState.ContentsHash(),
			newState.PendingReconciles,
			normalizedHistory,
			newState.stuckPositionsSignature(),
		)
	}

	branchStateKey := newState.Hash()
	if orderPruneUseOrderHash {
		branchStateKey = NodeHash(newState.OrderHash())
	}
	statesToEnqueue := make([]StateNode, 0, 1)

	if len(newState.PendingReconciles) > 1 {
		// Order expansion is per logical state branch. If already expanded, we skip
		// duplicative expansion work for equivalent branches.
		alreadyExpanded := e.optimizations != nil &&
			e.optimizations.branchAlreadyExpanded(branchStateKey, triggeredByStep, e.Config.Perturbations.PermuteOrder)
		if !alreadyExpanded {
			expandedStates := e.expandStateByReconcileOrder(newState, triggeredByStep)

			if logger.V(2).Enabled() && len(expandedStates) > 0 {
				variantFirstReconcilers := lo.Map(expandedStates, func(s StateNode, _ int) string {
					if len(s.PendingReconciles) > 0 {
						pr := s.PendingReconciles[0]
						return fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
					}
					return "empty"
				})
				pendingBefore := lo.Map(newState.PendingReconciles, func(pr PendingReconcile, _ int) string {
					return fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
				})
				logger.V(2).Info("ORDERING_VARIANTS_DIAGNOSTIC",
					"depth", newState.depth,
					"numVariants", len(expandedStates),
					"pendingBefore", pendingBefore,
					"variantFirstReconcilers", variantFirstReconcilers,
				)
			}

			for _, orderVariant := range expandedStates {
				if e.optimizations != nil && e.optimizations.noOpOrderingSkipEnabled() {
					fst := orderVariant.PendingReconciles[0]
					noOpKey := fmt.Sprintf("%s:%s:%s", orderVariant.ContentsHash(), fst.ReconcilerID, fst.Request.NamespacedName.String())
					if isNoOp, known := e.optimizations.isKnownNoOp(noOpKey); known && isNoOp {
						e.stats.SkippedNoOpOrderings++
						continue
					}
				}
				statesToEnqueue = append(statesToEnqueue, orderVariant)
			}
			if e.optimizations != nil {
				e.optimizations.markBranchExpanded(branchStateKey, triggeredByStep, e.Config.Perturbations.PermuteOrder)
			}
		} else if e.optimizations != nil {
			if logOrderingPrune {
				pendingIDs := make([]string, len(newState.PendingReconciles))
				for i, pr := range newState.PendingReconciles {
					pendingIDs[i] = fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
				}
				triggeredIDs := make([]string, len(triggeredByStep))
				for i, pr := range triggeredByStep {
					triggeredIDs[i] = fmt.Sprintf("%s:%s/%s", pr.ReconcilerID, pr.Request.Namespace, pr.Request.Name)
				}
				logger.Info("ordering pruning: skip expansion for already-expanded state",
					"depth", newState.depth,
					"branchKey", branchStateKey,
					"nodeHash", newState.Hash(),
					"orderHash", newState.OrderHash(),
					"orderSensitiveKey", orderPruneUseOrderHash,
					"contentsHash", newState.ContentsHash(),
					"permuteSignature", e.optimizations.permuteSignature(triggeredByStep, e.Config.Perturbations.PermuteOrder),
					"pending", pendingIDs,
					"triggeredByStep", triggeredIDs,
				)
			}
			e.stats.SkippedOrderExpansions++
		}
	}

	statesToEnqueue = append(statesToEnqueue, newState)
	stack, _ = e.enqueueStates(stack, subtreeTracker, statesToEnqueue, useSubtreeCompletion)
	return stack
}

func (e *Explorer) materializeNextState(
	stepLogger logr.Logger,
	stepCtx context.Context,
	stateView StateNode,
	stepResult *ReconcileResult,
	consumed *PendingReconcile,
	nextUserActionIdx int,
) (StateNode, []PendingReconcile) {
	stepResult.StateBefore = maps.Clone(stateView.Objects())
	stepResult.KindSeqBefore = maps.Clone(stateView.Contents.KindSequences)

	newContents, newSequences, newStateEvents := e.applyEffects(stepLogger, stateView, stepResult)
	triggeredByStep := e.getTriggeredReconcilers(stepResult.Changes)
	newPendingReconciles := e.determineNewPendingReconciles(stepCtx, stateView, consumed, stepResult)
	stepLogger.V(1).WithValues(
		"Depth", stateView.depth,
		"Count", len(newPendingReconciles),
		"Items", newPendingReconciles,
	).Info("final pending reconciles after step")

	stepResult.StateAfter = newContents
	stepResult.KindSeqAfter = newSequences
	stepResult.PendingReconciles = newPendingReconciles

	newState := StateNode{
		Contents:                 NewStateSnapshot(newContents, newSequences, newStateEvents),
		PendingReconciles:        newPendingReconciles,
		parent:                   &stateView,
		action:                   stepResult,
		divergenceKey:            stateView.divergenceKey,
		stuckReconcilerPositions: maps.Clone(stateView.stuckReconcilerPositions),
		ExecutionHistory:         append(slices.Clone(stateView.ExecutionHistory), stepResult),
		nextUserActionIdx:        nextUserActionIdx,
	}
	newState.ID = string(newState.Hash())

	return newState, triggeredByStep
}

// emitAbortedState records an aborted exploration branch and attempts to send it on the channel.
// Returns true if the context was cancelled before the send completed.
func (e *Explorer) emitAbortedState(
	ctx context.Context,
	abortedStatesCh chan<- ResultState,
	state StateNode,
	executionPathsToState map[NodeHash][]ExecutionHistory,
	path ExecutionHistory,
	err error,
) bool {
	e.stats.AbortedPaths++
	stateKey := state.Hash()
	executionPathsToState[stateKey] = append(executionPathsToState[stateKey], path)

	aborted := ResultState{
		ID:    fmt.Sprintf("aborted-%s", stateKey),
		State: state,
		Paths: []ExecutionHistory{path},
		Error: err,
	}

	select {
	case abortedStatesCh <- aborted:
		return false
	case <-ctx.Done():
		return true
	}
}

func (e *Explorer) applyEffects(stepLogger logr.Logger, stateView StateNode, stepResult *ReconcileResult) (ObjectVersions, KindSequences, []StateEvent) {
	changes := stepResult.Changes.ObjectVersions

	// initialize outputs which the effects will be applied to
	nextState := maps.Clone(stateView.Objects())
	nextSequences := maps.Clone(stateView.Contents.KindSequences)
	newStateEvents := slices.Clone(stateView.Contents.stateEvents)

	var highestSequence int64
	if len(newStateEvents) > 0 {
		// stateEvents are ordered, so the last entry carries the current max sequence.
		highestSequence = newStateEvents[len(newStateEvents)-1].Sequence
	}

	for _, effect := range stepResult.Changes.Effects {
		existingKey, exists := nextState.HasNamespacedNameForKind(effect.Key.ResourceKey)

		switch effect.OpType {
		case event.CREATE:
			if exists {
				// the effect validation mechanism should prevent a create effect from going through
				// with an 'AlreadyExists' error, so panic if it does happen
				panic("create effect object already exists in prev state: " + effect.Key.String())
			}
			// Mimic APIServer behavior: set Generation to 1 on CREATE if not already set
			newObj := e.versionManager.Resolve(changes[effect.Key])
			if newObj != nil {
				gen := util.GetObjectGeneration(newObj)
				if gen == 0 {
					newObj.SetGeneration(1)
					// Update the version hash after modifying Generation
					changes[effect.Key] = e.versionManager.Publish(newObj)
				}
			}
			nextState[effect.Key] = changes[effect.Key]
		case event.UPDATE, event.PATCH:
			if !exists {
				// it is possible that a stale read will cause a controller to update an object
				// that no longer exists in the global state. The effect validation mechanism
				// should cause the client operation to 404 and prevent the update effect from
				// going through. If it does go through, we should panic cause something broke.
				panic("update effect object not found in prev state: " + effect.Key.String())
			}
			oldVersion := nextState[existingKey]
			if exists && existingKey != effect.Key {
				delete(nextState, existingKey)
			}
			// Mimic APIServer behavior: increment Generation on spec updates (not status-only updates)
			oldObj := e.versionManager.Resolve(oldVersion)
			newObj := e.versionManager.Resolve(changes[effect.Key])
			if oldObj != nil && newObj != nil {
				// Compare specs to determine if Generation should be incremented
				// In Kubernetes, Generation is only incremented when spec changes, not on status-only updates
				// Use a safe check to avoid panics if spec comparison fails
				specChanged, err := snapshot.CheckSpecChanged(oldObj, newObj)
				if err != nil {
					// If we can't determine if spec changed, conservatively increment Generation
					// This is safer than not incrementing it
					stepLogger.V(2).WithValues("key", effect.Key, "error", err).Info("error checking spec change, incrementing Generation conservatively")
					specChanged = true
				}
				if specChanged {
					oldGen := util.GetObjectGeneration(oldObj)
					if oldGen == 0 {
						// If Generation is 0, set it to 1 (shouldn't happen in real K8s, but handle gracefully)
						// This can happen if the state snapshot has objects with Generation=0
						newObj.SetGeneration(1)
						stepLogger.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", 1).Info("set Generation to 1 on spec update (was 0)")
					} else {
						newObj.SetGeneration(oldGen + 1)
						stepLogger.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", newObj.GetGeneration()).Info("incremented Generation on spec update")
					}
					// Update the version hash after modifying Generation
					changes[effect.Key] = e.versionManager.Publish(newObj)
				}
			}
			nextState[effect.Key] = changes[effect.Key]
		case event.APPLY:
			// Server-side apply has upsert semantics (create if missing, update if present).
			if !exists {
				// Apply has upsert semantics; if the object doesn't exist, treat as CREATE.
				newObj := e.versionManager.Resolve(changes[effect.Key])
				if newObj != nil {
					gen := util.GetObjectGeneration(newObj)
					if gen == 0 {
						newObj.SetGeneration(1)
						// Update the version hash after modifying Generation
						changes[effect.Key] = e.versionManager.Publish(newObj)
					}
				}
				nextState[effect.Key] = changes[effect.Key]
				break
			}

			// Capture the existing version before any key replacement.
			oldVersion := nextState[existingKey]
			if exists && existingKey != effect.Key {
				delete(nextState, existingKey)
			}

			// Mimic APIServer behavior: increment Generation on spec updates (not status-only updates)
			oldObj := e.versionManager.Resolve(oldVersion)
			newObj := e.versionManager.Resolve(changes[effect.Key])
			if oldObj != nil && newObj != nil {
				// Compare specs to determine if Generation should be incremented
				// In Kubernetes, Generation is only incremented when spec changes, not on status-only updates
				// Use a safe check to avoid panics if spec comparison fails
				specChanged, err := snapshot.CheckSpecChanged(oldObj, newObj)
				if err != nil {
					// If we can't determine if spec changed, conservatively increment Generation
					// This is safer than not incrementing it
					stepLogger.V(2).WithValues("key", effect.Key, "error", err).Info("error checking spec change, incrementing Generation conservatively")
					specChanged = true
				}
				if specChanged {
					oldGen := util.GetObjectGeneration(oldObj)
					if oldGen == 0 {
						// If Generation is 0, set it to 1 (shouldn't happen in real K8s, but handle gracefully)
						// This can happen if the state snapshot has objects with Generation=0
						newObj.SetGeneration(1)
						stepLogger.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", 1).Info("set Generation to 1 on spec update (was 0)")
					} else {
						newObj.SetGeneration(oldGen + 1)
						stepLogger.V(2).WithValues("key", effect.Key, "oldGen", oldGen, "newGen", newObj.GetGeneration()).Info("incremented Generation on spec update")
					}
					// Update the version hash after modifying Generation
					changes[effect.Key] = e.versionManager.Publish(newObj)
				}
			}
			nextState[effect.Key] = changes[effect.Key]

		// need to determine how to update state based on preconditions
		case event.MARK_FOR_DELETION:
			if !exists {
				// We should never get here (effect validation should fail if there is no object matching the namespace/name in the state)
				// but if we do, we should panic because something is wrong.
				stepLogger.Info("warning: deleted key absent in state", "effectKey", effect.Key)
				panic("deleted key is not present in prev state. effect validation should have prevented this")
			}
			stepLogger.WithValues("Key", effect.Key).V(2).Info("marked object for deletion")
			if existingKey != effect.Key {
				delete(nextState, existingKey)
			}
			// the delete effect is valid, so we should add it to the state
			nextState[effect.Key] = changes[effect.Key]

		case event.REMOVE:
			if !exists {
				stepLogger.Error(nil, "warning: removed key absent in state", "effectKey", effect.Key)
				panic("removed key is not present in prev state. effect validation should have prevented this")
			}
			stepLogger.V(2).Info("removing object from state", "key", effect.Key)
			delete(nextState, existingKey)
		default:
			// at this part of the code we are only working with write effects
			panic(fmt.Errorf("unknown effect type: %s", effect.OpType))
		}

		highestSequence++
		newRV := highestSequence

		// increment resourceversion for the kind
		nextSequences[effect.Key.IdentityKey.CanonicalGroupKind()] = newRV
		stateEvent := StateEvent{
			ReconcileID: stepResult.FrameID,
			Sequence:    newRV,
			Effect:      effect,
			// TODO handle time info
			Timestamp: "",
		}
		newStateEvents = append(newStateEvents, stateEvent)
	}

	return nextState, nextSequences, newStateEvents
}

// takeReconcileStep transitions the execution from one StateNode to another StateNode
func (e *Explorer) takeReconcileStep(ctx context.Context, state StateNode, pr PendingReconcile) (*ReconcileResult, error) {
	stepLog := log.FromContext(ctx)
	startWall := time.Now()
	defer func() {
		if e.stats != nil && e.Config != nil && e.Config.RecordPerfStats {
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
		tolerableErrResult := &ReconcileResult{
			ControllerID: pr.ReconcilerID,
			FrameID:      frameID,
			FrameType:    FrameTypeExplore,
			Changes:      Changes{ObjectVersions: make(ObjectVersions)},
			Error:        err.Error(),
		}
		return tolerableErrResult, nil
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
		return failure, err
	}

	return reconcileResult, nil
}

// takeUserActionStep executes the next user action for this branch directly
// without routing it through a synthetic pending reconcile.
func (e *Explorer) takeUserActionStep(ctx context.Context, state StateNode) (*ReconcileResult, error) {
	stepLog := log.FromContext(ctx)
	startWall := time.Now()
	defer func() {
		if e.stats != nil && e.Config != nil && e.Config.RecordPerfStats {
			e.stats.RecordStep(UserControllerID, time.Since(startWall))
		}
	}()

	if e.userController == nil {
		return nil, fmt.Errorf("user controller is nil")
	}

	restoreClock := simclock.SetDepth(state.depth)
	defer restoreClock()

	stepLog.WithValues("ActionIdx", state.nextUserActionIdx).V(2).Info("about to execute user action")

	result, err := e.userController.ExecuteNextAction(ctx, state.Objects(), state.nextUserActionIdx)
	if err != nil {
		if result == nil {
			result = &ReconcileResult{
				ControllerID: UserControllerID,
				FrameType:    FrameTypeExplore,
				Error:        err.Error(),
			}
		}
		stepLog.WithValues("ActionIdx", state.nextUserActionIdx).Error(err, "error executing user action")
		return result, err
	}

	return result, nil
}

func (e *Explorer) getNewPendingReconciles(currPending, triggered []PendingReconcile) []PendingReconcile {
	// Ordering: existing pending first, then newly triggered.
	// This prevents reconcilers that frequently requeue/trigger from starving others.
	// Among duplicates for the same (ReconcilerID + NamespacedName):
	//   - Any StateChange source overrides others.
	//   - Otherwise, the first occurrence in this merged list wins.
	all := append(currPending, triggered...)

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
		// StateChange overrides any previously seen entry.
		if pr.Source == SourceStateChange && existing.Source != SourceStateChange {
			resultMap[key] = pr
		}
		// Otherwise, keep the existing one (first occurrence wins).
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

	if pr.Request.NamespacedName.Name == "" {
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
	config, ok := e.Config.Perturbations.Staleness[reconcilerID]
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

	logger.V(2).Info("getting possible views for reconciler", "ReconcilerID", reconcilerID, "CurrDepth", currDepth, "MaxDepth", e.Config.MaxDepth)
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
			nextUserActionIdx: currState.nextUserActionIdx,

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

	filtered := lo.Filter(asStateNodes, func(sn StateNode, _ int) bool {
		return sn.Contents.Priority != Skip
	})

	return filtered, nil
}

func (e *Explorer) determineNewPendingReconciles(ctx context.Context, state StateNode, consumed *PendingReconcile, result *ReconcileResult) []PendingReconcile {
	stepLog := log.FromContext(ctx)

	stillPending := slices.Clone(state.PendingReconciles)
	reconcilerID := UserControllerID
	// consumed is nil for user action steps, and non-nil for reconcile steps which were once pending.
	if consumed != nil {
		reconcilerID = consumed.ReconcilerID
		// INVARIANT 3: The reconciler taking the step should be present in the previous state's pending reconciles.
		reconcilerWasPending := false
		for _, pr := range state.PendingReconciles {
			if pr.ReconcilerID == consumed.ReconcilerID &&
				pr.Request.NamespacedName == consumed.Request.NamespacedName {
				reconcilerWasPending = true
				break
			}
		}
		if !reconcilerWasPending {
			stepLog.Error(nil, "INVARIANT VIOLATION: reconciler took step but was not in pending queue",
				"reconcilerID", consumed.ReconcilerID,
				"request", consumed.Request.NamespacedName,
				"pendingCount", len(state.PendingReconciles),
				"depth", state.depth)
		}

		// Remove the reconcile that just ran.
		stillPending = lo.Filter(state.PendingReconciles, func(pending PendingReconcile, _ int) bool {
			return pending != *consumed
		})
	}

	// Read captured enqueues from the global collector (from Watch callbacks during reconcile).
	// Get() automatically clears the collector after returning, so it's ready for the next step.
	// These are already PendingReconcile entries with the correct reconciler ID.
	capturedPending := GetGlobalAsyncEnqueueCollector().Get()
	if len(capturedPending) > 0 {
		stepLog.V(1).Info("captured async enqueues from tickers",
			"count", len(capturedPending),
			"depth", state.depth,
			"ReconcilerID", reconcilerID,
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
			"ReconcilerID", reconcilerID,
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

	// If the controller returned Requeue or RequeueAfter, re-enqueue the original request.
	// RequeueAfter is treated as actionable follow-up work.
	if consumed != nil && (result.ctrlRes.Requeue || result.ctrlRes.RequeueAfter > 0) {
		source := SourceRequeue
		if result.ctrlRes.RequeueAfter > 0 {
			source = SourceRequeueAfter
		}
		requeued := PendingReconcile{
			ReconcilerID: consumed.ReconcilerID,
			Request:      consumed.Request,
			Source:       source,
		}
		triggeredByChanges = append(triggeredByChanges, requeued)
	}

	allTriggered := append(triggeredByChanges, capturedPending...)
	newPending := e.getNewPendingReconciles(stillPending, allTriggered)

	// INVARIANT 1: If the step had no writes, no new pending reconciles should have "State Change" source
	wasNoOp := result.wasNoOp()
	if consumed != nil && wasNoOp {
		for _, triggered := range triggeredByChanges {
			if triggered.Source == SourceStateChange {
				stepLog.Error(nil, "INVARIANT VIOLATION: no-op step triggered State Change reconcile",
					"reconcilerID", consumed.ReconcilerID,
					"triggeredReconciler", triggered.ReconcilerID,
					"triggeredRequest", triggered.Request.NamespacedName,
					"depth", state.depth,
					"effectCount", len(result.Changes.Effects))
			}
		}
	}

	// INVARIANT 2: Only the reconciler that just took the step should be removed.
	// Check that all items in stillPending that were in the original pending are still present,
	// and the only thing removed is the reconcileInput.
	// TODO move this out of the hot path and under unit test coverage.
	if consumed != nil {
		for _, originalPending := range state.PendingReconciles {
			if originalPending == *consumed {
				continue // This one should be removed
			}
			// Check if it's still pending (either in stillPending or re-added through triggeredByChanges)
			foundInStillPending := false
			for _, sp := range stillPending {
				if sp.ReconcilerID == originalPending.ReconcilerID &&
					sp.Request.NamespacedName == originalPending.Request.NamespacedName {
					foundInStillPending = true
					break
				}
			}
			if !foundInStillPending {
				stepLog.Error(nil, "INVARIANT VIOLATION: pending reconcile mysteriously removed (not the one that took the step)",
					"removedReconciler", originalPending.ReconcilerID,
					"removedRequest", originalPending.Request.NamespacedName,
					"stepTakenBy", consumed.ReconcilerID,
					"depth", state.depth)
			}
		}
	}

	return newPending
}

func (e *Explorer) skipViaCachePrediction(
	cacheKey string,
	stateView StateNode,
	pendingReconcile PendingReconcile,
) bool {
	if e.optimizations == nil || !e.optimizations.cachePredictionEnabled() {
		return false
	}

	cached, ok := e.optimizations.getReconcileResult(cacheKey)
	if !ok {
		return false
	}

	// Check cache: can we predict the output state without running the reconcile?
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
	if e.optimizations.hasLogicalState(cached.outputObjectsHash, predictedPending, predictedHistory, stateView.stuckPositionsSignature()) {
		return true
	}
	return false
}

// computeSubtreeKey returns a key representing the logical state for subtree deduplication.
// Uses order-sensitive pending to distinguish different exploration orderings.
// Two states with the same objects and same pending ORDER will explore identical subtrees.
func (e *Explorer) computeSubtreeKey(state StateNode) string {
	// Use order-sensitive pending: different orderings can produce different outcomes
	pendingStrs := lo.Map(state.PendingReconciles, func(pr PendingReconcile, _ int) string {
		return pr.String()
	})
	// NOT sorted - order matters for subtree identity
	return fmt.Sprintf("%s|%s", state.ContentsHash(), strings.Join(pendingStrs, ","))
}

func sendWithCancel[T any](ctx context.Context, ch chan<- T, val T) bool {
	select {
	case <-ctx.Done():
		return true
	case ch <- val:
		return false
	}
}
