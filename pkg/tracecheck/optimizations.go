package tracecheck

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"
)

// optimizations is a run-state container for various exploration optimizations.
type optimizations struct {
	cfg OptimizationConfig

	visitedStatePaths     map[NodeHash]map[string]struct{}
	completedPaths        map[string]bool
	knownNoOps            map[string]bool
	reconcileResCache     map[string]*cachedReconcileResult
	exploredLogicalStates map[string]struct{}
	seenBranchingByState  map[string]bool
	subtreeTracker        *subtreeTracker
}

func newOptimizations(cfg OptimizationConfig) *optimizations {
	if !cfg.AnyEnabled() {
		return nil
	}
	return &optimizations{
		cfg:                   cfg,
		visitedStatePaths:     make(map[NodeHash]map[string]struct{}),
		completedPaths:        make(map[string]bool),
		knownNoOps:            make(map[string]bool),
		reconcileResCache:     make(map[string]*cachedReconcileResult),
		exploredLogicalStates: make(map[string]struct{}),
		seenBranchingByState:  make(map[string]bool),
		subtreeTracker:        newSubtreeTracker(),
	}
}

func (o *optimizations) earlyConvergenceEnabled() bool {
	return o != nil && o.cfg.EarlyConvergence
}

func (o *optimizations) completedPathDedupEnabled() bool {
	return o != nil && o.cfg.CompletedPathDedup
}

func (o *optimizations) orderingPruningEnabled() bool {
	return o != nil && o.cfg.OrderingPruning
}

func (o *optimizations) noOpOrderingSkipEnabled() bool {
	return o != nil && o.cfg.OrderingPruning && !o.cfg.DisableNoOpOrderingSkip
}

func (o *optimizations) cachePredictionEnabled() bool {
	return o != nil && o.cfg.CachePrediction
}

func (o *optimizations) permuteSignature(triggered []PendingReconcile, permuteOrder map[ReconcilerID]bool) string {
	if o == nil || len(triggered) == 0 || len(permuteOrder) == 0 {
		return ""
	}
	seen := make(map[ReconcilerID]struct{})
	ids := make([]string, 0, len(triggered))
	for _, pr := range triggered {
		if permute, ok := permuteOrder[pr.ReconcilerID]; !ok || !permute {
			continue
		}
		if _, exists := seen[pr.ReconcilerID]; exists {
			continue
		}
		seen[pr.ReconcilerID] = struct{}{}
		ids = append(ids, string(pr.ReconcilerID))
	}
	if len(ids) == 0 {
		return ""
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

func (o *optimizations) branchKey(stateHash NodeHash, triggered []PendingReconcile, permuteOrder map[ReconcilerID]bool) string {
	return fmt.Sprintf("%s|%s", stateHash, o.permuteSignature(triggered, permuteOrder))
}

func (o *optimizations) recordInitialPath(stateHash NodeHash, historyKey string) {
	if !o.completedPathDedupEnabled() {
		return
	}
	o.ensureVisited(stateHash)[historyKey] = struct{}{}
}

func (o *optimizations) ensureVisited(stateHash NodeHash) map[string]struct{} {
	if !o.completedPathDedupEnabled() {
		return nil
	}
	if _, ok := o.visitedStatePaths[stateHash]; !ok {
		o.visitedStatePaths[stateHash] = make(map[string]struct{})
	}
	return o.visitedStatePaths[stateHash]
}

func (o *optimizations) markVisited(stateHash NodeHash, historyKey string) {
	if !o.completedPathDedupEnabled() {
		return
	}
	o.ensureVisited(stateHash)[historyKey] = struct{}{}
}

func (o *optimizations) pathCompleted(stateHash NodeHash, historyKey string) bool {
	if !o.completedPathDedupEnabled() {
		return false
	}
	completionKey := fmt.Sprintf("%s|%s", stateHash, historyKey)
	return o.completedPaths[completionKey]
}

func (o *optimizations) markCompleted(stateHash NodeHash, historyKey string) {
	if !o.completedPathDedupEnabled() {
		return
	}
	completionKey := fmt.Sprintf("%s|%s", stateHash, historyKey)
	o.completedPaths[completionKey] = true
}

func (o *optimizations) recordNoOp(key string, wasNoOp bool) {
	if o == nil || !(o.cfg.EarlyConvergence || o.cfg.OrderingPruning) {
		return
	}
	o.knownNoOps[key] = wasNoOp
}

func (o *optimizations) isKnownNoOp(key string) (bool, bool) {
	if o == nil || !(o.cfg.EarlyConvergence || o.cfg.OrderingPruning) {
		return false, false
	}
	val, ok := o.knownNoOps[key]
	return val, ok
}

func (o *optimizations) checkEarlyConvergence(state StateNode) bool {
	if o == nil {
		return false
	}
	if !o.earlyConvergenceEnabled() || len(state.PendingReconciles) == 0 {
		return false
	}

	objectsHash := state.ContentsHash()
	for _, pr := range state.PendingReconciles {
		noOpKey := fmt.Sprintf("%s:%s:%s", objectsHash, pr.ReconcilerID, pr.Request.NamespacedName.String())
		if isNoOp, known := o.knownNoOps[noOpKey]; !known || !isNoOp {
			return false
		}
	}
	return true
}

func (o *optimizations) pendingSignature(pending []PendingReconcile) string {
	if o == nil {
		return ""
	}
	pendingStrs := lo.Map(pending, func(pr PendingReconcile, _ int) string {
		return pr.String()
	})
	return strings.Join(pendingStrs, ",")
}

func (o *optimizations) logicalStateKey(objectsHash ContentsHash, pending []PendingReconcile, historyKey string, stuckKey string) string {
	if !o.cachePredictionEnabled() {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s", objectsHash, o.pendingSignature(pending), stuckKey, historyKey)
}

func (o *optimizations) markLogicalState(objectsHash ContentsHash, pending []PendingReconcile, historyKey string, stuckKey string) {
	if !o.cachePredictionEnabled() {
		return
	}
	key := o.logicalStateKey(objectsHash, pending, historyKey, stuckKey)
	o.exploredLogicalStates[key] = struct{}{}
}

func (o *optimizations) hasLogicalState(objectsHash ContentsHash, pending []PendingReconcile, historyKey string, stuckKey string) bool {
	if !o.cachePredictionEnabled() {
		return false
	}
	key := o.logicalStateKey(objectsHash, pending, historyKey, stuckKey)
	_, explored := o.exploredLogicalStates[key]
	return explored
}

func (o *optimizations) setReconcileResult(cacheKey string, res *cachedReconcileResult) {
	if !o.cachePredictionEnabled() {
		return
	}
	o.reconcileResCache[cacheKey] = res
}

func (o *optimizations) getReconcileResult(cacheKey string) (*cachedReconcileResult, bool) {
	if !o.cachePredictionEnabled() {
		return nil, false
	}
	res, ok := o.reconcileResCache[cacheKey]
	return res, ok
}

func (o *optimizations) branchAlreadyExpanded(stateHash NodeHash, triggered []PendingReconcile, permuteOrder map[ReconcilerID]bool) bool {
	if o == nil || !o.orderingPruningEnabled() {
		return false
	}
	key := o.branchKey(stateHash, triggered, permuteOrder)
	return o.seenBranchingByState[key]
}

func (o *optimizations) markBranchExpanded(stateHash NodeHash, triggered []PendingReconcile, permuteOrder map[ReconcilerID]bool) {
	if o == nil || !o.orderingPruningEnabled() {
		return
	}
	key := o.branchKey(stateHash, triggered, permuteOrder)
	o.seenBranchingByState[key] = true
}
