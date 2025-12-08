# Explore Pruning Optimizations

This document captures potential optimizations for pruning the DFS state space exploration in `pkg/tracecheck/explore.go` while preserving detection of order-sensitive reconciler behavior.

## Current Pruning Mechanisms

| Mechanism | Location | Purpose |
|-----------|----------|---------|
| `statesLeadingToConvergence` | L248-252, 360-370, 386-393 | Skip (state+order) proven to converge |
| Divergence Circuit Breaker | L395-408 | Skip when subtree keeps finding same convergence |
| `visitedStatePaths` | L552-594 | Skip enqueue of (state, history) duplicates |
| `seenStatesPendingOrderSensitive` | L304-315 | Avoid re-branching same lineage |

## Potential Optimizations

### 1. No-op Reconcile Caching

Track `(ReconcilerID, StateHash) -> producedChanges bool`. If a reconciler produced no changes at a state before, it's likely idempotent there.

```go
// After a reconcile step completes with no effects:
if len(reconcileResult.Changes.Effects) == 0 {
    noOpReconciles[reconcilerID][stateHash] = true
}
```

**Benefit**: Could skip orderings that differ only by no-op reconciler position.

**Risk**: Need to be careful with stale views that might change behavior.

**Implementation complexity**: Medium

---

### 2. Commutative Reconciler Detection

If two reconcilers have disjoint read/write sets (don't touch the same Kinds), their order doesn't matter.

```go
// If ReconcilerA writes only ConfigMaps, and ReconcilerB writes only Pods:
// [A, B] and [B, A] produce same result -> only explore one
```

**Benefit**: Could cut ordering explosion in half for independent reconcilers.

**Challenge**: Requires analyzing reconciler dependencies statically or dynamically. The `ResourceDeps` structure already tracks which Kinds each reconciler depends on, so this information is partially available.

**Implementation complexity**: Medium-High

---

### 3. Early Duplicate Detection (Pre-Step)

Currently, deduplication happens AFTER `takeReconcileStep` (L577-594). We could check earlier:

```go
// Before taking step, compute what the resulting state hash would be
// (if we can predict it cheaply)
predictedHash := predictResultingStateHash(stateView, pendingReconcile)
if visitedStatePaths[predictedHash] != nil {
    continue // Skip the actual reconcile
}
```

**Benefit**: Avoid expensive reconcile step execution for duplicate states.

**Challenge**: Hard to predict state without executing. Would require either:
- Caching previous (input state, reconciler) -> output state mappings
- Some form of static analysis

**Implementation complexity**: High

---

### 4. Stable Reconciler Detection

If a reconciler runs multiple times and always produces the same result (or no changes), mark it as "stable" for that state:

```go
type reconcileOutcome struct {
    stateHash  StateHash
    hasChanges bool
    resultHash StateHash // hash of resulting state
}

// Track outcomes per (reconcilerID, inputStateHash)
// If same outcome seen N times, skip future runs in certain orderings
```

**Benefit**: Automatically detects idempotent reconcilers and reduces redundant exploration.

**Implementation complexity**: Medium

---

### 5. Order-Insensitive Skip When All Orderings Converge Same

If we've explored multiple orderings from a state and they ALL lead to the SAME converged state, we can use order-insensitive skip for remaining orderings:

```go
// Track: stateHash -> set of converged state hashes reached from different orderings
convergencesByState := make(map[StateHash]map[StateHash]bool)

// When finding convergence, record which state hash we started from
convergencesByState[ancestorStateHash][convergedStateHash] = true

// If all explored orderings from state X lead to converged state Y,
// we can skip remaining orderings with order-insensitive check
if len(convergencesByState[stateHash]) == 1 && exploredOrderingsCount >= threshold {
    // All orderings lead to same place, safe to skip remaining
}
```

**Benefit**: Aggressive pruning when order doesn't matter for a particular state.

**Preserves order sensitivity**: Still explores first few orderings to detect if order matters.

**Implementation complexity**: Medium

---

### 6. Bounded Ordering Exploration

Instead of exploring ALL orderings at every branch point, explore a bounded subset:

```go
const maxOrderingsPerState = 3 // configurable

if len(expandedStates) > maxOrderingsPerState {
    // Prioritize orderings by some heuristic:
    // - Prefer orderings where "important" reconcilers go first
    // - Prefer orderings that differ most from already-explored
    expandedStates = prioritizeAndLimit(expandedStates, maxOrderingsPerState)
}
```

**Benefit**: Caps exponential blowup from ordering permutations.

**Risk**: Might miss some order-sensitive bugs if the problematic ordering isn't in the explored subset.

**Mitigation**: Use smart prioritization based on reconciler characteristics.

**Implementation complexity**: Low-Medium

---

### 7. Subsume Equivalent Converged States

If multiple converged states are "semantically equivalent" (same objects, just different metadata like timestamps), treat them as one:

```go
// Define semantic equivalence that ignores non-essential fields
func semanticHash(state StateNode) SemanticHash {
    // Hash objects but ignore timestamps, resourceVersions, etc.
}

// Use semantic hash for convergence deduplication
```

**Benefit**: Reduces false "multiple converged states" reports.

**Note**: The `determinize_deps.sh` preprocessing already handles some of this, but there may be additional opportunities.

**Implementation complexity**: Medium

---

## Recommendation

**Option 5 (Order-Insensitive Skip When All Orderings Converge Same)** is most aligned with the goal of finding order-sensitive outcomes while pruning aggressively:

1. Explores enough orderings to detect if order matters
2. Aggressively prunes once we've confirmed order doesn't affect outcome for a state
3. Minimal risk of missing order-sensitive bugs
4. Reasonable implementation complexity

**Option 1 (No-op Reconcile Caching)** is a good complementary optimization that's relatively easy to implement and provides immediate benefit for reconcilers that frequently produce no changes.

## Implementation Priority

1. **Option 5** - Best balance of safety and pruning power
2. **Option 1** - Easy win for no-op reconciles
3. **Option 6** - Simple bounded exploration as a fallback
4. **Option 2** - If reconciler dependencies are well-defined
5. **Options 3, 4, 7** - More complex, implement if needed
