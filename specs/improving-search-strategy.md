# Improving DFS Search Strategy for Order Sensitivity Detection

## Problem Statement

The DFS exploration in `pkg/tracecheck/explore.go` spends too much time in lower subtrees (depths ≤50), producing many paths that:

1. Share the same **mutation history** (sequence of state-changing reconciles)
2. Converge to the **same final state**
3. Don't reveal any **ordering sensitivity** (different orderings → same outcome)

The goal of the DFS is to identify **ordering sensitivities** among controller reconciles—i.e., cases where different reconcile orderings produce different converged states. This would indicate a bug in the controller logic.

Currently, even with only one reconciler permuted (RevisionReconciler), the search spends most of its time at lower depths without efficiently exploring higher depths where interesting ordering sensitivities might exist.

## Current Optimizations

The explorer already has several optimizations:

- `knownNoOps` — skip orderings that put known no-ops first
- `seenBranchingByState` — don't re-expand orderings for the same logical state
- `visitedStatePaths` — track states by mutation signature
- `reconcileCache` — cache reconcile results to predict outputs
- `exploredLogicalStates` — skip predicted duplicate states
- `divergenceCircuitBreakerThreshold` — limit exploration when paths from a divergence point keep converging to the same state
- Early convergence detection when all pending reconciles are known no-ops

---

## Proposed Heuristics

### 1. Subtree Convergence Circuit Breaker (Enhanced)

The existing `divergenceCircuitBreakerThreshold` only triggers when paths from a *divergence point* (stale-read branch) converge to the same state.

**Enhancement**: Generalize to any branching point. When `expandStateByReconcileOrder` creates ordering variants:
- Track the `stateKey` (logical state before branching) as a "branching point"
- After exploring N orderings from that branching point → same converged state, stop exploring remaining orderings

```go
// Track: branchingPoint → set of converged StateHashes
convergencesByBranchPoint := make(map[StateHash]map[StateHash]int)

// At convergence, record: convergencesByBranchPoint[parent.branchingOrigin][stateKey]++
// At expansion, check: if convergencesByBranchPoint[stateKey] has 1 element with count >= threshold, skip
```

**Why it helps**: If the first 3 orderings of `[RevisionReconciler, KPA, ServerlessService]` all converge to the same state, the remaining 3 permutations probably will too.

---

### 2. Mutation Signature Deduplication (Fix the TODO) ✅ IMPLEMENTED

There was a disabled/broken optimization in `explore.go` that would skip states with the same `(stateHash, normalizedHistory)` pair. The issue was that it skipped based on "seen before" rather than "completed exploration".

**Problem**: If path A enqueues state S with history H, and path B later produces the same (S, H) before A has completed, we'd skip B. If A then fails (error, max depth), we've lost our chance to explore that path to convergence.

**Fix implemented**: Track *completion status* rather than just "seen". Only skip if we've COMPLETED exploration (reached convergence or max depth abort).

```go
// Track completion status for (stateHash, history) pairs.
completedPaths := make(map[string]bool)

// At convergence: mark as completed
completionKey := fmt.Sprintf("%s|%s", stateKey, currentState.ExecutionHistory.UniqueKey())
completedPaths[completionKey] = true

// At max depth abort: mark as completed
completionKey := fmt.Sprintf("%s|%s", stateKey, newState.ExecutionHistory.UniqueKey())
completedPaths[completionKey] = true

// Skip check: only skip if COMPLETED
if completed := completedPaths[completionKey]; completed {
    // Safe to skip - we've already fully explored this path
    continue
}
```

**Note**: Error aborts are intentionally NOT marked as completed, since the error might be path-specific (e.g., from a particular stale view) and a different path might succeed.

---

### 3. Pending Queue Signature Heuristic

**Observation**: If two states have the same `ObjectsHash` and the same *set* of pending reconciles (order-insensitive), they will explore the same subtree structure—just in different orders.

**Heuristic**: Track `(objectsHash, sortedPendingSet)` tuples. After exploring ONE ordering to convergence without finding divergence, mark the subtree as "order-insensitive" and skip other orderings.

```go
// Key: objectsHash + sorted pending reconcilers (not full PendingReconcile)
orderInsensitiveSubtrees := make(map[string]StateHash) // maps to converged state

key := fmt.Sprintf("%s|%s", state.ObjectsHash(), sortedReconcilerIDs(state.PendingReconciles))
if convergedState, found := orderInsensitiveSubtrees[key]; found {
    // All orderings converge to the same state; skip
}
```

**Key insight**: Order determines traversal path, not the set of reachable states. If permuting order doesn't change the converged state, we can skip redundant orderings.

---

### 4. No-Op Chain Detection

**Observation**: Long chains of no-ops don't change state but multiply the exploration space. If a reconcile is a no-op AND doesn't trigger new reconciles, its ordering relative to other no-ops is irrelevant.

**Heuristic**: When ALL pending reconciles are known no-ops for the current objects hash, collapse the entire subtree into a single "drain all no-ops" step.

```go
if e.allPendingAreKnownNoOps(state, knownNoOps) {
    // Execute all in arbitrary order, skip to final state
    finalState := e.drainAllNoOps(state)
    // Continue exploration from finalState
}
```

This is a more aggressive version of the existing early convergence optimization.

---

### 5. Effect-Based Interest Score

Not all reconciles are equally "interesting" for ordering sensitivity:
- **High interest**: Spec changes, Generation increments, resource creation/deletion
- **Low interest**: Status-only updates, no-ops, requeues

**Heuristic**: Assign an "interest score" to each branch based on the effects produced. Prioritize high-interest branches; circuit-break low-interest subtrees earlier.

```go
type subtreeInterest struct {
    specChanges    int
    statusOnly     int
    noOps          int
}

// After exploring a subtree, if specChanges == 0 and noOps > threshold,
// mark similar subtrees as low-priority
```

---

### 6. Depth-Adaptive Branching Factor

At low depths, the branching factor from ordering permutations is manageable. At high depths, it compounds exponentially.

**Heuristic**: Reduce the branching factor as depth increases:
- Depth 1-20: Full permutation exploration
- Depth 21-40: Only explore first 3 orderings; if they converge identically, skip rest
- Depth 41+: Only explore 1 ordering (assume order-insensitive)

```go
func (e *Explorer) maxOrderingsAtDepth(depth int) int {
    switch {
    case depth <= 20: return math.MaxInt
    case depth <= 40: return 3
    default:          return 1
    }
}
```

**Trade-off**: May miss deep ordering sensitivities, but allows broader exploration at shallower depths.

---

### 7. Reconciler "Stability" Tracking

Track per-reconciler statistics over the course of exploration:
- How often does it produce no-ops?
- How often does it produce spec changes?
- Does it ever cause divergence when reordered?

**Heuristic**: If a reconciler has *never* caused divergence across 100+ branches, deprioritize branches that only differ in its ordering.

```go
type reconcilerStats struct {
    totalRuns        int
    noOpRuns         int
    causedDivergence bool  // ever produced different converged state when reordered
}

// If !causedDivergence && noOpRate > 0.8, treat as "stable"
// Skip branches that only reorder stable reconcilers
```

---

### 8. BFS/DFS Hybrid with Frontier Sampling

Instead of pure DFS, use BFS to a certain depth to understand the state space "shape", then DFS into promising branches.

**Heuristic**:
1. BFS to depth 10 to enumerate distinct logical states
2. Identify states with high pending diversity (potential for ordering sensitivity)
3. DFS from those states, deprioritizing states that match already-explored patterns

**Benefit**: Prevents getting "stuck" in one deep subtree; gives visibility into overall state space structure.

---

### 9. Logical State Deduplication at Enqueue Time

Currently, deduplication happens after visiting. But we could deduplicate *at enqueue time* more aggressively.

**Key insight**: The subtree below a state depends only on:
1. `ObjectsHash` (current objects)
2. The *set* of pending reconciles (not order—order just affects traversal)
3. Which reconcile we execute first

```go
// subtreeKey = objectsHash + sortedPendingSet + firstReconciler
subtreeKey := fmt.Sprintf("%s|%s|%s",
    state.ObjectsHash(),
    sortedPendingSignature(state.PendingReconciles),
    state.PendingReconciles[0].ReconcilerID,
)

if _, explored := committedSubtrees[subtreeKey]; explored {
    continue // don't enqueue
}
committedSubtrees[subtreeKey] = struct{}{}
queue = append(queue, state)
```

---

### 10. Progressive Deepening with Divergence Detection

Instead of one deep DFS, do iterative deepening:
1. Explore to depth 20, record all converged states
2. If only 1 unique converged state, increase depth limit for branches that *didn't* converge
3. Repeat, focusing exploration on "unresolved" branches

**Benefit**: Surfaces divergences at any depth, rather than getting stuck in deep subtrees. Also provides early feedback about convergence behavior.

---

## Recommended Implementation Order

Based on the current scenario (only permuting RevisionReconciler, high no-op rate, repeated convergence to same state):

1. **Heuristic #3 (Pending Queue Signature)** — quick to implement; directly addresses "same set of pending, different order → same outcome"

2. **Heuristic #1 (Enhanced Circuit Breaker)** — generalizes existing mechanism to all branching points

3. **Heuristic #6 (Depth-Adaptive Branching)** — simple knob to prevent deep subtree explosion

4. **Heuristic #2 (Fix the TODO)** — enables existing but disabled optimization

5. **Heuristic #4 (No-Op Chain Detection)** — handles chains of no-ops more efficiently

---

## Success Metrics

To evaluate whether a heuristic is working:

1. **Coverage at depth**: Are we reaching higher depths within the same timeout?
2. **Unique converged states found**: Are we finding more distinct outcomes?
3. **Time distribution**: Is time more evenly spread across depths?
4. **Divergence detection**: If we inject a known ordering-sensitive bug, do we still find it?

Use `--emit-stats` to track `VisitsByDepth` before and after changes.
