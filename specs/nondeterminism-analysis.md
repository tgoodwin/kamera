# Non-Determinism Analysis: How Map Iteration Affects Unique State Count

## Root Cause Identified (2026-01-13)

**The non-determinism is in the Knative KPA reconciler**, not in tracecheck infrastructure.

### Evidence from Diagnostic Runs

Three trials with depth=100 produced different results:
- Trial 1: 3 converged states, 434 reconcile steps
- Trial 2: 3 converged states, 430 reconcile steps
- Trial 3: **2 converged states**, 418 reconcile steps

### Pinpointed Divergence Point

At depth 47, the KPA reconciler produces different effects:

**Trial 1**:
```json
"effectsOrder": ["UPDATE:{ServerlessService}", "UPDATE:{PodAutoscaler}"]  // 2 effects
```

**Trial 3**:
```json
"effectsOrder": ["UPDATE:{PodAutoscaler}"]  // 1 effect only!
```

The KPA reconciler is making **non-deterministic decisions** about whether to update the ServerlessService resource. This difference cascades through exploration, causing different final states.

### Likely Cause in Knative Code

The KPA reconciler in `knative.dev/serving/pkg/reconciler/autoscaling/kpa` likely iterates over internal maps when deciding which resources need updates. Go's random hash seed causes different iteration orders, which affects the reconciler's decision logic.

---

## Original Analysis (preserved below)

## The Puzzle

**Observation**:
- Different program runs produce different numbers of unique states (73 vs 145)
- Back-to-back runs in the same session produce identical results
- `trigger.go:getTriggered()` sorts its output, so its result should be deterministic

**Question**: How can non-deterministic map iteration affect the NUMBER of unique logical states explored?

## Hypothesis 1: Exploration Order Affects Optimization Pruning

Even if all individual operations are eventually deterministic, the ORDER in which states are explored affects which optimizations fire first:

```
Scenario A (explores state X before Y):
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Explore state X                                         │
│         → Cache: "When you see inputs I, output is O"           │
│         → Mark logical state L as explored                      │
│                                                                 │
│ Step 2: Encounter state Y (which would produce same output O)   │
│         → Cache prediction fires! Skip Y                        │
│         → States explored: {X}                                  │
└─────────────────────────────────────────────────────────────────┘

Scenario B (explores state Y before X):
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: Explore state Y                                         │
│         → Cache: "When you see inputs I, output is O"           │
│         → Mark logical state L' as explored                     │
│                                                                 │
│ Step 2: Encounter state X                                       │
│         → Cache prediction fires! Skip X                        │
│         → States explored: {Y}                                  │
│                                                                 │
│ But Y leads to different subsequent states than X would have!   │
│ → Different exploration tree → Different unique state count     │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight**: The optimizations are correct (they skip equivalent work), but WHICH equivalent path they keep depends on exploration order.

## Hypothesis 2: Stack Order Affects DFS Traversal

The exploration uses a stack (DFS). The order in which states are pushed affects which ones get popped first:

```
If pending reconciles are [A, B, C] and we generate orderings:
  - A-first variant
  - B-first variant
  - C-first variant

The order these are pushed to the stack determines exploration order.
If there's any non-determinism in HOW these variants are ordered before pushing,
the DFS explores different subtrees first.
```

## Hypothesis 3: Reconciler Execution Produces Different Effect Orders

The Knative reconcilers might internally iterate over maps when:
1. Deciding which status fields to update
2. Creating owner references
3. Generating events

If `Changes.Effects` has different orderings:
- The triggers computed from effects are the same SET but different order
- Even after sorting, the exploration ORDER of triggered reconciles varies
- Different exploration order → different optimization firings

## Where to Look in the Code

### 1. Effect Generation in Reconcilers

Check Knative reconciler code for map iterations when building status updates:
```go
// Example of problematic pattern
for key := range someMap {
    effects = append(effects, makeEffect(key))
}
// Effects slice has non-deterministic order!
```

### 2. Stack Push Order

Check `enqueueStates` and `enqueueWithMarker` for whether variants are pushed in deterministic order.

### 3. Hash-Based State Identity

Check if Go's hash seed affects which states are considered "equal" when using hash-based deduplication.

## Verification Strategy

To confirm which hypothesis is correct:

1. **Add logging**: Log the order of `Changes.Effects` after each reconcile step
2. **Sort effects**: Add sorting to effect slices before processing
3. **Instrument pruning**: Log when each optimization fires and what it prunes

## Why Back-to-Back Runs Are Deterministic

Go's map hash seed is set at program startup. Within a single program invocation:
- All maps use the same seed
- Iteration order is consistent (though unpredictable)

Between invocations:
- New seed → different iteration order → different exploration tree

## Proposed Fix

If the root cause is in `Changes.Effects` ordering, the fix is to sort effects before processing:

```go
// In getTriggered or wherever effects are processed
sort.Slice(changes.Effects, func(i, j int) bool {
    // Sort by (OpType, Group, Kind, Namespace, Name)
    ...
})
```

If the root cause is in stack push order, ensure variants are sorted before pushing.

## Impact on Correctness

The non-determinism doesn't affect CORRECTNESS:
- All explored paths are valid
- All skipped paths are legitimately redundant (equivalent to an explored path)

But it affects REPRODUCIBILITY:
- Can't compare benchmark runs across invocations
- Hard to debug exploration behavior
