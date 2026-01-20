# Ordering Pruning Optimization

## Overview

The **Ordering Pruning** optimization prevents redundant expansion of reconciler orderings when a state has already been branched on. When multiple reconciles are pending, the explorer must consider different execution orders. However, expanding the same logical state multiple times would generate duplicate branches. This optimization tracks which states have already been expanded and skips re-expansion.

## Background: Order Permutation Branching

When a reconcile step completes, it may trigger multiple new reconciles. The order in which these execute can affect the final outcome:

```
                    State S
                    pending=[A, B, C]
                         │
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
  Run A first       Run B first       Run C first
  pending=[B,C]     pending=[A,C]     pending=[A,B]
       │                 │                 │
      ...               ...               ...
       │                 │                 │
       ▼                 ▼                 ▼
  Converged X       Converged Y       Converged Z
  (may differ!)     (may differ!)     (may differ!)
```

The explorer generates these ordering variants using `expandStateByReconcileOrder()`.

## The Problem: Duplicate Expansions

Without optimization, the same logical state can be reached via different paths and re-expanded each time:

```
                      Initial
                     /       \
                Path A        Path B
                  │             │
                  ▼             ▼
             State S₁       State S₂
             (objects=X,    (objects=X,
              pending=[A,B]) pending=[A,B])
                  │             │
            ┌─────┴─────┐  ┌────┴─────┐
            ▼           ▼  ▼          ▼
        [A first]  [B first] [A first] [B first]
            │           │      │          │
           ...         ...    ...        ...

    Problem: We expand both S₁ and S₂ to generate [A first] and [B first],
    but S₁ and S₂ have the same logical state, so this is redundant!
```

## The Solution: Track Expanded States

We track which logical states have already been expanded for ordering permutations:

```go
// Key components:
// 1. State hash (NodeHash) - identifies the state
// 2. Permute signature - identifies which reconcilers are being permuted

branchKey := stateHash + "|" + permuteSignature(triggeredByStep, permuteOrder)

if seenBranchingByState[branchKey] {
    // Skip expansion - already generated these orderings
} else {
    // Generate ordering variants
    expandedStates := expandStateByReconcileOrder(newState, triggeredByStep)
    seenBranchingByState[branchKey] = true
}
```

## The Permute Signature

The permute signature captures **which reconcilers are being permuted**, not all pending reconciles:

```go
func (o *optimizations) permuteSignature(triggered []PendingReconcile, permuteOrder map[ReconcilerID]bool) string {
    // Only include reconcilers that:
    // 1. Were triggered by the last state change
    // 2. Are enabled for permutation in permuteOrder config

    ids := []string{}
    for _, pr := range triggered {
        if permuteOrder[pr.ReconcilerID] {
            ids = append(ids, string(pr.ReconcilerID))
        }
    }
    sort.Strings(ids)  // Order-insensitive (set semantics)
    return strings.Join(ids, ",")
}
```

### Why Include Permute Signature?

```
Consider two different arrivals at State S:

Arrival 1:
    Prior step triggered [A, B]
    PermuteOrder = {A: true, B: true, C: false}
    → Expands orderings: A-first, B-first
    branchKey = "S|A,B"

Arrival 2:
    Prior step triggered [A, C]
    PermuteOrder = {A: true, B: true, C: false}
    → Would expand orderings: A-first only (C not permutable)
    branchKey = "S|A"

These are DIFFERENT keys, so both expansions occur.
This is correct: different triggering contexts may need different orderings.
```

## Implementation Details

### Data Structures

```go
type optimizations struct {
    // Track which (state, permuteSignature) pairs have been expanded
    seenBranchingByState map[string]bool
}
```

### Key Operations

```go
// Check if we've already expanded this state for these triggered reconcilers
func (o *optimizations) branchAlreadyExpanded(
    stateHash NodeHash,
    triggered []PendingReconcile,
    permuteOrder map[ReconcilerID]bool,
) bool {
    key := o.branchKey(stateHash, triggered, permuteOrder)
    return o.seenBranchingByState[key]
}

// Mark that we've expanded this state
func (o *optimizations) markBranchExpanded(
    stateHash NodeHash,
    triggered []PendingReconcile,
    permuteOrder map[ReconcilerID]bool,
) {
    key := o.branchKey(stateHash, triggered, permuteOrder)
    o.seenBranchingByState[key] = true
}
```

### Usage in Main Loop

```go
if len(newState.PendingReconciles) > 1 {
    alreadyExpanded := e.optimizations.branchAlreadyExpanded(
        branchStateKey, triggeredByStep, e.Config.PermuteOrder)

    if !alreadyExpanded {
        // Generate ordering variants
        expandedStates := e.expandStateByReconcileOrder(newState, triggeredByStep)
        for _, orderVariant := range expandedStates {
            statesToEnqueue = append(statesToEnqueue, orderVariant)
        }
        e.optimizations.markBranchExpanded(branchStateKey, triggeredByStep, e.Config.PermuteOrder)
    } else {
        e.stats.SkippedOrderExpansions++
    }
}
```

## No-Op Ordering Skip (Sub-optimization)

When ordering pruning is enabled, an additional heuristic skips orderings whose first reconcile is a known no-op:

```
State S: pending=[A, B, C]

If we know A is a no-op on this state:
    Ordering [A, B, C] will produce same result as [B, A, C] or [B, C, A]

    A-first ordering:
        Run A → no changes
        State unchanged, pending=[B, C]

    B-first ordering:
        Run B → may change state
        Then explore remaining...

Since A-first just delays real work, we can skip A-first orderings.
```

### Implementation

```go
// In the ordering expansion loop
if e.optimizations.noOpOrderingSkipEnabled() {
    fst := orderVariant.PendingReconciles[0]
    noOpKey := fmt.Sprintf("%s:%s:%s",
        orderVariant.ContentsHash(), fst.ReconcilerID, fst.Request.NamespacedName.String())

    if isNoOp, known := e.optimizations.isKnownNoOp(noOpKey); known && isNoOp {
        e.stats.SkippedNoOpOrderings++
        continue  // Skip this ordering variant
    }
}
```

### Disabling No-Op Skip

The `DisableNoOpOrderingSkip` config option allows disabling this sub-optimization while keeping ordering pruning enabled:

```go
cfg.Optimizations.OrderingPruning = true
cfg.Optimizations.DisableNoOpOrderingSkip = true  // Keep pruning, disable no-op skip
```

## Visual Example

```
                         Initial State
                              │
                         Step produces
                         changes, triggers
                         [ConfigReconciler, PodReconciler]
                              │
                              ▼
                         State S
                         pending=[ConfigReconciler, PodReconciler, OtherController]
                              │
                              │ PermuteOrder = {ConfigReconciler: true, PodReconciler: true}
                              │
              ┌───────────────┼───────────────┐
              ▼               │               ▼
        ConfigReconciler      │         PodReconciler
        first                 │         first
              │               │               │
             ...              │              ...
              │               │               │
              ▼               │               ▼
         Converged A          │          Converged B
                              │
                              │ (OtherController not in PermuteOrder,
                              │  so no OtherController-first variant)
                              │
    ─────────────────────────────────────────────────────────────

    Later, different path reaches State S:

                         Different Path
                              │
                         Same State S
                         pending=[ConfigReconciler, PodReconciler, OtherController]
                              │
                              │ Check: branchKey = "S|ConfigReconciler,PodReconciler"
                              │ Already expanded? YES
                              │
                              ▼
                         SKIP EXPANSION
                         (just enqueue base state)
```

## OnlyPermuteTriggered Configuration

The `OnlyPermuteTriggered` config option controls the scope of permutation:

```go
type OptimizationConfig struct {
    // ...

    // OnlyPermuteTriggered limits order permutations to reconcilers
    // triggered by the last step.
    // When true: Only permute among triggered reconcilers
    // When false: Can permute any pending reconciler to first position
    OnlyPermuteTriggered bool
}
```

```
OnlyPermuteTriggered = true:
─────────────────────────────
    State after step: pending=[A, B, C]
    Step triggered: [A, B]

    Only generate: A-first, B-first variants
    NOT: C-first variant (C wasn't triggered by this step)

OnlyPermuteTriggered = false:
─────────────────────────────
    State after step: pending=[A, B, C]
    Step triggered: [A, B]

    Generate: A-first, B-first, C-first variants
    (All pending reconcilers eligible for first position)
```

## Soundness Considerations

### When Ordering Pruning Is Safe

The optimization is **sound** when:
1. The same logical state produces the same ordering variants
2. Ordering variants are independent of how we arrived at the state
3. The PermuteOrder config is consistent throughout exploration

### Potential Soundness Gaps

```
Gap: Triggered-only expansion
─────────────────────────────

If OnlyPermuteTriggered is true, we only permute triggered reconcilers.

Arrival 1 at State S:
    Triggered by prior step: [A, B]
    Generates: A-first, B-first

Arrival 2 at State S:
    Triggered by prior step: [C]
    Would generate: C-first
    But ordering pruning sees same stateKey, skips!

Result: C-first ordering never explored.

Mitigation: branchKey includes permuteSignature, which captures
the triggered set. Different triggers = different keys.
```

```
Gap: No-op ordering skip
────────────────────────

If A is marked as no-op, we skip A-first orderings.

But if A's no-op status was determined on a DIFFERENT state:
    State X: A is no-op
    State S: A might NOT be no-op

    If we skip A-first on State S based on State X's result,
    we might miss valid orderings.

Mitigation: No-op tracking is keyed by objectsHash + reconciler + request.
Different states = different no-op lookups.
```

## Statistics

The explorer tracks:
- `SkippedOrderExpansions`: States where ordering expansion was skipped
- `SkippedNoOpOrderings`: Individual ordering variants skipped due to no-op-first

## Configuration

```go
cfg.Optimizations.OrderingPruning = true           // Main toggle
cfg.Optimizations.OnlyPermuteTriggered = true      // Limit to triggered reconcilers
cfg.Optimizations.DisableNoOpOrderingSkip = false  // Keep no-op skip enabled
```

## Relationship to Other Optimizations

```
┌─────────────────────────────────────────────────────────────────────┐
│              Ordering Pruning in the Optimization Stack             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Ordering Pruning                                                   │
│  └── Prevents duplicate ordering expansions at same logical state  │
│      └── Uses: seenBranchingByState map                            │
│                                                                     │
│  No-Op Ordering Skip (sub-optimization)                             │
│  └── Skips orderings where first reconcile is known no-op          │
│      └── Uses: knownNoOps map (shared with Early Convergence)      │
│                                                                     │
│  Subtree Completion                                                 │
│  └── Skips entire subtrees once fully explored                     │
│      └── More aggressive, subsumes some ordering pruning benefit   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```
