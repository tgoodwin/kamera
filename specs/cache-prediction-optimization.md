# Cache Prediction Optimization

## Overview

The **Cache Prediction** optimization uses memoized reconcile results to predict the output of a reconcile step without actually executing it. If we can predict that a reconcile would produce a state we've already committed to exploring, we skip executing it entirely.

## Background: Reconcile Determinism

Kubernetes reconcilers are designed to be deterministic given the same input state. This means:

```
Input: (Objects state, Reconciler, Request)
           ↓
       Reconciler executes
           ↓
Output: (New objects state, Triggered reconciles, Was no-op?)
```

If we've seen the same input before, we know the output without re-running the reconciler.

## The Two-Part Strategy

### Part 1: Cache Reconcile Results

When a reconcile completes, we cache the result:

```go
type cachedReconcileResult struct {
    outputObjectsHash   ContentsHash       // hash of output objects
    wasNoOp             bool               // did it produce changes?
    numEffects          int                // number of effects (for history signature)
    triggeredReconciles []PendingReconcile // reconciles triggered by the changes
}

// Cache key: objectsHash:reconcilerID:namespace/name
e.optimizations.setReconcileResult(reconcileResKey, &cachedReconcileResult{...})
```

### Part 2: Predict and Skip

Before executing a reconcile, check if we can predict the output:

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Cache Prediction Flow                                │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  1. Build cache key from current state                                 │
│     key = objectsHash + ":" + reconcilerID + ":" + request             │
│                                                                        │
│  2. Look up cached result                                              │
│     cached, found = reconcileResCache[key]                             │
│     If not found → execute reconcile normally                          │
│                                                                        │
│  3. Predict output state                                               │
│     - predictedObjectsHash = cached.outputObjectsHash                  │
│     - predictedPending = (current pending - this reconcile) + cached.triggered  │
│     - predictedHistory = currentHistory + stepSignature                │
│                                                                        │
│  4. Check if output already committed for exploration                  │
│     If hasLogicalState(predictedObjectsHash, predictedPending, predictedHistory):  │
│         SKIP this reconcile (CachePredictedSkips++)                    │
│     Else:                                                              │
│         Execute reconcile normally                                     │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

## Visual Example

```
                    ┌──────────────────┐
                    │   State A        │
                    │ objects=O        │
                    │ pending=[R1,R2]  │
                    │ history=H1       │
                    └────────┬─────────┘
                             │
                             │ Run R1
                             ▼
        ┌────────────────────────────────────────┐
        │            Reconcile R1                 │
        │                                        │
        │  Input: (O, R1, request)               │
        │  Output: O', triggered=[R3]            │
        │                                        │
        │  Cache: O:R1:req → {O', [R3], 2 effects}  │
        └────────────────────────────────────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   State B        │
                    │ objects=O'       │
                    │ pending=[R2,R3]  │
                    │ history=H1+R1@2  │
                    └──────────────────┘
                             │
            Mark as explored: (O', [R2,R3], H1+R1@2)

═══════════════════════════════════════════════════════════════════════

Later, different path reaches State A:

                    ┌──────────────────┐
                    │   State A        │
                    │ objects=O        │
                    │ pending=[R1,R2]  │
                    │ history=H2       │  ← Different history!
                    └────────┬─────────┘
                             │
                             │ About to run R1
                             │
        ┌────────────────────────────────────────┐
        │         Cache Prediction               │
        │                                        │
        │  Cache lookup: O:R1:req → found!       │
        │  predicted output = O'                 │
        │  predicted pending = [R2,R3]           │
        │  predicted history = H2+R1@2           │
        │                                        │
        │  Check: (O', [R2,R3], H2+R1@2) seen?   │
        │         → NO (different history)       │
        │                                        │
        │  Result: Execute R1 normally           │
        └────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════

Another path reaches State A with SAME effective history:

                    ┌──────────────────┐
                    │   State A        │
                    │ objects=O        │
                    │ pending=[R1,R2]  │
                    │ history=H1       │  ← Same effective history
                    └────────┬─────────┘
                             │
                             │ About to run R1
                             │
        ┌────────────────────────────────────────┐
        │         Cache Prediction               │
        │                                        │
        │  Cache lookup: O:R1:req → found!       │
        │  predicted output = O'                 │
        │  predicted pending = [R2,R3]           │
        │  predicted history = H1+R1@2           │
        │                                        │
        │  Check: (O', [R2,R3], H1+R1@2) seen?   │
        │         → YES! Already exploring       │
        │                                        │
        │  Result: SKIP (CachePredictedSkips++)  │
        └────────────────────────────────────────┘
```

## The Logical State Key

Cache prediction tracks which logical states have been committed for exploration:

```go
func (o *optimizations) logicalStateKey(
    objectsHash ContentsHash,
    pending []PendingReconcile,
    historyKey string,
    stuckKey string,
) string {
    return fmt.Sprintf("%s|%s|%s|%s",
        objectsHash,
        o.pendingSignature(pending),  // order-sensitive
        stuckKey,                      // stuck reconciler positions
        historyKey)                    // effective history
}
```

The key includes:
1. **objectsHash**: The resource contents
2. **pending signature**: Order-sensitive list of pending reconciles
3. **stuckKey**: Positions of "stuck" reconcilers (for stale-read scenarios)
4. **historyKey**: The normalized execution history

## Predicting the History Signature

When predicting the output history, we need to account for whether the cached result was a no-op:

```go
// Predict the history signature after this step
currentHistory := stateView.ExecutionHistory.UniqueKey()
var predictedHistory string
if cached.wasNoOp {
    // No-ops don't change the history signature
    predictedHistory = currentHistory
} else {
    // Add this step to the history
    stepSig := fmt.Sprintf("%s@%d", pendingReconcile.ReconcilerID, cached.numEffects)
    if currentHistory == "" {
        predictedHistory = stepSig
    } else {
        predictedHistory = fmt.Sprintf("%s,%s", currentHistory, stepSig)
    }
}
```

## Implementation Details

### Data Structures

```go
type optimizations struct {
    // Cache of reconcile results: key → output prediction
    reconcileResCache map[string]*cachedReconcileResult

    // Set of logical states we've committed to exploring
    exploredLogicalStates map[string]struct{}
}
```

### Key Operations

```go
// Record a reconcile result for future prediction
func (o *optimizations) setReconcileResult(cacheKey string, res *cachedReconcileResult) {
    if !o.cachePredictionEnabled() {
        return
    }
    o.reconcileResCache[cacheKey] = res
}

// Look up a cached result
func (o *optimizations) getReconcileResult(cacheKey string) (*cachedReconcileResult, bool) {
    if !o.cachePredictionEnabled() {
        return nil, false
    }
    res, ok := o.reconcileResCache[cacheKey]
    return res, ok
}

// Mark a logical state as committed for exploration
func (o *optimizations) markLogicalState(
    objectsHash ContentsHash,
    pending []PendingReconcile,
    historyKey string,
    stuckKey string,
) {
    key := o.logicalStateKey(objectsHash, pending, historyKey, stuckKey)
    o.exploredLogicalStates[key] = struct{}{}
}

// Check if a logical state is already being explored
func (o *optimizations) hasLogicalState(
    objectsHash ContentsHash,
    pending []PendingReconcile,
    historyKey string,
    stuckKey string,
) bool {
    key := o.logicalStateKey(objectsHash, pending, historyKey, stuckKey)
    _, explored := o.exploredLogicalStates[key]
    return explored
}
```

### Usage in Main Loop

```go
// Before running a reconcile
reconcileResKey := fmt.Sprintf("%s:%s:%s",
    stateView.ContentsHash(), reconcilerID, pendingReconcile.Request.NamespacedName.String())

if e.skipViaCachePrediction(reconcileResKey, stateView, pendingReconcile) {
    e.stats.CachePredictedSkips++
    continue
}

// ... execute reconcile ...

// After reconcile completes, cache the result
e.optimizations.setReconcileResult(reconcileResKey, &cachedReconcileResult{
    outputObjectsHash:   newState.ContentsHash(),
    wasNoOp:             wasNoOp,
    numEffects:          len(stepResult.Changes.Effects),
    triggeredReconciles: triggeredByStep,
})

// Mark the output state as committed for exploration
e.optimizations.markLogicalState(
    newState.ContentsHash(),
    newState.PendingReconciles,
    normalizedHistory,
    newState.stuckPositionsSignature())
```

## Cache Key Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                       Cache Key Components                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Reconcile Cache Key (for result lookup):                           │
│  ─────────────────────────────────────────                          │
│  objectsHash:reconcilerID:namespace/name                            │
│       │           │              │                                  │
│       │           │              └─ The specific request            │
│       │           └─ Which reconciler                               │
│       └─ The observed state (NOT including pending list)            │
│                                                                     │
│  Note: The pending list is NOT part of the cache key because        │
│  reconcile behavior depends only on objects, not on what other      │
│  reconciles are pending.                                            │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Logical State Key (for skip decision):                             │
│  ─────────────────────────────────────                              │
│  objectsHash|pendingSignature|stuckKey|historyKey                   │
│       │           │             │          │                        │
│       │           │             │          └─ Effective history     │
│       │           │             └─ Stuck reconciler positions       │
│       │           └─ Full pending list (order matters)              │
│       └─ The resource contents                                      │
│                                                                     │
│  This captures EVERYTHING that affects future exploration.          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Predicting New Pending List

```go
// Current pending minus the reconcile we're about to run
predictedPending := lo.Filter(stateView.PendingReconciles, func(pr PendingReconcile, _ int) bool {
    return pr != pendingReconcile
})

// Plus any reconciles triggered by the cached result
predictedPending = e.getNewPendingReconciles(predictedPending, cached.triggeredReconciles)
```

This mirrors what `determineNewPendingReconciles` does after an actual reconcile.

## Soundness Considerations

### When It's Safe

The optimization is **sound** when:
1. **Determinism**: Same (objects, reconciler, request) always produces same result
2. **Complete caching**: All factors affecting output are captured in the cache key
3. **Accurate prediction**: We correctly compute predicted pending and history

### Potential Gaps

```
Gap: Non-deterministic reconcilers
─────────────────────────────────

If a reconciler's output depends on external factors (time, random, external state):
    Cache might return wrong prediction

Mitigation: Kubernetes reconcilers should be deterministic by design.
           External calls are mocked/stubbed in the explorer.
```

```
Gap: Stale read effects
───────────────────────

If a reconciler observes a stale view of the world:
    The cache key (objectsHash) reflects the TRUE state
    But the reconciler saw a DIFFERENT (stale) state

Mitigation: Stale views are handled separately (getPossibleViewsForReconcile).
           Each stale view gets its own cache entries.
```

## Statistics

The explorer tracks:
- `CachePredictedSkips`: Reconciles skipped via cache prediction

## Configuration

Enable with:
```go
cfg.Optimizations.CachePrediction = true
```

## Relationship to Other Optimizations

```
┌─────────────────────────────────────────────────────────────────────┐
│                 Cache Prediction in Context                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Early Convergence                                                  │
│  └── Skips states where ALL pending are known no-ops                │
│      └── Uses: knownNoOps map                                       │
│                                                                     │
│  Cache Prediction (THIS)                                            │
│  └── Skips INDIVIDUAL reconciles when output already queued         │
│      └── Uses: reconcileResCache + exploredLogicalStates            │
│      └── More surgical: skips one reconcile at a time               │
│                                                                     │
│  Subtree Completion                                                 │
│  └── Skips ENTIRE subtrees once fully explored                      │
│      └── Uses: subtreeTracker with stack markers                    │
│      └── Most aggressive: ignores history entirely                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Cache Prediction is complementary to other optimizations:
- **More targeted** than Early Convergence (works on individual reconciles, not just no-op states)
- **Less aggressive** than Subtree Completion (still respects history differences)
- **Synergistic** with Ordering Pruning (reduces work within each ordering variant)

## Example Scenario

```
Scenario: ConfigMap controller and Deployment controller both triggered

Step 1: State S, pending=[ConfigMapReconciler, DeploymentReconciler]
        Run ConfigMapReconciler
        → Updates ConfigMap, triggers [PodReconciler]
        Cache: S:ConfigMapReconciler:cm/config → {S', [PodReconciler], 1 effect}
        New state S' with pending=[DeploymentReconciler, PodReconciler]
        Mark explored: (S', [DeploymentReconciler, PodReconciler], history="ConfigMap@1")

Step 2: Different ordering at State S
        pending=[DeploymentReconciler, ConfigMapReconciler]
        Run DeploymentReconciler
        → No changes (ConfigMap not ready yet)
        New state S with pending=[ConfigMapReconciler]

Step 3: About to run ConfigMapReconciler
        Cache lookup: S:ConfigMapReconciler:cm/config → found!
        Predicted output: S'
        Predicted pending: [PodReconciler] (DeploymentReconciler finished)
        Wait - this is different from Step 1!
        Check hasLogicalState(S', [PodReconciler], history="Deployment@0,ConfigMap@1") → NO

        Execute normally (different pending list after)

Step 4: Another path reaches exact same state as Step 1
        State S, pending=[ConfigMapReconciler, DeploymentReconciler], same history
        About to run ConfigMapReconciler
        Cache lookup: S:ConfigMapReconciler:cm/config → found!
        Predicted: (S', [DeploymentReconciler, PodReconciler], history="ConfigMap@1")
        Check hasLogicalState(...) → YES! Already exploring

        SKIP via Cache Prediction ✓
```
