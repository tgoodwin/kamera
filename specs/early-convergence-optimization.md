# Early Convergence Optimization

## Overview

The **Early Convergence** optimization allows the explorer to skip processing states where all pending reconciles are known no-ops and we've already found a converged path to the equivalent logical state. This avoids redundantly executing reconciles that cannot produce new outcomes.

## Background: No-Op Reconciles

In Kubernetes controller exploration, a "no-op" reconcile is one that:
- Reads the current state
- Determines no changes are needed
- Returns without making any mutations

No-op reconciles are common because:
1. Controllers often check if work is already done before acting
2. Multiple controllers may watch the same resources but only one takes action
3. Controllers may be triggered by events they don't care about

```
Example: Two paths reach the same state with pending=[Foo, Bar]

Path A                              Path B
   │                                   │
   ▼                                   ▼
State S (pending=[Foo, Bar])        State S (pending=[Foo, Bar])
   │                                   │
   ├─ Run Foo (no-op) ──────────────► Same logical state
   │                                   │
   ├─ Run Bar (no-op) ──────────────► Same logical state
   │                                   │
   ▼                                   ▼
Converged                           Should skip (identical outcome)
```

## The Insight

If ALL pending reconciles for a state are known no-ops, then:
1. Running them in any order produces no state changes
2. The final converged state will be identical to the current logical state
3. Any path through this state will arrive at the same convergence

Therefore, if we've already recorded a converged path to this logical state, subsequent paths can skip the no-op execution entirely.

## How It Works

### Step 1: Track No-Op Results

When a reconcile completes, we record whether it was a no-op:

```go
// After executing a reconcile step
wasNoOp := err == nil && stepResult.wasNoOp()
e.optimizations.recordNoOp(reconcileResKey, wasNoOp)
```

The key is: `objectsHash:reconcilerID:namespace/name`

This captures: "For these specific objects, this reconciler on this request produces no changes."

### Step 2: Check at State Processing

When visiting a state with pending reconciles:

```
┌─────────────────────────────────────────────────────────────────┐
│                    State S Processing                            │
├─────────────────────────────────────────────────────────────────┤
│ 1. Compute objectsHash for current state                        │
│                                                                 │
│ 2. For EACH pending reconcile:                                  │
│    ├─ Build key: objectsHash:reconcilerID:request               │
│    └─ Check: Is this key in knownNoOps AND value is true?       │
│                                                                 │
│ 3. If ALL pending are known no-ops:                             │
│    ├─ Check: Have we already converged at this objectsHash?     │
│    └─ If YES: Skip this state (increment EarlyConvergence stat) │
│                                                                 │
│ 4. Otherwise: Continue normal processing                        │
└─────────────────────────────────────────────────────────────────┘
```

### Step 3: The Code Path

```go
// In explore() main loop
if e.optimizations.checkEarlyConvergence(currentState) {
    if _, alreadyConverged := seenConvergedStates[NodeHash(contentsKey)]; alreadyConverged {
        e.stats.EarlyConvergence++
        continue  // Skip this state
    }
}
```

The `checkEarlyConvergence` function:

```go
func (o *optimizations) checkEarlyConvergence(state StateNode) bool {
    objectsHash := state.ContentsHash()
    for _, pr := range state.PendingReconciles {
        noOpKey := fmt.Sprintf("%s:%s:%s", objectsHash, pr.ReconcilerID, pr.Request.NamespacedName.String())
        if isNoOp, known := o.knownNoOps[noOpKey]; !known || !isNoOp {
            return false  // At least one pending is not a known no-op
        }
    }
    return true  // ALL pending are known no-ops
}
```

## Visual Example

```
                        Initial State
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
      Run Foo first    Run Bar first    Run Baz first
      (mutates)        (no-op)          (no-op)
            │                │                │
            ▼                ▼                ▼
      State A          State B          State C
      pending=[Bar]    pending=[Foo,Baz] pending=[Foo,Bar]
            │                │                │
            │                │                │
      ... continues    ... continues    ... continues
            │                │                │
            ▼                ▼                ▼
      Converged X      Converged Y      Converged Z
```

Now consider if we reach State A again via a different path:

```
     Different Path
          │
          ▼
     State A (same objects as before)
     pending=[Bar]
          │
          │ ◄─── Bar is known no-op on these objects
          │ ◄─── Already converged to this objectsHash before
          │
          ▼
     SKIP (Early Convergence)
```

## Soundness Considerations

### When It's Safe

The optimization is **sound** when:
1. No-op status is deterministic: Same inputs always produce same no-op result
2. All pending are truly no-ops: If even one pending might mutate, we can't skip
3. We've already converged: Only skip if we know the outcome

### When It Might Be Unsound

The optimization could miss paths if:
- A reconcile is a no-op in isolation but affects subsequent reconciles through side-effects
- No-op status depends on factors not captured in objectsHash (e.g., external state, time)

In practice, Kubernetes reconcilers are designed to be idempotent and deterministic based on observed state, so these concerns rarely apply.

## Relationship to Other Optimizations

```
┌──────────────────────────────────────────────────────────────────┐
│                    Optimization Hierarchy                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Path Dedup         ──► Conservative: same (state,history) pair   │
│       │                                                          │
│       ▼                                                          │
│  Early Convergence  ──► More aggressive: all pending are no-ops  │
│       │                                                          │
│       ▼                                                          │
│  Subtree Completion ──► Most aggressive: ignore history entirely │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
```

Early Convergence is complementary to:
- **Path Dedup**: Path dedup skips based on exact (state, history) matches; early convergence skips based on predicted outcomes
- **Ordering Pruning**: Both use the knownNoOps map; ordering pruning skips no-op-first orderings
- **Cache Prediction**: Both predict outcomes without execution; cache prediction is more general

## Configuration

Enable with:
```go
cfg.Optimizations.EarlyConvergence = true
```

## Statistics

The explorer tracks:
- `EarlyConvergence`: Count of states skipped via this optimization
- `NoOpReconciles`: Count of reconciles that were no-ops (feeds the knownNoOps map)

## Example Scenario

Consider a scenario with 3 controllers: `A`, `B`, `C`
- `A` mutates a Deployment
- `B` watches Deployments but only acts on certain labels (no-op for most)
- `C` watches Services (unrelated, always no-op for Deployment changes)

```
After A mutates Deployment:
    State has pending=[B, C]

Path 1: Run B first
    B sees Deployment, wrong labels → no-op
    Run C → unrelated kind → no-op
    Converged at StateX

Path 2: Run C first
    C → no-op (doesn't watch Deployments)
    Now pending=[B]
    B → no-op
    Converged at StateX (same as Path 1)

Path 3 (via different history):
    Arrives at same state with pending=[B, C]
    Check: B on this objectsHash → known no-op ✓
    Check: C on this objectsHash → known no-op ✓
    Check: Already converged at this objectsHash? → YES (StateX)
    SKIP via Early Convergence
```

This avoids re-running B and C when we know they won't change anything.
