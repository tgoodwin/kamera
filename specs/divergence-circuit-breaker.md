# Divergence Circuit Breaker

## Overview

The **Divergence Circuit Breaker** limits exploration of subtrees that repeatedly converge to the same state. When paths from a common divergence point keep reaching identical convergences, further exploration of that subtree is likely to produce redundant results. The circuit breaker stops this exploration early to allocate resources to more promising parts of the state space.

## Background: The Churn Problem

Deep DFS exploration can spend disproportionate time in "churny" subtrees where:
1. Many ordering permutations exist
2. Most permutations converge to the same final state
3. Little new information is gained from continued exploration

```
                    Divergence Point D
                    (branching occurs here)
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
    Ordering 1        Ordering 2        Ordering 3
         │                 │                 │
        ...               ...               ...
         │                 │                 │
         ▼                 ▼                 ▼
    Converged X       Converged X       Converged X    ← Same state!
         
         │                 │                 │
         ▼                 ▼                 ▼
    Ordering 4        Ordering 5        Ordering 6
         │                 │                 │
        ...               ...               ...
         │                 │                 │
         ▼                 ▼                 ▼
    Converged X       Converged X       Converged X    ← Still same!

    After N convergences to the same state, stop exploring
    more orderings from divergence point D.
```

## The Insight

If multiple paths from the same divergence point keep reaching the same converged state, the system is likely exhibiting **order-independent** behavior in that subtree. Additional exploration is unlikely to discover new outcomes, so we can safely stop.

## How It Works

### Step 1: Track Divergence Points

When the explorer branches (due to ordering permutations or stale-read views), it records a **divergence key** on the child states:

```go
// When generating ordering variants or stale views
divergenceHash := currState.Hash()
newState := StateNode{
    // ...
    divergenceKey: divergenceHash,  // Points back to branching state
}
```

### Step 2: Track Convergences by Divergence

When a state converges, we record which divergence point it came from:

```go
if currentState.divergenceKey != "" {
    convergenceKey := currentState.ConvergenceHash()
    convergencesByDivergenceKey[currentState.divergenceKey] = append(
        convergencesByDivergenceKey[currentState.divergenceKey], 
        convergenceKey)
}
```

### Step 3: Check the Circuit Breaker

Before processing a state, check if too many paths from its divergence point have already converged to the same state:

```go
if threshold := e.Config.divergenceCircuitBreakerThreshold; threshold > 0 && currentState.divergenceKey != "" {
    convergencesUnderKey := convergencesByDivergenceKey[currentState.divergenceKey]
    repeatedCount := util.MostCommonElementCount(convergencesUnderKey)
    if repeatedCount > threshold {
        // Skip this state - circuit breaker triggered
        continue
    }
}
```

## Visual Example

```
                         Initial State
                              │
                              ▼
                         State A
                         pending=[X, Y, Z]
                              │
    ┌─────────────────────────┼─────────────────────────┐
    │                         │                         │
    ▼                         ▼                         ▼
X first (Ord1)           Y first (Ord2)           Z first (Ord3)
divergenceKey=A          divergenceKey=A          divergenceKey=A
    │                         │                         │
   ...                       ...                       ...
    │                         │                         │
    ▼                         ▼                         ▼
Converged to S           Converged to S           Converged to S
                              │
                              ▼
                    convergencesByDivergenceKey[A] = [S, S, S]
                    mostCommonCount = 3

    ─────────────────────────────────────────────────────────────

                         State A (reached again)
                         pending=[X, Y, Z]
                              │
    More orderings waiting: Ord4, Ord5, Ord6...
    divergenceKey=A
                              │
                    Check: mostCommonCount(A) = 3
                    Threshold = 3
                              │
                    3 > 3? NO (not exceeded yet)
                              │
                    Continue exploring Ord4...
                              ▼
                         Converged to S

    ─────────────────────────────────────────────────────────────
    
                    convergencesByDivergenceKey[A] = [S, S, S, S]
                    mostCommonCount = 4
    
    Next ordering (Ord5):
                    Check: mostCommonCount(A) = 4
                    Threshold = 3
                              │
                    4 > 3? YES!
                              │
                    CIRCUIT BREAKER TRIGGERED
                    Skip Ord5, Ord6, etc.
```

## The Most Common Element Count

The circuit breaker doesn't just count total convergences—it counts how many times the **most common** converged state appears:

```go
// From util package
func MostCommonElementCount[T comparable](elements []T) int {
    counts := make(map[T]int)
    maxCount := 0
    for _, elem := range elements {
        counts[elem]++
        if counts[elem] > maxCount {
            maxCount = counts[elem]
        }
    }
    return maxCount
}
```

This is important because:

```
Scenario: Convergences = [S1, S1, S1, S2, S3]

Total convergences = 5
Most common = S1, count = 3

If threshold = 3:
    3 > 3? NO → Continue exploring

If convergences become [S1, S1, S1, S1, S2, S3]:
    Most common = S1, count = 4
    4 > 3? YES → Trigger circuit breaker
```

The circuit breaker only fires when we're **repeatedly hitting the same outcome**, not just exploring many paths.

## Configuration

Set the threshold in `ExploreConfig`:

```go
cfg := ExploreConfig{
    // ...
    divergenceCircuitBreakerThreshold: 5,  // Trigger after 5+ convergences to same state
}
```

- **Threshold = 0**: Circuit breaker disabled
- **Threshold = N**: Stop after N+1 paths from the same divergence point converge to the same state

## Divergence Key Inheritance

The divergence key propagates through exploration:

```go
newState := StateNode{
    // ...
    // Inherit divergence point from the parent
    divergenceKey: stateView.divergenceKey,
}
```

This means the circuit breaker tracks convergences across the entire subtree below a divergence point, not just immediate children.

```
                    Divergence Point D
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
           Child 1      Child 2      Child 3
           div=D        div=D        div=D
              │            │            │
              ▼            ▼            ▼
         Grandchild    Grandchild    Grandchild
           div=D        div=D        div=D     ← Still tracking D!
              │            │            │
              ▼            ▼            ▼
           Conv X       Conv X       Conv X

    All 3 convergences counted against divergence point D
```

## When New Divergence Keys Are Created

A new divergence key is created when:
1. **Ordering permutations** are generated (`expandStateByReconcileOrder`)
2. **Stale-read views** are generated (`getPossibleViewsForReconcile`)

```go
// In getPossibleViewsForReconcile
divergenceHash := currState.Hash()
asStateNodes := lo.Map(possiblePastViews, func(staleState *StateSnapshot, _ int) StateNode {
    return StateNode{
        // ...
        divergenceKey: divergenceHash,  // New divergence point
    }
})
```

## Soundness Considerations

### When It's Safe

The circuit breaker is **sound** when:
1. The threshold is set high enough to catch truly redundant exploration
2. The state space below the divergence point is order-independent
3. All meaningful outcomes have been discovered before the threshold is reached

### Potential Gaps

```
Gap: Order-dependent outcomes missed
────────────────────────────────────

If the first N paths converge to state X, but path N+1 would converge to Y:
    Circuit breaker triggers, Y is never discovered
    
Mitigation: Set threshold high enough based on expected variance.
           Use with other optimizations that detect order-independence earlier.
```

```
Gap: Different divergence points mixing
───────────────────────────────────────

If a state has two different divergence keys from different paths:
    Only one key is tracked (the one set when state was created)
    
Current behavior: Each StateNode has a single divergenceKey.
                 This is acceptable because we're pruning within subtrees,
                 not across unrelated parts of the state space.
```

## Statistics

The explorer logs when the circuit breaker triggers:

```go
logger.V(1).Info("skipping state; subtree circuit breaker triggered",
    "StateKey", stateKey,
    "DivergenceKey", currentState.divergenceKey,
    "Threshold", threshold,
    "RepeatedConvergences", repeatedCount)
```

## Relationship to Other Optimizations

```
┌─────────────────────────────────────────────────────────────────────┐
│           Divergence Circuit Breaker in Context                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Ordering Pruning                                                   │
│  └── Prevents duplicate BRANCHING at same logical state            │
│      └── Avoids generating redundant ordering variants              │
│                                                                     │
│  Divergence Circuit Breaker (THIS)                                  │
│  └── Stops EXPLORATION when outcomes are repetitive                 │
│      └── Even if variants were generated, stops running them        │
│                                                                     │
│  Subtree Completion                                                 │
│  └── Skips ENTIRE subtrees once fully explored                      │
│      └── Different mechanism: tracks completion, not repetition     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

The circuit breaker is **complementary** to other optimizations:
- **Ordering Pruning** stops generating duplicate variants; the circuit breaker stops when variants that were generated keep producing the same result
- **Subtree Completion** requires full exploration; the circuit breaker works with partial exploration

## Example Scenario

```
Scenario: Deployment controller + Service controller + Pod controller

Initial: Create Deployment
         → Triggers DeploymentReconciler, ServiceReconciler

State S: pending=[DeploymentReconciler, ServiceReconciler]
         divergenceKey = S.Hash()

Path 1: DeploymentReconciler first
        → Creates ReplicaSet → Triggers PodReconciler
        → ServiceReconciler runs → No-op (no endpoints yet)
        → PodReconciler runs → Creates Pods
        → ... eventually ...
        → Converged to FinalState

Path 2: ServiceReconciler first
        → No-op (no Pods yet)
        → DeploymentReconciler runs → Creates ReplicaSet → Triggers PodReconciler
        → PodReconciler runs → Creates Pods
        → ... eventually ...
        → Converged to FinalState    ← Same as Path 1!

Path 3: Another ordering
        → ... eventually ...
        → Converged to FinalState    ← Same again!

After threshold convergences to FinalState:
        → Circuit breaker triggers
        → Remaining orderings skipped
        → Explorer moves to other parts of state space
```

## Tuning the Threshold

- **Too low** (e.g., 2): May miss valid alternative outcomes
- **Too high** (e.g., 100): May waste time on truly redundant exploration
- **Suggested starting point**: 5-10 for most scenarios

The right threshold depends on:
1. Expected number of distinct outcomes in a subtree
2. How much exploration budget you have (time, memory)
3. Whether you're looking for all outcomes vs. just some
