# Completed Path Deduplication Optimization

> ⚠️ **Deprecation Warning**: Empirical analysis (see `ablation/RESULTS.md`) shows that this
> optimization provides **zero benefit** when subtree completion is enabled. In the Knative
> serving workload, `completedPathDedup` fires 0 times across all test configurations.
> The subtree completion optimization fully subsumes path-dedup by using a coarser key
> (ignoring execution history). Consider disabling this optimization to reduce code complexity.

## Overview

The **Completed Path Deduplication** optimization tracks which (state, history) pairs have been fully explored to completion (reached a leaf node). When we encounter the same pair again, we can safely skip re-exploration because the identical path would produce identical results.

## Background: Why History Matters

In the explorer's DFS, the same state can be reached via different execution histories:

```
                    Initial
                   /       \
              Path A        Path B
             /     \       /     \
          ...       ...  ...      ...
            \       /      \      /
             \     /        \    /
              State S        State S
           (via history A) (via history B)
```

The question is: **Can we skip exploring State S via Path B if we've already explored it via Path A?**

The answer depends on what information is captured in the "key" we use for deduplication.

## The Two-Stage Approach

### Stage 1: Mark Visited (Start of Exploration)

When we first encounter a (state, history) pair, we mark it as "visited":

```go
e.optimizations.markVisited(ContentsHash, normalizedHistory)
```

This tracks: "We have started exploring this (state, history) combination."

### Stage 2: Mark Completed (End of Exploration)

When a path reaches a leaf (convergence, max depth, or error), we mark it as "completed":

```go
e.optimizations.markCompleted(stateKey, currentState.ExecutionHistory.UniqueKey())
```

This tracks: "We have finished exploring this (state, history) combination to completion."

## Why "Completed" Not Just "Visited"?

```
UNSAFE: Skip if visited
═══════════════════════════════════════════════════════════════════

Timeline:
─────────────────────────────────────────────────────────────────►

T1: Visit State S via Path A
    Mark S as "visited"
    Begin DFS beneath S...

T2: Still exploring S's subtree (Path A)
    Some branches still on stack

T3: Encounter State S via Path B
    Check: S visited? YES
    SKIP ← BUG! Path A hasn't finished, might hit error

T4: Path A hits max depth, aborts
    S's subtree was never fully explored

T5: We've now missed valid convergences via Path B
```

```
SAFE: Skip only if completed
═══════════════════════════════════════════════════════════════════

Timeline:
─────────────────────────────────────────────────────────────────►

T1: Visit State S via Path A
    Mark S as "visited"
    Begin DFS beneath S...

T2: Still exploring S's subtree (Path A)

T3: Encounter State S via Path B
    Check: S completed? NO (still in progress)
    Continue exploring via Path B ✓

T4: Path A completes successfully
    Mark S as "completed" via Path A

T5: If we encounter S again via history A
    Check: S completed? YES
    Safe to skip ✓
```

## How the History Signature Works

The "normalized history" (execution history unique key) captures the **effective mutations** along a path, ignoring no-ops:

```go
func (eh ExecutionHistory) UniqueKey() string {
    var effectiveSteps []string
    for _, step := range eh {
        if !step.wasNoOp() {  // Only include steps that made changes
            effectiveSteps = append(effectiveSteps, 
                fmt.Sprintf("%s@%d", step.ControllerID, len(step.Changes.Effects)))
        }
    }
    return strings.Join(effectiveSteps, ",")
}
```

This means paths that differ only in no-op orderings will have the **same** normalized history:

```
Path A: Foo(mutate) → Bar(no-op) → Baz(no-op) → Converged
Path B: Bar(no-op) → Foo(mutate) → Baz(no-op) → Converged
Path C: Baz(no-op) → Bar(no-op) → Foo(mutate) → Converged

All three have normalized history: "Foo@3" (assuming 3 effects)
```

## The Deduplication Key

The complete key combines state hash and history signature:

```
┌───────────────────────────────────────────────────────────────┐
│                    Completion Key                              │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│   NodeHash (state)         │   HistorySignature              │
│   ──────────────────────── │   ──────────────────────────    │
│   • Objects content hash   │   • Sequence of mutating steps  │
│   • Pending reconciles     │   • Format: "ReconcilerID@N,..." │
│   • (order-sensitive)      │   • Ignores no-ops              │
│                            │                                 │
└────────────────────────────┴─────────────────────────────────┘

Combined key: "{NodeHash}|{HistorySignature}"
```

## Visual Example

```
                         Initial State
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
    Run A (mutates)     Run B (no-op)        Run C (no-op)
         │                    │                    │
         ▼                    ▼                    ▼
    State S₁              State S₂             State S₃
    history="A@2"        pending=[A,C]        pending=[A,B]
         │                    │                    │
    ... explore ...      ... explore ...      ... explore ...
         │                    │                    │
         ▼                    ▼                    ▼
    CONVERGED            Eventually           Eventually
    Mark completed:      reaches State S₁     reaches State S₁
    key=S₁|"A@2"         with history="A@2"   with history="A@2"
                              │                    │
                              ▼                    ▼
                         Check: completed?     Check: completed?
                         key=S₁|"A@2" → YES    key=S₁|"A@2" → YES
                              │                    │
                              ▼                    ▼
                         SKIP ✓                SKIP ✓
```

## Implementation Details

### Data Structures

```go
type optimizations struct {
    // Track which (state, history) pairs we've started exploring
    visitedStatePaths map[NodeHash]map[string]struct{}
    
    // Track which (state, history) pairs we've fully explored
    completedPaths map[string]bool  // key: "{NodeHash}|{HistorySignature}"
}
```

### Key Operations

```go
// Check if this exact path has been completed before
func (o *optimizations) pathCompleted(stateHash NodeHash, historyKey string) bool {
    completionKey := fmt.Sprintf("%s|%s", stateHash, historyKey)
    return o.completedPaths[completionKey]
}

// Mark a path as fully explored
func (o *optimizations) markCompleted(stateHash NodeHash, historyKey string) {
    completionKey := fmt.Sprintf("%s|%s", stateHash, historyKey)
    o.completedPaths[completionKey] = true
}
```

### Usage in Main Loop

```go
// Before processing a state
normalizedHistory := newState.ExecutionHistory.UniqueKey()
if e.optimizations != nil && e.optimizations.pathCompleted(ContentsHash, normalizedHistory) {
    logger.V(1).Info("skipping - path already completed exploration")
    e.stats.SkippedPaths++
    continue
}

// ... process state ...

// When reaching convergence
if e.optimizations != nil {
    e.optimizations.markCompleted(stateKey, currentState.ExecutionHistory.UniqueKey())
}
```

## Comparison with Other Deduplication Approaches

```
┌────────────────────────────────────────────────────────────────────┐
│                   Deduplication Strategies                          │
├─────────────────────┬────────────────────┬─────────────────────────┤
│  Approach           │  Key Components    │  What It Skips          │
├─────────────────────┼────────────────────┼─────────────────────────┤
│  Simple State       │  State hash only   │  Any re-visit of state  │
│  (UNSAFE)           │                    │  (misses different      │
│                     │                    │  history outcomes)      │
├─────────────────────┼────────────────────┼─────────────────────────┤
│  Completed Path     │  State + History   │  Same effective path    │
│  Dedup (THIS)       │  (normalized)      │  reaching same state    │
├─────────────────────┼────────────────────┼─────────────────────────┤
│  Subtree            │  Logical state     │  Any re-visit once      │
│  Completion         │  (objects+pending) │  subtree fully explored │
└─────────────────────┴────────────────────┴─────────────────────────┘
```

## Soundness Analysis

### Why This Is Sound

1. **Determinism**: Given the same state and the same effective history, the same future exploration will occur
2. **Completion tracking**: We only skip if the prior exploration actually finished
3. **Normalized history**: Ignoring no-ops is safe because no-ops don't affect state

### Edge Cases

```
Case: Same state, different pending order
──────────────────────────────────────────────

State S with pending=[A,B] via history H₁
State S with pending=[B,A] via history H₁

These have DIFFERENT NodeHashes (pending is order-sensitive),
so they are tracked separately. Both will be explored.
This is correct: different orderings may produce different outcomes.
```

```
Case: Error during exploration
────────────────────────────────

Path A reaches State S, then hits an error
→ Path A does NOT mark S as completed
→ Path B can still explore S and may succeed
```

## Statistics

The explorer tracks:
- `SkippedPaths`: Count of states skipped via completed path dedup

## Configuration

Enable with:
```go
cfg.Optimizations.CompletedPathDedup = true
```

## Relationship to Subtree Completion

Completed Path Dedup is **more conservative** than Subtree Completion:

```
Completed Path Dedup:
    Key = (state hash) + (history signature)
    Skips: Exact same path reaching exact same state

Subtree Completion:
    Key = (objects hash) + (pending set)
    Skips: ANY path reaching same logical state once subtree done
```

Both can be enabled together. Subtree Completion is more aggressive but requires the marker-based tracking mechanism to ensure soundness. Completed Path Dedup is simpler and always safe.
