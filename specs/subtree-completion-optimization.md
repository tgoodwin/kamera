# Subtree Completion Optimization

## Overview

This document describes a proposed optimization for the DFS exploration in `pkg/tracecheck/explore.go`: **skipping re-exploration of subtrees that have already been fully processed**. This optimization is more aggressive than existing path deduplication but requires careful implementation to remain sound.

## Background: The Exploration Model

The explorer performs a depth-first search over a state space where:

- **State**: A snapshot of Kubernetes resource contents plus a list of pending reconciles
- **Transition**: Executing a pending reconcile, which may modify resources and trigger new reconciles
- **Convergence**: A state with no pending reconciles (or only ignorable async enqueues)
- **Branching**: Occurs due to (1) order permutations of pending reconciles, and (2) stale read perturbations

```
                    Initial State
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
        Order A      Order B      Order C     ← order permutation branching
            │            │            │
            ▼            ▼            ▼
          ...          ...          ...
            │            │            │
            ▼            ▼            ▼
       Converged    Converged    Converged    ← leaf nodes
```

## The Core Insight

**Key observation**: The future exploration from any state depends only on:
1. The current resource contents (objects)
2. The set of pending reconciles

It does **not** depend on:
- How we arrived at this state (execution history)
- The depth in the exploration tree
- Which path we took to get here

Therefore, if we've already **fully explored** all paths from a given (objects, pending) pair, re-exploring from the same logical state would produce identical results.

## The Problem: "Visited" vs "Completed"

The challenge is distinguishing between states we've **started** exploring and states we've **finished** exploring.

### Unsafe Approach (Currently Disabled)

```go
// UNSOUND: Marks as "explored" when we START, not when we FINISH
exploredSubtrees := make(map[string]struct{})

// When encountering a state:
subtreeKey := computeSubtreeKey(state)
if _, seen := exploredSubtrees[subtreeKey]; seen {
    continue  // SKIP - but subtree might not be done yet!
}
exploredSubtrees[subtreeKey] = struct{}{}  // mark as "explored"
```

This is **unsound** because:

```
Timeline:
─────────────────────────────────────────────────────────────────►

T1: Encounter state S via path A
    Mark S as "explored"
    Begin DFS beneath S...

T2: Still exploring S's subtree (path A)
    Some branches still in queue

T3: Encounter state S via path B (different history, same logical state)
    Check: S in exploredSubtrees? YES
    SKIP ← This is the bug! Path A hasn't finished yet

T4: Path A hits an error or max depth, aborts
    S's subtree was never fully explored

T5: We've now missed all paths through S via path B
    Potential convergence outcomes lost
```

### Why This Differs From Path Deduplication

The existing path deduplication (`pathCompleted`) uses a different, more conservative approach:

| Aspect | Path Deduplication | Subtree Completion |
|--------|-------------------|-------------------|
| **Key** | `(stateHash, historySignature)` | `(objectsHash, pendingSet)` |
| **What it tracks** | Individual execution paths | All paths from a logical state |
| **When marked complete** | When a specific path reaches a leaf | When ALL descendants are done |
| **Granularity** | Fine (each history is separate) | Coarse (history doesn't matter) |
| **Detection difficulty** | Easy (leaf = done) | Hard (need "all children finished") |

### Concrete Example

Consider this exploration tree:

```
        State S (objects=X, pending=[A,B])
               /                \
    S₁ (run A first)      S₂ (run B first)
         /    \                /    \
       ...    ...            ...    ...
        │      │              │      │
        ▼      ▼              ▼      ▼
    Conv₁  Conv₂          Conv₃  Conv₄
```

**Path deduplication** tracks:
- `(S, history=[])` → in progress
- `(S₁, history=[A])` → in progress
- `(Conv₁, history=[A,B,...])` → **completed**
- etc.

Each (state, history) pair is tracked independently. If we reach `S` with a different history, we don't skip it because that's a different key.

**Subtree completion** would track:
- `(objects=X, pending=[A,B])` → all paths beneath this logical state

If we've explored **all** of S₁, S₂, Conv₁, Conv₂, Conv₃, Conv₄, then encountering the same logical state `(objects=X, pending=[A,B])` via any history can be skipped entirely.

## Why Subtree Completion is More Powerful

Path deduplication cannot skip this scenario:

```
Path 1: Init → P → Q → S → Conv
Path 2: Init → R → S → Conv (same S, different history)
```

With path deduplication:
- Path 1 explores S with `history=[...,P,Q]`
- Path 2 has `history=[...,R]` — different key, must re-explore

With subtree completion:
- Path 1 fully explores the subtree beneath S
- Path 2 encounters the same logical state S
- Since S's subtree is complete, skip entirely

The savings compound when S has a large subtree with many ordering permutations.

## The Implementation Challenge

In recursive DFS, detecting subtree completion is natural:

```python
def explore(state):
    if state.logical_key in completed_subtrees:
        return  # safe to skip

    in_progress.add(state.logical_key)

    for child in expand(state):
        explore(child)  # recursive call

    # When we return here, ALL children are done
    in_progress.remove(state.logical_key)
    completed_subtrees.add(state.logical_key)
```

In **iterative** DFS with a queue/stack, there's no natural "return" point:

```go
for len(queue) > 0 {
    state := pop(queue)
    // process state
    for _, child := range expand(state) {
        push(queue, child)
    }
    // No indication when all children are done!
}
```

The queue doesn't preserve the parent-child relationship needed to detect when a subtree is complete.

## Proposed Solutions

### Option 1: Reference Counting

Track how many "in-flight" explorations exist for each logical state.

```go
type subtreeTracker struct {
    inFlight  map[LogicalStateKey]int   // count of open explorations
    completed map[LogicalStateKey]bool  // fully explored
}

// When expanding state S into children C1, C2, ..., Cn:
tracker.inFlight[S.logicalKey()] = n

// When any path finishes (converge, abort, skip):
// Walk up the parent chain, decrementing counts
func (t *subtreeTracker) markPathFinished(state StateNode) {
    for s := &state; s != nil; s = s.parent {
        key := s.logicalKey()
        t.inFlight[key]--
        if t.inFlight[key] == 0 {
            t.completed[key] = true
        }
    }
}
```

**Pros**: Accurate, handles all cases
**Cons**: Requires parent pointer traversal on every completion; memory for counters

### Option 2: Stack Markers

Insert "completion marker" entries into the DFS stack.

```go
type stackEntry struct {
    state    StateNode
    isMarker bool
    markerKey LogicalStateKey  // which state this marker is for
}

// When expanding state S:
push(stackEntry{isMarker: true, markerKey: S.logicalKey()})
for _, child := range expand(S) {
    push(stackEntry{state: child})
}

// When popping:
entry := pop()
if entry.isMarker {
    // Everything pushed after this marker has been processed
    completed[entry.markerKey] = true
    continue
}
// else process entry.state normally
```

**Pros**: Clean, leverages DFS structure naturally
**Cons**: Markers consume stack space; need to handle nested markers correctly

#### Nested Expansion Works Correctly

The marker approach correctly handles nested expansions where children themselves have children. Here's a step-by-step trace:

```
Tree structure:
    State S
    ├── C1 (expands to G1, G2)
    └── C2 (leaf)

Step-by-step stack evolution:

1. Process S, expand to C1, C2:
   Push: Marker(S), C1, C2
   Stack: [Marker(S), C1, C2]          (top: C2)

2. Pop C2, process (leaf, no children):
   Stack: [Marker(S), C1]

3. Pop C1, expand to G1, G2:
   Push: Marker(C1), G1, G2
   Stack: [Marker(S), Marker(C1), G1, G2]    (top: G2)

4. Pop G2, process (leaf):
   Stack: [Marker(S), Marker(C1), G1]

5. Pop G1, process (leaf):
   Stack: [Marker(S), Marker(C1)]

6. Pop Marker(C1):
   → Mark C1's subtree as COMPLETE
   Stack: [Marker(S)]

7. Pop Marker(S):
   → Mark S's subtree as COMPLETE
   Stack: []
```

This mirrors exactly what happens with recursive DFS call stack unwinding:
- `explore(S)` calls `explore(C1)` and `explore(C2)`
- `explore(C1)` calls `explore(G1)` and `explore(G2)`
- Returns propagate: G1 → G2 → C1 complete → C2 → S complete

The key insight: **markers are pushed before children, so they pop after all descendants**.

### Option 3: Deferred Completion via Channels

Use goroutines and channels to propagate completion.

```go
// Each logical state has a "done" channel
doneChans := make(map[LogicalStateKey]chan struct{})

// When all children finish, close the channel
// Parent can select on children's done channels
```

**Pros**: Decouples completion tracking from main loop
**Cons**: Complexity; may not integrate well with existing design

### Option 4: Post-Order Marking with Generation Numbers

Assign generation numbers to track DFS "depth" in the stack.

```go
type stackEntry struct {
    state      StateNode
    generation int  // when this was pushed
}

var currentGeneration int

// When pushing children of S:
S.childGeneration = currentGeneration
currentGeneration++
for _, child := range expand(S) {
    push(stackEntry{state: child, generation: currentGeneration})
}

// When popping an entry with generation G:
// All entries with generation > G that were pushed after have been processed
// Mark completion for any state whose childGeneration < G
```

**Pros**: No markers in stack; O(1) check
**Cons**: Complex bookkeeping; need to maintain generation→state mapping

## Recommendation

**Option 2 (Stack Markers)** is the cleanest fit for the current iterative DFS structure:

1. Minimal change to the queue/stack type
2. Naturally correct: markers pop only after all descendants
3. Easy to reason about
4. Integrates with existing `enqueueState` / `getNext` pattern

The implementation would:
1. Define a union type for stack entries (state or marker)
2. Push a marker before pushing children
3. When popping a marker, record completion for that logical state
4. Check `completed` before processing any state

## Implementation Details

### Logical State Key Design

The key must capture everything that affects future exploration:

1. **Objects hash**: The resource contents determine what reconcilers will observe
2. **Pending set (order-insensitive)**: The set of reconcilers that need to run — but NOT their order, since we expand all orderings under a single marker
3. **Stuck positions**: The `stuckReconcilerPositions` field affects which reconcilers get triggered, so states with different stuck positions have different futures

The pending set must be **order-insensitive** because ordering variants (e.g., `[A,B]` vs `[B,A]`) represent the same logical state. We expand all orderings and cover them with a single marker.

### Tracker State: `completed` vs `inProgress`

The tracker maintains two maps:

- **`completed`**: Logical states whose subtrees have been fully explored. This is the core of the optimization — when we encounter a completed state, we skip it entirely.

- **`inProgress`**: Logical states currently being explored (marker pushed but not yet popped). This handles "diamond" convergence.

#### Why `inProgress` is Needed: Diamond Convergence

Consider a scenario where two different paths in the DFS reach the same logical state L:

```
        A
       / \
      B   C
       \ /
        L (same logical state reached via both paths)
```

Without `inProgress` tracking:
1. Process B → L not completed, push Marker(L), enqueue L
2. Process C → L not completed, push another Marker(L), enqueue L again
3. Result: duplicate markers, duplicate exploration work

With `inProgress` tracking:
1. Process B → L not completed, not in progress → mark in progress, push Marker(L), enqueue L
2. Process C → L already in progress → skip (existing exploration will cover it)
3. Result: single marker, no redundant work

Technically, `inProgress` is an optimization rather than a correctness requirement — without it, duplicate work would occur but results would still be correct. However, diamond convergence may be common enough (e.g., two reconcilers making idempotent changes that converge to the same state) that avoiding redundant exploration is worthwhile.

### Scope: Ordering Expansion Only (For Now)

This design focuses on **ordering expansion** as the branching mechanism tracked by markers. All ordering variants of a logical state share one marker.

**TODO (Staleness Expansion)**: Staleness expansion (`getPossibleViewsForReconcile`) creates states with different object contents, hence different logical keys. Currently these are processed inline rather than enqueued as siblings. A future iteration should consider unifying the branching model so all expansion types (ordering, staleness) use consistent marker-based completion tracking.

## Soundness Considerations

For the optimization to be **sound**, we must ensure:

1. **Never skip in-progress subtrees**: Only skip if `completed[key] == true`
2. **Handle errors correctly**: If a path aborts due to error, that's still "completion" of that path — the subtree might still be valid via other paths, so don't mark complete until ALL paths finish
3. **Handle max-depth correctly**: Max-depth aborts are acceptable completions — we've explored as far as allowed
4. **Logical state key must be complete**: The key must capture everything that affects future exploration:
   - Object contents (hash)
   - Pending reconcile set (order-insensitive for the key, since we expand all orderings)
   - Possibly: stuck reconciler positions (for stale read scenarios)

## Relationship to Other Optimizations

| Optimization | What it prunes | Soundness basis |
|--------------|---------------|-----------------|
| **Path dedup** | Same (state, history) reached again | History-sensitive; very conservative |
| **Early convergence** | All pending are known no-ops | No-op = no change = same outcome |
| **Ordering pruning** | Re-expanding order variants | One expansion covers all orderings |
| **Cache prediction** | Predicted duplicate output | Memoized reconcile results |
| **Subtree completion** | Re-exploring finished subtrees | Future depends only on current state |

Subtree completion is the most aggressive because it ignores history entirely. It subsumes some of the other optimizations when applicable.

## Open Questions

1. **What exactly is the logical state key?**
   - `objectsHash + sorted(pendingReconcilerIDs)`?
   - Include stuck positions for stale-read perturbation?
   - Include any other state that affects future exploration?

2. **How do we handle partial completion?**
   - If 90% of a subtree is explored and 10% errored, is it "complete"?
   - Should we track "complete" vs "complete with errors"?

3. **Memory overhead**
   - How many unique logical states do we expect?
   - Should we bound the `completed` map size with LRU eviction?

4. **Interaction with divergence circuit breaker**
   - The circuit breaker stops early when subtrees keep converging identically
   - How does this interact with subtree completion tracking?

## Next Steps

1. Prototype Option 2 (stack markers) on a branch
2. Add instrumentation to measure how often we'd skip via subtree completion
3. Verify soundness by comparing results with/without the optimization
4. Benchmark memory and performance impact
