# Refactor Stale View Branching to Stack-Based Strategy

**Issue:** kamera-g4j
**Date:** 2026-01-20

## Problem

In `pkg/tracecheck/explore.go`, stale view branching uses an inline for loop (lines 763-1031):

```go
for _, stateView := range possibleViews {
    // ... reconcile step for each view
    // ... enqueue resulting states
}
```

This processes views sequentially in the same stack frame, missing the subtree completion optimization that order branching uses.

## Solution

Refactor stale view branching to use the same stack-based strategy as order perturbation:

1. Push all stale views onto the stack with a completion marker
2. Each view becomes a separate stack entry processed in DFS order
3. When marker pops, mark the stale view branch as completed
4. Future encounters of same stale view branch can skip

## Implementation

### New Types

```go
// StaleViewBranchKey identifies a stale view branching point.
type StaleViewBranchKey struct {
    ParentStateHash NodeHash
    ReconcilerID    ReconcilerID
    RequestKey      string // NamespacedName.String()
}

// Extend stackEntry:
type stackEntry struct {
    state  *StateNode
    marker *LogicalStateKey      // ordering subtree completion marker

    // Stale view branching:
    staleViewMarker    *StaleViewBranchKey // stale view completion marker
    staleViewReconcile *PendingReconcile   // if set, this is a stale view ready to reconcile
}

func (e stackEntry) isStaleViewMarker() bool {
    return e.staleViewMarker != nil
}

func (e stackEntry) isStaleViewEntry() bool {
    return e.staleViewReconcile != nil
}
```

### Stale View Tracker

```go
type staleViewTracker struct {
    completed  map[StaleViewBranchKey]struct{}
    inProgress map[StaleViewBranchKey]struct{}
}

func newStaleViewTracker() *staleViewTracker { ... }
func (t *staleViewTracker) isCompleted(key StaleViewBranchKey) bool { ... }
func (t *staleViewTracker) markInProgress(key StaleViewBranchKey) { ... }
func (t *staleViewTracker) markCompleted(key StaleViewBranchKey) { ... }
```

### Stats

Add to `ExploreStats`:
- `StaleViewCompletionSkips int`

### Main Loop Refactor

Replace the for loop with:

```go
var stateView StateNode
var pendingReconcile PendingReconcile

if entry.isStaleViewEntry() {
    // Already a stale view - use directly, skip view generation
    stateView = currentState
    pendingReconcile = *entry.staleViewReconcile
} else {
    pendingReconcile = currentState.PendingReconciles[0]

    possibleViews, err := e.getPossibleViewsForReconcile(...)
    if err != nil { return err }

    if len(possibleViews) == 0 {
        // existing no-views handling
        continue
    }

    if len(possibleViews) > 1 {
        // Multiple views - push all with marker
        staleKey := StaleViewBranchKey{
            ParentStateHash: currentState.Hash(),
            ReconcilerID:    pendingReconcile.ReconcilerID,
            RequestKey:      pendingReconcile.Request.NamespacedName.String(),
        }
        if staleViewTracker.isCompleted(staleKey) {
            e.stats.StaleViewCompletionSkips++
            continue
        }
        staleViewTracker.markInProgress(staleKey)
        stack = append(stack, stackEntry{staleViewMarker: &staleKey})
        for i := range possibleViews {
            stack = append(stack, stackEntry{
                state:              &possibleViews[i],
                staleViewReconcile: &pendingReconcile,
            })
        }
        continue
    }

    // Single view
    stateView = possibleViews[0]
}

// Process single stateView (no for loop)
// ... reconcile step, new state, ordering variants, enqueue ...
```

Handle marker when popped:
```go
if entry.isStaleViewMarker() {
    staleViewTracker.markCompleted(*entry.staleViewMarker)
    continue
}
```

## Testing

- Run existing integration tests to ensure no regression
- Verify stale view exploration produces same results
- Check `StaleViewCompletionSkips` stat is populated when expected
