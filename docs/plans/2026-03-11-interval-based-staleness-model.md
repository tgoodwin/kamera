# Interval-Based Staleness Model

## Context

The current staleness mechanism branches on all possible stale views at every reconcile step, causing combinatorial explosion and violating real informer semantics. In real K8s:

- A controller's informer cache is eventually consistent and **monotonically advancing** — it may lag behind etcd, but never goes backward on a single execution path (absent crashes/restarts)
- Staleness is a **window**: the controller falls behind at some point and catches up later
- During the stale window, writes from the controller go to real cluster state; reads come from its stale view

We want to replace the per-step branching with **staleness intervals** — declarative descriptions of when a controller falls behind and when it catches up, expressed in terms of KindSequences rather than execution depth.

## Data Model

### Staleness Interval

```go
// StalenessInterval defines a window during which a reconciler's view
// of a specific resource kind lags behind the actual cluster state.
type StalenessInterval struct {
    ReconcilerID ReconcilerID
    Kind         string  // canonical group/kind, e.g. "apiextensions.crossplane.io/CompositionRevision"
    StaleAt      int64   // KindSequence value at which the view freezes/lags
    CatchUpAt    int64   // KindSequence value at which the view snaps to current
    Lag          int64   // how far behind frontier; -1 = frozen at StaleAt sequence
}
```

**Semantics**: When the frontier KindSequence for `Kind` is in `[StaleAt, CatchUpAt)`:
- If `Lag == -1`: reconciler sees `Kind` frozen at sequence `StaleAt`
- If `Lag > 0`: reconciler sees `Kind` at `frontier - Lag` (but never below `StaleAt`)

When frontier reaches `CatchUpAt`, the reconciler's view snaps to current.

### Workflow JSON Schema

In `tuning.stalenessIntervals`:
```json
{
  "tuning": {
    "stalenessIntervals": [
      {
        "reconciler": "CompositeReconciler",
        "kind": "apiextensions.crossplane.io/CompositionRevision",
        "staleAt": 3,
        "catchUpAt": 7,
        "lag": -1
      }
    ]
  }
}
```

### Internal Config

Add to `ExploreConfig.Perturbations`:
```go
type Perturbations struct {
    // ... existing fields ...
    StalenessIntervals []StalenessInterval `json:"stalenessIntervals,omitempty"`
}
```

This replaces the role of `Staleness map[ReconcilerID]StalenessConfig` for the interval-based approach. The existing `Staleness` map and `getPossibleViewsForReconcile` branching remain for now (disabled via MaxRestarts=0) but are not used when intervals are configured.

## Enforcement

### Replace `stuckReconcilerPositions` with interval-aware observation

Currently `stuckReconcilerPositions` stores a fixed `KindSequences` per reconciler — frozen forever. We replace this with a function that evaluates intervals against the current state.

**Option A — precomputed per-state** (recommended for simplicity):
- On `StateNode`, store the full `[]StalenessInterval` config (carried forward from ExploreConfig, not cloned per state)
- In `ObserveAs(reconcilerID)`, evaluate each interval:
  1. Look up current frontier: `sn.Contents.KindSequences[interval.Kind]`
  2. If frontier is in `[StaleAt, CatchUpAt)`: compute stale sequence and observe at it
  3. Otherwise: reconciler sees current state for that kind

```go
func (sn StateNode) ObserveAs(reconcilerID ReconcilerID) ObjectVersions {
    staleSeqs := sn.evaluateStalenessIntervals(reconcilerID)
    if len(staleSeqs) == 0 {
        return sn.Contents.All()
    }
    kindSequences := maps.Clone(sn.Contents.KindSequences)
    for k, seq := range staleSeqs {
        kindSequences[k] = seq
    }
    return sn.Contents.ObserveAt(kindSequences)
}

func (sn StateNode) evaluateStalenessIntervals(reconcilerID ReconcilerID) KindSequences {
    if sn.stalenessIntervals == nil {
        return nil
    }
    result := make(KindSequences)
    for _, interval := range sn.stalenessIntervals {
        if interval.ReconcilerID != reconcilerID {
            continue
        }
        frontier := sn.Contents.KindSequences[interval.Kind]
        if frontier < interval.StaleAt || frontier >= interval.CatchUpAt {
            continue // not in stale window
        }
        if interval.Lag == -1 {
            result[interval.Kind] = interval.StaleAt
        } else {
            staleSeq := frontier - interval.Lag
            if staleSeq < interval.StaleAt {
                staleSeq = interval.StaleAt
            }
            result[interval.Kind] = staleSeq
        }
    }
    return result
}
```

### No branching in `getPossibleViewsForReconcile`

With intervals, staleness is **not** a source of branching. The interval defines exactly what the reconciler sees — there's no combinatorial choice. `getPossibleViewsForReconcile` returns only the current state; `ObserveAs` handles the stale view transparently.

The ordering permutation (`PermuteOrder`) remains the source of branching — which controller runs next. Staleness intervals just shape what each controller sees when it does run.

### `StalenessInfo` computation

The existing `computeStalenessInfo` (explore.go:1416) should still fire when a reconciler is in a stale window. Update `applyReconcileStep` to check `evaluateStalenessIntervals` instead of `stuckReconcilerPositions`:

```go
if consumed != nil {
    staleSeqs := stateView.evaluateStalenessIntervals(consumed.ReconcilerID)
    if len(staleSeqs) > 0 {
        stepResult.StalenessInfo = e.computeStalenessInfo(stateView, consumed.ReconcilerID, staleSeqs)
    }
}
```

## Plumbing

### `coverage.InputTuning` → `ExploreConfig`

1. Add `StalenessIntervals` to `coverage.InputTuning` (pkg/coverage/types.go)
2. Parse in `applyInputTuning` (examples/crossplane/scenario.go) — map JSON field names to internal struct
3. Pass through to `ExploreConfig.Perturbations.StalenessIntervals`

### `StateNode` carries intervals

Add `stalenessIntervals []StalenessInterval` to `StateNode`. Set once from `ExploreConfig` at exploration start, cloned to children (pointer/slice reference is fine — intervals are immutable config).

### Interaction with existing `Staleness` map

When `StalenessIntervals` is configured:
- Skip `getPossibleViewsForReconcile` branching entirely (return `[]StateNode{currState}`)
- `ObserveAs` uses interval evaluation instead of `stuckReconcilerPositions`

When `StalenessIntervals` is empty, fall back to existing behavior (currently disabled via MaxRestarts=0, but preserved for future re-enablement).

## Files to Modify

| File | Change |
|------|--------|
| `pkg/tracecheck/explore.go` | Add `StalenessInterval` type; skip branching when intervals configured |
| `pkg/tracecheck/state.go` | Add `stalenessIntervals` field to `StateNode`; update `ObserveAs` with `evaluateStalenessIntervals` |
| `pkg/coverage/types.go` | Add `StalenessIntervals` to `InputTuning` |
| `examples/crossplane/scenario.go` | Parse `stalenessIntervals` in `applyInputTuning` |
| `pkg/explore/parallel_runner.go` | Pass intervals through in `buildExplicitStalenessPlans` |

## Verification

1. Unit test: `evaluateStalenessIntervals` returns correct stale sequences for various frontier values (before, during, after interval; frozen vs lag modes)
2. Unit test: `ObserveAs` with intervals returns stale objects during window, current objects outside
3. Integration: Create a workflow JSON with explicit staleness intervals, run crossplane scenario, verify:
   - `campaign-metrics` shows converged states (not all max-depth aborted)
   - `StalenessInfo` appears in dump for steps within the stale window
   - Reconciler sees current state after `CatchUpAt`

## What This Defers

- **Reference-informed interval generation**: Auto-deriving intervals from reference run KindSequence ranges. Will be a planner feature built on top of this infrastructure.
- **MaxRestarts / per-step branching**: Left disabled. May be removed entirely or repurposed once intervals prove sufficient.
- **Multiple overlapping intervals per reconciler**: Supported by the data model (slice of intervals) but not specifically tested in v1.
