# Workflow Analysis: crossplane-deletion/xr-deleted-with-active-composition

## What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-deletion_xr-deleted-with-active-composition.json \
  -interactive=false -log-level=info -output=/tmp/workflow-xr-deletion -depth 100
```

The workflow seeds a FunctionRevision, Composition, CompositionRevision (validated), and a bound XWidget, then issues a DELETE on the XWidget. The tuning enables controller permutation across all three reconcilers and configures stale reads for CompositeReconciler on `example.org/XWidget` with lookback=1.

## Trace summary

### Reference phase (no perturbation)

| Metric | Value |
|--------|-------|
| Unique node visits | 4 |
| Total node visits | 4 |
| Unique resource states | 3 |

Execution path: DELETE XWidget -> CleanupReconciler removes object -> CompositeReconciler gets "not found" -> converges. Final state has 3 objects (Composition, CompositionRevision, FunctionRevision).

### Rerun phase (with controller permutation)

| Metric | Value |
|--------|-------|
| Unique node visits | 7 |
| Total node visits | 9 |
| Unique resource states | 4 |

The rerun explores an alternative ordering where CompositeReconciler runs before CleanupReconciler. This reveals two findings.

## Finding 1: CompositeReconciler performs unconditional Status().Update() on deletion path

**Controller:** `composite/reconciler.go` (CompositeReconciler)

**Version:** crossplane v2.1.0

**Mechanism:**

When CompositeReconciler runs on a deleted XWidget (one with `deletionTimestamp` set), lines 553-574 of `reconciler.go` execute the deletion branch:

```go
if meta.WasDeleted(xr) {
    status.MarkConditions(xpv1.Deleting())
    if err := r.composite.RemoveFinalizer(ctx, xr); err != nil { ... }
    log.Debug("Successfully deleted composite resource")
    status.MarkConditions(xpv1.ReconcileSuccess())
    return reconcile.Result{Requeue: false}, errors.Wrap(r.client.Status().Update(updateCtx, xr), errUpdateStatus)
}
```

The `Status().Update()` call on line 574 is unconditional. On the first execution, it writes three new status conditions (Responsive/WatchCircuitClosed, Ready/Deleting, Synced/ReconcileSuccess). On subsequent executions these conditions are identical, but the update still produces a new resource version.

**Trace evidence:**

In the rerun trace, after the DELETE action, CompositeReconciler runs first and produces a Status().Update() effect:

```
delta: +status.conditions: [{Responsive, WatchCircuitClosed, True},
                            {Ready, Deleting, False},
                            {Synced, ReconcileSuccess, True}]
```

This creates a 4th unique resource state (hash `9cb41cd6...`) that does not appear in the reference run. The status update triggers CleanupReconciler and CompositeReconciler again, producing a nonconvergence cycle:

- Total States grows: 3 -> 4 -> 5 -> 6 (unbounded)
- Resource States stays constant at 3

This matches the same pattern as the known CompositionRevisionReconciler unconditional status write (documented in BUGS.md), but occurs in a different reconciler (CompositeReconciler) and on a different code path (deletion handling).

**Distinction from known bug:** The known bug in BUGS.md is about CompositionRevisionReconciler's unconditional `Status().Update()` during normal reconciliation. This bug is about CompositeReconciler's unconditional `Status().Update()` specifically on the **deletion path**. The mechanism is the same (unconditional status write -> watch event -> re-enqueue cycle) but the affected controller and trigger condition are different.

**Practical impact:** When an XR is being deleted, the CompositeReconciler enters a tight reconcile loop, writing identical status conditions to a doomed object on every pass. In production, this is dampened by rate limiting but still wastes API server resources during deletion. The unnecessary status writes also delay actual garbage collection since each write produces a new resource version that the cleanup logic must process.

**Idiomatic fix (in Crossplane):**

```go
if meta.WasDeleted(xr) {
    status.MarkConditions(xpv1.Deleting())
    if err := r.composite.RemoveFinalizer(ctx, xr); err != nil { ... }
    log.Debug("Successfully deleted composite resource")
    status.MarkConditions(xpv1.ReconcileSuccess())

    // Only update status if conditions actually changed.
    original := xr.DeepCopy() // capture before MarkConditions
    if !equality.Semantic.DeepEqual(original.Object["status"], xr.Object["status"]) {
        return reconcile.Result{}, errors.Wrap(r.client.Status().Update(updateCtx, xr), errUpdateStatus)
    }
    return reconcile.Result{}, nil
}
```

## Finding 2: Stale-read tuning from workflow JSON is never applied

**Component:** Kamera crossplane harness (`scenario.go` / `parallel_runner.go`)

The workflow JSON specifies `staleReads: {CompositeReconciler: [example.org/XWidget]}` with `staleLookback: {example.org/XWidget: 1}`. However, this configuration is effectively dead:

1. The **reference** phase calls `disablePerturbations(scenario.Config)`, which strips all perturbation config including staleness.
2. The **rerun** phase is built by `buildDefaultScenarioRerunPlans`, which also calls `disablePerturbations(base)` and only re-enables `PermuteOrder`. The staleness config is discarded.
3. The **auto-generated staleness phase** from `buildStalenessPerturbationPlans` derives its own staleness config from observed reads in the reference trace, ignoring the JSON tuning entirely.

Additionally, `applyInputTuning` constructs `StalenessConfig` without setting `MaxRestarts`, which defaults to 0. The staleness generation code at `explore.go:1920` checks `currRestarts >= maxRestarts` (0 >= 0 = true) and skips stale view generation. Even if the config were applied, no stale views would be produced.

**Consequence:** The intended stale-read scenario (CompositeReconciler reads pre-deletion XWidget and proceeds with full reconcile on a doomed object) was never actually tested. The target bug hypothesis -- stale XR read during deletion causing unnecessary reconcile work -- could not be validated or refuted.

**Fix needed in harness:**
1. `applyInputTuning` should default `MaxRestarts` to 1 when `StaleReadBounds` are configured.
2. `buildDefaultScenarioRerunPlans` should preserve manually-configured staleness from the scenario config rather than stripping it.

## What was checked

- Ran workflow at depth 10 and depth 15/20 with info and debug log levels
- Analyzed both reference and rerun JSONL trace files in detail
- Examined state transitions, object version hashes, and effects at each step
- Read Crossplane CompositeReconciler source code at `/Users/tgoodwin/go/pkg/mod/github.com/crossplane/crossplane/v2@v2.1.0/internal/controller/apiextensions/composite/reconciler.go`
- Traced the Kamera staleness mechanism through `explore.go`, `staleness.go`, and `parallel_runner.go`
- Confirmed nonconvergence signal: Total States growing unboundedly while Resource States remains constant
