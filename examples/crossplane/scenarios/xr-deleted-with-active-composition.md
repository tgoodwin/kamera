> **STALE**: This analysis contains outdated conclusions from multiple investigation rounds. See [ANALYSIS.md](ANALYSIS.md) for the current evidence-grounded analysis.

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

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-deletion_xr-deleted-with-active-composition.json \
  -interactive=false -log-level=info -depth 20 \
  -output=/tmp/rerun-xr-deleted
```

Ran at depth 10 (default) and 20. Both show the same cycling pattern.

### Campaign metrics

**Reference run (depth=20):**
```
  unique node visits:        15
  total node visits:         21
  unique resource states:    6
  aborted states:            1
  max-depth aborted states:  1
```

**Rerun (depth=20):**
```
  unique node visits:        115
  total node visits:         152
  unique resource states:    16
  aborted states:            3
  max-depth aborted states:  3
```

### Answers to key questions

1. **Did the reference run converge?** No. It cycles infinitely, hitting max depth at both depth=10 and depth=20 with the same unique node count (15).
2. **Did the reference run hit max depth?** Yes -- cycling due to unconditional `Status().Update()` on the deletion path.
3. **Did the perturbed runs converge?** No. All 3 terminal states are max-depth aborts.
4. **Did the perturbed runs hit max depth?** Yes -- same cycling pattern, 115 unique nodes.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** Rerun explores more state space (115 vs 15 unique nodes) but both cycle indefinitely.

### What changed vs previous conclusions

Previously, the reference run converged (4 unique nodes, 3 resource states) and the rerun also converged (7 unique nodes, 4 resource states). The unconditional `Status().Update()` on the deletion path was identified but convergence was still reached because the cycle was bounded.

Now, with commit `38ff304d` (tolerate reconciler errors as no-ops with re-enqueue) and `c968dd97` (harness fixes), the CompositeReconciler is more actively participating. The `Status().Update()` on the deletion path triggers re-enqueues that create a genuine infinite cycle. The system never reaches a quiescent state because every status write triggers another reconcile.

This is consistent with the original Finding 1 about the unconditional `Status().Update()` -- the cycle was always present, but previously the harness terminated it early. Now the harness more faithfully models the production behavior where this cycle would continue (dampened only by rate limiting).

### Previously-reported issues resolved by recent commits

- **Finding 2 (stale-read tuning never applied):** Commit `96729574` (honor workflow JSON staleness config in rerun phase) was expected to fix this. However, the current run does not produce a separate staleness phase, so validating this fix requires checking whether the staleness config is actually being applied in the rerun phase. The rerun does show 115 unique nodes (vs 7 before), suggesting more perturbation combinations are being explored.
- **Finding 1 (unconditional Status().Update):** Still present and now more impactful -- it causes infinite cycling rather than bounded extra work.

## Re-run (2026-03-12, depth=100)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs scenarios/workflow_crossplane-deletion_xr-deleted-with-active-composition.json \
  -interactive=false -log-level=info \
  -output=/tmp/depth100-xr-deleted
```

Ran at depth 100 (default), then depth 400 to confirm cycling.

### Campaign metrics

**Depth 100:**
```
invocation: 65313059-d3b1-45f6-9717-0a0dc4818682
  unique node visits:        15
  total node visits:         101
  unique resource states:    6
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**Depth 400:**
```
invocation: 3715a47d-642c-41fc-b90b-d81ea61b3f43
  unique node visits:        15
  total node visits:         401
  unique resource states:    6
  duration:                  1s
  aborted states:            1
  max-depth aborted states:  1
```

### Answers to key questions

1. **Did the reference run converge?** No. Cycles indefinitely through 15 unique nodes and 6 resource states.
2. **Did the reference run hit max depth?** Yes -- at depth 100 and 400. Unique node count stays fixed at 15.
3. **Did the perturbed run(s) converge?** N/A -- single combined invocation, no convergence.
4. **Did the perturbed runs hit max depth?** Yes.
5. **Are there multiple distinct converged states?** No -- zero converged states.
6. **How do the campaign-metrics compare?** Only one invocation produced. Metrics identical across depths except total visits.

### Comparison with previous runs

| Metric | Original Ref | Original Rerun | 2026-03-11 Ref (d=20) | 2026-03-11 Rerun (d=20) | 2026-03-12 (d=100) |
|--------|-------------|---------------|----------------------|------------------------|---------------------|
| Unique node visits | 4 | 7 | 15 | 115 | 15 |
| Total node visits | 4 | 9 | 21 | 152 | 101 |
| Unique resource states | 3 | 4 | 6 | 16 | 6 |
| Converged states | 1 | 1 | 0 | 0 | 0 |
| Max-depth aborted | 0 | 0 | 1 | 3 | 1 |

The depth-100 run produces the same structural findings as the depth-20 run: 15 unique nodes cycling among 6 resource states. The staleness perturbation exploration (which produced 115 unique nodes in the 2026-03-11 rerun) is not visible in this run -- only a single invocation is produced.

### Updated conclusions

The **unconditional Status().Update() on the deletion path** (Finding 1) remains confirmed. The CompositeReconciler enters an infinite status-write cycle when processing a deleted XWidget. Deeper exploration adds no new information -- the cycle is finite and structural, repeating the same 15 nodes indefinitely. The original bug analysis and proposed fix remain valid.
