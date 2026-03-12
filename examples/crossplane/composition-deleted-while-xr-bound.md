# Workflow Analysis: composition-deleted-while-xr-bound

## What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-deletion_composition-deleted-while-xr-bound.json \
  -interactive=false -log-level=info \
  -output=/tmp/workflow-composition-deletion
```

**Workflow file:** `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`

**Setup:** FunctionRevision + Composition (`widget-composition-ephemeral`) + CompositionRevision (rev-1, validated) + XWidget (bound via `compositionRef` and `compositionRevisionRef`). User input: DELETE Composition.

**Tuning:** `permuteControllers: [CompositeReconciler, CompositionReconciler]`, `staleReads: {CompositeReconciler: [Composition]}`, `staleLookback: {Composition: 1}`

## Trace results

| Metric | Reference | Rerun |
|--------|-----------|-------|
| Unique node visits | 4 | 6 |
| Total node visits | 4 | 7 |
| Unique resource states | 3 | 3 |
| Converged states | 1 (hash: `qjpgnp1v`) | 1 (hash: `qjpgnp1v`) |

Both reference and rerun traces converge to the same final state hash `184s8enj` (after Composition removal) and then `qjpgnp1v` as the terminal converged state.

## Trace analysis: post-deletion convergence behavior

### Step-by-step trace (reference run)

1. **External User** (depth 0): DELETE Composition. Sets `deletionTimestamp: 2025-01-01T00:00:00Z` on the Composition. Composition spec is stripped (only metadata remains). Pending: `CleanupReconciler`, `CompositionReconciler`.

2. **CleanupReconciler** (depth 1): GETs the deletion-marked Composition, issues REMOVE to purge it from state. Composition is gone from cluster state. Pending: `CompositionReconciler`.

3. **CompositionReconciler** (depth 2): Attempts to GET the Composition but it is already removed. Returns NotFound, produces no effects. No further pending reconciles.

### Critical observation: CompositeReconciler never fires

Across all trace paths in both reference and rerun, the **CompositeReconciler is never triggered**. The XWidget object is unchanged throughout the entire exploration. Its final state is identical to its initial state -- still carrying:

```yaml
spec:
  compositionRef:
    name: widget-composition-ephemeral
  compositionRevisionRef:
    name: widget-composition-ephemeral-rev-1
  compositionUpdatePolicy: Automatic
```

This is because:
- The harness registers CompositeReconciler `.ForGK(example.org/XWidget)` -- it is only triggered by XWidget changes.
- No `Watches` are registered from Composition or CompositionRevision to XWidget.
- In real Crossplane, the CompositeReconciler is triggered by periodic `RequeueAfter` (60s poll interval), not by Composition watch events.

### Stale reads had no observable effect

The configured stale read perturbation for `CompositeReconciler` reading `Composition` had no effect because the CompositeReconciler was never triggered in any path.

## Bug hypothesis: orphaned compositionRef with no recovery path

**Controller:** `composite/reconciler.go` and `composite/api.go` (CompositeReconciler / APIRevisionFetcher / APILabelSelectorResolver)

**Version:** crossplane v2.1.0

**Summary:** When a Composition is deleted while an XR (composite resource) is bound to it, the XR enters a permanent error loop with no self-healing path. The `compositionRef` remains set, pointing at a non-existent Composition, and no code path clears or re-evaluates it.

**Mechanism:**

1. XR has `compositionRef.name: widget-composition-ephemeral` set during initial composition selection.

2. User deletes Composition `widget-composition-ephemeral`. In production, Kubernetes GC may also cascade-delete the CompositionRevision (via ownerReference).

3. On the next periodic reconcile (driven by `RequeueAfter: 60s`), CompositeReconciler runs:
   - **`SelectComposition`** (`api.go:256-257`): checks `if cp.GetCompositionReference() != nil { return nil }`. Since `compositionRef` is already set, it returns immediately without re-evaluating. This is a one-way latch.
   - **`Fetch`** (`api.go:173-176`): calls `f.client.Get(ctx, compositionRef, comp)`. Returns `NotFound`.
   - Error propagates as `errFetchComp` -> `errGetComposition`. Reconciler sets `ReconcileError` condition and returns the error.

4. The error triggers exponential-backoff requeue. Every retry hits the same `NotFound` error.

5. **There is no code path that clears `compositionRef` when the referenced Composition is deleted.** The reconciler cannot recover without manual intervention (user must delete the XR or manually clear/change the compositionRef).

**Evidence from Crossplane source:**

- `api.go:251-252` contains an unimplemented TODO: `"need to block the deletion of composition via finalizer once it's selected since it's integral to this resource."` This acknowledges the problem but neither prevention (finalizer) nor recovery (re-selection) was implemented.

- `api.go:256-257`: `SelectComposition` short-circuits when `compositionRef` is already set, preventing re-selection of an alternative Composition.

- `reconciler.go:611-624`: The `Fetch` error path sets `ReconcileError` and returns, with no special handling for the "Composition deleted" case.

- No code in the composite package calls `SetCompositionReference` to clear or reset the reference.

**In production:** The XR enters permanent `ReconcileError` state with error `cannot fetch Composition: cannot get Composition: Composition.apiextensions.crossplane.io "..." not found`. The controller-runtime backoff limiter caps retries but the error never resolves. If owner-reference GC also deletes the CompositionRevision, the situation is even worse -- both the Composition and revision are gone, and the XR's `compositionRevisionRef` also becomes dangling.

**Kamera harness limitation:** The current harness does not model periodic `RequeueAfter` polling for the CompositeReconciler, which means the CompositeReconciler never fires in the traces. To fully exercise this bug in Kamera, either:
1. Register a Watches mapping from Composition deletions to XWidget reconciles, or
2. Model the CompositeReconciler's periodic RequeueAfter as an async enqueue source.

**Severity:** Medium-High. Any user who deletes a Composition that has bound XRs will strand those XRs in a permanent error state requiring manual intervention.

**Idiomatic fix options (in Crossplane):**

Option A -- Prevention via finalizer (as suggested by the TODO):
```go
// In SelectComposition, after selecting a Composition:
if err := r.finalizer.AddFinalizer(ctx, comp); err != nil {
    return errors.Wrap(err, "cannot add finalizer to selected Composition")
}
```

Option B -- Recovery via compositionRef reset:
```go
// In Fetch, when Composition is NotFound:
comp := &v1.Composition{}
if err := f.client.Get(ctx, ref, comp); err != nil {
    if kerrors.IsNotFound(err) {
        // Clear stale compositionRef so SelectComposition can re-evaluate
        cr.SetCompositionReference(nil)
        if updateErr := f.client.Update(ctx, cr); updateErr != nil {
            return nil, errors.Wrap(updateErr, "cannot clear stale composition reference")
        }
        return nil, errors.New("Composition was deleted; cleared compositionRef for re-selection")
    }
    return nil, errors.Wrap(err, errGetComposition)
}
```
