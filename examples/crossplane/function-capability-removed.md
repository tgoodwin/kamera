# Workflow: crossplane-staleness/function-capability-removed

## Summary

This investigation exercised the cross-controller trust chain between `CompositionRevisionReconciler` and `CompositeReconciler` when a FunctionRevision's capabilities change. The scenario removes the `"composition"` capability from a FunctionRevision and explores whether stale reads allow the CompositeReconciler to use an invalidated pipeline.

**Finding:** There is a real concurrency vulnerability where the CompositeReconciler successfully composes resources using a function whose required capability has been removed, because it trusts the stale `ValidPipeline=True` condition on the CompositionRevision.

## What was run

Workflow file: `workflow_crossplane-staleness_function-capability-removed.json`

```bash
cd examples/crossplane
go run . -inputs workflow_crossplane-staleness_function-capability-removed.json \
  -interactive=false -log-level=info \
  -output=/tmp/workflow-function-capability-v4
```

**Tuning:**
- `permuteControllers: [CompositeReconciler, CompositionRevisionReconciler]`
- `staleReads: {CompositionRevisionReconciler: [pkg.crossplane.io/FunctionRevision]}`
- `staleLookback: {pkg.crossplane.io/FunctionRevision: 1}`
- `userActionReadyDepths: {"0": 0}` (inject capability removal immediately)

**Results:**
- Reference: 4 unique node visits, 4 resource states, 1 converged state
- Rerun (with perturbations): 78 unique node visits, 17 resource states, 23 converged states

## Harness fixes required

Two fixes were needed to make this workflow operational:

### 1. Missing FunctionRevision watch registration

The Kamera harness did not register a watch for `FunctionRevision` changes on the `CompositionRevisionReconciler`. In Crossplane production, `revision.Setup()` registers:

```go
Watches(&pkgv1.FunctionRevision{}, EnqueueCompositionRevisionsForFunctionRevision(mgr.GetClient(), o.Logger))
```

This watch maps FunctionRevision changes to CompositionRevision reconcile requests. Without it, the FunctionRevision update produced no reconciler triggers, and the exploration converged immediately with zero state changes.

**Fix:** Added `.Watches("pkg.crossplane.io/FunctionRevision", functionRevisionToCompositionRevisionMapper())` to the `CompositionRevisionReconciler` builder in `scenario.go`. The mapper extracts the function package name from the FunctionRevision's labels and returns a reconcile request for the known CompositionRevision.

### 2. Incorrect ownerReference UID

The workflow JSON had `"uid": ""` in the CompositionRevision's ownerReference to the Composition. The Composition gets a deterministic UID (`331xxcxr`) from Kamera, but the empty UID caused the CompositionReconciler to fail ownership checks.

**Fix:** Set `"uid": "331xxcxr"` in the ownerReference.

### 3. Missing userActionReadyDepths

The workflow JSON did not specify `userActionReadyDepths`, so the user action (removing function capability) was only applied after convergence. Due to the known Bug #1 (unconditional `Status().Update()` causing infinite reconcile cycles), convergence never occurred, and the user action was never applied.

**Fix:** Added `"userActionReadyDepths": {"0": 0}` to apply the user action immediately at depth 0.

## Source code analysis

### CheckCapabilities (revision reconciler)

File: `internal/xfn/capabilities.go` (Crossplane v2.1.0)

`RevisionCapabilityChecker.CheckCapabilities()` lists ALL FunctionRevisions via `c.client.List()`, finds the active revision for each named function (matching by `pkg.crossplane.io/package` label), and checks if it has the required capabilities. If any function is missing a capability, it returns an error.

Key code path in `internal/controller/apiextensions/revision/reconciler.go`:
```go
if err := r.functions.CheckCapabilities(ctx, []string{pkgmetav1.FunctionCapabilityComposition}, names...); err != nil {
    status.MarkConditions(xpv1.ReconcileSuccess(), v1.MissingCapabilities(err.Error()))
    return reconcile.Result{}, r.client.Status().Update(ctx, rev)
}
status.MarkConditions(xpv1.ReconcileSuccess(), v1.ValidPipeline())
return reconcile.Result{}, r.client.Status().Update(ctx, rev)
```

### ValidPipeline trust chain (composite reconciler)

File: `internal/controller/apiextensions/composite/reconciler.go` (Crossplane v2.1.0)

The CompositeReconciler performs a simple condition check:
```go
if c := rev.GetCondition(v1.TypeValidPipeline); c.Status != corev1.ConditionTrue {
    err := errors.Errorf("selected CompositionRevision %s does not have a valid function pipeline: %s", rev.GetName(), msg)
    status.MarkConditions(xpv1.ReconcileError(err))
    _ = r.client.Status().Update(ctx, xr)
    ...
}
```

The CompositeReconciler does NOT independently verify function capabilities. It fully trusts the `ValidPipeline` condition set by the CompositionRevisionReconciler.

## Bug hypothesis: Stale ValidPipeline enables composition with invalidated functions

**Severity:** P2 (data integrity risk, but eventually self-correcting)

**Mechanism:**

When a FunctionRevision's capabilities change (e.g., the `"composition"` capability is removed), there is a window where the CompositeReconciler can compose resources using the now-invalid function pipeline. This occurs due to the cross-controller trust chain:

1. Initial state: FunctionRevision has `capabilities: ["composition"]`, CompositionRevision has `ValidPipeline=True`
2. User/operator removes the capability: FunctionRevision updated to `capabilities: []`
3. **Race window:** The CompositeReconciler runs before the CompositionRevisionReconciler re-validates the pipeline
4. CompositeReconciler reads CompositionRevision, sees `ValidPipeline=True` (stale condition), and proceeds to compose
5. CompositeReconciler calls the function (which no longer declares the "composition" capability) and applies its output
6. CompositionRevisionReconciler eventually runs, detects missing capability, sets `ValidPipeline=False`
7. Future CompositeReconciler runs now correctly reject the pipeline

**Evidence from traces:**

In 4 out of 23 converged states (all aborted due to the infinite loop from Bug #1), the CompositeReconciler successfully composed resources AFTER the capability was removed. The consistent pattern across all affected paths:

```
Step 0: [External User] removes composition capability
Step 1: [CompositeReconciler] reads XWidget, selects revision (still has ValidPipeline=True)
Step 2: [CompositeReconciler] SUCCESSFULLY composes resources (creates ConfigMap, updates XWidget)
Step 3+: [CompositionRevisionReconciler] eventually detects MissingCapabilities, sets ValidPipeline=False
```

The system eventually self-corrects (all 23 converged states end with `ValidPipeline=False`), but during the race window, resources are composed using a function that should have been rejected.

**Impact:**

- In production, the race window is bounded by controller-runtime's workqueue latency (typically milliseconds to seconds)
- The FunctionRevision watch handler fans out to ALL CompositionRevisions referencing the function, so the re-validation happens promptly
- However, if the CompositeReconciler's reconcile loop runs faster than the CompositionRevisionReconciler's, the stale validation window could result in one or more composition cycles using an invalidated function
- This could produce incorrect or unexpected composed resources if the function's behavior changes when its capabilities change

**Root cause:**

The CompositeReconciler uses a cached condition (`ValidPipeline`) as a gate, but this condition can be stale with respect to the underlying FunctionRevision state. There is no mechanism for the CompositeReconciler to detect that the condition is outdated.

**Possible mitigations (in Crossplane):**

1. **Add a generation/hash check:** The CompositionRevision's `ValidPipeline` condition could include the FunctionRevision's resourceVersion or a hash of its capabilities. The CompositeReconciler could compare this against the current FunctionRevision state.
2. **Double-check in CompositeReconciler:** The CompositeReconciler could independently verify function capabilities before composing, rather than solely trusting the condition.
3. **Optimistic locking:** Use a revision-based check on the FunctionRevision when composing to detect concurrent changes.

## Relationship to Bug #1

This investigation also confirmed that Bug #1 (unconditional `Status().Update()` in CompositionRevisionReconciler) actively interferes with testing. The infinite reconcile loop prevents state convergence, which in turn prevents user actions from being applied at their natural scheduling point (convergence). The `userActionReadyDepths` workaround was necessary to force the user action at depth 0.
