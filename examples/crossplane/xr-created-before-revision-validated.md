# Scenario: xr-created-before-revision-validated

## Bug Found: CompositeReconciler fails on unvalidated CompositionRevision during bootstrap

**Controller:** `composite/reconciler.go` (CompositeReconciler)

**Version:** crossplane v2.1.0

**Severity:** Medium -- causes guaranteed transient errors and backoff delays during XR bootstrap when Composition and XR are created close together.

## Summary

When a Composition and XR are created simultaneously (or nearly so), the CompositeReconciler will always fail on its first attempt because it depends on a CompositionRevision having `ValidPipeline=True`, which requires two other controllers to run first. There is no ordering of the three controllers that avoids at least one error during bootstrap.

## Mechanism

Three controllers participate in the bootstrap of an XR backed by a pipeline-mode Composition:

1. **CompositionReconciler** reconciles the Composition and creates a CompositionRevision owned by it.
2. **CompositionRevisionReconciler** reconciles the newly created CompositionRevision, checks function capabilities, and sets `ValidPipeline=True` on the revision's status.
3. **CompositeReconciler** reconciles the XR, selects a CompositionRevision, and requires `ValidPipeline=True` before proceeding.

The fundamental problem is that the CompositeReconciler (step 3) requires output from both step 1 AND step 2 before it can succeed. Kamera's exhaustive ordering exploration shows that ALL possible controller orderings result in at least one error:

- **CompositeReconciler runs first:** Fails with "no compatible CompositionRevisions found" (no revision exists yet).
- **CompositionReconciler runs, then CompositeReconciler:** Fails with "selected CompositionRevision does not have a valid function pipeline: pipeline status unknown" (revision exists but ValidPipeline not set).
- **CompositionReconciler + CompositionRevisionReconciler run, then CompositeReconciler:** Succeeds, but this is the ONLY valid ordering. Any interleaving or reordering causes errors.

The code responsible is in `composite/reconciler.go` lines 631-646:

```go
if c := rev.GetCondition(v1.TypeValidPipeline); c.Status != corev1.ConditionTrue {
    msg := "pipeline status unknown"
    if c.Message != "" {
        msg = c.Message
    }
    err := errors.Errorf("selected CompositionRevision %s does not have a valid function pipeline: %s", rev.GetName(), msg)
    r.record.Event(xr, event.Warning(reasonCompose, err))
    status.MarkConditions(xpv1.ReconcileError(err))
    _ = r.client.Status().Update(ctx, xr)
    return reconcile.Result{}, err
}
```

This is a hard error (not a requeue), which causes controller-runtime's exponential backoff. The controller will eventually succeed once the revision is validated, but the initial backoff penalty is unnecessary.

## Production Impact

In production, controller-runtime's workqueue dampers/rate limiters mean this bug manifests as:

- **Guaranteed error events** (`Warning/ComposeResources`) on every XR created before its Composition's revision is validated.
- **Unnecessary backoff delays** -- the first error triggers exponential backoff (typically 5s, 10s, 20s...), delaying XR readiness even though the revision becomes valid almost immediately.
- **Status condition churn** -- the XR is stamped with `ReconcileError` on the error path (line 642), then overwritten once the retry succeeds.
- Under **high cluster load** or API server latency, the window for this race widens and the backoff penalty grows.

## Evidence from Traces

Kamera explored this scenario with three phases:

| Phase | Distinct States | Total States | Final States | All Errors? |
|-------|----------------|-------------|--------------|-------------|
| Reference (single ordering) | 3 | 3 | 1 | Yes |
| Rerun (permuted orderings) | 6 | 6 | 3 | Yes |
| Staleness (stale CompositionRevision reads) | 2 | 2 | 1 | Yes |

Every explored execution path ended in an error. Two distinct failure modes were observed:

1. `"cannot fetch Composition: no compatible CompositionRevisions found"` -- CompositeReconciler runs before CompositionReconciler.
2. `"selected CompositionRevision ... does not have a valid function pipeline: pipeline status unknown"` -- CompositeReconciler runs after CompositionReconciler but before CompositionRevisionReconciler.

## Distinct from Known Bug

This is distinct from the known `CompositionRevisionReconciler` unconditional `Status().Update()` bug documented in `BUGS.md`. That bug causes infinite reconcile cycling from redundant writes. This bug is about the CompositeReconciler making a hard-fail check on `ValidPipeline` status that is impossible to satisfy during bootstrap without a specific three-controller ordering.

## Suggested Fix (in Crossplane)

Option A: Treat missing/unknown `ValidPipeline` as a soft requeue rather than hard error:

```go
if c := rev.GetCondition(v1.TypeValidPipeline); c.Status != corev1.ConditionTrue {
    log.Debug("CompositionRevision pipeline not yet validated, requeueing")
    return reconcile.Result{RequeueAfter: 1 * time.Second}, nil
}
```

Option B: Have the CompositionReconciler set `ValidPipeline` on the revision it creates (inline validation at creation time), removing the need for the two-step create-then-validate flow.

## What Was Run

```bash
cd examples/crossplane
go run . \
  -inputs workflow_crossplane-staleness_xr-created-before-revision-validated.json \
  -interactive=false \
  -log-level=info \
  -output=/tmp/workflow-xr-before-validation-v5 \
  -depth 20
```

Trace output is in `/tmp/workflow-xr-before-validation-v5/`.

## Harness Changes Required

Three harness changes were needed to exercise this scenario:

1. **Initial pending reconciles for environment state objects** (`scenario.go`): `buildStateFromCoverageInput` now computes initial pending reconciles for environment state objects based on their GVK and the registered primary reconciler. Previously, environment state objects were inert and never triggered their reconcilers.

2. **UserActionReadyDepths in coverage input tuning** (`types.go`, `scenario.go`): Added `userActionReadyDepths` field to `InputTuning` so workflow JSON files can schedule user actions at specific exploration depths. This is essential for scenarios where the user action must interleave with environment object reconciliation.

3. **Preserve UserActionReadyDepths in disablePerturbations** (`parallel_runner.go`): `disablePerturbations` previously cleared `UserActionReadyDepths` along with other perturbation knobs. Since `UserActionReadyDepths` is scheduling metadata (not a perturbation), it is now preserved in reference and rerun phases.
