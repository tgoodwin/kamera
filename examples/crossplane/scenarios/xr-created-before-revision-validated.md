> **STALE**: This analysis contains outdated conclusions from multiple investigation rounds. See [ANALYSIS.md](ANALYSIS.md) for the current evidence-grounded analysis.

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

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-staleness_xr-created-before-revision-validated.json \
  -interactive=false -log-level=info \
  -output=/tmp/rerun-xr-before-validation
```

Note: The workflow JSON already has `maxDepth: 100`.

### Campaign metrics

**Reference run only (no rerun generated -- reference did not converge):**
```
  unique node visits:        12
  total node visits:         101
  unique resource states:    6
  aborted states:            1
  max-depth aborted states:  1
```

No rerun file was produced because the reference run cycled without converging, and the rerun phase requires a converged reference.

### Answers to key questions

1. **Did the reference run converge?** No. It cycles (12 unique nodes, 101 total visits at maxDepth=100). The 8:1 total/unique ratio confirms pure cycling.
2. **Did the reference run hit max depth?** Yes -- infinite cycle even at depth 100.
3. **Did the perturbed runs converge?** N/A -- no rerun was generated.
4. **Did the perturbed runs hit max depth?** N/A.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** Only reference metrics available. Previously, both reference and rerun terminated in error states (distinct states: 3/6/2 across three phases). Now, the errors are tolerated and re-enqueued, converting the error-termination pattern into infinite cycling.

### What changed vs previous conclusions

Previously, ALL paths ended in error states -- either "no compatible CompositionRevisions found" or "pipeline status unknown". There were 3 phases (reference, rerun, staleness) with distinct state counts of 3, 6, and 2 respectively.

Now, commit `38ff304d` (tolerate reconciler errors as no-ops with re-enqueue) converts these error-aborts into re-enqueues. The exploration now sees:
- CompositeReconciler errors (no revisions or unvalidated revision) -> tolerated, re-enqueued
- CompositionReconciler and CompositionRevisionReconciler make progress (create and validate revision)
- The unconditional `Status().Update()` in CompositionRevisionReconciler (Bug #1) causes additional cycling

The net result is that the exploration reaches 12 unique states (more than the 3 from before) but cycles because of the combined effect of error re-enqueues and unconditional status writes. However, no path achieves "Successfully composed" -- the bug is still present: the CompositeReconciler cannot successfully bootstrap when errors are re-enqueued because it keeps hitting the same ordering issue.

**The core bug finding (bootstrap ordering problem) remains valid and is now even more clearly demonstrated:** the error tolerance allows the system to keep trying, but it still cannot converge because the fundamental three-controller ordering constraint cannot be satisfied in a single linear traversal when the unconditional status writes keep injecting new resource versions.

### Previously-reported issues resolved by recent commits

- **All paths ended in error:** Commit `38ff304d` allows exploration past errors, which is an improvement. The errors are now tolerated rather than being hard stops.
- **OwnerReference UID fix:** Commit `d2935ba9` auto-fixes UIDs, no manual correction needed.
- **The bootstrap ordering bug is unchanged.** The CompositeReconciler still requires the specific ordering (CompositionReconciler -> CompositionRevisionReconciler -> CompositeReconciler) and fails in all other orderings.

## Re-run (2026-03-12, depth=100)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs scenarios/workflow_crossplane-staleness_xr-created-before-revision-validated.json \
  -interactive=false -log-level=info \
  -output=/tmp/depth100-xr-before-validation
```

The workflow JSON specifies `maxDepth: 100`. Attempting `-depth 400` did not override the JSON cap.

### Campaign metrics

**Depth 100 (JSON maxDepth):**
```
invocation: 79492640-3535-4938-8155-f9570143a365
  unique node visits:        12
  total node visits:         101
  unique resource states:    6
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**With -depth 400 (still capped at 100 by JSON):**
```
invocation: c724d35c-5b91-48d7-ad9c-b702c50010af
  unique node visits:        12
  total node visits:         101
  unique resource states:    6
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

Only the reference file was produced (no rerun), consistent with the 2026-03-11 behavior where non-converging reference runs prevent rerun generation.

### Answers to key questions

1. **Did the reference run converge?** No. Cycles through 12 unique nodes and 6 resource states.
2. **Did the reference run hit max depth?** Yes -- at maxDepth=100 (JSON cap).
3. **Did the perturbed run(s) converge?** N/A -- no rerun generated (reference did not converge).
4. **Did the perturbed runs hit max depth?** N/A.
5. **Are there multiple distinct converged states?** No -- zero converged states.
6. **How do the campaign-metrics compare?** Only reference run available. Identical to 2026-03-11 reference run.

### Comparison with previous runs

| Metric | Original (3 phases) | 2026-03-11 Ref | 2026-03-12 |
|--------|---------------------|----------------|------------|
| Unique node visits | 3/6/2 | 12 | 12 |
| Total node visits | 3/6/2 | 101 | 101 |
| Unique resource states | 3/3/2 | 6 | 6 |
| Converged states | 0 (all errors) | 0 (cycling) | 0 (cycling) |
| Max-depth aborted | 0 | 1 | 1 |

The 2026-03-12 run exactly reproduces the 2026-03-11 reference run. The JSON maxDepth=100 remains the effective ceiling. No new findings.

### Updated conclusions

The **bootstrap ordering bug** is robustly confirmed across three separate runs. The CompositeReconciler cannot bootstrap an XR without the specific three-controller ordering (CompositionReconciler -> CompositionRevisionReconciler -> CompositeReconciler), and the unconditional `Status().Update()` in CompositionRevisionReconciler prevents convergence even when errors are tolerated. The proposed fixes (soft requeue for unvalidated revisions, or inline validation at creation time) remain valid.
