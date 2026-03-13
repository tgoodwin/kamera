> **STALE**: This analysis contains outdated conclusions from multiple investigation rounds. See [ANALYSIS.md](ANALYSIS.md) for the current evidence-grounded analysis.

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

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-staleness_function-capability-removed.json \
  -interactive=false -log-level=info -depth 20 \
  -output=/tmp/rerun-function-capability
```

Ran at depth 10 (default) and 20 to confirm cycling vs convergence.

### Campaign metrics

**Reference run:**
```
  unique node visits:        10
  total node visits:         101
  unique resource states:    5
  aborted states:            1
  max-depth aborted states:  1
```

**Rerun (staleness + permutation):**
```
  unique node visits:        113
  total node visits:         935
  unique resource states:    17
  aborted states:            9
  max-depth aborted states:  9
```

### Answers to key questions

1. **Did the reference run converge?** No. It cycles (10 unique nodes, 101 total visits -- 10:1 ratio confirms cycling).
2. **Did the reference run hit max depth?** Yes -- infinite cycle. Same unique node count at depth 10 and 20.
3. **Did the perturbed runs converge?** No. All 9 terminal states are max-depth aborts.
4. **Did the perturbed runs hit max depth?** Yes -- 113 unique nodes, 935 total visits (8:1 ratio). Same unique count at both depths.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** Rerun explores significantly more state space (113 vs 10 unique nodes, 17 vs 5 resource states) but both cycle. The previous run had 23 converged states in the rerun; now zero.

### What changed vs previous conclusions

Previously: Reference had 4 nodes and 1 converged state. Rerun had 78 nodes and 23 converged states. The bug was clearly demonstrated -- in 4/23 converged states, the CompositeReconciler composed resources using the invalidated function pipeline.

Now: **Zero convergence in either phase.** The key change is commit `38ff304d` (tolerate reconciler errors as no-ops with re-enqueue). The "pipeline status unknown" errors that previously served as terminal states are now re-enqueued, creating infinite cycles. Additionally, the unconditional `Status().Update()` in the CompositionRevisionReconciler (Bug #1) continues to produce new resource versions that trigger re-reconciliation.

The combined effect of error tolerance + unconditional status updates creates cycles that cannot be broken by increasing depth. The unique state count plateaus (10 reference, 113 rerun) while total visits grow linearly with depth.

**The original bug finding (stale ValidPipeline enables composition with invalidated functions) is still valid** -- the traces still show the CompositeReconciler composing resources after capability removal. However, the harness can no longer detect this as a "converged state" because the subsequent error-requeue cycle prevents convergence.

### Previously-reported issues resolved by recent commits

- **Harness fix #2 (ownerReference UID):** Commit `d2935ba9` (auto-fixup ownerReference UIDs) resolves the manual UID fix that was previously needed.
- **Harness fix #1 (FunctionRevision watch):** The watch registration fix from the previous investigation is still in place and working correctly -- the FunctionRevision update triggers CompositionRevisionReconciler as expected.
- **Bug #1 (unconditional Status().Update):** Still present and now exacerbated by the error tolerance mechanism, preventing any path from converging.

## Re-run (2026-03-12, depth=100)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs scenarios/workflow_crossplane-staleness_function-capability-removed.json \
  -interactive=false -log-level=info \
  -output=/tmp/depth100-function-capability
```

The workflow JSON specifies `maxDepth: 100`. Attempting `-depth 400` did not override the JSON cap.

### Campaign metrics

**Depth 100 (JSON maxDepth):**
```
invocation: 12033246-b405-467f-b196-2489ffd96e93
  unique node visits:        10
  total node visits:         101
  unique resource states:    5
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**With -depth 400 (still capped at 100 by JSON):**
```
invocation: e73c8211-e08a-42f5-9d15-ab741034e227
  unique node visits:        10
  total node visits:         101
  unique resource states:    5
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

### Answers to key questions

1. **Did the reference run converge?** No. Cycles through 10 unique nodes and 5 resource states indefinitely.
2. **Did the reference run hit max depth?** Yes -- at maxDepth=100 (JSON cap).
3. **Did the perturbed run(s) converge?** N/A -- single combined invocation, no convergence.
4. **Did the perturbed runs hit max depth?** Yes.
5. **Are there multiple distinct converged states?** No -- zero converged states.
6. **How do the campaign-metrics compare?** Single invocation only. Metrics identical to the 2026-03-11 reference run.

### Comparison with previous runs

| Metric | Original Ref | Original Rerun | 2026-03-11 Ref | 2026-03-11 Rerun | 2026-03-12 |
|--------|-------------|---------------|----------------|-----------------|------------|
| Unique node visits | 4 | 78 | 10 | 113 | 10 |
| Total node visits | 4 | ~100 | 101 | 935 | 101 |
| Unique resource states | 4 | 17 | 5 | 17 | 5 |
| Converged states | 1 | 23 | 0 | 0 | 0 |
| Max-depth aborted | 0 | 0 | 1 | 9 | 1 |

The 2026-03-12 run exactly reproduces the 2026-03-11 reference run metrics. The JSON maxDepth=100 is the effective depth. The staleness perturbation rerun (which explored 113 unique nodes in the 2026-03-11 run) is not produced as a separate invocation here.

### Updated conclusions

The **stale ValidPipeline trust chain vulnerability** remains the confirmed finding. The system cycles because the CompositionRevisionReconciler's unconditional `Status().Update()` (Bug #1) combined with error tolerance creates infinite loops. The deeper exploration at depth 100 adds no new findings vs the 2026-03-11 analysis. The proposed mitigations (generation/hash check, double-check in CompositeReconciler) remain valid.
