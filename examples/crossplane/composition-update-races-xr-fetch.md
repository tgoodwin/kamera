# Workflow Analysis: crossplane-staleness/composition-update-races-xr-fetch

## What was run

**Workflow:** `workflow_crossplane-staleness_composition-update-races-xr-fetch.json`

**Setup:** FunctionRevision + Composition + CompositionRevision(rev-1) + XWidget (bound, Automatic policy). User action: UPDATE Composition (change `writeConnectionSecretsToNamespace` from `default` to `crossplane-system`).

**Tuning:** `permuteControllers: [CompositeReconciler, CompositionReconciler]`, `staleReads: {CompositeReconciler: [Composition, CompositionRevision]}`, `staleLookback: {Composition: 1, CompositionRevision: 2}`

**Command:**
```bash
go run . -inputs workflow_crossplane-staleness_composition-update-races-xr-fetch.json \
  -interactive=false -log-level=info \
  -output=/tmp/workflow-composition-update-races
```

**Note:** The original workflow JSON had `"uid": ""` in the CompositionRevision's ownerReference, which didn't match the deterministic UID (`331xxcxr`) that Kamera assigns to the Composition. This was corrected to `"uid": "331xxcxr"` before running.

## Observations

### Reference run (no stale reads, no controller permutation)
- **uniqueNodeVisits:** 3
- **totalNodeVisits:** 3
- **uniqueResourceStates:** 3
- **Terminal states:** 1 (errored)
- **Error:** `selected CompositionRevision widget-composition-4e01990 does not have a valid function pipeline: pipeline status unknown`

The reference run itself does not converge. The execution path is:
1. `CompositionReconciler` creates new revision `widget-composition-4e01990` (rev 2)
2. `CompositionRevisionReconciler` validates the OLD revision `widget-composition-rev-1` (rev 1) -- not the new one
3. `CompositeReconciler` fetches the latest revision (rev 2, via `LatestRevision`), finds it lacks `ValidPipeline` condition, and errors

### Rerun (with stale reads and controller permutation)
- **uniqueNodeVisits:** 97
- **totalNodeVisits:** 122
- **uniqueResourceStates:** 16
- **Terminal states:** 8 unique, 13 total entries
  - 10 end with "pipeline status unknown" error
  - 3 hit max depth (due to known unconditional Status().Update bug in CompositionRevisionReconciler)
  - 0 converge successfully

No exploration path converges to a stable, error-free state.

## Bug Finding: Race between CompositionRevision creation and validation

**Controller:** `CompositeReconciler` (composite/reconciler.go) interacting with `CompositionReconciler` (composition/reconciler.go) and `CompositionRevisionReconciler` (revision/reconciler.go)

**Version:** crossplane v2.1.0

**Severity:** Medium -- causes transient reconcile errors in production; controller-runtime's exponential backoff eventually resolves the issue when the revision gets validated, but XR reconciliation is delayed.

### Mechanism

1. User updates a Composition (e.g., changes `writeConnectionSecretsToNamespace`).
2. `CompositionReconciler` reconciles the Composition and creates a new `CompositionRevision` (rev 2) with the updated spec. The new revision is created WITHOUT the `ValidPipeline` condition.
3. The `CompositionRevisionReconciler` is triggered for the new revision, but may not run immediately. In the meantime, the old revision (rev 1) may be reconciled instead, or other controllers may run first.
4. `CompositeReconciler` reconciles the XR. Because `compositionUpdatePolicy: Automatic`, `APIRevisionFetcher.Fetch()` calls `LatestRevision(comp, revisions)` which selects the newest revision (rev 2) by revision number.
5. The reconciler then checks `rev.GetCondition(v1.TypeValidPipeline)` at `reconciler.go:633`. Since the `CompositionRevisionReconciler` hasn't validated rev 2 yet, the condition is missing (status != True).
6. The reconciler returns an error: `"selected CompositionRevision ... does not have a valid function pipeline: pipeline status unknown"`.
7. The XR is marked with `ReconcileError` condition.

### Why this is a real bug (not just a test artifact)

This race exists even WITHOUT stale reads. In the reference run (fresh reads only), the same error occurs because the `CompositeReconciler` can be triggered (via ownerReference fanout from the new CompositionRevision creation) before the `CompositionRevisionReconciler` validates the new revision. The ordering `CompositionReconciler -> CompositeReconciler` (without `CompositionRevisionReconciler` for the new revision in between) is a valid real-world controller scheduling order.

In production, controller-runtime's rate limiter and requeue mechanism eventually resolve this: the `CompositeReconciler` returns an error, gets requeued with backoff, and by the time it retries, the `CompositionRevisionReconciler` has validated the revision. However:
- The XR temporarily enters a `ReconcileError` state, which may alarm monitoring systems
- Under high load, the backoff delay can be significant
- If many XRs reference the same Composition, all of them hit this error simultaneously

### Evidence from traces

**Reference run path 0:**
```
Step 0: CompositionReconciler
  - effect: CREATE CompositionRevision/widget-composition-4e01990
  - observation: GET Composition/widget-composition
  - observation: LIST CompositionRevision/widget-composition-rev-1

Step 1: CompositionRevisionReconciler
  - effect: UPDATE CompositionRevision/widget-composition-rev-1  (validates OLD rev, not new)
  - observation: GET CompositionRevision/widget-composition-rev-1
  - observation: LIST FunctionRevision/kamera-stub-rev

Step 2: CompositeReconciler
  - error: "selected CompositionRevision widget-composition-4e01990 does not have a valid function pipeline: pipeline status unknown"
```

The critical observation: In Step 1, the `CompositionRevisionReconciler` validates `widget-composition-rev-1` (the old revision), not `widget-composition-4e01990` (the newly created revision). The new revision sits unvalidated when the `CompositeReconciler` picks it up.

### Stale reads amplify the problem

With stale reads enabled (`staleLookback: {CompositionRevision: 2}`), the problem worsens because:
- Even after the `CompositionRevisionReconciler` validates the new revision, the `CompositeReconciler` may read a stale (pre-validation) version
- 10 out of 13 terminal states hit this error (vs 1 out of 1 in the reference run)
- No paths converge successfully

### Idiomatic fix (in Crossplane)

The `APIRevisionFetcher.Fetch` method should filter out revisions that don't have `ValidPipeline: True`:

```go
func (f *APIRevisionFetcher) Fetch(ctx context.Context, cr resource.Composite) (*v1.CompositionRevision, error) {
    // ... existing code to get comp and list revisions ...

    // Filter to only validated revisions before selecting latest
    validated := make([]v1.CompositionRevision, 0, len(rl.Items))
    for _, rev := range rl.Items {
        if c := rev.GetCondition(v1.TypeValidPipeline); c.Status == corev1.ConditionTrue {
            validated = append(validated, rev)
        }
    }

    latest := v1.LatestRevision(comp, validated)
    // ...
}
```

Alternatively, the `CompositionReconciler` could set `ValidPipeline` at creation time (since it has all the information needed), eliminating the two-phase create-then-validate pattern.

## Additional observation: Known bug still present

The `CompositionRevisionReconciler` unconditional `Status().Update()` bug (documented in BUGS.md) is also visible in this scenario. The 3 max-depth terminal states are caused by the infinite reconcile cycle from this known bug. `totalNodeVisits: 122` vs `uniqueNodeVisits: 97` reflects both the known bug and the new race condition.

## Regarding the original target bug (stale Composition UID in LatestRevision)

The original hypothesis was that a stale Composition `Get` mixed with a fresh revision `List` would cause `LatestRevision`'s `metav1.IsControlledBy` check to fail due to UID mismatch. This specific scenario is NOT triggered in this harness because:

1. The Composition is only updated (not deleted and recreated), so its UID remains the same across versions
2. Kamera assigns deterministic UIDs based on group/kind/namespace/name, which don't change across updates

The stale-UID scenario would require the Composition to be deleted and recreated with a new UID, which is a different (and rarer) operational scenario. However, the race condition found above is a distinct, practically significant bug that affects normal Composition updates.
