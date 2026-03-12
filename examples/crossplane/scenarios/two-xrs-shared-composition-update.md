# Scenario: two-xrs-shared-composition-update

## Setup

Two XWidgets (xr-one, xr-two) with `compositionUpdatePolicy: Automatic`, both bound to the same Composition (`widget-composition-shared`) and initially pointing to `widget-composition-shared-rev-1` (validated). A user action updates the Composition's `writeConnectionSecretsToNamespace` from `default` to `crossplane-system`.

**Tuning:**
- `permuteControllers`: CompositionReconciler, CompositionRevisionReconciler, CompositeReconciler
- `staleReads`: CompositeReconciler reads stale CompositionRevision
- `staleLookback`: CompositionRevision lookback of 2

**Command:**
```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-concurrency_two-xrs-shared-composition-update.json \
  -interactive=false -log-level=info -output=/tmp/workflow-two-xrs -depth 10
```

## Workflow fix required

The workflow JSON required a fix before it could run: the `ownerReferences[].uid` on the CompositionRevision was set to `""`, but `EnsureDeterministicIdentity` assigns the Composition a deterministic UID (`1r9jr5wn`). The mismatch caused `controllerutil.SetControllerReference` to fail with "already controlled by Composition widget-composition-shared (UID )". Fixed by setting `"uid": "1r9jr5wn"` in the ownerReference.

This indicates a gap in `buildStartStateFromObjects` (in `pkg/tracecheck/start_state.go`): after assigning deterministic UIDs to all objects, it does not patch up ownerReference UIDs to match. Any workflow with pre-existing ownerReferences must manually compute and set the correct deterministic UID.

## Exploration results

| Metric | Value |
|--------|-------|
| Unique node visits | 414 |
| Total node visits | 462 |
| Unique resource states | 66 |
| Recorded terminal states | 59 |
| Pipeline-unknown errors | 40 (68%) |
| Max-depth reached | 19 (32%) |

## Key findings

### 1. Original divergence hypothesis is not reproducible under the current staleness model

The target hypothesis was: under stale reads, one XR pins to rev-1 while the other advances to rev-2, causing permanent divergence.

**This cannot occur in the Kamera staleness model.** Staleness is applied per-reconciler-ID, not per-reconcile-invocation. When `CompositeReconciler` has a stale read position, ALL XR reconciliations in that branch (both xr-one and xr-two) see the same stale CompositionRevision list. Therefore:
- In stale branches: both XRs see only rev-1 and both stay on rev-1
- In fresh branches: both XRs see rev-1 + rev-2 and both select rev-2

No single branch can produce asymmetric revision selection between two XRs sharing the same reconciler.

**Note:** This does not mean the real-world bug is impossible. In production Kubernetes, informer cache updates between two consecutive reconcile invocations of the same controller could cause one invocation to see stale data while the next sees fresh data. The Kamera staleness model is a coarser approximation that applies staleness uniformly within a branch.

### 2. Pipeline validation ordering creates a hard error wall

In 40 of 59 terminal states (68%), the exploration was aborted because the CompositeReconciler selected the newly created rev-2 before the CompositionRevisionReconciler validated its pipeline. The error:

```
selected CompositionRevision widget-composition-shared-4e01990 does not have a valid function pipeline: pipeline status unknown
```

This is a legitimate race condition that occurs in real Crossplane deployments. The `APIRevisionFetcher.Fetch()` method in `internal/controller/apiextensions/composite/api.go` selects the latest revision by revision number (via `LatestRevision()`) regardless of whether the revision's pipeline has been validated. The pipeline check happens AFTER `Fetch` returns, at `reconciler.go:633`.

Critically, `Fetch` performs a side-effecting `client.Update` on the XR (setting `compositionRevisionRef` to rev-2) at `api.go:191` BEFORE the pipeline validity check. This means:

1. The XR's `compositionRevisionRef` is persisted to rev-2
2. The pipeline check fails
3. The reconcile returns an error
4. On retry, `Fetch` finds `current.Name == latest.GetName()` (both are rev-2), so it does not re-update
5. The reconcile loops on the pipeline-unknown error until the CompositionRevisionReconciler validates rev-2

This is not a bug per se (it's a documented retry pattern), but it means the CompositeReconciler will error-loop for every newly created revision until the CompositionRevisionReconciler runs. The Kamera explorer treats these errors as fatal for the branch, preventing exploration of post-validation behavior.

### 3. Known bug (unconditional status writes) dominates max-depth states

In all 19 max-depth states, both XRs have empty `compositionRevisionRef` -- the CompositeReconciler never ran on them after the user action. The CompositionReconciler and CompositionRevisionReconciler enter the known infinite loop (unconditional `Status().Update()` on CompositionRevision triggers Composition re-reconcile via ownerReference fanout), consuming all available depth.

The CompositeReconciler is registered `.ForGK(example.org/XWidget)` and is only triggered by XWidget changes. After the user action updates the Composition, no XWidget changes occur, so the CompositeReconciler is never re-triggered. In production Crossplane, a periodic `RequeueAfter` (60s poll interval) eventually triggers the CompositeReconciler, but the harness does not model periodic requeue.

### 4. Distinct object versions observed

Both XRs were observed individually with three possible `compositionRevisionRef` values across all branches:
- `EMPTY` (not yet reconciled after user action)
- `widget-composition-shared-rev-1` (selected via stale read showing only rev-1)
- `widget-composition-shared-4e01990` (selected via fresh read showing rev-2)

However, in states where both XRs have non-empty refs, they always agree (both on rev-1). The rev-2 selection only occurs in branches that immediately abort due to the pipeline validation error.

## Assessment

No new Crossplane bug was found in this scenario. The target divergence cannot manifest under the current Kamera staleness model because staleness is per-reconciler rather than per-reconcile-invocation. The primary blockers to deeper exploration are the known unconditional status-write bug and the pipeline validation error causing branch abandonment.

**Potential improvements to the Kamera harness to better explore this scenario:**
1. Fix `buildStartStateFromObjects` to automatically patch ownerReference UIDs after assigning deterministic identity
2. Treat transient reconcile errors (like pipeline-not-yet-validated) as tolerable/retriable rather than branch-terminating
3. Consider per-invocation staleness (rather than per-reconciler) to model informer cache updates between reconcile invocations

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-concurrency_two-xrs-shared-composition-update.json \
  -interactive=false -log-level=info -depth 30 \
  -output=/tmp/rerun-two-xrs-shared
```

Ran at depth 10, 20, and 30 to check convergence trends.

### Campaign metrics

**Reference run (depth=30):**
```
  unique node visits:        19
  total node visits:         31
  unique resource states:    9
  aborted states:            1
  max-depth aborted states:  1
```

**Rerun (depth=30):**
```
  unique node visits:        536
  total node visits:         669
  unique resource states:    66
  aborted states:            6
  max-depth aborted states:  6
```

**Depth progression (rerun):**

| Depth | Unique nodes | Total visits | Aborted states | Resource states |
|-------|-------------|--------------|----------------|-----------------|
| 10    | 516         | 564          | 36             | 66              |
| 20    | 536         | 609          | 6              | 66              |
| 30    | 536         | 669          | 6              | 66              |

### Answers to key questions

1. **Did the reference run converge?** No. It cycles (19 unique nodes at depth=20 and depth=30; total visits grow linearly).
2. **Did the reference run hit max depth?** Yes -- pure cycle. Unique nodes plateau at 19.
3. **Did the perturbed runs converge?** No. All 6 terminal states are max-depth aborts.
4. **Did the perturbed runs hit max depth?** Yes. Unique nodes plateau at 536 between depth=20 and depth=30, confirming all state space is explored but no path converges.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** Rerun explores significantly more state space (536 vs 19 unique nodes, 66 vs 9 resource states) due to controller permutations and staleness. Previous run had 414 unique nodes; now 536 with better harness support.

### What changed vs previous conclusions

Previously: 414 unique nodes, 0 converged. 68% pipeline-unknown errors, 32% max-depth. The pipeline-unknown errors blocked exploration of post-validation behavior.

Now with recent commits:
- **Commit `38ff304d` (tolerate reconciler errors):** All 40 pipeline-unknown errors from before are now tolerated and re-enqueued. This allows the exploration to proceed past the validation race. The 68% of branches that previously terminated at "pipeline status unknown" now continue exploring. This is a major improvement.
- **Commit `d2935ba9` (auto-fixup ownerReference UIDs):** The manual UID fix in the workflow JSON is no longer needed. The harness automatically patches ownerReference UIDs.
- **State space growth:** 516->536 unique nodes (vs 414 before), 66 resource states (same as before). The additional nodes come from error-tolerance paths that were previously dead ends.
- **All paths now cycle** due to the unconditional `Status().Update()` bug in CompositionRevisionReconciler creating infinite resource version increments.

**The original assessment remains valid:** No new Crossplane bug found beyond the known issues (pipeline validation race, unconditional status writes). The divergence hypothesis cannot manifest under the per-reconciler staleness model. Both XRs always see the same stale/fresh view of CompositionRevisions within a given branch.

### Previously-reported issues resolved by recent commits

- **Improvement #1 (auto-fix ownerReference UIDs):** Fully resolved by commit `d2935ba9`. No manual UID computation needed.
- **Improvement #2 (tolerate transient errors):** Fully resolved by commit `38ff304d`. Pipeline-unknown errors are now re-enqueued, enabling exploration past the validation race. This was the main blocker for this scenario.
- **Improvement #3 (per-invocation staleness):** Not addressed by recent commits. This would require a deeper architectural change to the staleness model.
- **The unconditional Status().Update() bug (Bug #1)** remains the dominant cause of non-convergence across all paths.
