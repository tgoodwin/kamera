> **STALE**: This analysis contains outdated conclusions from multiple investigation rounds. See [ANALYSIS.md](ANALYSIS.md) for the current evidence-grounded analysis.

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

## Re-run (2026-03-11)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs workflow_crossplane-deletion_composition-deleted-while-xr-bound.json \
  -interactive=false -log-level=info -depth 40 \
  -output=/tmp/rerun-composition-deleted
```

Ran at depth 10 (default), 20, and 40 to rule out max-depth issues.

### Campaign metrics

**Reference run:**
```
  unique node visits:        15
  total node visits:         41
  unique resource states:    6
  aborted states:            1
  max-depth aborted states:  1
```

**Rerun (staleness perturbations):**
```
  unique node visits:        115
  total node visits:         212
  unique resource states:    16
  aborted states:            3
  max-depth aborted states:  3
```

### Answers to key questions

1. **Did the reference run converge?** No. It hit max depth even at depth=40 with only 15 unique nodes, indicating an infinite cycle.
2. **Did the reference run hit max depth?** Yes -- the exploration cycles and never reaches a fixed point. Increasing depth from 10 to 20 to 40 did not help (same 15 unique nodes, just more total visits).
3. **Did the perturbed runs converge?** No. All 3 terminal states are max-depth aborts.
4. **Did the perturbed runs hit max depth?** Yes -- same cycling behavior with 115 unique nodes explored.
5. **Are there multiple distinct converged states?** N/A -- no states converged.
6. **How do the campaign-metrics compare?** The rerun explores significantly more state space (115 unique nodes vs 15) due to staleness perturbation combinations, but both ultimately cycle.

### What changed vs previous conclusions

Previously, both reference and rerun converged to the same state (`qjpgnp1v`) at depth ~3. The CompositeReconciler never fired because it was only triggered by XWidget changes.

Now, due to recent Kamera commits (particularly `38ff304d` -- tolerate reconciler errors as no-ops with re-enqueue, and broader harness fixes in `c968dd97`), the **CompositeReconciler now fires** and its error is tolerated and re-enqueued. This creates an infinite cycle:

1. CompositeReconciler runs, tries to fetch the deleted Composition, gets an error
2. Error is tolerated as no-op, CompositeReconciler is re-enqueued
3. On re-enqueue, it hits the same error again
4. This repeats forever, never converging

This is actually a **more accurate model of production behavior** -- in real Crossplane, the CompositeReconciler would indeed enter an infinite error-retry loop when its Composition is deleted. The previous analysis noted this as a "harness limitation" that the CompositeReconciler never fired. That limitation is now resolved, and the infinite loop confirms the original bug hypothesis: the orphaned `compositionRef` with no recovery path causes permanent error cycling.

### Previously-reported issues resolved by recent commits

- **Harness limitation (CompositeReconciler never fires):** Resolved by `c968dd97` (harness fixes) and `38ff304d` (error tolerance). The CompositeReconciler now participates in exploration.
- The original **bug hypothesis (orphaned compositionRef)** is now **directly confirmed** by the infinite cycle -- Kamera shows the system never converges because the reconciler cannot recover from the deleted Composition reference.

## Re-run (2026-03-12, depth=100)

### What was run

```bash
cd /Users/tgoodwin/projects/kamera/examples/crossplane
go run . -inputs scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound.json \
  -interactive=false -log-level=info \
  -output=/tmp/depth100-composition-deleted
```

Ran at depth 100 (default), then 200, then 400 to confirm cycling behavior.

### Campaign metrics

**Depth 100:**
```
invocation: 33b71c12-6602-4e00-a11d-08ebdb4aae51
  unique node visits:        15
  total node visits:         101
  unique resource states:    6
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**Depth 200:**
```
invocation: dcd46070-8035-43f1-8c56-98400d690694
  unique node visits:        15
  total node visits:         201
  unique resource states:    6
  duration:                  0s
  aborted states:            1
  max-depth aborted states:  1
```

**Depth 400:**
```
invocation: 64fd1a78-1756-4f5b-a1ca-f710d0be59f1
  unique node visits:        15
  total node visits:         401
  unique resource states:    6
  duration:                  1s
  aborted states:            1
  max-depth aborted states:  1
```

### Answers to key questions

1. **Did the reference run converge?** No. The exploration cycles indefinitely through 15 unique nodes and 6 unique resource states.
2. **Did the reference run hit max depth?** Yes -- at depth 100, 200, and 400. The unique node count stays fixed at 15 regardless of depth, confirming a finite cycle.
3. **Did the perturbed run(s) converge?** N/A -- only 1 invocation was produced (combined reference+rerun). It did not converge.
4. **Did the perturbed runs hit max depth?** Yes.
5. **Are there multiple distinct converged states?** No -- zero converged states at any depth.
6. **How do the campaign-metrics compare between reference and perturbed runs?** Only one invocation produced. The metrics are identical across all depths except total node visits (which scales linearly with depth).

### Comparison with previous runs

| Metric | Original | 2026-03-11 (depth=40) Ref | 2026-03-11 (depth=40) Rerun | 2026-03-12 (depth=100) |
|--------|----------|---------------------------|-----------------------------|-----------------------|
| Unique node visits | 4-6 | 15 | 115 | 15 |
| Total node visits | 4-7 | 41 | 212 | 101 |
| Unique resource states | 3 | 6 | 16 | 6 |
| Converged states | 1 | 0 | 0 | 0 |
| Max-depth aborted | 0 | 1 | 3 | 1 |

The deeper exploration (depth 100-400) confirms the findings from the 2026-03-11 re-run. The system enters a finite cycle of 15 unique nodes among 6 resource states and never converges. Increasing depth from 40 to 400 does not help -- the cycle is structural.

Note that in this run, only a single invocation was produced (the reference and rerun appear combined), whereas the 2026-03-11 run produced separate reference and rerun invocations with different exploration breadth. The staleness perturbation in the 2026-03-11 rerun explored more states (115 unique nodes, 16 resource states) but also failed to converge.

### Updated conclusions

The **orphaned compositionRef bug** is robustly confirmed. The infinite reconciliation loop is a genuine production behavior: after Composition deletion, the CompositeReconciler enters an error-retry cycle that never self-heals. The depth-100+ exploration adds no new information beyond confirming the cycle is finite and structural. The original bug analysis and proposed fixes remain valid.
