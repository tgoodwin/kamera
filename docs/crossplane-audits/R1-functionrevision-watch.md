# R-1: FunctionRevision watch wiring

**Status:** ✅ AUDITED — production wiring confirmed; harness matches with one bounded fidelity gap.

**Threat addressed:** [CC-3](./cross-cutting-threats.md#cc-3-watch-fanout-and-re-enqueue-ordering), [F5-T1, F5-T2](./per-finding-threats.md#f5-7223--f5-portion--stale-validpipeline-race)

## Question

Does `CompositionRevisionReconciler` watch `FunctionRevision` in production v2.2.0? If so, does our harness wire the same watch?

## Production source (v2.2.0)

`internal/controller/apiextensions/revision/reconciler.go:64-69`

```go
return ctrl.NewControllerManagedBy(mgr).
    Named(name).
    For(&v1.CompositionRevision{}).
    Watches(&pkgv1.FunctionRevision{}, EnqueueCompositionRevisionsForFunctionRevision(mgr.GetClient(), o.Logger)).
    WithOptions(o.ForControllerRuntime()).
    Complete(errors.WithSilentRequeueOnConflict(r))
```

`internal/controller/apiextensions/revision/watch.go:35-76` — `EnqueueCompositionRevisionsForFunctionRevision`:

1. Reads `pkgv1.LabelParentPackage` from the FunctionRevision.
2. **Lists ALL CompositionRevisions** (`kube.List(ctx, revs)`).
3. For each CompositionRevision whose `spec.pipeline[].FunctionRef.Name` matches the parent package name, enqueues a reconcile request.

## Harness wiring (kamera)

`examples/crossplane/scenario.go:65-66, 201-230`

```go
.Watches("pkg.crossplane.io/FunctionRevision", functionRevisionToCompositionRevisionMapper())
```

The mapper:

```go
return []reconcile.Request{
    {NamespacedName: types.NamespacedName{Name: compositionName + "-rev-1"}},
}
```

## Findings

✅ **The watch is wired in our harness.** Production has it; harness has it. Earlier triage notes flagged this as missing; it was fixed before the SPRINT-0001 campaign. The "Missing FunctionRevision watch registration" entry in `examples/crossplane/.agents/scenarios/function-capability-removed.md` describes the fix.

⚠️ **Fidelity gap: hardcoded single-revision target.** Our mapper returns exactly one reconcile request, hardcoded to `compositionName + "-rev-1"`. Production lists *all* CompositionRevisions and enqueues every one whose pipeline references the function. If a scenario has multiple CompositionRevisions (e.g., alpha-rev-1 + beta-rev-1), our harness would only re-evaluate `alpha-rev-1` even when both should be re-evaluated.

## Impact on F5

**No material impact for F5 in its current scenario set.** The F5 scenarios use a single Composition with a single CompositionRevision (`widget-composition-rev-1`). The hardcoded mapping name happens to match, so the watch fires identically to production.

## Impact on F1

**No impact.** F1 doesn't change function capabilities, so the FunctionRevision watch never fires.

## Threat resolution

- **F5-T1 (real CR re-enqueues fast enough to close the race):** PARTIALLY RESOLVED by code read. Production does enqueue on FunctionRevision change, but the question of whether CR's workqueue + scheduler timing closes the F5 race window faster than our scheduler explores it remains open. That requires a real-cluster experiment (R-12).
- **F5-T2 (FunctionRevision watch unmodeled in harness):** RESOLVED — watch is modeled.

## Caveats

- Our mapper has no access to the store, so the production behavior of "list and filter" is approximated by the hardcoded name. Multi-revision scenarios (alpha + beta with shared functions) would need a different mapper. This isn't in scope for the current four findings.
- The mapper may not fire on FunctionRevision DELETE events the same way production does. Not investigated; not material for the current findings.
