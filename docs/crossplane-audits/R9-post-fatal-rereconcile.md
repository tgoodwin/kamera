# R-9: Crossplane composition reconciler post-fatal re-reconcile behavior

**Status:** ✅ AUDITED — after a SEVERITY_FATAL compose error, Crossplane re-enqueues the XR (via the workqueue's rate-limited error path AND the periodic 1m poll), but every retry retakes the same `composition_functions.go:439` early-return path. GC at `composition_functions.go:538` is never reached while the function stays fatal. The orphan persists for as long as the function stays fatal.

**Threat addressed:** [F6-A2, F6-T2](../upstream-updates/AUDIT-threats-to-validity.md#f6-7223--f6-orphan-portion--fatal-function-leaves-orphans)

## Question

After a SEVERITY_FATAL compose error, does Crossplane re-enqueue the XR for retry? Is there ANY code path where a subsequent reconcile would call `GarbageCollectComposedResources` while the function is still fatal (i.e., while `desired` would still be incomplete)?

## Research method

1. Read v2.2.0 `internal/controller/apiextensions/composite/reconciler.go:540-870` end-to-end.
2. Traced what happens after `return reconcile.Result{}, err` at line 755 (the fatal compose path).
3. Looked for `RequeueAfter`, exponential backoff, watch re-trigger conditions, and the periodic resync timer.
4. Cross-referenced with R-3 (which already shows GC isn't called on the in-reconcile fatal path).
5. Cross-referenced with R-7 (controller-runtime rate-limited re-enqueue on error).

## Findings

### What happens at line 755

`reconciler.go:709-756` (the Compose error path):

```go
res, err := r.resource.Compose(ctx, xr, CompositionRequest{Revision: rev})
if err != nil {
    log.Debug(errCompose, "error", err)
    if kerrors.IsConflict(err) { return reconcile.Result{Requeue: true}, nil }
    err = errors.Wrap(err, errCompose)
    r.record.Event(xr, event.Warning(reasonCompose, err))
    // ... [Invalid handling, RBAC handling]
    status.MarkConditions(xpv1.ReconcileError(err))
    resultMeta := r.handleCommonCompositionResult(updateCtx, res, xr)
    for _, c := range xr.GetConditions() {
        if v1.IsSystemConditionType(c.Type) { continue }
        if !resultMeta.conditionTypesSeen[c.Type] {
            c.Status = corev1.ConditionUnknown
            c.Reason = reasonFatalError
            // ...
            status.MarkConditions(c)
        }
    }
    _ = r.client.Status().Update(updateCtx, xr)
    return reconcile.Result{}, err  // <-- line 755, returns error
}
```

`Compose` returns `errors.Errorf(errFmtFatalResult, fn.Step, rs.GetMessage())` from `composition_functions.go:439` on SEVERITY_FATAL. So the returned `err` here is non-nil and non-conflict.

### controller-runtime's handling of the error return

Per R-7 (`controller-runtime@v0.23.0/pkg/internal/controller/controller.go:484-489`):

```go
case err != nil:
    if errors.Is(err, reconcile.TerminalError(nil)) {
        ctrlmetrics.TerminalReconcileErrors.WithLabelValues(c.Name).Inc()
    } else {
        c.Queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: true, Priority: ptr.To(priority)}, req)
    }
```

The wrapped fatal error is NOT a `TerminalError`. Therefore controller-runtime re-enqueues the XR with rate-limited backoff (default exponential `5ms..1000s`).

### What the next reconcile does

When the rate-limited re-enqueue fires, the reconciler runs from the top. Flow (paraphrased from `reconciler.go:540-755`):

1. Fetch XR (line ~553).
2. Resolve composition revision via `APIRevisionFetcher` (line ~620, which under Manual policy just `Get`s the revision).
3. Call `r.resource.Compose(ctx, xr, CompositionRequest{Revision: rev})` (line 709).
4. The Composer iterates the function pipeline (`composition_functions.go:323-458`).
5. Per R-3 (`composition_functions.go:438-439`): on SEVERITY_FATAL, `return CompositionResult{...}, errors.Errorf(errFmtFatalResult, ...)`.
6. Crossplane's reconciler hits the `if err != nil` branch at `reconciler.go:710` again. **Identical code path. Return at line 755.**

**`GarbageCollectComposedResources` (called only at `composition_functions.go:538`) is unreachable on every retry as long as the function returns SEVERITY_FATAL.** The early-return at 439 short-circuits the desired-state-processing block (461-532) and the GC call (538).

### The 1m poll interval

`reconciler.go:857`: `result := reconcile.Result{RequeueAfter: jitter(r.pollInterval)}`. This fires only on the **success path** (after line 776+). It never fires on the fatal path because we return at line 755 before reaching line 857.

But: even if it did, the next reconcile would do the same thing — call Compose, get fatal back, return at 755.

`cmd/crossplane/core/core.go:113`: `PollInterval default:"1m"` — so even watch-triggered re-enqueues happen frequently.

### Watch-triggered re-enqueues

Real-time compositions watches (set up at `reconciler.go:758-774`) re-enqueue the XR when any composed resource changes. But there are no composed resources to change, because GC was skipped and the fatal function returns no Desired. The previously-composed `ConfigMap` (the orphan) does not change, so it does not trigger a re-enqueue.

If something else triggers an XR re-enqueue (status update racing the workqueue, unrelated controllers, etc.), the resulting reconcile still hits the same path.

### What ends the orphan persistence

The orphan stops persisting when **the function stops returning SEVERITY_FATAL**. At that point, `Compose` returns successfully, the reconciler proceeds past line 755 into the success path at line 776+, and `GarbageCollectComposedResources` at `composition_functions.go:538` runs on the next reconcile. The orphan is GC'd then.

## Source links

- `internal/controller/apiextensions/composite/reconciler.go:709-756` (fatal compose error handler, return at 755)
- `internal/controller/apiextensions/composite/reconciler.go:857` (1m RequeueAfter — only on success path)
- `internal/controller/apiextensions/composite/composition_functions.go:438-439` (SEVERITY_FATAL early-return)
- `internal/controller/apiextensions/composite/composition_functions.go:538` (GC call site, unreachable from fatal)
- `cmd/crossplane/core/core.go:113` (`PollInterval default:"1m"`)
- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/internal/controller/controller.go:484-489` (error → rate-limited re-enqueue)

## Threat resolution

| Threat | Resolution |
|---|---|
| **F6-A2**: real Crossplane has a fallback GC path that cleans orphans on subsequent reconciles | RESOLVED — every re-reconcile retakes the same fatal early-return; GC never runs while the function stays fatal. |
| **F6-T2**: production has a periodic resync that retries fatal functions and eventually GCs | PARTIALLY RESOLVED — the 1m poll re-fires reconciles, but each reconcile follows the same fatal early-return. There is no separate "force GC" path. The orphan is only cleaned up after the function is fixed. |
| Permanent-orphan claim | BOUNDED — accurate when scoped to "while the function stays fatal." Once the function is fixed, the next successful reconcile WILL GC. The draft is already scoped this way. |

## What this means for F6 posting

The F6 orphan-persistence claim, **scoped to "while the function stays fatal,"** is source-grounded. There is no fallback GC, periodic resync GC, or watch-driven GC that would clean the orphan during the fatal window.

The claim "orphan persists forever" would be wrong if the user fixes the function, so the draft must avoid that absolute language. The current draft (`docs/upstream-updates/7223-f5-f6-reframe.md`) phrases this carefully.

R-13 (real-cluster reproduction) is still the highest-confidence audit, but the post-fatal re-reconcile fidelity threat is closed.

## What's NOT addressed

- Whether Crossplane operators in the wild would consider the orphan a real bug (they might say "your function is broken; fix it"). This is a maintainer-judgment question, not a fidelity question.
- Whether a function that returns BOTH `Desired.Resources` AND a fatal `Result` would behave differently. Per R-3, the early-return at `composition_functions.go:439` fires regardless of whether `Desired` was populated; the captured-but-unused `d` variable is discarded. So no change.
- The "fatal then later fixed" recovery path is source-grounded but not exercised in the current F6 scenario set. R-13 should cover it.
