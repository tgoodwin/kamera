# R-3: SEVERITY_FATAL early-return + stale `Ready=True` source confirmation

**Status:** ✅ AUDITED — both F6 sub-claims source-grounded in v2.2.0.

**Threat addressed:** [F6-A1, F6-A2, F6-A3](./per-finding-threats.md#f6-7223--f6-orphan-portion--fatal-function-leaves-orphans), F6-stale-Ready-True

## Question

Does v2.2.0's `composition_functions.go` actually skip `GarbageCollectComposedResources` on SEVERITY_FATAL? Does the error path leave `Ready=True` stale?

## Finding 1: GC is skipped on fatal early-return

**Source:** `internal/controller/apiextensions/composite/composition_functions.go:429-440` (pipeline-step iteration loop) and `:538` (GC call site).

The loop iterates pipeline steps. On each step:
- Line 397: `d = rsp.GetDesired()` — captures desired state from this step's function output.
- Line 429-457: iterates `rsp.GetResults()`. On SEVERITY_FATAL (line 438):
  ```go
  case fnv1.Severity_SEVERITY_FATAL:
      return CompositionResult{Events: events, Conditions: conditions}, errors.Errorf(errFmtFatalResult, fn.Step, rs.GetMessage())
  ```
  **Return immediately, with the loop incomplete and the captured-but-unused `d`.**

The garbage collector call:
- Line 461-532: builds `desired` from completed-loop state.
- Line 538: `if err := c.composite.GarbageCollectComposedResources(ctx, xr, observed, desired); err != nil { ... }`

**The early-return at 439 happens BEFORE line 461. `desired` is never built. GC is never called.** Confirmed.

## Finding 2: Stale `Ready=True` survives the error path

**Source:** `internal/controller/apiextensions/composite/reconciler.go:709-756` (Compose error handler).

The error path:

```go
res, err := r.resource.Compose(ctx, xr, CompositionRequest{Revision: rev})
if err != nil {
    log.Debug(errCompose, "error", err)

    if kerrors.IsConflict(err) {
        return reconcile.Result{Requeue: true}, nil
    }
    err = errors.Wrap(err, errCompose)
    r.record.Event(xr, event.Warning(reasonCompose, err))
    // ... [Invalid handling, RBAC handling]
    status.MarkConditions(xpv1.ReconcileError(err))      // line 738 — sets Synced=False

    resultMeta := r.handleCommonCompositionResult(updateCtx, res, xr)
    // We encountered a fatal error. For any custom status conditions that were
    // not received due to the fatal error, mark them as unknown.
    for _, c := range xr.GetConditions() {
        if v1.IsSystemConditionType(c.Type) {            // line 744 — SKIPS system conditions
            continue
        }
        if !resultMeta.conditionTypesSeen[c.Type] {
            c.Status = corev1.ConditionUnknown
            // ...
            status.MarkConditions(c)
        }
    }
    _ = r.client.Status().Update(updateCtx, xr)
    return reconcile.Result{}, err
}
```

Key fact: **`v1.IsSystemConditionType(c.Type)` is true for `Ready`** (it's the canonical system condition alongside `Synced`). Line 744 explicitly skips system conditions when iterating to mark unknown.

The result: on a fatal compose error, the controller writes `Synced=False` (line 738) and marks all NON-system conditions as `Unknown`, but does **not** touch `Ready`. If the prior successful compose set `Ready=True`, that value persists across the error.

**This is exactly what the F6 stale-Ready-True claim asserts.** Confirmed at the source level.

## Threat resolution

- **F6-A1 (SEVERITY_FATAL early-return reached):** RESOLVED — exact line cited.
- **F6-A2 (no fallback GC path):** PARTIALLY RESOLVED — GC isn't called on the error path; whether subsequent reconciles eventually GC requires R-9.
- **F6-A3 (orphan would actually persist in production):** SOURCE-GROUNDED at the per-reconcile level. The persistence-across-time question (does a later successful compose run GC?) is R-9.
- **F6 stale-Ready-True:** RESOLVED — system-condition filter at reconciler.go:744 explicitly preserves Ready.

## What this means for the upstream draft

The F6 stale-Ready-True claim can be posted with high confidence. The draft can cite `reconciler.go:738-754` directly with the system-condition-filter argument.

The F6 orphan-persistence claim can be posted with moderate confidence. The single-reconcile mechanism is source-grounded. The cross-reconcile question (does the orphan persist forever, or does a subsequent reconcile clean it up?) needs R-9.

## What's NOT addressed

- **R-9:** does any subsequent reconcile call GC on a previously-fatal XR? Specifically: when the function gets fixed and the next compose succeeds, does GC run on the now-orphaned old resources? (Yes per the v2.2.0 success path at line 538.) But while the function remains fatal, GC never runs on retry — every reconcile hits the same early-return. The orphan persists for as long as the function stays fatal.
