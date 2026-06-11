# R-7: controller-runtime workqueue + watch ordering semantics

**Status:** ✅ AUDITED — workqueue de-duplicates by key but does NOT serialize cross-controller ordering. The F5 race window is timing-feasible in production; our scheduler explores valid orderings.

**Threat addressed:** [CC-3](../upstream-updates/AUDIT-threats-to-validity.md#cc-3-watch-fanout-and-re-enqueue-ordering), [F5-T1](../upstream-updates/AUDIT-threats-to-validity.md#f5-7223--f5-portion--stale-validpipeline-race)

## Question

Does controller-runtime's workqueue rate-limit / coalesce rapid re-enqueues from the same key in a way that would close the F5 race window in production? Specifically: when a `FunctionRevision` changes, is `CompositionRevisionReconciler` guaranteed to run before `CompositeReconciler` does its next compose?

## Research method

1. Read `sigs.k8s.io/controller-runtime@v0.23.0/pkg/internal/controller/controller.go` end-to-end (566 lines).
2. Read `sigs.k8s.io/controller-runtime@v0.23.0/pkg/controller/priorityqueue/priorityqueue.go` for the de-dup, rate-limit, and FIFO semantics.
3. Read kamera's `pkg/explore/parallel_runner.go` and `pkg/tracecheck/state.go:761-804` for the harness's permutation strategy.

## Findings

### Workqueue de-duplicates per controller, per key — not across controllers

`sigs.k8s.io/controller-runtime@v0.23.0/pkg/controller/priorityqueue/priorityqueue.go:107-151` builds **one priority queue per controller** (`pq := &priorityqueue[T]{...}`, called from `controller.go:338` `c.NewQueue(c.Name, c.RateLimiter)` for each controller).

Within a single controller's queue:

- `lockedAddWithOpts` (lines 197-286) checks `if _, ok := w.items[key]; !ok` (line 219). Re-enqueues for the **same key** are merged into one queue entry.
- The merge takes `min(readyAt)` (lines 239-243) and `max(priority)` (lines 247-255). If `RateLimited: true` is passed, `rlAfter := w.rateLimiter.When(key)` (line 208) computes a per-key exponential backoff (default `5ms..1000s` per `New` defaults at line 70).

This means a single FunctionRevision change can produce many re-enqueues for the same `CompositionRevision` key, but they collapse into one upcoming run. Good — that matches K8s expectations.

### **Cross-controller**: queues are independent

`controller.go:338` is invoked once per controller. There is no shared queue across `CompositionRevisionReconciler` and `CompositeReconciler`. There is no global ordering primitive.

`controller.go:308-316`: each controller starts `MaxConcurrentReconciles` worker goroutines that each call `processNextWorkItem` independently. There is no synchronization between controllers.

The watch fanout is `Watches(&pkgv1.FunctionRevision{}, EnqueueCompositionRevisionsForFunctionRevision(...))` — this puts a request on the `CompositionRevisionReconciler`'s queue. The same FunctionRevision change does **not** put anything on the `CompositeReconciler`'s queue directly. The latter only re-runs because either (a) the `CompositionRevision` it observes is updated (writeable side-effect of the former running) or (b) its own poll interval (default 1m) fires, or (c) any of its other watches fire.

So when a FunctionRevision changes:
1. Both queues can have pending work concurrently — `CompositionRevisionReconciler` re-evaluating the revision, AND `CompositeReconciler` re-running for any other reason.
2. There is no rate limiter that says "delay `CompositeReconciler` until `CompositionRevisionReconciler` finishes."
3. The Go runtime scheduler decides which worker goroutine runs first.

This is exactly the race surface the F5 claim describes.

### `RequeueAfter` and the rate limiter (controller.go:483-515)

On reconcile result:

- error → `c.Queue.AddWithOpts(priorityqueue.AddOpts{RateLimited: true, Priority: ptr.To(priority)}, req)` (line 488) — re-add with backoff.
- `result.RequeueAfter > 0` → `Forget(req); AddWithOpts(After: result.RequeueAfter)` — exact delay.
- `result.Requeue` → re-add rate-limited.
- success → `Forget(req)` — drop from rate limiter so next enqueue runs immediately.

None of this introduces cross-controller serialization. The "wait for the dependent controller to settle" pattern that production uses (e.g., Crossplane's `errors.WithSilentRequeueOnConflict` at `revision/reconciler.go:69`) handles **conflicts** on the same key, not cross-controller ordering.

### Harness permutation strategy

`pkg/tracecheck/state.go:761-804` `expandStateByReconcileOrder`: for each pending reconcile in the `triggered` (or all-pending) set, generate one alternative branch where that reconciler is moved to the front. So if the pending set is `[A, B]` and both are in the permute scope, the harness branches into `[A, B]` and `[B, A]` — a fan-out of N for N pending reconciles.

`pkg/tracecheck/state.go:358-370` `ReadyPendingReconciles` is FIFO order; permutation overrides FIFO by swapping one to the front per branch.

`pkg/explore/parallel_runner.go:585-621` `buildDefaultScenarioRerunPlans`: enables permutation for **all controllers observed in the reference run**, with no priority weighting.

This means our scheduler explores orderings that production CAN produce (controller A before controller B, B before A) without imposing any constraint. The only ordering production COULDN'T produce is one where both controllers run as one atomic step, which our harness also doesn't produce.

The harness does NOT model:
- Wall-clock backoff timing (each pending reconcile is "ready" immediately unless deferred by an explicit perturbation).
- Worker-pool exhaustion (production has `MaxConcurrentReconciles`; we don't model worker contention).
- Per-key rate-limiting across re-enqueues.

For F5 specifically, none of these missing details would CLOSE the race window. The race fires because the workqueue does not synchronize the two controllers, and our scheduler correctly captures that. If anything, real production timing is MORE chaotic (Go runtime scheduling, multiple goroutines), making the race more reachable, not less.

## Source links

- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/internal/controller/controller.go:308-316` (per-controller workers)
- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/internal/controller/controller.go:338` (`NewQueue` per controller)
- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/internal/controller/controller.go:483-515` (post-reconcile result handling)
- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/controller/priorityqueue/priorityqueue.go:197-286` (per-key dedup + min(readyAt))
- `sigs.k8s.io/controller-runtime@v0.23.0/pkg/controller/priorityqueue/priorityqueue.go:69-71` (rate-limiter default `5ms..1000s` exponential)
- `pkg/tracecheck/state.go:761-804` (permutation expansion)
- `pkg/tracecheck/state.go:358-370` (ready pending FIFO)
- `pkg/explore/parallel_runner.go:585-621` (rerun plan: enable permute for all observed controllers)

## Threat resolution

| Threat | Resolution |
|---|---|
| **CC-3 / F5-T1**: rate-limiter coalesces rapid re-enqueues such that `CompositionRevisionReconciler` always wins the race | RESOLVED — coalescing is per-key, per-controller. There is no cross-controller serialization. |
| Our permutation explores orderings real CR would never produce | RESOLVED — the orderings we explore (A-then-B, B-then-A) are exactly what production worker goroutines can produce. |
| Real CR can produce orderings we can't | PARTIAL — production has wall-clock chaos (Go scheduler, goroutine timing) that our scheduler approximates with explicit permutation. The qualitative behaviors are equivalent for F5; we don't capture, e.g., partial reconcile interleaving (production never preempts mid-reconcile, neither does our harness, so this is fine). |

## What this means for F5 posting

The "rate limiter closes the race" hypothesis is refuted. There is no controller-runtime mechanism that would force `CompositionRevisionReconciler` to win against a concurrent `CompositeReconciler`. The race is timing-feasible in production whenever the goroutine scheduler happens to pick `CompositeReconciler` first, which is non-deterministic.

This does NOT prove the race is high-probability in practice. R-12 (real-cluster reproduction with a production goroutine scheduler) is still the smoking-gun audit. But the workqueue-fidelity threat is closed: the simulation is not producing impossible orderings.

## What's NOT addressed

- **Quantitative race probability**: how often does the goroutine scheduler pick `CompositeReconciler` first in practice? That requires real-cluster runs (R-12).
- **Crossplane-internal `ControllerEngine`**: Crossplane uses its own engine for dynamic XR controllers (`composite.go:758-774` `engine.StartWatches`). This wraps controller-runtime; it does not add cross-controller serialization but does add per-XR controller lifecycle. Not a concern for F5 (a single XR).
- **`errors.WithSilentRequeueOnConflict`** wrapping at `revision/reconciler.go:69` and `composite.go` similar — this only suppresses log spam on conflict; it does not serialize across controllers.
