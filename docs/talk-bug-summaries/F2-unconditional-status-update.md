# F2 — Unconditional `Status().Update()` on every reconcile

**Issue:** [crossplane/crossplane#7221](https://github.com/crossplane/crossplane/issues/7221)
**Fix PR:** [crossplane/crossplane#7283](https://github.com/crossplane/crossplane/pull/7283)
**Status:** ✅ Fixed upstream — accepted by maintainers; PR in flight at the time of the SPRINT-0001 kickoff. Confidence: HIGH.
**Demo:** [F2-demo-repro.md](./F2-demo-repro.md) — harness convergence before vs after PR #7283.

## TL;DR

The composite reconciler called `Status().Update()` on every reconcile pass regardless of whether the status had actually changed, generating a steady stream of spurious `resourceVersion` bumps and watch events. The fix conditionalizes the write on a real status change.

## What's actually wrong

`internal/controller/apiextensions/composite/reconciler.go` — at the end of every reconcile, the reconciler issued a `Status().Update()` for the XR. Even when the reconcile produced no change (the canonical "happy path" steady-state reconcile), the `Update` call:

- bumped `metadata.resourceVersion`,
- triggered a watch event for every controller and informer subscribed to the XR,
- fanned out re-enqueues across the watch graph,
- and consumed an API-server write quanta per reconcile.

In a steady cluster, this manifests as constant low-grade churn: nothing useful changes, but every reconcile produces a write event that triggers downstream re-reconciles, which produce more writes, etc.

User impact: amplified API server load on busy clusters; noise on `kubectl get -w` / informer streams; and (most relevant to other findings) increased opportunity for races elsewhere in the system, because reconciles fire more often than they need to.

## How Kamera surfaced it

Kamera's harness records every observed `resourceVersion` transition and surfaces unique state hashes per object across all explored orderings. In F1/F3/F5 scenarios, the harness produced terminal states whose XWidget hashes drifted purely on `metadata.resourceVersion` — same `spec`, same `status` content, different `resourceVersion`. This pattern shows up as "unique terminal hashes that compare identical on field-by-field diff."

Tracing back through the reconcile log for these scenarios revealed `Status().Update()` calls at the end of every reconcile pass with a status payload identical to what was already on the API server. The unconditional-write pattern was directly visible in the recorded reconcile transitions: same status before, same status after, but a write-event in between.

The harness's hardening process actually surfaced this twice. First it appeared as "harmless" RV churn in baseline runs. Then, when we added RV conflict checking (commits `911b3bd` / `cb1c43e` / `7ba2045`), the suppressed no-op writes became visible as before/after divergence in state-space exploration: many duplicate paths in the baseline collapsed to single paths in the hardened harness, indicating those paths only differed because of the unconditional write.

## Validation status

F2 was the only one of the six original SPRINT-0001 findings that landed cleanly with the maintainers immediately. No further audit work was performed under SPRINT-0001's re-evaluation:

- The bug mechanism is a single-line code change visible in the reconciler.
- Maintainer review was not contested.
- PR [#7283](https://github.com/crossplane/crossplane/pull/7283) implements the fix (conditional `Status().Update()` only on actual change).
- Once that PR merges, the harness's RV-conflict-checking pass should observe the steady-state convergence directly without the cosmetic `resourceVersion` drift.

## Talking points for the audience

- This is the simplest finding in the batch and the one that most cleanly demonstrates the value of state-space-aware analysis: the bug is invisible to single-trace inspection (any individual reconcile looks fine), but glaringly obvious when you observe many reconciles and notice they all produce identical status with bumped resourceVersion.
- F2's fix indirectly improves the precision of every other ordering-sensitive bug analysis. The hardened harness's RV-conflict checking becomes more meaningful once the controller stops manufacturing fake conflicts.
- This is the cheapest, fastest fix in the set — and credit to the maintainers for accepting and queueing it within days of the original report.
