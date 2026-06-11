# F6-stale-Ready — Stale `Ready=True` while `Synced=False`

**Issue:** [crossplane/crossplane#7223](https://github.com/crossplane/crossplane/issues/7223) (F6 sub-claim)
**Status:** ✅ Reproduces (source-grounded; mechanism confirmed on real cluster, exact `Ready=True` value pending follow-up). Confidence: HIGH.
**Audits:** [R-3](../crossplane-audits/R3-fatal-branch.md), [R-13](../crossplane-audits/R13-f6-real-cluster.md).

## TL;DR

When the composite reconciler enters its error path (e.g., function returns SEVERITY_FATAL, Composition deleted, or any other reconcile error), it sets `Synced=False` but does not clear or downgrade other system conditions. `Ready` is a system condition. So if the XR was previously `Ready=True`, it stays `Ready=True` even while `Synced=False / ReconcileError`. The combined state is `Synced=False + Ready=True`, which contradicts the user's mental model of the conditions.

This is the **cleanest claim in the entire batch** — it's source-grounded at an exact line and isn't race-dependent.

## What's actually wrong

`internal/controller/apiextensions/composite/reconciler.go` in v2.2.0:

- The error path (the branch entered when the reconciler returns a non-nil error) iterates over conditions to mark them `Unknown`.
- Line 744 — the iteration **skips system conditions**. `Ready` is a system condition.
- Line 739 nearby sets `Synced=False`, but does not touch `Ready`.

Result: whatever value `Ready` had immediately before the error transition persists. If the XR was `Ready=True` from a prior successful composition, it stays `Ready=True` while `Synced=False / ReconcileError`. The condition transition timestamps don't update either — `Ready`'s `lastTransitionTime` keeps its pre-error value.

User impact: a user who's done `kubectl wait --for=condition=Ready xr/foo` sees `Ready=True` and assumes the XR is healthy. But `Synced=False` says reconciliation is failing. Worse, the `Ready=True` is stale — it reflects a prior reconcile, not the current state. Any downstream automation that gates on `Ready=True` (CI/CD pipelines, dependent XR creation, etc.) will proceed against an XR whose actual reconcile state is failed.

The bug is **guaranteed**, not race-dependent: the system-condition skip is unconditional on the error path. Every error transition leaves `Ready` stale.

## How Kamera surfaced it

This finding emerged in the F3 and F6-orphan harness scenarios (composition-deleted-while-XR-bound, composition-switches-to-fatal). In both, the XR's terminal state showed `Synced=False / ReconcileError` simultaneously with `Ready=True`. The harness's state-hashing surfaces conditions blocks; trace inspection of XWidget objects in the JSONL dumps confirmed `Ready=True` persisting alongside the error condition.

It's worth flagging that the original SPRINT-0001 evidence for this sub-claim was lighter than for F1/F3/F6-orphan — the per-terminal condition extraction wasn't surfaced by `kamera analyze diff`'s default summary output, only by tracing into the dumps. The hardened-harness re-run did not extract per-terminal conditions automatically; the F3 trace audit (R-4) incidentally re-confirmed `Ready=True` alongside `Synced=False / ReconcileError` in the F3 terminals as a side effect of inspecting the XWidget objects directly.

## How we validated it

### Tier 1 — source grounding (R-3)

R-3 read the v2.2.0 reconciler source. The "set conditions to Unknown on error" loop at the relevant block in `reconciler.go` explicitly checks `xpv1.IsSystemConditionType(c.Type)` and skips if true. `xpv1.TypeReady` returns `true` from that check (along with `Synced` itself, which is set explicitly elsewhere). The mechanism is unconditional — every reconcile error path produces stale `Ready`.

This is the strongest possible evidence: there is no "this might or might not happen depending on timing" framing. The line of code makes it deterministic.

### Tier 3 — partial real-cluster confirmation (R-13)

R-13 ran the F6-orphan scenario on the real cluster (XR switched to a Composition with a non-resolvable function). For the full 3-minute observation:
- `Synced=False / ReconcileError` (transitions in immediately).
- `Ready=False (Creating)` "Unready resources: cm" (persists with same `lastTransitionTime`, reason, and message — unchanged across the error transition).

This confirms the **underlying invariant**: system conditions persist unchanged across the error transition. R-3's mechanism is empirically verified on the real cluster.

What's *not* directly reproduced in R-13 is a positive `Ready=True` value alongside `Synced=False`. That requires the XR to have been `Ready=True` before the error transition, which requires a composed resource that publishes its own `Ready` condition. Vanilla ConfigMaps don't publish a `Ready` condition, so the XR in R-13 was never `Ready=True` to begin with — Crossplane infers `Ready=False (Creating)` "Unready resources: cm" from the absence of a Ready condition on the composed dependent.

To produce a positive `Ready=True` reproduction, a future test would compose a resource type that does publish a `Ready` condition — e.g., a Pod, a managed-resource Provider, or a downstream XR. That's left as deferred work.

The mechanism (system-condition-skip on error path) is unambiguously confirmed. Only the specific stale value differs in this test. R-3's source-grounding is sufficient on its own to defend the claim.

## Suggested fix (open)

Two reasonable directions, not pre-judged:

1. **Mark `Ready=Unknown` on error** alongside `Synced=False`. Mirrors how non-system conditions are handled. Makes the user-visible state honest: "we tried to reconcile, it failed, we don't know if you're ready."
2. **Special-case `Ready` to recompute** based on the (now-failed) reconcile. Cost: requires defining what `Ready` means under partial reconcile failure.

Direction 1 is the mechanically simpler fix and matches the existing condition-handling pattern. Direction 2 is more semantically correct but requires design.

## Talking points for the audience

- This is the cleanest source-grounded finding in the batch. Single line, deterministic mechanism, no race window. R-3 makes it impossible to mis-frame.
- The interaction with F3 and F6-orphan is worth noting: every time those bugs fire, F6-stale-Ready also fires. So a maintainer who fixes F6-stale-Ready improves the user-visible state in two other failure modes for free.
- The R-13 partial-validation story is honest about what we directly observed vs what we inferred. Vanilla ConfigMaps don't publish a Ready condition — the test composition would have needed a richer composed resource type to produce a positive `Ready=True` reproduction. The underlying mechanism (system-condition-skip) is confirmed; the specific stale value `True` is deferred.
- This is also a good example of a bug that's invisible to single-trace inspection of a healthy reconcile but visible the moment any reconcile errors. A user who's never hit a reconcile error never sees this; a user who hits one immediately sees a confusing state.
