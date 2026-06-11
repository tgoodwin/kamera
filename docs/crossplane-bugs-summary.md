# Crossplane bug claims: status after re-evaluation

Six issues / one PR were filed against [crossplane/crossplane](https://github.com/crossplane/crossplane) in March 2026 based on Kamera harness findings. This doc summarizes each claim's final state after SPRINT-0001's fidelity-hardened re-runs (Tier 0), Tier 1 + Tier 2 audits, the R-4 trace audit, and Tier 3 real-cluster reproductions on Crossplane v2.2.0 + kind v1.30 (executed 2026-04-29).

> **Talk-prep summaries.** Per-bug briefings (mechanism, how Kamera found it, how we validated it) live in [`talk-bug-summaries/`](./talk-bug-summaries/). Each section below also links to its talk-prep doc.

| Verdict legend | Meaning |
|---|---|
| ✅ **Reproduces (real cluster)** | Bug observed directly on Crossplane v2.2.0 in kind; simulation-fidelity threats eliminated. |
| ✅ **Reproduces (harness + source-grounded)** | Bug observed in fidelity-hardened harness; mechanism source-grounded; real-cluster reproduction not pursued or inconclusive but does not change posture. |
| 🔄 **Shifts** | Bug still real; specific evidence shape changed since original report. |
| ✅ **Fixed upstream** | Maintainer accepted; PR in flight. |
| 🟡 **Closed by author** | Issue closed before re-evaluation; selected sub-claim re-validated for internal record. |

---

## [#7220 — F1: Manual update policy uses wrong CompositionRevision](https://github.com/crossplane/crossplane/issues/7220)

**Talk-prep summary:** [`talk-bug-summaries/F1-manual-policy-wrong-revision.md`](./talk-bug-summaries/F1-manual-policy-wrong-revision.md).

**Original claim.** Under `compositionUpdatePolicy: Manual`, the XR's `compositionRevisionRef` can point to a revision that doesn't match `compositionRef`, and Crossplane silently composes with the pinned (wrong) revision content. No validation, no error.

**Status:** ✅ **Reproduces (real cluster)** — confidence HIGH.

**New evidence:**
- **Tier 0 (harness re-run):** still-reproduces classification confirmed against fidelity-hardened harness (PR #76 RV checking + DELETE/GC fixes).
- **Tier 1 — R-2:** Harness fixture seeds the right initial state (no pre-baked CompositionRevisions with bad labels). Bug mechanism source-grounded: `APIRevisionFetcher.Fetch` calls `Get(currentRevision.Name)` with no validation under Manual policy.
- **Tier 2 — R-6:** No XR-targeted mutating, defaulting, or conversion webhook ships in v2.2.0; the only webhook is the unrelated `crossplane-no-usages` validator on DELETE. The hypothesis "a webhook auto-corrects the bad cross-reference in production" is closed.
- **Tier 3 — [R-11](./crossplane-audits/R11-f1-real-cluster.md):** Direct real-cluster reproduction. `kubectl patch xwidget/example` to mutate `compositionRef alpha→beta` while leaving `compositionRevisionRef` pinned to alpha-rev is silently accepted. No defaulting webhook fires. For ≥60 seconds: `compositionRef=beta`, `compositionRevisionRef=alpha-a39f01a`, `Synced=True (ReconcileSuccess)`, composed resource is `alpha-output { source: alpha }` — confirming Crossplane uses the **pinned revision's content** despite the new compositionRef. The `alpha-output` ConfigMap's `resourceVersion` is constant throughout (Crossplane reconciles to the pinned desired state and finds no drift). Switching the policy to Automatic reconciles `compositionRevisionRef` to beta-rev within ~60s — confirming the bug only persists under Manual policy.

**Draft for upstream comment:** [`upstream-updates/7220-f1-manual-policy.md`](./upstream-updates/7220-f1-manual-policy.md).

---

## [#7221 — F2: Unconditional `Status().Update()`](https://github.com/crossplane/crossplane/issues/7221)

**Talk-prep summary:** [`talk-bug-summaries/F2-unconditional-status-update.md`](./talk-bug-summaries/F2-unconditional-status-update.md).

**Original claim.** Composition reconciler calls `Status().Update()` unconditionally on every reconcile, generating spurious resourceVersion churn and watch noise.

**Status:** ✅ **Fixed upstream.** Maintainer accepted; PR [#7283](https://github.com/crossplane/crossplane/pull/7283) is in flight.

**New evidence:** None needed — accepted by maintainers; F2 was the only one of the six that landed cleanly with a fix in flight at the time of the SPRINT-0001 kickoff. No further audit work performed.

---

## [#7222 — F3: Composition deletion error loop](https://github.com/crossplane/crossplane/issues/7222)

**Talk-prep summary:** [`talk-bug-summaries/F3-composition-deletion-error-loop.md`](./talk-bug-summaries/F3-composition-deletion-error-loop.md).

**Original claim.** Deleting a Composition while an XR is bound puts the XR into a permanent `Synced=False / ReconcileError` loop with no automatic recovery.

**Status:** 🔄 **Shifts (still-reproduces, broader than originally framed)** — confidence HIGH.

**Audit history:** This finding was briefly framed as a "provisional retraction with maintainer ask" based on hash-only evidence in the hardened harness. The R-4 trace audit refuted that framing. R-14 then **reproduced the bug directly on a real cluster with a different error family than the harness re-run**, broadening the original report rather than narrowing it.

**New evidence:**
- **Tier 0 (harness re-run):** New XWidget terminal hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`) emerged in the hardened harness; `ConfigMap` divergence collapsed; `errFetchComp` ("Composition not found") family disappeared from terminals. Initially looked like potential retraction.
- **R-4 (trace audit):** Parsed all 6 terminal states' XWidget objects out of the 211 MB JSONL dump. **All 4 unique terminal hashes carry `Synced=False / reason=ReconcileError`** with the original `errSelectComp` message ("cannot select Composition: no compatible Compositions found"). The hash drift is cosmetic — explained entirely by `metadata.generation` (3 vs 4) and `metadata.resourceVersion` (12, 13, 19, 20) re-sequencing under the new GC reconciler. Refutes the retraction framing.
- **Tier 2 — R-8:** Harness GC behavior matches Background-style cascade for single-owner dependents; the ConfigMap-collapse is consistent with that, not with a fidelity bug.
- **Tier 3 — [R-14](./crossplane-audits/R14-f3-real-cluster.md):** Direct real-cluster reproduction. `kubectl delete composition` returns immediately (no finalizer guards Composition deletion in v2.2.0). The owned `CompositionRevision` is GC'd by Kubernetes via `ownerReferences`. By T+90s the XR transitions to `Synced=False / ReconcileError` with message `"cannot fetch Composition: cannot get CompositionRevision: CompositionRevision.apiextensions.crossplane.io \"widget-composition-alpha-a39f01a\" not found"`. This persists for the full 5-minute observation window. The composed `alpha-output` ConfigMap is **untouched** (rv constant at 924). **Notable: the error family observed on the real cluster is `errFetchComp`, the family that did NOT surface in the hardened-harness re-run** (which only exhibited `errSelectComp`). This is because Manual-policy XRs (real-cluster test) hold their refs and hit the fetch path, while the harness scenarios use Automatic policy and clear refs on the cleanup path. **Both error families are real production F3 pathologies.**

**Draft for upstream comment:** [`upstream-updates/7222-f3-composition-deletion.md`](./upstream-updates/7222-f3-composition-deletion.md). The `api.go:251-252` TODO ("need to block the deletion of composition via finalizer once it's selected") remains the natural fix surface.

---

## [#7223 — F5 + F6 (combined issue)](https://github.com/crossplane/crossplane/issues/7223)

This issue bundled two distinct claims. After audits the F6 sub-claim split into two — `F6-orphan` and `F6-stale-Ready`.

### F5: Stale `ValidPipeline=True` race

**Talk-prep summary:** [`talk-bug-summaries/F5-stale-validpipeline-race.md`](./talk-bug-summaries/F5-stale-validpipeline-race.md).

**Original claim.** A timing race between FunctionRevision capability changes and CompositionRevision validation can leave `ValidPipeline=True` stale, allowing an XR reconcile to compose with a now-invalid pipeline.

**Status:** ✅ **Reproduces (harness + source-grounded)** — confidence HIGH; real-cluster reproduction inconclusive but does not change posture.

**New evidence:**
- **Tier 0:** Still-reproduces / shifts in the hardened harness.
- **Tier 1 — R-1:** Production wires the FunctionRevision → CompositionRevision watch. The harness wires it (with hardcoded single-revision target — bounded gap, doesn't affect current scenarios).
- **Tier 2 — R-7:** controller-runtime workqueues de-dup per-key per-controller but do **not** serialize cross-controller reconciles. The F5 race window is timing-feasible in production; the harness scheduler explores valid orderings.
- **Tier 3 — [R-12](./crossplane-audits/R12-f5-real-cluster.md):** ⚠️ **Inconclusive on real cluster.** Approach A (manual `kubectl patch` of `status.capabilities = []` on the FunctionRevision) was attempted; the patch initially succeeded but the package-manager controller reverts it within seconds. This itself is a meaningful observation: **the FunctionRevision controller actively reconciles `status.capabilities` from package metadata**, which means manual mid-reconcile capability stripping cannot be made stable from the outside. Approach B (build and deploy a custom function package without the `composition` capability, then trigger version transition) was deferred — out of scope for the initial real-cluster pass. F5 confidence does not depend on this; R-1 + R-7 are sufficient.

### F6-orphan: composed resources persist as orphans while function stays fatal

**Talk-prep summary:** [`talk-bug-summaries/F6-orphan-persistence-while-fatal.md`](./talk-bug-summaries/F6-orphan-persistence-while-fatal.md).

**Original claim.** When a Composition switches to use a function that returns SEVERITY_FATAL, previously-composed resources persist as orphans (no GC) until the function is fixed.

**Status:** ✅ **Reproduces (real cluster)** — confidence HIGH (when scoped to "while function stays fatal"); recovery on next successful reconcile per R-9.

**New evidence:**
- **Tier 1 — R-3:** Source-grounded at exact line numbers in v2.2.0. SEVERITY_FATAL early-return at `composition_functions.go:439` skips the GC call at `composition_functions.go:538`.
- **Tier 1 — R-2:** F6 fatal stub triggers the documented production code path.
- **Tier 2 — R-9:** Every retry retakes the same fatal early-return; GC is unreachable while the function stays fatal. Once the function is fixed, the next successful reconcile WILL GC. Draft scoping to "while fatal" is correct.
- **Tier 2 — R-10:** Stub adequately models the response contract for the fatal path. Bare runner (no `FetchingFunctionRunner` wrap) is OK because no current scenario uses response `Requirements`.
- **Tier 3 — [R-13](./crossplane-audits/R13-f6-real-cluster.md):** Direct real-cluster reproduction. After patching XR to a Composition referencing a non-resolvable function (same observable-effect as SEVERITY_FATAL: pipeline can't complete, GC step skipped), `alpha-output` ConfigMap is **untouched for 3 minutes** (`rv` constant at 3555, data still `{source: alpha}` from the prior alpha Composition). XR's `resourceRefs` still tracks `[ConfigMap/default/alpha-output]`. `Synced=False / ReconcileError` loop persists indefinitely with no recovery. The error family observed (`"cannot find an active FunctionRevision"`) differs from `SEVERITY_FATAL` but reaches the same GC-skip code path; R-3 source-grounding is the strongest evidence for the SEVERITY_FATAL case specifically.

### F6-stale-Ready: stale `Ready=True` while `Synced=False`

**Talk-prep summary:** [`talk-bug-summaries/F6-stale-ready-true.md`](./talk-bug-summaries/F6-stale-ready-true.md).

**Original claim.** When the XR enters an error path, `Ready=True` is left stale alongside `Synced=False / ReconcileError`, producing a confusing user-visible state.

**Status:** ✅ **Reproduces (source-grounded; mechanism confirmed on real cluster, exact value pending follow-up)** — confidence HIGH.

**New evidence:**
- **Tier 1 — R-3:** Source-grounded at `reconciler.go:744`. The reconciler skips system conditions when iterating to mark unknown on the error path. `Ready` is a system condition. **Stale `Ready` (whatever value it had pre-error) is therefore *guaranteed* on the error path, not race-dependent.** This is the cleanest claim in the entire batch.
- **Tier 3 — [R-13](./crossplane-audits/R13-f6-real-cluster.md):** Confirmed the underlying invariant on the real cluster: `Ready` persists unchanged across the error transition (same `lastTransitionTime`, reason, and message for the full 3-minute observation). The specific stale value in this test was `Ready=False (Creating)` rather than `Ready=True`, because the test composition's composed resource (vanilla ConfigMap) doesn't publish a Ready condition — so the XR was never `Ready=True` to begin with. The mechanism is unambiguously confirmed; producing a positive `Ready=True` reproduction would require a composed resource type that publishes its own Ready condition (e.g., a Pod or downstream XR), and is left as deferred follow-up work. R-3's source-grounding is sufficient on its own to defend the claim.

**Draft for upstream comment:** [`upstream-updates/7223-f5-f6-reframe.md`](./upstream-updates/7223-f5-f6-reframe.md). Withdraws the original GC-on-fatal proposal.

---

## [#7224 — C2 + C4 (combined issue)](https://github.com/crossplane/crossplane/issues/7224)

**Talk-prep summaries:** [`talk-bug-summaries/C2-claim-deletion-false-positive.md`](./talk-bug-summaries/C2-claim-deletion-false-positive.md), [`talk-bug-summaries/C4-cross-xr-ownership-theft.md`](./talk-bug-summaries/C4-cross-xr-ownership-theft.md).

**Original claim.** Two separate fidelity / correctness concerns about claim deletion and composition state validation. Closed by issue author 2026-04-09.

**Status:** 🟡 **Closed by author.**

**New evidence:**
- **C2 (claim-deletion-validation):** Re-validated internally that the harness commit `e4daf33` (DELETE that preserves finalizers/spec/status + GarbageCollectorReconciler) closes the original C2 false positive. Documented in [`upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md`](./upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md). **Not for upstream posting** — issue is closed.
- **C4:** Still gated on a planned harness fix (Fix 3); not pursued in this re-evaluation. No upstream draft.

---

## Summary table

| Issue | Sub-claim | Final status | Real-cluster tier 3? |
|---|---|---|---|
| [#7220](https://github.com/crossplane/crossplane/issues/7220) | F1 — Manual update policy wrong revision | ✅ Reproduces (real cluster) | ✅ R-11 |
| [#7221](https://github.com/crossplane/crossplane/issues/7221) | F2 — Unconditional Status().Update() | ✅ Fixed upstream (PR #7283) | n/a |
| [#7222](https://github.com/crossplane/crossplane/issues/7222) | F3 — Composition deletion error loop | 🔄 Shifts (broader than originally framed) | ✅ R-14 (new error family surfaced) |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) | F5 — Stale ValidPipeline race | ✅ Reproduces (harness + source) | ⚠️ R-12 inconclusive (Approach B deferred) |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) | F6-orphan — Orphan persistence while fatal | ✅ Reproduces (real cluster) | ✅ R-13 |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) | F6-stale-Ready — Stale Ready=True | ✅ Reproduces (source-grounded; mechanism confirmed on cluster) | ⚠️ R-13 partial (mechanism confirmed; positive Ready=True reproduction deferred) |
| [#7224](https://github.com/crossplane/crossplane/issues/7224) | C2 — Claim deletion validation | 🟡 Closed by author; harness fix internally validated | n/a |
| [#7224](https://github.com/crossplane/crossplane/issues/7224) | C4 — (gated on Fix 3) | 🟡 Closed by author; not pursued | n/a |

## Posting status

All four open-issue drafts (F1, F3, F5, F6 sub-claims under #7223) are at **HIGH** confidence after Tier 1 + Tier 2 + Tier 3. Real-cluster reproductions exist for F1, F3, F6-orphan; F5 and F6-stale-Ready stand on source code + Tier 2 grounds. Drafts are staged in [`upstream-updates/`](./upstream-updates/) for manual review before posting.
