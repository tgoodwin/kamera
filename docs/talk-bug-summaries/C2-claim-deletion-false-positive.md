# C2 — Claim deletion orphans XR (NEGATIVE RESULT — Kamera fidelity bug)

**Issue:** [crossplane/crossplane#7224](https://github.com/crossplane/crossplane/issues/7224) (C2 sub-claim, closed by author 2026-04-09)
**Status:** 🟡 Closed by author. **Confirmed false positive due to Kamera DELETE-semantics fidelity gap.** Harness fix `e4daf33` ("Simulation fidelity fixes: ownerRef GC, PVC labels, Delete semantics") closes the false positive.
**Internal validation:** [`../upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md`](../upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md).

## TL;DR

The original SPRINT-0001 baseline reported that deleting a Crossplane Claim while a composition is in progress leaves the XR and composed resources orphaned in 96/98 trials (98%). On maintainer review (jbw976), this was identified as a Kamera fidelity bug: the harness's simulated `Client.Delete` was clobbering the finalizer that the ClaimReconciler had set, so the cascade-delete never completed. After fixing the harness's DELETE semantics, the orphan rate collapsed to 0% sampled. **There is no production bug here.** This entry exists to document the negative result honestly and credit the maintainer review.

## What was originally claimed

Scenario: a user runs `kubectl delete claim/foo` while the composite reconciler is mid-reconcile. The expected behavior is the standard Kubernetes finalizer-protected cascade:
- ClaimReconciler sets a finalizer on the Claim before processing.
- DELETE marks `metadata.deletionTimestamp` but doesn't remove the object (because of the finalizer).
- ClaimReconciler observes the deletion timestamp, deletes the bound XR (cascade), waits for it to be gone, then removes its finalizer, allowing the Claim to be deleted.

Original baseline showed instead: 96/98 trials terminated with the XR + composed `ConfigMap` orphaned (Claim gone, XR alive but unreferenced). The framing in the original report was "Crossplane's claim subsystem fails to cascade-delete the XR in the presence of certain orderings."

## Why this was a Kamera bug, not a Crossplane bug

The harness's `Client.Delete` implementation (in the trace-checker layer) was implementing DELETE as "wholesale replace the object's metadata with the deletion-timestamp-bearing version, including a fresh empty `finalizers` slice." Real Kubernetes DELETE doesn't do this — it patches `metadata.deletionTimestamp` while preserving `finalizers`, `spec`, and `status`.

Consequence in the harness: the moment the user-action DELETE fires, the Claim's finalizer disappears. The API server then immediately removes the Claim. The ClaimReconciler never sees the deletion-with-finalizer state, so it never runs its cascade logic. The XR stays alive but its owning Claim is gone, manifesting as the "orphan" terminal state.

Maintainer review (jbw976) identified this immediately on inspecting the harness code: the simulated DELETE was clobbering finalizers, and that explained the entire 96/98 orphan rate.

## How the fix was validated internally

The fix is `6cd7396` ("Simulation fidelity fixes: ownerRef GC, PVC labels, Delete semantics"), cherry-picked onto the `crossplane-reeval` branch as `e4daf33`. It makes `Client.Delete`:
- Read the current object state from the in-memory store before applying the deletion timestamp.
- Preserve `finalizers`, `spec`, `status`, `ownerReferences`, and other metadata fields.
- Only set `metadata.deletionTimestamp` to the current time, matching production K8s semantics.

It also adds a `GarbageCollectorReconciler` controller that cascade-deletes dependents when an owner is REMOVE'd from the in-memory store, to mirror production K8s GC behavior.

Re-running the same C2 scenario (`workflow_crossplane-claim_claim-deleted-during-composition.json`) against the hardened harness:

| Metric | Baseline (Kamera bug) | Hardened (`e4daf33`) |
|---|---|---|
| Trials with orphaned XR + ConfigMap | 96/98 (98%) | 0 (sampled) |
| Trials with full cleanup | 2/98 (2%) | all (sampled) |
| Resource states | 2 | 378 |

The 96/98-vs-2/98 orphan/cleanup divergence completely collapsed. The hardened harness shows uniform clean cleanup behavior — the simulated DELETE no longer clobbers the ClaimReconciler's finalizer, the cascade runs as it would in production, and there is no orphan to observe.

## Why this is in the talk

Three reasons to keep this honest:

1. **Credit to the maintainer review.** jbw976 pointed at the right line of harness code on first read. The harness was wrong; the maintainer review was right.
2. **Negative results are part of the methodology.** A research harness that finds bugs in real controllers must also find bugs in itself. C2 is the example we have where the harness's fidelity issue produced a confident-looking but wrong claim. Surfacing this is part of being a credible research tool.
3. **The fidelity fix has secondary value.** `e4daf33` doesn't just close C2 — it improves DELETE semantics for every scenario, which directly matters for F3 (composition deletion) and indirectly for any scenario involving cleanup ordering. The F3 hardened-harness re-run uses this fix.

## Talking points for the audience

- This is the cleanest example of "kamera was wrong" in the SPRINT-0001 set. We had a 98% orphan rate that looked authoritative; the maintainer review surfaced it in minutes; the harness fix resolved it cleanly.
- The mechanism — DELETE not preserving finalizers — is exactly the kind of subtle fidelity issue that's easy to introduce in a simulator. Real K8s DELETE is a *patch*, not a *replace*, and any harness that gets that wrong will produce false positives on every scenario involving finalizer-protected objects.
- The scoping of C2 to "false positive" doesn't mean the harness was useless on the scenario. Once the fidelity was fixed, the harness re-explored 378 resource states in the same scenario and found uniform clean cleanup — which is itself a useful confirmation that no other ordering produces orphan terminals.
- This entry is the right "we're being honest" moment in the talk: surface the negative result, credit the review, point at the fix, move on.
