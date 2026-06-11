# #7224 C2 fidelity-fix validation — STAGED EVIDENCE ONLY, NOT FOR POSTING

> **DO NOT POST TO #7224.** This issue is closed. This file is internal validation that the Kamera fidelity fix (`6cd7396` / `e4daf33`) closes the C2 false positive that forced #7224's retraction. Keep for sprint records and future "did we close the gap" auditing only.

**Re-run date:** 2026-04-28
**Harness HEAD:** `crossplane-reeval` @ `89acd8a`

## What was being validated

Issue #7224 was filed 2026-03-19 reporting two divergences in the Crossplane Claim subsystem:
- **C2** (claim deletion orphans XR + composed resources) — 96/98 trials orphan, 2/98 cleanup.
- **C4** (two XRs steal ownership of shared composed resource) — split 55/45 on which XR wins.

After review by jbw976, both were determined to be Kamera simulation-fidelity bugs:
- **C2** caused by Kamera's external-user DELETE clobbering the finalizer the ClaimReconciler set.
- **C4** caused by Kamera's SSA implementation handling field-level merge semantics incorrectly (real `meta.AddControllerReference()` rejects cross-XR theft).

The fix for C2 (`6cd7396` "Simulation fidelity fixes: ownerRef GC, PVC labels, Delete semantics", cherry-picked as `e4daf33` onto `crossplane-reeval`) makes `Client.Delete` read current object state before setting `deletionTimestamp`, preserving existing finalizers/spec/status.

The fix for C4 (Fix 3 — SSA field-manager conflict detection) is **not yet implemented**. C4 was therefore explicitly out of scope for this validation.

## C2 re-run

**Input:** `examples/crossplane/scenarios/workflow_crossplane-claim_claim-deleted-during-composition.json`
**Output:** `/tmp/crossplane-reeval-89acd8a/c2/primary/`
**Run command:** `crossplane-harness -interactive=false -closed-loop=true -depth 100 -inputs <input> -output <dump>`
**Wall-clock:** ~12 minutes (Monte Carlo across many trials).

## Campaign metrics

| | Baseline (Kamera bug) | Hardened (`e4daf33`) |
|---|---|---|
| Unique node visits | n/a | 1,200 |
| Total node visits | n/a | 3,030 |
| Resource states | 2 | 378 |
| Trials with orphaned XR + ConfigMap | 96/98 (98%) | 0 (sampled) |
| Trials with full cleanup | 2/98 (2%) | all (sampled) |
| Max-depth aborted states | n/a | 30 |

## Terminal-state diff

Reference + rerun phases: **1 converged state with 0 differing objects.**

Sampled per-trial diffs: each trial converges to a single terminal with 0 differing objects.

The 96/98-vs-2/98 orphan/cleanup divergence has **completely collapsed**. The hardened harness shows uniform clean cleanup behavior — the simulated DELETE no longer clobbers the ClaimReconciler's finalizer, the cascade runs as it would in production, and there is no orphan to observe.

## Conclusion

**`e4daf33` (cherry-pick of `6cd7396`) closes the C2 fidelity gap.** Future C2-class scenarios in any project's harness can be re-investigated without re-introducing the false positive that forced #7224's retraction.

## Status of C4

The C4 false positive (cross-XR ownership theft via shared composed resource) is **still reproducible on the hardened harness** because Fix 3 — SSA field-manager conflict detection — has not yet been implemented. Tracked in `docs/plans/2026-03-25-patch-semantics-fidelity.md`. Implementing Fix 3 is a prerequisite for any future C4-class re-investigation.

## Confidence note

The validation is robust on a sampling basis (multiple trials inspected, all show single-terminal convergence) but was not exhaustive across all 98 baseline-equivalent trials. The metrics-level signal (unique state count and converged-state count) is consistent with full-cleanup-everywhere; if any latent orphan-creating ordering survived, it would manifest as a multi-state diff. None did.
