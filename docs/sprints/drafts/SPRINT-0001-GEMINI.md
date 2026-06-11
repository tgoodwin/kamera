# Sprint Plan: SPRINT-0001 (Crossplane Bug Re-Evaluation)

**Intent:** Plan a sprint to re-evaluate four open Crossplane bug reports against a fidelity-hardened Kamera harness on the `crossplane-reeval` branch (HEAD `89acd8a`).

## Goals
- Re-validate findings F1, F3, F5, and F6 against the hardened harness.
- Classify each issue as **still-reproduces**, **shifts**, or **retracts**.
- Validate that the DELETE semantics fix (`6cd7396`) closes the #7224 false positive (C2).
- Produce draft GitHub issue updates for manual posting.

## Scope Boundaries
- **In Scope:**
    - Re-running scenarios at depth 100 on `crossplane-reeval` branch.
    - Hash-based and trace-based comparison against recorded baselines in `docs/crossplane-reevaluation-plan.md`.
    - Drafting updates in `docs/upstream-updates/`.
    - Validation of C2 fix.
- **Out of Scope:**
    - Fix 3 (SSA field-manager conflict detection) implementation.
    - C4 (shared composition ownership theft) re-investigation.
    - Fixed timebox.

## Task List

### 1. Preparation & Harness Setup
- [ ] Checkout `crossplane-reeval` branch (HEAD `89acd8a`).
- [ ] Verify harness builds against `crossplane v2.2.0` on current state.
- [ ] Ensure all required scenario JSONs are staged/accessible in `examples/crossplane/scenarios/`.

### 2. Execution: High-Priority Re-runs (Depth 100)
- [ ] **F3 (Composition Deletion):** Run `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`.
    - *Baseline:* 44 unique nodes / 324 visits / 3 aborted states / distinct error paths `errFetchComp` and `errSelectComp`.
- [ ] **F5 (Stale ValidPipeline):** Run `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json`.
    - *Baseline:* 3 outcome categories (A: correct, B/C: bug).
- [ ] **F6 (Fatal Function Orphans):** Run `workflow_crossplane-function-failure_composition-switches-to-fatal.json`.
    - *Baseline:* Orphan persistence in 86% of trials; stale `Ready=True` on XR.

### 3. Execution: Sanity Checks & Validation (Depth 100)
- [ ] **F1 (Manual Policy):** Run `workflow_crossplane-policy_manual-update-policy-composition-switch.json`.
    - *Baseline:* Trace step 4 shows silent composition with wrong revision.
- [ ] **C2 Validation (Claim Deletion):** Run `workflow_crossplane-claim_claim-deleted-during-composition.json`.
    - *Goal:* Confirm divergence collapses (no orphaning) now that DELETE preserves finalizers.

### 4. Analysis & Classification
- [ ] Compare F3 terminal-state hashes and campaign metrics. Determine if `GarbageCollectorReconciler` or DELETE semantics shifted the outcome.
- [ ] Verify F1 still produces the same logic failure (expected: no change).
- [ ] Analyze F5 outcome ratios and confirm resourceVersion conflict checking hasn't eliminated the race.
- [ ] Confirm F6 still produces orphans and stale status.
- [ ] Document result for C2 (staged only, do not post to #7224).

### 5. Reporting & Documentation
- [ ] **Draft Update #7220 (F1):** Confirm reproduction on hardened harness `89acd8a`.
- [ ] **Draft Update #7222 (F3):** Document if bug persists or outcome shifted/retracted.
- [ ] **Draft Update #7223 (F5/F6):**
    - Reframe to separate F5 (race) and F6 (stale Ready).
    - Drop the unsafe GC-on-fatal proposal.
- [ ] Save all drafts to `docs/upstream-updates/`.

## Sequencing
1. **Prep:** Checkout branch and build.
2. **Critical Path:** Run F3 (most likely to shift) and F5.
3. **Validation:** Run C2 to confirm harness fidelity.
4. **Sanity:** Run F1 and F6.
5. **Finalize:** Compare all hashes and draft the GitHub updates.

## Risks
- **F3 Divergence Collapse:** If F3 is a false positive, it reduces our high-priority bug count.
- **Harness Regressions:** `crossplane-reeval` is an integrated branch; potential for new simulation bugs.
- **Depth 100 Overhead:** Long execution times for complex scenarios.

## Acceptance Criteria
- All 5 scenarios executed at depth 100.
- New terminal-state hashes compared against §3 baselines in `docs/crossplane-reevaluation-plan.md`.
- Each issue classified (Still Reproduces / Shifts / Retracts).
- Draft updates for #7220, #7222, and #7223 available in `docs/upstream-updates/`.
- C2 fix validated internally.
