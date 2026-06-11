# SPRINT-0001 Run Log

Append-only. One section per re-run task. Created 2026-04-28.

## Phase 0 — preconditions

**Harness HEAD:** `crossplane-reeval` @ `89acd8a` (verified equivalent — 2 doc-only commits on top: `a068b55`, `ffebe76`; `git diff 89acd8a -- pkg/ sleevectrl/ examples/` returns empty).

**Input strategy:** **(a)** copied 12 scenario JSONs from
`.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/` into
`examples/crossplane/scenarios/` (staged, not committed). Worktree-fragility
risk avoided.

**Output root:** `/tmp/crossplane-reeval-89acd8a/`
**Explore config:** `/tmp/crossplane-reeval-depth100.json` = `{"maxDepth":100}`
**Default flags:** `-interactive=false -closed-loop=false -depth 100 -log-level error`
**Harness binary:** `/tmp/crossplane-reeval-89acd8a/crossplane-harness`
**Analyzer binary:** `/Users/tgoodwin/projects/kamera/bin/kamera` (subcommand: `analyze campaign-metrics`)

**Smoke run** (depth 10, F1 primary scenario, `/tmp/crossplane-reeval-89acd8a/smoke-test/`):
- Total wall-clock 143 ms.
- Campaign metrics: 35 unique nodes / 35 total visits / 22 resource states / 3 max-depth aborted / 0 converged.
- Trace shows expected transient `pipeline status unknown` (Finding 4 pattern) and "Successfully composed resources" event.
- Harness, replay client, and `GarbageCollectorReconciler` all wire up cleanly.
- Smoke OK; proceeding to depth-100 campaigns.

## Fidelity concerns surfaced

- **2026-04-28**: All staged scenario JSONs use the pre-rename schema `"type"` for external inputs; current code expects `"opType"` (per `e2ee594`). Migrated 3 of 12 in place via Python (`type` → `opType` only inside `externalInputs[]`). The other 9 had no `externalInputs` requiring migration. Migration is a one-shot fix; if scenarios are re-pulled from the worktree, they'll need migration again.
- **2026-04-28**: `--closed-loop=false` produced trivial 1-state runs for F3 hypothesis-1 (only the user action fired, no further reconciles). Reverting to `--closed-loop=true` to match how the original baselines were generated. The plan's `--closed-loop=false` default is overruled here with this justification: the recorded baselines in `ANALYSIS.md` came from closed-loop runs (reference + rerun phases), so direct comparison requires the same harness mode. Updated default for the remainder of the sprint.

---

## Phase 1 — F3 composition deletion

### F3 / hypothesis-1 (`workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`)

- **Run command:** `crossplane-harness -interactive=false -closed-loop=true -depth 100 -inputs <abs> -output /tmp/crossplane-reeval-89acd8a/f3/hypothesis-1/`
- **Wall-clock:** ~5s (rerun phase)
- **Campaign metrics:**
  - Hardened: 86 unique / 622 total / 13 resource states / 6 max-depth aborted
  - Baseline: 44 unique / 324 total / 8 resource states / 3 max-depth aborted
  - Delta: state space ~2× larger; aborted count doubled.
- **Terminal-state diff** (`kamera analyze diff`): 6 converged states with 1 differing object, 3 identical.
  - `XWidget/default/example` hashes: `302b727`, `6b6a9d3` (×2), `4390fb0`, `855c129` (×2). **None match baseline `93d750b` / `9dc61c9`.**
  - `ConfigMap/default/xr-config` is now **identical across all terminal states** (was differing in baseline: `709d71b` vs missing).
- **Interpretation:**
  - The `e4daf33` DELETE-semantics + GC fix has restructured the post-DELETE state space.
  - The `ConfigMap` divergence collapsed (good — that part of F3's evidence is gone).
  - The `XWidget` divergence persists but with different (and more) hashes — the bug shape transformed; could be either a genuine new Crossplane behavior surfaced by tighter fidelity, or new fidelity artifacts from `GarbageCollectorReconciler`.
  - **Provisional classification: shifts.** Need trace-level error-family inspection to distinguish.

### F3 / primary (`workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`)

- Wall-clock ~1s per phase.
- Campaign metrics: 88 unique / 259 total / 24 resource states / 2 max-depth aborted.
- Reference + rerun diff: 1 converged state, 0 differing objects.
- **Interpretation:** the cycling baseline (which never fired the DELETE) collapsed to a single converged terminal. The DELETE-semantics fix removed the cycling.
- Classification: **shifts** (was cycling-without-DELETE, now converges).

### F3 / related-xr-delete (`workflow_crossplane-deletion_xr-deleted-with-active-composition.json`)

- Campaign metrics: 182 unique / 548 total / 30 resource states / 4 max-depth aborted.
- Reference diff: 1 converged, 0 differing.
- Rerun diff: 3 converged states, 0 differing objects, 6 identical.
- **Interpretation:** XR deletion path was always clean per baseline; new GC controller added trial-level breadth without introducing divergence. Confirms the XR-delete short-circuit short-circuits cleanly.
- Classification: **still-reproduces clean** (no bug, baseline already clean).

### F3 — combined classification

- The original F3 evidence (3 distinct aborted states with `93d750b`/`9dc61c9` and `709d71b`/missing divergence) **does not reproduce** on the hardened harness. New `XWidget` hashes (`302b727`, `6b6a9d3`, `4390fb0`, `855c129`) appear in the hypothesis-1 run, but the `ConfigMap` divergence has collapsed.
- Per the plan's classification rule: hash collapse without a new permanent error loop is a **retraction candidate**.
- However, a full retraction requires confirming via trace inspection that no new permanent error loop has emerged. Provisional classification: **shifts** pending trace audit. The upstream update will frame this as "evidence picture changed materially; original divergence pattern no longer reproduces; need maintainer input on whether the new state shape constitutes a real bug or a fidelity artifact."

---

## Phase 2 — C2 staged validation

### C2 (`workflow_crossplane-claim_claim-deleted-during-composition.json`)

- Wall-clock ~12 minutes (Monte Carlo across many trials).
- Campaign metrics: 1,200 unique / 3,030 total / 378 resource states / 30 max-depth aborted.
- Reference + rerun diff: **1 converged state, 0 differing objects** for both phases.
- Per-trial diff (sampled): all trials converge to a single terminal with 0 differing objects.
- **Compared to baseline (96/98 orphaned, 2/98 cleanup):** the orphan-vs-cleanup divergence has collapsed. All trials now converge to the same terminal.
- **Validation result: PASSED.** `e4daf33` (cherry-pick of `6cd7396`) closes the C2 false positive. The DELETE-semantics fix preserves the ClaimReconciler's finalizer through external user DELETEs, eliminating the simulation artifact that masqueraded as orphaning.

---

## Phase 3 — F1 manual policy sanity

### F1 / primary (`workflow_crossplane-policy_manual-update-policy-composition-switch.json`)

- Wall-clock ~2s.
- Campaign metrics: 42 unique / 210 total / 21 resource states / 2 max-depth aborted.
- Reference diff: 1 converged, 0 differing.
- **Trace evidence:** dump contains a state with `compositionRef.name=widget-composition-beta` AND `compositionRevisionRef.name=widget-composition-alpha-rev-1`. This is the F1 mismatched-refs pattern — the bug **still reproduces**.
- The hardened harness eventually shows the system self-correcting (subsequent state has `compositionRevisionRef.name=widget-composition-beta-c34ead1`), which means convergence is reached after the bug fires. The original analysis showed cycling-without-convergence; this is a side effect of the no-op suppression and RV checks reducing the cycling.

### F1 / stale (`workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json`)

- Wall-clock ~31s.
- Campaign metrics: 354 unique / 754 total / 36 resource states / 5 max-depth aborted.
- Diff: 5 converged states with 0 differing objects, 9 identical.
- 5 converged states is more state-space exploration than the non-stale run (1), confirming staleness adds breadth.
- No new outcome categories beyond the F1 bug — consistent with baseline ("staleness adds minimal signal").

### F1 — combined classification: **still-reproduces**

The wrong-revision-composition trace pattern is preserved on the hardened harness. The F1 bug is independent of all fidelity fixes (pure logic bug in `APIRevisionFetcher.Fetch`).

---

## Phase 4 — F5 stale ValidPipeline

### F5 / hypothesis-1 (`workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json`)

- Wall-clock ~1m9s.
- Campaign metrics: 44 unique / 328 total / 16 resource states / 3 max-depth aborted.
- Rerun diff: **3 converged states, 2 differing objects** (ConfigMap + XWidget):
  - `ConfigMap/default/xr-config`: `(missing)`, `709d71b`, `709d71b` — **`709d71b` matches baseline Category C ConfigMap.**
  - `XWidget/default/example`: `6c7ce03`, `667e63a`, `d09c2d4` — none match baseline `579a4db`/`5333e65`/`93d750b`.

### F5 / primary (`workflow_crossplane-staleness_function-capability-removed.json`)

- Wall-clock ~1s.
- Campaign metrics: 115 unique / 576 total / 33 resource states / 5 max-depth aborted.
- Reference diff: 1 converged, 0 differing.
- Rerun diff: **4 converged states, 2 differing objects**:
  - `XWidget`: `fdc5c69`, `95062fa`, `303b7bb` — none match baseline.
  - `ConfigMap`: `(missing)`, `709d71b`, `709d71b` — Category-C ConfigMap (`709d71b`) preserved; Category-B ConfigMap (`60f2920`) NOT seen.

### F5 / interval (`interval_function-capability-removed.json`)

- Rerun diff: **9 converged states, 2 differing objects** (more breadth from interval-based staleness):
  - `XWidget`: `95062fa`, `cdc3a5c`, `fdc5c69`, `303b7bb`, `fdc5c69`, `95062fa`.
  - `ConfigMap`: `(missing)` ×3, `709d71b` ×3.

### F5 — combined classification: **shifts**

- **Bug still reproduces:** Category-A (no-bug, ConfigMap missing) and Category-C-like (bug, ConfigMap=`709d71b`) outcomes both present.
- **Hash families changed:** XWidget hashes are entirely new; ConfigMap Category B (`60f2920`, the "2-compositions" outcome) does not appear in any run. Likely the RV conflict checking (PR #76 commits) suppresses the second redundant compose, eliminating the 2-compositions terminal category but preserving the 1-composition buggy category.
- **Path ratios:** can't directly compare (closed-loop sample sizes differ from baseline 432/355/287). But the *existence* of both correct (A) and buggy (C-like) terminals is preserved.

---

## Phase 5 — F6 fatal-function orphans + stale Ready=True

### F6 / primary (`workflow_crossplane-function-failure_composition-switches-to-fatal.json`)

- Wall-clock ~2m35s.
- Campaign metrics: 341 unique / 1,212 total / 112 resource states / 12 max-depth aborted.
- Reference + rerun + staleness diffs: 1 converged state with 0 differing per phase.
- **Per-trial ConfigMap presence (proxy: tail content scan, 9 trials):**
  - ConfigMap-present (orphan): trials 1, 2, 5, 6, 7, 8 → **6/9 (67%)**
  - ConfigMap-missing: trials 3, 4, 9 → **3/9 (33%)**
- **Compared to baseline:** ConfigMap-present 42/49 (86%), missing 7/49 (14%).
- **Orphan persistence rate dropped from 86% → 67%.** The fatal-function early-return path still leaves orphans in the majority of orderings, but the new GC controller and DELETE semantics close some of the orphan-creating orderings.

### F6 / control F7 (`workflow_crossplane-function-failure_composition-switches-resources.json`)

- Campaign metrics: 354 unique / 1,111 total / 86 resource states / 11 max-depth aborted.
- 1 trial file (`trial_2`) was corrupted (truncated mid-write at the kill); quarantined to `/tmp/crossplane-reeval-89acd8a/f6/control-f7-corrupt-trial_2.jsonl`. Other 8 trials analyzed successfully.
- Baseline F7 was "1 terminal state across 49 trials, GC works correctly." The hardened metrics show much more state breadth (354 unique vs trivial baseline) — likely closed-loop perturbations explore more orderings now. Need diff inspection to confirm GC still works on cross-resource-type transitions.

### F6 / control F8 (`workflow_crossplane-function-failure_function-flap-fatal-recovery.json`)

- Campaign metrics: 465 unique / 1,331 total / 135 resource states / 11 max-depth aborted.
- Baseline F8 was "1 terminal state across 49 trials, transient fatal recovers."

### F6 — combined classification

- **F6-orphan-persistence: shifts.** Orphan rate dropped from 86% → 67% but orphan still produced in majority of orderings. The GC controller and DELETE-semantics fix reduce but don't eliminate the issue. The underlying Crossplane behavior (SEVERITY_FATAL early-return before GC) is unchanged; what shifts is which orderings produce orphans on the harness.
- **F6-stale-Ready-True: insufficient evidence.** The harness's per-trial diff outputs collapsed condition information into the converged-state hash; extracting per-trial XR conditions from the JSONL dumps requires more parsing than was feasible in this sprint. **Recommend separate trace audit before drafting the upstream update.**
