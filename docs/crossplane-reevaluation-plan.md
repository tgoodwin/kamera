# Crossplane Issue Re-Evaluation Plan

**Owner:** tgoodwin
**Created:** 2026-04-28
**Status:** Draft — pending re-runs

After upstream review of [#7224](https://github.com/crossplane/crossplane/issues/7224)
identified two Kamera simulation-fidelity bugs masquerading as Crossplane bugs
(C2, C4), several fidelity gaps in the Kamera harness were closed (or are now
on a design plan). This document tracks which open Crossplane issues need to
be re-validated against the fidelity-hardened harness, and links each one to
the scenario JSON, the dump location of the prior campaign, and the relevant
fidelity commits.

---

## 1. Upstream issues filed against `crossplane/crossplane`

| # | Bug ID | Title | State | Disposition | Re-eval needed |
|---|--------|-------|-------|-------------|---------------|
| [#7220](https://github.com/crossplane/crossplane/issues/7220) | F1 | Manual update policy: switching `compositionRef` silently composes with wrong Composition's revision | Open | No maintainer response yet (P0). | Yes (sanity) |
| [#7221](https://github.com/crossplane/crossplane/issues/7221) | F2 | `CompositionRevisionReconciler` calls `Status().Update()` unconditionally — infinite reconcile loop | Open, label `performance` | Maintainer accepted; fix proposed in [PR #7283](https://github.com/crossplane/crossplane/pull/7283). | No — fix in flight |
| [#7222](https://github.com/crossplane/crossplane/issues/7222) | F3 | Deleting a Composition while XR is bound produces permanent `ReconcileError` | Open | No maintainer response yet (P1). | **Yes (high priority)** |
| [#7223](https://github.com/crossplane/crossplane/issues/7223) | F5 + F6 | Stale `ValidPipeline` condition + fatal-function orphaned resources | Open | F6 GC-on-fatal proposal **rejected as unsafe** by maintainer (later pipeline steps may add more desired resources). F5 acknowledged real but lower severity. Stale `Ready=True` in error path acknowledged. | Yes (medium) — and reframe issue body |
| [#7224](https://github.com/crossplane/crossplane/issues/7224) | C2 + C4 | Claim deletion orphans XR; shared composition silent ownership theft | **Closed (completed) 2026-04-09** | Both **false positives** due to Kamera fidelity bugs. Closed by author after maintainer review. | N/A — closed |
| [PR #7283](https://github.com/crossplane/crossplane/pull/7283) | F2 fix | Skip no-op status updates in revision and composite reconcilers | Open | Awaiting review. | N/A |

### Maintainer engagement note

`jbw976` (Crossplane maintainer) invited a Kamera presentation at an upcoming
Crossplane community meeting (#7224 thread, 2026-04-03). DM follow-up on
Crossplane Slack acknowledged.

---

## 2. Fidelity gap fixes since #7224 was filed (2026-03-19)

**Important — branch state as of 2026-04-28:** none of these are on `main`.
They are staged on the `crossplane-reeval` branch (off `main`,
HEAD = `89acd8a`), which is the integrated state the sprint runs against.

`crossplane-reeval` composition:

| Commit on `crossplane-reeval` | Origin | What it fixes | Code path |
|-------------------------------|--------|---------------|-----------|
| `89acd8a` | new (this branch) | **Sort initial-state hashes before seeding `resourceVersions`** so MC trials see identical initial RVs across runs. Pre-merge fix for the RV PR. | `pkg/tracecheck/explore.go` |
| `b12542d` | new (this branch) | **`init()` hard assertion of `mergeFromPatch` struct layout** so future controller-runtime upgrades panic with a clear message instead of going false-negative on RV checks. Pre-merge fix for the RV PR. | `pkg/replay/client.go` |
| `e4daf33` | cherry-pick of [`6cd7396`](https://github.com/tgoodwin/kamera/commit/6cd7396) from `zk-harness-and-fidelity-fixes` (2026-03-22) | **DELETE preserves finalizers/spec/status.** `Client.Delete` reads current object state before setting `deletionTimestamp` (matches real K8s DELETE which patches metadata, not replaces). Adds `GarbageCollectorReconciler` for cascade deletion of REMOVE'd parents. **Closes the C2 fidelity gap that forced #7224 retraction.** | `pkg/replay/client.go`, `sleevectrl/pkg/controller/gc_controller.go`, `pkg/tracecheck/trigger.go` |
| `1def992` | cherry-pick of [`04dd00c`](https://github.com/tgoodwin/kamera/commit/04dd00c) from [PR #76](https://github.com/tgoodwin/kamera/pull/76) | Tests for resourceVersion conflict checking and `OptimisticLock` detection. | `pkg/replay/*_test.go` |
| `7ba2045` | cherry-pick of [`f432bdc`](https://github.com/tgoodwin/kamera/commit/f432bdc) from PR #76 | Seed initial RVs and bump RV on CRD status patches. | `pkg/replay/`, `pkg/tracecheck/` |
| `cb1c43e` | cherry-pick of [`69c6e43`](https://github.com/tgoodwin/kamera/commit/69c6e43) from PR #76 | Detect `MergeFromWithOptimisticLock` and extract base resourceVersion. | `pkg/replay/client.go` |
| `911b3bd` | cherry-pick of [`4cfa2e7`](https://github.com/tgoodwin/kamera/commit/4cfa2e7) from PR #76 | **resourceVersion conflict tracking** (Fix 1 from plan). | `pkg/tracecheck/manager.go`, `pkg/replay/preconditions.go` |

Plus design-plan-only:
- [`2b56937`](https://github.com/tgoodwin/kamera/commit/2b56937) (already on `main`) — patch semantics fidelity design plan at `docs/plans/2026-03-25-patch-semantics-fidelity.md`. Audits 5 gaps.

**Outstanding upstream PRs / merge decisions:**
- **[PR #76](https://github.com/tgoodwin/kamera/pull/76)** — RV conflict checking. Open. Self-review posted 2026-04-28 with two pre-merge fixes (struct-layout assertion + deterministic seeding) already staged on `crossplane-reeval`. Merge into `main` with the two fixes cherry-picked, OR accept `crossplane-reeval` as the integrated state.
- **`6cd7396`** — DELETE semantics + GC controller. **No PR yet.** Buried in `zk-harness-and-fidelity-fixes` (112 ahead / 91 behind `main`) along with unrelated harness work (zookeeper-operator, rabbitmq-operator, cass-operator). Cherry-pick succeeded onto `crossplane-reeval` with auto-merge resolving 3 file overlaps. Needs a clean dedicated PR for upstream review post-sprint.

### Pre-#7224 fidelity work (also relevant — earlier campaigns predate these)

| Commit | Date | What it fixes |
|--------|------|---------------|
| [`d2935ba`](https://github.com/tgoodwin/kamera/commit/d2935ba) | 2026-03-11 | Auto-fixup of `ownerReference` UIDs after deterministic identity assignment. |
| [`9bb274b`](https://github.com/tgoodwin/kamera/commit/9bb274b) | 2026-03-17 | Apply JSON/Merge patches in replay client; SSA no-op detection (matches real API server v1.21+). |
| [`6bd26e9`](https://github.com/tgoodwin/kamera/commit/6bd26e9) | (pre-`9bb274b`) | Restrict no-op effect suppression to APPLY only — leaves PATCH/UPDATE no-ops visible (preserves F2 reproducibility). |

### Still open as code (planned, not yet implemented)

- **Fix 3 — SSA field-manager conflict detection** (per `docs/plans/2026-03-25-patch-semantics-fidelity.md`). Without this, the C4-style "two XRs steal ownership of same composed resource" pattern still reproduces in the harness. **Implement before re-publishing any cross-XR ownership findings.**
- Fix 2 — status SMP merge fidelity (documented as known limitation; cost/benefit ratio poor).
- Fix 4 — preserve patch type through effect pipeline (low priority, observability only).
- Fix 5 — label propagation comment (no code change needed).

---

## 3. Scenario re-evaluation matrix

### Input vs output

- **Inputs (scenario JSON definitions):** live on disk under
  `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/`.
  All 11 inputs referenced below have been verified present (2026-04-28).
  They were untracked from `main` in commits `4a6e80d` and `cc11c2b` as
  paper-research artifacts; copy back into `examples/crossplane/scenarios/`
  to re-run, or pass the absolute worktree path directly via `--inputs`.
- **Outputs (prior dump directories under `/tmp/...`):** assumed gone
  (macOS clears `/tmp` on reboot, dumps are from 2026-03-12 to 2026-03-17).
  **We are not regenerating the original campaign baselines.** The
  comparison target for each re-run is the terminal-state hashes already
  recorded in `examples/crossplane/.agents/ANALYSIS.md`, captured inline
  per-issue below.

Per-issue plan:

### #7220 — F1 (Manual update policy wrong revision)

- **Hypothesis:** Pure-logic bug, no perturbation needed. Reproduction is direct from trace step 4 of the reference run. Fidelity fixes do not touch the `Get(currentRevision)` code path in `APIRevisionFetcher.Fetch` (`api.go:161-167`).
- **Expected outcome:** Reproduces identically.
- **Re-eval priority:** Low (sanity check).
- **Input scenario JSONs (verified on disk):**
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch.json`
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json`
- **Recorded baseline (from `ANALYSIS.md` Finding 1):** at trace step 4 of the reference path, `CompositeReconciler` runs with `compositionRef → widget-composition-beta` but `compositionRevisionRef → widget-composition-alpha-rev-1`, produces 7 write effects including a ConfigMap, and reports success. Acceptance criterion: this exact step pattern still appears in the new trace.

### #7222 — F3 (Composition deletion permanent error loop)

- **Hypothesis:** **Most likely to shift outcome.** The reproduction depends on what state survives the external-user DELETE of the Composition. Pre-fix, the DELETE may have clobbered finalizers/spec in ways that masked or altered the divergence. Post-`6cd7396`, finalizer/spec preservation could:
  - (a) eliminate the divergence entirely (false positive),
  - (b) shift which error path each ordering hits ("Composition not found" vs "no compatible Compositions"),
  - (c) reproduce identically if the bug is genuinely in the post-DELETE handling and not in our DELETE modeling.
- The new `GarbageCollectorReconciler` (also in `6cd7396`) cascade-deletes the orphaned `CompositionRevision` on REMOVE — this could change the "permanent error loop" character because in the prior campaign, F3 explicitly noted "the model does not simulate GC."
- **Re-eval priority:** **High.**
- **Input scenario JSONs (verified on disk):**
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` (the variant with `userActionReadyDepths: {"0": 0}` that actually fired the DELETE — primary input for the re-run)
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-deletion_xr-deleted-with-active-composition.json` (related)
- **Recorded baseline (from `ANALYSIS.md` Finding 3, hypothesis-1 run):**
  - Campaign metrics: 44 unique nodes / 324 total visits / 8 resource states / 3 aborted states / cycling 7.4×
  - Effect counts: APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343
  - 3 distinct aborted states with 2 differing objects:
    - `XWidget/default/example`: state `aborted-3pu4puz1: 93d750b` (states 0,1) vs state `aborted-zwbbv3wq: 9dc61c9` (state 2)
    - `ConfigMap/default/xr-config`: state `aborted-3pu4puz1: 709d71b` (states 0,1) vs state `aborted-zwbbv3wq: missing` (state 2)
  - Two distinct error paths: `errFetchComp` ("Composition not found", CleanupReconciler-first ordering, 65 paths) vs `errSelectComp` ("no compatible Compositions found", CompositeReconciler-first ordering, 64 + 127 paths).
- **Action items if reproduction changes:**
  - If divergence collapses: update #7222 with retraction.
  - If divergence persists: update #7222 with new trace evidence on the fidelity-hardened harness; cite specific commit SHAs in the issue.

### #7223 — F5 (Stale `ValidPipeline` allows composition with invalidated functions)

- **Hypothesis:** Pure ordering bug. Reads cached `ValidPipeline` condition on `CompositionRevision`. The 4/26 resourceVersion conflict checking could change which orderings produce successful writes vs 409s, narrowing or shifting the 6/9 categories.
- **Re-eval priority:** Medium.
- **Input scenario JSONs (verified on disk):**
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-staleness_function-capability-removed.json`
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` (corrected staleness window `staleAt=4, catchUpAt=8`; reference run converges)
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/interval_function-capability-removed.json` (interval-based staleness variant)
- **Recorded baseline (from `ANALYSIS.md` Finding 5, hypothesis-1 run):**
  - 3 distinct outcome categories across 9 terminal states (1,074 total paths):
    - **Category A (correct, error-only):** XWidget hash `579a4db`, ConfigMap missing — CompositionRevisionReconciler runs before CompositeReconciler. 432 paths.
    - **Category B (bug, 2 compositions):** XWidget hash `5333e65`, ConfigMap hash `60f2920` — CompositeReconciler runs twice before others. 355 paths.
    - **Category C (bug, 1 composition):** XWidget hash `93d750b`, ConfigMap hash `709d71b` — CompositeReconciler runs once before others. 287 paths.
  - Reference (no staleness) campaign metrics: 35 unique / 42 total / 13 resource states / 3 converged states / 0 aborted.
  - Staleness campaign metrics: 113 unique / 935 total / 17 resource states / 0 converged / 9 max-depth aborted.
- **Acceptance criterion:** F5 still produces ≥ 2 outcome categories after capability removal (Category A correct, Categories B/C buggy composition with same hash families).

### #7223 — F6 (Fatal function leaves orphans + stale Ready=True)

- **Hypothesis:** Orphan persistence is a logic property of the Crossplane `composition_functions.go` early-return path; the maintainer has already rejected the GC fix as unsafe (later pipeline steps could add resources still in use). The stale `Ready=True` in the error path is a separate, valid finding (`reconciler.go:739` only sets `Synced=False`, never updates Ready).
- **Re-eval priority:** Low for re-running. **Higher for sharpening the issue body.**
- **Action items:**
  - **Split #7223** into two narrower issues, or rewrite #7223's body, to cleanly separate:
    - F5 ordering race (still valid, lower severity)
    - F6-Ready-True (stale Ready=True after error path, valid)
    - Drop the F6 GC-on-fatal proposal entirely.
- **Input scenario JSONs (verified on disk):**
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-function-failure_composition-switches-to-fatal.json` (primary)
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-function-failure_composition-switches-resources.json` (clean control — F7, GC works)
  - `/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/workflow_crossplane-function-failure_function-flap-fatal-recovery.json` (clean control — F8, transient fatal recovers)
- **Recorded baseline (from `ANALYSIS.md` Finding 6):** 7 distinct terminal states across 49 trials. ConfigMap-present (orphan) in 42/49 trials (86%); missing in 7/49 (14%). Hash families: A=`88115b88` (17×), B=`47f80aa2` (17×), C=`841a3d02` (6×), D=`58034af3` missing (3×), E=`dd2a95e9` missing (3×), F=`45dbfa98` (2×), G=`e34d4646` missing (1×). XR consistently shows `Synced=False` + `Ready=True` on ConfigMap-present states.

### Issues NOT to re-run

These were ruled out internally before any upstream filing — keep this section
for completeness so they don't get re-litigated:

| Scenario JSON | Reason |
|---------------|--------|
| `workflow_crossplane-staleness_composition-update-races-xr-fetch.json` (and variants `-hypothesis-1`, `-hypothesis-2`) | "Finding 4" CompositionRevision creation-vs-validation race — confirmed transient/self-resolving. All orderings converge to identical state. Not a bug. |
| `workflow_crossplane-concurrency_two-xrs-shared-composition-update.json` | Hypothesis untestable under per-reconciler-ID staleness model. |
| `workflow_crossplane-concurrent-deletion_xr-and-composition-deleted-simultaneously.json` | XR delete path doesn't fetch Composition; no divergence possible via this mechanism. |
| `workflow_crossplane-concurrent-deletion_two-xrs-deleted-simultaneously.json` | Stub returns static ConfigMap name; scenario degenerate without per-XR resource names. |
| `workflow_crossplane-concurrent-deletion_manual-xr-switch-with-old-composition-deleted.json` | Combines F1+F3, no new outcome categories. |
| `workflow_crossplane-concurrent-staleness_composition-update-with-capability-removal.json` | Combines F4+F5, no new outcome categories. |
| `workflow_crossplane-claim_*.json` (5 files) | C1, C3, C5 were clean. C2/C4 (the ones that diverged) were the false positives in #7224 — re-running would just reconfirm the simulation bugs *unless* SSA field-manager fidelity (Fix 3) is implemented first. |

---

## 4. Execution checklist

- [ ] **Check out `crossplane-reeval` branch** (HEAD `89acd8a`) — this is the integrated fidelity-hardened state. `main` does NOT have the RV PR or the DELETE-semantics fix.
- [ ] Copy needed scenario JSONs from worktree back into `examples/crossplane/scenarios/`, or run from worktree path directly via `--inputs <abs-path>`. **Inputs verified present on disk 2026-04-28.** No need to regenerate prior dumps; baselines are the hashes inlined in §3 (sourced from `examples/crossplane/.agents/ANALYSIS.md`).
- [ ] Verify the harness builds against `crossplane v2.2.0` on current `main` (HEAD = `04dd00c`).
- [ ] Re-run F3 (`workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`) at depth 100. Diff terminal-state hashes against `/tmp/finding3-hypothesis1/`.
- [ ] Re-run F1 sanity check (`workflow_crossplane-policy_manual-update-policy-composition-switch.json`). Confirm trace step 4 still composes with wrong revision and reports success.
- [ ] Re-run F5 (`workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json`). Confirm 3 outcome categories (A/B/C) still appear; record any ratio shift.
- [ ] Re-run F6 (`workflow_crossplane-function-failure_composition-switches-to-fatal.json`). Confirm orphan persistence and stale `Ready=True`.
- [ ] Update each open Crossplane issue with: re-run date, harness commit SHA, terminal-state diff vs prior campaign, retract or sharpen as needed.
- [ ] **Before** re-engaging on any C4-style ownership-theft scenario: implement Fix 3 (SSA field-manager conflict detection) per `docs/plans/2026-03-25-patch-semantics-fidelity.md`.

---

## 5. Source pointers

- Bug summary across all projects: `examples/BUG-FINDINGS.md` (untracked)
- Detailed Crossplane analysis with traces and code refs: `examples/crossplane/.agents/ANALYSIS.md` (untracked)
- F2 deep-dive (the `kamera-3d7` finding): `docs/findings/2026-02-26-kamera-3d7-crossplane-unconditional-status-update.md`
- Patch semantics fidelity plan: `docs/plans/2026-03-25-patch-semantics-fidelity.md`
- Sister presentation summary: `docs/crossplane-testing-summary.md`
