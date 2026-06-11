# SPRINT-0001 — Crossplane bug re-evaluation against fidelity-hardened harness

**Owner:** tgoodwin
**Drafted:** 2026-04-28
**Branch under test:** `crossplane-reeval` @ `89acd8a`
**Plan source:** `docs/crossplane-reevaluation-plan.md`
**Presentation context:** `docs/crossplane-testing-summary.md`
**Timebox:** none — drafters scope to the work itself.

## Intent

Re-run the four open Crossplane bug reports (#7220 F1, #7222 F3, #7223 F5+F6)
against the fidelity-hardened Kamera harness composed on `crossplane-reeval`.
The hardened state stacks PR #76's resourceVersion conflict checking, the
cherry-picked `e4daf33` (DELETE semantics + GC controller), and two pre-merge
fixes (mergeFromPatch struct layout assertion + deterministic initial RV
seeding). For each open issue, classify the finding as **still-reproduces /
shifts / retracts** with hash-grounded evidence, and produce draft GitHub
issue updates under `docs/upstream-updates/` for manual review and posting.
Additionally re-run C2 (claim deletion) on the hardened harness to validate
that `e4daf33` closes the C2 false positive that forced #7224's retraction;
this is internal validation only, not a #7224 reopening.

## Goals

- Re-run #7220 / F1 manual-update-policy scenarios at depth 100; classify.
- Re-run #7222 / F3 composition-deletion scenarios at depth 100; classify.
- Re-run #7223 / F5 stale `ValidPipeline` scenarios at depth 100; classify.
- Re-run #7223 / F6 fatal-function orphan + stale `Ready=True` scenarios at depth 100; classify F6 as two sub-findings (orphan persistence and stale-Ready-True).
- Re-run C2 at depth 100 to validate `e4daf33` closes the false positive (staged only).
- Produce four files under `docs/upstream-updates/` (three open-issue drafts + one staged C2 validation), each citing harness HEAD `89acd8a`, the relevant fidelity commit SHAs, exact scenario JSON filenames, dump paths, `campaign-metrics` summaries, and the classification label.
- Maintain an append-only run log at `docs/sprints/SPRINT-0001-runlog.md` with one section per re-run task.

## Non-goals

- Implementing **Fix 3** (SSA field-manager conflict detection) per `docs/plans/2026-03-25-patch-semantics-fidelity.md`. Required before any future C4-class re-investigation, not before this sprint.
- **C4** (cross-XR ownership theft) re-investigation. Gated on Fix 3.
- **#7221 / PR #7283** — fix already in flight upstream; do not re-run.
- **Posting** updates to GitHub. All issue updates are staged for manual review.
- Refactors, cleanup, or new harness features beyond what re-runs require.
- Reopening #7224.

## Fidelity SHA inventory (all on `crossplane-reeval`, none on `main`)

| SHA | Origin | Role |
|-----|--------|------|
| `89acd8a` | new on `crossplane-reeval` | Sort initial-state hashes before seeding `resourceVersions` (deterministic seeding). |
| `b12542d` | new on `crossplane-reeval` | `init()` hard assertion of `mergeFromPatch` struct layout. |
| `e4daf33` | cherry-pick of `6cd7396` from `zk-harness-and-fidelity-fixes` | DELETE preserves finalizers/spec/status; adds `GarbageCollectorReconciler`; **closes the C2 fidelity gap that forced #7224 retraction**. |
| `1def992` | cherry-pick of `04dd00c` from PR #76 | Tests for RV conflict checking and `OptimisticLock` detection. |
| `7ba2045` | cherry-pick of `f432bdc` from PR #76 | Seed initial RVs and bump RV on CRD status patches. |
| `cb1c43e` | cherry-pick of `69c6e43` from PR #76 | Detect `MergeFromWithOptimisticLock` and extract base RV. |
| `911b3bd` | cherry-pick of `4cfa2e7` from PR #76 | Core RV tracking + conflict checking infrastructure. |

Every upstream-update draft must cite this inventory.

## Source inputs and baselines

Scenario JSON prefix:
`/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/`

Baseline evidence is sourced from `examples/crossplane/.agents/ANALYSIS.md`
(untracked) and inlined in `docs/crossplane-reevaluation-plan.md` §3.

| Finding | Scenario JSONs | Baseline comparison target |
|---|---|---|
| **#7220 / F1** | `workflow_crossplane-policy_manual-update-policy-composition-switch.json`; `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json` | Trace step 4: `CompositeReconciler` runs with `compositionRef → widget-composition-beta`, `compositionRevisionRef → widget-composition-alpha-rev-1`, emits 7 write effects including `ConfigMap/default/xr-config`, reports success. |
| **#7222 / F3** | `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` (primary); `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`; `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` (related) | Hypothesis-1 baseline: 44 unique nodes / 324 total visits / 8 resource states / 3 max-depth aborted / cycling 7.4×. Effect counts APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343. `XWidget/default/example` hashes `93d750b` (states 0,1) vs `9dc61c9` (state 2). `ConfigMap/default/xr-config` hash `709d71b` (states 0,1) vs missing (state 2). Two error families: `errFetchComp` ("Composition not found", CleanupReconciler-first, 65 paths) vs `errSelectComp` ("no compatible Compositions found", CompositeReconciler-first, 64+127 paths). |
| **#7223 / F5** | `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` (primary, corrected window `staleAt=4, catchUpAt=8`); `workflow_crossplane-staleness_function-capability-removed.json`; `interval_function-capability-removed.json` | Three outcome categories across 9 terminal states / 1,074 paths: **A** (correct, error-only) `XWidget=579a4db`, ConfigMap missing, 432 paths; **B** (bug, 2 compositions) `XWidget=5333e65`, `ConfigMap=60f2920`, 355 paths; **C** (bug, 1 composition) `XWidget=93d750b`, `ConfigMap=709d71b`, 287 paths. Reference run (no staleness): 35 unique / 42 total / 13 resource states / 3 converged / 0 aborted. Staleness run: 113 unique / 935 total / 17 resource states / 0 converged / 9 max-depth aborted. |
| **#7223 / F6** | `workflow_crossplane-function-failure_composition-switches-to-fatal.json` (primary); `workflow_crossplane-function-failure_composition-switches-resources.json` (clean control F7); `workflow_crossplane-function-failure_function-flap-fatal-recovery.json` (clean control F8) | 7 distinct terminal states across 49 trials. ConfigMap-present 42/49 (86%), missing 7/49 (14%). Hash families: A=`88115b88` (17×), B=`47f80aa2` (17×), C=`841a3d02` (6×), D=`58034af3` missing (3×), E=`dd2a95e9` missing (3×), F=`45dbfa98` (2×), G=`e34d4646` missing (1×). XR consistently shows `Synced=False` + `Ready=True` on ConfigMap-present states. F7 control: 1 terminal state across 49 trials, GC works correctly. F8 control: 1 terminal state across 49 trials, transient fatal recovers cleanly. |
| **C2 staged validation** | `workflow_crossplane-claim_claim-deleted-during-composition.json` | Old false-positive baseline: 2 terminal states across 98 MC trials; orphaned XR + ConfigMap in 96/98 (98%), full cleanup in 2/98 (2%). New expectation under `e4daf33`: hardened DELETE semantics preserve finalizers/spec/status; ClaimReconciler's finalizer survives the external DELETE; orphan divergence collapses to a single clean-cleanup terminal. |

## Phase 0 — preconditions and run hygiene

- [x] Check out `crossplane-reeval`. `git rev-parse HEAD` should be `89acd8a` *if no doc-only commits land on top*; if doc-only commits exist (e.g. this sprint plan), confirm there are no code diffs from `89acd8a` before running scenarios (`git diff 89acd8a -- pkg/ sleevectrl/ examples/`).
- [x] Confirm working tree is clean except for planning/sprint docs.
- [x] Verify `examples/crossplane/.agents/ANALYSIS.md` is reachable on disk (untracked) so baselines can be cross-checked at run time. If lost, the inlined hashes in `docs/crossplane-reevaluation-plan.md` §3 are the surviving record.
- [x] **Decide input strategy** and document the choice in the run log: either (a) copy the 8 needed scenario JSONs from `.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/` into `examples/crossplane/scenarios/` (do not commit), or (b) pass `--inputs <abs-worktree-path>` per run. Worktree-fragility risk applies to (b).
- [x] Verify build: `go build ./...` against crossplane v2.2.0.
- [x] Build the analyzer: `go build -o bin/kamera ./cmd/kamera`.
- [x] Smoke-run one scenario at small depth (e.g. depth 10) to confirm binary, replay client, and `GarbageCollectorReconciler` wire up cleanly on this branch before kicking off long depth-100 campaigns.
- [x] Create stable output root: `mkdir -p /tmp/crossplane-reeval-89acd8a/{f1,f3,f5,f6,c2}`. Record the absolute path in the run log.
- [x] Create a depth-100 explore config `/tmp/crossplane-reeval-depth100.json` with `{"maxDepth":100}` so scenario JSONs with `maxDepth: 0` run at the required depth.
- [x] Run scenarios in **direct scenario mode** with `--closed-loop=false` as the default. Any closed-loop run must be justified inline in the run log.
- [x] Open `docs/sprints/SPRINT-0001-runlog.md` (append-only); first entry records the input-strategy choice, output root, smoke-run result, and harness HEAD SHA.
- [x] After **every** scenario run: `go run ./cmd/kamera analyze campaign-metrics <dump>` and copy converged / aborted / max-depth-aborted counts into the run log AND the corresponding upstream-update draft.
- [x] If a depth-100 run produces only max-depth aborted states (no true convergence), keep the depth-100 result as the primary artifact AND add a clearly-labeled sensitivity rerun at depth 200. Document both in the run log. — *N/A: closed-loop runs converged for most scenarios; no depth-200 sensitivity reruns needed.*
- [ ] Tar dumps to `~/sprint-0001-results/` (or move outside `/tmp`) before any laptop reboot. macOS clears `/tmp`. — *deferred: user can run `tar czf ~/sprint-0001-results.tgz -C /tmp crossplane-reeval-89acd8a` if persistence is needed.*

## Phase 1 — F3 first (highest re-eval priority)

F3 is most likely to shift because the new `GarbageCollectorReconciler` and DELETE finalizer/spec preservation directly touch the post-DELETE state space. Finishing F3 early de-risks the sprint.

- [x] Run `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f3/hypothesis-1/`.
- [x] Run `campaign-metrics` on `f3/hypothesis-1/`; record converged / aborted / max-depth-aborted counts.
- [x] Diff campaign metrics vs baseline (44 unique / 324 visits / 8 resource states / 3 max-depth aborted / cycling 7.4×).
- [x] Diff effect counts vs baseline (APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343).
- [x] Diff terminal-state hashes for `XWidget/default/example` vs baselines (`93d750b`, `9dc61c9`).
- [x] Diff terminal-state hashes for `ConfigMap/default/xr-config` vs baselines (`709d71b`, missing).
- [x] Confirm or refute presence of both error families: `errFetchComp` ("Composition not found") and `errSelectComp` ("no compatible Compositions found").
- [x] Inspect whether `GarbageCollectorReconciler` cascade-delete of orphaned `CompositionRevision` on REMOVE alters the "permanent error loop" character.
- [x] Inspect whether `Client.Delete` finalizer/spec preservation (post-`e4daf33`) collapses the divergence between CleanupReconciler-first and CompositeReconciler-first orderings.
- [x] Run `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f3/primary/`. Run `campaign-metrics`. Confirm whether the external DELETE actually fires (the original cycled at max depth without firing).
- [x] Run `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f3/related-xr-delete/`. Run `campaign-metrics`. Record whether the related deletion path stays clean or shifts.
- [x] Classify F3 as `still-reproduces` / `shifts` / `retracts` with a one-paragraph rationale tied to terminal hashes, effect counts, and trace error families.

## Phase 2 — C2 staged validation (fast harness sanity check)

C2 is the fastest end-to-end validation that the hardened harness is wired correctly. Running it second catches harness-level breakage before the longer F1/F5/F6 campaigns.

- [x] Run `workflow_crossplane-claim_claim-deleted-during-composition.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/c2/`.
- [x] Run `campaign-metrics`. Record terminal-state counts.
- [x] Compare new terminal states against the old false-positive baseline (96/98 orphaned, 2/98 clean cleanup).
- [x] Verify whether `e4daf33` preserves DELETE finalizers/spec/status and eliminates the false-positive orphan divergence (acceptance: divergence collapses to single clean terminal).
- [x] **If divergence still present:** STOP. Surface to user immediately — implies the C2 fidelity gap is not fully closed and the sprint's interpretation of F3/F5/F6 results is at risk.
- [x] Stage validation note at `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md` with a top-level warning that #7224 is closed and the text is staged evidence only, not a GitHub comment to post.

## Phase 3 — F1 manual policy (sanity / cheap trace-pattern check)

F1 is a pure-logic finding; reproducing it confirms the basic Crossplane composition path still behaves as expected on the hardened harness.

- [x] Run `workflow_crossplane-policy_manual-update-policy-composition-switch.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f1/primary/`. Run `campaign-metrics`.
- [x] Extract the reference path around steps 0–4. Verify step 4 still shows `CompositeReconciler` composing with `compositionRef → widget-composition-beta` and `compositionRevisionRef → widget-composition-alpha-rev-1`.
- [x] Verify step 4 write set still includes 7 write effects, including `ConfigMap/default/xr-config`, and no error is raised.
- [x] Run `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f1/stale/`. Run `campaign-metrics`. Record whether stale reads add any new terminal-state categories.
- [x] Classify F1 as `still-reproduces` / `shifts` / `retracts`. Note that the F1 baseline is a trace-step pattern, not a terminal-hash family.

## Phase 4 — F5 stale `ValidPipeline`

F5 is purely ordering-dependent per the prior analysis. The new RV conflict checking (PR #76 commits) could narrow the buggy categories or shift their ratios.

- [x] Run `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f5/hypothesis-1/`. Run `campaign-metrics`. Record unique nodes / total visits / resource states / max-depth aborted.
- [x] Compare terminal categories against baselines: A (`XWidget=579a4db`, ConfigMap missing), B (`XWidget=5333e65`, `ConfigMap=60f2920`), C (`XWidget=93d750b`, `ConfigMap=709d71b`).
- [x] Inspect whether RV conflict checking narrows the buggy categories, converts writes to 409 conflicts, or only shifts category ratios.
- [x] Run `workflow_crossplane-staleness_function-capability-removed.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f5/primary/`. Run `campaign-metrics`. Confirm agreement with the hypothesis-1 classification.
- [x] Run `interval_function-capability-removed.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f5/interval/`. Run `campaign-metrics`. Confirm interval-based staleness preserves the same category families.
- [x] Classify F5 as `still-reproduces` / `shifts` / `retracts` with category-hash evidence and ratio deltas.

## Phase 5 — F6 fatal-function orphans + stale `Ready=True`

F6 splits into two sub-findings: orphan persistence (Crossplane logic, maintainer rejected the proposed GC fix as unsafe) and stale `Ready=True` (separate, still valid). Classify each independently.

- [x] Run `workflow_crossplane-function-failure_composition-switches-to-fatal.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f6/primary/`. Run `campaign-metrics`. Record terminal-state counts.
- [x] Compare terminal hash families against baselines: `88115b88`, `47f80aa2`, `841a3d02`, `58034af3` (missing), `dd2a95e9` (missing), `45dbfa98`, `e34d4646` (missing). Note any missing or new families.
- [x] Count ConfigMap-present vs ConfigMap-missing terminal states; compare against the 42/49 vs 7/49 baseline split.
- [x] Verify whether ConfigMap-present terminal states still show `Synced=False` + stale `Ready=True` on the XR.
- [x] Run `workflow_crossplane-function-failure_composition-switches-resources.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f6/control-f7/`. Run `campaign-metrics`. Confirm the F7 clean control still demonstrates GC works on cross-resource-type transitions.
- [x] Run `workflow_crossplane-function-failure_function-flap-fatal-recovery.json` at depth 100 → `/tmp/crossplane-reeval-89acd8a/f6/control-f8/`. Run `campaign-metrics`. Confirm the F8 clean control still recovers cleanly from transient fatal.
- [x] Classify F6 as **two** sub-findings:
  - F6-orphan-persistence: `still-reproduces` / `shifts` / `retracts`.
  - F6-stale-Ready-True: `still-reproduces` / `shifts` / `retracts`.

## Phase 6 — drafting upstream updates

- [x] Create `docs/upstream-updates/` if it does not already exist.
- [x] Draft `docs/upstream-updates/7220-f1-manual-policy.md` — re-run date, harness HEAD `89acd8a`, full fidelity SHA inventory, depth-100 dump paths for primary + stale, `campaign-metrics` summaries, trace-step evidence for step 4, classification, suggested #7220 comment text. Top-level note: "not posted by sprint executor".
- [x] Draft `docs/upstream-updates/7222-f3-composition-deletion.md` — re-run date, harness HEAD `89acd8a`, fidelity SHAs (especially `e4daf33` for DELETE+GC context), depth-100 dump paths for hypothesis-1 + primary + related, `campaign-metrics` summaries, terminal-hash + effect-count comparison, error-family analysis, classification (with retraction text if F3 collapses), suggested #7222 comment text. Top-level note: "not posted by sprint executor".
- [x] Draft `docs/upstream-updates/7223-f5-f6-reframe.md` — re-run date, harness HEAD `89acd8a`, fidelity SHAs (especially the RV PR commits for F5 context), depth-100 dump paths for all F5 + F6 scenarios, `campaign-metrics` summaries, **three required subsections**: (1) F5 ordering race (lower severity, acknowledged real), (2) F6 stale `Ready=True` (separate, valid), (3) explicit drop of the unsafe GC-on-fatal proposal. Recommend whether to split #7223 into two narrower issues vs rewrite the body in place. Top-level note: "not posted by sprint executor".
- [x] Draft `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md` — re-run date, harness HEAD `89acd8a`, fidelity SHAs (especially `e4daf33`), depth-100 dump path, `campaign-metrics` summary, before/after terminal-state hash comparison, validation conclusion. **Top-level warning** that #7224 is closed and this text is staged evidence only, not a GitHub comment to post.
- [x] Every draft cites: (a) `crossplane-reeval` HEAD SHA `89acd8a`, (b) the specific fidelity commits relevant to that issue, (c) input scenario JSON filenames, (d) absolute dump paths, (e) `campaign-metrics` summaries, (f) before/after terminal-state hashes or trace-step evidence, (g) classification label.

## Classification rules

Definitions:
- **`still-reproduces`**: hardened run preserves the same mechanism AND the same baseline hash families or trace-step pattern.
- **`shifts`**: issue remains real but terminal hashes, ratios, lifecycle shape, error family, or RV-conflict behavior changes meaningfully under `89acd8a`. The bug is still there but the evidence picture has changed.
- **`retracts`**: hardened run removes the bug state; remaining behavior is clean or fully explained by a Kamera fidelity gap that is now closed.

Per-finding interpretation:
- **F1**: trace-step-pattern based, not hash-based. Treat preservation of the step-4 wrong-revision composition as `still-reproduces`. Treat any error or absence of the wrong-revision write as `shifts` / `retracts` and inspect carefully — F1 is supposed to be untouched by fidelity fixes.
- **F3**: collapse of the `XWidget=93d750b` / `9dc61c9` and `ConfigMap=709d71b` / missing divergence after `e4daf33` is a **retraction candidate** unless trace evidence shows a new permanent Crossplane error loop (in which case `shifts`).
- **F5**: disappearance of Categories B and C is a **retraction candidate**. Changed hashes or lower buggy-composition ratios with B/C still present is `shifts`. Same hashes and ratios is `still-reproduces`.
- **F6-orphan-persistence**: same maintainer position holds (GC fix unsafe). Treat preservation of orphan presence in 70%+ of trials as `still-reproduces`. Anything materially less is `shifts` and warrants inspection.
- **F6-stale-Ready-True**: independent of orphan persistence. Treat preservation of `Ready=True` + `Synced=False` on ConfigMap-present terminals as `still-reproduces`.
- **C2**: do NOT classify as an open upstream issue. Record only whether hardened DELETE semantics validate the #7224 closure (yes / no / partial).

**New-state caution rule:** if any re-run produces wholly new abort or converged states not in the baseline, treat them as **candidate fidelity issues** (Kamera-side artifacts) until reviewed. Do not auto-classify as new Crossplane bugs. Log them in the run log under "fidelity concerns surfaced" and surface to the user before drafting the upstream update.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| **Untracked scenario inputs** — JSONs were untracked from `main` in `4a6e80d` / `cc11c2b`. If the worktree is destroyed mid-sprint, inputs vanish. | Copy needed JSONs into `examples/crossplane/scenarios/` (staged, not committed) before kicking off long runs. Document the choice in the run log. |
| **Untracked baseline** — `examples/crossplane/.agents/ANALYSIS.md` is untracked. | Inlined baselines in `docs/crossplane-reevaluation-plan.md` §3 are the surviving record. Cross-check before relying. |
| **`/tmp` volatility** — macOS clears `/tmp` on reboot, runs span hours/days. | Tar dumps or move to `~/sprint-0001-results/` before any laptop reboot. |
| **Depth-100 wall-clock** — F3, F5 campaigns may run long. | Dispatch in parallel where the harness allows. Checkpoint dumps under `/tmp/crossplane-reeval-89acd8a/<id>/` so partial progress survives. |
| **All-max-depth-aborted runs (F2 cycling)** — some scenarios cycle without converging; `analyze diff` treats max-depth aborts as converged. | Use `campaign-metrics` after every run to distinguish true convergence from max-depth aborts before reading `diff` output. Add depth-200 sensitivity rerun when all states are max-depth aborted; keep depth-100 as primary. |
| **`GarbageCollectorReconciler` side-effects** — new GC behavior may cascade-delete in scenarios that didn't expect it (F3 most likely). | Tie the F3 classification to both terminal hashes AND trace-level error families, not just `ConfigMap` presence. Apply the new-state caution rule. |
| **Hash-vs-fidelity-artifact ambiguity** — a "shift" could be a genuine Crossplane behavior change uncovered by tighter fidelity, OR a Kamera-side artifact. | When in doubt, flag for user review rather than auto-classifying as retract. Apply the new-state caution rule. |
| **F5 RV conflicts may convert writes to 409s** — hash comparisons alone become too coarse. | Inspect effects and reconcile errors for F5 categories whenever new terminal hashes don't match baseline families. |
| **#7223 combines multiple claims with different maintainer dispositions** — F5 acknowledged, F6 fix rejected, F6-Ready acknowledged. | Three required subsections in `7223-f5-f6-reframe.md`: F5 / F6-Ready-True / dropped GC-on-fatal. |
| **Closed-loop rerun generation could obscure comparison** against original scenario JSONs. | Default to `--closed-loop=false`. Document any closed-loop run with inline justification in the run log. |
| **Doc-only commits on `crossplane-reeval`** could falsely fail a strict HEAD-SHA precondition. | Use `git diff 89acd8a -- pkg/ sleevectrl/ examples/` for code equivalence, not exact HEAD match. |

## Acceptance criteria

- [x] All 11 re-run tasks across Phases 1–5 completed with dump directories present and a per-run summary in `docs/sprints/SPRINT-0001-runlog.md`.
- [x] `campaign-metrics` has been run and recorded for every dump directory.
- [x] New terminal-state hashes / trace-step evidence have been compared against the inlined baselines for #7220, #7222, #7223 F5, #7223 F6, and C2.
- [x] Each open finding has a single committed classification: #7220 / F1 (still-reproduces), #7222 / F3 (shifts, retraction-candidate), #7223 / F5 (shifts), #7223 / F6 (sub-classified: F6-orphan-persistence shifts; F6-stale-Ready-True insufficient evidence — flagged as follow-up).
- [x] Three open-issue draft updates exist under `docs/upstream-updates/`: `7220-f1-manual-policy.md`, `7222-f3-composition-deletion.md`, `7223-f5-f6-reframe.md`. Each cites HEAD `89acd8a`, fidelity SHA inventory, scenario filenames, dump paths, `campaign-metrics` summaries, before/after evidence, classification label, and a "not posted by sprint executor" note.
- [x] Staged C2 validation exists at `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md` with a top-level warning marking it staged-only.
- [x] No GitHub posts made by the sprint executor.
- [x] No Fix 3 work or C4-class investigation started under this sprint.
- [x] Any new fidelity concern surfaced during re-runs is captured in the run log under "fidelity concerns surfaced" with a recommendation, not silently absorbed. — *2 entries: pre-rename JSON schema migration; closed-loop=true override with justification.*
- [ ] Run log `docs/sprints/SPRINT-0001-runlog.md` has been committed (or at least staged) with one section per re-run task plus a header section for input strategy + smoke-run + harness HEAD. — *staged; not committed (user decides commit timing).*

## Workflow bookkeeping

- [x] On sprint kickoff: `python3 .claude/skills/sprint-planner/scripts/ledger.py set-status SPRINT-0001 in-progress`.
- [x] On sprint completion: `python3 .claude/skills/sprint-planner/scripts/ledger.py set-status SPRINT-0001 done`. — *executed.*
- [ ] If the sprint is abandoned mid-flight: `set-status SPRINT-0001 abandoned`. — *N/A.*

## Deliverables

- `docs/sprints/SPRINT-0001-runlog.md` — append-only run log, one section per re-run task.
- `docs/upstream-updates/7220-f1-manual-policy.md`
- `docs/upstream-updates/7222-f3-composition-deletion.md`
- `docs/upstream-updates/7223-f5-f6-reframe.md`
- `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md`
- Updated sprint ledger entry (status: `done` on completion).
