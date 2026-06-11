# SPRINT-0001 — Crossplane Re-Evaluation on Fidelity-Hardened Harness

**Owner:** tgoodwin
**Drafted:** 2026-04-28
**Branch under test:** `crossplane-reeval` @ `89acd8a`
**Plan source:** `docs/crossplane-reevaluation-plan.md`
**No fixed timebox.**

## Goal

Re-run the four open Crossplane bug reports against the fidelity-hardened
Kamera harness (RV conflict checking + DELETE/GC fix + pre-merge fixes), then
classify each finding as **still-reproduces / shifts / retracts** and produce
draft GitHub issue updates ready for the user to post manually. Additionally
re-run C2 to confirm `6cd7396` closes the false positive that forced #7224's
retraction (staged update only — #7224 is already closed).

## In scope

- Issues #7220 (F1), #7222 (F3), #7223 (F5+F6).
- C2 claim-deletion sanity re-run on hardened harness (validation only).
- Per-issue baseline diff against hashes inlined in
  `docs/crossplane-reevaluation-plan.md` §3 (sourced from
  `examples/crossplane/.agents/ANALYSIS.md`). We are NOT regenerating prior
  `/tmp/...` dumps.
- All re-runs at depth 100 on `crossplane-reeval` HEAD `89acd8a`.

## Out of scope

- **Fix 3** (SSA field-manager conflict detection) — design plan only;
  blocks any C4 ownership-theft re-investigation but not this sprint.
- **C4 re-investigation** — gated on Fix 3.
- **#7221 / PR #7283** — fix already in flight upstream; do not re-run.
- **Posting** updates to GitHub. All issue updates are staged under
  `docs/upstream-updates/` for manual review/post by the user.
- Refactors, cleanup, or new harness features beyond what re-runs require.

## Preconditions

- [ ] Confirm checkout: `git rev-parse HEAD` = `89acd8a` on `crossplane-reeval`.
- [ ] Confirm working tree clean (or only contains the planning/summary docs and the new sprint file).
- [ ] Verify `examples/crossplane/.agents/ANALYSIS.md` is reachable (untracked) so baseline hashes can be cross-checked at run time.
- [ ] Decide input strategy: copy 8 needed scenario JSONs from `.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/` back into `examples/crossplane/scenarios/`, OR pass `--inputs <abs-path>` per run. Document the choice in the run log.
- [ ] Verify harness builds: `go build ./...` against `crossplane v2.2.0`.
- [ ] Smoke-run a tiny depth (e.g. depth 10) on one scenario to confirm the binary, replay client, and GC controller wire up cleanly on this branch.
- [ ] Create output root: `mkdir -p /tmp/sprint-0001/{f1,f3,f5,f6,c2}` (or equivalent) and record the absolute path in the run log.

## Re-run tasks (depth 100, branch `crossplane-reeval`)

### F1 — #7220 manual update policy (sanity, low priority)

- [ ] Re-run `workflow_crossplane-policy_manual-update-policy-composition-switch.json` at depth 100; dump to `/tmp/sprint-0001/f1/primary/`.
- [ ] Re-run `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json` at depth 100; dump to `/tmp/sprint-0001/f1/stale/`.
- [ ] Inspect trace step 4: confirm `CompositeReconciler` runs with `compositionRef → widget-composition-beta` but `compositionRevisionRef → widget-composition-alpha-rev-1`, emits 7 write effects including a ConfigMap, reports success.
- [ ] Record terminal-state hashes in run log; classify F1 as still-reproduces / shifts / retracts.

### F3 — #7222 composition deletion (HIGH priority — most likely to shift)

- [ ] Re-run `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` at depth 100 (primary input — has `userActionReadyDepths: {"0": 0}` which actually fires the DELETE); dump to `/tmp/sprint-0001/f3/hypothesis-1/`.
- [ ] Re-run `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json` at depth 100; dump to `/tmp/sprint-0001/f3/primary/`.
- [ ] Re-run `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` at depth 100 (related control); dump to `/tmp/sprint-0001/f3/control/`.
- [ ] Diff campaign metrics vs baseline (44 unique nodes / 324 visits / 8 resource states / 3 aborted / cycling 7.4×). Record deltas.
- [ ] Diff effect counts vs baseline (APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343).
- [ ] Diff aborted-state hashes for `XWidget/default/example` vs baselines (`93d750b` states 0/1; `9dc61c9` state 2) and `ConfigMap/default/xr-config` (`709d71b` states 0/1; missing state 2).
- [ ] Confirm or refute presence of both error paths: `errFetchComp` ("Composition not found") and `errSelectComp` ("no compatible Compositions found").
- [ ] Specifically inspect whether `GarbageCollectorReconciler` cascade-delete of orphaned `CompositionRevision` on REMOVE alters the "permanent error loop" character.
- [ ] Specifically inspect whether `Client.Delete` finalizer/spec preservation (post-`6cd7396`) collapses the divergence.
- [ ] Classify F3 as still-reproduces / shifts / retracts and capture the reasoning.

### F5 — #7223 stale `ValidPipeline` (medium priority)

- [ ] Re-run `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` at depth 100 (primary; corrected staleness window `staleAt=4, catchUpAt=8`); dump to `/tmp/sprint-0001/f5/hypothesis-1/`.
- [ ] Re-run `workflow_crossplane-staleness_function-capability-removed.json` at depth 100; dump to `/tmp/sprint-0001/f5/primary/`.
- [ ] Re-run `interval_function-capability-removed.json` at depth 100; dump to `/tmp/sprint-0001/f5/interval/`.
- [ ] Confirm ≥2 outcome categories appear (acceptance criterion). Recorded baseline categories:
  - Cat A (correct, error-only): XWidget `579a4db`, ConfigMap missing — 432 paths
  - Cat B (bug, 2 compositions): XWidget `5333e65`, ConfigMap `60f2920` — 355 paths
  - Cat C (bug, 1 composition): XWidget `93d750b`, ConfigMap `709d71b` — 287 paths
- [ ] Diff campaign metrics vs baseline (113 unique / 935 visits / 17 resource states / 0 converged / 9 max-depth aborted).
- [ ] Record any path-ratio shift between A/B/C; assess whether RV conflict checking narrows successful writes.
- [ ] Classify F5 as still-reproduces / shifts / retracts.

### F6 — #7223 fatal-function orphans + stale `Ready=True` (low priority for re-run)

- [ ] Re-run `workflow_crossplane-function-failure_composition-switches-to-fatal.json` at depth 100; dump to `/tmp/sprint-0001/f6/primary/`.
- [ ] Re-run `workflow_crossplane-function-failure_composition-switches-resources.json` (clean control F7); dump to `/tmp/sprint-0001/f6/control-f7/`.
- [ ] Re-run `workflow_crossplane-function-failure_function-flap-fatal-recovery.json` (clean control F8); dump to `/tmp/sprint-0001/f6/control-f8/`.
- [ ] Confirm orphan persistence ratio against baseline (ConfigMap-present 42/49 ≈ 86%, missing 7/49 ≈ 14%).
- [ ] Confirm hash families A=`88115b88`, B=`47f80aa2`, C=`841a3d02`, D=`58034af3` (missing), E=`dd2a95e9` (missing), F=`45dbfa98`, G=`e34d4646` (missing) still appear (note any missing/new ones).
- [ ] Confirm XR consistently shows `Synced=False` + `Ready=True` on ConfigMap-present terminal states (the F6-Ready-True finding).
- [ ] Classify F6 separately as: orphan-persistence (still-reproduces / shifts / retracts) and stale-Ready-True (still-reproduces / shifts / retracts).

### C2 — #7224 claim deletion (validation only — staged, not posted)

- [ ] Re-run `workflow_crossplane-claim_claim-deleted-during-composition.json` at depth 100; dump to `/tmp/sprint-0001/c2/`.
- [ ] Confirm `e4daf33` (cherry-pick of `6cd7396`) collapses the prior false-positive divergence (XR no longer orphaned because `Client.Delete` now preserves finalizers).
- [ ] If divergence still present: STOP and surface to user — implies the C2 fidelity gap is not fully closed.
- [ ] Stage validation note at `docs/upstream-updates/7224-c2-staged-validation.md` (do NOT post; #7224 is closed).

## Drafting tasks (one file per open issue)

- [ ] Create `docs/upstream-updates/` directory.
- [ ] Draft `docs/upstream-updates/7220-update.md` — re-run date, harness commit `89acd8a`, terminal-state diff vs baseline, classification, retract-or-sharpen recommendation.
- [ ] Draft `docs/upstream-updates/7222-update.md` — same structure; if F3 retracts, write a clean retraction with the fidelity commits cited (`e4daf33`, `911b3bd`, etc.). If it shifts, include the new error-path breakdown.
- [ ] Draft `docs/upstream-updates/7223-update.md` — separately address F5 (ordering race, lower severity) and F6-Ready-True. Explicitly drop the F6 GC-on-fatal proposal (maintainer rejected as unsafe). Recommend whether to split #7223 into two narrower issues vs rewrite the body in place.
- [ ] Draft `docs/upstream-updates/7224-c2-staged-validation.md` — internal-only validation that `6cd7396` closes the C2 false positive; mark "not for posting".
- [ ] Each draft must cite: (a) `crossplane-reeval` HEAD SHA, (b) the specific fidelity commits relevant to that issue, (c) input scenario JSON filename(s), (d) before/after terminal-state hashes.

## Sequencing

1. Preconditions block (build, smoke run, baseline access).
2. F3 re-run first — highest probability of changed outcome; finishing this early de-risks the sprint.
3. C2 re-run second — fast confirmation that `6cd7396` is doing its job (also validates the hardened harness end-to-end before the remaining issues).
4. F5, then F6, then F1 (sanity) in any order — these are lower-risk re-runs.
5. Drafting tasks last, one issue at a time, after all hashes for that issue are in hand.

## Risks

- **Untracked scenario inputs.** All scenario JSONs were untracked from `main` in `4a6e80d` / `cc11c2b`. If the worktree is destroyed mid-sprint, the inputs vanish with it. Mitigation: copy into `examples/crossplane/scenarios/` and stage (do not commit) before kicking off long runs.
- **Untracked baseline.** `examples/crossplane/.agents/ANALYSIS.md` is untracked. If lost, the inlined hashes in `docs/crossplane-reevaluation-plan.md` §3 are the only surviving record. Mitigation: re-read and confirm against the plan doc before relying.
- **Depth-100 wall-clock.** F3 and F5 campaigns at depth 100 may run long. Mitigation: dispatch in parallel where the harness allows; checkpoint dumps to `/tmp/sprint-0001/<id>/` so partial progress survives.
- **GC controller side-effects.** The new `GarbageCollectorReconciler` may cascade-delete in scenarios that didn't expect it (notably F3). Treat any wholly new abort/converged states as candidate fidelity issues, not Crossplane bugs, until reviewed.
- **`/tmp` volatility.** macOS clears `/tmp` on reboot. Mitigation: tar dumps or move to `~/sprint-0001-results/` before sleeping the laptop overnight.
- **Hash-comparison ambiguity.** A "shift" could be a genuine Crossplane behavior change uncovered by tighter fidelity, OR a harness-side artifact. When in doubt, flag for user review rather than auto-classifying as retract.

## Acceptance criteria

- [ ] All 11 re-run tasks above completed with dump directories present and a per-run summary line in the sprint log.
- [ ] Each of #7220 / #7222 / #7223 has a single committed classification (still-reproduces / shifts / retracts), with F6 sub-classified separately (orphan vs Ready=True).
- [ ] Three open-issue draft updates (`7220-update.md`, `7222-update.md`, `7223-update.md`) plus the staged C2 validation (`7224-c2-staged-validation.md`) exist under `docs/upstream-updates/`, each citing harness HEAD `89acd8a` and the relevant fidelity commit SHAs.
- [ ] No GitHub posts made by the agent. The user reviews and posts manually.
- [ ] Any new fidelity concern surfaced during re-runs (e.g. unexpected GC cascade, unexplained hash family) is captured in the sprint log with a recommendation, not silently absorbed.

## Deliverables

- Run log (append-only) at `docs/sprints/drafts/SPRINT-0001-runlog.md` with one section per re-run task.
- Four draft files under `docs/upstream-updates/`.
- Updated sprint ledger entry.
