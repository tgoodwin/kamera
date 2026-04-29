# SPRINT-0001: Crossplane Bug Re-Evaluation on Fidelity-Hardened Kamera

Intent: re-evaluate the open Crossplane findings against the `crossplane-reeval`
harness code state at `89acd8a`, using the scenario inputs already verified under
`/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/`.
The sprint has no fixed timebox. The output is not upstream posting; it is a
set of evidence-backed draft issue updates for manual review and posting.

## Goals

- [ ] Re-run #7220 / F1 manual update policy scenarios at depth 100 on `crossplane-reeval` harness code commit `89acd8a` and classify the finding as `still-reproduces`, `shifts`, or `retracts`.
- [ ] Re-run #7222 / F3 Composition deletion scenarios at depth 100 on `crossplane-reeval` harness code commit `89acd8a` and classify the finding as `still-reproduces`, `shifts`, or `retracts`.
- [ ] Re-run #7223 / F5 stale `ValidPipeline` scenarios at depth 100 on `crossplane-reeval` harness code commit `89acd8a` and classify the finding as `still-reproduces`, `shifts`, or `retracts`.
- [ ] Re-run #7223 / F6 fatal-function orphan and stale `Ready=True` scenarios at depth 100 on `crossplane-reeval` harness code commit `89acd8a` and classify the finding as `still-reproduces`, `shifts`, or `retracts`.
- [ ] Re-run C2 claim deletion on the hardened harness to validate that `6cd7396` / cherry-pick `e4daf33` closes the false positive, staged only and not posted to closed issue #7224.
- [ ] Produce review-ready draft GitHub issue updates under `docs/upstream-updates/` for #7220, #7222, and #7223, plus a staged non-posting C2 validation note.

## Scope Boundaries

In scope: reruns, campaign metrics, terminal-state hash comparison, issue
classification, and draft upstream update text. The harness code state is fixed
to `89acd8a`, which includes PR #76 resourceVersion
conflict checking commits, `e4daf33` as the cherry-pick of `6cd7396` DELETE
semantics plus GC controller, `b12542d` mergeFromPatch layout assertion, and
`89acd8a` deterministic initial resourceVersion seeding.

Out of scope: implementing Fix 3 SSA field-manager conflict detection, any
C4-class shared-composition ownership re-investigation, fixing Crossplane,
posting comments to GitHub, or reopening #7224.

## Source Inputs and Baselines

Use this scenario prefix for all runs:

`/Users/tgoodwin/projects/kamera/.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios`

Baseline evidence comes from `docs/crossplane-reevaluation-plan.md`, sourced
from `examples/crossplane/.agents/ANALYSIS.md`.

| Finding | Scenario JSONs | Baseline comparison target |
| --- | --- | --- |
| #7220 / F1 | `workflow_crossplane-policy_manual-update-policy-composition-switch.json`; `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json` | Trace step 4: `CompositeReconciler` has `compositionRef=widget-composition-beta`, `compositionRevisionRef=widget-composition-alpha-rev-1`, emits 7 write effects including `ConfigMap/default/xr-config`, and reports success. |
| #7222 / F3 | `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`; `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json`; `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` | Hypothesis-1 baseline: 44 unique nodes, 324 total visits, 8 resource states, 3 max-depth aborted states. `XWidget/default/example` hashes `93d750b` vs `9dc61c9`; `ConfigMap/default/xr-config` hash `709d71b` vs missing. Error families: `errFetchComp` Composition not found and `errSelectComp` no compatible Compositions. |
| #7223 / F5 | `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json`; `workflow_crossplane-staleness_function-capability-removed.json`; `interval_function-capability-removed.json` | Three outcome categories: A correct `XWidget=579a4db`, ConfigMap missing, 432 paths; B buggy `XWidget=5333e65`, `ConfigMap=60f2920`, 355 paths; C buggy `XWidget=93d750b`, `ConfigMap=709d71b`, 287 paths. |
| #7223 / F6 | `workflow_crossplane-function-failure_composition-switches-to-fatal.json`; `workflow_crossplane-function-failure_composition-switches-resources.json`; `workflow_crossplane-function-failure_function-flap-fatal-recovery.json` | Baseline: 7 terminal states across 49 trials. ConfigMap present in 42/49 trials and missing in 7/49. Hash families: `88115b88`, `47f80aa2`, `841a3d02`, `58034af3` missing, `dd2a95e9` missing, `45dbfa98`, `e34d4646` missing. ConfigMap-present states show `Synced=False` and stale `Ready=True`. |
| C2 staged validation | `workflow_crossplane-claim_claim-deleted-during-composition.json` | Old false-positive baseline: 2 terminal states across 98 Monte Carlo trials; orphaned XR and ConfigMap present in 96/98, full cleanup in 2/98. New expectation: hardened DELETE semantics preserve finalizers/spec/status and GC behavior removes the false-positive orphan divergence. |

## Sequencing and Tasks

### Phase 0: Run Hygiene

- [ ] Confirm the working tree is on `crossplane-reeval` and the harness code under test is commit `89acd8a`; if the branch tip contains doc-only sprint commits, confirm there are no code diffs from `89acd8a` before running scenarios.
- [ ] Record the fidelity SHAs in the sprint notes: `89acd8a`, `b12542d`, `e4daf33` / upstream `6cd7396`, `1def992`, `7ba2045`, `cb1c43e`, and `911b3bd`.
- [ ] Build the analyzer with `go build -o bin/kamera ./cmd/kamera`.
- [ ] Build or smoke-test the Crossplane harness from `examples/crossplane` before the campaign runs.
- [ ] Create `docs/upstream-updates/` if it does not already exist.
- [ ] Create a depth-100 explore config such as `/tmp/crossplane-reeval-depth100.json` with `{"maxDepth":100}` so scenario JSONs with `maxDepth: 0` run at the required depth.
- [ ] Use a stable output root such as `/tmp/crossplane-reeval-89acd8a/` and record every dump directory in the issue-specific notes.
- [ ] Run scenarios in direct scenario mode with the scenario JSON as written, using `--closed-loop=false`, unless a run note explicitly justifies using the closed-loop reference/rerun pipeline.
- [ ] After every scenario run, run `go run ./cmd/kamera analyze campaign-metrics <dump-or-dump-dir>` and copy the metrics into the corresponding issue notes.
- [ ] If a required depth-100 run has zero truly converged states and all states are max-depth aborted, keep the depth-100 result for comparison and add a clearly labeled sensitivity rerun at depth 200 per the scenario-run tuning rule.

### Phase 1: #7222 / F3 First, Highest Re-Eval Priority

- [ ] Run `workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7222-f3-hypothesis-1/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7222-f3-hypothesis-1/` and record converged, aborted, and max-depth aborted counts.
- [ ] Compare new terminal-state object hashes for `XWidget/default/example` and `ConfigMap/default/xr-config` against baseline hashes `93d750b`, `9dc61c9`, `709d71b`, and missing.
- [ ] Check whether the new trace still contains both F3 error families: `errFetchComp` / Composition not found and `errSelectComp` / no compatible Compositions.
- [ ] Check whether the new `GarbageCollectorReconciler` introduced by `e4daf33` changes the CompositionRevision lifecycle enough to collapse, shift, or preserve the F3 divergence.
- [ ] Run `workflow_crossplane-deletion_composition-deleted-while-xr-bound.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7222-f3-original/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7222-f3-original/` and confirm whether the external DELETE fires or remains blocked by cycling.
- [ ] Run `workflow_crossplane-deletion_xr-deleted-with-active-composition.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7222-f3-related-xr-delete/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7222-f3-related-xr-delete/` and record whether the related deletion path stays clean or shifts.
- [ ] Classify #7222 / F3 as `still-reproduces`, `shifts`, or `retracts` with a one-paragraph rationale tied to terminal hashes and trace error families.

### Phase 2: #7220 / F1 Manual Policy Sanity Check

- [ ] Run `workflow_crossplane-policy_manual-update-policy-composition-switch.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7220-f1-primary/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7220-f1-primary/` and record whether the run cycles at max depth as before.
- [ ] Extract the reference path around steps 0-4 and verify that step 4 still has `CompositeReconciler` composing with `compositionRef=widget-composition-beta` and `compositionRevisionRef=widget-composition-alpha-rev-1`.
- [ ] Verify that the step 4 write set still includes 7 write effects, including `ConfigMap/default/xr-config`, and that no error is raised.
- [ ] Run `workflow_crossplane-policy_manual-update-policy-composition-switch-stale.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7220-f1-stale/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7220-f1-stale/` and record whether stale reads add any new terminal-state categories.
- [ ] Classify #7220 / F1 as `still-reproduces`, `shifts`, or `retracts` with a note that the inlined F1 baseline is a trace-step pattern rather than a terminal-state hash family.

### Phase 3: #7223 / F5 Stale ValidPipeline

- [ ] Run `workflow_crossplane-staleness_function-capability-removed-hypothesis-1.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f5-hypothesis-1/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f5-hypothesis-1/` and record unique node visits, total node visits, resource states, and max-depth aborted states.
- [ ] Compare terminal categories against baseline Category A `XWidget=579a4db` with ConfigMap missing, Category B `XWidget=5333e65` and `ConfigMap=60f2920`, and Category C `XWidget=93d750b` and `ConfigMap=709d71b`.
- [ ] Check whether resourceVersion conflict checking from PR #76 narrows the buggy categories, converts writes to conflicts, or only shifts category ratios.
- [ ] Run `workflow_crossplane-staleness_function-capability-removed.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f5-original/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f5-original/` and record whether it agrees with the hypothesis-1 classification.
- [ ] Run `interval_function-capability-removed.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f5-interval/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f5-interval/` and record whether the interval-based staleness variant preserves the same category families.
- [ ] Classify #7223 / F5 as `still-reproduces`, `shifts`, or `retracts` with category hashes and any ratio changes.

### Phase 4: #7223 / F6 Fatal Function and Stale Ready

- [ ] Run `workflow_crossplane-function-failure_composition-switches-to-fatal.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f6-fatal-primary/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f6-fatal-primary/` and record terminal-state counts.
- [ ] Compare terminal hash families against baseline `88115b88`, `47f80aa2`, `841a3d02`, `58034af3`, `dd2a95e9`, `45dbfa98`, and `e34d4646`.
- [ ] Count ConfigMap-present versus ConfigMap-missing terminal states and compare against the baseline 42/49 present and 7/49 missing split.
- [ ] Verify whether ConfigMap-present terminal states still show `Synced=False` and stale `Ready=True` on the XR.
- [ ] Run `workflow_crossplane-function-failure_composition-switches-resources.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f6-resources-control/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f6-resources-control/` and confirm the F7 clean control still demonstrates GC works when the function switches resources without fatal.
- [ ] Run `workflow_crossplane-function-failure_function-flap-fatal-recovery.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7223-f6-flap-control/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7223-f6-flap-control/` and confirm the F8 clean control still recovers from transient fatal.
- [ ] Classify #7223 / F6 as `still-reproduces`, `shifts`, or `retracts`, separating the rejected GC-on-fatal proposal from the still-valid stale `Ready=True` evidence.

### Phase 5: C2 Staged Validation Only

- [ ] Run `workflow_crossplane-claim_claim-deleted-during-composition.json` at depth 100 into `/tmp/crossplane-reeval-89acd8a/7224-c2-claim-deletion-staged/`.
- [ ] Run campaign metrics for `/tmp/crossplane-reeval-89acd8a/7224-c2-claim-deletion-staged/` and record terminal-state counts.
- [ ] Compare the new C2 terminal states against the old false-positive baseline of 96/98 orphaned XR plus ConfigMap states and 2/98 full cleanup states.
- [ ] Verify whether `e4daf33` / upstream `6cd7396` preserves DELETE finalizers/spec/status and eliminates the false-positive orphan divergence.
- [ ] Write `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md` with a top-level warning that #7224 is closed and the text is staged evidence only, not a GitHub comment to post.

### Phase 6: Draft Upstream Updates

- [ ] Write `docs/upstream-updates/7220-f1-manual-policy.md` with the new run date, harness code commit `89acd8a`, relevant fidelity SHAs, depth-100 output path, campaign metrics, trace-step evidence, classification, and suggested #7220 comment text.
- [ ] Write `docs/upstream-updates/7222-f3-composition-deletion.md` with the new run date, harness code commit `89acd8a`, `e4daf33` / `6cd7396` DELETE+GC context, depth-100 output paths, campaign metrics, terminal hash comparison, classification, and suggested #7222 comment text.
- [ ] Write `docs/upstream-updates/7223-f5-f6-reframe.md` with the new run date, harness code commit `89acd8a`, F5 terminal category comparison, F6 stale `Ready=True` evidence, explicit removal of the unsafe GC-on-fatal proposal, classification for F5 and F6, and suggested #7223 comment text.
- [ ] Include a short "not posted by sprint executor" note in every file under `docs/upstream-updates/`.
- [ ] Include exact scenario JSON filenames and dump paths in every upstream update draft.
- [ ] Include the `campaign-metrics` output summary in every upstream update draft.
- [ ] Include the classification label `still-reproduces`, `shifts`, or `retracts` in every upstream update draft.

## Classification Rules

- [ ] Mark `still-reproduces` when the hardened run preserves the same mechanism and the same baseline hash families or trace-step pattern.
- [ ] Mark `shifts` when the issue remains real but terminal hashes, ratios, lifecycle shape, error family, or resourceVersion conflict behavior changes under `89acd8a`.
- [ ] Mark `retracts` when the hardened run removes the bug state and the remaining behavior is clean or explained by a Kamera fidelity gap now closed.
- [ ] For F3, treat a collapse of `XWidget=93d750b` / `9dc61c9` and `ConfigMap=709d71b` / missing divergence after `e4daf33` as a retraction candidate unless trace evidence shows a new permanent Crossplane error loop.
- [ ] For F5, treat disappearance of Category B and Category C as a retraction candidate; treat changed hashes or lower ratios with buggy composition still present as `shifts`.
- [ ] For F6, classify the orphan persistence and stale `Ready=True` separately before writing the combined #7223 update.
- [ ] For C2, do not classify as an open upstream issue; record whether hardened DELETE semantics validate the #7224 closure.

## Risks and Mitigations

Risk: some depth-100 runs may still be all max-depth aborted because F2 cycling remains in the target Crossplane version.

- [ ] Use `campaign-metrics` after each run to distinguish true convergence from max-depth aborts before reading `analyze diff` output.
- [ ] Add sensitivity reruns at doubled depth when all states are max-depth aborted, while keeping the required depth-100 result as the primary sprint artifact.

Risk: closed-loop rerun generation may obscure comparison against the original scenario JSONs.

- [ ] Prefer `--closed-loop=false` for the primary comparison run and record any intentional closed-loop run separately.

Risk: F3 may change because `e4daf33` now preserves DELETE state and adds GC behavior, which can alter both the Composition and CompositionRevision lifecycle.

- [ ] Tie the F3 classification to both terminal hashes and trace-level error families, not just the presence or absence of `ConfigMap/default/xr-config`.

Risk: F5 resourceVersion conflicts may convert a previous write into a 409-style conflict, making hash comparisons alone too coarse.

- [ ] Inspect effects and reconcile errors for F5 categories whenever the new terminal hashes do not match the baseline families.

Risk: #7223 combines multiple claims with different maintainer dispositions.

- [ ] Structure the #7223 draft update with separate F5, F6 stale `Ready=True`, and dropped GC-on-fatal sections.

## Acceptance Criteria

- [ ] All required F1, F3, F5, F6, and C2 scenario JSONs listed in this plan have depth-100 dump outputs recorded under `/tmp/crossplane-reeval-89acd8a/` or an explicitly documented equivalent path.
- [ ] `campaign-metrics` has been run and recorded for every dump directory used in the sprint.
- [ ] New terminal-state hashes or trace-step evidence have been compared against the inlined baselines for #7220, #7222, #7223 F5, #7223 F6, and C2.
- [ ] Each open finding has a classification: #7220 / F1, #7222 / F3, #7223 / F5, and #7223 / F6.
- [ ] `docs/upstream-updates/7220-f1-manual-policy.md` exists and is ready for user review.
- [ ] `docs/upstream-updates/7222-f3-composition-deletion.md` exists and is ready for user review.
- [ ] `docs/upstream-updates/7223-f5-f6-reframe.md` exists and is ready for user review.
- [ ] `docs/upstream-updates/7224-c2-claim-deletion-validation-not-for-posting.md` exists and is clearly marked as staged validation only.
- [ ] No GitHub issue comment has been posted by the sprint executor.
- [ ] No Fix 3 SSA field-manager work or C4-class investigation has been started under this sprint.
