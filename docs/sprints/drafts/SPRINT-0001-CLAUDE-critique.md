# SPRINT-0001-CLAUDE critique vs CODEX and GEMINI

## CODEX draft

### Stronger than mine

- **Run hygiene phase is more operational.** CODEX's Phase 0 names a concrete
  binary (`go build -o bin/kamera ./cmd/kamera`), a concrete depth-100 explore
  config workaround for scenario JSONs that have `maxDepth: 0`
  (`/tmp/crossplane-reeval-depth100.json` with `{"maxDepth":100}`), and an
  explicit `--closed-loop=false` default. My plan hand-waves the scenario
  invocation and never addresses scenario JSONs with `maxDepth: 0`, which is a
  real failure mode that would silently truncate runs.
- **`campaign-metrics` is a first-class step after every run.** CODEX folds
  `go run ./cmd/kamera analyze campaign-metrics <dump>` into every phase and
  requires copying the output into the per-issue notes. My plan asks for hash
  diffs but never names the tool used to produce campaign metrics, which is
  exactly the metric I claim to be diffing against the baselines.
- **Sensitivity rerun at depth 200 when all states are max-depth aborted.**
  CODEX has an explicit fallback rule. I list "depth-100 wall-clock" as a risk
  but never give the executor an action to take when the campaign converges to
  zero true terminals.
- **Explicit Classification Rules section.** CODEX writes down what
  `still-reproduces` / `shifts` / `retracts` actually mean, and gives
  per-finding rules (e.g. "for F3, treat collapse of `93d750b`/`9dc61c9` and
  `709d71b`/missing as a retraction candidate unless trace evidence shows a
  new permanent loop"). My plan asks the executor to classify but leaves the
  semantics implicit.
- **Fidelity SHA inventory.** CODEX names every SHA in scope (`89acd8a`,
  `b12542d`, `e4daf33`/`6cd7396`, `1def992`, `7ba2045`, `cb1c43e`, `911b3bd`).
  Mine references "fidelity commits" generically and only names a couple.
- **C2 baseline is sharper.** CODEX inlines the 96/98 orphaned vs 2/98 clean
  split. My C2 task only says "confirm divergence collapses" without the
  numeric expectation.
- **#7223 update structure.** CODEX explicitly enforces three subsections in
  the draft: F5, F6 stale-Ready=True, and dropped GC-on-fatal. Mine recommends
  splitting #7223 into two issues without prescribing the draft layout.

### Weaker than mine

- **Scenario-input strategy is decided unilaterally.** CODEX hard-codes the
  worktree path
  (`.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios`) as
  the source of truth for every run. I treat this as a Precondition decision
  (copy into tree vs `--inputs <abs>`) and call out the fragility — if the
  worktree is destroyed mid-sprint, CODEX's plan breaks silently and the
  inputs are gone forever (untracked from `4a6e80d`/`cc11c2b`).
- **No `/tmp` volatility risk.** CODEX puts everything under
  `/tmp/crossplane-reeval-89acd8a/` and never warns about macOS clearing
  `/tmp`. Mine flags this and recommends tarring or moving to
  `~/sprint-0001-results/`.
- **No GC-cascade-as-fidelity-bug warning.** CODEX treats the new
  `GarbageCollectorReconciler` purely as something that may shift outcomes.
  Mine adds the explicit guidance that wholly new abort/converged states
  should be treated as candidate fidelity issues until reviewed, not
  auto-classified as Crossplane bugs.
- **Sequencing puts F1 before F5/F6.** Phase 2 is F1, Phase 3 is F5, Phase 4
  is F6. F1 is the lowest-information re-run (sanity, expected no change).
  Putting it ahead of F5 risks burning depth-100 wall-clock on a confirmation
  before the high-information runs are done. Mine does F3 → C2 → F5 → F6 → F1,
  which finishes the genuinely-uncertain ones first.
- **Six phases is heavy structure for what is effectively five re-runs and
  four drafts.** The acceptance criteria duplicate the per-phase task list.

### Missing tasks in CODEX

- No "verify `examples/crossplane/.agents/ANALYSIS.md` is reachable" step. The
  inlined hashes in `docs/crossplane-reevaluation-plan.md` §3 are the only
  surviving baseline if that file is lost; CODEX never confirms it.
- No precondition smoke run on a tiny depth (e.g. 10) before kicking off the
  long depth-100 campaigns.
- No fidelity-concern surfacing rule in acceptance criteria — CODEX requires
  classifications and drafts but not a written record of any new fidelity
  artifact uncovered by re-runs.

### Risks underweighted in CODEX

- Untracked scenario inputs (worktree-destruction) is the biggest single
  blast-radius risk and is absent.
- Hash-comparison ambiguity (shift-vs-fidelity-artifact) gets one line under
  F3 but is not generalized as a sprint-wide policy.

### Sequencing wrong in CODEX

- F1 ahead of F5/F6 (see above).
- C2 in Phase 5, after F5 and F6. C2 is the fastest end-to-end validation
  that the hardened harness is wired correctly; running it second (as I do)
  catches harness-level breakage before sinking depth-100 budget into F5/F6.
  CODEX runs it last.

## GEMINI draft

### Stronger than mine

- **Brevity.** The plan fits on one screen and the phases are easy to scan.
  For an executor who already knows the plan doc cold, the cognitive load is
  lower than mine.
- Nothing else is materially stronger.

### Weaker than mine

- **Massively under-specified at task granularity.** "Run F3 scenario" is one
  bullet; mine breaks this into hypothesis-1, primary, and the
  xr-deleted-with-active-composition control with three dump targets and
  six diff bullets. GEMINI's executor would have to redesign the run from
  scratch.
- **No dump paths, no campaign-metrics tool reference, no terminal-hash
  comparison procedure.** GEMINI's "Compare F3 terminal-state hashes and
  campaign metrics" assumes the executor knows what command produces what.
- **Only five scenarios listed.** GEMINI executes one scenario per finding.
  F3 alone has three relevant scenarios in the plan
  (`hypothesis-1`, base, `xr-deleted-with-active-composition`); F5 has three
  (`hypothesis-1`, base, `interval`); F6 has primary plus F7/F8 controls.
  GEMINI's "All 5 scenarios executed" acceptance bar is roughly half of what
  the plan calls for.
- **No C2 acceptance artifact.** GEMINI says "Document result for C2 (staged
  only)" in the analysis section but does not produce an explicit
  `docs/upstream-updates/7224-c2-...md` deliverable. Mine and CODEX both
  require this file.
- **No fidelity SHA list, no fidelity-concern surfacing rule, no
  classification rules.**
- **No depth-100 / `maxDepth: 0` workaround**, no `--closed-loop=false`
  default, no sensitivity rerun policy.

### Missing tasks in GEMINI

- F3 controls (`composition-deleted-while-xr-bound.json` non-hypothesis,
  `xr-deleted-with-active-composition.json`).
- F5 secondary scenarios (`function-capability-removed.json` non-hypothesis,
  `interval_function-capability-removed.json`).
- F6 controls (F7 `composition-switches-resources`, F8
  `function-flap-fatal-recovery`).
- F1 stale variant (`...-composition-switch-stale.json`).
- C2 staged validation deliverable file.
- Untracked-input handling.
- Smoke run / precondition build verification.

### Risks underweighted in GEMINI

- "Harness Regressions: `crossplane-reeval` is an integrated branch; potential
  for new simulation bugs" is the right risk but has no mitigation. CODEX and
  I both connect this to a concrete behavior (treat new aborts/converged
  states as candidate fidelity issues; classify only after review).
- No risk for untracked baselines (`ANALYSIS.md`, scenario JSONs).
- No risk for hash-vs-fidelity-artifact ambiguity.
- No risk for `/tmp` volatility.

### Sequencing wrong in GEMINI

- "Critical Path: Run F3 (most likely to shift) and F5" before C2 validation.
  Same problem as CODEX: C2 is the fastest harness-fidelity sanity check and
  should land between prep and the long F-runs, not after them.
- Also runs F1 and F6 together as "Sanity," which conflates an
  expected-no-change re-run (F1) with a high-information bug re-run (F6).

## If I were merging

- Keep from CODEX:
  - Phase 0 run hygiene with the `bin/kamera` build, the depth-100 explore
    config for `maxDepth: 0` scenarios, and `--closed-loop=false` default.
  - `campaign-metrics` as a mandatory per-run step.
  - Sensitivity rerun at depth 200 when all states are max-depth aborted.
  - Classification Rules section, including the per-finding retraction
    triggers (F3 hash collapse, F5 disappearance of categories B and C).
  - Full fidelity SHA inventory.
  - 96/98 vs 2/98 numeric C2 baseline.
  - The "structure the #7223 draft with separate F5 / F6-Ready / dropped GC"
    instruction.
- Keep from GEMINI:
  - Nothing structural. The brevity is the only feature, and it is bought
    by removing content the sprint actually needs. At most, lift GEMINI's
    one-line phase headers as section titles in the merged plan to keep the
    table of contents readable.
- Keep from mine:
  - Sequencing F3 → C2 → F5 → F6 → F1.
  - Scenario-input strategy as an explicit precondition decision (copy into
    tree vs `--inputs <abs>`), with the worktree-fragility risk called out.
  - Untracked baseline (`ANALYSIS.md`) reachability check.
  - Smoke run at small depth before depth-100 campaigns.
  - `/tmp` volatility mitigation.
  - "Treat new aborts/converged states as candidate fidelity issues until
    reviewed" rule, generalized as a sprint-wide policy and added to
    acceptance criteria as a fidelity-concern logging requirement.
  - Full per-scenario task expansion (controls F7/F8 for F6, hypothesis +
    base + interval for F5, hypothesis + base + xr-deleted control for F3,
    primary + stale for F1).
