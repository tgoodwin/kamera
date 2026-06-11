# SPRINT-0001 Codex Critique of Peer Drafts

This critique compares `SPRINT-0001-GEMINI.md` and `SPRINT-0001-CLAUDE.md`
against my draft, `SPRINT-0001-CODEX.md`.

## Gemini Draft

### What Is Stronger Than Mine

Gemini's draft is much easier to scan. The top-level task groups
`Preparation & Harness Setup`, `Execution: High-Priority Re-runs (Depth 100)`,
`Execution: Sanity Checks & Validation (Depth 100)`, `Analysis &
Classification`, and `Reporting & Documentation` make the critical sprint
shape obvious in a way my longer draft does not.

Gemini also gives a concise prioritization signal by putting `F3 (Composition
Deletion)`, `F5 (Stale ValidPipeline)`, and `F6 (Fatal Function Orphans)` under
`Execution: High-Priority Re-runs (Depth 100)`, then placing `F1 (Manual
Policy)` and `C2 Validation (Claim Deletion)` under sanity/validation. My draft
is more complete, but Gemini's structure more clearly says which findings are
the main re-evaluation targets.

The `Draft Update #7223 (F5/F6)` task is especially strong because it names the
editorial action: "Reframe to separate F5 (race) and F6 (stale Ready)" and
"Drop the unsafe GC-on-fatal proposal." My draft has the same content, but
Gemini's phrasing is sharper as an upstream-update instruction.

### What Is Weaker Than Mine

Gemini collapses the sprint too aggressively. Its acceptance criterion says
`All 5 scenarios executed at depth 100`, but the sprint actually needs the
primary scenarios plus stale variants and controls. My draft enumerates all
required reruns: two F1 inputs, three F3 inputs, three F5 inputs, three F6
inputs, and C2.

The execution tasks are under-specified. For example, `F3 (Composition
Deletion): Run workflow_crossplane-deletion_composition-deleted-while-xr-bound-hypothesis-1.json`
omits the companion F3 runs
`workflow_crossplane-deletion_composition-deleted-while-xr-bound.json` and
`workflow_crossplane-deletion_xr-deleted-with-active-composition.json`.
Likewise, `F5 (Stale ValidPipeline)` omits
`workflow_crossplane-staleness_function-capability-removed.json` and
`interval_function-capability-removed.json`, and `F6 (Fatal Function Orphans)`
omits the F7/F8 controls
`workflow_crossplane-function-failure_composition-switches-resources.json` and
`workflow_crossplane-function-failure_function-flap-fatal-recovery.json`.

Gemini's `Analysis & Classification` section is too expectation-driven. Tasks
like `Confirm F6 still produces orphans and stale status` and `Verify F1 still
produces the same logic failure (expected: no change)` risk biasing the executor
toward the old conclusion. My draft consistently asks for classification as
`still-reproduces`, `shifts`, or `retracts` after comparing hashes, trace
patterns, error families, and campaign metrics.

The draft also lacks concrete run hygiene. It does not require
`campaign-metrics` after every run, does not guard against `analyze diff`
treating max-depth-aborted states as converged, does not specify a stable output
root, and does not mention direct scenario mode / `--closed-loop=false`.

### Missing Tasks

Gemini is missing the task to create a depth-100 explore config for scenario
JSONs with `maxDepth: 0`, which my draft names as `Create a depth-100 explore
config such as /tmp/crossplane-reeval-depth100.json`.

Gemini is missing the task to record the fidelity SHAs. My draft names `89acd8a`,
`b12542d`, `e4daf33` / upstream `6cd7396`, `1def992`, `7ba2045`, `cb1c43e`, and
`911b3bd`.

Gemini is missing the task to run `campaign-metrics` after every scenario run
and copy the converged/aborted/max-depth-aborted counts into issue notes.

Gemini is missing the F3-specific checks for whether `GarbageCollectorReconciler`
changes the CompositionRevision lifecycle and whether both `errFetchComp` and
`errSelectComp` still appear.

Gemini is missing the F5-specific resourceVersion-conflict analysis: whether PR
#76 narrows the buggy categories, converts writes to conflicts, or only shifts
category ratios.

Gemini is missing the F6 split classification between orphan persistence and
stale `Ready=True`. It only has one `Confirm F6 still produces orphans and stale
status` task.

Gemini is missing the staged C2 output filename and the warning that #7224 is
closed and the validation note is not for posting.

### Risks Underweighted

Gemini underweights the max-depth-abort risk. The only related risk is `Depth
100 Overhead`, but the operational risk is not just runtime: all states may be
max-depth aborted, in which case the run has no true converged-state comparison.
My draft calls for `campaign-metrics` after every run and depth-200 sensitivity
reruns when all states are max-depth aborted.

Gemini underweights input preservation. It says to ensure scenario JSONs are
accessible, but it does not call out that the scenario inputs may live under the
`.claude/worktrees/endpoints-pod-watch/...` path and may be untracked.

Gemini underweights harness-fidelity interpretation risks. The draft mentions
`Harness Regressions`, but does not identify the concrete mechanisms most likely
to move results: `GarbageCollectorReconciler`, DELETE finalizer/spec/status
preservation from `6cd7396`, and resourceVersion conflict checking from PR #76.

Gemini underweights #7223 editorial risk. It correctly says to drop the unsafe
GC-on-fatal proposal, but it does not force separate classification of F5 and
F6 or separate handling of stale `Ready=True` versus orphan persistence.

### Sequencing Problems

Gemini's sequence puts `Critical Path: Run F3 (most likely to shift) and F5`
before `Validation: Run C2 to confirm harness fidelity`. I would run F3 first,
then C2 early, because C2 validates the DELETE semantics fix that also affects
interpretation of F3 and gives a quick sanity check on the hardened harness
before spending time on the larger F5/F6 campaigns.

Gemini defers analysis into a broad `Analysis & Classification` phase after all
runs. The better sequence is per-issue: run one scenario family, immediately run
`campaign-metrics`, inspect hashes/error families/effects, then classify or
record blockers before moving on. That prevents a pile of dumps with ambiguous
provenance.

Gemini places `F6 (Fatal Function Orphans)` in high-priority execution, while
Claude and my draft treat F6 as important but less likely to be changed by the
harness hardening than F3 and F5. It should not displace F3/C2/F5 on the
critical path.

## Claude Draft

### What Is Stronger Than Mine

Claude's `Preconditions` section is stronger than mine in two important ways.
The task `Decide input strategy: copy 8 needed scenario JSONs from
.claude/worktrees/endpoints-pod-watch/examples/crossplane/scenarios/ back into
examples/crossplane/scenarios/, OR pass --inputs <abs-path> per run` directly
addresses the fragile location of the inputs. My draft names the scenario
prefix, but Claude turns input preservation into an executable precondition.

Claude's `Smoke-run a tiny depth (e.g. depth 10) on one scenario` task is also
stronger than my `Build or smoke-test the Crossplane harness` task because it
specifies the purpose: verify the binary, replay client, and GC controller wire
up cleanly before long depth-100 campaigns.

Claude adds a useful deliverable that my draft omits: `Run log (append-only) at
docs/sprints/drafts/SPRINT-0001-runlog.md with one section per re-run task`.
That is a good operational artifact for preserving dump paths, metrics, and
classification notes.

Claude's F3 analysis is more detailed in one area: `Diff effect counts vs
baseline (APPLY=192, DELETE=64, REMOVE=64, UPDATE=1343)`. My draft compares
terminal hashes and error families but does not explicitly call out effect-count
diffing.

Claude's risk section is stronger on environment durability. `Untracked scenario
inputs`, `Untracked baseline`, and `/tmp volatility` are concrete risks that my
draft mostly handles indirectly through fixed paths and run notes.

### What Is Weaker Than Mine

Claude's plan is slightly less explicit about mandatory `campaign-metrics` after
every run. It says `Diff campaign metrics` for F3 and F5 and mentions summaries
in acceptance criteria, but my draft repeats `Run campaign metrics` as a task
after every individual scenario, including F1, F3 original/control, F5
original/interval, F6 controls, and C2. That repetition is useful because the
repository guidance specifically says to run `campaign-metrics` after every
scenario run.

Claude's draft names `All re-runs at depth 100 on crossplane-reeval HEAD
89acd8a`, but its `Preconditions` also says `Confirm checkout: git rev-parse
HEAD = 89acd8a`. That is stricter than my `if the branch tip contains doc-only
sprint commits, confirm there are no code diffs from 89acd8a before running
scenarios`. If planning docs have already been committed after `89acd8a`,
Claude's exact-HEAD precondition could falsely block execution even when the
harness code under test is unchanged.

Claude's output naming is less review-ready than mine. It proposes
`docs/upstream-updates/7220-update.md`, `7222-update.md`, and `7223-update.md`.
My draft's filenames, such as
`7220-f1-manual-policy.md`,
`7222-f3-composition-deletion.md`, and
`7223-f5-f6-reframe.md`, make the issue/finding mapping clearer.

Claude's `F5` acceptance task says `Confirm >=2 outcome categories appear
(acceptance criterion)`. That may be too weak: if the hardened run preserves
only the correct Category A and removes buggy B/C, two categories are not the
right standard. My draft's classification rule is more precise: disappearance
of Category B and Category C is a retraction candidate, while changed hashes or
lower ratios with buggy composition still present is `shifts`.

### Missing Tasks

Claude is missing my explicit `Classification Rules` section. It has
classification tasks per finding, but it does not define decision rules for
`still-reproduces`, `shifts`, and `retracts`, or the specific F3/F5/F6/C2
interpretation rules.

Claude is missing the explicit instruction to use direct scenario mode with
`--closed-loop=false` unless a run note justifies the closed-loop pipeline. My
draft includes that because closed-loop rerun generation could make comparison
against the original scenario JSONs harder.

Claude is missing the depth-doubling rule for all-max-depth-aborted runs. It
mentions depth-100 wall clock and campaign metrics, but not the required action:
keep the depth-100 result and add a sensitivity rerun at depth 200 when all
states are max-depth aborted.

Claude is missing my staged C2 filename warning in the task itself. It says
`Stage validation note at docs/upstream-updates/7224-c2-staged-validation.md
(do NOT post; #7224 is closed)`, but my draft's task requires a top-level
warning in the file that the text is staged evidence only, not a GitHub comment
to post.

Claude is missing my requirement that every upstream update include a `not
posted by sprint executor` note, exact scenario JSON filenames, dump paths,
campaign-metrics summaries, and classification labels.

### Risks Underweighted

Claude underweights the risk that `analyze diff` can mislead on aborted states.
It has good campaign-metric language, but it does not explicitly warn that
`analyze diff` treats max-depth-aborted states as converged. My draft carries
that repository-specific guardrail into the sprint plan.

Claude underweights the editorial risk in #7223 compared with my draft. It says
to draft `7223-update.md` and separately address F5 and F6, but my draft makes
the combined #7223 risk a mitigation task: `Structure the #7223 draft update
with separate F5, F6 stale Ready=True, and dropped GC-on-fatal sections`.

Claude underweights run-mode ambiguity. Without the `--closed-loop=false`
guidance, an executor could accidentally compare closed-loop reference/rerun
outputs against baselines produced from direct scenario JSONs.

### Sequencing Problems

Claude's sequence is stronger than Gemini's and mostly compatible with mine:
`F3 re-run first`, `C2 re-run second`, then `F5, then F6, then F1`. I would keep
the early F3/C2 ordering.

The one sequencing issue is putting F1 last. F1 is low risk, but it is also a
cheap trace-pattern sanity check. My draft runs F1 before F5/F6 after F3, which
gives an early signal that the basic Crossplane composition path still behaves
as expected before spending time on larger staleness/fatal-function campaigns.
That said, this is a minor sequencing disagreement compared with Gemini's
broader under-specification.

Claude also puts `Updated sprint ledger entry` under deliverables. Ledger
updates belong to sprint-planning workflow bookkeeping, not the execution plan
for re-evaluating Crossplane findings. If included in the final merged plan, it
should be separated from the technical acceptance criteria.

## If I Were Merging

I would keep Gemini's compact top-level structure and especially its sharp
`Draft Update #7223 (F5/F6)` wording about separating F5/F6 and dropping the
unsafe GC-on-fatal proposal.

I would keep Claude's `Preconditions`, input-strategy task, tiny-depth smoke
run, append-only run log, effect-count diffing for F3, and concrete risks for
untracked inputs, untracked baselines, and `/tmp` volatility.

I would keep my draft's exhaustive scenario matrix, per-run `campaign-metrics`
tasks, direct-scenario-mode guidance, depth-doubling rule for all-aborted runs,
classification rules, issue-specific upstream-update filenames, and explicit
not-for-posting safeguards.
