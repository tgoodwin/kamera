# Critique: SPRINT-0001-GEMINI

This critique evaluates `SPRINT-0001-GEMINI.md` (the "GEMINI draft") against `SPRINT-0001-CLAUDE.md` ("CLAUDE") and `SPRINT-0001-CODEX.md` ("CODEX").

## Comparison with CLAUDE

### What is stronger than GEMINI
- **Baseline Rigor:** CLAUDE's "Source Inputs and Baselines" table is a significant upgrade. It maps each finding to specific JSON files and, crucially, provides the exact baseline comparison targets (hashes and trace steps) directly in the plan. GEMINI merely references the plan doc.
- **Command-Line Specificity:** CLAUDE includes explicit instructions to run `go run ./cmd/kamera analyze campaign-metrics`, ensuring consistent metric collection across all runs.
- **Classification Logic:** The "Classification Rules" section in CLAUDE provides a formal framework for deciding between `still-reproduces`, `shifts`, and `retracts`. GEMINI's analysis section is vague by comparison.
- **Sensitivity Fallbacks:** CLAUDE includes a specific task to trigger a depth-200 rerun if depth-100 results are entirely max-depth aborted, which is a critical operational detail for this specific harness.

### What is weaker than GEMINI
- **Readability:** CLAUDE's task list is a high-density "wall of text." GEMINI's structure is more skimmable and easier to use as a checklist during active execution.
- **Scope Focus:** GEMINI's "Scope Boundaries" are more concise and clearly delineate what is *not* being done (e.g., Fix 3, C4), whereas CLAUDE buries some of these details in long paragraphs.

## Comparison with CODEX

### What is stronger than GEMINI
- **Operational Preconditions:** CODEX includes a robust "Preconditions" checklist (checkout verification, smoke-tests, build checks) that GEMINI lacks. The "Smoke-run a tiny depth" task is a great practical step.
- **Risk Realism:** CODEX identifies "ground-truth" risks that GEMINI missed, such as macOS `/tmp` volatility and the fact that scenario inputs are currently untracked and could be lost if the worktree is destroyed.
- **Reporting Clarity:** CODEX explicitly lists what should be cited in each draft (HEAD SHA, fidelity commits, scenario filenames), which ensures higher-quality deliverables.

### What is weaker than GEMINI
- **Structural Flow:** CODEX's task list is organized by finding (F1, F3, etc.), which is logical but results in repetitive sub-tasks (Re-run, Inspect, Record). GEMINI's category-based grouping (Execution, Analysis, Reporting) feels more like a professional workflow.

## Missing Tasks & Underweighted Risks

### Missing Tasks
- **Hygiene & Metrics:** Both CLAUDE and CODEX include tasks for recording fidelity SHAs and campaign metrics for *every* dump. GEMINI only mentions "comparing hashes" at the end.
- **Operational Safety:** Moving results out of `/tmp` (CODEX) and sensitivity reruns (CLAUDE) are missing from GEMINI.
- **Build Validation:** GEMINI assumes the harness builds; CODEX/CLAUDE make verifying the build a task.

### Underweighted Risks
- **Fidelity Collisions:** The risk that the new `GarbageCollectorReconciler` creates side-effects in scenarios that didn't expect it (highlighted in CODEX) is ignored in GEMINI.
- **Input Volatility:** GEMINI does not account for the risk of losing untracked scenario JSONs (highlighted in CODEX).
- **Convergence Failure:** GEMINI underweights the risk of depth-100 runs returning only max-depth aborted states due to F2 cycling.

### Sequencing Issues
- GEMINI groups all drafting at the end. CODEX/CLAUDE suggest a more iterative approach or provide more structure to the drafting phase, which reduces the risk of forgetting details from early runs.

## Synthesis & Merging Recommendation

If I were merging these drafts into a "Gold Standard" plan, I would:

1. **Keep the "Source Inputs and Baselines" table from CLAUDE.** It is the single most valuable addition for data-driven re-evaluation.
2. **Keep the "Preconditions" and "Classification Rules" from CODEX/CLAUDE.** These add the necessary engineering rigor to the "Preparation" and "Analysis" phases.
3. **Keep GEMINI's high-level structure (Goals, Scope, Tasks, Risks, AC).** It provides the best scaffolding for the detailed content.
4. **Adopt CODEX's Risk section.** Specifically the warnings about `/tmp` volatility and untracked scenario inputs.
5. **Use CLAUDE's "Phase 0: Run Hygiene"** to replace GEMINI's thin "Preparation" section.
