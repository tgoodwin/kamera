# Monte Carlo Search Mode for Non-Branch-Safe Controllers

## Goal

Add a single-path Monte Carlo exploration mode that can apply perturbations (pending-order variation and stale views) without DFS branching, so harnesses with process-local singleton state (for example Karpenter) can still be explored safely.

## Problem

Current exploration in `pkg/tracecheck/explore.go` is DFS with explicit branch fanout for:

- pending reconcile order permutations
- stale-view permutations

This is useful for exhaustive interleaving search, but assumes branch isolation. Some harnesses depend on shared in-memory controller state that is not persisted into `StateNode`; those harnesses are unsafe under in-process branch fanout.

## Design Summary

### 1. Search mode in `ExploreConfig`

Add a search mode to `tracecheck.ExploreConfig`:

- `dfs` (existing behavior)
- `monte_carlo` (new)

Add Monte Carlo settings:

- `Seed` (deterministic base seed)
- `TrialIndex` (set per run/trial)
- `EnableRandomPendingSelection` (v1: always true when `mode=monte_carlo`)
- `EnableRandomStaleViewSelection` (v1: true when `mode=monte_carlo`)

Selection RNG seed is deterministic per trial from `(Seed, scenario-group-id, TrialIndex)`.

### 2. Selection strategy hooks in `explore.go`

Introduce two internal strategy helpers:

- `selectPendingReconcile(state StateNode) (PendingReconcile, error)`
- `selectStateView(currState StateNode, pending PendingReconcile) (StateNode, bool, error)`

Behavior:

- DFS mode:
  - pending: first item (current behavior)
  - view: existing stale-view branching behavior
- Monte Carlo mode:
  - pending: uniformly random from full `PendingReconciles`
  - view: if staleness candidates exist under existing `Perturbations.Staleness` bounds, sample one uniformly; else use current state view

Notes:

- v1 does **not** gate pending sampling by depth.
- v1 reuses existing staleness configuration (`StaleReadBounds`, `MaxRestarts`) exactly.

### 3. Single-path semantics in Monte Carlo mode

In Monte Carlo mode, never enqueue alternative branches:

- no initial pending-order variant fanout
- no stale-view fanout
- no order-variant fanout in `enqueueNextStates`

Each trial executes one sampled path from input to terminal condition (converged/aborted/timeout/max-depth).

### 4. Trial orchestration with process isolation

Use process-per-run as the default execution model for Monte Carlo trials.

Runner behavior:

- expand jobs to `scenario x trial`
- each job runs as a child process (`go run . ... --parallel-child-index=...` pattern)
- child receives deterministic trial seed/index metadata
- each child emits one trial dump

This avoids in-process singleton bleed across trials.

### 5. Dump metadata and grouping

Use dump context metadata (already supported) for bookkeeping.

In `context.scenario.attributes`, add:

- `search_mode=monte_carlo`
- `mc_group_id=<stable group key for scenario/input>`
- `mc_trial_index=<0..N-1>`
- `mc_trial_count=<N>`
- `mc_seed=<derived seed>`
- `mc_role=trial|aggregate`

### 6. Built-in aggregation for DFS-like TUI UX

The runner/supervisor performs aggregation automatically after all trials finish.

For each `mc_group_id`, merge all trial dumps into one aggregate dump:

- merge converged states by `State.Hash()`
- append paths per converged state, then apply existing path dedupe (`dedupePathsByUniqueKey`)
- merge aborted states similarly
- preserve scenario/workflow context and set `mc_role=aggregate`

TUI catalog should present aggregate entries as the primary view for Monte Carlo workflows so UX matches DFS: one scenario row with all converged states/paths represented.

## Non-Goals (v1)

- Exhaustive interleaving coverage guarantees in Monte Carlo mode
- Adaptive sampling policies (biased/reward-guided)
- Cross-trial in-process reset hooks for singleton harnesses
- New stale-view semantics beyond existing perturbation bounds

## Configuration Surface

### Explore config JSON

Extend `pkg/explore/configuration.go` with a `search` block, for example:

```json
{
  "search": {
    "mode": "monte_carlo",
    "monteCarlo": {
      "seed": 1337,
      "trials": 1000
    }
  }
}
```

`trials` is consumed by runner orchestration; `seed` and `trialIndex` are injected into each trial config before child execution.

## File-Level Impact

- `pkg/tracecheck/explore.go`
  - add search-mode-aware pending/view selection helpers
  - skip branch fanout when mode is Monte Carlo
- `pkg/tracecheck/state.go`
  - keep current permutation logic; caller in Monte Carlo mode bypasses expansion
- `pkg/tracecheck/explorebuilder.go`
  - clone new search config fields
- `pkg/explore/configuration.go`
  - parse/overlay `search` config fields
- `pkg/explore/parallel_runner.go`
  - generate `scenario x trial` job matrix
  - pass deterministic trial seeds
  - aggregate per-group trial dumps
- `pkg/interactive/inspector_catalog.go` and `pkg/interactive/inspector_catalog_tview.go`
  - prefer/display Monte Carlo aggregate entries for DFS-like UX
- `pkg/interactive/inspector_dump.go`
  - no schema break; ensure attributes are preserved in write/read paths
- example harness scenario builders (`examples/*/scenario.go`)
  - ensure scenario context includes stable scenario/input identity (`input_index` or equivalent) for grouping

## Testing Plan

### Unit tests

- pending selection
  - Monte Carlo mode chooses non-first pending over repeated runs
  - same seed + trial index => deterministic selection sequence
- stale view selection
  - respects existing staleness bounds/restart limits
  - deterministic given seed
- no branch fanout in Monte Carlo mode
  - `len(stack)` progression remains single-path except normal step progression

### Runner tests

- `scenario x trial` expansion count is correct
- deterministic seed derivation reproducibility
- aggregate dump contains merged states/paths for same `mc_group_id`
- dedupe behavior preserved (equivalent paths collapse)

### Catalog/TUI tests

- catalog identifies trial vs aggregate via attributes
- Monte Carlo aggregate appears as primary scenario view

## Risks and Mitigations

- Risk: Monte Carlo trials still bleed state in-process
  - Mitigation: process-per-run default for Monte Carlo
- Risk: global simclock/async collector interactions across trials
  - Mitigation: process isolation + explicit per-trial metadata
- Risk: aggregate/trial file clutter in dump directory
  - Mitigation: metadata role tags and catalog filtering preference

## Implementation Plan

### Phase 1: Config and strategy scaffolding

1. Add search-mode and Monte Carlo config types to `ExploreConfig`.
2. Extend config cloning and JSON overlay.
3. Add deterministic RNG initialization in explorer runtime from `(seed, scenario, trial index)`.

### Phase 2: Monte Carlo execution semantics in explorer

1. Add `selectPendingReconcile` and replace direct `PendingReconciles[0]` usage.
2. Add `selectStateView` and replace stale-view fanout path for Monte Carlo mode.
3. Disable order/stale/initial fanout when `mode=monte_carlo`.
4. Keep DFS behavior unchanged when `mode=dfs`.

### Phase 3: Process-per-run trial orchestration

1. Add trial count/seed plumbing in runner options.
2. Expand supervisor jobs to `scenario x trial`.
3. Pass trial index and derived seed into child executions.
4. Emit trial dump attributes (`mc_*`, role=trial).

### Phase 4: Built-in aggregation and catalog behavior

1. Add supervisor aggregation pass per `mc_group_id`.
2. Emit aggregate dumps (`mc_role=aggregate`).
3. Update catalog listing behavior to prioritize aggregate entries for Monte Carlo workflows.

### Phase 5: Validation

1. Add unit tests for selection determinism and single-path semantics.
2. Add runner integration tests for `scenario x trial` and aggregate dump correctness.
3. Add catalog tests for grouped Monte Carlo UX.

## Open Questions

- Should non-Monte Carlo workflows ignore `mc_*` attributes entirely in catalog rendering (likely yes)?
- Should aggregate dumps include merged stats or only state/path data (v1 can keep stats optional)?
