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
- `Trials` (number of trial runs for the scenario)
- `TrialIndex` (set per run/trial)
- `ScenarioGroup` (stable scenario/input grouping key)

Selection RNG seed is deterministic per trial from `(Seed, scenario-group-id, TrialIndex)`.

### 2. Selection strategy in `explore.go`

Use existing explorer helpers:

Behavior:

- DFS mode:
  - pending: first item (current behavior)
  - view: existing stale-view branching behavior in `getPossibleViewsForReconcile`
- Monte Carlo mode:
  - pending: uniformly random from full `PendingReconciles`
  - view: `getPossibleViewsForReconcile` samples one stale candidate uniformly when multiple are available; otherwise uses current state view

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

### 6. Catalog aggregation for DFS-like TUI UX

Aggregation is performed by inspector catalog loading, not by the runner writing aggregate dump files.

For each `mc_group_id`:

- if explicit on-disk aggregates exist, keep one aggregate entry (newest `ModifiedAt`) and hide trials/older aggregates
- otherwise, merge trial dumps in-memory into one virtual aggregate entry
- merged state keying uses catalog aggregation logic (`aggregateStateKey`), and paths are deduped via existing path dedupe

Catalog/TUI presents aggregate entries as the primary view for Monte Carlo workflows so UX matches DFS: one scenario row with all converged states/paths represented.

Current behavior is intentionally eager during catalog load. Performance optimizations (for large trial sets) are deferred to follow-up work.

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
- `pkg/interactive/inspector_catalog.go` and `pkg/interactive/inspector_catalog_tview.go`
  - aggregate/group Monte Carlo entries for DFS-like UX
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
2. Add Monte Carlo-aware stale-view sampling in `getPossibleViewsForReconcile`.
3. Disable order/stale/initial fanout when `mode=monte_carlo`.
4. Keep DFS behavior unchanged when `mode=dfs`.

### Phase 3: Process-per-run trial orchestration

1. Add trial count/seed plumbing in runner options.
2. Expand supervisor jobs to `scenario x trial`.
3. Pass trial index and derived seed into child executions.
4. Emit trial dump attributes (`mc_*`, role=trial).

### Phase 4: Catalog aggregation and grouping behavior

1. Add/keep catalog aggregation per `mc_group_id`.
2. Prefer explicit aggregate entries when present.
3. Update catalog listing behavior to prioritize aggregate entries for Monte Carlo workflows.

### Phase 5: Validation

1. Add unit tests for selection determinism and single-path semantics.
2. Add runner integration tests for `scenario x trial`.
3. Add catalog tests for grouped Monte Carlo UX.

## Open Questions

- Should non-Monte Carlo workflows ignore `mc_*` attributes entirely in catalog rendering (likely yes)?
- When should eager catalog aggregation move to lazy/on-demand aggregation for large trial sets?
