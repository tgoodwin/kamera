# Parallel Scenario Runner & Scenario Type

**Date**: 2026-01-27
**Epic**: kamera-coverage
**Status**: Design complete, ready for implementation

## Problem Statement

The coverage strategy requires generating many test inputs, each with per-scenario exploration tuning (ordering permutations, staleness, depth/timeout). The current explore runner assumes a single `StateNode` and single `ExploreConfig`. We need a first-class scenario data type and a parallel runner that can execute multiple scenarios concurrently without accidental shared state.

## Goals

- Introduce a `Scenario` type produced by input generators and consumed by exploration runners.
- Support running many scenarios in parallel with per-scenario configs.
- Make state isolation explicit and safe: each scenario run must have its own snapshot store, emitter, and version manager.
- Provide per-scenario dumps/stats for later inspection with `cmd/inspect`.

## Non-Goals

- Parallelizing within a single exploration tree (path exploration) is out of scope.
- Interactive inspector sessions during parallel runs are out of scope.
- Reworking the entire coverage CLI flow is out of scope.

## Proposed Types (pkg/explore)

```go
// Scenario is the unit produced by input generators and consumed by runners.
type Scenario struct {
    Name         string
    InitialState tracecheck.StateNode
    Config       tracecheck.ExploreConfig
    Invariant    func(tracecheck.StateNode) error
}

// ScenarioResult captures everything needed to inspect outcomes.
type ScenarioResult struct {
    Name           string
    Result         *tracecheck.Result
    VersionManager tracecheck.VersionManager
    Stats          *tracecheck.ExploreStats
    DumpPath       string
    InvariantError error
    Err            error
}
```

## Parallel Runner API (pkg/explore)

```go
type ParallelOptions struct {
    MaxParallel int
    DumpDir     string
}

type ParallelRunner struct {
    builder *tracecheck.ExplorerBuilder
}

func NewParallelRunner(builder *tracecheck.ExplorerBuilder) (*ParallelRunner, error)
func (r *ParallelRunner) RunAll(ctx context.Context, scenarios []Scenario, opts ParallelOptions) ([]ScenarioResult, error)
```

### Execution Flow

1. Create a worker pool with size `MaxParallel` (default `GOMAXPROCS` when unset or <=0).
2. For each scenario, fork the builder to isolate state (fresh snapshot store + emitter).
3. Apply the scenario config to the fork (clone first to avoid map sharing).
4. Build an explorer and run `Explore` on the scenario’s `InitialState`.
5. Evaluate invariant (if non-nil) against converged states; record first error.
6. Optionally dump results under `DumpDir` with safe filenames (stats are embedded in dump files when perf stats are enabled).

## Builder Refactor: Explicit Isolation

Add a `Fork()` (or `Isolated()`) method on `tracecheck.ExplorerBuilder` that returns a new builder with isolated mutable state:

- **Fresh per-fork state**: `snapshot.Store`, `event.Emitter`, and any derived `VersionManager` created by `Build`.
- **Shared read-only config**: reconcilers, watchers, resource deps, recorder strategies, reconciler-to-kind, scheme, replay builder, and priority strategy builder.
- **Cloned config**: `ExploreConfig` (including map fields and perturbation maps) must be deep copied to prevent scenario-level mutations leaking across runs.

To reduce accidental sharing, any builder methods that set mutable runtime state should only operate on the current builder instance (not shared global stores). `Fork()` becomes the primary API for parallel runs and should be used even in sequential batch runs for safety.

## Dumping & Later Inspection

Each scenario can optionally produce a dump file usable by `cmd/inspect`:

```
<dump-dir>/<scenario-name>.jsonl
```

Names should be sanitized (lowercase, non-alnum to `_`, length capped, numeric suffixes for collisions). The runner returns the final `DumpPath` so callers can open the dump later.

Stats dumping mirrors the existing behavior but is per-scenario:

```
<stats-dir>/<scenario-name>.json
```

## Error Handling

- Scenario-specific failures (panic/error in exploration, invariant failure) should be captured on the `ScenarioResult`.
- The overall runner only returns a non-nil error when the runner infrastructure fails (context cancellation, invalid options, dump directory errors).
- This preserves partial results in large batches.

## Testing Strategy

- Unit tests for `Fork()` to ensure config/maps are cloned and snapshot stores are isolated.
- Parallel runner tests validating:
  - per-scenario configs do not leak (e.g., different `MaxDepth`).
  - dumps/stats are written with unique names.
  - invariant errors are captured without dropping results.

## Migration Notes

- Existing single-run code remains unchanged; `Runner` continues to run one scenario.
- Input generators should emit `[]Scenario` instead of bare `StateNode` + config tuples.

## Open Questions

1. Should invariant evaluation apply to all converged states or only when exactly one exists?
2. Should per-scenario `Config` be treated as full override or as an overlay onto builder defaults?
3. Do we want a default naming convention that includes scenario indices for deterministic ordering?
