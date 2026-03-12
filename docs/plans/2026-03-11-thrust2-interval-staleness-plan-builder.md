# Thrust 2 (v1): Interval-Based Staleness Plan Builder

## Context

The old `buildStalenessPerturbationPlans` uses `StalenessConfig` (lookback limits + Monte Carlo branching) which was ineffective at targeting real bugs. Since that was written, the staleness simulation was reworked to use `StalenessInterval` — declarative windows `[StaleAt, CatchUpAt)` where a controller's view of a kind lags behind the frontier. Intervals are deterministic (no branching from staleness itself; ordering permutation provides the branching).

This plan rewrites `buildStalenessPerturbationPlans` to generate interval-based plans from the reference trace, following the `buildUserActionInterleavingPlans` pattern: enumerate every possible interval, one plan per interval. Cross-controller dataflow pruning is deferred.

## Algorithm: Deriving Intervals from the Reference Trace

Walk the first converged path. For each step, `ReconcileResult` provides:
- `ControllerID` — who ran
- `Changes.Observations` — what was read, with `Key.CanonicalGroupKind()`
- `KindSeqBefore` / `KindSeqAfter` — per-kind frontier before/after the step

**Extract two things:**

1. **Kind max frontiers**: For each kind, the maximum KindSequence frontier value observed across all steps. This gives the upper bound of the sequence space.

2. **Observed (controller, kind) pairs**: Which controllers read which kinds anywhere in the reference trace. This is a *kind-level* filter — if controller C never reads kind K, no interval for (C, K) is generated regardless of reordering.

**Enumerate intervals exhaustively over the range**: For each observed (controller, kind) pair:
- Let `maxSeq` = the max frontier value for that kind
- For each `staleAt` in `[0, maxSeq)`, for each `catchUpAt` in `(staleAt, maxSeq + 1]`:
  - Emit `StalenessInterval{ReconcilerID: C, Kind: K, StaleAt: staleAt, CatchUpAt: catchUpAt, Lag: -1}`

No read-point filtering within the window — intervals are decoupled from specific execution ordering so they remain valid when composed with other perturbation strategies (e.g., user-action interleaving that shifts frontier timelines).

This is O(maxSeq²) per (controller, kind) pair, but maxSeq values are typically single-digit, keeping the total plan count manageable.

## Changes

### 1. New helper: `maxKindFrontiers`
**File**: `pkg/explore/parallel_runner.go`

```go
func maxKindFrontiers(path tracecheck.ExecutionHistory) map[string]int64
```

Walk all steps, track the maximum `KindSeqAfter[kind]` value per kind. Returns the upper bound of each kind's sequence space.

### 2. Rewrite `buildStalenessPerturbationPlans`
**File**: `pkg/explore/parallel_runner.go` (lines 745-811)

- Guard: need converged reference with at least one non-empty path
- Call `observedReadKindsPerController` to get (controller, kind) pairs (reuse existing helper)
- Call `maxKindFrontiers` to get per-kind range upper bounds
- Enumerate intervals exhaustively: for each (controller, kind), for each `(staleAt, catchUpAt)` in `[0, maxSeq]`, emit `StalenessInterval{Lag: -1}`
- Sort for deterministic output
- One plan per interval:
  - `disablePerturbations(base)` as baseline
  - Enable `PermuteOrder` for all observed controllers
  - Set `Config.Perturbations.StalenessIntervals` to single-element slice
  - Attributes: `perturbation.strategy=staleness_interval`, reconciler, kind, staleAt, catchUpAt

### 3. Keep `observedReadKindsPerController`
Reused as the (controller, kind) pair filter — no changes needed.

### 4. Update tests
**File**: `pkg/explore/parallel_runner_test.go`

- Rewrite `TestBuildStalenessPerturbationPlansProducesStalenessPhase`: construct reference with steps that have `KindSeqBefore`/`KindSeqAfter` populated, verify plans use `StalenessIntervals` (not `Staleness`), verify one plan per interval, verify attributes
- Rewrite nil-result test
- Add test for `maxKindFrontiers`
- Add test: kind with maxSeq=0 → no intervals (range is empty)
- Keep existing `observedReadKindsPerController` tests unchanged

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Lag value | Always `-1` (frozen) | Most aggressive — controller completely misses updates. Simplest for v1. |
| Plans per interval | One plan per interval | Matches interleaving pattern. Enables per-interval attribution. |
| Ordering permutation | Enabled for all observed controllers | Staleness × reordering combinations are where bugs surface. |
| Search mode | DFS (default) | Intervals are deterministic; ordering provides branching. No MC needed. |
| Interval enumeration | Exhaustive over `[0, maxSeq]` range | Decoupled from specific execution ordering; remains valid when composed with other perturbation strategies (e.g., user-action interleaving). |
| (Controller, kind) filter | Only pairs observed in reference | Cheap filter — if C never reads K, staleness of K for C can't matter regardless of ordering. |
| Multiple kinds | Separate interval per kind | Independent dimensions; cross-product is a future enhancement. |

## Verification

1. `go test ./pkg/explore/ -run TestBuildStaleness` — new tests pass
2. `go test ./pkg/explore/ -run TestMaxKindFrontiers` — helper test passes
3. `go test ./pkg/explore/` — all existing tests still pass (including observedReadKindsPerController tests)
4. `go test ./pkg/tracecheck/` — no regressions in core engine
