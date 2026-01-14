# Exploration Non-Determinism

## Overview

The state space explorer can produce different results across separate program invocations, even with identical configurations. This document explains the sources of non-determinism and their impact.

## Observed Behavior

```
Run 1 (separate invocation):
    Total visits: 477
    Unique logical states: 73
    Time: ~25s

Run 2 (separate invocation):
    Total visits: 1017
    Unique logical states: 145
    Time: ~53s

Back-to-back runs (same session):
    Both produce identical results (477 visits, 73 states)
```

## Root Cause: Go's Map Hash Seed

Go randomizes its map hash seed at program startup for security (hash collision attacks). This affects:

1. **Map iteration order**: `for k, v := range myMap` visits keys in hash-bucket order
2. **Set operations**: Sets implemented as `map[K]struct{}` have non-deterministic iteration

Even if all final outputs are sorted, the **order in which items are processed** can affect:
- Which exploration path is taken first
- Which states get cached/memoized first
- Which ordering variants are generated in what sequence

## Impact on Exploration

```
                          Non-determinism in Action
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Run 1: Map iteration produces order [A, B, C]                          │
│         First path explores: A → X → Y → converged                      │
│         State X gets cached/marked first                                │
│         Subsequent paths hitting X can skip                             │
│                                                                         │
│  Run 2: Map iteration produces order [C, A, B]                          │
│         First path explores: C → Z → W → converged                      │
│         State Z gets cached/marked first                                │
│         Different paths can skip, different exploration tree            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Potential Sources in the Codebase

### 1. Trigger Manager (`trigger.go`)

```go
// Line 247: Iterating over primaries (a Set/map)
for primaryReconcilerID := range primaries {
    // Order affects which reconciler is added to result first
}

// Line 294: Same pattern for owner reconcilers
for ownerReconcilerID := range primaries {
    // ...
}
```

**Mitigation**: The final result IS sorted (lines 323-331), but the intermediate processing order can affect deduplication logic.

### 2. Explorer Builder (`explorebuilder.go`)

```go
// Line 261: Iterating over reconcilers map
for id := range b.reconcilers {
    // Order affects initialization sequence
}

// Line 469: Iterating over strategies map
for name, constructor := range b.recorderInjectedStrategies {
    // Order affects which containers are built first
}
```

### 3. Resource Dependencies (`ResourceDeps`)

The `ResourceDeps` type is `map[string]Set[ReconcilerID]`, so iterations produce non-deterministic order.

## Why Back-to-Back Runs Are Consistent

When running multiple trials in quick succession:
- The Go runtime may reuse the same hash seed within a session
- Process memory layout may be similar
- This is NOT guaranteed—just observed behavior

## Implications

### For Correctness
- **Soundness is preserved**: All exploration paths are valid
- **Completeness varies**: Different runs may explore different subsets
- **Final states are correct**: Just reached via different paths

### For Benchmarking
- **Run multiple trials**: Single-run comparisons can be misleading
- **Use statistical analysis**: Report means and variance
- **Prefer back-to-back runs**: More stable for A/B comparisons

### For Debugging
- **Reproduce issues**: Set `GODEBUG=randmaphash=0` to disable map randomization (NOT recommended for production)
- **Log exploration order**: Enable verbose logging to trace path differences
- **Use deterministic seeds**: Consider sorting all map iterations

## Potential Fixes

### Option 1: Sort All Map Iterations (Conservative)

Find all `for k := range map` patterns and replace with sorted iteration:

```go
// Before (non-deterministic)
for k, v := range myMap {
    process(k, v)
}

// After (deterministic)
keys := make([]K, 0, len(myMap))
for k := range myMap {
    keys = append(keys, k)
}
sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
for _, k := range keys {
    process(k, myMap[k])
}
```

### Option 2: Use Ordered Maps

Replace `map[K]V` with an ordered container that maintains insertion order.

### Option 3: Accept Non-Determinism

Document that exploration is non-deterministic and design tests/benchmarks accordingly.

## Recommendation

For the exploration use case, **Option 3 (Accept Non-Determinism)** is recommended because:

1. **Correctness is maintained**: All paths are valid
2. **Performance matters**: Sorting adds overhead
3. **Different paths = good coverage**: Non-determinism can help find more bugs
4. **Benchmarks should average**: Multiple trials give better signal anyway

However, if reproducibility is critical (e.g., for regression tests), consider **Option 1** for key code paths like `getTriggered()`.
