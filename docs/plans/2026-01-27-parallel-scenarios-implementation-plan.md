# Parallel Scenario Runner Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `Scenario` type and parallel scenario runner with explicit state isolation to support per-scenario exploration configs.

**Architecture:** Introduce `Scenario`/`ScenarioResult` in `pkg/explore`, add a `ParallelRunner` that forks a base `ExplorerBuilder` per scenario, and refactor `ExplorerBuilder` with a `Fork()` API that deep-copies config/registries while injecting fresh stores and emitters.

**Tech Stack:** Go 1.24+, `pkg/explore`, `pkg/tracecheck`, `pkg/event`, `pkg/snapshot`.

---

### Task 1: Add builder-fork tests (state isolation)

**Files:**
- Create: `pkg/tracecheck/explorebuilder_fork_test.go`

**Step 1: Write the failing test**

```go
func TestExplorerBuilderForkIsolatesStoresAndConfig(t *testing.T) {
    scheme := runtime.NewScheme()
    b := NewExplorerBuilder(scheme)
    b.WithMaxDepth(5)

    fork := b.Fork()

    if b.snapStore == fork.snapStore {
        t.Fatalf("expected fork to have a fresh snapshot store")
    }
    if b.emitter == fork.emitter {
        t.Fatalf("expected fork to have a fresh emitter")
    }

    fork.config.MaxDepth = 99
    if b.config.MaxDepth == 99 {
        t.Fatalf("expected config to be cloned")
    }

    fork.config.PermuteOrder[ReconcilerID("X")] = true
    if b.config.PermuteOrder[ReconcilerID("X")] {
        t.Fatalf("expected permute map to be cloned")
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/tracecheck -run TestExplorerBuilderForkIsolatesStoresAndConfig`
Expected: FAIL with “Fork undefined” or nil pointer for fork.

**Step 3: Write minimal implementation**

Implement `Fork()` on `ExplorerBuilder` returning a new builder with cloned config and fresh store/emitter.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/tracecheck -run TestExplorerBuilderForkIsolatesStoresAndConfig`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/tracecheck/explorebuilder.go pkg/tracecheck/explorebuilder_fork_test.go
git commit -m "add explorer builder fork for isolated runs"
```

---

### Task 2: Define Scenario types

**Files:**
- Create: `pkg/explore/scenario.go`

**Step 1: Write the failing test**

```go
func TestScenarioResultFields(t *testing.T) {
    scenario := Scenario{Name: "example"}
    result := ScenarioResult{Name: scenario.Name}
    if result.Name != "example" {
        t.Fatalf("expected scenario name to propagate")
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/explore -run TestScenarioResultFields`
Expected: FAIL with “Scenario undefined”.

**Step 3: Write minimal implementation**

```go
type Scenario struct {
    Name         string
    InitialState tracecheck.StateNode
    Config       tracecheck.ExploreConfig
    Invariant    func(tracecheck.StateNode) error
}

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

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/explore -run TestScenarioResultFields`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/explore/scenario.go pkg/explore/scenario_test.go
git commit -m "add explore scenario types"
```

---

### Task 3: Parallel runner tests (config isolation, dumps, invariants)

**Files:**
- Create: `pkg/explore/parallel_runner_test.go`
- Modify (if needed): `pkg/test/integration/controller` or add minimal test reconciler in `pkg/explore`

**Step 1: Write failing tests**

Test cases:
- `TestParallelRunnerDoesNotLeakConfig`: two scenarios with different `MaxDepth`, ensure the builder config doesn’t leak between runs (e.g., assert stats or recorded config used).
- `TestParallelRunnerWritesDump`: run with `DumpDir`, ensure dump file exists with sanitized name.
- `TestParallelRunnerCapturesInvariantError`: create an invariant that fails and assert `InvariantError` is set.

**Step 2: Run tests to verify failure**

Run: `go test ./pkg/explore -run TestParallelRunner`
Expected: FAIL with “ParallelRunner undefined”.

**Step 3: Implement minimal runner scaffolding (no concurrency yet)**

Add `ParallelRunner` and `RunAll` that runs scenarios sequentially but returns results; tests should still fail on missing behavior (dump/invariant/concurrency).

**Step 4: Run tests and iterate until failures point to missing features**

Run: `go test ./pkg/explore -run TestParallelRunner`
Expected: FAIL with assertions on missing dump/invariant behavior.

---

### Task 4: Implement ParallelRunner with concurrency and dumping

**Files:**
- Create: `pkg/explore/parallel_runner.go`
- Modify: `pkg/explore/runner.go` (share helper functions if needed)

**Step 1: Implement worker pool**

- Use `MaxParallel` (default `GOMAXPROCS` when <= 0).
- Run scenarios via `Fork()` of builder and `Explore` per scenario.

**Step 2: Add dump + stats helpers**

- Sanitize scenario names for filenames.
- Use `interactive.SaveInspectorDump` for dumps.
- Write stats JSON (mirror `dumpStatsIfRequested`).

**Step 3: Implement invariant evaluation**

- Evaluate invariant against each converged state.
- Record first error in `ScenarioResult.InvariantError`.

**Step 4: Run tests**

Run: `go test ./pkg/explore -run TestParallelRunner`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/explore/parallel_runner.go pkg/explore/runner.go pkg/explore/parallel_runner_test.go
git commit -m "add parallel scenario runner"
```

---

### Task 5: Full test pass

**Step 1: Run focused tests**

Run: `go test ./pkg/tracecheck ./pkg/explore`
Expected: PASS.

**Step 2: Commit if needed**

If any fixes required:

```bash
git add pkg/tracecheck pkg/explore
git commit -m "fix parallel scenario runner tests"
```

