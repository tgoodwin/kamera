# Process-Parallel Mode and Scenario Serialization Design

## Objective
Document the current `--parallel-mode=process` approach in Knative, explain its
tradeoffs, and define a path to a serialized scenario manifest model.

## Problem Context
`simclock` uses package-global process state. Running multiple scenarios in a
single process (goroutines) can couple clock/ticker state across runs and create
non-deterministic interference. A process-isolated execution mode avoids this by
giving each scenario its own process-level globals.

## Current Implementation (as of 2026-02-18)
Location:
- `examples/knative-serving/main.go`
- `examples/knative-serving/parallel_mode.go`

Public flags:
- `--parallel`
- `--parallel-mode=goroutine|process` (default `goroutine`)
- `--parallel-workers=<n>` (0 => `GOMAXPROCS`)

Internal flags (parent -> child):
- `--parallel-child=true`
- `--scenario-index=<idx>`

Execution flow:
1. Parent resolves batch inputs (`--inputs` or default baseline when
   `--parallel` is enabled).
2. Parent expands inputs to scenarios (`scenariosFromInputs`).
3. If mode is `goroutine`, run existing `ParallelRunner`.
4. If mode is `process`, parent spawns child processes (`os/exec`) and passes
   original CLI args plus child overrides.
5. Child re-runs input loading + scenario expansion, selects one scenario by
   `--scenario-index`, and executes only that scenario.

Why this works:
- Each scenario runs in a separate OS process, so global `simclock` state is
  isolated without a large `simclock` refactor.
- The parent still controls overall parallelism and failure propagation.

## Tradeoffs in Current Approach

### Pros
- Minimal invasive change to existing runner code.
- Strong process-level isolation for globals (`simclock` and similar).
- Cross-platform behavior via `os/exec` (no fork-only assumptions).

### Cons
- Parent and child both expand scenarios. This duplicates work and increases
  startup cost for larger scenario sets.
- Correctness depends on deterministic re-expansion:
  same args + same seed must produce identical scenario ordering and count.
- Child selection by index is opaque and hard to inspect/debug after the fact.
- Output naming can drift from parent indexing semantics because child runs one
  local scenario at index `0`.
- No stable artifact that records "exactly what scenarios were executed".

### Risk Areas
- Any future nondeterminism in scenario generation breaks index-based mapping.
- Future harnesses with non-serializable closures/invariants in `Scenario` need
  a clearer boundary between "scenario spec" and runtime-only fields.

## Proposed Direction: Serialized Scenario Manifest

### Design Goal
Make process-mode selection explicit and reproducible by serializing expanded
scenario specs once in the parent, then having children consume that artifact.

### Core Idea
Introduce a manifest file produced by the parent:
- Parent does expansion once.
- Parent writes `N` scenario specs to disk with stable ordering.
- Child receives `--scenario-manifest=<path>` and `--scenario-index=<idx>`.
- Child loads exactly one manifest entry and executes it.

### Manifest Scope
Manifest should encode data needed to reconstruct a runnable scenario without
re-running high-level expansion logic.

For Knative, include per scenario:
- `name`
- expanded `coverage.Input` (objects + pending)
- resolved explore config/tuning values
- optional context metadata (workflow, input ref, attributes)

Do not serialize:
- function pointers/closures (`Scenario.Invariant`)
- runtime-only handles (`VersionManager`, stats pointers)

### Example Shape (illustrative)
```json
{
  "version": "v1",
  "harness": "knative-serving",
  "scenarios": [
    {
      "index": 0,
      "name": "knative-default/base",
      "input": { "...": "expanded coverage.Input" },
      "config": { "...": "tracecheck.ExploreConfig" },
      "context": { "...": "optional scenario context" }
    }
  ]
}
```

## Benefits of Manifest Model
- Eliminates parent/child scenario expansion drift.
- Makes executed scenario set explicit, inspectable, and replayable.
- Enables retrying individual scenarios by index from a persisted artifact.
- Provides a foundation for future distributed workers (local or remote).

## Costs and Constraints
- Need manifest schema versioning and backward compatibility policy.
- Large scenario inputs can produce large files (consider JSONL or compression).
- Temporary file lifecycle/cleanup must be explicit.
- Requires harness-specific adapters where scenario construction is custom.

## Incremental Adoption Plan
1. Keep existing index-recompute path as fallback.
2. Add manifest write/read path behind process mode.
3. Prefer manifest path by default in process mode once validated.
4. Remove recompute path only after proving parity across representative runs.

## Open Questions
- Where should manifest files live by default (`/tmp`, dump dir sibling, or
  explicit user path)?
- Should we retain manifests automatically for debugging or clean them by
  default?
- Should parent aggregate per-child status into a summary report artifact?
- Is a shared generic manifest format needed across harnesses now, or should we
  start harness-local and converge later?
