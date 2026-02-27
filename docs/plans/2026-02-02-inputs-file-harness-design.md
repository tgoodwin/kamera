# Inputs File Harness Design

## Objective
Standardize a simple way for project-specific harnesses to consume a JSON file of
`coverage.Input` values and run all scenarios via the `pkg/explore` parallel
runner, without making harnesses part of the Kamera CLI.

## Scope
- **In:** A shared `--inputs` flag, harness-side loading of inputs, and parallel
  execution in batch mode.
- **Out:** Converting `coverage.Input` to `explore.Scenario` (planned later) and
  any Kamera CLI dispatcher that shells out to harnesses.

## Proposed UX
Harnesses keep their current single-scenario behavior, but add a batch mode:

- `--inputs <path>`: JSON file containing an array of `coverage.Input`.
- `--dump-output <dir>`: directory for per-scenario dumps when `--inputs` is set.
- `--emit-stats`: includes top-level stats in each dump file.
- `--interactive`: ignored/disabled in batch mode.

Example:

```
go run ./examples/knative-serving --inputs ./inputs.json --dump-output ./dumps --emit-stats
```

## Data Flow
1. Harness calls `flag.Parse()` and builds the `ExplorerBuilder` as today.
2. If `--explore-config` is provided, load it as the base config.
3. If `--inputs` is empty: run the existing single-scenario path.
4. If `--inputs` is set:
   - Load inputs from disk (new helper, e.g. `coverage.LoadInputs(path)`).
   - Convert each `coverage.Input` to an `explore.Scenario`.
   - Run all scenarios with `explore.NewParallelRunner(builder)` and
     `RunAll(...)`.

The conversion from `coverage.Input` to `explore.Scenario` is a separate slice
of work and will be plugged into step 4 when available.

## Parallel Runner Behavior
- Use `ParallelRunner` for all inputs, preserving input order in results.
- Treat `--dump-output` as a **directory** in batch mode.
- Force `interactive=false` (or error if explicitly set) since parallel runs
  do not surface the inspector UI.

## Error Handling
- If `--inputs` is set but the file cannot be read/decoded: return an error.
- If the decoded inputs list is empty: return an error.
- If `--dump-output` points to a file: return an error that
  requests a directory path.

## Testing
- Unit tests for `coverage.LoadInputs` (valid file, malformed JSON, empty list).
- Unit tests for the `Input -> Scenario` conversion once implemented.
- Optional harness smoke test that exercises batch mode with a tiny inputs file.

## Rollout
- Add `--inputs` to `pkg/explore/flags.go` so all harnesses share the flag.
- Add a small, reusable loader in `pkg/coverage` for the inputs file.
- Update a single harness first (e.g., knative-serving) to validate the flow.
- Apply the same pattern to other harnesses.
