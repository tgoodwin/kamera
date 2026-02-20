# Process-Isolated Parallel Supervisor Design (`kamera-iuz.4`)

**Date:** 2026-02-19  
**Epic:** `kamera-iuz` (Process-isolated parallel exploration mode)  
**Task:** `kamera-iuz.4` (Implement supervisor-child process orchestration)

## Problem

`simclock` uses package-level global state, so goroutine-parallel exploration in one process is unsafe for some harness/controller paths. We need process isolation without relying on `fork()`-style cloning.

## Goal

Add an optional process-based parallel path that:

- uses a supervisor process to fan out child `go run .` executions,
- executes one input index per child process,
- keeps outputs in existing inspector dump/stats formats,
- aggregates failures after all children complete.

## Scope

### In scope

- New process-mode flags and orchestration logic in `pkg/explore`.
- Supervisor/child mode selection.
- One-child-per-input-index execution model.
- Error capture via existing dump context attributes.
- Aggregate failure reporting and process-based progress logs.

### Out of scope

- New status artifact formats.
- File-system polling progress monitoring.
- New max-parallel CLI tuning flag.
- Changing harness `main.go` flow (existing batch `RunAll(...)` callsites should continue to work).
- Supporting implicit/default in-memory inputs in process mode.

## User-Facing Behavior

### New flag

- `--parallel-processes` (bool): enables process-isolated batch execution.

### Process mode preconditions

- `--parallel-processes` requires explicit `--inputs <file>`.
- Supervisor launch requires running from a harness module directory where `go run .` is valid.

### Internal child flag

- `--parallel-child-index` (int, default `-1`): internal selector used by supervisor.
- Not intended for normal manual use.

## Execution Model

Two modes exist when batch path calls `ParallelRunner.RunAll(...)`:

1. **Supervisor mode**
   - `--parallel-processes=true` and `--parallel-child-index < 0`.
   - Parent loads `--inputs` to determine input count.
   - Parent spawns one child per input index using `go run .`.
   - Parent uses process-based progress reporting (`completed/failed/total`).
   - Parent runs all children (up to default concurrency), then returns aggregate error if any failed.

2. **Child mode**
   - `--parallel-processes=true` and `--parallel-child-index >= 0`.
   - Child executes only the selected input index.
   - Child forces in-process scenario parallelism to 1.

## Input/Scenario Contract

Process mode enforces:

- exactly one input index per child, and
- exactly one produced scenario for that selected input.

If selected input expands to zero or multiple scenarios, child fails fast with a clear error.

This keeps index-to-artifact mapping deterministic and avoids hidden fanout inside child processes.

## Artifact Contract

No new file types are introduced.

- Dumps and stats continue using existing per-scenario naming conventions.
- Existing inspector dump JSON format remains the canonical artifact.

### Child success

- Writes normal dump/stats artifacts when configured.

### Child failure

Child should still attempt to write an inspector dump at expected path, even if exploration never starts:

- dump may contain `states: []`,
- failure details are recorded in `context.scenario.attributes`, e.g.:
  - `status=error`
  - `error_phase=<phase>`
  - `error_message=<trimmed message>`

Stderr remains available for immediate CLI diagnostics.

## Failure Policy

- Supervisor does **not** fail fast on first child failure.
- Supervisor waits for all children to complete.
- Final result is non-zero/error if any child failed.
- Final summary includes failed indices and brief per-child failure context (exit/error summary).

## Progress Reporting

Progress is process-based only:

- parent reports starts/completions/failures from child process lifecycle,
- no directory/file polling is required.

## Containment and Integration

Primary implementation location: `pkg/explore`.

- Add new flags in `pkg/explore/flags.go`.
- Add process orchestration/child-selection helpers in `pkg/explore`.
- Keep existing harness batch branches unchanged where they already call `RunAll(...)`.

Harnesses that cannot produce exactly one scenario for a selected input will fail fast in process mode (acceptable for this task).

## Testing Plan

- Unit tests for supervisor argument validation and mode selection.
- Unit tests for child index selection and single-scenario enforcement.
- Unit tests for aggregate failure behavior (run-all then fail).
- Unit tests for failure dump context attributes (`status`, `error_phase`, `error_message`).
- Unit test/assertion that child mode forces in-process parallelism to 1.

## Deferred Work (for `kamera-iuz.5`)

- Validation and docs polish for operational tradeoffs.
- Broader regression/integration coverage around simclock safety and result equivalence expectations.
- Optional future UX enhancements (e.g., explicit max-parallel setting for process mode).
