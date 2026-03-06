# Overnight Throughput Campaign Metrics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add invocation-scoped, phase-local campaign metrics to dumps so overnight runs can be evaluated by global novelty throughput without mixing unrelated runs.

**Architecture:** Reuse existing explore stats counters as the source of truth, propagate a generated `invocation_id` through scenario context attributes, and persist a new top-level `campaignMetrics` block in every phase dump. Keep v1 simple: raw counters only, one record per completed phase, grouping by invocation done later by a reporter command.

**Tech Stack:** Go, `pkg/explore`, `pkg/interactive`, `pkg/analysis`, Cobra CLI (`cmd/kamera` / `internal/kamera`), `go test`.

## Scope Decisions (Locked)

- Uniqueness scope: global per command invocation.
- Counter semantics: match existing explore stats (`UniqueNodeVisits`, `TotalNodeVisits`, `UniqueResourceStates`).
- Emission granularity: one metrics record per completed phase/run dump.
- Storage: augment dump payload (new top-level `campaignMetrics`), no separate metrics JSONL file.
- Aggregation safety: include `invocation_id` on all dump artifacts.
- Derivations: reporters compute rates/ratios; dumps store raw counters only.

## Beads Tracking

- Epic: `kamera-27m` - Overnight throughput campaign metrics v1
- Task 1: `kamera-27m.1` - Propagate invocation_id into all phase dumps
- Task 2: `kamera-27m.2` - Add top-level campaignMetrics block to dump schema
- Task 3: `kamera-27m.3` - Emit phase-local raw counters into campaignMetrics
- Task 4: `kamera-27m.4` - Add invocation-scoped campaign metrics reporter command

### Task 1: Invocation Identity Propagation

**Files:**
- Modify: `pkg/explore/runner.go`
- Modify: `pkg/explore/parallel_runner.go`
- Modify: `pkg/explore/scenario.go` (only if type additions are needed)
- Test: `pkg/explore/parallel_runner_test.go`

**Step 1: Write the failing test**

- Add a test that runs at least two phases (reference + rerun) and asserts each emitted dump has the same non-empty `invocation_id` in scenario attributes.

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/explore -run Invocation -count=1`
Expected: FAIL due to missing `invocation_id` plumbing.

**Step 3: Write minimal implementation**

- Generate an invocation ID once per command invocation in runner orchestration.
- Inject it into dump context attributes for every phase dump.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/explore -run Invocation -count=1`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/explore/runner.go pkg/explore/parallel_runner.go pkg/explore/parallel_runner_test.go
git commit -m "propagate invocation id into phase dumps"
```

### Task 2: Dump Schema Adds Top-Level campaignMetrics

**Files:**
- Modify: `pkg/analysis/types.go`
- Modify: `pkg/interactive/inspector_dump.go`
- Test: `pkg/analysis/dump_test.go`
- Test: `pkg/interactive/inspector_dump_test.go` (or closest dump writer tests)

**Step 1: Write the failing test**

- Add round-trip tests asserting dumps can serialize/deserialize a new top-level `campaignMetrics` block.

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/analysis ./pkg/interactive -run CampaignMetrics -count=1`
Expected: FAIL because schema/writer lacks the field.

**Step 3: Write minimal implementation**

- Add `CampaignMetrics` struct at dump top-level.
- Ensure write/read paths preserve it with no behavior changes when empty.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/analysis ./pkg/interactive -run CampaignMetrics -count=1`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/analysis/types.go pkg/interactive/inspector_dump.go pkg/analysis/dump_test.go pkg/interactive/inspector_dump_test.go
git commit -m "add campaign metrics block to dump schema"
```

### Task 3: Emit Phase-Local Raw Metrics into campaignMetrics

**Files:**
- Modify: `pkg/explore/parallel_runner.go`
- Modify: `pkg/explore/runner.go`
- Modify: `pkg/interactive/inspector_dump.go`
- Test: `pkg/explore/parallel_runner_test.go`

**Step 1: Write the failing test**

- Add tests asserting each completed phase dump includes raw counters:
  - `uniqueNodeVisits`
  - `totalNodeVisits`
  - `uniqueResourceStates`
  - `durationNs` (or equivalent raw duration)

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/explore -run CampaignMetrics -count=1`
Expected: FAIL due to missing metrics emission.

**Step 3: Write minimal implementation**

- Populate `campaignMetrics` from `ExploreStats` at phase completion.
- Keep raw counters only; do not add rates/ratios in dump.

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/explore -run CampaignMetrics -count=1`
Expected: PASS.

**Step 5: Commit**

```bash
git add pkg/explore/parallel_runner.go pkg/explore/runner.go pkg/interactive/inspector_dump.go pkg/explore/parallel_runner_test.go
git commit -m "emit phase-local campaign metrics in dumps"
```

### Task 4: Reporter Command (Post-MVP but Tracked Now)

**Files:**
- Create/Modify: `internal/kamera/*` (new reporter command wiring)
- Modify: `cmd/kamera/main.go` (if needed by command wiring conventions)
- Test: `internal/kamera/*_test.go`

**Step 1: Write failing tests**

- Given a dump directory, group by `invocation_id` and report summed counters per invocation.
- Ensure unrelated invocation IDs are never merged.

**Step 2: Run tests to verify failure**

Run: `go test ./internal/kamera -run CampaignMetrics -count=1`
Expected: FAIL before implementation.

**Step 3: Implement minimal command**

- Add CLI command to scan dumps and print per-invocation aggregates.
- Keep output text-only v1.

**Step 4: Run tests**

Run: `go test ./internal/kamera -run CampaignMetrics -count=1`
Expected: PASS.

**Step 5: Commit**

```bash
git add internal/kamera
git commit -m "add campaign metrics reporter by invocation"
```

## Verification Gate (Before broader rollout)

Run:

```bash
go test ./pkg/explore ./pkg/analysis ./pkg/interactive ./internal/kamera -count=1
```

Expected:
- dump schema remains backward compatible.
- per-phase dumps carry `invocation_id` + `campaignMetrics` raw counters.
- reporter can safely aggregate by invocation without mixing runs.
