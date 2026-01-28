# Hotspot Detection Rules (Static Graph)

## Objective
Define concrete detection rules to find hotspot instances in the static dependency
graph described in `docs/design/coverage-strategy.md`. This document focuses on
graph predicates only (not performance or indexes). Hotspots are *overlapping*;
a subgraph can match multiple categories.

## Inputs and Notation
We operate over the static bipartite graph:

- Controllers: `C`
- Resources (GVK only): `R`
- Edge kinds: `reconciles`, `watches`, `reads`, `writes`, `owns`

Helper sets (conceptual):

- `Triggers(C)` = { R | C -> R with edge kind `reconciles` or `watches` }
- `Reads(C)` = { R | C -> R with edge kind `reads` }
- `Writes(C)` = { R | C -> R with edge kind `writes` }
- `Writers(R)` = { C | C -> R with edge kind `writes` }
- `Owners(Rchild)` = { Rparent | Rparent -> Rchild with edge kind `owns` }

Target refinements (optional):
- For `reads`/`writes`, you may filter by `target: spec|status`.
- When targets match, contention risk is higher.

## Hotspot Detection Rules

### 1) Multi-Writer Contention
**Predicate:**
- Exists resource `R` with `|Writers(R)| >= 2`.

**Optional refinements:**
- Only count writers to the same `target` (spec vs status).
- Escalate if any of the writers also reads `R`.

**Output instance:**
- `{resource: R, writers: [C1, C2, ...], target: spec|status|any}`

### 2) Missing Trigger / Stale Read
**Predicate:**
- Exists controller `C` and resource `R` where:
  - `R ∈ Reads(C)`
  - `R ∉ Triggers(C)`

**Escalation:**
- If `Writers(R)` includes some `C2` (other than `C`), mark as high risk.
- Additionally, flag read-after-write dependencies: if there exists `C2` such that
  `C2 -> R` is a `writes` edge and `C -> R` is a `reads` edge, record the
  dependency even if `C` *does* watch `R`. This captures ordering sensitivity when
  write and read paths overlap.

**Output instance:**
- `{controller: C, resource: R, writers: [C2...], is_missing_trigger: bool}`

### 3) Fan-out with Converging Writes (Order Sensitivity)
**Predicate (direct):**
- There exists resource `Rstart` and controllers `C1 != C2` such that:
  - `Rstart ∈ Triggers(C1)` and `Rstart ∈ Triggers(C2)`
  - `Writes(C1)` and `Writes(C2)` intersect on some `Rend`

**Predicate (indirect via ownership):**
- `C1` writes `Rchild1`, `C2` writes `Rchild2`, and both are owned by the same
  `Rparent` (using `owns` edges). This is a looser form of convergence.

**Output instance:**
- `{trigger: Rstart, controllers: [C1, C2], converges_on: Rend}`

### 4) Aggregation / Join Controller
**Predicate:**
- There exists controller `C` where:
  - `|Reads(C)| >= 2`
  - `Writes(C)` is non-empty

**Optional refinements:**
- Require reads to be distinct from outputs.
- Prefer cases where read targets include `status` (snapshot risk).

**Output instance:**
- `{controller: C, inputs: [R1, R2...], outputs: [Rout...]}`

### 5) Feedback Cycle
**Predicate:**
- Build a derived digraph with:
  - `C -> R` for `writes` edges
  - `R -> C` for `watches` and `reconciles` edges
- If any directed cycle exists, report the cycle as a feedback hotspot.

**Output instance:**
- `{cycle: [C1, R1, C2, R2, ...]}`

## Output Conventions
Each detector should return a list of instances with enough detail to map back
into test generation (controllers, resources, and relevant edges). A single
subgraph can be labeled with multiple hotspot classes.

## Static vs Dynamic Signals
Some bug triggers (e.g., transient states or edge-triggered logic) cannot be
inferred from the static graph alone. The static rules above identify *risk
regions* (e.g., read-after-write dependencies, feedback cycles) that become
actionable during exploration when we permute reconcile order or inject
staleness.
