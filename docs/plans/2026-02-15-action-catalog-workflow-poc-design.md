# Action Catalog + Workflow POC Design

## Objective
Capture the current design direction for input generation and workflow coverage:
keep Kamera focused on perturbation/exploration, push domain-specific action/input
knowledge into project harnesses, and validate the approach first in Knative.

## Why This Update
We want to avoid over-investing in a general input-generator framework before a
proof of concept exists. Across current target projects (Knative, Crossplane,
Kratix, Karpenter), manual domain research + harness-local generation is faster
and better scoped for the research phase.

## Scope Decision
- **Kamera core owns:** perturbation profiles, scheduling/exploration, and
  state-space search.
- **Harness owns:** project-specific user-facing actions and concrete input
  generation.
- **Out of scope (for now):** a generic cross-project YAML/DSL input workflow
  interface.

## Source of Truth for Actions
Action catalogs should be derived from **public project documentation** and
mapped to user-facing CRD operations.

Process:
1. Identify user-facing CRDs and their documented operations.
2. Normalize docs language into concise action names for the harness.
3. Record which user-facing fields each action mutates.
4. Keep an evidence mapping (`action -> doc URL`) in design notes or code docs.

This gives a defensible basis for coverage intent and avoids inventing actions
that users cannot actually perform through the documented API surface.

## Workflow Semantics (Multi-Step)
For workflows with multiple user actions:
1. Apply step `N` by mutating only user-facing resources.
2. Run reconciliation to **full quiescence**.
3. Carry forward the **full reconciled state** into step `N+1`.

Important implications:
- Users do **not** need to author intermediate/non-user-facing resources.
- Supporting resources created by controllers are preserved automatically via
  state carryover.
- We avoid exposing intermediate-state details in the user action contract.

## Quiescence Policy
- Current decision: each step must settle to full quiescence before continuing.
- `partial settle` is intentionally deferred and can later be treated as an
  explicit perturbation mode.
- Failure handling policy on non-quiescence (abort workflow vs skip workflow)
  remains open.

## Knative POC Direction
Initial proof point is intentionally quick-and-dirty and Go-only in
`examples/knative-serving`.

Current state:
- `scenariosFromInputs(...)` now produces single-action variants from a baseline
  Knative `Service` input.
- Single-action mutations currently include:
  - image update
  - min-scale update
  - max-scale update
  - container concurrency update
- Parallel mode no longer depends exclusively on `--inputs`; `--parallel` can
  run with harness-provided default baseline input.

This is sufficient to validate the first "fuzz-ish" sweep over selected degrees
of freedom without introducing a generalized interface.

## Coverage Strategy Integration
This design complements `docs/design/coverage-strategy-v2.md`:
- Seeds remain user-facing.
- Action catalogs define *what operations exist* for each project.
- Workflows define *how operations are sequenced* to exercise more of the API
  than create-only scenarios.
- Perturbations remain orthogonal and are still owned by Kamera.

## Immediate Next Steps
1. Keep Knative in single-action mode until behavior is stable.
2. Add 2-step Knative workflows (e.g., create -> template update,
   create -> traffic split) using the same step/quiesce/carryover model.
3. Add a lightweight action evidence table for each project catalog.
4. Replicate the pattern in one additional harness (likely Karpenter) before
   designing any shared interface.

## Open Questions
- On step non-quiescence: abort current workflow or skip to next workflow?
- Minimal oracle set for workflow-level correctness beyond convergence and
  crashes (project-specific readiness assertions).
- How much of action-catalog evidence should live in code comments vs docs.
