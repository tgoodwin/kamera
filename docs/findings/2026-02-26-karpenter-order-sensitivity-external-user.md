# Finding: Karpenter Order Sensitivity Exposed by `External User` Pod Action

Date: 2026-02-26  
Scope: `examples/karpenter` harness behavior under user-action Pod introduction

## Summary

We observed a real order sensitivity in the Karpenter harness when the pending Pod
is introduced as an `External User` action.

- With the Pod fully introduced via user action (`CREATE`) and no startup seeding,
  exploration collapsed to a short flow (about 11-12 steps) and did not create a
  `Node`.
- The trigger manager currently emits pending reconcilers in lexicographic order.
  That implementation detail changed effective behavior: `provisioner` could run
  before Karpenter had ingested pod state via `state.pod` and
  `provisioner.trigger.pod`.
- The generic `PodLifecycleController` also acted as a confounder by consuming Pod
  changes before Karpenter-specific controllers in this scenario.

This is an important finding: controller ordering is not just a simulator
implementation detail; it can expose real sequencing assumptions in controller
interactions.

## Observed Evidence

### 1. Legacy/startup-seeded path (long flow, Node created)

When Pod state was startup-seeded, representative sequence included:

`state.pod -> provisioner.trigger.pod -> provisioner -> ... -> node.registrar -> state.node`

and produced a long reconciliation path with node provisioning/registration.

### 2. Pure user-action introduction (short flow, no Node)

When Pod was introduced as a pure `External User` create step:

`state.nodepool -> External User -> PodLifecycleController -> provisioner -> provisioner.trigger.pod -> state.pod -> ...`

`provisioner` became a no-op early and no `Node` was created in converged state.

### 3. Current harness stabilization

Current harness behavior that preserves expected Karpenter provisioning while still
showing `External User`:

- PodLifecycle disabled in Karpenter harness.
- Pod startup seeding retained for ordering stability.
- `External User` retained as a later replayed `UPDATE` action.

On `/tmp/karpenter-refactor-check-3/karpenter_default_base_0.jsonl`:

- Path length: `38`
- `External User` steps: `1`
- `PodLifecycleController` steps: `0`
- `Node CREATE` effects: `1`

## Root Cause Interpretation

Karpenter's effective provisioning pipeline in this harness depends on the relative
timing of:

1. Pod state ingestion (`state.pod`)
2. Pod trigger path (`provisioner.trigger.pod`)
3. Provisioner execution (`provisioner`)

If `provisioner` executes before upstream state/trigger steps have populated
cluster-local state and batching context, it can converge early and fail to create
capacity.

## Additional Finding: DFS + In-Memory State Interaction

Follow-up permutation experiments indicate a second effect beyond simple ordering:
branch-local replay state is isolated, but Karpenter's process-local controller
state is not branch-local in the harness.

### Experiment

Using `/tmp/karpenter-order-inputs-statepod.json` with:

- `permuteControllers=["state.pod"]`
- pruning/caching optimizations disabled

we still observed both outcomes from the same startup order:

- long paths (`len=38`, `len=51`) with `Node CREATE=1`
- short path (`len=8`) with `Node CREATE=0`

Critically, the first startup steps were identical in all three paths:

`state.pod -> provisioner.trigger.pod -> provisioner -> state.nodepool`

Yet step 2 (`provisioner`) diverged:

- long paths: `CREATE NodeClaim/default-00001`
- short path: no effects

This strongly suggests the divergence was not caused by startup pending order in
that run, but by mutable process-local state observed differently across DFS branch
execution.

### Why this can happen in current harness

1. Karpenter reconcilers are wired around singleton-like shared objects:
   - `state.Cluster` created once via `sync.Once`
   - `provisioning.Provisioner` created once via `sync.Once`
   - shared switching client + singleton ticker source
   (see `examples/karpenter/builder.go`).
2. DFS exploration reuses those reconciler instances across branches within one
   `Explore()` invocation; only API object snapshots are branch-isolated.
3. Initial permutation setup can enqueue both:
   - `initialStateVariants` from `expandStateByReconcileOrder(...)`
   - and `initialState` itself
   (see `pkg/tracecheck/explore.go`),
   and `expandStateByReconcileOrder` can return a variant identical to the
   original order when the permutable reconciler is already first
   (see `pkg/tracecheck/state.go`).

That combination can expose branch-order-dependent behavior even when startup
controller order appears "fixed" in path traces.

### Control

With `permuteControllers=null` (no order permutation), the same base scenario
converged to a single long path (`len=38`, `Node CREATE=1`) in the dump, which is
consistent with permutation machinery being required to trigger the branch-skew
effect above.

## Next Step: Permutation Experiment (Requested)

Goal: Validate whether reconciliation reaches the expected long flow *iff*
`state.pod` precedes `provisioner.trigger.pod`, and diverges when order flips.

### Experiment setup

1. Add a harness mode for "pure user create" to bypass Pod startup seeding and to
   keep the user action as `CREATE` (no `CREATE -> UPDATE` conversion).
2. Keep `PodLifecycleController` disabled during this experiment to remove that
   confounder.
3. Enable permutation on:
   - `state.pod`
   - `provisioner.trigger.pod`
   - `provisioner`
4. Disable pruning optimizations that can hide ordering branches
   (`orderingPruning`, `completedPathDedup`, `subtreeCompletion`, cache prediction)
   for exhaustive order exploration.

### Suggested analysis outputs

For each converged path, compute:

- first index of `state.pod`
- first index of `provisioner.trigger.pod`
- first index of `provisioner`
- whether `Node CREATE` appears
- path length

Then test these predicates:

- `state.pod < provisioner.trigger.pod` -> expected long flow / Node creation
- `provisioner.trigger.pod < state.pod` -> expected alternate behavior

### Why this matters

If confirmed, this would be strong evidence that Kamera's order permutation is
surfacing a real Karpenter sequencing dependency, not only a harness artifact.
