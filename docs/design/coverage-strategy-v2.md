# Coverage Strategy v2: Execution-Guided Input Generation

## Motivation

The goal is to design an input generation strategy for the Kamera simulator that sufficiently covers
the input space for a given Kubernetes control plane project (Knative, Crossplane, Karpenter, Kratix,
etc.). These projects use multiple controllers and resources to achieve platform functionalities, and
bugs often manifest as race conditions, stale reads, or non-convergence in controller interactions.

An earlier design (`coverage-strategy.md`) proposed a fully static pipeline: analyze a dependency
graph to find structurally risky interaction patterns, then fabricate initial state (objects + pending
reconciles) targeting those patterns and launch parallel explorations. Implementation work on
that static-only approach revealed two
fundamental challenges when trying to generate interesting inputs *before* any execution happens:

**Fabricating valid intermediate objects.** Many resources in a control plane are not user-facing.
They are created by controllers in response to upstream inputs (e.g., Knative `Revision` is created
by `ConfigurationReconciler`). Fabricating valid instances requires replicating controller behavior --
the very thing we are trying to test.

**Fabricating valid event histories for staleness.** Staleness injection means showing a controller
an older version of a resource. For that to be meaningful, there must be a real history of mutations.
Pre-computing intermediate versions requires understanding execution dynamics that don't exist yet.

Both challenges point to the same root issue: many aspects of what makes an input interesting (valid
intermediate state, real mutation histories, conditional code paths) can only be determined by
observing the system actually executing.

## Strategy: Closed-Loop, Execution-Guided

The strategy separates three concerns:

1. **What to simulate** (seeds): user-facing resources, fuzzed over their CRD schemas
2. **What to perturb** (perturbation profiles): which controllers to reorder, which reads to make
   stale -- inferred statically from patterns in the dependency graph
3. **When to perturb** (checkpoints): intermediate states during execution where perturbation is
   meaningful -- derived from a reference execution trace

### Pipeline Overview

```
User-facing CRD schemas --> Seed Fuzzing --> [Seed_1, Seed_2, ...]
Static Dep Graph --> Infer PerturbationProfiles --> [Profile_1, Profile_2, ...]

For each seed:
  Phase 1 (Baseline):
    Run deterministic execution (no perturbations) --> reference trace

  Phase 2 (Checkpoint Scan):
    Analyze reference trace against profiles --> [(checkpoint, profile, context), ...]

  Phase 3 (Branch):
    For each (checkpoint, profile, context):
      Resume exploration from checkpoint state with perturbation applied
```

## Component Design

### Seeds (User-Facing Resources Only)

Seeds are instances of user-facing CRDs: `Service`, `Composition`, `NodePool`, `Promise`, etc.
These are the actual API surface that users interact with. Their schemas are well-documented and tractable to fuzz.

Intermediate resources (`Revision`, `KPA`, `Deployment`, `FunctionRevision`, etc.) are NOT included in seeds. They are produced naturally by controller execution during the baseline phase.

Seed generation uses the strategies described in `input-generation.md` (code-first, file-based,
or schema inference), scoped to user-facing types only.

Fuzzing over seeds covers two dimensions:

**Spec variation** (config values within a single resource):
- Enumeration exhaustion (e.g., `protocol: HTTP | GRPC`)
- Boundary values (e.g., `replicas: 0 | 1 | MaxInt`)
- Required vs optional field presence

**Structural variation** (shape of the input graph):
- Cardinality: 1 vs N instances of the same resource type (e.g., 1 Service vs 3 Services)
- Composition: which combinations of user-facing resources appear together
- Overlap: resources with shared selectors, names, or cross-references

Structural variation matters because some bugs only manifest when multiple instances compete for
shared downstream resources (e.g., two Services whose Routes converge on the same Ingress). The
dependency graph can inform which structural configurations are interesting -- for example, if two
user-facing resources both trigger reconcile chains that write to the same downstream type, testing
with multiple instances of those resources is more likely to expose contention.

Determining the full set of valid user-facing input flows (which resources can be created
independently, which must be co-created, what structural combinations are meaningful) requires
further design work. This should account for the dependency graph topology, CRD validation
constraints, and project-specific semantics. This is deferred as a follow-up design task.

### Perturbation Profiles (Statically Inferred)

A perturbation profile captures *what kind* of perturbation to apply, expressed in terms of
controllers and resource types. Profiles are inferred from structural patterns in the static
dependency graph (the bipartite controller-resource graph described in `dependency-analysis.md`).

```
type PerturbationProfile struct {
    // Diagnostics
    PatternType  string   // multi_writer, missing_trigger, diamond, reducer, feedback_cycle
    Controllers  []string // controllers involved in the pattern
    Resources    []string // resource types (canonical GVK) involved

    // Ordering perturbation
    Permute      []string // controller IDs whose scheduling order to permute

    // Staleness perturbation
    StaleReads   map[string][]string // controller -> resource types to inject staleness on

    // Budget
    MaxDepth     int // depth limit for branched exploration
}
```

#### Graph patterns and their profiles

**Multi-Writer Contention.** Two controllers write the same resource. Risk: thrashing,
last-writer-wins. Profile: permute the writer controllers.

**Missing Trigger / Stale Read.** A controller reads a resource but does not watch it, and another
controller writes that resource. Risk: stale configs, missed reconciles. Profile: inject staleness
when the reader reads that resource type.

**Diamond Pattern.** One upstream resource triggers multiple controllers whose effects converge on
a shared downstream resource. Risk: ordering sensitivity, race conditions. Profile: permute the
converging controllers.

**Reducer Controller.** A controller reads many resources to compute one output. Risk: partial
updates, inconsistent snapshots. Profile: inject staleness on the input resources.

**Feedback Cycle.** Writes feed back into triggers in a cycle. Risk: oscillation, infinite
reconciles. Profile: permute cycle controllers, increase depth budget.

| Pattern | Permute | StaleReads | MaxDepth |
|---------|---------|------------|----------|
| Multi-Writer | writer controllers | -- | default |
| Missing Trigger | -- | reader -> [resource] | default |
| Diamond | converging controllers | -- | default |
| Reducer | -- | reducer -> [input resources] | default |
| Feedback Cycle | cycle controllers | -- | increased |

### Reference Execution (Baseline Phase)

For each seed, run a simulated execution with no perturbation. This produces a reference trace: a sequence of reconcile steps, each recording:

- Which controller ran, on which key
- The state snapshot the controller observed (read set)
- The effects produced (creates, updates, deletes)
- New pending reconciles generated (triggers)

It produces:
1. **Valid intermediate objects** created by real controller logic
2. **A real mutation history** for every resource
3. **The set of controllers that actually ran** (confirming which profiles are relevant)
4. **A sequence of intermediate states** from which checkpoints can be selected

### Checkpoint Scanner (Trace Analysis)

A trace analysis pass analyzes the reference trace to identify points in the execution where a
perturbation profile could be applied (a "checkpoint"). The high-level intent:

**For ordering profiles**: identify states where the controllers targeted for permutation are
simultaneously pending. These are the fork points where scheduling order matters -- in the
deterministic baseline, one ran first; at the checkpoint, we branch and explore alternative
orderings.

**For staleness profiles**: identify states where a controller is about to reconcile and reads a
resource that has been mutated during the execution. The reference trace provides the concrete
prior versions of that resource, giving us real stale values to inject rather than fabricated ones.

A single perturbation profile may produce **multiple checkpoints** in one trace (e.g., two controllers are
co-pending at several points during execution). Each checkpoint is an independent branching
opportunity.

The exact mechanisms of checkpoint identification are left to implementation. The
important property is that checkpoint selection is informed by both the static perturbation profile (which
controllers/resources to look at) and the dynamic trace (when those interactions actually occur).

### Branched Exploration (Checkpoint-and-Branch)

At each identified checkpoint, resume exploration from the intermediate state with the profile's
perturbation applied:

- **Ordering**: enable permutation for the profiled controllers from this state forward
- **Staleness**: configure stale read injection using the concrete prior versions identified
  by the checkpoint scanner

This uses the existing subtree exploration mechanism (restarting DFS from an intermediate
`StateNode`). Each (checkpoint, profile) pair becomes an independent exploration that can run
in parallel.

## Oracle & Failure Classification

Instead of writing manual assertions for every scenario, we define properties of the system
execution that indicate failure.

**Convergence Failure (Liveness Bug).** `AbortedStates > 0` with `Reason: MaxDepth`. The system
entered a loop or dead-end retry cycle that exceeded the step budget.

**Nondeterminism (Race Condition).** `len(ConvergedStates) > 1`. The final state depends on
controller scheduling order. Kubernetes controllers must be eventually consistent; multiple
converged states violate this contract.

**Crash.** `AbortedStates > 0` with `Reason: Panic` or `Error`. Unhandled nil pointer, type
assertion failure, or explicit error that stops reconciliation.

**Stuck State (Logic Error).** `len(ConvergedStates) == 1` and `PendingReconciles` is empty, but
some expected goal is unmet. Requires a domain-specific assertion attached to the scenario.

## Open Questions

1. **Seed scope**: How do we determine which GVKs are user-facing vs non-user-facing?
   Use explicit resource role metadata from the dependency graph contract
   (`docs/design/dependency-graph-contract.md`), not runtime heuristics over edges.

2. **Seed flow enumeration**: Determining the full set of valid user-facing input flows -- which
   resources can be created independently, which must be co-created, what structural combinations
   are meaningful -- requires a dedicated design effort informed by the dependency graph, CRD
   validation constraints, and project-specific semantics.

3. **Mutation scenarios**: Testing "what happens when the system is in steady state and then a
   resource is updated/deleted" requires a two-phase seed: converge first, then apply a mutation.
   This is a natural extension of the baseline phase but needs explicit design.

4. **Checkpoint scanner predicates**: The exact conditions for identifying interesting checkpoints
   (especially for staleness) need to be worked out during implementation. The intent is clear;
   the edge cases are not.

5. **Profile relevance**: Not every profile is relevant to every seed. A profile about controllers
   that never run for a given seed is wasted work. The baseline phase naturally reveals which
   controllers ran, providing a simple filter.
