---
name: scenario-design
description: Design Kamera workflow scenarios that target specific bug patterns in Kubernetes controllers. Use this when you have a control plane to test and need to produce perturbation-aware workflow inputs (environment state, user actions, staleness/ordering tuning) that maximize the chance of surfacing real bugs.
metadata:
  short-description: Design perturbation-aware test scenarios from controller analysis
---

# Scenario Design Skill

Use this skill to design Kamera workflow scenarios that target specific bug
patterns in Kubernetes controller systems. The output is a set of workflow JSON
inputs, each constructed backward from a concrete vulnerability hypothesis.

## Prior Findings

Before designing new scenarios, check whether the project has an existing
analysis file at `examples/<project>/.agents/ANALYSIS.md`. This file records
what scenarios have already been explored and what they found.

**If the file exists**, read it and reason about:

- Which scenarios surfaced confirmed bugs — these directions are productive and
  worth extending (e.g., testing edge cases, related code paths, combining with
  other findings)
- Which scenarios found nothing — understand *why* (wrong environment state,
  scenario design gap, the hypothesized bug doesn't exist) before concluding
  the area is exhausted
- Which bug patterns appear repeatedly across scenarios — this signals systemic
  issues in the controller design worth probing from multiple angles
- Which areas of the controller have not yet been exercised at all

Use this to avoid redundant scenarios and to identify unexplored angles. Do not
look for explicit "do not re-explore" markers — read the findings and draw your
own conclusions about what's left.

**If the file does not exist**, the project is in early exploration. Skip this
step and proceed directly to Phase 1.

## Prerequisite

You need one of:

- A dependency graph artifact (`dependency-graph.json`) from the
  `dependency-analysis` skill
- Direct access to the controller source code

The dependency graph gives you the topology (which controllers read/write which
resources). Source code gives you the decision logic (what assumptions a
controller makes about the data it reads). Both are needed for high-quality
scenarios. If you only have the graph, you can still produce scenarios, but they
will target structural patterns rather than specific code-level vulnerabilities.

If `dependency-graph.json` does not already exist for the project, invoke the
`dependency-analysis` skill to produce it before Phase 1. If source code access
is available but no graph exists and running the skill is not practical, proceed
directly from source — but note that scenario quality will be lower without the
validated topology.

## Methodology

The process has five phases. Each phase produces artifacts that feed the next.

### Phase 1: Read/Write Signature Extraction

For each controller in scope, catalog every API interaction within its
`Reconcile()` method and transitive callees.

**What to extract:**

| Operation | What to record |
|-----------|---------------|
| `client.Get(kind)` | Resource kind, which field/condition is inspected from the result |
| `client.List(kind)` | Resource kind, what label/field selectors are applied, what filter/sort runs on results |
| `client.Create(kind)` | Resource kind, what triggers the create (always? conditional?) |
| `client.Update(kind)` | Resource kind, is it conditional on a diff check or unconditional? |
| `client.Status().Update(kind)` | Resource kind, same conditionality question |
| `client.Delete(kind)` | Resource kind, under what conditions |

**Output format** (one table per controller):

```
Controller: FooReconciler
Primary: group/Kind

Reads:
  - Get(group/Kind) → uses .spec.fieldX to decide Y
  - List(group/OtherKind) → filters by label "foo", picks latest by .spec.revision

Writes:
  - Create(group/OtherKind) → conditional on hash mismatch
  - Status().Update(group/Kind) → unconditional (BUG SMELL)

Watches:
  - Owns(group/OtherKind) → enqueues owner on change
  - Watches(group/ThirdKind) → custom mapper enqueues related Foos
```

**Unconditional writes are a signal.** If a controller calls `Update` or
`Status().Update` without first checking whether the data actually changed
(e.g., `DeepEqual` on old vs new), flag it. This is the most common source of
nonconvergence bugs -- the write triggers watch events that re-enqueue
reconciles, creating an infinite loop masked by rate limiting in production.

### Phase 2: Cross-Controller Data Flow Graph

From the read/write signatures, identify chains where one controller's write
is another controller's read input.

```
ControllerA writes Resource X
  → ControllerB reads Resource X, uses field F to decide action
    → ControllerB writes Resource Y
      → ControllerC reads Resource Y ...
```

These chains are **trust chains**: each downstream controller trusts that the
upstream controller's output is correct and current.

**What to record for each chain link:**

1. The writing controller and what it writes
2. The reading controller and what assumption it makes about the data
3. The temporal gap: is the reader triggered by the write (watch), or does it
   read opportunistically (Get/List without a watch)?

Trust chains with no watch trigger on the intermediate resource are the highest
priority for staleness injection -- the reader has no mechanism to be notified
when the data changes.

### Phase 3: Vulnerability Window Identification

For each read in Phase 1, ask two questions:

1. **What assumption does the reader make about the data?**
   - "ValidPipeline condition is True means the pipeline is actually valid"
   - "The latest revision in the list belongs to the Composition I just fetched"
   - "The object I just Got has not been deleted"

2. **Under what conditions is that assumption violated?**
   - "FunctionRevision capabilities changed after validation"
   - "Composition was updated between my Get and my List"
   - "Object was marked for deletion between my Get and my status update"

Each (assumption, violation condition) pair is a **vulnerability window**. These
are the raw material for scenario construction.

**Decision-point detail matters.** The vulnerability isn't "stale
CompositionRevision" in the abstract -- it's specifically that function X at
line Y calls `LatestRevision()` which filters by `IsControlledBy()`, and if
the Composition UID in the filter is stale, the filter produces zero results.
Record the specific code location and branching logic.

### Phase 4: Code Path Branch Enumeration

Controllers have branching logic that creates different read patterns. Each
significant branch is a separate "flow" with its own vulnerability surface.

**Common branch dimensions in Kubernetes controllers:**

- Policy fields (`compositionUpdatePolicy: Manual vs Automatic`)
- Deletion handling (`meta.WasDeleted` → finalizer removal path)
- First-run vs steady-state (object has no status yet vs already reconciled)
- Error recovery (previous reconcile set an error condition)
- Ownership (object has ownerReference vs orphaned)

For each branch:
- What reads does this branch perform?
- What reads does it skip?
- Does the branch have different staleness exposure than the default path?

Branches that skip validation are high-priority targets. Example: a "Manual
update policy" path that only does `Get(current revision)` without checking
whether the revision belongs to the correct parent -- it skips the List +
filter that the "Automatic" path uses.

### Phase 5: Scenario Construction

Construct scenarios **backward** from vulnerability windows.

For each vulnerability window from Phase 3:

1. **Choose the target**: one (assumption, violation condition) pair
2. **Design environment state**: pre-seed objects so the system reaches the
   vulnerable code path. Include only what's necessary -- every extra object
   adds noise
3. **Design user inputs**: the sequence of CREATE/UPDATE/DELETE actions that
   trigger the relevant controllers
4. **Configure perturbation tuning**: staleness targets, lookback depths,
   controller permutation -- chosen to maximize the chance of hitting the
   vulnerability window
5. **Document the hypothesis**: what bug pattern this scenario targets and why

## Perturbation Tuning Guide

### When Staleness Is Fruitful

Inject staleness when a controller **reads data written by another controller**
and **makes a decision based on that data**. The decision is the key -- a
read that's only used for logging or metrics is not interesting.

**High-value staleness targets:**

| Pattern | StaleReads config | Why |
|---------|-------------------|-----|
| Controller B reads resource X written by Controller A, no watch trigger | `{B: [X]}` | B has no notification mechanism; stale reads are realistic |
| Controller reads a condition/status set by another controller | `{reader: [status-resource]}` | Status propagation has inherent lag in real clusters |
| Controller does Get + List of related resources in sequence | `{controller: [both-resources]}` | The two reads can return data from different points in time |
| Controller reads a resource it doesn't own | `{controller: [resource]}` | No ownership = no guaranteed watch = realistic staleness |

**Low-value staleness targets (usually skip):**

- Controller reads its own primary resource (the one in the reconcile request) --
  this is fetched fresh at the start of every reconcile
- Controller reads a singleton/config resource that rarely changes
- Controller reads a resource and immediately writes back to it (self-correcting)

**Lookback depth heuristic:** Start with `staleLookback: 1` (one version behind).
Use `2` when the resource undergoes rapid sequential mutations (e.g., a revision
counter that increments multiple times during a single reconciliation cycle).
Higher values rarely add signal and explode the search space.

### When Order Permutation Is Fruitful

Permute controller ordering when **multiple controllers have pending reconciles
simultaneously** and **their execution order affects the outcome**.

**High-value permutation targets:**

| Pattern | Why permutation helps |
|---------|----------------------|
| **Diamond convergence**: two controllers triggered by the same event, both writing to a shared downstream resource | Order determines which write "wins" or whether one controller sees the other's partial output |
| **Create-then-validate chains**: Controller A creates a resource, Controller B validates it, Controller C consumes the validation result | If C runs before B, it sees unvalidated data. Permutation explores this ordering |
| **Deletion races**: one controller deletes/finalizes an object while another is mid-reconcile reading it | Order determines whether the reader sees pre- or post-deletion state |
| **Multi-instance reconciles**: same controller type triggered for multiple instances of the same resource kind | Order determines which instance "goes first" and whether shared resources are contended |

**Low-value permutation targets (usually skip):**

- Controllers that operate on completely disjoint resource sets (no shared reads
  or writes) -- ordering cannot affect outcome
- A single controller with no other pending reconciles -- nothing to permute
  against
- Controllers in a strict sequential dependency where B cannot run until A's
  output exists -- permutation will just cause B to error and retry, which
  is the normal path

### Combining Staleness and Permutation

The most interesting scenarios combine both:

1. **Permute** to force Controller C to run before Controller B
2. **Inject staleness** so Controller C reads an older version of the resource
   that Controller B would have updated

This models the real-world scenario where cache lag and scheduling jitter
conspire to create a window where a controller operates on stale data that
would normally have been refreshed before it ran.

**Rule of thumb:** If you're permuting controllers, also inject staleness on
the resources that the "early" controller reads from the "late" controller's
write set. This amplifies the ordering effect.

## Scenario Categories

Organize scenarios by the bug pattern they target:

### Convergence / Liveness
- Unconditional status updates (infinite reconcile loops)
- Feedback cycles between controllers
- Retry storms from transient errors that never clear

### Consistency / Safety
- Stale reads causing incorrect decisions (wrong revision selected, invalid
  pipeline used)
- Cross-reference validation gaps (Manual policy doesn't verify
  revision-composition affinity)
- Trust chain breaks (validator certifies based on stale input, consumer trusts
  stale certification)

### Deletion / Lifecycle
- Deletion during active reconciliation (stale read of pre-deletion object)
- Orphaned references (parent deleted, child still references it)
- Finalizer races (finalizer removed while another reconcile is in progress)

### Concurrency
- Multiple instances of same resource competing for shared downstream resources
- Diamond convergence with ordering-dependent outcome
- Create-validate-consume chain with reordering

## Output Format

Produce workflow JSON matching the Kamera coverage input schema:

```json
{
  "name": "category/descriptive-name",
  "environmentState": {
    "objects": [ ... ]
  },
  "tuning": {
    "maxDepth": 0,
    "permuteControllers": ["ControllerA", "ControllerB"],
    "staleReads": {
      "ControllerA": ["group/Kind"]
    },
    "staleLookback": {
      "group/Kind": 1
    }
  },
  "userInputs": [
    {
      "id": "human-readable description of action",
      "type": "CREATE|UPDATE|DELETE",
      "object": { ... }
    }
  ]
}
```

Each scenario should include a comment-level annotation (in accompanying docs
or analysis notes) with:

- **Target bug pattern**: which category from above
- **Vulnerability window**: the specific (assumption, violation) pair
- **Code reference**: file and line where the vulnerable decision is made
- **Why existing scenarios don't cover this**: what's novel about this input

## Naming Convention

Use `category/descriptive-name` where category is one of:

- `staleness` -- staleness injection is the primary perturbation
- `ordering` -- controller permutation is the primary perturbation
- `deletion` -- tests deletion/lifecycle edge cases
- `policy` -- tests behavior under specific policy/config branches
- `concurrency` -- tests multi-instance or multi-controller contention
- `baseline` -- no perturbation, tests basic convergence

## Quality Checks

Before finalizing scenarios:

1. **Every staleness target must correspond to a real read** in the controller
   source. Don't inject staleness on resources the controller never reads.
2. **Every permutation target must be a registered reconciler ID.** Check the
   controller wiring code for exact ID strings.
3. **Environment state must be minimal.** Include only objects needed to reach
   the target code path. Extra objects add noise and slow exploration.
4. **User inputs must trigger the relevant controllers.** Verify via the watch/
   trigger topology that the input resource actually enqueues the target
   reconciler.
5. **Canonical kind strings in staleReads/staleLookback must match what the
   replay client uses.** These are typically `group/Kind` (no version), not
   the full GVK. Check the project's reconciler wiring code for the exact
   format.
