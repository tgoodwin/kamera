---
name: scenario-design
description: Design Kamera workflow scenarios that target specific bug patterns in Kubernetes controllers. Use this when you have a control plane to test and need to produce perturbation-aware workflow inputs (environment state, external events, staleness/ordering/fault-injection tuning) that maximize the chance of surfacing real bugs.
metadata:
  short-description: Design perturbation-aware test scenarios from controller analysis
---

# Scenario Design Skill

**Your role is to FIND bugs, not fix them.** You are analyzing third-party
controller source code to identify vulnerabilities. Never modify, rewrite, or
propose changes to the controller source code under test. Your only outputs are
kamera scenario files, harness configuration, and analysis documentation.

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

The process has six phases. Phase 0 establishes the landscape; Phases 1–5
produce increasingly targeted scenario inputs.

### Phase 0: Code Surface Mapping

Before diving into individual controllers, map the full code surface to
understand what you're working with and plan where to focus.

**Map controller subsystems.** Identify logical groupings of controllers that
collaborate on a shared workflow. For example, Karpenter has provisioning
(provisioner, trigger, state informers), disruption (disruption, queue,
nodeclaim.disruption), and lifecycle (lifecycle, hydration, registration).
Understanding subsystem boundaries helps plan which areas to explore first
and when to pivot between them.

**Catalog multi-write reconciles.** For each controller, note how many API
writes happen within a single `Reconcile()` call and in what sequence. A
reconcile that does PATCH → PATCH → CREATE → UPDATE is a four-write sequence
with three crash-vulnerability windows between writes. These are the primary
targets for fault injection scenarios. Single-write reconciles have no
intra-reconcile crash surface.

**Identify shared in-memory state.** Caches, singletons, queues, and maps
that multiple controllers read from or write to. These are the targets for
ordering-sensitive bugs — if Controller A writes to a shared cache and
Controller B reads from it, the ordering of A vs B affects B's behavior.
Also note how this state is reset (e.g., on process restart) — this informs
the `OnCrash` callback design for fault injection.

**Verify harness capabilities.** Before designing scenarios, check that the
harness (builder.go or equivalent) supports the controller's full API surface.
Specifically verify:
- Are all controllers registered? Missing controllers produce silent negatives.
- Do watch triggers cover cross-resource dependencies? (e.g., if Controller A
  watches Kind B, is that watch wired up in the harness?)
- Does the replay client support the query patterns the controllers use?
  (field selectors via `MatchingFields`, subresource patches, optimistic
  locking)
Fix harness gaps before designing scenarios — a gap produces misleading
results that waste investigation time.

**Output:** A subsystem map with controller groupings, multi-write catalogs,
shared state inventory, and harness gap list. This feeds all subsequent phases.

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

3. **What external events could violate the assumption?**
   Think about events originating **outside** the controller system:
   - **User/operator actions**: spec changes, deletions, label updates
   - **kube-scheduler decisions**: pod becomes Unschedulable, pod bound to node
   - **Cloud provider changes**: instance terminated, AMI deleted, NodeClass
     becomes not-Ready
   - **Infrastructure events**: node becomes NotReady, network partition,
     certificate expiry

   These external events are modeled as `externalInputs` with `source:
   EnvironmentEvent` or `source: UserAction`. The key question: **if this
   external event arrives while the controller is mid-reconcile, does the
   controller handle it correctly?**

Each (assumption, violation condition) pair is a **vulnerability window**. These
are the raw material for scenario construction. External events are often the
most realistic violation triggers — they represent real-world scenarios that
users encounter in production.

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
3. **Design external inputs**: the sequence of state changes that trigger the
   vulnerability. Consider BOTH sources:
   - **UserAction**: operator applies a new manifest, deletes a resource,
     changes a policy. These are deliberate human actions.
   - **EnvironmentEvent**: kube-scheduler binds a pod, cloud provider AMI is
     deleted, a node goes NotReady. These are things that "happen" to the
     cluster from external systems.
   Use the UPDATE technique to stagger events: put the object in
   environmentState with an initial state, then UPDATE it at a specific depth
   via `userActionReadyDepths` to simulate the event arriving mid-execution.
4. **Configure perturbation tuning**: staleness targets, lookback depths,
   controller permutation, fault injection -- chosen to maximize the chance
   of hitting the vulnerability window
5. **Document the hypothesis**: what bug pattern this scenario targets and why

### Area Coverage Tracking

After each batch of scenarios targeting a subsystem, explicitly assess the
remaining attack surface:

- **What perturbation dimensions have been tried?** If you've only explored
  ordering, the area isn't exhausted — staleness, external events, and fault
  injection may surface different bugs.
- **What code paths remain untested?** Cross-reference the Phase 0 subsystem
  map with scenarios run so far. Unexercised controllers or write sequences
  are open territory.
- **Are remaining hypotheses high-confidence or speculative?** A cluster of
  bugs in one area (e.g., 5 bugs from one root cause) signals a systemic
  issue worth exhausting. If remaining hypotheses are all speculative, the
  area may be well-designed — consider pivoting.
- **Would new hypotheses require harness changes?** If so, weigh the harness
  investment against the expected bug yield.

When pivoting to a new subsystem, document the reasoning in `ANALYSIS.md`:
what was explored, what was found, why you're moving on, and what remains
unexplored for future investigation. This prevents future agents from
re-exploring exhausted areas or missing promising leads.

This is a judgment call, not a mechanical rule. A subsystem with many negative
results may still have unexplored angles worth pursuing.

## Perturbation Dimensions

Kamera supports four independent perturbation dimensions that compose:

1. **Ordering** (`permuteControllers`) — which controller runs next when
   multiple have pending reconciles. Explores scheduling nondeterminism.
2. **Staleness** (`stalenessIntervals`) — a controller reads stale data from
   the API server, simulating informer cache lag.
3. **External events** (`externalInputs`) — exogenous state changes injected
   at configurable execution points. These originate outside the controller
   system: user/operator actions, kube-scheduler decisions, cloud provider
   state changes.
4. **Fault injection** (`faultInjection`) — a controller crashes mid-reconcile
   after N write effects. Simulates process failure with shared state reset.

The most interesting bugs arise from combining dimensions. For example, D12
(emptiness disruption deletes active workload) uses ordering + staleness +
external events: the disruption controller runs before state.pod, with stale
pod data, while a pod is being scheduled via an external event.

## Perturbation Tuning Guide

### When External Events Are Fruitful

Inject external events when an **infrastructure or user action** can change
cluster state **while controllers are mid-reconcile**. The event represents
something that happens outside the controller system.

**Common external event patterns:**

| Pattern | Source | Example |
|---------|--------|---------|
| Pod becomes Unschedulable | `EnvironmentEvent` (kube-scheduler) | Pod can't fit on any node → triggers provisioning |
| NodeClass goes not-Ready | `EnvironmentEvent` (cloud provider) | AMI deleted → NodeClass controller sets Ready=False |
| NodePool requirements change | `UserAction` (operator) | User tightens arch requirements mid-provisioning |
| Pod scheduled to node | `EnvironmentEvent` (kube-scheduler) | spec.nodeName set → node appears occupied |

**Key design pattern:** Use UPDATE (not CREATE) to stagger events. CREATE-type
external inputs are seeded into the initial state at depth 0. To inject an event
at a specific depth, put the object in `environmentState` with an initial state
(e.g., pod with `PodScheduled=True`), then use an UPDATE external input with
`userActionReadyDepths` to change it at the desired depth (e.g., set
`PodScheduled=False, reason=Unschedulable`).

### When Fault Injection Is Fruitful

Inject faults when a controller's `Reconcile()` performs **multiple API writes
in sequence** and a crash between writes could leave inconsistent state.

**High-value crash targets:**

| Controller | Writes | Crash point |
|-----------|--------|-------------|
| Disruption StartCommand | taint Node + set condition + create replacement + mark deletion | After taint but before replacement |
| Lifecycle finalize | delete Node + delete cloud instance + remove finalizer | After Node delete but before instance delete |
| Node termination | taint + drain + detach volumes + terminate | After taint but before drain |
| Provisioner batching | CREATE NodeClaim × N | After creating K of N |

**Crash semantics:** The `crashAfterEffect` count refers to write effects
(CREATE, PATCH, DELETE). The Nth write IS applied; the (N+1)th triggers the
crash error. The controller's error handling path runs, preventing subsequent
in-memory side effects. `OnCrash` callbacks reset shared state (cluster caches,
queues) to simulate a process restart.

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

### Combining Perturbation Dimensions

The most interesting scenarios combine multiple dimensions:

1. **Permute** to force Controller C to run before Controller B
2. **Inject staleness** so Controller C reads an older version of the resource
   that Controller B would have updated
3. **External event** to inject a state change between two controllers' reconciles
4. **Fault injection** to crash a controller mid-way through a multi-write
   operation, then observe how other controllers handle the partial state

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
  "name": "d7/descriptive-name",
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
  "externalInputs": [
    {
      "id": "human-readable description of action",
      "opType": "CREATE|UPDATE|DELETE",
      "source": "UserAction|EnvironmentEvent",
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

Each bug hypothesis gets a unique identifier (e.g., D1, D2, ..., D6) that persists across
scenario iterations, analysis notes, and conversation references. The identifier is assigned
when the hypothesis is first formulated and does not change even if the scenario is revised.

**Scenario file naming:** `<id>_<descriptive-name>.json`

Examples:
- `d1_nodes-limit-batching-bypass.json`
- `d2_nodes-limit-sequential-off-by-one.json`
- `d5_custom-resource-limit-ignored.json`
- `d6_multi-nodepool-spillover.json`

Variant files (e.g., earlier attempts, with-staleness versions) append a suffix:
- `d1_nodes-limit-batching-bypass-with-staleness.json`
- `d2_earlier-attempt-preseeded.json`

**Scenario `name` field inside JSON:** `<id>/<descriptive-name>`

Examples: `"name": "d1/nodes-limit-batching-bypass"`

Every scenario gets a D-identifier when it is created. Check ANALYSIS.md for the
highest existing identifier and increment from there. If a scenario turns out to be a
dead end, keep the identifier — don't reuse it.

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
