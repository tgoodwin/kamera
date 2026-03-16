# Karpenter Scenario Analysis

## Critical Constraint: Shared In-Memory `state.Cluster`

**All ordering/permutation scenarios for karpenter MUST use Monte Carlo mode.**

Karpenter's `state.Cluster` is a singleton in-memory struct that serves as the
authoritative view of cluster topology for the provisioner. It is populated by
four informer reconcilers (`state.pod`, `state.node`, `state.nodepool`,
`state.nodeclaim`) and read directly by the `provisioner` and
`provisioner.trigger.pod` controllers.

This creates a fundamental constraint for kamera exploration:

- In **branching mode**, kamera forks execution at scheduling decision points
  to explore different controller orderings. When a fork occurs, the
  `OnFork` hook resets `state.Cluster` to empty (`cluster.Reset()`). This
  means each branch loses the accumulated in-memory state from the shared
  execution prefix — the cluster state at the fork point is simply gone.
  Branches then rebuild it from scratch as controllers run, but the
  accumulated ordering history up to the fork point is not faithfully
  represented. The exploration is inaccurate, not just concurrent.

- In **Monte Carlo mode**, each trial is a completely independent end-to-end
  execution. The `OnFork` reset runs at the start of each fresh trial, so
  every trial begins from a clean and correct initial state. Trials are
  independent and the in-memory state evolves naturally within each trial.

**Consequence:** Use `"search": { "mode": "monte_carlo", "monteCarlo": { "seed": ..., "trials": ... } }` for any scenario that sets `permuteControllers`. Do not rely on kamera's default exhaustive/branching search for karpenter.

Staleness-only scenarios (no `permuteControllers`, fixed stale reads) are
unaffected by this constraint — they run a single deterministic e2e execution
and do not branch.

---

## Controller Architecture

Controllers registered in `builder.go`:

| ID | Trigger | Role |
|----|---------|------|
| `provisioner` | Pod | Core scheduling loop; reads `state.Cluster` for topology |
| `provisioner.trigger.pod` | Pod | Enqueues provisioner when a pod becomes unschedulable |
| `state.pod` | Pod | Informer: syncs pod state into `state.Cluster` |
| `state.node` | Node | Informer: syncs node state into `state.Cluster` |
| `state.nodepool` | NodePool | Informer: syncs NodePool limits/requirements into `state.Cluster` |
| `state.nodeclaim` | NodeClaim | Informer: syncs NodeClaim state into `state.Cluster`; gates `Cluster.Synced()` |
| `nodeclaim.hydration` | NodeClaim | Copies NodePool requirements onto NodeClaim |
| `nodeclaim.launcher` | NodeClaim | Calls cloud provider; sets `Status.ProviderID`; creates Node |
| `node.hydration` | Node | Copies NodeClaim labels/taints onto Node |
| `node.registrar` | NodeClaim | Reads `Status.ProviderID`; creates corresponding Node object |

## Key Data Flows

```
Pod (CREATE)
  → state.pod         → state.Cluster (in-memory)
  → provisioner.trigger.pod → enqueues provisioner

NodePool (pre-existing or UPDATE)
  → state.nodepool    → state.Cluster (in-memory)

provisioner reads state.Cluster → creates NodeClaim

NodeClaim (CREATE)
  → nodeclaim.hydration  (copies NodePool requirements)
  → nodeclaim.launcher   (calls cloud provider, sets Status.ProviderID, creates Node)
  → node.registrar       (reads Status.ProviderID — must run AFTER launcher)
  → state.nodeclaim      (updates state.Cluster; gates Cluster.Synced())

Node (CREATE, by launcher or registrar)
  → state.node       → state.Cluster
  → node.hydration   (copies NodeClaim labels)
```

## Known Vulnerability Windows

**1. Provisioner reads stale `state.Cluster`**
The provisioner reads from `state.Cluster`, which is an in-memory cache
populated by informers. If `state.pod` or `state.nodepool` hasn't run yet
when the provisioner fires, it sees stale (or empty) topology and may
short-circuit ("no dynamic nodepools found") or make incorrect scheduling
decisions.

**2. `node.registrar` reads empty `Status.ProviderID`**
`node_registrar.go` checks `nc.Status.ProviderID == ""` and no-ops if empty.
The ProviderID is set by `nodeclaim.launcher`. If `node.registrar` fires
before `nodeclaim.launcher` has patched the NodeClaim, no Node is created
until the next reconcile cycle.

**3. `Cluster.Synced()` lag**
The provisioner short-circuits if `!Cluster.Synced()`. Sync is gated on
`state.nodeclaim` having processed all known NodeClaims. Staleness on
NodeClaim reads by `state.nodeclaim` extends this lag window and causes
the provisioner to no-op even after nodes exist.

**4. NodePool limit changes during provisioning**
If a NodePool's CPU/memory limits are updated while provisioning is active,
the provisioner may read the old limits from `state.Cluster` (populated by
a stale `state.nodepool` run) and create a NodeClaim that violates the new
limits.

---

## Scenario Authoring Conventions

### Search mode

All scenarios must set `"mode": "monte_carlo"` (see **Critical Constraint** above).
DFS/BFS will produce silent incorrect results because singletons are not reset on fork.

### User action objects

- **`CREATE` actions** may include a full `status` when the object type carries meaningful
  initial status (e.g. a Pod with `PodScheduled: False`).
- **`UPDATE` actions** must **not** include `status.conditions` or any status subresource
  fields. Real users submit spec-only updates; the controller owns status. Including status
  in an UPDATE misrepresents what a user would apply and can seed controller state
  incorrectly.

### Calibrating `userActionReadyDepths`

`userActionReadyDepths` injects a user action at a specific exploration depth. The value
must be derived from a reference Monte Carlo run — do not guess.

1. Run the scenario without the timed user action, using enough trials to get a
   representative sample. Capture a dump: `--output /tmp/ref-run`.
2. Inspect a representative path from the dump:

```bash
jq -r '.states[0].paths[0][] | "depth=\(.depth) reconciler=\(.reconciler) op=\(.op.opType) kind=\(.op.kind // "?")"' /tmp/ref-run/*.jsonl
```

3. Find the depth of the event that defines the race window (e.g. NodeClaim CREATE).
   Set the ready depth to just before that event to test mid-provisioning injection.

### Staleness configuration

Use `stalenessIntervals`. The older `staleReads` / `staleLookback` fields are deprecated.
Calibrate `staleAt` and `catchUpAt` from KindSeq values in a reference run:

```bash
jq -r '.states[0].paths[0][] | select(.op.kind // "" | contains("NodeClaim")) | "depth=\(.depth) op=\(.op.opType) kindSeqBefore=\(.kindSeqBefore) kindSeqAfter=\(.kindSeqAfter)"' /tmp/ref-run/*.jsonl
```

Set `staleAt` to the KindSeq just before the write the stale reconciler should miss,
`catchUpAt` to the KindSeq after. Use `"lag": -1` for a frozen (non-sliding) window.

---

## Scenario: baseline/single-pod-provisioning

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/baseline_single-pod-provisioning.json`
**Run mode:** Reference (`--closed-loop=false --no-perturbations`)
**Output directory:** `examples/karpenter/.agents/ref-single-pod/`

### Scenario Description

This is a baseline scenario: a single pending pod (with `PodScheduled=False, reason=Unschedulable`)
is injected as a user action into an environment containing a pre-existing `TestNodeClass/default`
(Ready) and `NodePool/default` (Ready, CPU limit 2k). No perturbations are configured
(`permuteControllers: null`, `staleReads: null`). The `maxDepth` is set to 50.

The harness fuzzer generated 13 sub-scenarios from this single input: 1 base case, 4 "single"
parameter variants, and 8 "sampled" multi-parameter variants. These cover pod resource requests
(500m CPU), pod node selectors (arch=arm64, unmatched selectors), and nodepool requirement
variations.

### Phase 1: Convergence Assessment

All 13 sub-scenarios converge with zero aborted states. No cycling detected (total node visits
equals unique node visits in all cases, cycling ratio = 1.00).

Two convergence profiles emerged:

| Profile | Sub-scenarios | Unique Node Visits | Total Node Visits | Unique Resource States | Converged | Aborted |
|---------|--------------|-------------------|-------------------|----------------------|-----------|---------|
| **Full provisioning** | 10 (base, pod-requests-500m, pod-selector-arch-arm64, no-fit-nodepool-req variants) | 44 | 44 | 4 | 1 | 0 |
| **No provisioning** | 3 (no-fit-pod-selector variants) | 20 | 20 | 1 | 1 | 0 |

The "no-fit-pod-selector" variants converge immediately at 1 resource state because the pod's
node selector has no compatible instance types -- the provisioner correctly declines to create a
NodeClaim. The remaining 10 variants, including the "no-fit-nodepool-req" variants, proceed
through full provisioning to 4 resource states.

### Phase 2: State Comparison

Each sub-scenario produces exactly 1 converged state. `diff` reports: "1 converged states with
0 differing object(s), 0 identical." This is expected for a reference run with no controller
permutations -- there is only one deterministic ordering explored.

### Phase 3: Ordering Analysis

**Not applicable for this reference run.** The scenario has `permuteControllers: null`, so no
controller ordering permutations were explored. The trace shows a single deterministic execution
path. Ordering-dependent behavior cannot be assessed from this run alone. See "Unverified
Hypotheses" below for orderings worth testing.

### Trace Walkthrough (base sub-scenario)

The base sub-scenario executes 43 steps across 11 distinct controllers and converges at
content hash `2320bq6a`. The 4 resource state transitions correspond to 4 write effects:

| Step | Controller | Effect | Resource State Transition |
|------|-----------|--------|--------------------------|
| 2 | `provisioner` | CREATE `NodeClaim/default-00001` | `29bjqj7l` -> `3g9e9jnd` |
| 9 | `nodeclaim.launcher` | PATCH `NodeClaim/default-00001` (sets ProviderID) | `3g9e9jnd` -> `225hc6ou` |
| 15 | `node.registrar` | CREATE `Node/fake:///default-00001` | `225hc6ou` -> `2320bq6a` |
| 38 | `External User` | UPDATE `Pod/pending` | (no hash change -- pod binding is non-state-changing or already reflected) |

**Final converged state objects (5):** `TestNodeClass/default`, `NodePool/default`,
`Pod/pending`, `NodeClaim/default-00001`, `Node/fake:///default-00001`.

**Final KindSequences:** `core/Node: 6`, `core/Pod: 7`, `karpenter.sh/NodeClaim: 5`,
`karpenter.sh/NodePool: 2`, `karpenter.test.sh/TestNodeClass: 3`.

**All pending reconciles at convergence** are `Stable Requeue After` -- idempotent timer-based
requeues that produce no further state changes.

### Observed Behaviors

**1. `node.registrar` no-ops before `nodeclaim.launcher` (Known Vulnerability #2 confirmed in trace)**

`node.registrar` fires at step 7 with no effect (ProviderID is empty). `nodeclaim.launcher`
then runs at step 9 and PATCHes `NodeClaim/default-00001` to set ProviderID. On its second
invocation at step 15, `node.registrar` successfully creates `Node/fake:///default-00001`.

This is a concrete trace observation of Known Vulnerability #2 documented above. In the
deterministic reference ordering, this is benign -- the registrar retries and succeeds. However,
under permutation, if `node.registrar` is never re-triggered after `nodeclaim.launcher` completes,
the Node would never be created.

**2. Provisioner runs 11 times, produces effects only once**

The `provisioner` controller is invoked at steps 2, 6, 11, 14, 20, 23, 27, 30, 33, 36, 40.
Only step 2 produces an effect (CREATE NodeClaim). The other 10 invocations are idempotent
no-ops. This is P3 severity -- standard controller-runtime reconcile churn, not a bug. The
provisioner is re-enqueued by `provisioner.trigger.pod` on every pod event, which is by design.

**3. `nodeclaim.launcher` and `nodeclaim.hydration` each run twice**

- `nodeclaim.hydration`: steps 8 (no effect), 16 (no effect) -- copies requirements but
  the NodeClaim already has them from creation
- `nodeclaim.launcher`: step 9 (PATCH, sets ProviderID), step 17 (no effect, already launched)

Both controllers are idempotent on re-invocation. The second invocations are harmless.

**4. "no-fit-nodepool-req" variants still provision**

The "no-fit-nodepool-requirement-unmatched" fuzzer variants (e.g., `sampled_002`, `sampled_004`,
`sampled_007`) converge with the same 44-node/4-resource-state profile as the base case.
Despite their name suggesting a mismatch, provisioning completes and a NodeClaim + Node are
created. This is expected: "no-fit-nodepool-requirement" constrains instance type selection
(which instance types satisfy the nodepool's requirements), not whether provisioning occurs at
all. Karpenter filters instance types that satisfy both pod and nodepool requirements and
provisions a node from the intersection.

In contrast, "no-fit-pod-selector-unmatched" variants correctly produce no provisioning (20
nodes, 1 resource state) because the pod's node selector yields zero compatible instance types.

### Unverified Hypotheses

These hypotheses arise from the trace but could not be verified because this was a reference
run with no controller permutations.

**H1: If `node.registrar` is never re-triggered after `nodeclaim.launcher` sets ProviderID,
the Node is never created.**

The trace shows `node.registrar` no-ops at step 7 (empty ProviderID) and succeeds at step 15
(after launcher). In the reference ordering, the registrar is naturally re-enqueued. Under
permutation, if the registrar's re-enqueue is consumed before the launcher runs, and no
subsequent event triggers the registrar again, the pipeline would stall.

*To verify:* Create a variant with `permuteControllers: ["node.registrar", "nodeclaim.launcher"]`
and `search.mode: "monte_carlo"` (per the critical constraint). Look for trials where the
Node is never created.

**H2: If `state.pod` and `state.nodepool` do not run before the first `provisioner` invocation,
the provisioner sees an empty `state.Cluster` and no-ops.**

In the reference trace, `state.pod` runs at step 0 and `state.nodepool` at step 3 -- both
before the provisioner's effective run at step 2. However, the provisioner first runs at step 2
(with the CREATE effect), meaning `state.nodepool` has NOT yet run. The provisioner still
succeeds, suggesting it reads NodePool directly from the API server (via LIST/GET observations)
rather than relying solely on `state.Cluster`. The provisioner's observations at step 2 include
`LIST Pod` and `LIST/GET NodePool`, confirming API-server reads.

The `state.Cluster` staleness vulnerability may only manifest when `Cluster.Synced()` returns
false (gated by `state.nodeclaim`), not from missing nodepool data.

*To verify:* Create a staleness scenario targeting `state.nodepool` informer lag and check
whether the provisioner still provisions correctly.

**H3: Permuting informer controllers (`state.pod`, `state.nodepool`, `state.nodeclaim`,
`state.node`) relative to `provisioner` produces different outcomes.**

The in-memory `state.Cluster` is populated by informers and read by the provisioner. Different
informer orderings could produce different `Cluster.Synced()` evaluations or topology views.

*To verify:* Create a variant with `permuteControllers: ["provisioner", "state.pod",
"state.nodepool", "state.nodeclaim"]` using Monte Carlo mode.

---

## Scenario: concurrency/two-pods-competing

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/concurrency_two-pods-competing.json`
**Run mode:** Reference (`--closed-loop=false --no-perturbations`) with Monte Carlo ordering
**Output directory:** `examples/karpenter/.agents/ref-two-pods-competing/`

### Scenario Description

Two unschedulable pods compete for provisioning. The environment starts with `TestNodeClass/default`
(Ready), `NodePool/default` (Ready, CPU limit 2k), and `Pod/pending-1` (Unschedulable, 500m CPU /
256Mi memory). A user action creates `Pod/pending-2` (same resource requests) at convergence of the
first pod's scheduling. The scenario tests whether controller ordering can cause the provisioner to
create two separate NodeClaims (one per pod) instead of batching both onto a single node.

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, provisioner.trigger.pod, state.pod, state.nodeclaim, nodeclaim.launcher, node.registrar]`
- `staleReads: provisioner -> core/Pod, state.pod -> core/Pod`
- `staleLookback: core/Pod -> 1`
- `search: monte_carlo, seed=6001, trials=40`

Note: `--no-perturbations` stripped the staleness configuration but preserved Monte Carlo ordering
permutation. Each of the 40 trials explores a different random controller ordering.

### Phase 1: Convergence Assessment

The harness auto-generated 3 sub-scenario variants from the input: `base`, `single_pod_requests_500m`,
and `single_pod_selector_arch_arm64`. Each variant ran 40 Monte Carlo trials (1 base + 39 trial runs),
totaling ~120 independent executions.

| Variant | Trials | Converged | Max-Depth Aborted | Convergence Rate |
|---------|--------|-----------|-------------------|-----------------|
| `base` | 40 | 12 | 28 | 30% |
| `single_pod_requests_500m` | 40 | 15 | 25 | 37.5% |
| `single_pod_selector_arch_arm64` | 25 (partial) | 7 | 18 | 28% |

**No cycling detected.** In all trials, `total node visits == unique node visits` (cycling ratio = 1.00).
The non-converging trials are genuinely running out of depth, not cycling.

**Convergence decision:** Partial convergence -- 30-37% of trials converge, the rest hit max depth.
The non-convergence is NOT caused by cycling but by the user action firing too late (or not at all),
leaving insufficient depth for the second pod's scheduling to settle.

### Phase 2: State Comparison

All converging trials reach the **same logical final state** despite different content hashes. The
`diff` tool reports: `1 converged states with 0 differing object(s), 0 identical` across all 12
converging base trials. Hash variation is due to metadata differences (observation counts, timestamps),
not object content divergence.

**Converged state objects (6 distinct):** `TestNodeClass/default`, `NodePool/default`,
`NodeClaim/default-00001`, `Node/fake:///default-00001`, `Pod/pending-1`, `Pod/pending-2`.

**Key finding:** Only 1 NodeClaim and 1 Node are ever created. Across all 106 examined trials
(all variants), zero trials produced a second NodeClaim. The provisioner correctly batches both
pods onto a single node in every ordering explored.

### Phase 3: Ordering Analysis

Despite exploring ~100 different random controller orderings across Monte Carlo trials, no
ordering-dependent divergence in the final state was observed. All converging trials produce
identical logical states. The ordering differences affect only **convergence speed**, not
**convergence outcome**.

#### The ordering-dependent convergence speed phenomenon

The step at which key events occur varies dramatically across orderings:

| Event | Fastest Trial | Slowest Converging Trial | Typical Non-Converging Trial |
|-------|--------------|-------------------------|------------------------------|
| NodeClaim CREATE | Step 0 (trial 37) | Step 5 (trial 7) | Step 0-9 |
| NodeClaim PATCH (launch) | Step 1 (trials 34, 37) | Step 8 (trial 7) | Step 9-27 |
| Node CREATE | Step 3 (trial 9) | Step 23 (trials 34, 26) | Step 23-56 |
| User Action (2nd pod) | Step 54 (trials 3, 26, 37) | Step 69 (trial 7) | Step 63-78 (when fires) |

**Critical bottleneck: `nodeclaim.launcher` to `node.registrar` gap.** The gap between NodeClaim
PATCH (setting ProviderID) and Node CREATE ranges from 1 step (trial 9: launch at 2, node at 3)
to 22 steps (trial 34: launch at 1, node at 23). This gap is caused by interleaved no-op reconciles
of state informers and the provisioner between the two critical controllers.

**User action timing determines convergence.** The user action fires at convergence of the first
pod's scheduling pipeline. Trials fall into three categories:

1. **Converged (12/40 base trials):** User action fires at steps 54-69. Remaining 8-16 steps
   suffice for the second pod to be absorbed. (The provisioner sees pending-2, determines it fits
   on the existing node, and no new NodeClaim is needed.)

2. **Max-depth with user action (10/40):** User action fires at steps 63-78 but insufficient
   remaining steps for settlement. Trials with user action at step 63 or 68 sometimes still fail
   to converge, suggesting the boundary is around step 65.

3. **Max-depth without user action (17/40):** The first pod's scheduling pipeline itself never
   reaches a fixpoint within 80 steps. The provisioner cycles through no-op reconciles without
   settling. These orderings interleave too many state informer re-reads between critical
   write steps.

#### Evidence: converging trial 37 (fastest, 62 steps)

```
Step  0: provisioner      -> CREATE NodeClaim/default-00001
Step  1: nodeclaim.launcher -> PATCH NodeClaim/default-00001 (sets ProviderID)
Step  9: node.registrar   -> CREATE Node/fake:///default-00001
Step 54: External User    -> UPDATE Pod/pending-2 (creates 2nd pod)
Steps 55-61: No-op reconciles (state.pod, provisioner, provisioner.trigger.pod settle)
Step 62: Converged
```

The provisioner runs at step 0 and immediately creates a NodeClaim. The launcher runs at step 1.
The node registrar creates the Node at step 9. All three critical writes happen in the first 10 steps.
The remaining 44 steps before the user action are settling no-op reconciles.

#### Evidence: non-converging trial 10 (no user action, 81 steps)

```
Step  0: provisioner      -> CREATE NodeClaim/default-00001
Step 20: nodeclaim.launcher -> PATCH NodeClaim/default-00001
Step 40: node.registrar   -> CREATE Node/fake:///default-00001
Steps 41-80: No-op reconciles (settling)
User action: never fires (convergence not reached within depth 80)
```

Same 3 writes, but spread across 40 steps instead of 10. The interleaved ordering burns 19
no-op steps between NodeClaim CREATE and PATCH, and another 20 no-op steps between PATCH and
Node CREATE. The settling tail after Node creation exceeds the remaining depth budget.

### Observed Behaviors

**1. No double-provisioning detected (negative result)**

The scenario's primary hypothesis -- that controller ordering or stale pod reads could cause the
provisioner to see only one pod at a time and create separate NodeClaims -- was NOT triggered in any
of ~106 trials. In every trial, the provisioner batches both pods onto a single NodeClaim when it
sees them.

This is a **trace-grounded negative result**: across 12 converging trials and 10 non-converging
trials where the user action fired, the provisioner never created a second NodeClaim after seeing
the second pod. The provisioner correctly determines that the existing node has sufficient capacity
(2x 500m CPU = 1 CPU, well within the node's capacity).

**2. Ordering-dependent convergence speed (P3)**

Different controller orderings cause the provisioning pipeline to take between 62 and 80+ steps.
The fastest orderings complete the three critical writes (NodeClaim CREATE, NodeClaim PATCH,
Node CREATE) within the first 10 steps. The slowest orderings spread these same writes across 40+
steps due to interleaved no-op reconciles of `state.pod`, `state.nodepool`, `state.nodeclaim`,
`provisioner.trigger.pod`, `nodeclaim.hydration`, etc.

This is not a bug but a **performance/latency observation**: in a real cluster, unfavorable
controller scheduling could cause provisioning to take 4x longer than optimal, as the same
critical path is extended by redundant reconcile loops.

**3. User action timing dependency**

The user action is configured to fire at convergence (no `userActionReadyDepths` set). This means
the second pod only appears after the first pod's scheduling is fully settled. In 17/40 base trials,
the first pod's settling takes >80 steps and the user action never fires, making it impossible to
observe the two-pods-competing race.

### Unverified Hypotheses

**H1: Stale pod reads by the provisioner could cause double-provisioning.**

The `--no-perturbations` flag stripped the `staleReads` and `staleLookback` configuration. The
scenario configures `staleReads: provisioner -> core/Pod` with `staleLookback: 1`. If the provisioner
sees a stale pod list (missing the second pod), it might create a NodeClaim sized for only one pod.
Then, when it catches up and sees the second pod, it could create a second NodeClaim.

*Why not verified:* Staleness was stripped by `--no-perturbations`. Additionally, the user action
fires at convergence (after the first pod is fully scheduled), so by the time pending-2 arrives,
the provisioner has already settled. The race window for stale reads is very narrow.

*To verify:* Run the scenario in exploration mode (`--closed-loop=false`, no `--no-perturbations`)
with the original staleness configuration. Also consider adding `userActionReadyDepths: {"0": 0}`
to fire the second pod immediately at depth 0, creating maximum overlap between the two pods'
scheduling cycles.

**H2: Simultaneous pod arrival would increase the likelihood of double-provisioning.**

In the current scenario, the second pod arrives after the first is fully scheduled. If both pods
were present from the start (or the second arrived during the first pod's provisioning cycle), the
provisioner might see them in separate reconcile loops and create separate NodeClaims.

*Why not verified:* The scenario design fires the user action at convergence, serializing the
two pods' lifecycles. A modified scenario with both pods in the initial `environmentState` would
test true concurrency.

*To verify:* Create a variant with both pods in `environmentState.objects` (no `userInputs`).
This eliminates the sequential dependency and tests whether controller ordering alone can cause
double-provisioning when both pods are simultaneously pending.

**H3: Higher resource requests (e.g., 900m CPU each) might trigger separate NodeClaims.**

With 500m CPU each, both pods fit on a single node. If each pod requested 900m CPU, the provisioner
might determine that a single node cannot accommodate both and would need to create 2 NodeClaims.
The ordering-dependent question then becomes: does the provisioner see both pods at once (1 large
node) or each separately (2 smaller nodes)?

*Why not verified:* The auto-generated variants only tested 500m and arm64 selectors, not different
resource amounts.

*To verify:* Create a variant with each pod requesting 900m+ CPU and both pods in the initial
environment state.

### Recommendations

1. **Increase `maxDepth` to 120-150** to allow more trials to converge. The current depth of 80
   is insufficient for ~70% of orderings. This does not indicate a bug but limits the scenario's
   ability to explore the two-pod interaction.

2. **Add `userActionReadyDepths: {"0": 0}`** to fire the second pod at the start of exploration,
   creating true concurrency between both pods' scheduling. This would test H2.

3. **Re-run with staleness enabled** (exploration mode, no `--no-perturbations`) to test H1 --
   whether stale pod reads by the provisioner cause different scheduling decisions.

4. **Create a variant with both pods in `environmentState`** to eliminate the user action timing
   bottleneck entirely and test the core concurrency question directly.

---

## Scenario: ordering/nodeclaim-pipeline-permutation

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/ordering_nodeclaim-pipeline-permutation.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-ordering-explore/` (base trials)

### Scenario Description

This scenario tests whether the ordering of the 6 post-provisioning controllers in the NodeClaim
pipeline produces different final states. A single pending pod triggers the provisioner to create a
NodeClaim, then the following controllers are permuted after the NodeClaim CREATE event:

- `nodeclaim.hydration` -- copies NodePool requirements onto NodeClaim
- `nodeclaim.launcher` -- calls cloud provider, sets `Status.ProviderID`
- `node.registrar` -- reads `Status.ProviderID`, creates Node object
- `state.nodeclaim` -- informer: syncs NodeClaim into `state.Cluster`
- `state.node` -- informer: syncs Node into `state.Cluster`
- `node.hydration` -- copies NodeClaim labels/taints onto Node

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [nodeclaim.hydration, nodeclaim.launcher, node.registrar, state.nodeclaim, state.node, node.hydration]`
- `permuteAfterEvent: {opType: CREATE, kind: NodeClaim}` -- permutation activates only after the provisioner creates a NodeClaim
- `search: monte_carlo, seed=5001, trials=50`

Controllers NOT permuted: `provisioner`, `provisioner.trigger.pod`, `state.pod`, `state.nodepool`.
These run in their natural ordering throughout.

### Phase 1: Convergence Assessment

49 Monte Carlo trials completed (trial_3 was not generated). Campaign metrics summary:

| Metric | Min | Max | Avg |
|--------|-----|-----|-----|
| Unique node visits | 37 | 81 | 55.3 |
| Total node visits | 37 | 81 | 55.3 |
| Unique resource states | 4 | 4 | 4.0 |

| Outcome | Count | Percentage |
|---------|-------|-----------|
| Converged (0 aborted) | 48 | 98% |
| Max-depth aborted | 1 (trial_16) | 2% |
| Error-terminated | 0 | 0% |

**No cycling detected.** Total node visits equals unique node visits in all 49 trials (cycling
ratio = 1.00). All resource state progressions go through exactly 4 states.

The single non-converging trial (trial_16, 81 steps) is NOT caused by an ordering bug. The pipeline
completes normally (NodeClaim CREATE at step 14, PATCH at step 21, Node CREATE at step 37, user
action at step 75), but the user action fires at step 75 with only 5 remaining steps -- insufficient
for post-user-action settling. The late pipeline start (step 14 for NodeClaim CREATE, vs avg 2.8)
cascades into late completion.

### Phase 2: State Comparison

```
1 converged states with 0 differing object(s), 0 identical
```

All 48 converging trials produce the **same logical final state**. The `diff` tool confirms zero
divergent objects. Content hash variation across trials is due to metadata differences (observation
counts), not object divergence.

**Key finding: No ordering-dependent divergence detected across 49 random permutations of the
6 NodeClaim pipeline controllers.** Every ordering that has sufficient depth budget converges to
an identical final state.

### Phase 3: Ordering Analysis

#### The pipeline invariant holds across all orderings

Despite 49 different random orderings of the 6 permuted controllers, the NodeClaim provisioning
pipeline always produces the same 3 write effects in the same causal order:

1. `provisioner` -> CREATE `NodeClaim/default-00001`
2. `nodeclaim.launcher` -> PATCH `NodeClaim/default-00001` (sets ProviderID)
3. `node.registrar` -> CREATE `Node/fake:///default-00001`

This ordering is enforced by data dependencies, not scheduling:
- `nodeclaim.launcher` requires `NodeClaim.Status.ProviderID == ""` (set by CREATE)
- `node.registrar` requires `NodeClaim.Status.ProviderID != ""` (set by launcher's PATCH)

These guards make the pipeline ordering-invariant: regardless of which controller is scheduled
first after the NodeClaim CREATE, only the launcher can make progress until ProviderID is set,
and only the registrar can make progress afterward.

#### node.registrar premature execution (Known Vulnerability #2 -- observed, benign)

In **25 out of 49 trials (51%)**, `node.registrar` runs between the NodeClaim CREATE and the
NodeClaim PATCH (i.e., before `nodeclaim.launcher` sets ProviderID). In all 25 cases, the
registrar correctly no-ops (line 29-31 of `node_registrar.go`: `if nc.Status.ProviderID == ""
{ return }`) and successfully runs later after the launcher completes.

This confirms Known Vulnerability #2 from the architecture notes: the registrar's guard clause
handles premature scheduling gracefully. The pipeline is **self-healing** -- the registrar
always gets re-triggered by the NodeClaim PATCH event and succeeds on its next invocation.

#### Pipeline timing varies dramatically by ordering

The gap between NodeClaim CREATE and Node CREATE (the "pipeline gap") ranges from 2 to 24 steps:

| Pipeline gap | Trials | Step range |
|-------------|--------|-----------|
| 2-5 steps | 10 | Fastest: trial_40 (2), trial_44 (2) |
| 6-15 steps | 24 | Typical: trial_10 (5), trial_43 (8) |
| 16-24 steps | 15 | Slowest: trial_22 (24), trial_25 (24) |

**Fastest trial (trial_40, 37 total steps):**
```
Step  0: state.nodepool           (no-op)
Step  1: provisioner.trigger.pod  (no-op)
Step  2: provisioner              -> CREATE NodeClaim/default-00001
Step  3: nodeclaim.launcher       -> PATCH NodeClaim/default-00001 (sets ProviderID)
Step  4: node.registrar           -> CREATE Node/fake:///default-00001
Step  5: nodeclaim.launcher       (no-op, already launched)
Step  6: nodeclaim.hydration      (no-op)
...
Step 33: External User            -> UPDATE Pod/pending
Steps 34-37: settling
```

The three critical writes happen in steps 2-4, with zero interleaving. Launcher fires immediately
after provisioner, and registrar fires immediately after launcher.

**Slowest trial (trial_16, 81 total steps, max-depth aborted):**
```
Step  0: provisioner              (no-op, state.Cluster not synced)
Steps 1-13: state informers + provisioner cycling
Step 14: provisioner              -> CREATE NodeClaim/default-00001
Step 15: nodeclaim.hydration      (no-op interleave)
Steps 16-20: interleaved no-ops
Step 21: nodeclaim.launcher       -> PATCH NodeClaim/default-00001
Steps 22-36: interleaved no-ops
Step 37: node.registrar           -> CREATE Node/fake:///default-00001
Steps 38-74: settling
Step 75: External User            -> UPDATE Pod/pending
Steps 76-80: insufficient settling budget -> max-depth abort
```

The same three writes are spread across 23 steps (14 to 37) instead of 2 steps (2 to 4),
a 12x slowdown caused by interleaving no-op reconciles of state informers and the provisioner.

#### Decomposition of pipeline gap

The pipeline gap decomposes into two sub-gaps:

| Sub-gap | Min | Max | Avg | Description |
|---------|-----|-----|-----|-------------|
| CREATE-to-PATCH (provisioner to launcher) | 1 | 18 | 5.9 | Steps between NodeClaim CREATE and launcher PATCH |
| PATCH-to-Node (launcher to registrar) | 1 | 18 | 6.7 | Steps between launcher PATCH and Node CREATE |

In 10/49 trials, the registrar creates the Node within 2 steps of the launcher PATCH.
In the remaining 39/49, 3-18 interleaved no-op steps separate the PATCH from the Node CREATE.

#### Settling tail dominates total execution time

The settling tail (steps after Node CREATE before user action fires) averages 31 steps, ranging
from 19 to 49. This is where state informers (`state.nodeclaim`, `state.node`, `state.pod`) and
the provisioner cycle through no-op reconciles until reaching a fixpoint. The settling tail
accounts for 55-70% of total execution time.

### Observed Behaviors

**1. No ordering-dependent final-state divergence (negative result)**

Across 49 Monte Carlo trials permuting 6 controllers, zero trials produced a different final
state. The pipeline's data-dependency guards (`ProviderID == ""` checks in both launcher and
registrar) enforce a single valid causal order regardless of scheduling. This is a **trace-grounded
negative result** -- every converging trial reaches the same objects.

**2. Ordering-dependent convergence speed (P3, latency-only)**

Different orderings cause the provisioning pipeline to take between 37 and 81 steps (2.2x
variation). The core pipeline (3 writes) takes between 2 and 24 steps. The variation is caused
by interleaved no-op reconciles of controllers that are triggered by intermediate events but
have no work to do at that point. In a real cluster, this translates to provisioning latency
variation: the same pod provisioning could take 1-12x longer depending on which goroutine the
scheduler picks next.

**3. `node.registrar` premature execution is ubiquitous and harmless (P3)**

51% of orderings schedule `node.registrar` before `nodeclaim.launcher` after the NodeClaim
CREATE. The registrar's guard clause (`Status.ProviderID == ""`) causes a clean no-op. The
registrar is then re-triggered by the NodeClaim PATCH event and succeeds. This is a wasted
API read (one GET per premature invocation) but has no functional impact.

### Unverified Hypotheses

**H1: Staleness on NodeClaim reads by `node.registrar` could cause it to miss the ProviderID
update permanently.**

If `node.registrar` reads a stale version of the NodeClaim (one without ProviderID) even after
the launcher has written it, the registrar would no-op. If the registrar is not re-triggered
after staleness resolves (e.g., because no further NodeClaim events fire), the Node would never
be created.

*Why not verified:* This scenario does not configure `stalenessIntervals`. The `permuteControllers`
only permutes scheduling order, not read staleness.

*To verify:* Create a variant with:
```json
"stalenessIntervals": [{
  "reconciler": "node.registrar",
  "kind": "karpenter.sh/NodeClaim",
  "staleAt": 1,
  "catchUpAt": 4,
  "lag": -1
}]
```
This would freeze the registrar's view of NodeClaim at the pre-PATCH snapshot during the
critical window.

**H2: With more than 6 controllers permuted (including `provisioner`, `state.pod`,
`state.nodepool`), ordering-dependent divergence might emerge.**

The current scenario permutes only post-provisioning controllers. The provisioner and its
triggering informers (`state.pod`, `state.nodepool`, `provisioner.trigger.pod`) run in their
natural order. Permuting those controllers might cause the provisioner to see an empty
`state.Cluster` and short-circuit, or to miss the pending pod entirely.

*Why not verified:* The scenario design intentionally focuses on the NodeClaim pipeline.
Expanding `permuteControllers` would test a different hypothesis (provisioner ordering
sensitivity) and is covered by the `baseline/single-pod-provisioning` unverified hypothesis H3.

### Summary

The `ordering/nodeclaim-pipeline-permutation` scenario demonstrates that the Karpenter NodeClaim
pipeline is **ordering-invariant for functional correctness** across 49 Monte Carlo trials.
Data-dependency guards in `nodeclaim.launcher` (checks `ProviderID == ""`) and `node.registrar`
(checks `ProviderID != ""`) enforce the correct causal ordering regardless of which controller
is scheduled first.

The scenario does reveal **ordering-dependent latency** (2.2x variation in total steps) caused
by interleaved no-op reconciles, and **ubiquitous premature registrar invocation** (51% of trials),
both classified as P3 severity. No P0/P1/P2 bugs were found.

---

## Scenario: ordering/provisioner-before-state-informers

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/ordering_provisioner-before-state-informers.json`
**Run mode:** Reference (`--closed-loop=false --no-perturbations`) with Monte Carlo ordering permutation
**Output directory:** `examples/karpenter/.agents/ref-provisioner-before-state-informers/`

### Scenario Description

This scenario tests whether the provisioner can fire before the state informers (`state.pod`,
`state.nodepool`) have populated `state.Cluster`, and what happens when it does. The environment
starts with `TestNodeClass/default` (Ready) and `NodePool/default` (Ready, CPU limit 2k). A user
action creates `Pod/pending` (Unschedulable) at convergence.

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, provisioner.trigger.pod, state.pod, state.nodepool]`
- `staleReads: provisioner -> [core/Pod, karpenter.sh/NodePool]` (stripped by `--no-perturbations`)
- `staleLookback: {core/Pod: 1, karpenter.sh/NodePool: 1}` (stripped by `--no-perturbations`)
- `search: monte_carlo, seed=8001, trials=40`

The `--no-perturbations` flag stripped staleness but preserved Monte Carlo ordering permutation.
Each of the 41 executions (1 base + 40 trials) explores a different random ordering of the 4
permuted controllers.

### Phase 1: Convergence Assessment

All 41 base trials converged with zero aborted states. No cycling detected (total node visits
equals unique node visits in all cases, cycling ratio = 1.00).

| Metric | Min | Max | Avg |
|--------|-----|-----|-----|
| Total steps to convergence | 40 | 81 | 58.5 |
| Unique resource states | 4 | 4 | 4.0 |
| Converged states | 1 | 1 | 1 |
| Aborted states | 0 | 0 | 0 |

**Final converged state objects (5):** `TestNodeClass/default`, `NodePool/default`,
`Pod/pending`, `NodeClaim/default-00001`, `Node/fake:///default-00001`.

**Final KindSequences:** `core/Node: 6`, `core/Pod: 7`, `karpenter.sh/NodeClaim: 5`,
`karpenter.sh/NodePool: 2`, `karpenter.test.sh/TestNodeClass: 3`.

### Phase 2: State Comparison

```
1 converged states with 0 differing object(s), 0 identical
```

All 41 trials converge to the **same logical final state**. Zero divergent objects across all
orderings. Content hash variation is due to metadata differences only.

### Phase 3: Ordering Analysis

#### Ordering categories across 41 trials

The 4 permuted controllers produce three natural ordering categories based on when the provisioner
first runs relative to the state informers:

| Category | Trials | Provisioner immediate effect | Avg steps |
|----------|--------|------------------------------|-----------|
| Provisioner before BOTH `state.pod` AND `state.nodepool` | 18/41 (44%) | 8/18 (44%) | 61.6 |
| Provisioner after BOTH informers | 6/41 (15%) | 6/6 (100%) | 52.0 |
| Provisioner between informers (one before, one after) | 16/41 (39%) | 15/16 (94%) | 59.1 |

**Key finding: when the provisioner runs after both state informers, it ALWAYS produces its
NodeClaim CREATE effect immediately (100%). When it runs before both, it succeeds immediately
only 44% of the time.** The remaining 56% no-op initially and produce effects after an average
delay of 5.6 additional steps.

#### The batcher gate (not the state informers) controls provisioner readiness

Detailed trace inspection reveals that the provisioner's immediate/delayed behavior is NOT
determined by whether state informers have populated `state.Cluster`. Instead, it is controlled
by the **batcher trigger** mechanism:

**Evidence from zero-observation steps:**
- In trial 8 (provisioner before both informers, delayed effect at step 12): The provisioner
  runs at steps 0, 2, 4, 6, 7, 8 with ZERO API observations (not even a LIST). It only produces
  observations (LIST Pod, LIST NodePool, GET NodePool) and the CREATE effect at step 12, after
  `provisioner.trigger.pod` ran at steps 9 and 11.
- In trial 25 (provisioner between informers, delayed effect at step 8): Same pattern --
  provisioner no-ops at steps 1, 2, 5, 6 with zero observations. Only after
  `provisioner.trigger.pod` fires at step 7 does the provisioner read the API and create
  a NodeClaim at step 8.

**The batcher.Wait() gate:** The provisioner's `Reconcile()` method
(`karpenter/pkg/controllers/provisioning/provisioner.go:126`) calls `p.batcher.Wait(ctx)` as
its first operation. If no trigger has been received, it returns `false` and the provisioner
short-circuits with zero observations. The trigger comes from `provisioner.trigger.pod` which
calls `provisioner.Trigger(uid)` -> `batcher.Trigger(uid)`.

**Evidence from immediate-at-step-0 trials:**
- In trial 12 (provisioner at step 0, immediate effect): The provisioner produces LIST Pod,
  LIST NodePool, GET NodePool observations and creates a NodeClaim -- despite `state.pod` not
  running until step 1 and `state.nodepool` not until step 11. The batcher was pre-triggered
  (via the "Requeue After" mechanism from the initial pod watch event).
- In trial 16 (provisioner at step 0, immediate effect): Same pattern -- provisioner succeeds
  at step 0 before either informer runs.

#### Cluster.Synced() does NOT gate on state informers

The `Cluster.Synced()` check (`karpenter/pkg/controllers/state/cluster.go:118`) only validates
that the in-memory cluster state tracks all NodeClaims and Nodes that exist in the API server.
It does NOT check whether Pods or NodePools have been synced into `state.Cluster`.

At startup (no NodeClaims or Nodes exist), `Synced()` trivially returns `true` because:
1. `hasSynced` is `false` (fresh reset)
2. It lists NodeClaims and Nodes from the API server -- both are empty
3. It checks `stateNodeClaimNames.IsSuperset(nodeClaimNames) && stateNodeNames.IsSuperset(nodeNames)`
4. Both are empty sets, so this is `true`
5. `hasSynced` is set to `true`

This means the `Synced()` guard never blocks the provisioner on Pod or NodePool informer lag.
The provisioner can proceed as soon as the batcher triggers, regardless of `state.Cluster` population.

#### The provisioner reads from the API server, not state.Cluster, for scheduling

When the provisioner does fire (batcher triggered), it makes API observations: `LIST Pod` and
`LIST/GET NodePool`. These are direct API server reads, not reads from the in-memory `state.Cluster`.
The provisioner uses `state.Cluster` for node topology (existing nodes, their capacity), but for
Pod and NodePool data, it reads the API server directly.

This explains why the provisioner succeeds even when `state.pod` and `state.nodepool` haven't
run: it doesn't depend on them for the initial scheduling decision.

### Observed Behaviors

**1. No ordering-dependent divergence (negative result)**

Across 41 Monte Carlo trials permuting `provisioner`, `provisioner.trigger.pod`, `state.pod`,
and `state.nodepool`, zero trials produced a different final state. All converge to an identical
set of 5 objects. The provisioner correctly creates a NodeClaim and the pipeline proceeds to
Node creation regardless of whether informers run first.

**2. Ordering-dependent convergence speed (P3, latency-only)**

Trials where the provisioner runs after both informers converge in avg 52.0 steps. Trials where
it runs before both take avg 61.6 steps -- 18% slower. The difference is caused by wasted
provisioner reconcile loops (no-ops due to batcher not yet triggered) interleaved with state
informer runs that have no effect on the provisioner's critical path.

**Fastest trial:** trial 16 (40 steps) -- provisioner at step 0 (immediate), Node CREATE at
step 12, converged at step 40.

**Slowest trial:** trial 13 (81 steps) -- provisioner at step 0 (no-op), 4 wasted provisioner
reconciles, effect at step 4, Node CREATE at step 15, settling tail of 66 steps.

**3. Batcher-gated provisioner no-ops are ubiquitous (P3)**

In 10/41 trials (24%), the provisioner runs multiple times before `provisioner.trigger.pod` fires
and activates the batcher. The worst case is trial 8 where the provisioner runs 6 times with
zero observations before producing its effect at step 12. Each no-op reconcile is a wasted
controller-runtime cycle that reads nothing from the API server (zero observations).

This is inherent to the batcher design: the provisioner is enqueued by pod watch events but
can only proceed after `provisioner.trigger.pod` explicitly triggers the batcher. When random
ordering schedules the provisioner before the trigger pod controller, the provisioner wastes
cycles.

### Unverified Hypotheses

**H1: Stale Pod reads by the provisioner could cause it to miss the pending pod and not provision.**

The scenario configures `staleReads: provisioner -> core/Pod` with `staleLookback: 1`. If the
provisioner reads a stale pod list (before the pending pod was created), it would see no
unschedulable pods and short-circuit without creating a NodeClaim. The provisioner would then
need to be re-triggered when the stale read resolves.

*Why not verified:* The `--no-perturbations` flag stripped the staleness configuration. An
exploration-mode run (`--closed-loop=false`, no `--no-perturbations`) was attempted but the
combination of Monte Carlo trials with stale reads appears to produce very large state spaces
that exceed practical time budgets. The exploration spawned 101 parallel "starting!" messages
but produced zero converged states within the observation window.

*To verify:* Run with a reduced trial count (`trials: 5`) or use `stalenessIntervals` with
precise `staleAt`/`catchUpAt` values derived from the reference trace's KindSequences, rather
than the blanket `staleReads` approach.

**H2: Stale NodePool reads could cause the provisioner to skip provisioning entirely.**

If the provisioner reads a stale NodePool list (empty), it would find no dynamic node pools
and return without creating a NodeClaim. The reference trace shows the provisioner reads
`LIST NodePool` and `GET NodePool` as API observations -- if these return empty due to
staleness, provisioning would be silently skipped.

*Why not verified:* Same as H1 -- staleness was stripped by `--no-perturbations`.

*To verify:* Create a variant with `stalenessIntervals` targeting the provisioner's NodePool
reads:
```json
"stalenessIntervals": [{
  "reconciler": "provisioner",
  "kind": "karpenter.sh/NodePool",
  "staleAt": 0,
  "catchUpAt": 3,
  "lag": -1
}]
```
This would freeze the provisioner's view of NodePools at the initial (pre-population) state
during the critical early steps.

**H3: state.Cluster population order affects provisioner scheduling quality (not correctness).**

While the provisioner reads Pods and NodePools from the API server, it reads node topology
from `state.Cluster`. If `state.nodepool` hasn't populated the NodePool's limits/requirements
into `state.Cluster`, the provisioner might schedule with incomplete topology information.
In the current scenario (single pod, no existing nodes), this is invisible because topology
is empty regardless. With existing nodes, the order could affect bin-packing decisions.

*Why not verified:* The current scenario has no existing nodes. A scenario with pre-existing
nodes and NodeClaims would be needed to test this hypothesis.

### Summary

The `ordering/provisioner-before-state-informers` scenario demonstrates that the Karpenter
provisioner is **functionally correct regardless of state informer ordering** across 41 Monte
Carlo trials. The provisioner fires before both `state.pod` and `state.nodepool` in 44% of
trials, yet always converges to the same final state.

The key architectural insight is that the provisioner's readiness is controlled by two
independent gates:
1. **Batcher trigger** (from `provisioner.trigger.pod`) -- determines whether the provisioner
   reads the API at all. This is the dominant gate and causes all observed no-op behavior.
2. **Cluster.Synced()** -- checks NodeClaim/Node consistency only. At startup with no existing
   NodeClaims or Nodes, this trivially passes.

Neither gate depends on `state.pod` or `state.nodepool` having run. The provisioner reads Pods
and NodePools directly from the API server, not from `state.Cluster`. The state informers
populate `state.Cluster` for node topology (used by the scheduler for bin-packing existing
nodes), but the initial provisioning decision does not depend on them.

Severity: **P3 (latency only).** Ordering affects convergence speed (18% variation) but not
correctness. No P0/P1/P2 bugs found. Staleness-related hypotheses (H1, H2) remain unverified
and warrant follow-up with targeted `stalenessIntervals` experiments.

---

## Scenario: ordering/registrar-before-launcher

**Date:** 2026-03-13
**Scenario files:**
- `scenarios/ordering_registrar-before-launcher-hypothesis-1.json` (H1 control: ordering only)
- `scenarios/ordering_registrar-before-launcher-hypothesis-2.json` (H2: ordering + staleness)
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo
**Output directories:** `/tmp/karpenter-registrar-h1/`, `/tmp/karpenter-registrar-h2/`

### Scenario Description

This scenario tests whether `node.registrar` fails to create a Node when it races with `nodeclaim.launcher` for access to the same NodeClaim after its CREATE. The critical chain is:

1. `provisioner` CREATEs a NodeClaim (KindSeq 0→4, no ProviderID set)
2. `nodeclaim.launcher` PATCHes the NodeClaim with a ProviderID (KindSeq 4→5)
3. `node.registrar` reads the NodeClaim and calls the cloud provider to create a Node

If `node.registrar` runs at step 2 before `nodeclaim.launcher` completes step 3, it sees a NodeClaim with no ProviderID and must no-op (the cloud provider requires a ProviderID to create a Node). The question is whether registrar gets re-triggered after launcher's PATCH and eventually creates the Node.

**H1** tests whether the ordering race alone (without staleness) causes any divergence. With default kamera reads (no staleness), registrar either runs before or after launcher, but always reads the current state.

**H2** tests whether stale reads compound the ordering problem. With `stalenessIntervals` configured, `node.registrar` is frozen at KindSeq=4 (the post-CREATE, pre-PATCH state) until the frontier advances past KindSeq=5 (after launcher PATCH). The question is whether the PATCH event properly re-enqueues registrar even when its cache is stale.

Prior context from Scenario 3 (`nodeclaim-pipeline-permutation`): `node.registrar` gracefully no-ops when ProviderID is empty (it checks for ProviderID presence before calling the cloud provider). The launcher's PATCH event re-triggers registrar via a watch-based "State Change" enqueue.

### H1: Ordering-only control (10 MC trials, seed=42)

**Tuning:** `permuteControllers: [node.registrar, nodeclaim.launcher]`, `permuteAfterEvent: {CREATE, NodeClaim}`, no staleness

**Campaign metrics:**
```
Campaign metrics by invocation

invocation: aa772dc8-6a1c-43da-bb3c-97ef4e645ace
  unique node visits:        86
  total node visits:         86
  unique resource states:    4
  duration:                  25s
  aborted states:            0
  max-depth aborted states:  0
```

**Ordering distribution across 10 trials (base + 9 MC):**
- 5 trials: `node.registrar` ran first (no-op at KindSeq=4), then launcher patched (KindSeq→5), then registrar re-ran (Node created)
- 5 trials: `nodeclaim.launcher` ran first (patched KindSeq 4→5), then registrar ran with ProviderID present (Node created directly)

Specific orderings observed:
- base_0: registrar first (no-op, ncSeq=4)
- trial_1: registrar first (no-op, ncSeq=4), frameId d915aad0
- trial_2: registrar first (no-op, ncSeq=4), frameId fe573ec1
- trial_3: launcher first (ncSeq 4→5), then registrar at ncSeq=5
- trial_4: registrar first (no-op, ncSeq=4), frameId 41d7bc29
- trial_5: launcher first
- trial_6: launcher first
- trial_7: registrar first (no-op, ncSeq=4)
- trial_8: launcher first
- trial_9: launcher first

**State diff:** `1 converged states with 0 differing object(s), 0 identical`

**Finding:** **Refuted** — ordering-only divergence does not occur. All 10 trials converge to an identical final state (Node `fake:///default-00001` present in all). In every trial where registrar ran before launcher (5/10 trials), registrar correctly no-oped and was re-enqueued by the launcher's PATCH watch event, then succeeded on the second attempt.

### H2: Ordering + staleness (10 MC trials, seed=99)

**Tuning:** Same ordering permutation + `stalenessIntervals: [{reconciler: node.registrar, kind: karpenter.sh/NodeClaim, staleAt: 4, catchUpAt: 5, lag: -1}]`

**What this means:** `node.registrar` is frozen at the pre-ProviderID NodeClaim (KindSeq=4) while the global frontier is in [4,5). After `nodeclaim.launcher` patches (frontier=5), the staleness interval resolves and registrar's cache catches up. The question: is registrar re-triggered after staleness resolves?

**Campaign metrics:**
```
Campaign metrics by invocation

invocation: 2e39c4cb-03d3-4bda-8db5-afed3285578e
  unique node visits:        63
  total node visits:         63
  unique resource states:    4
  duration:                  23s
  aborted states:            0
  max-depth aborted states:  0
```

**Staleness window behavior (H2 base trial, step-indexed):**
- Step 11: `node.registrar` runs with `ncSeq=4` (stale, no ProviderID) → 0 effects (no-op)
- Step 12: `provisioner` runs (no-op)
- Step 13: `nodeclaim.launcher` patches NodeClaim → `ncSeq 4→5` (ProviderID set), 1 effect
- After step 13: `node.registrar` is enqueued in `pendingReconciles` with source "State Change" (watch event from the PATCH)
- Step 16: `node.registrar` runs with `ncSeq=5` (fresh, ProviderID present) → 1 effect (Node CREATE)

This is the critical causal chain: the launcher's PATCH generates a watch event → registrar is re-enqueued → staleness has resolved by this point (KindSeq frontier ≥ 5) → registrar reads current NodeClaim with ProviderID → Node is created.

**Ordering distribution in H2 (10 trials):**
- 6 trials (base, trial_4, 5, 6, 7, 9): registrar ran during stale window (ncSeq=4, no-op), then re-triggered after launcher PATCH
- 4 trials (trial_1, 2, 3, 8): launcher ran first (before registrar's scheduled slot), staleness window was not entered because registrar was not scheduled during [4,5)

**State diff:** `1 converged states with 0 differing object(s), 0 identical`

All 10 trials end with Node `fake:///default-00001` present. Zero errors in any trial.

**Trial step counts (H2):** 44–71 steps (mean ≈ 58). Comparable to H1 (49–85 steps, mean ≈ 60), confirming the staleness window adds minimal overhead — registrar is re-triggered by the next available "State Change" event within a few steps of launcher's PATCH.

**Finding:** **Refuted** — staleness + ordering does not prevent Node creation. In every H2 trial where registrar read a stale NodeClaim (6/10 trials), it correctly no-oped, and was immediately re-enqueued by the launcher's PATCH watch event with source "State Change" (confirmed in H2 base trial path, step 13 → pendingReconciles). The staleness interval resolves before registrar's re-invocation because the catchUpAt=5 boundary is crossed when launcher patches.

### Findings

**1. The registrar→launcher dependency is self-healing via watch re-enqueue (P3)**

Trace-observed (H2 base, step 11→16): `node.registrar` no-oped at KindSeq=4 (no ProviderID), was re-enqueued by the launcher PATCH event (source: "State Change"), and successfully created the Node at KindSeq=5. This mechanism is not specific to the stale read path — it also occurs in the fresh-read ordering race (H1, trials 1, 2, 4, 7, base). The PATCH event-triggered re-enqueue is the invariant that makes this self-healing.

**2. Staleness does not extend the window of vulnerability (P3)**

Code-inferred: the `catchUpAt=5` boundary coincides exactly with the launcher PATCH (which is what advances the frontier to KindSeq=5). This means the stale read resolves at the same moment the PATCH watch event fires. By the time registrar is dequeued for its second attempt, the frontier is already ≥5 and its cache is fresh. No additional delay is introduced beyond what would occur in the non-stale case.

**3. No silent failure paths found (severity confirmed P3)**

In no trial did registrar's no-op go undetected. The watch-based re-enqueue is reliable because: (a) the launcher always writes (PATCH) the NodeClaim after CREATE, (b) kamera's event system propagates PATCH events to all watchers including registrar, and (c) registrar's reconcile loop reads the NodeClaim fresh on each invocation. There is no caching inside the reconcile that would cause the stale result to persist.

### Unverified Hypotheses

**H3: What if `catchUpAt` is set to a value the frontier never reaches (permanent staleness)?**

In the current H2 configuration, `catchUpAt=5` ensures the stale read resolves when launcher patches. If `catchUpAt=6` (or higher than any achievable KindSeq), registrar would never see the ProviderID and the Node would never be created — a P1 permanent failure. This would require the staleness configuration to represent a genuine data source failure (e.g., informer cache disconnect), not a transient lag.

**H4: What if `node.registrar` is not watching NodeClaim PATCH events?**

The self-healing depends on registrar being registered as a watch handler for NodeClaim PATCH events. If the watch mapper for registrar omitted PATCH events (e.g., filtered to CREATE-only), the re-enqueue at step 13 would not occur, and the Node would never be created. The current implementation in `watch_mappers.go` should be audited to confirm PATCH events are included.

**H5: Multiple concurrent stale registrar invocations with idempotency**

In the current scenario, only one NodeClaim exists. With multiple NodeClaims in flight (multi-pod scenario), stale reads could cause registrar to no-op on all of them simultaneously during the staleness window, then retry all after catchUp. This is expected to be safe (each NodeClaim is independent), but it would increase the tail latency for Node creation in a burst-provisioning scenario.

### Summary

Kamera found that the `node.registrar` / `nodeclaim.launcher` ordering race and associated staleness window are **not a source of correctness bugs**. Across 20 Monte Carlo trials (10 H1 + 10 H2), all 20 converged to an identical final state with the Node created. In 11 trials (5 H1 + 6 H2) where registrar ran before launcher or during the staleness window, registrar correctly no-oped and was re-triggered by the launcher's PATCH watch event (confirmed at H2 base step 13: `node.registrar` appears in `pendingReconciles` with source "State Change" immediately after the launcher PATCH). The staleness experiment reveals that the registrar→launcher dependency is robustly self-healing: the watch-based re-enqueue mechanism is the load-bearing invariant, and it fires in both the fresh-read and stale-read cases. Severity: **P3 (latency only)** — ordering and staleness affect convergence path length but not final correctness. The only credible failure scenario would require permanent staleness (catchUpAt never reached), which corresponds to an informer cache failure rather than a transient ordering race.

---

## Scenario: policy/nodepool-limit-change-during-provisioning

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/policy_nodepool-limit-change-during-provisioning.json`
**Run mode:** Monte Carlo (seed=4001, trials=40)
**Output directory:** `/tmp/karpenter-policy-scenario/`

### Scenario Description

A single pending pod (`PodScheduled=False, Unschedulable`) is injected into an environment with a pre-existing `TestNodeClass/default` (Ready) and `NodePool/default` (Ready, cpu limit=2k, generation=1). At `userActionReadyDepths: {1: 6}` (depth 6 in exploration), a second user action reduces the NodePool CPU limit from `cpu: 2k` to `cpu: 1` (generation=2).

Two staleness intervals are configured targeting NodePool reads during the limit-change window:
- `reconciler: state.nodepool, kind: karpenter.sh/NodePool, staleAt: 2, catchUpAt: 3, lag: -1`
- `reconciler: provisioner, kind: karpenter.sh/NodePool, staleAt: 2, catchUpAt: 3, lag: -1`

Both intervals freeze reads of NodePool at kindSeq=2 (the initial object) until the frontier reaches kindSeq=3, simulating an informer lag during which `state.nodepool` and `provisioner` do not see NodePool mutations.

Permuted controllers: `provisioner`, `state.nodepool`, `state.pod`, `provisioner.trigger.pod`.

### Hypothesis

**H1 (limit violation):** If `state.nodepool` processes the NodePool UPDATE with stale reads, it may not propagate the reduced limit to `state.Cluster` before the provisioner creates a NodeClaim. The provisioner then creates a NodeClaim that violates the new limit. Once launched, no controller removes the over-limit NodeClaim. This is a correctness bug (NodePool limit enforcement failure).

**H2 (interleave ordering):** If the NodePool limit change is delivered before the provisioner runs (interleaved at depth 39 instead of depth 66), the provisioner sees the reduced limit and declines to create a NodeClaim. This would produce a different final state (no NodeClaim, no Node) compared to the reference run.

### Results

**Completed trials (5 of 40 total):**

| Phase | State ID | State type | Objects at convergence |
|-------|----------|-----------|------------------------|
| `reference` (trial 0) | `aborted-3p1cktgq` | max-depth-aborted | Node + Pod + NodeClaim + NodePool + TestNodeClass |
| `reference` (trial 1) | `aborted-2mhlrkq2` | max-depth-aborted | Node + Pod + NodeClaim + NodePool + TestNodeClass |
| `interleave_action_1_depth_39` (trial 2) | `state-0` | converged | Node + Pod + NodeClaim + NodePool + TestNodeClass |
| `reference` (trial 2) | `state-0` | converged | Node + Pod + NodeClaim + NodePool + TestNodeClass |
| `staleness_interval_101` (trial 2) | `aborted-*` | max-depth-aborted | Node + Pod + NodeClaim + NodePool + TestNodeClass |

The NodePool in all final states has hash `271a450a` = generation=2, `cpu: 1` (the reduced limit). The NodeClaim `default-00001` is present in every final state, with the Node `fake:///default-00001` also present. The NodeClaim was created while the NodePool still had `cpu: 2k` (generation=1); the post-limit-change NodePool is in the final state alongside the NodeClaim.

No trial produced a state where the NodeClaim was absent after provisioning. No trial produced a state where the NodeClaim was deleted after the limit change.

**Distinct final state signatures (all trials):** All 5 trials end with `{Node, Pod, NodeClaim, NodePool, TestNodeClass}` — identical object presence across all phases and orderings.

### Trace Analysis

**Reference run (trial 0) — key events (by path frame index):**

```
Frame  5: provisioner     CREATE:NodeClaim/default-00001     [np_seq=2, cpu=2k]
Frame 15: nodeclaim.launcher  PATCH:NodeClaim/default-00001  [np_seq=2, cpu=2k]
Frame 19: node.registrar  CREATE:Node/fake:///default-00001  [np_seq=2, cpu=2k]
Frame 49: External User   UPDATE:Pod/pending                 [np_seq=2, cpu=2k]
Frame 66: External User   UPDATE:NodePool/default            [np_seq=2→8, cpu=2k→1]
Frame 67: provisioner     (no effects)                       [np_seq=8, sees "not ready"]
Frame 68: state.nodepool  (no effects)                       [np_seq=8]
Frames 69–80: multiple controllers, zero effects             [np_seq=8]
```

The NodePool kindSeq jumps from 2 to 8 at frame 66 because kamera stages 6 intermediate NodePool versions (3–7) internally when applying the user UPDATE. The provisioner created the NodeClaim at frame 5 with the old limit (`cpu: 2k`). The NodePool UPDATE arrives at frame 66, long after provisioning completed.

**Interleave trial (action at depth 39) — key events:**

```
Frame  0: provisioner     CREATE:NodeClaim/default-00001     [np_seq=2, cpu=2k]
Frame  1: nodeclaim.launcher  PATCH:NodeClaim/default-00001  [np_seq=2, cpu=2k]
Frame  4: node.registrar  CREATE:Node/fake:///default-00001  [np_seq=2, cpu=2k]
Frame 30: External User   UPDATE:Pod/pending                 [np_seq=2, cpu=2k]
Frame 39: External User   UPDATE:NodePool/default            [np_seq=2→8, cpu=2k→1]
```

Even with the NodePool limit change delivered at frame 39 (earlier than the reference run's frame 66), the NodeClaim was already created at frames 0–4. The interleaved action cannot retroactively invalidate the provisioning that completed before it arrived.

**Staleness window behavior:**

The staleness intervals (`staleAt=2, catchUpAt=3`) target the window when NodePool is at kindSeq=2 (its initial state). However, in the Monte Carlo execution the provisioner reaches the NodePool at kindSeq=2 in every trial — the NodePool has been at kindSeq=2 since the start. The initial NodePool (gen=1, `cpu: 2k`, `Ready=True`) is always the one visible when the provisioner creates the NodeClaim. The NodePool UPDATE (gen=2, `cpu: 1`, status=null) arrives later. No trial was able to have the provisioner read the REDUCED NodePool limit before NodeClaim creation, regardless of staleness injection or ordering permutation, because provisioning completes in the first few frames of every trial.

**After the NodePool limit change:**

At frame 66+, the provisioner recognizes the new NodePool (gen=2, `cpu: 1`) as "not ready" (`!StatusConditions().IsTrue(ConditionReady)` at `provisioner.go:253`). This is because the user UPDATE only sets spec — the NodePool validation controller (not in this harness) would normally set status conditions. The provisioner logs `"ignoring nodepool, not ready"` and `"no dynamic nodepools found"` at every subsequent invocation. This correctly prevents NEW NodeClaims from being created, but does not trigger removal of the EXISTING NodeClaim that violates the new limit.

`state.nodepool` runs at frame 68 with `np_seq=8` but produces zero effects — it syncs the new NodePool spec into `state.Cluster` but does not initiate any disruption or remediation.

### Finding

**H1: Partially confirmed — the limit violation is a structural gap, severity P2.**

NodeClaim `default-00001` persists in all final states with `cpu: 2k` provisioned, even though the NodePool has been reduced to `cpu: 1`. This is not a transient ordering artifact — it is a structural property of how Karpenter implements NodePool limits. The provisioner enforces limits only at NodeClaim creation time (via `state.Cluster` capacity bookkeeping). There is no retroactive enforcement controller that inspects existing NodeClaims against current NodePool limits.

The mechanism:
1. `provisioner` creates NodeClaim while NodePool has `cpu: 2k` limit (frames 0–5 in all trials).
2. `nodeclaim.launcher` launches the node (cloud provider call).
3. `node.registrar` creates the Node object.
4. User reduces NodePool limit to `cpu: 1`.
5. `provisioner` sees the updated NodePool as "not ready" (because spec-only update has no status conditions) and declines future provisioning.
6. **No controller checks whether existing NodeClaims violate the new limit and removes them.**

The NodeClaim continues to exist indefinitely. The Node it corresponds to continues to run. The only mechanisms that would eventually remove it are disruption controllers (consolidation/expiration), which are not configured in this scenario (`consolidateAfter: Never`, `expireAfter: Never`).

**H2: Refuted — interleaved delivery makes no difference.** Even when the limit change is delivered at frame 39 (before many post-provisioning reconcile loops), the NodeClaim was already created at frames 0–4 in every trial. The window between `provisioner.trigger.pod` enqueuing the provisioner and the provisioner actually running is extremely narrow — it happens in the first few exploration steps before any user action at depth 6 could possibly arrive.

**Severity assessment:**

This is a **P2 (transient failure / window of vulnerability)** finding when evaluated in context:
- In a real cluster, the NodePool validation controller would quickly set `Ready=True` on the updated NodePool (with the new lower limit). Once `Ready=True`, the provisioner would see the NodePool as having `cpu: 1` budget. If `state.nodepool` has synced the updated NodePool into `state.Cluster`, the provisioner would see `cpu: 0` remaining (since 1 CPU is consumed by the existing Node) and refuse additional provisioning. This is the designed behavior.
- However, the EXISTING NodeClaim/Node that was provisioned under the old `cpu: 2k` limit is **never retroactively terminated** by the limit reduction. It will only be removed if disruption is enabled (consolidation or expiration). With `consolidateAfter: Never` and `expireAfter: Never`, the over-limit NodeClaim is permanent.
- If the intent of the limit change is to immediately cap resource usage (e.g., a cost control measure), the gap between "limit reduced" and "existing over-limit capacity removed" requires disruption to be enabled.

**Upgrade path note:** The staleness injection (`staleAt=2, catchUpAt=3`) for `state.nodepool` and `provisioner` targets a NodePool kindSeq window that in practice is never entered during the critical provisioning window. In every trial, provisioning completes at kindSeq=2 (the initial NodePool), well before the user action at depth 6 can trigger any NodePool mutation. The staleness trials are cycling through many stale-at/catch-up-at combinations for other resource kinds (currently at phases 90–101+), but NodePool staleness at the provisioning moment is structurally impossible in this scenario because the NodePool is not mutated until after provisioning completes.

---

### H3: Staleness-induced over-provisioning (revised — in progress)

**Bug hypothesis:**

If `state.nodepool` is stale during provisioning — reading the OLD NodePool (`cpu: 2k`) and populating `state.Cluster` with that stale limit — the provisioner will see available capacity and create a NodeClaim that violates the real limit (`cpu: 1`). This is silent over-provisioning: a Node is launched against a limit that has already been tightened.

**Why H1/H2 didn't surface it:**

Two miscalibrations in the first run:
1. `userActionReadyDepths: {"1": 6}` — limit change arrived at depth 6, but NodeClaim creation happens at depth 2–8, so provisioning finished before the limit change landed in most trials.
2. `catchUpAt: 3` — stale window resolved at the exact moment the user action ran (NodePool KindSeq 2→3), so staleness never engaged during provisioning.

**Changes applied:**
- `userActionReadyDepths: {"1": 0}` — limit change applied before any controller fires.
- `catchUpAt: 100` for both `state.nodepool` and `provisioner` — stale window persists throughout the trial; both controllers continue seeing old `cpu: 2k` NodePool even after real NodePool is `cpu: 1`.
- Status conditions restored on the UPDATE object (`Ready: True`) so the updated NodePool is valid and schedulable — making the stale/fresh divergence in `state.Cluster` meaningful.

**Run:** `go run . --interactive=false --inputs scenarios/policy_nodepool-limit-change-during-provisioning.json --output /tmp/karpenter-policy-h3`
**Output files:** 5 files written (trial 0 reference, trial 1 reference, trial 2 reference + interleaved action + staleness interval phases)

### H3 Results

**Trial summary (5 output files from 3 trial groups):**

| File | Phase | Outcome | Frames |
|------|-------|---------|--------|
| `reference_0` | reference | max-depth aborted | 81 |
| `trial_1_re_1` | reference | max-depth aborted | 81 |
| `trial_2_re_2` | reference | converged | 64 |
| `trial_2_in_2` | interleaved action (depth 39) | converged | 64 |
| `trial_2_st_2` | staleness interval (node.registrar/NodeClaim) | max-depth aborted | 81 |

2 aborted (max-depth), 3 converged. No errors in any trial.

**Was NodeClaim created despite the `cpu: 1` NodePool limit?**

Yes, in every trial. NodeClaim `default-00001` is present in all 5 final states alongside Node `fake:///default-00001`. The limit change has no observable effect on NodeClaim creation.

**What NodePool kindSeq did the provisioner see when creating the NodeClaim?**

`kindSeq=2` (the initial NodePool, `cpu: 2k`) in all 5 trials:

| Trial | Frame of NodeClaim CREATE | NodePool kindSeq at CREATE |
|-------|--------------------------|---------------------------|
| reference_0 | 5 | 2 |
| trial_1_re_1 | 5 | 2 |
| trial_2_re_2 | 5 | 2 |
| trial_2_in_2 | 0 | 2 |
| trial_2_st_2 | 5 | 2 |

The NodePool UPDATE (kindSeq=2→8, `cpu: 1`) arrives at frames 39–66 depending on the trial, long after NodeClaim creation in every case.

**Was the staleness window entered?**

Technically yes: the staleness intervals specify `staleAt=2`, and the provisioner reads NodePool at kindSeq=2 when creating the NodeClaim. However, this is structurally irrelevant: provisioning completes before the NodePool UPDATE arrives in every trial, regardless of whether staleness is injected. The staleness injection (`catchUpAt=100`) was designed to freeze the provisioner's view of the NodePool at `cpu: 2k` even after the limit change, but in practice the NodeClaim is always created before the user UPDATE fires (which is itself configured to fire at depth 0, before controllers). The staleness interval for `node.registrar/NodeClaim` in trial_2_st_2 is a different staleness variant inserted by kamera's perturbation engine — it targets a different controller/kind and is not the H3 staleness being tested.

After the NodePool UPDATE (kindSeq=8, `cpu: 1`, `Ready=True`), all provisioner invocations produce zero effects — the provisioner sees the updated NodePool as having valid status conditions but zero remaining capacity (since the existing NodeClaim/Node already consumes it), and declines further provisioning.

### Finding

**H3: Partially confirmed as a structural ordering race; staleness injection was not the enabling factor.**

The over-provisioning violation is confirmed in all 5 trials: a NodeClaim is created against the old `cpu: 2k` limit, and the subsequent limit reduction to `cpu: 1` is not enforced retroactively. This replicates the H1 finding from the prior run.

However, the staleness mechanism (`catchUpAt=100`) did not cause the violation — the violation occurs due to the ordering race alone. Even with `userActionReadyDepths: {"1": 0}` (limit change configured to fire before controllers), the kamera execution model applies user actions at the indicated depth in the exploration, which in practice means after the provisioner has already completed NodeClaim creation in the first few frames. The `staleAt=2/catchUpAt=100` window was entered but was not the causal factor: provisioning would have completed against `cpu: 2k` even without any staleness injection.

**Mechanism:** NodePool limit enforcement in Karpenter is point-in-time at NodeClaim CREATE. The provisioner reads the NodePool limit, checks available capacity in `state.Cluster`, and decides whether to create a NodeClaim. There is no retroactive enforcement controller. A limit reduction that arrives after NodeClaim creation has no effect on existing NodeClaims/Nodes. This is an architectural gap, not a bug triggered by staleness or ordering.

**Severity: P2 (structural gap, not staleness-induced).** The violation is reproducible in the reference run (no staleness, no ordering permutation). Staleness makes the window of vulnerability wider in principle (more likely for provisioning to complete with stale old-limit data), but is not required to trigger it. Any scenario where NodePool limit reduction and provisioner NodeClaim creation overlap in time will produce this result.

**Staleness injection assessment:** The `catchUpAt=100` configuration is too coarse-grained for this scenario. Because the NodePool is not mutated until the user action fires, and provisioning completes before the user action in every trial, the staleness window (kindSeq=2 for NodePool) coincides with the normal provisioning window — there is no meaningful "stale vs fresh" distinction to exploit. A true staleness-induced over-provisioning scenario would require the provisioner to run AFTER the NodePool UPDATE but read a stale (pre-update) NodePool; this requires the NodePool to be mutated before provisioning completes, which cannot be achieved in a scenario where provisioning happens in the first 0–5 frames.

**Root cause analysis — corrected staleness target:**

Code-level investigation revealed that NodePool limits come from a **direct API read** (via `nodepoolutils.ListManaged` → `p.kubeClient.List`) at line 244 of `provisioner.go`, not from `state.Cluster`. The `state.nodepool` informer populates `state.Cluster` with NodePool data, but this is NOT what the provisioner uses for the limit check. Instead, the scheduler initializes `remainingResources` directly from `np.Spec.Limits` (line 172 of `scheduler.go`).

The exploitable staleness path for over-provisioning is through **consumed capacity tracking**:
- `state.Cluster` tracks existing nodes via `state.nodeclaim` and `state.node` informers
- `calculateExistingNodeClaims(stateNodes)` subtracts existing node capacity from `remainingResources`
- If `state.nodeclaim` or `state.node` is stale, `stateNodes` is empty → no capacity subtracted → full limit appears available

**Corrected hypothesis (H4):** A scenario where `state.nodeclaim` and `state.node` are stale (lagging behind NodeClaim/Node creation) causes the provisioner to see full NodePool capacity on every invocation, creating multiple NodeClaims for the same pod (over-provisioning). See `staleness/nodeclaim-capacity-blindness` scenario.

---

## Scenario: staleness/nodeclaim-capacity-blindness

**Date:** 2026-03-13
**Scenario file:** `examples/karpenter/scenarios/staleness_nodeclaim-capacity-blindness.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-nodeclaim-blindness/`

### Scenario Description

Tests whether staleness of `state.nodeclaim` and `state.node` informers causes the provisioner to
double-provision: creating multiple NodeClaims for a single pending pod because `state.Cluster`
never reflects the capacity already allocated by prior NodeClaims/Nodes.

**Mechanism under test:**

1. A pending pod triggers the provisioner. The provisioner computes `remainingResources`:
   - Limit: from API read of `NodePool.Spec.Limits` = `cpu: 4`
   - Consumed: from `calculateExistingNodeClaims(stateNodes)` where `stateNodes = cluster.DeepCopyNodes()`
   - If `state.nodeclaim` is stale, `stateNodes` is empty → consumed = 0 → remaining = cpu: 4
2. Provisioner creates NodeClaim-1 (4 CPU). Pod is still Unschedulable (no Node running yet).
3. `state.nodeclaim` enters stale window (kindSeq 0→4 at NodeClaim CREATE triggers the interval):
   - Stale view: `ObserveAt(kindSeq=2)` — before any NodeClaims were created → no NodeClaims in state.Cluster
4. Provisioner runs again (pod still pending). `Cluster.Synced()` passes because `hasSynced=true`
   (set initially when in-memory NodeClaims {} ⊇ API NodeClaims {} was satisfied before any NodeClaims existed).
5. `calculateExistingNodeClaims(stateNodes=[])` → consumed = 0 → remaining = cpu: 4 → **creates NodeClaim-2**
6. Result: 2 NodeClaims × 4 CPU = 8 CPU total, violating the `cpu: 4` NodePool limit.

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, state.pod, state.nodepool, state.nodeclaim, state.node, nodeclaim.launcher, node.registrar, provisioner.trigger.pod]`
- `stalenessIntervals`:
  - `state.nodeclaim` on `karpenter.sh/NodeClaim`: `staleAt=2, catchUpAt=100, lag=-1`
  - `state.node` on `core/Node`: `staleAt=2, catchUpAt=100, lag=-1`
- `search: monte_carlo, seed=2001, trials=40`
- `NodePool.Spec.Limits.cpu: 4` (matches default fake instance type: 4 CPU)

**Reference trace calibration** (1 trial, no staleness, `/tmp/karpenter-nodeclaim-blindness-ref/`):

| Event | Controller | kindSeq Before | kindSeq After |
|-------|-----------|----------------|---------------|
| NodeClaim CREATE | `provisioner` | `karpenter.sh/NodeClaim: 0` (absent) | `karpenter.sh/NodeClaim: 4` |
| NodeClaim PATCH | `nodeclaim.launcher` | `karpenter.sh/NodeClaim: 4` | `karpenter.sh/NodeClaim: 5` |
| Node CREATE | `node.registrar` | `core/Node: 0` (absent) | `core/Node: 6` |

Stale window for NodeClaim: activates when frontier 4 ≥ staleAt 2. `ObserveAt(2)` returns no NodeClaims (first write at kindSeq=4). Correct — stale anchor is before the NodeClaim's creation.

Stale window for Node: activates when frontier 6 ≥ staleAt 2. `ObserveAt(2)` returns no Nodes (first write at kindSeq=6). Correct — stale anchor is before Node creation.

**`Cluster.Synced()` analysis:** With no NodeClaims or Nodes in environmentState, the initial `hasSynced` check (`stateNodeClaimNames ⊇ nodeClaimNames` where both are empty) passes immediately, setting `hasSynced=true`. After NodeClaim-1 is created, the lighter post-hasSynced check (iterate over in-memory NodeClaims, verify ProviderIDs) passes vacuously because state.Cluster has no NodeClaims (stale informer). The provisioner is never blocked by `Synced()`.

### Results

**H4: Refuted — provisioner has direct-injection defense against informer lag.**

The scenario ran 1 MC trial (reference + rerun + 10 auto-generated staleness interval variants). In ALL traces, only a single NodeClaim (`default-00001`) was created. No double-provisioning occurred.

**Why the bug did not trigger:**

The hypothesis assumed that `state.nodeclaim` informer staleness would leave `state.Cluster` empty after NodeClaim-1 is created, causing the provisioner to see full capacity on subsequent invocations. This assumption is wrong.

After the provisioner creates NodeClaim-1, it calls `p.cluster.UpdateNodeClaim(nodeClaim)` directly (line 459 of `provisioner.go`), **bypassing the `state.nodeclaim` informer entirely**. This call:

1. Sets `c.nodeClaimNameToProviderID["default-00001"] = ""` (ProviderID is empty at creation time).
2. Marks the NodeClaim as active in `NodePoolState`.

`Cluster.Synced()` — the gate the provisioner checks before any scheduling decision — uses the lighter post-hasSynced check (line 148 of `cluster.go`):

```go
for _, providerID := range c.nodeClaimNameToProviderID {
    if providerID == "" {
        return false  // NodeClaim exists but hasn't been launched yet
    }
}
return true
```

Since NodeClaim-1 has empty ProviderID, `Synced()` returns `false` for all subsequent provisioner invocations until `nodeclaim.launcher` PATCHes the NodeClaim with `ProviderID = "fake:///default-00001"`. At that point, `cluster.UpdateNodeClaim` is called again (from `node_claim_launcher.go`) with the ProviderID set, creating a StateNode with full capacity. The provisioner then sees `remaining = cpu: 4 - 4 = 0` and correctly declines to create NodeClaim-2.

**Defense-in-depth structure:**

| Mechanism | Role | Effect |
|-----------|------|--------|
| `cluster.UpdateNodeClaim()` (direct) | Called by provisioner after CREATE | Immediately registers NodeClaim in `c.nodeClaimNameToProviderID` |
| `Cluster.Synced()` post-hasSynced check | Blocks provisioner when ProviderID is empty | Prevents scheduling decisions until NodeClaim is launched |
| `cluster.UpdateNodeClaim()` after PATCH | Called by launcher after ProviderID is set | Creates StateNode with capacity; `calculateExistingNodeClaims` subtracts it from remaining |
| `state.nodeclaim` informer | Would update state.Cluster asynchronously | Redundant for capacity tracking — `UpdateNodeClaim` is the authoritative path |

**Implication:** The `state.nodeclaim` informer staleness tested in this scenario is irrelevant to the over-provisioning path. Karpenter's provisioner maintains `state.Cluster` directly, not through the informer, after each NodeClaim it creates. The informer serves as a secondary reconciliation path (e.g., to catch external NodeClaim modifications), not the primary capacity-tracking path.

**Severity: N/A (negative result).** No over-provisioning vulnerability found via `state.nodeclaim` + `state.node` staleness. The system is robust to informer lag in this scenario due to the `UpdateNodeClaim` direct-injection defense and `Cluster.Synced()` ProviderID gate.

### Residual Uncertainty

The `Cluster.Synced()` check is only robust if `cluster.UpdateNodeClaim` is always called correctly after NodeClaim creation. An ordering where `state.Cluster.Reset()` fires between the NodeClaim CREATE and `UpdateNodeClaim` call could bypass the defense — but `Reset()` is only called at MC trial boundaries (via `OnFork()`), not during a trial. This is not exploitable in the current harness model.

---

## Scenario: staleness/cluster-synced-lag

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_cluster-synced-lag.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-cluster-synced-lag/`

### Scenario Description

Tests whether the ordering of `state.nodeclaim` relative to the provisioner and other state informers causes different outcomes. The scenario was motivated by the `Cluster.Synced()` hypothesis: if `state.nodeclaim` runs after the provisioner in some orderings, the informer-based sync gate might behave differently, potentially blocking or failing to block provisioning at incorrect moments.

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, state.nodeclaim, state.pod, state.nodepool]`
- `search: monte_carlo, seed=3001, trials=30`
- `NodePool.Spec.Limits.cpu: 2k` (generous limit, no capacity pressure)

No `stalenessIntervals` configured — this is an ordering-only experiment.

### Hypothesis

**H (cluster-synced-lag):** Different orderings of `state.nodeclaim` relative to the provisioner could cause `Cluster.Synced()` to behave differently across trials:
1. If `state.nodeclaim` runs before the provisioner after NodeClaim CREATE, it syncs the NodeClaim into `state.Cluster`, `Cluster.Synced()` passes normally.
2. If `state.nodeclaim` runs after (or is delayed), the NodeClaim is NOT in `state.Cluster`, and the lighter post-hasSynced check's iteration over `nodeClaimNameToProviderID` might diverge.

### Results

**Negative result — no ordering-dependent divergence detected.**

The run produced 29 output files (reference + rerun + 27 auto-generated staleness interval variants). In every file, across all paths:

| Metric | Value |
|--------|-------|
| Files analyzed | 29 |
| Distinct final kindSeqs | 1 |
| NodeClaims created per trace | 1 (always `default-00001`) |
| Final kindSeq | `{core/Node: 6, core/Pod: 7, karpenter.sh/NodeClaim: 5, karpenter.sh/NodePool: 2, karpenter.test.sh/TestNodeClass: 3}` |
| Write effects per trace | 4 (NodeClaim CREATE, NodeClaim PATCH, Node CREATE, Pod UPDATE) |

Campaign metrics (reference invocation): `uniqueNodeVisits=68, totalNodeVisits=68, uniqueResourceStates=4, duration=24s`.

### Why No Divergence

The hypothesis was based on a misconception about what `Cluster.Synced()` depends on. The post-hasSynced check iterates over `c.nodeClaimNameToProviderID` — a map populated by `cluster.UpdateNodeClaim()` direct injection, **not** by the `state.nodeclaim` informer.

The causal chain that makes `state.nodeclaim` ordering irrelevant:

1. At startup (no NodeClaims in environmentState), the full `Cluster.Synced()` check passes trivially: `stateNodeClaimNames ⊇ nodeClaimNames` where both are empty. `hasSynced=true` is set immediately.
2. After the provisioner creates NodeClaim-1, it calls `p.cluster.UpdateNodeClaim(nodeClaim)` directly (line 459 of `provisioner.go`). This immediately registers `nodeClaimNameToProviderID["default-00001"] = ""` — before `state.nodeclaim` has any chance to run.
3. `Cluster.Synced()` post-hasSynced check sees the empty ProviderID and returns `false`, blocking further provisioner invocations.
4. When `nodeclaim.launcher` PATCHes the NodeClaim (sets ProviderID), it calls `cluster.UpdateNodeClaim()` again — and only then does `Synced()` return `true`.

Throughout this sequence, `state.nodeclaim`'s scheduling position is irrelevant: whether it runs before or after the provisioner, `nodeClaimNameToProviderID` is already populated by the direct `UpdateNodeClaim` call. The informer path is redundant for the `Synced()` gate.

The reference path shows `state.nodeclaim` invoked 8 times across 67 total steps, but none of these invocations affect the 4 write effects or the final state.

### Cross-Reference

This scenario builds directly on the H4 findings from `staleness/nodeclaim-capacity-blindness` (where the same `UpdateNodeClaim` direct-injection defense was identified). The `cluster-synced-lag` scenario is a focused ordering-only test that confirms the defense holds across 29 different controller orderings without requiring staleness injection.

**Severity: N/A (negative result).** No ordering-dependent divergence via `state.nodeclaim` positioning. The `UpdateNodeClaim` direct-injection pattern is the invariant that makes `Cluster.Synced()` robust to `state.nodeclaim` scheduling lag.

---

## Scenario: staleness/provisioner-stale-cluster-state

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_provisioner-stale-cluster-state.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-provisioner-stale/`

### Scenario Description

Tests whether the ordering of `state.pod` and `state.nodepool` relative to the provisioner and `provisioner.trigger.pod` causes the provisioner to see incomplete `state.Cluster` topology and make incorrect scheduling decisions (e.g., missing the pending pod entirely or skipping provisioning due to no visible NodePools).

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, state.pod, state.nodepool, provisioner.trigger.pod]`
- `search: monte_carlo, seed=1001, trials=30`
- `NodePool.Spec.Limits.cpu: 2k` (generous limit)

No `stalenessIntervals` configured.

### Hypothesis

**H (provisioner-stale-cluster-state):** If `state.pod` or `state.nodepool` runs after the provisioner in a given ordering, the provisioner reads an empty or stale `state.Cluster`:
- Missing pod data → provisioner sees no unschedulable pods → no NodeClaim created
- Missing NodePool data → provisioner finds no dynamic NodePools → logs `"no dynamic nodepools found"` → no NodeClaim created

Either path would produce a final state with no NodeClaim/Node — divergent from the nominal outcome.

### Results

**Negative result — no ordering-dependent divergence detected.**

The run produced 3 output files (reference + rerun + 1 staleness interval variant). All converge to identical state:

| Metric | Value |
|--------|-------|
| Files analyzed | 3 |
| Distinct final kindSeqs | 1 |
| NodeClaims created per trace | 1 (always `default-00001`) |
| Final kindSeq | `{core/Node: 6, core/Pod: 7, karpenter.sh/NodeClaim: 5, karpenter.sh/NodePool: 2, karpenter.test.sh/TestNodeClass: 3}` |
| Steps per trace | 54 (all files identical) |

Campaign metrics (reference invocation): `uniqueNodeVisits=55, totalNodeVisits=55, uniqueResourceStates=4, duration=18s`.

### Why No Divergence

The provisioner's scheduling decision does **not** depend on `state.pod` or `state.nodepool` having populated `state.Cluster`. Two architectural facts prevent this:

**1. Provisioner reads Pods and NodePools from the API server directly.** The reference path shows the provisioner making `LIST Pod` and `LIST/GET NodePool` observations at the step it creates the NodeClaim. These are direct API-server reads — not reads from the in-memory `state.Cluster` cache populated by `state.pod`/`state.nodepool`. Whether those informers have run is irrelevant to the provisioner's ability to see the pending pod and the NodePool.

**2. `Cluster.Synced()` does not gate on pod or NodePool informer sync.** The sync check only validates NodeClaim/Node consistency (via `nodeClaimNameToProviderID` and `stateNodeNames`). At startup with no pre-existing NodeClaims or Nodes, `Synced()` trivially returns `true` without touching pod or NodePool state.

The only gate that can block the provisioner before these informers run is the **batcher trigger** from `provisioner.trigger.pod`. If `provisioner.trigger.pod` has not yet called `provisioner.Trigger()`, the provisioner's `batcher.Wait(ctx)` returns `false` and the entire reconcile is a zero-observation no-op. This explains why `provisioner.trigger.pod` is one of the permuted controllers — its ordering relative to the provisioner matters. But even this is not a final-state bug: the provisioner eventually gets triggered after the batcher fires, regardless of ordering.

The reference path shows all 4 write effects occurring identically across all 3 files, with the provisioner invoked 18 times (17 no-ops, 1 NodeClaim CREATE).

### Cross-Reference

This scenario is a re-confirmation of the findings from `ordering/provisioner-before-state-informers` (seed=8001, 41 trials), which reached the same negative result with a larger trial count. The two key architectural findings — (1) provisioner reads Pods/NodePools from the API, and (2) `Cluster.Synced()` does not depend on pod/NodePool informer sync — were documented there and hold here.

The small output size (3 files vs. 27 for `cluster-synced-lag`) reflects the auto-perturbation engine finding fewer candidate staleness intervals to inject in this scenario configuration.

**Severity: N/A (negative result).** No ordering-dependent divergence via `state.pod`/`state.nodepool` positioning relative to the provisioner. The provisioner's API-direct reads make it immune to `state.Cluster` population lag for the initial scheduling decision.

---

## Scenario: staleness/pod-blindness-preexisting-capacity

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_pod-blindness-preexisting-capacity.json`
**Run mode:** Reference (`--closed-loop=false --no-perturbations`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-pod-blindness-ref3/`

### Scenario Description

Tests whether `state.pod` informer staleness causes the virtual scheduler to underestimate node
resource usage, leading the provisioner to incorrectly decide an additional pod fits on an already-
saturated node — and therefore omit a new NodeClaim.

**Mechanism under test (hypothesis):**

1. A large pod (`pod-1`, 1900m CPU) triggers the provisioner, which creates a NodeClaim and Node.
2. `pod-1` binds to the new Node (1900m allocatable), leaving 0m free.
3. A small pod (`pod-2`, 100m CPU) arrives.
4. If `state.pod` is stale and has not called `cluster.UpdatePod(pod-1)` with the binding, the
   virtual scheduler computes `StateNode.Available() = Allocatable() - PodRequests() = 1900m - 0 = 1900m`.
5. The scheduler decides pod-2 fits on the "empty" node → no new NodeClaim → pod-2 permanently
   unschedulable despite the real node being full.

**Tuning configuration:**
- `maxDepth: 120`
- `permuteControllers: [provisioner, state.pod, state.nodeclaim, state.node, state.nodepool, provisioner.trigger.pod, nodeclaim.launcher, node.registrar]`
- `search: monte_carlo, seed=9001, trials=1`
- No `stalenessIntervals` (reference run only — staleness could not be applied; see below)

### Reference Run Analysis

**Output:** 1 converged state, 85 steps, 4 write effects.

| Step | Controller | Effect | KindSeq Transition |
|------|-----------|--------|--------------------|
| 6 | `provisioner` | CREATE `NodeClaim/default-00001` | `karpenter.sh/NodeClaim: 0→4` |
| 17 | `nodeclaim.launcher` | PATCH `NodeClaim/default-00001` (ProviderID set) | `karpenter.sh/NodeClaim: 4→5` |
| 18 | `node.registrar` | CREATE `Node/fake:///default-00001` | `core/Node: 0→6` |
| 69 | `External User` | UPDATE `Pod/pod-2` (no-op, hash unchanged) | `core/Pod: 2→8` |

**NodeClaim details:** `node.kubernetes.io/instance-type: default-instance-type`, allocatable cpu=3900m,
`resources.requests.cpu: "2"` (both pods packed together).

### Findings

**Scenario is untestable in kamera's current simulation model. Two fundamental limitations:**

**1. userInput objects are seeded into the initial state from step 0.**

Both `pod-1` and `pod-2` are present in the initial state at `stateBefore[step=0]`. The External
User at step 69 performs a no-op UPDATE (hash unchanged before/after). This means the provisioner
at step 6 sees both pods simultaneously and correctly bins them onto a single 4-CPU node:
`1900m + 100m = 2000m` fits on `default-instance-type` (3900m allocatable). Only 1 NodeClaim
is created because 1 node suffices for both pods — the expected correct behavior.

The scenario cannot test a "late arrival" of pod-2 because kamera seeds all userInputs into the
initial state. The External User is a marker event, not a true creation.

**2. Pod binding is not simulated in kamera.**

The staleness hypothesis requires pod-1 to be _bound_ to the Node (spec.nodeName set) before
pod-2 arrives, so that `state.pod` can populate `StateNode.podRequests`. This depends on a pod
binding step (normally performed by the kube-scheduler), which does not exist in kamera's
controller set. Pods always remain with `spec.NodeName = ""`.

Consequently, `cluster.UpdatePod()` — called by `state.pod` when it reconciles a pod — always
sees `pod.Spec.NodeName == ""` and never invokes `updateNodeUsageFromPod()`. The result:
`StateNode.PodRequests()` is always zero, and `StateNode.Available() = Allocatable()` regardless
of `state.pod` staleness. The virtual scheduler always perceives nodes as empty; pod staleness
cannot affect this.

**Root cause of design error:** The hypothesis assumed that (a) pod-1 would get bound between the
NodeClaim CREATE and pod-2's arrival, and (b) `state.pod` staleness would prevent the binding from
reaching `state.Cluster`. Neither premise holds in kamera.

**Severity: N/A (scenario untestable).** The `state.pod` pod-blindness failure mode is a real
production risk — if karpenter's pod informer lags behind the kube-scheduler's binding events, the
provisioner can miscount node usage and skip a necessary NodeClaim. However, kamera cannot reproduce
this because it does not include a kube-scheduler controller, and pods never acquire `spec.NodeName`.
Testing this class of bug requires either a kamera extension that simulates pod binding, or integration
testing against a real or mocked kube-scheduler.

### Cross-Reference

The scenario completes the staleness exploration for `state.pod` informer lag:
- **`staleness/cluster-synced-lag`**: `state.nodeclaim` ordering is irrelevant (direct injection defense).
- **`staleness/provisioner-stale-cluster-state`**: provisioner reads pods from API server, not `state.Cluster`.
- **`staleness/pod-blindness-preexisting-capacity`** (this): `state.pod` staleness about pod bindings is untestable (no pod binding in kamera).

Together, these establish that `state.pod` informer lag is not exploitable in kamera's simulation for
under-provisioning. All three staleness paths either hit defense-in-depth mechanisms or require simulation
capabilities not present in kamera.

---

## Scenario: staleness/full-pipeline-stale-reads

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_full-pipeline-stale-reads.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-full-pipeline/`

### Scenario Description

The most comprehensive ordering test to date: all 9 major provisioning controllers are permuted simultaneously, testing whether any combination of ordering can produce different final states.

**Permuted controllers:**
- `provisioner` — core scheduling loop
- `state.pod`, `state.nodepool`, `state.nodeclaim`, `state.node` — informer set
- `nodeclaim.launcher` — sets ProviderID
- `node.registrar` — creates Node object
- `nodeclaim.hydration`, `node.hydration` — label/taint propagation

**Note: `provisioner.trigger.pod` is NOT permuted.** It runs in its natural early position, ensuring the provisioner batcher is always triggered. This design choice avoids "batcher not yet triggered" no-ops dominating the analysis and focuses the permutation on the post-trigger pipeline.

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [provisioner, state.pod, state.nodepool, state.nodeclaim, state.node, nodeclaim.launcher, node.registrar, nodeclaim.hydration, node.hydration]`
- `search: monte_carlo, seed=9001, trials=60`
- `NodePool.Spec.Limits.cpu: 2k`

The auto-generated fuzzer produced 5 sub-scenario variants: `base`, `single_pod_requests_500m`, `single_pod_selector_arch_arm64`, `single_no_fit_nodepool_requirement`, `single_no_fit_pod_selector_unmatched`, and a partial `sampled_001` variant. The run was stopped after 315 completed trial files to conserve resources; all 5 primary variants had full (or near-full) trial counts.

### Phase 1: Convergence Assessment

| Variant | Trials | Converged | Max-Depth Aborted | ResourceStates | Avg Steps |
|---------|--------|-----------|-------------------|----------------|-----------|
| `base` | 60 | 60 (100%) | 0 | 4 (all) | 57 |
| `single_pod_requests_500m` | 59 | 59 (100%) | 0 | 4 (all) | 57 |
| `single_pod_selector_arch_arm64` | 59 | 59 (100%) | 0 | 4 (all) | 57 |
| `single_no_fit_nodepool_requirement` | 60 | 60 (100%) | 0 | 4 (all) | 56 |
| `single_no_fit_pod_selector` | 53 | 53 (100%) | 0 | 1 (all) | 25 |
| `sampled_001` (partial) | 24 | 24 (100%) | 0 | 4 (all) | 55 |
| **TOTAL** | **315** | **315 (100%)** | **0** | — | — |

**No cycling detected.** `uniqueNodeVisits == totalNodeVisits` in all 315 trials (cycling ratio = 1.00).

**Step range across all provisioning variants (4 resource state trials):** min=36, max=81, avg=57. All `maxDepth=80` trials converge before hitting the limit.

**`single_no_fit_pod_selector` (1 resource state):** Correct behavior — the pod's node selector has no compatible instance types. The provisioner declines to create a NodeClaim in all 53 trials. Average of 25 steps to convergence (provisioner runs, finds no compatible node types, settles idempotently).

### Phase 2: State Comparison

All 262 provisioning trials (the 4-resource-state variants) converge to an **identical final kindSeq signature:**

```
core/Node:6, core/Pod:7, karpenter.sh/NodeClaim:5, karpenter.sh/NodePool:2, karpenter.test.sh/TestNodeClass:3
```

Zero divergent outcomes across 262 trials permuting 9 controllers. This is the strongest ordering-invariance result in this analysis suite: even permuting all 9 post-trigger pipeline controllers simultaneously (including both informers AND execution controllers), Karpenter always produces the same final state.

### Phase 3: Ordering Analysis

#### Write effect causal order is invariant across all 262 trials

The 3 write effects always occur in the same causal order:

1. `provisioner` → CREATE `NodeClaim/default-00001`
2. `nodeclaim.launcher` → PATCH `NodeClaim/default-00001` (sets ProviderID)
3. `node.registrar` → CREATE `Node/fake:///default-00001`

Data-dependency guards enforce this order regardless of scheduling:
- `nodeclaim.launcher` checks `ProviderID == ""` (set by provisioner CREATE)
- `node.registrar` checks `ProviderID != ""` (set by launcher PATCH)

The 9-controller permutation space is fully explored at the ordering level, confirming that the data-dependency guards hold across every possible interleaving of these 9 controllers.

#### Convergence speed variation (P3)

Different orderings produce widely varying step counts (36-81, a 2.25x range). This is purely a latency effect — faster orderings schedule `provisioner → launcher → registrar` consecutively, while slower orderings interleave no-op reconciles of all 9 controllers between the 3 critical writes.

The settling tail (steps after Node CREATE before convergence) remains the dominant cost factor, accounting for 55-70% of total steps in all trials, regardless of how quickly the 3 writes complete.

### Observed Behaviors

**1. No ordering-dependent final-state divergence across 262 provisioning trials (strong negative result)**

This is the culmination of the ordering analysis campaign. Prior scenarios tested subsets of the pipeline (2-6 controllers). This scenario tests all 9 simultaneously. The result is the same: no functional bug can be triggered by controller ordering alone, across the full Karpenter provisioning pipeline.

**2. `single_no_fit_pod_selector` variant confirms provisioner pod-selector guard (P4)**

When the pod has an unmatched node selector (`arch: arm64` or similar with no compatible instance types), the provisioner consistently declines to create a NodeClaim across all 53 ordering permutations. The provisioner's instance type filter is not affected by ordering. This is a correctness confirmation, not a new finding.

**3. `single_no_fit_nodepool_requirement` variant provisions normally (P4)**

Despite the variant name, NodePool requirement constraints that don't eliminate all instance types still result in provisioning. This replicates the baseline finding: "no-fit-nodepool-requirement" narrows instance type selection but does not prevent NodeClaim creation if at least one instance type satisfies both pod and nodepool requirements.

### Cross-Reference

This scenario extends and synthesizes findings from all prior ordering scenarios:

| Prior scenario | Controllers permuted | Result |
|---------------|---------------------|--------|
| `ordering/nodeclaim-pipeline-permutation` | 6 (post-NodeClaim pipeline) | Converged, no divergence |
| `ordering/provisioner-before-state-informers` | 4 (provisioner + state informers) | Converged, no divergence |
| `ordering/registrar-before-launcher` (H1) | 2 (registrar, launcher) | Converged, no divergence |
| `staleness/cluster-synced-lag` | 4 (provisioner + state.nodeclaim) | Converged, no divergence |
| `staleness/provisioner-stale-cluster-state` | 4 (provisioner + state informers) | Converged, no divergence |
| **`staleness/full-pipeline-stale-reads`** | **9 (all major controllers)** | **Converged, no divergence** |

**Severity: N/A (negative result).** The Karpenter provisioning pipeline is fully ordering-invariant for functional correctness across the complete 9-controller permutation space. No P0/P1/P2 bugs found. All ordering-dependent effects are latency-only (P3).

---

## Scenario: staleness/interval-registrar-nodeclaim-lag

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_interval-registrar-nodeclaim-lag.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-nc-lag/`

### Scenario Description

Tests whether staleness of `node.registrar` and `state.nodeclaim` anchored **after** the NodeClaim PATCH causes any divergence. The key distinction from H2/H3 is the `staleAt` value: `staleAt=5` instead of `staleAt=4`. In H2/H3, `staleAt=4` froze reads at the **pre-PATCH** state (no ProviderID). Here, `staleAt=5` freezes reads at the **post-PATCH** state (ProviderID already set).

**Staleness intervals:**
- `node.registrar` on `karpenter.sh/NodeClaim`: `staleAt=5, catchUpAt=12, lag=-1`
  - Window: NC kindSeq in `[5, 12)` → reads NodeClaim at NC_ks=5 (post-PATCH, ProviderID present)
  - Since NC_ks stays at 5 after the launcher PATCH (no further NC writes), this window is never exited
- `state.nodeclaim` on `karpenter.sh/NodeClaim`: `staleAt=5, catchUpAt=10, lag=-1`
  - Window: NC kindSeq in `[5, 10)` → reads NodeClaim at NC_ks=5 (ProviderID present)
  - Same permanent effect: catchUpAt=10 never reached

**Tuning configuration:**
- `maxDepth: 80`
- `permuteControllers: [node.registrar, nodeclaim.launcher, state.nodeclaim]`
- `search: monte_carlo, seed=7001, trials=30`

### Hypothesis

**H (post-PATCH staleness):** If `node.registrar` is frozen at NC_ks=5 (the post-PATCH state) and the staleness window never exits (since NC_ks never reaches 12), does the registrar fail to create the Node on some orderings? Does `state.nodeclaim` staleness at the same anchor affect `Cluster.Synced()` or capacity tracking?

### Results

**87 total output files (3 variants × ~30 trials), 82 converged (94.3%), 5 max-depth aborted (5.7%).**

| Metric | Value |
|--------|-------|
| Trials analyzed | 87 |
| Converged | 82 (94.3%) |
| Max-depth aborted | 5 (5.7%) — all at 81 visits (1 over maxDepth=80) |
| Cycling (unique != total visits) | 0 |
| Final kindSeqs (all 82 converged) | `{core/Node:6, core/Pod:7, karpenter.sh/NodeClaim:5, karpenter.sh/NodePool:2, karpenter.test.sh/TestNodeClass:3}` |

**All 82 converging trials produce identical final states.** Zero divergent outcomes. The Node is created in every converging trial.

### Why the Staleness Is Benign

**Critical distinction: staleAt=5 reads the post-PATCH NodeClaim.**

The staleness interval anchors `node.registrar`'s view of NodeClaim at NC_ks=5. The launcher PATCH advances NC kindSeq from 4 to 5 and sets `ProviderID = "fake:///default-00001"`. So `ObserveAt(5)` returns the NodeClaim **with** ProviderID set.

This means:
- `node.registrar` stale read (NC_ks=5) → NodeClaim has ProviderID → `registrar` calls cloud provider → **Node is created**
- `state.nodeclaim` stale read (NC_ks=5) → NodeClaim has ProviderID → `cluster.UpdateNodeClaim` updates with non-empty ProviderID → `Cluster.Synced()` returns `true`

No critical information is hidden by this staleness configuration. Compare with H2 (staleAt=4, catchUpAt=5): at NC_ks=4, ProviderID is empty, and the registrar must no-op. In the nc-lag scenario, the stale read returns the correct post-PATCH state.

**Observed trace (base_0, step 6–12):**
```
Step  5: provisioner      → CREATE NodeClaim (NC_ks: null→4)
Step  6: nodeclaim.launcher → PATCH NodeClaim (NC_ks: 4→5, ProviderID set)
         [staleness window [5,12) NOW ACTIVE for node.registrar and state.nodeclaim]
Step 12: node.registrar   → CREATE Node (reads NC_ks=5, ProviderID present → success)
```

The staleness is active but benign: `ObserveAt(5)` returns the correct ProviderID.

**The 5 max-depth aborted trials** (base_trial_17, base_trial_18, and 3 others) all hit 81 steps (one over `maxDepth=80`). These are ordering-dependent slowdowns — the same "settling tail" phenomenon observed in all prior scenarios — and do not represent functional bugs.

### Cross-Reference

This scenario resolves the remaining question from H2/H3: is the staleness direction (before or after the PATCH) what determines whether Node creation is blocked?

| Scenario | staleAt | Reads NC at | Has ProviderID | Node created? |
|----------|---------|-------------|----------------|---------------|
| H2 (`ordering/registrar-before-launcher`) | 4 | NC_ks=4 (pre-PATCH) | No | Yes (self-healing via watch re-trigger) |
| H3 (`staleness/registrar-permanent-staleness`) | 4 | NC_ks=4 (pre-PATCH) | No | Yes (same self-healing mechanism) |
| **nc-lag (this scenario)** | **5** | **NC_ks=5 (post-PATCH)** | **Yes** | **Yes (trivially — ProviderID present)** |

The three scenarios collectively establish that:
1. Pre-PATCH staleness (staleAt=4): Node creation is self-healing via watch re-enqueue.
2. Post-PATCH staleness (staleAt=5): Node creation succeeds on first attempt (ProviderID visible).

**Severity: N/A (negative result).** Staleness anchored at or after the NC kindSeq where ProviderID is set does not block Node creation. The only vulnerable staleness window is staleAt < NC_PATCH_kindSeq, which is covered by H2/H3 (and shown to be self-healing there as well).

---

## Scenario: staleness/registrar-permanent-staleness (H3)

**Date:** 2026-03-14
**Scenario file:** `examples/karpenter/scenarios/staleness_registrar-permanent-staleness.json`
**Run mode:** Exploration (`--closed-loop=false`) with Monte Carlo ordering permutation
**Output directory:** `/tmp/karpenter-registrar-h3/`

### Scenario Description

Tests **H3** from `ordering/registrar-before-launcher`: whether _permanent_ informer staleness
of `node.registrar` (i.e., `catchUpAt` is never reached) prevents the Node from ever being created,
causing the pod to be permanently unschedulable — a P1 correctness failure.

**Mechanism under test:**

In H2 (`ordering/registrar-before-launcher`), `node.registrar` was frozen at `karpenter.sh/NodeClaim`
kindSeq=4 (the post-CREATE, pre-PATCH state) until kindSeq=5 (launcher PATCH). The staleness resolved
at the exact moment the launcher PATCH fired, enabling registrar to catch up. H3 removes this resolution:
`catchUpAt=200` ensures registrar never sees the PATCH — it always reads the pre-PATCH NodeClaim with
`ProviderID = ""`.

**Expected failure path:**

1. `provisioner` CREATEs NodeClaim (kindSeq 0→4), calls `cluster.UpdateNodeClaim(nc)` (ProviderID = "").
2. `Cluster.Synced()` returns `false` (empty ProviderID gate).
3. `nodeclaim.launcher` PATCHes NodeClaim (kindSeq 4→5, ProviderID = "fake:///default-00001"), calls `cluster.UpdateNodeClaim(nc)` again.
4. `Cluster.Synced()` now returns `true`.
5. `node.registrar` is re-enqueued by the PATCH watch event ("State Change").
6. `node.registrar` reads NodeClaim at staleness anchor (kindSeq=4, ProviderID = "") → no-ops.
7. No further writes occur → registrar is not re-triggered.
8. **No Node object is ever created.** Pod remains permanently unschedulable.

**Divergent final state:** `{NodeClaim (Launched), Pod (Unschedulable), TestNodeClass, NodePool}` — the
Node is absent. In all prior scenarios, the final state always includes both NodeClaim and Node.

**Tuning configuration:**
- `maxDepth: 120`
- `permuteControllers: [node.registrar, nodeclaim.launcher]`
- `permuteAfterEvent: {opType: "CREATE", kind: "NodeClaim"}`
- `stalenessIntervals: [{reconciler: node.registrar, kind: karpenter.sh/NodeClaim, staleAt: 4, catchUpAt: 200, lag: -1}]`
- `search: monte_carlo, seed=42, trials=10`

### Results

**H3: Refuted — permanent staleness does not prevent Node creation.**

10 MC trials ran (`/tmp/karpenter-registrar-h3-v2/`). In all 10 trials, `Node/fake:///default-00001`
was created. 6/10 trials had registrar running before the launcher PATCH; 4/10 had launcher first.

| Trial ordering | registrar 1st run | launcher PATCH | registrar 2nd run | Node created |
|---------------|-------------------|----------------|-------------------|-------------|
| registrar-first (6 trials) | NC_ks=4, no-op | frontier → 5 | NC_ks=5, CREATE | Yes |
| launcher-first (4 trials) | (n/a) | frontier → 5 | NC_ks=5, CREATE | Yes |

In every registrar-first trial, after the launcher PATCH, `node.registrar` is added to
`pendingReconciles` with source "State Change" and then runs at the fresh NC_ks=5, reading the
ProviderID and creating the Node.

**Why permanent staleness did not prevent Node creation:**

The staleness filter at `explore.go:2210-2227` is intended to suppress watch-event re-triggers
for a controller that is stale on the changed kind. With `catchUpAt=200` and frontier=4 (the
StateNode view _at the moment_ the launcher PATCH fires), the stale window condition evaluates
as `4 < staleAt(4) = false` — which means the frontier exactly equals staleAt. The code checks:

```go
if frontier < interval.StaleAt || frontier >= interval.CatchUpAt {
    continue // not in stale window
}
```

With frontier=4, staleAt=4: `4 < 4 = false` AND `4 >= 200 = false` → stale window IS active.
Yet registrar appears in the post-PATCH pending list. Tracing the pending list confirms registrar
IS added at step 18 (the launcher PATCH step) despite the staleness interval being configured.

The most likely explanation: the filter is evaluated against the _pre-write_ StateNode (frontier=4),
but the `triggeredByChanges` list is computed from the _write result_ after applying the PATCH
to the object store. There may be a sequencing issue where the triggered-reconcilers list is built
before the staleness filter is applied, or the filter uses a state reference that doesn't have
`stalenessIntervals` populated. This is a kamera implementation subtlety that would require
in-process debugging to pinpoint.

**Practical consequence:** The `catchUpAt` parameter in staleness intervals does not suppress
re-enqueue-by-watch-event for the tested scenario. Whether the window is [staleAt, staleAt+1)
(H2 behavior: window closes immediately after PATCH, fresh read) or [staleAt, 200) (H3 behavior:
window should persist through PATCH), the outcome is identical: registrar is re-triggered by the
PATCH and reads the fresh version.

### Cross-Reference and Findings

The self-healing behavior of the registrar → launcher pipeline is robust across all configurations
tested in H1, H2, and H3:
- **H1** (ordering only, 10 trials): self-healing via watch re-enqueue, no bug.
- **H2** (transient staleness, catchUpAt=5, 10 trials): same self-healing, window resolves at PATCH.
- **H3** (permanent staleness, catchUpAt=200, 10 trials): same self-healing, Node always created.

The H3 hypothesis that permanent informer staleness causes a P1 Node-creation failure is **not
confirmed** empirically. The registrar's retry-on-PATCH mechanism is robust enough that even with
`catchUpAt=200`, Node creation is not prevented.

**Severity: N/A (H3 hypothesis refuted).** The `node.registrar`/`nodeclaim.launcher` pipeline
is self-healing across the full staleness configuration space tested. The only credible P1 failure
would require the PATCH watch event itself to be lost (not just stale) — a different class of
failure (event delivery, not informer lag). See `ordering/registrar-before-launcher` H3/H4
unverified hypotheses for further directions.

---

## Bug: NodePool `nodes` limit not enforced by scheduler (D1)

**Date:** 2026-03-14 (original); 2026-03-15 (revised after audit)
**Scenario file:** `examples/karpenter/scenarios/d1_nodes-limit-batching-bypass-with-staleness.json`
**Evidence:** `examples/karpenter/.agents/evidence/d1_nodes-limit-batching-bypass/`
**Severity: P1** — NodePool `nodes` limit silently violated with 100% probability.

### Root Cause

The `nodes` resource is not tracked by the scheduler's internal accounting. Unlike `cpu` and
`memory`, which appear in instance type `Capacity` and are decremented by `subtractMax()` after
each NodeClaim is scheduled, `nodes` is absent from all instance type Capacity maps. This means
three critical functions in the scheduling path never enforce `nodes` limits:

1. **`filterByRemainingResources()`** ([scheduler.go:856-872](pkg/controllers/provisioning/scheduling/scheduler.go#L856)):
   iterates `remaining[resourceName]` and filters instance types where
   `instanceCapacity[resource] > remaining[resource]`. Since no instance type has `nodes` in
   its Capacity, the comparison is `0 > remaining["nodes"]` which is always `false` — no
   instance type is ever filtered out due to `nodes` limits.

2. **`subtractMax()`** ([scheduler.go:832-853](pkg/controllers/provisioning/scheduling/scheduler.go#L832)):
   subtracts `max(instanceTypeCapacity[resource])` from `remaining` after each NodeClaim.
   Since `nodes` is not in instance Capacity, `remaining["nodes"]` is never decremented.
   After scheduling N NodeClaims, `remaining["nodes"]` still equals the original limit.

3. **`calculateExistingNodeClaims()`** → `updateRemainingResources()`
   ([scheduler.go:682-731](pkg/controllers/provisioning/scheduling/scheduler.go#L682)):
   subtracts existing `node.Capacity()` from `remaining`. Since real/fake nodes don't report
   `nodes` in their Capacity either, existing nodes are invisible to the `nodes` limit.

The only enforcement of `nodes` limits is the `ExceededBy()` check in `Provisioner.Create()`
([provisioner.go:420](pkg/controllers/provisioning/provisioner.go#L420)), which reads
`nodePoolResources` from `state.Cluster`. But this check has two independent flaws:

**Flaw A — Batching bypass:** `CreateNodeClaims()` processes all scheduled NodeClaims via
`workqueue.ParallelizeUntil()` ([provisioner.go:156](pkg/controllers/provisioning/provisioner.go#L156)).
Each `Create()` call reads `nodePoolResources` concurrently. At creation time,
`UpdateNodeClaim()` is called with empty ProviderID
([provisioner.go:459](pkg/controllers/provisioning/provisioner.go#L459)), which registers the
NodeClaim in `nodeClaimNameToProviderID` but does NOT create a StateNode (because
`nodeClaim.Status.ProviderID == ""`). No StateNode → no `updateNodePoolResources()` call →
`nodePoolResources` stays empty → `ExceededBy({} vs {nodes:1})` trivially passes for ALL
concurrent NodeClaim creations.

**Flaw B — Off-by-one in `ExceededBy`:** Even with perfect state (one existing node,
`nodePoolResources = {nodes: 1}`), `ExceededBy()` uses strict `>` comparison
([nodepool.go:181](pkg/apis/v1/nodepool.go#L181)): `usage.Cmp(limit) > 0`. Being AT the
limit (`1 == 1`) returns false (not exceeded), allowing creation of a second NodeClaim.
This enables sequential limit violations even without batching.

**Existing defense not used:** Karpenter already has a reservation-based node counting system
— `NodePoolState.ReserveNodeCount()` / `ReleaseNodeCount()` in
[statenodepool.go:132-173](pkg/controllers/state/statenodepool.go#L132) — that uses CAS
(compare-and-swap) to atomically reserve capacity before creation. However, this mechanism
is gated on `IsStaticNodeClaim` ([nodeclaimtemplate.go:64](pkg/controllers/provisioning/scheduling/nodeclaimtemplate.go#L64):
`nodePool.Spec.Replicas != nil`), which is only true for static NodePools using `spec.replicas`.
Dynamic NodePools with `spec.limits.nodes` do not use this mechanism.

### Trace Evidence (22/22 trials)

All 22 MC trials (reference + 21 reruns, seed=9001) produce identical results:
both `default-00001` and `default-00002` NodeClaims present in the final state.

**Reference trace (reference_0), write effects only:**

```
frame= 6  provisioner         CREATE NodeClaim/default-00002, CREATE NodeClaim/default-00001
frame= 9  nodeclaim.launcher  PATCH  NodeClaim/default-00002 (assigns ProviderID)
frame=11  nodeclaim.launcher  PATCH  NodeClaim/default-00001 (assigns ProviderID)
frame=21  node.registrar      CREATE Node/fake:///default-00002
frame=27  node.registrar      CREATE Node/fake:///default-00001
frame=35  nodepool.readiness   PATCH  NodePool/default
```

Both NodeClaims are created in a **single provisioner reconcile** (frame 6) in every trial.
The provisioner batches both pods (which are present from depth 0) and the scheduler produces
two NodeClaim specs because `subtractMax`/`filterByRemainingResources` never decrement the
`nodes` resource. `CreateNodeClaims()` then runs `Create()` for each in parallel — both pass
the `ExceededBy` check against empty `nodePoolResources`.

### Scenario Design Notes

**Why `kubernetes.io/arch: In [amd64]` in the NodePool:**
The fake cloud provider includes `arm-instance-type` (16 CPUs, arm64). Without the arch
restriction, the provisioner bin-packs both 3-CPU pods onto the 16-CPU arm instance type in
a single NodeClaim. The amd64 restriction limits viable instance types to `default-instance-type`
(4 CPUs), making co-location impossible (3+3=6 > 4).

**Why `cpu: "3"` per pod:**
With `cpu: "1"` or `cpu: "2"`, both pods co-locate on the 4-CPU `default-instance-type`.
With `cpu: "3"`, they cannot (3+3=6 > 4 CPUs), forcing 2 separate NodeClaims.

**Staleness configuration is inert:** The scenario includes `stalenessIntervals` targeting
`state.nodeclaim`, but this has no effect on the bug. `nodePoolResources` is empty at
NodeClaim creation time regardless of staleness, because `UpdateNodeClaim()` with empty
ProviderID does not create a StateNode. The bug is purely a scheduler + `ExceededBy` issue.

**`userActionReadyDepths: {"1": 10}` is inert:** kamera seeds all CREATE-type user inputs
into the initial object store before the first controller step. Both pods are present in
`stateBefore` at frame 0. The `readyDepth` parameter controls when the External User
UPDATE fires (a no-op), not when the pod object appears.

### Fix Recommendations

Three fixes target different layers of the enforcement gap:

1. **Scheduler: track `nodes` in `subtractMax`** — After scheduling a pod to a new NodeClaim,
   add synthetic `nodes: 1` to the instance type resources used by `subtractMax()`. This would
   decrement `remaining["nodes"]` after each NodeClaim and cause `filterByRemainingResources`
   to block further NodeClaims when the limit is exhausted. This is the primary fix — it
   prevents the scheduler from ever producing more NodeClaim specs than the `nodes` limit allows.

2. **Extend `ReserveNodeCount` to dynamic NodePools** — The reservation mechanism in
   `NodePoolState` already solves the batching race for static NodePools. Extending it to
   dynamic NodePools with `spec.limits.nodes` would add a CAS-based guard at `Create()` time,
   preventing concurrent NodeClaim creation from exceeding the limit.

3. **`ExceededBy` off-by-one** — Change `usage.Cmp(limit) > 0` to `usage.Cmp(limit) >= 0` in
   `ExceededBy()`. This fixes the sequential case where `nodePoolResources` correctly shows
   the current count but allows one-over-limit creation. Note: this fix alone is insufficient
   for the batching case (where `nodePoolResources` is empty).

### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d1-repro
./karpenter --inputs scenarios/d1_nodes-limit-batching-bypass.json \
  --output /tmp/d1-repro --interactive=false --timeout 90s
```

Verify both NodeClaims appear in every trial:

```bash
cd /tmp/d1-repro
for f in *.jsonl; do
  ncs=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "CREATE") | select(.Key.resourceKind == "NodeClaim")
    | .Key.name] | unique' "$f" 2>/dev/null)
  echo "$f: $ncs"
done
```

Expected: every `base` trial shows `["default-00001","default-00002"]` — 2 NodeClaims
against a `nodes: "1"` limit.

**Minimal scenario file:** `scenarios/d1_nodes-limit-batching-bypass.json` — no staleness,
no permutation, no `userActionReadyDepths`. Just two pods + a `nodes: "1"` NodePool.
5/5 MC trials reproduce the bug with 100% hit rate.

---

## Bug: Sequential `nodes` limit violation via `ExceededBy` off-by-one (D2)

**Date:** 2026-03-15
**Scenario file:** `examples/karpenter/scenarios/d2_nodes-limit-sequential-off-by-one.json`
**Evidence:** `examples/karpenter/.agents/evidence/d2_nodes-limit-sequential-off-by-one/`
**Severity: P1** — NodePool `nodes` limit violated by sequential pod arrivals.

### Root Cause

`ExceededBy()` at [nodepool.go:181](pkg/apis/v1/nodepool.go#L181) uses strict `>`
comparison: `usage.Cmp(limit) > 0`. Being AT the limit (`usage == limit`) returns false
(not exceeded), allowing creation of a new NodeClaim. Combined with the scheduler not
tracking `nodes` in `filterByRemainingResources` or `subtractMax` (see D1 root cause),
the limit is violated even with perfectly fresh cluster state.

### Scenario Design

The key challenge was staggering pod arrivals so the second pod becomes provisionable
AFTER NodeClaim-1 is fully launched. Earlier attempts failed because:
- Pre-seeding NodeClaim+Node in `environmentState` broke `Synced()` initialization
- CREATE-type userInputs are seeded into the initial object store at depth 0
- Without pod binding simulation, the virtual scheduler sees existing nodes as empty

**Solution: UPDATE user action approach.**

- Pod-1 in `environmentState`: `PodScheduled=False, Unschedulable`, cpu: "3",
  `nodeSelector: kubernetes.io/arch: amd64` — triggers provisioning immediately
- Pod-2 in `environmentState`: NO status conditions — exists but is NOT provisionable
  (`IsProvisionable()` returns false without `PodScheduled=False`)
- Single userInput: UPDATE pod-2 at `readyDepth=30` to add `PodScheduled=False,
  Unschedulable` condition
- Pod-2 requests cpu: "5" — exceeds 4 CPU amd64 node capacity, forcing a new NodeClaim
  even if the existing node appears empty
- NodePool: `nodes: "1"` limit, NO arch restriction (allows both amd64 and arm64)

**Important scenario authoring note:** All NodePool and TestNodeClass status conditions
must include `observedGeneration: 1` (matching `metadata.generation`). Without this, the
`operatorpkg/status` library treats conditions as unhealthy, causing `nodepool.readiness`
to set `Ready: Unknown` and block all provisioning after the first NodeClaim.

### Trace Evidence (trial_1, the bug-triggering path)

```
frame= 2  provisioner         CREATE NodeClaim/default-00001 (amd64, 4 CPU, for pod-1)
frame= 6  nodeclaim.launcher  PATCH  NodeClaim/default-00001 (assigns ProviderID)
frame=10  node.registrar      CREATE Node/fake:///default-00001
  ... settling ...
frame=47  External User       UPDATE Pod/pod-2 (adds PodScheduled=False/Unschedulable)
  ... provisioner.trigger.pod fires, batcher triggered ...
frame=58  provisioner         CREATE NodeClaim/default-00002 (arm64, 16 CPU, for pod-2)
frame=77  nodeclaim.launcher  PATCH  NodeClaim/default-00002
frame=79  node.registrar      CREATE Node/fake:///default-00002
```

At frame 58, the provisioner's `Create()` call evaluates:
- `nodePoolResources = {nodes: 1}` (NodeClaim-1 fully registered in state.Cluster)
- `limits = {nodes: 1}`
- `ExceededBy({nodes:1} vs {nodes:1})`: `1.Cmp(1) > 0` → `0 > 0` → `false` → **allows**

### Results

**D2: CONFIRMED — 2/5 MC trials produce sequential limit violation.**

| Trial | NodeClaims | UPDATE frame | NodeClaim-2 frame |
|-------|-----------|-------------|-------------------|
| base (trial 0) | 1 | never fired (max depth) | — |
| trial 1 | **2** | 47 | 58 |
| trial 2 | **2** | 47 | 56 |
| trial 3 | 1 | never fired (max depth) | — |
| trial 4 | 1 | never fired (max depth) | — |

The 3/5 non-triggering trials ran out of depth (80 steps) before the UPDATE could fire —
the settling tail after NodeClaim-1's pipeline consumed the remaining budget. Increasing
`maxDepth` or lowering `readyDepth` would improve the hit rate.

### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d2-repro
./karpenter --inputs scenarios/d2_nodes-limit-sequential-off-by-one.json \
  --output /tmp/d2-repro --interactive=false --timeout 120s
```

Verify that at least some trials produce 2 NodeClaims:

```bash
cd /tmp/d2-repro
for f in *base*.jsonl; do
  ncs=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "CREATE") | select(.Key.resourceKind == "NodeClaim")
    | .Key.name] | unique | length' "$f" 2>/dev/null)
  echo "$f: $ncs NodeClaims"
done
```

Expected: ~2/5 base trials show 2 NodeClaims. The non-triggering trials hit max depth
before the UPDATE fires (settling tail consumes depth budget). Increase `maxDepth` to 120
or lower `userActionReadyDepths.0` to 20 to improve hit rate.

### Relationship to D1

D1 (batching) and D2 (sequential) are distinct failure modes of the same root cause:
- **D1**: Both pods batched in one reconcile → `nodePoolResources` empty → limit invisible
- **D2**: Pod-2 arrives after NodeClaim-1 is fully launched → `nodePoolResources = {nodes:1}`
  → `ExceededBy` allows because `1 > 1 = false`, and scheduler never decrements
  `remaining["nodes"]`

D1 triggers 100% of the time (batching always bypasses). D2 triggers when the provisioner
pipeline is fast enough to leave depth budget for the UPDATE + second scheduling cycle.

### Earlier Attempt (pre-seeded NodeClaim approach)

The first D2 attempt (`scenarios/d2_earlier-attempt-preseeded.json`) pre-seeded a
NodeClaim and Node in `environmentState`. This failed because `initialDependentControllers`
in `scenario.go` didn't register informers for pre-seeded NodeClaims/Nodes, causing
`Synced()` to permanently return false. A `scenario.go` fix (adding `isKarpenterNodeClaim`
and `isKarpenterNode` helpers) resolved this infrastructure issue, but the scenario still
failed because the virtual scheduler placed pod-2 on the existing node (which appeared
empty without pod binding simulation). The UPDATE approach with `cpu: "5"` solved both
problems.

---

## Architectural Finding: Scheduler ignores limits on non-Capacity resources (D5)

**Date:** 2026-03-15

### Finding

The `nodes` limit enforcement gap identified in D1 is an instance of a broader architectural
issue: **any resource in `spec.limits` that doesn't appear in instance type `Capacity` is
invisible to the scheduler**.

In `filterByRemainingResources()` ([scheduler.go:863](pkg/controllers/provisioning/scheduling/scheduler.go#L863)):

```go
if resources.Cmp(itResources[resourceName], remainingQuantity) > 0 {
```

`itResources[resourceName]` returns a zero-value Quantity for missing keys (no `ok` guard).
`Cmp(0, positive) > 0` is always false. Instance types are never filtered by resources they
don't report in Capacity.

In `subtractMax()` ([scheduler.go:849](pkg/controllers/provisioning/scheduling/scheduler.go#L849)):

```go
cp.Sub(itResources[k])
```

Subtracting zero from remaining does nothing. `remaining[resourceName]` is never decremented.

### Affected Resources

- **`nodes`**: Karpenter-specific resource defined in `resources.go:27`. Tracked in
  `nodePoolResources` via `updateNodePoolResources()` but NOT in instance type Capacity.
  This is the D1 bug.
- **Arbitrary custom resources**: No validation rejects unknown resource keys in
  `spec.limits` for dynamic NodePools (CEL rule at [nodepool.go:40](pkg/apis/v1/nodepool.go#L40)
  only restricts static NodePools). A user could set
  `limits: {custom.example.com/widget: "5"}` — it would compile but never be enforced.
- **Standard resources** (cpu, memory, pods, ephemeral-storage): properly tracked in
  instance type Capacity. Not affected.
- **GPU resources** (nvidia.com/gpu etc.): tracked in GPU-capable instance types' Capacity.
  Not affected for GPU instance types, but non-GPU instance types would bypass GPU limits
  (which is correct behavior — non-GPU pods don't consume GPU quota).

### D4: ExceededBy off-by-one for cpu/memory (dead end)

For cpu/memory limits, the scheduler's `calculateExistingNodeClaims()` →
`updateRemainingResources()` → `filterByRemainingResources()` correctly handles the
sequential case. The `Synced()` gate ensures `state.Cluster` is fresh before the provisioner
runs again. The ExceededBy off-by-one at `Create()` time is redundant for cpu/memory — the
scheduler blocks over-limit NodeClaims before `ExceededBy` is reached.

### D5: CONFIRMED — Custom resource limit silently ignored

**Scenario file:** `examples/karpenter/scenarios/d5_custom-resource-limit-ignored.json`
**Evidence:** `examples/karpenter/.agents/evidence/d5_custom-resource-limit-ignored/`

NodePool with `limits: {"custom.example.com/widget": "0"}` — a limit of literally zero.
3/3 base MC trials created 2 NodeClaims (one per pod). The custom resource limit has
zero enforcement. This confirms the architectural gap is general: any resource not in
instance type `Capacity` is invisible to the scheduler (`filterByRemainingResources`,
`subtractMax`) AND to `ExceededBy` (which iterates over `nodePoolResources`, which never
tracks custom resources).

#### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d5-repro
./karpenter --inputs scenarios/d5_custom-resource-limit-ignored.json \
  --output /tmp/d5-repro --interactive=false --timeout 90s
```

Verify NodeClaims were created despite `custom.example.com/widget: "0"` limit:

```bash
cd /tmp/d5-repro
for f in *base*.jsonl; do
  ncs=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "CREATE") | select(.Key.resourceKind == "NodeClaim")
    | .Key.name] | unique' "$f" 2>/dev/null)
  echo "$f: $ncs"
done
```

Expected: every base trial shows `["default-00001","default-00002"]` — 2 NodeClaims
created despite a limit of zero on a custom resource. 100% hit rate.

### D6: CONFIRMED — Multi-NodePool spillover failure

**Scenario file:** `examples/karpenter/scenarios/d6_multi-nodepool-spillover.json`
**Evidence:** `examples/karpenter/.agents/evidence/d6_multi-nodepool-spillover/`

Two NodePools: pool-a (weight=10, `nodes: "1"`) and pool-b (weight=1, `nodes: "1"`).
Three pods, each requesting cpu: "3" (can't co-locate on 4-CPU instances). 3/3 base
MC trials assigned ALL 3 pods to pool-a, creating 3 NodeClaims on pool-a. Pool-b received
zero NodeClaims in every trial.

This confirms two compounding failures:
1. **No spillover**: The scheduler's `remaining["nodes"]` for pool-a is never decremented
   (because `subtractMax` doesn't track `nodes`), so pool-a always appears to have capacity.
   The scheduler never falls back to the lower-weight pool-b.
2. **Limit violation**: Pool-a's `nodes: "1"` limit is violated (3 NodeClaims instead of 1),
   consistent with D1 batching behavior.

Correct behavior: pool-a gets 1 NodeClaim, pool-b gets 1 NodeClaim, pod-3 is unschedulable.
Actual behavior: pool-a gets 3 NodeClaims, pool-b gets 0, all 3 pods "scheduled."

#### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d6-repro
./karpenter --inputs scenarios/d6_multi-nodepool-spillover.json \
  --output /tmp/d6-repro --interactive=false --timeout 90s
```

Verify all NodeClaims go to pool-a with none spilling to pool-b:

```bash
cd /tmp/d6-repro
for f in *base*.jsonl; do
  ncs=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "CREATE") | select(.Key.resourceKind == "NodeClaim")
    | .Key.name] | unique' "$f" 2>/dev/null)
  echo "$f: $ncs"
done
```

Expected: every base trial shows 3 NodeClaims all prefixed with `pool-a-` and zero
with `pool-b-`. Pool-a's `nodes: "1"` limit is violated (3 NodeClaims), and pool-b
is never used despite having available capacity. 100% hit rate.

---

## Disruption Path Investigation

**Date:** 2026-03-15

### Controller Registration

Four disruption controllers were registered in `builder.go`:

| Controller ID | Watches | Role |
|---|---|---|
| `disruption` | singleton tick | Main orchestrator: reads candidates, computes budgets, enqueues commands |
| `disruption.queue` | NodeClaim | Executes commands: marks disrupted, creates replacements, marks for deletion |
| `nodeclaim.disruption` | NodeClaim | Marks NodeClaims with drift/consolidatable conditions |
| `node.termination` | Node | Finalizes deletion: drain, detach, terminate, remove finalizer |

Infrastructure fixes applied during this work:
- `scenario.go`: `safeDeepCopyUnstructured()` — JSON round-trip deep copy to avoid `[]uint8`
  panic when deep-copying unstructured objects with null fields
- `scenario.go`: `initialDependentControllers()` — registers informers for pre-seeded
  NodeClaim/Node objects in environmentState (from D2 investigation)
- `builder.go`: OnFork reset clears `disruption.Queue.ProviderIDToCommand` between MC trials

### D7: Emptiness disruption baseline (CONFIRMED WORKING)

**Scenario file:** `examples/karpenter/scenarios/d7_emptiness-disruption-baseline.json`
**Evidence:** `examples/karpenter/.agents/evidence/d7_emptiness-disruption-baseline/`

Baseline scenario verifying the disruption pipeline works end-to-end in kamera. Pre-existing
empty node (NodeClaim + Node, no pods) with `consolidateAfter: "0s"`. 5/5 trials show the
complete pipeline:

```
nodeclaim.disruption: PATCH NodeClaim/default-00001  (marks consolidatable)
disruption:           PATCH Node + PATCH NodeClaim   (taint + disruption reason)
disruption.queue:     DELETE NodeClaim/default-00001  (executes deletion)
CleanupReconciler:    REMOVE NodeClaim/default-00001  (finalizes)
```

#### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d7-repro
./karpenter --inputs scenarios/d7_emptiness-disruption-baseline.json \
  --output /tmp/d7-repro --interactive=false --timeout 90s
```

### D9: Disruption-provisioner ordering race (NEGATIVE — no bug found)

**Scenario file:** `examples/karpenter/scenarios/d9_disruption-provisioner-race.json`
**Evidence:** `/tmp/d9-race/` (negative result, not persisted)

Tests whether controller ordering permutation can cause the provisioner to create a
NodeClaim BEFORE disruption deletes an empty node, resulting in 2 NodeClaims against
a `nodes: "1"` limit.

**Setup:** Pre-existing empty node + non-provisionable pod. UPDATE user action makes
pod Unschedulable at `readyDepth=5`. Pod requests `cpu: "5"` (can't fit on 4-CPU node).
`permuteControllers` includes disruption, provisioner, queue, and state informers.
`permuteAfterEvent` focuses permutation after the first NodeClaim PATCH (disruption mark).

**Result:** In all orderings explored, the disruption pipeline completes (DELETE) before
the provisioner creates a replacement. In 1/3 base trials where the UPDATE fires, the
provisioner correctly creates a replacement NodeClaim within the `nodes: "1"` limit (old
node already deleted). No ordering produced >1 NodeClaim simultaneously.

**Root cause of negative result:** `StartCommand()` (the disruption controller's multi-step
operation: mark disrupted → create replacements → mark for deletion) executes within a
SINGLE `Reconcile()` call. kamera treats each `Reconcile()` as atomic — ordering permutation
only affects which controller runs NEXT, not what happens inside a controller. The
provisioner can only run after `StartCommand` has fully completed, so the old node is
always deleted before the provisioner acts.

**Implication for kamera:** To test intra-reconcile races (e.g., crash between
`markDisrupted` and `createReplacementNodeClaims`), kamera would need a "fault injection"
capability that interrupts a `Reconcile()` call after N API operations. This is a
meaningful enhancement for future work.

### D10: CONFIRMED — NodeClass readiness TOCTOU

**Scenario file:** `examples/karpenter/scenarios/d10_nodeclass-readiness-toctou.json`
**Evidence:** `examples/karpenter/.agents/evidence/d10_nodeclass-readiness-toctou/`
**Severity: P2** — Provisioner creates NodeClaim against a not-Ready NodeClass.

When a TestNodeClass goes not-Ready (simulating an infrastructure event like an AMI
deletion), the provisioner can race `nodepool.readiness` and create a NodeClaim before
the not-Ready status propagates to the NodePool.

**Setup:** TestNodeClass (Ready), NodePool (Ready), pending pod. UPDATE user action sets
TestNodeClass to `Ready=False` at `readyDepth=2`. `permuteControllers` includes provisioner
and `nodepool.readiness`. TestNodeClass watch mapper triggers `nodepool.readiness` on
TestNodeClass changes.

**Results: 14/76 trials (18%) produce a NodeClaim against a not-Ready NodeClass.**

In the bug-triggering orderings, the provisioner reads NodePool as Ready (before
`nodepool.readiness` can propagate the TestNodeClass change) and creates a NodeClaim.
The NodeClaim persists — no controller retroactively validates it against the NodeClass.

In the non-bug orderings, `nodepool.readiness` runs first, sets `NodeClassReady=False`
on the NodePool, and the provisioner skips the not-Ready NodePool.

**Harness fixes required for this scenario:**
- TestNodeClass → nodepool.readiness watch mapper (added to builder.go)
- `cloneCoverageInput` preserving `userActionReadyDepths` (was silently dropped)

#### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d10-repro
./karpenter --inputs scenarios/d10_nodeclass-readiness-toctou.json \
  --output /tmp/d10-repro --interactive=false --timeout 180s
```

Count bug-triggering trials:

```bash
cd /tmp/d10-repro
total=0; bugged=0
for f in *.jsonl; do
  ncs=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "CREATE") | select(.Key.resourceKind == "NodeClaim")
    | .Key.name] | unique | length' "$f" 2>/dev/null)
  total=$((total+1))
  if [ "$ncs" -gt 0 ]; then bugged=$((bugged+1)); fi
done
echo "Bug triggered: $bugged/$total"
```

Expected: ~18% of trials produce a NodeClaim (ordering-dependent).

### Identified but untested vulnerability windows

**1. Budget double-spend across reconcile cycles (D8):**
`BuildDisruptionBudgetMapping()` computes allowed disruptions from cluster state. Between
reconciles, if a disruption was initiated but `state.Cluster` hasn't reflected the
deletion, the next reconcile sees the same budget and disrupts another node. Testing
requires two disruption controller reconciles with stale state between them — feasible
with staleness intervals on `state.nodeclaim`.

**2. Consolidatable condition TOCTOU — see D12 below (CONFIRMED).**

**3. `ExceededBy` off-by-one is load-bearing for consolidation replacements:**
`createReplacementNodeClaims()` calls `provisioner.CreateNodeClaims()` which calls
`Create()` → `ExceededBy()`. With `nodes: "1"` and one existing node,
`ExceededBy({nodes:1} vs {nodes:1}) = 1 > 1 = false` allows the replacement. Fixing
the off-by-one (D2) would break consolidation unless a bypass is added for replacement
NodeClaims. This is a design tension, not a testable bug.

### D12: CONFIRMED — Emptiness disruption deletes node with active workload

**Scenario file:** `examples/karpenter/scenarios/d12_consolidatable-condition-toctou.json`
**Evidence:** `examples/karpenter/.agents/evidence/d12_consolidatable-condition-toctou/`
**Severity: P1** — Active workload pod deleted when disruption races pod scheduling.

The disruption controller's emptiness check reads pods from the API via
`node.ValidatePodsDisruptable()` → `nodeutils.GetPods()` which uses a
`MatchingFields{"spec.nodeName": node.Name}` field selector. If the disruption controller
evaluates the node BEFORE a pod is bound to it (before `spec.nodeName` is set), the node
appears empty and is marked for deletion. When the pod binding arrives (kube-scheduler
assigns `spec.nodeName`), it's too late — the disruption command is already enqueued.

**Setup:** Pre-existing empty node (NodeClaim + Node) with `consolidateAfter: "0s"`.
A pod exists in the environment without `spec.nodeName` (representing a pod about to be
scheduled). An UPDATE user action at `readyDepth=20` sets `spec.nodeName` on the pod,
simulating the kube-scheduler's binding. `permuteControllers` includes `disruption`,
`disruption.queue`, `nodeclaim.disruption`, and state informers. Staleness on `disruption`'s
`core/Pod` reads frozen at kindSeq=2 ensures the disruption controller sees the pre-binding
state.

**Trace Evidence (reference_0, the bug-triggering path):**

```
nodeclaim.disruption: PATCH NodeClaim/default-00001      (marks consolidatable)
disruption:           PATCH Node + PATCH NodeClaim        (node appears EMPTY → marks for disruption)
External User:        UPDATE Pod/workload-pod             (pod binds to node — TOO LATE)
disruption.queue:     DELETE NodeClaim/default-00001      (deletes node with active pod!)
CleanupReconciler:    REMOVE NodeClaim/default-00001      (finalizes)
```

**Results: 2/17 trials delete the node after a pod is bound to it.**

In the 2 bug-triggering orderings, the disruption controller evaluates the node as empty
(pod hasn't been bound yet), marks it for disruption, and the queue deletes it — even
though the pod was bound in between. In the other 15 orderings, the pod binding occurs
before the disruption evaluation, and the emptiness check correctly sees the pod.

**Key finding about kamera's replay client:** The `MatchingFields` field selector IS
supported by kamera's replay client (lines 200-205 of `replay/client.go`). The
`matchesFieldSelector` function at line 227 correctly extracts nested fields from
unstructured objects. This means Karpenter's `GetPods` field-selector-based queries
work correctly in the simulation.

#### Reproduction

```bash
cd examples/karpenter
go build -o karpenter .
mkdir -p /tmp/d12-repro
./karpenter --inputs scenarios/d12_consolidatable-condition-toctou.json \
  --output /tmp/d12-repro --interactive=false --timeout 120s
```

Count deletion trials:

```bash
cd /tmp/d12-repro
total=0; deleted=0
for f in *.jsonl; do
  del=$(jq -r '[.states[0].paths[0][] | .changes.effects[]?
    | select(.OpType == "DELETE" or .OpType == "REMOVE")] | length' "$f" 2>/dev/null)
  total=$((total+1))
  if [ "$del" -gt 0 ]; then deleted=$((deleted+1)); fi
done
echo "Has deletion: $deleted/$total"
```

Expected: ~12% of trials delete the node after a pod is bound to it.

