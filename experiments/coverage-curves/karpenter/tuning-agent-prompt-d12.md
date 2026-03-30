# Karpenter D12 Perturbation Tuning Agent Prompt

Adapted from v2 (relaxed) for the Karpenter D12 disruption TOCTOU bug.

## Prompt

```
# Perturbation Tuning Experiment -- EXECUTE NOW

You MUST execute immediately. Do NOT plan. Do NOT ask questions. Start analyzing,
creating files, running scenarios, and logging results RIGHT NOW.

## Experiment Start Time: {{TIMESTAMP}}

## Goal

You are given a scenario file for a known bug in Karpenter (the Kubernetes
autoscaler). The bug manifests as **different terminal states with different
numbers of objects** when the scenario is run with the right perturbation
configuration. Specifically, the scenario starts with a pre-existing empty node
(NodeClaim + Node) and an unbound workload pod. An external event binds the pod
to the node mid-execution. Under certain perturbation conditions, the disruption
controller incorrectly deletes the NodeClaim (seeing the node as empty because
it has a stale view of pods), destroying the node the pod was just scheduled to.

The pathological outcome has **fewer objects** in the terminal state (the
NodeClaim is deleted). The correct outcome preserves all objects.

Your job is to find the most focused perturbation configuration that reproduces
the bug, meaning the fewest controllers, tightest parameter ranges, and smallest
state space that still produces terminal states with different object counts.

There is no hard time limit per run. A run that takes a few minutes but
reproduces the bug is valuable. Prioritize finding ANY reproduction first, then
iteratively tighten the configuration.

## Bug Mechanism (hints)

This bug involves three perturbation dimensions:
1. **Ordering**: which controllers run in what sequence
2. **Staleness**: the disruption controller seeing a stale (outdated) view of
   Pod objects, missing the pod binding
3. **External event timing**: when the pod binding (UPDATE Pod with spec.nodeName)
   fires relative to the disruption controller's emptiness check

The bug is a TOCTOU (time-of-check, time-of-use) race: the disruption
controller checks if a node is empty via a field-selector pod query. If it
reads stale state that misses the recently-bound pod, it marks the node for
disruption and deletes the NodeClaim.

## Perturbation Dimensions Available

- **permuteControllers**: list of controller IDs whose execution order is
  randomized at each step (Monte Carlo mode required for Karpenter)
- **stalenessIntervals**: freeze a controller's view of a resource kind at a
  specific sequence number, simulating a stale informer cache
- **userActionReadyDepths**: the depth at which external events fire (controls
  when the pod binding happens)
- **search.mode**: must be "monte_carlo" for Karpenter (stateful controllers
  don't support DFS backtracking)
- **search.monteCarlo.trials**: number of random orderings to sample

## Inputs

- **Scenario file:** {{SCENARIO_PATH}}
- **Dependency graph:** {{DEPGRAPH_PATH}}
- **Project source code:** {{SOURCE_PATH}}
- **Harness directory:** {{HARNESS_DIR}}

## What You Must Do

### Step 1: Understand the bug surface

Read the scenario file to understand:
- What controllers are involved
- What environment objects are seeded (TestNodeClass, NodePool, NodeClaim, Node, Pod)
- What external inputs are applied (UPDATE Pod with spec.nodeName)
- What perturbations are currently configured

Read the dependency graph to understand:
- Which controllers read/write which resources
- Which controllers share resources (potential race surfaces)
- The trigger topology (what events trigger which controllers)

Read relevant controller source code to understand:
- How the disruption controller evaluates node emptiness
- How nodeclaim.disruption marks nodes as consolidatable
- How the disruption.queue executes delete commands
- What state.pod and state.node informers do
- When the lifecycle controller fires and what effects it produces

### Step 2: Design your first perturbation configuration

Based on your analysis, create a variant scenario file. Consider:
- Which controllers need to be permuted for the race to occur
- What staleness interval would cause the disruption controller to miss the pod
- At what depth the pod binding should fire to create the race window
- How many Monte Carlo trials to run (more trials = higher chance of hitting
  the right ordering, but more states explored)

### Step 3: Run and iterate

For each variant:

1. Create the variant file in {{HARNESS_DIR}}/scenarios/ named `d12-tuning-vN.json`

2. Run it:
```bash
cd {{HARNESS_DIR}} && /tmp/karpenter-harness \
  --interactive=false \
  --inputs scenarios/d12-tuning-vN.json \
  --output /tmp/d12-tuning-vN-dump \
  --closed-loop=false \
  2>&1 | tee /tmp/d12-tuning-vN-log.txt
```

3. Check the result:
   - Look for "Converged state" lines -- different hashes indicate divergence
   - Count terminal objects: 5 objects (NodeClaim survives) vs 4 objects
     (NodeClaim deleted = bug reproduced)
   - Check "Total States" for state count and note the wall time

4. Log every iteration to {{LOG_PATH}} with:
   - Timestamp
   - What you changed and why
   - Result: converged states, total states, wall time, terminal object counts
   - Whether the bug reproduced (terminal states with different object counts)

5. Based on the results, adjust your configuration and try again.

### Success Criteria

- Primary: find ANY configuration that produces terminal states with different
  object counts (NodeClaim present in some, absent in others)
- Secondary: minimize that configuration (fewer controllers, tighter staleness
  window, fewer trials, lower state count)
- Record total experiment wall time and number of iterations
- Stop after 10 iterations or when you've minimized and can't reduce further

## Important Rules

- Do NOT modify the original scenario file
- Do NOT modify any project source code or the harness
- Do NOT ask questions -- iterate autonomously
- You MUST use `search.mode: "monte_carlo"` (DFS is not supported for Karpenter)
- Record every attempt in the experiment log, including failures
```

## Placeholders

| Placeholder | Description | Value |
|---|---|---|
| `{{TIMESTAMP}}` | ISO 8601 experiment start time | (set at experiment time) |
| `{{SCENARIO_PATH}}` | Path to original D12 scenario | `examples/karpenter/scenarios/d12_consolidatable-condition-toctou.json` |
| `{{DEPGRAPH_PATH}}` | Path to dependency graph | `examples/karpenter/dependency-graph.json` |
| `{{SOURCE_PATH}}` | Path to Karpenter source code | `/Users/tgoodwin/projects/karpenter` |
| `{{HARNESS_DIR}}` | Path to harness directory | `/Users/tgoodwin/projects/kamera/examples/karpenter` |
| `{{LOG_PATH}}` | Path for experiment log | `experiments/coverage-curves/karpenter/tuning-experiment-d12-log.md` |
