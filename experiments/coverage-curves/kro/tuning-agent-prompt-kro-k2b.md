# KRO K2b Perturbation Tuning Agent Prompt

Adapted from v2 (relaxed) for the KRO K2b fault injection bug.

## Prompt

```
# Perturbation Tuning Experiment -- EXECUTE NOW

You MUST execute immediately. Do NOT plan. Do NOT ask questions. Start analyzing,
creating files, running scenarios, and logging results RIGHT NOW.

## Experiment Start Time: {{TIMESTAMP}}

## Goal

You are given a scenario file for a known bug in KRO (Kubernetes Resource
Orchestrator). The bug manifests as **different terminal states with different
numbers of child resources** when the scenario is run with the right perturbation
configuration. Specifically, the Application instance should create 3 child
resources (Deployment, Service, Ingress), but under certain perturbation
conditions some children are never created.

Your job is to find the most focused perturbation configuration that reproduces
the bug -- meaning the fewest controllers, tightest crash point, and smallest
state space that still produces terminal states with incomplete child resources.

There is no hard time limit per run. A run that takes a few minutes but
reproduces the bug is valuable -- you can then minimize from there. Prioritize
finding ANY reproduction first, then iteratively tighten the configuration.

## Inputs

- **Scenario file:** {{SCENARIO_PATH}}
- **Project source code:** {{SOURCE_PATH}}
- **Harness directory:** {{HARNESS_DIR}}

## What You Must Do

### Step 1: Understand the bug surface

Read the scenario file to understand:
- What controllers are involved (both KRO controllers and simulated K8s controllers)
- What environment objects are seeded (the ResourceGraphDefinition)
- What external inputs are applied (Application CREATE)
- What perturbations are available (ordering, fault injection, staleness)

Read relevant controller source code to understand:
- What the Instance Controller (ApplicationController) does during reconciliation
- Its write sequence: finalizer, ApplySet metadata, child resource applies (parallel)
- Where a mid-reconcile crash could leave the system in an inconsistent state

### Step 2: Design your first perturbation configuration

Based on your analysis, create a variant scenario file with a perturbation
configuration you believe will reproduce the bug. Available perturbation knobs:

- **permuteControllers**: List of controller IDs whose execution order should be explored
- **faultInjection**: Crash a controller after N write effects (`crashAfterEffect`, `triggerOnce`)
- **stalenessIntervals**: Make a controller read stale versions of a resource kind
- **maxDepth**: Maximum exploration depth

### Step 3: Run and iterate

For each variant:

1. Create the variant file in {{HARNESS_DIR}}/scenarios/ named `k2b-tuning-vN.json`

2. Run it:
```bash
cd {{HARNESS_DIR}} && go run . \
  --interactive=false \
  --inputs scenarios/k2b-tuning-vN.json \
  --output /tmp/k2b-tuning-vN-dump \
  2>&1 | tee /tmp/k2b-tuning-vN-log.txt
```

3. Check the result by looking at the output:
   - Look for terminal state lines ("Converged state" or "aborted" at max depth)
   - Check the log for "# Distinct States" and "Resource States" counts
   - Look at the dump files: each `.jsonl` file contains `campaignMetrics` with
     `uniqueNodeVisits`, `totalNodeVisits`, and `uniqueResourceStates`
   - Check terminal state object counts -- the bug is present when different
     terminal states have different numbers of objects (e.g., one has 9 objects
     with all children, another has 3-7 objects with missing children)

4. To compare terminal states across reference and rerun phases:
```bash
# Check object counts in terminal states
python3 -c "
import json, glob
for f in sorted(glob.glob('/tmp/k2b-tuning-vN-dump/*.jsonl')):
    d = json.load(open(f))
    phase = d['context']['scenario']['attributes'].get('phase','?')
    for s in d.get('states', []):
        objs = s['state']['contents']['objects']
        kinds = sorted([o['key']['identityKind'] for o in objs])
        print(f'{phase}: {len(kinds)} objects {kinds}')
"
```

5. Log every iteration to {{LOG_PATH}} with:
   - Timestamp
   - What you changed and why
   - Result: terminal state object counts, total states, wall time
   - Whether the bug reproduced (terminal states with different object counts)

6. Based on the results, adjust your configuration and try again.

### How to Detect the Bug

The bug is reproduced when you observe **terminal states with different numbers
of objects**. In the healthy case, the Application instance should result in 9
objects total: CRD, RGD, Application, Deployment, ReplicaSet, Pod, Service,
Endpoints, Ingress.

The bug manifests as terminal states with fewer objects -- some children
(Deployment, Service, or Ingress) are missing. Different exploration paths
may produce different subsets of children. For example:
- One path ends with 9 objects (all children present)
- Another path ends with 3 objects (no children created)
- Another path ends with 4 objects (only Ingress created)

The reference phase (no perturbations) should always produce 9 objects.
If a rerun phase (with perturbations) produces fewer objects, that's the bug.

### Success Criteria

- Primary: find ANY configuration that produces terminal states with different
  object counts between reference and rerun phases
- Secondary: minimize that configuration (fewer controllers, tighter crash
  point, lower state count)
- Record total experiment wall time and number of iterations
- Stop after 10 iterations or when you've minimized and can't reduce further

## Available Controller IDs

- `ResourceGraphDefinitionController` -- KRO controller that creates CRDs from RGD definitions
- `ApplicationController` -- KRO Instance Controller that manages child resources
- `DeploymentController` -- Simulated K8s controller for Deployments
- `ReplicaSetController` -- Simulated K8s controller for ReplicaSets
- `PodLifecycleController` -- Simulated K8s controller for Pod lifecycle
- `ServiceController` -- Simulated K8s controller for Services
- `EndpointsController` -- Simulated K8s controller for Endpoints

## Important Rules

- Do NOT modify the original scenario file
- Do NOT modify any project source code
- Do NOT ask questions -- iterate autonomously
- Record every attempt in the experiment log, including failures
```

## Placeholders

| Placeholder | Description | Example |
|---|---|---|
| `{{TIMESTAMP}}` | ISO 8601 experiment start time | `2026-03-28T15:00:00-07:00` |
| `{{SOURCE_PATH}}` | Path to KRO source code | `/Users/tgoodwin/projects/kro` |
| `{{HARNESS_DIR}}` | Path to KRO harness directory | `/Users/tgoodwin/projects/kamera/examples/kro` |
| `{{SCENARIO_PATH}}` | Path to base scenario file | `/Users/tgoodwin/projects/kamera/examples/kro/scenarios/k2b_exhaustive.json` |
| `{{LOG_PATH}}` | Path for experiment log | `/Users/tgoodwin/projects/kamera/experiments/coverage-curves/kro/tuning-k2b-log.md` |
