# Perturbation Tuning Agent Prompt (v2 -- relaxed)

Reusable prompt template for the agent tuning experiment. No hard timeout per run;
the agent is free to let runs complete and use full results to inform its next
iteration. Produces fewer iterations to first reproduction compared to v1.

## Prompt

```
# Perturbation Tuning Experiment -- EXECUTE NOW

You MUST execute immediately. Do NOT plan. Do NOT ask questions. Start analyzing,
creating files, running scenarios, and logging results RIGHT NOW.

## Experiment Start Time: {{TIMESTAMP}}

## Goal

You are given a scenario file for a known bug in {{PROJECT}}. The bug manifests as 2+
distinct converged states when the scenario is run with the right perturbation
configuration. Your job is to find the most focused perturbation configuration
that reproduces the bug -- meaning the fewest controllers, tightest depth range,
and smallest state space that still produces 2+ converged states.

There is no hard time limit per run. A run that takes 3 minutes but reproduces
the bug is valuable -- you can then minimize from there. Prioritize finding ANY
reproduction first, then iteratively tighten the configuration.

## Inputs

- **Scenario file:** {{SCENARIO_PATH}}
- **Dependency graph:** {{DEPGRAPH_PATH}}
- **Project source code:** {{SOURCE_PATH}}
- **Harness directory:** {{HARNESS_DIR}}

## What You Must Do

### Step 1: Understand the bug surface

Read the scenario file to understand:
- What controllers are involved
- What environment objects are seeded
- What external inputs are applied
- What perturbations are currently configured

Read the dependency graph to understand:
- Which controllers read/write which resources
- Which controllers share resources (potential race surfaces)
- The trigger topology (what events trigger which controllers)

Read relevant controller source code to understand:
- What each controller does during reconciliation
- Where ordering-sensitive interactions occur
- Which controllers could produce different outcomes depending on execution order

### Step 2: Design your first perturbation configuration

Based on your analysis, create a variant scenario file with a perturbation
configuration you believe will reproduce the bug.

### Step 3: Run and iterate

For each variant:

1. Create the variant file in {{HARNESS_DIR}}/scenarios/ named `{{SCENARIO_PREFIX}}_relaxed-vN.json`

2. Run it:
```bash
cd {{HARNESS_DIR}} && {{GO_CMD}} run . \
  --interactive=false \
  --inputs scenarios/{{SCENARIO_PREFIX}}_relaxed-vN.json \
  --output /tmp/{{SCENARIO_PREFIX}}-relaxed-vN-dump.json \
  2>&1 | tee /tmp/{{SCENARIO_PREFIX}}-relaxed-vN-log.txt
```

3. Check the result:
   - Look for "Converged state" lines in the output
   - Check the last "Total States" log line for state counts
   - Note the wall time

4. Log every iteration to {{LOG_PATH}} with:
   - Timestamp
   - What you changed and why
   - Result: converged states, total states, wall time
   - Whether the bug reproduced (2+ converged states)

5. Based on the results, adjust your configuration and try again.

### Success Criteria

- Primary: find ANY configuration that produces 2+ converged states
- Secondary: minimize that configuration (fewer controllers, lower state count)
- Record total experiment wall time and number of iterations
- Stop after 10 iterations or when you've minimized and can't reduce further

## Important Rules

- Do NOT modify the original scenario file
- Do NOT modify any project source code
- Do NOT ask questions -- iterate autonomously
- Record every attempt in the experiment log, including failures
```

## Placeholders

| Placeholder | Description | Example |
|---|---|---|
| `{{TIMESTAMP}}` | ISO 8601 experiment start time | `2026-03-28T08:40:40-07:00` |
| `{{PROJECT}}` | Project name | `KCP` |
| `{{SCENARIO_PATH}}` | Full path to original scenario | `/Users/.../scenarios/kcp4_late-apiexport.json` |
| `{{DEPGRAPH_PATH}}` | Full path to dependency graph | `/Users/.../dependency-graph.json` |
| `{{SOURCE_PATH}}` | Path to project source code | `/Users/tgoodwin/projects/kcp` |
| `{{HARNESS_DIR}}` | Path to harness directory (where `go run .` works) | `/Users/tgoodwin/projects/kcp/kamera` |
| `{{SCENARIO_PREFIX}}` | Scenario name prefix for variant files | `kcp4` |
| `{{GO_CMD}}` | Go command (may need PATH prefix) | `go` or `PATH=/Users/.../go1.25.0/bin:$PATH go` |
| `{{LOG_PATH}}` | Path for experiment log | `/Users/.../relaxed-tuning-kcp4-log.md` |

## Comparison with v1

| Metric | v1 (timeout) | v2 (relaxed) |
|---|---|---|
| Iterations to first reproduction | 11 | 3 |
| Final minimal controllers | 2 | 1 |
| Final state space | 310 | 144 |
| Total wall time | ~25 min | ~30 min |
| Total iterations | 15 | 16 |

v2 trades slightly more total wall time for dramatically faster first reproduction.
The agent observes full run results (even expensive ones), leading to more informed
decisions and deeper minimization.
