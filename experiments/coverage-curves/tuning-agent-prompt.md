# Perturbation Tuning Agent Prompt (v1 — timeout-constrained)

**Superseded by `tuning-agent-prompt-v2.md`.** This version imposes a hard 60-second timeout
per run, which forces aggressive scope reduction but causes many wasted iterations when the
agent can't observe full results. Kept for reference and comparison.

Fill in the placeholders and launch as an autonomous agent.

## Prompt

```
# Perturbation Tuning Experiment — EXECUTE NOW

You MUST execute immediately. Do NOT plan. Do NOT ask questions. Start analyzing, creating files, running scenarios, and logging results RIGHT NOW.

## Experiment Start Time: {{TIMESTAMP}}

## Goal

You are given a scenario file for a known bug in {{PROJECT}}. The bug manifests as 2+ distinct converged states when the scenario is run with the right perturbation configuration. Your job is to find a perturbation configuration that reproduces the bug with a simulation that completes in under 60 seconds.

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

Based on your analysis, hypothesize which subset of controllers are likely involved in the bug. Create a variant scenario file with:
- A reduced `permuteControllers` list (start with 2-3 controllers)
- A reasonable `maxDepth` (start with 40, reduce later)
- Keep `externalInputs` and `userActionReadyDepths` from the original unless you have reason to change them
- Optionally add `permuteDepthRange` to focus on the interesting window

### Step 3: Run and iterate

For each variant:

1. Create the variant file in {{HARNESS_DIR}}/scenarios/ named `{{SCENARIO_PREFIX}}_tuning-vN.json`

2. Run it:
```bash
cd {{HARNESS_DIR}} && timeout 60 {{GO_CMD}} run . --interactive=false --inputs scenarios/{{SCENARIO_PREFIX}}_tuning-vN.json --output /tmp/{{SCENARIO_PREFIX}}-tuning-vN-dump.json 2>&1 | tee /tmp/{{SCENARIO_PREFIX}}-tuning-vN-log.txt
```

3. Check the result:
   - Look for "Converged state" lines in the output
   - Check the last "Total States" log line for state counts
   - If the run times out (no convergence output), the perturbation scope is too broad or maxDepth too high

4. Log every iteration to {{LOG_PATH}} with:
   - Timestamp
   - What you changed and why
   - Result: converged states, total states, wall time
   - Whether the bug reproduced (2+ converged states)

5. Adjust and try again:
   - If timeout: reduce controllers, reduce maxDepth, add permuteDepthRange
   - If 0 converged states: you may have removed a necessary controller or object
   - If 1 converged state: close, but maxDepth may be too low or a key controller is missing
   - If 2+ converged states: bug reproduced! Try to reduce further if wall time > 30s

### Success Criteria

- 2+ converged states (bug reproduced)
- Simulation completes within the 60-second timeout
- Record total experiment wall time and number of iterations

## Important Rules

- Do NOT modify the original scenario file
- Do NOT modify any project source code
- Do NOT ask questions — iterate autonomously
- Record every attempt in the experiment log, including failures
- Stop when you have a configuration that reproduces the bug in under 60 seconds, or after 15 iterations
```

## Placeholders

| Placeholder | Description | Example |
|---|---|---|
| `{{TIMESTAMP}}` | ISO 8601 experiment start time | `2026-03-27T10:00:00-07:00` |
| `{{PROJECT}}` | Project name | `KCP` |
| `{{SCENARIO_PATH}}` | Full path to original scenario | `/Users/.../scenarios/kcp4_late-apiexport.json` |
| `{{DEPGRAPH_PATH}}` | Full path to dependency graph | `/Users/.../dependency-graph.json` |
| `{{SOURCE_PATH}}` | Path to project source code | `/Users/tgoodwin/projects/kcp` |
| `{{HARNESS_DIR}}` | Path to harness directory (where `go run .` works) | `/Users/tgoodwin/projects/kcp/kamera` |
| `{{SCENARIO_PREFIX}}` | Scenario name prefix for variant files | `kcp4` |
| `{{GO_CMD}}` | Go command (may need PATH prefix) | `go` or `PATH=/Users/.../go1.25.0/bin:$PATH go` |
| `{{LOG_PATH}}` | Path for experiment log | `/Users/.../tuning-experiment-kcp4-log.md` |
