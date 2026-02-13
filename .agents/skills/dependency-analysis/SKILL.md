---
name: dependency-analysis
description: Generate and validate controller-resource dependency graph artifacts for Kamera coverage strategy v2. Use this when extracting triggers/reads/writes from Kubernetes control planes, assigning resource roles, and producing contract-compliant dependency-graph.json plus schema-map.json outputs.
metadata:
  short-description: Build contract-compliant dependency graphs
---

# Dependency Analysis Skill

Use this skill when you need to analyze a Kubernetes control-plane codebase and
produce dependency artifacts consumed by Kamera v2 input generation.

## Contract First

The graph artifact is valid only if it satisfies:

- `docs/design/dependency-graph-contract.md`

Validation command:

```bash
scripts/validate-dependency-graph.sh \
  --graph dependency-graph.json \
  --schema-map schema-map.json
```

If validation fails, do not proceed downstream. Fix or regenerate artifacts.

## Required Outputs

For each analyzed project, produce:

1. `dependency-graph.json` (contract-compliant)
2. `schema-map.json` (must include every graph resource ID)
3. `analysis-notes.md` (human evidence summary for reviewers)

## Workflow

1. Discover controllers and setup entrypoints:
- Find controller/reconciler wiring (`SetupWithManager`, `.For`, `.Watches`,
  `.Owns`, `NewController`).
- Search across all plausible directories (`internal/`, `controllers/`,
  `pkg/reconciler/`, `pkg/controllers/`, `cmd/`).

2. Verify active registration:
- Confirm discovered controllers are actually registered from entrypoints.
- Missing registered controller in graph is a quality failure.

3. Extract trigger topology as `triggers` edges:
- `.For(...)` -> `trigger=primary`
- `.Watches(...)` -> `trigger=secondary`
- `.Owns(...)` -> `trigger=owns`
- explicit/manual enqueue -> `trigger=manual`

4. Extract interactions from reconcile call paths:
- Reads: `Get`, `List`, lister/cache reads
- Writes: `Create`, `Update`, `Patch`, `Delete`, `Status().Update/Patch`
- Set `surface` to one of: `spec`, `status`, `metadata`, `any`

5. Canonicalize resource IDs:
- Resource IDs must be `group/version/kind`
- Core group must be `core`
- `resource.id == resource.gvk`

6. Assign resource roles:
- `user-facing`: top-level fuzzed API inputs
- `supporting`: harness/support objects, non-user top-level fuzz inputs
- `builtin`: Kubernetes built-in resources

Role assignment heuristics (required):
- Do not classify from graph shape alone (for example, do not rely only on
  incoming/outgoing `writes` patterns).
- Gather local evidence first:
  - CRD/API docs in repo (`api/`, `config/crd/`, comments, godoc)
  - sample manifests and walkthroughs in repo (`examples/`, `config/samples/`,
    tutorials, quickstarts)
  - controller wiring/ownership context (`For` roots vs derived resources)
- For ambiguous resources, perform supplementary web search:
  - prioritize official project documentation and API references
  - check install/quickstart/tutorial pages for resources users are expected to
    author directly
  - check release/migration docs if resource purpose may have changed
- Evidence threshold for `user-facing`:
  - at least two independent signals
  - at least one signal from project docs/examples (local or official site)
- If evidence is mixed/weak, default to `supporting` and record uncertainty in
  `analysis-notes.md`.

Decision cues:
- `user-facing` when docs/examples/CLI workflows instruct users to create or
  update the resource directly.
- `supporting` when resource mostly exists for controller internals, plumbing,
  or simulation scaffolding.
- `builtin` for Kubernetes built-in API groups (for example `core`, `apps`,
  `batch`, `rbac.authorization.k8s.io`).

7. Build artifacts:
- Emit `dependency-graph.json` using contract field names and enums.
- Emit `schema-map.json` with complete resource-key coverage.

8. Validate and gate:
- Run validator script.
- Reject artifacts on any validation error.

## LLM Generation Prompt (Template)

Use this template when delegating artifact creation to an LLM:

> Analyze `<project-path>` and produce `dependency-graph.json`,
> `schema-map.json`, and `analysis-notes.md`.
>
> Requirements:
> 1. `dependency-graph.json` MUST satisfy
>    `docs/design/dependency-graph-contract.md` exactly.
> 2. Use contract field names/enums exactly (`nodes`, `edges`, `id`, `gvk`,
>    `role`, `kind=triggers|reads|writes`, `trigger`, `surface`).
> 3. Verify controller registration from entrypoints, not grep-only discovery.
> 4. Traverse reconcile helper call chains for read/write extraction.
> 5. Assign explicit `role` to every resource node.
> 6. For ambiguous role assignments, run web search against official project
>    docs and include citations in `analysis-notes.md`.
> 7. Ensure every graph resource ID exists in `schema-map.json`.
>
> Reject output if any required field is missing, any edge endpoint is dangling,
> any controller lacks `primary` trigger, any role decision lacks evidence, or
> schema-map coverage is incomplete.

## Reviewer Checks

```bash
# one-command gate
scripts/validate-dependency-graph.sh \
  --graph dependency-graph.json \
  --schema-map schema-map.json

# role evidence sanity check (should list each resource role rationale)
rg "role rationale" analysis-notes.md

# resource coverage spot-check (should be empty)
comm -23 \
  <(jq -r '.nodes[] | select(.kind=="resource") | .id' dependency-graph.json | sort -u) \
  <(jq -r '.mapping | keys[]' schema-map.json | sort -u)
```

## Non-Goals

- Do not infer missing semantics downstream with heuristics.
- Do not silently accept legacy graph shapes as contract-compliant.
- Do not continue to profile/seed generation until contract validation passes.
