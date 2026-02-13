# Dependency Graph Contract (v2)

This document defines the machine-readable contract for dependency graph artifacts
used by coverage-strategy-v2. The goal is to make graph validity binary:
either the artifact satisfies this contract or it does not.

## Scope

This contract applies to static controller/resource graph artifacts (for example,
`graph-experiment/*-dependency-graph.json`) consumed by:

- perturbation profile inference
- seed selection for user-facing inputs
- downstream validation and planning tools

Downstream logic must not infer missing semantics with heuristics when this
contract is present.

## Canonical JSON Shape

```json
{
  "nodes": [
    { "kind": "controller", "id": "ServiceReconciler" },
    {
      "kind": "resource",
      "id": "serving.knative.dev/v1/Service",
      "gvk": "serving.knative.dev/v1/Service",
      "role": "user-facing"
    }
  ],
  "edges": [
    {
      "kind": "triggers",
      "from": "ServiceReconciler",
      "to": "serving.knative.dev/v1/Service",
      "trigger": "primary"
    },
    {
      "kind": "reads",
      "from": "ServiceReconciler",
      "to": "serving.knative.dev/v1/Configuration",
      "surface": "spec"
    },
    {
      "kind": "writes",
      "from": "ServiceReconciler",
      "to": "serving.knative.dev/v1/Route",
      "surface": "status"
    }
  ]
}
```

## Required Top-Level Fields

- `nodes`: array of node objects
- `edges`: array of edge objects

No other top-level fields are allowed in the contract form.

## Node Rules

### Controller Node

Required fields:

- `kind`: must be `controller`
- `id`: canonical controller identifier

Allowed fields:

- `kind`, `id`

### Resource Node

Required fields:

- `kind`: must be `resource`
- `id`: canonical GVK string
- `gvk`: canonical GVK string; must equal `id`
- `role`: one of `user-facing`, `supporting`, `builtin`

Allowed fields:

- `kind`, `id`, `gvk`, `role`

### Canonical Identifier Rules

- Controller `id` must be stable and unique in the graph.
- Resource `id`/`gvk` must be `group/version/kind`.
- Core API group must use literal `core` (for example, `core/v1/Pod`).
- `kind` segment is case-sensitive Kubernetes Kind (for example, `Service`,
  `NodeClaim`).

## Edge Rules

All edges are directed `controller -> resource`.

### Triggers Edge

Required fields:

- `kind`: must be `triggers`
- `from`: controller `id`
- `to`: resource `id`
- `trigger`: one of `primary`, `secondary`, `owns`, `manual`

Allowed fields:

- `kind`, `from`, `to`, `trigger`

### Reads Edge

Required fields:

- `kind`: must be `reads`
- `from`: controller `id`
- `to`: resource `id`
- `surface`: one of `spec`, `status`, `metadata`, `any`

Allowed fields:

- `kind`, `from`, `to`, `surface`

### Writes Edge

Required fields:

- `kind`: must be `writes`
- `from`: controller `id`
- `to`: resource `id`
- `surface`: one of `spec`, `status`, `metadata`, `any`

Allowed fields:

- `kind`, `from`, `to`, `surface`

### Global Edge Constraints

- `from` must reference an existing controller node.
- `to` must reference an existing resource node.
- No duplicate edges with identical full tuples.
  - `triggers`: (`kind`, `from`, `to`, `trigger`)
  - `reads`/`writes`: (`kind`, `from`, `to`, `surface`)

## Role Semantics

- `user-facing`: resource type is valid for fuzzed top-level seed generation.
- `supporting`: resource type may be required as fixed harness configuration or
  pre-seeded support objects, but is not fuzzed as a top-level user input.
- `builtin`: Kubernetes built-in resource type present in the graph; may be
  read/written/triggered but is not automatically considered a top-level
  fuzz seed.

Downstream rule: top-level seed selection must use `role` directly, not
structural heuristics over edges.

## Contract Validation Checklist

An artifact satisfies the contract only if every check below is true:

1. Top-level keys are exactly `nodes` and `edges`.
2. All node objects match one of the two allowed node shapes.
3. All node IDs are unique.
4. All resource `gvk` values are canonical and equal to resource `id`.
5. All edge objects match one of the three allowed edge shapes.
6. Every edge endpoint resolves to existing node IDs with correct kinds.
7. No duplicate edges by the tuple rules above.
8. Every resource node has an explicit `role`.
9. Every controller has at least one `triggers` edge with `trigger=primary`.

If any check fails, consumers should reject the graph and report validation
errors. Consumers must not silently guess missing fields.

## One-Command Validation

Run:

```bash
scripts/validate-dependency-graph.sh \
  --graph graph-experiment/<project>-dependency-graph.json \
  --schema-map graph-experiment/<project>-schema-map.json
```

Exit code:

- `0`: contract-compliant
- non-zero: non-compliant (errors printed)

## Legacy Mapping (Non-Contract Artifacts)

Older artifacts may use:

- controller field `name` instead of `id`
- resource field `gvk` without explicit `id`
- edge kind `watches` + `watchKind` instead of `triggers` + `trigger`

These are not contract-compliant. They should be normalized into the contract
form before use by v2 pipeline components.
