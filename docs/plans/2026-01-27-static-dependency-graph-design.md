# Static Dependency Graph Data Model

## Objective
Define a simple in-memory representation for the static dependency graph described
in `docs/design/coverage-strategy.md`. The graph is bipartite: controller nodes
and resource nodes (GVK only). The model should be easy to reason about now and
friendly to future serialization, without heavy pointer graphs.

## Core Model (Unified Graph)
We represent the graph as a single node map and a flat edge list:

- Nodes are either Controller or Resource (GVK only).
- Edges are directed and typed: Reconciles, Watches, Owns, Reads, Writes.
- Multiple edges between the same two nodes are allowed (e.g., Watches and
  Reads between a controller and a resource).

Proposed types (shape):

- `Graph{ Nodes map[NodeID]Node, Edges []Edge }`
- `Node{ ID, Kind, Controller, Resource }`
- `Edge{ ID, From, To, Kind, Attr }`

No pointers are required in the core graph. This keeps the structure simple for
future serialization (JSON/YAML/DOT) while still supporting rich analysis.

## Typed Edge Attributes
Edges have typed attributes for the cases that need them:

- Reads/Writes include `Target: Spec|Status|Unknown`.
- Watches include `Kind: Primary|Owned|Indexed|Unknown`.

Other edge kinds have zero-value attributes. This preserves the semantic
distinction between triggering relationships (Watches) and data dependencies
(Reads), which is required for later hotspot analysis (e.g., stale reads,
ordering sensitivity).

## Deterministic, Human-Readable IDs
IDs are derived deterministically from content to keep them readable in logs and
DOT output:

- Controller node: `c:<name>`
- Resource node: `r:<group>/<version>/<kind>`
- Edge: `e:<from>|<kind>|<to>|<attrs>`

Edge IDs include a compact attribute suffix (e.g., `reads:spec`,
`watches:owned`) to avoid collisions between multiple edges connecting the same
node pair.

## Construction Helpers and Constraints
Graph helpers should normalize IDs and enforce bipartite constraints:

- `Reconciles`, `Watches`, `Reads`, `Writes`: Controller -> Resource only
- `Owns`: Resource -> Resource only
- Controller -> Controller edges are invalid

Errors should be returned (not panics) to tolerate imperfect static inference.
Helpers may insert missing nodes on edge creation to reduce builder boilerplate.

## Optional Indexes
For analysis queries, we can add derived indexes without changing the canonical
storage:

- `Outgoing[NodeID][]EdgeID`
- `Incoming[NodeID][]EdgeID`
- `ByKind[EdgeKind][]EdgeID`

These can be built lazily or via a helper to keep the base model minimal.

## LLM-Friendly Assembly Workflow
To keep LLM output compact, define a "raw" input format where edges reference
nodes by canonical string keys. This avoids repeating full node objects per edge
and keeps the LLM ignorant of internal ID rules.

Canonical GVK string format:

- `<group>/<version>/<kind>`
- Use `core` as the group for core resources (consistent with
  `util.CanonicalGroupKind` semantics).

Example:

```json
{
  "nodes": [
    {"kind": "controller", "name": "RouteReconciler"},
    {"kind": "resource", "gvk": "serving.knative.dev/v1/Route"}
  ],
  "edges": [
    {"kind": "watches", "from": "RouteReconciler", "to": "serving.knative.dev/v1/Route", "watchKind": "primary"},
    {"kind": "reads", "from": "RouteReconciler", "to": "serving.knative.dev/v1/Route", "target": "status"}
  ]
}
```

The builder assembles this into a `Graph` by:

- Parsing controller names and canonical GVK strings into nodes.
- Deriving deterministic IDs from controller `name` and resource GVK.
- Normalizing/validating enums (`kind`, `watchKind`, `target`).
- Enforcing bipartite edge constraints.
- Returning errors for unknown kinds or invalid relationships.

This keeps external producers simple while preserving internal consistency.

## Open Questions (Deferred)
- Escaping rules for IDs if resource kinds contain separators.
