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

## Open Questions (Deferred)
- Canonical representation for core group (empty vs "core") in node IDs.
- Escaping rules for IDs if resource kinds contain separators.

