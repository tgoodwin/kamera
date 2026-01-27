# AGENTS.md - Static Graph Extraction Guide

## Purpose
Generate a compact `graph.json` that captures controller/resource dependencies
for static analysis. This is optimized for LLM output but also usable by humans.
The graph is bipartite (controllers + resources). Resource nodes are GVK-only.

## Output Format (LLM-friendly)
Produce a JSON object with `nodes` and `edges`:

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

### Canonical GVK string
- Format: `<group>/<version>/<kind>`
- Use `core` for the empty group (e.g., `core/v1/Service`)

### Edge kinds
- `reconciles` (Controller -> Resource)
- `watches` (Controller -> Resource)
- `owns` (Resource -> Resource)
- `reads` (Controller -> Resource)
- `writes` (Controller -> Resource)

### Attribute enums
- `watchKind`: `primary` | `owned` | `indexed` | (omit for unknown)
- `target`: `spec` | `status` | (omit for unknown)

### Multi-edges
Multiple edges between the same controller/resource pair are allowed and
expected (e.g., `watches` and `reads`).

## LLM Prompt Template
Use this prompt when asking an LLM to extract the graph:

"""
You are extracting a static dependency graph of controllers and resources.
Return JSON with:
- nodes: controller names and resource GVKs
- edges: typed relations with `from`/`to` strings

Rules:
- Resources are GVK-only, formatted as <group>/<version>/<kind>.
- Use group `core` for empty group.
- Edge kinds: reconciles, watches, owns, reads, writes.
- watchKind: primary (For), owned (Owns), indexed (Watches/EnqueueRequestsFromMapFunc).
- target: spec for Create/Update/Patch; status for Status().Update/Status().Patch.
- Include nodes list plus edges. Avoid internal IDs.
- Allow multiple edges between the same nodes.

Return only JSON.
"""

## Manual Extraction Checklist
1) **Identify controllers**
   - Look for `Reconciler` types and controller setup code.
   - Common patterns: `ctrl.NewControllerManagedBy(...)`, `builder.ControllerManagedBy(...)`.
   - Do not assume controller-runtime patterns are the only source of truth:
     many projects construct or interact with `unstructured.Unstructured`
     directly and may bypass builder helpers.

2) **Primary reconciled resource (reconciles + watches primary)**
   - `For(&v1.Type{})` indicates the primary resource.
   - Add `reconciles` edge from controller -> primary GVK.
   - Add `watches` edge with `watchKind: primary`.

3) **Owned resources (owns)**
   - `Owns(&v1.Child{})` indicates ownership.
   - Add `owns` edge from parent GVK -> child GVK.
   - Add `watches` edge from controller -> child GVK with `watchKind: owned`.

4) **Indexed or mapped watches**
   - `Watches(&source.Kind{Type: &v1.X{}}, handler.EnqueueRequestsFromMapFunc(...))`
     or indexers imply `watchKind: indexed`.
   - Add `watches` edge from controller -> watched GVK.

5) **Reads**
   - `client.Get/List` of a type implies a `reads` edge.
   - Target is `spec` if only spec fields are used; `status` if status is read.
   - If unclear, omit `target`.

6) **Writes**
   - `Create/Update/Patch` implies `writes` with `target: spec`.
   - `Status().Update/Status().Patch` implies `writes` with `target: status`.

7) **Resources list**
   - Every resource referenced in edges should appear in `nodes`.
   - Controllers referenced in edges should appear in `nodes`.

## Validation Tips
- Ensure bipartite edges (controller -> resource) except `owns` (resource -> resource).
- Keep node names exact (controller type or logical name used in setup).
- Use consistent GVK casing (Kind is Go type name).
