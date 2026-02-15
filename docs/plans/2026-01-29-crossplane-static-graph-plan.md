# Crossplane Static Dependency Graph Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Produce `examples/crossplane/dependency-graph.json` describing controller/resource dependencies for the Crossplane repo using the static graph spec.

**Architecture:** Manually inspect Crossplane controller constructors and reconcile loops to identify primary resources, watches, ownership, reads, and writes. Emit a single JSON graph using canonical GVK strings and the raw node/edge format defined in `docs/plans/2026-01-27-static-dependency-graph-design.md`.

**Tech Stack:** Go code inspection, ripgrep, JSON (graph format).

### Task 1: Inventory controllers and primary resources

**Files:**
- Modify: `examples/crossplane/dependency-graph.json`
- Reference: `docs/plans/2026-01-27-static-dependency-graph-design.md`

**Step 1: Locate controller entrypoints**

Run:
```bash
rg -n "NewController|NewReconciler|NewImpl|Setup|WithManager" ~/projects/crossplane/cmd ~/projects/crossplane/internal ~/projects/crossplane/pkg
```
Expected: list of controller factory functions and reconciler types.

**Step 2: Enumerate controllers and their primary resources**

Read controller constructors and reconciler definitions to record:
- Controller name
- Primary resource GVK it reconciles (Reconciles edge)

### Task 2: Extract watches and ownership edges

**Files:**
- Modify: `examples/crossplane/dependency-graph.json`

**Step 1: Record watch registrations**

Inspect controller setup for watch registrations. Record:
- Watches edges from controller -> resource
- Watch kind: primary/owned/indexed/unknown

**Step 2: Record ownership relationships**

Identify code that sets owner references or declares ownership; map to resource -> resource `owns` edges.

### Task 3: Extract read/write edges

**Files:**
- Modify: `examples/crossplane/dependency-graph.json`

**Step 1: Scan reconcile loops for reads**

Look for listers/informers or client `Get`/`List` calls and record `reads` edges (spec/status/unknown).

**Step 2: Scan reconcile loops for writes**

Look for client `Create`/`Update`/`Patch`/`Status().Update` calls and record `writes` edges (spec/status/unknown).

### Task 4: Assemble and validate graph JSON

**Files:**
- Create/Modify: `examples/crossplane/dependency-graph.json`

**Step 1: Emit nodes and edges**

Create the JSON with:
- `nodes`: controller names + resource GVKs
- `edges`: typed edges following the spec

**Step 2: Validate JSON**

Run:
```bash
jq . examples/crossplane/dependency-graph.json >/dev/null
```
Expected: no output, exit code 0.

