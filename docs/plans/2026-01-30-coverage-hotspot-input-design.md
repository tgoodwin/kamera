# Hotspot → Input Design (Coverage Pipeline)

## Objective
Define a concrete, deterministic translation from a `HotspotInstance` (from static dependency graphs) to a simple `Input` representation suitable for later conversion into a `Scenario`. This step intentionally excludes “dimensions of variation,” which will be handled later.

## Scope
- **In:** Hotspot → Input translation, object materialization from the input map, and tuning fields.
- **Out:** Dimension expansion (Input → []Input) and any tracecheck-specific configuration details.

## Data Types (pkg/coverage)
Keep the translation layer independent of `tracecheck`.

```
type Input struct {
  Name            string
  EnvironmentState EnvironmentState
  UserInputs      []UserInput
  Tuning          InputTuning
}

type EnvironmentState struct {
  Objects []*unstructured.Unstructured
}

type UserInput struct {
  ID      string
  Type    string
  Object  *unstructured.Unstructured
}

type InputTuning struct {
  MaxDepth           int
  PermuteControllers []string
  // controllerID -> []canonical groupKind
  StaleReads    map[string][]string
  // canonical groupKind -> lookback limit
  StaleLookback map[string]int
}

type InputMap struct {
  Mapping map[string][]InputTemplate `json:"mapping"`
}

type InputTemplate struct {
  Name   string                     `json:"name"`
  Object *unstructured.Unstructured `json:"object"`
}
```

Notes:
- `StaleReads` is keyed by controller ID; `StaleLookback` is keyed by canonical GroupKind.

## Input Map Assumption
Treat the input map as a **single GVK → template object** lookup (schema seed).
- If a GVK is missing, **error**.
- No template selection logic is needed at this stage.

Parsing: deserialize into `InputMap` and decode each `object` into
`*unstructured.Unstructured` at load time so templates are validated and ready
for deep-copying during Hotspot → Input.

## Object Materialization & Normalization
Each referenced GVK produces exactly **one normalized object** shared across controllers.

Normalization rules:
1. **Identity:** Always override `metadata.name` and `metadata.namespace` with deterministic values (e.g., `hs-<type>-<idx>-<kind>`, namespace `default` for namespaced kinds). Cluster-scoped resources have empty namespace.
2. **Strip status:** Remove the entire `status` subtree.
3. **Strip server-assigned fields:** Remove `uid`, `resourceVersion`, `generation`, `managedFields`, `creationTimestamp`, `selfLink`, and similar runtime fields.
4. **Preserve spec + safe metadata:** Keep `spec`, labels, and annotations unless later overridden by variations.

## Hotspot → Input Algorithm
Inputs: `HotspotInstance`, dependency graph lookup, input map.

1. **Collect GVKs:**
   - Start with `hotspot.Resources` (resource nodes).
   - Expand any GVKs referenced in attributes (`inputs`, `outputs`, `writers`), if present.
   - Deduplicate by GVK.
2. **Resolve objects:**
   - For each GVK, look up a template, deep-copy, normalize, and store.
3. **Build user inputs:**
   - For each resolved resource, create a `UserInput` with `type=CREATE` and the normalized object.
4. **Set Tuning (compact hints):**
   - **Multi-writer / Diamond / Feedback cycle:** `PermuteControllers = hotspot.Controllers`.
   - **Missing trigger / Reducer:** `StaleReads[reader] = []groupKind{...}` for referenced inputs.
   - `StaleLookback` defaults to a small value (e.g., 1) for those kinds.
   - `MaxDepth` can be increased for feedback cycles.

## Error Handling & Warnings
- **Missing GVK template:** return error.
- **Multiple reconciles targets for a controller:** warn and pick deterministically.
- **No hotspots:** return error (CLI should not emit empty output silently).

## Rationale
This keeps the translation deterministic and explicit while remaining independent of `tracecheck`. It also preserves a clean seam for later “dimensions of variation” expansion without changing the core mapping logic.

## CLI Entry Point (Incremental Testing)
Add a `cmd/generate` command to exercise the pipeline before end-to-end wiring.

Suggested behavior:
- Inputs: `--graph <graph.json>`, `--input-map <input-map.json>`
- Output: `--out <inputs.json>` containing a JSON array of `Input`
- Selection: `--hotspot-type` and `--limit` to filter and cap output
- Failure: error if zero hotspots found

This allows incremental validation of Hotspot → Input translation without
materializing scenarios or running the explorer.
