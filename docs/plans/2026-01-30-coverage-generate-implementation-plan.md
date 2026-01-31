# Coverage Generate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `cmd/generate` CLI plus a `pkg/coverage` layer that loads an input map and translates hotspots into deterministic `Input` JSON for incremental testing.

**Architecture:** Introduce a new `pkg/coverage` package that owns Input types, input map parsing, normalization, and hotspot → input translation. The CLI loads a dependency graph and input map, detects hotspots, filters/limits them, translates to Inputs, and writes a single JSON array.

**Tech Stack:** Go, `pkg/analyze` for graphs/hotspots, `k8s.io/apimachinery/pkg/apis/meta/v1/unstructured` for templates, stdlib `encoding/json`.

### Task 1: Add coverage input types and input-map loader

**Files:**
- Create: `pkg/coverage/types.go`
- Create: `pkg/coverage/input_map.go`
- Test: `pkg/coverage/input_map_test.go`

**Step 1: Write the failing test**

```go
func TestLoadInputMap(t *testing.T) {
  // table: valid single-template map, missing mapping, multi-template error
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/coverage -run TestLoadInputMap`
Expected: FAIL (package/file missing)

**Step 3: Write minimal implementation**

```go
// types.go
package coverage

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

type Input struct {
  Name    string                   `json:"name"`
  Objects []*unstructured.Unstructured `json:"objects"`
  Pending []Pending                `json:"pending"`
  Tuning  InputTuning              `json:"tuning"`
}

type Pending struct {
  ControllerID string          `json:"controllerId"`
  Key          NamespacedName  `json:"key"`
}

type NamespacedName struct {
  Namespace string `json:"namespace"`
  Name      string `json:"name"`
}

type InputTuning struct {
  MaxDepth           int                 `json:"maxDepth"`
  PermuteControllers []string            `json:"permuteControllers"`
  StaleReads         map[string][]string `json:"staleReads"`
  StaleLookback      map[string]int      `json:"staleLookback"`
}

type InputMap struct {
  Mapping map[string][]InputTemplate `json:"mapping"`
}

type InputTemplate struct {
  Name   string                     `json:"name"`
  Object *unstructured.Unstructured `json:"object"`
}
```

```go
// input_map.go
func LoadInputMap(path string) (InputMap, error) {
  // read file, json.Unmarshal into InputMap
  // error if Mapping nil/empty
  // error if any mapping entry has len != 1
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/coverage -run TestLoadInputMap`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/coverage/types.go pkg/coverage/input_map.go pkg/coverage/input_map_test.go
git commit -m "add coverage input map loader"
```

### Task 2: Add normalization helper for template objects

**Files:**
- Create: `pkg/coverage/normalize.go`
- Test: `pkg/coverage/normalize_test.go`

**Step 1: Write the failing test**

```go
func TestNormalizeTemplate(t *testing.T) {
  // object with status + server fields; expect stripped
  // name/namespace overridden deterministically
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/coverage -run TestNormalizeTemplate`
Expected: FAIL (function missing)

**Step 3: Write minimal implementation**

```go
func NormalizeTemplate(obj *unstructured.Unstructured, name, namespace string) *unstructured.Unstructured {
  // deep copy
  // set name, namespace (namespace only if non-empty)
  // remove status
  // remove metadata fields: uid, resourceVersion, generation, managedFields,
  // creationTimestamp, selfLink
  // return copy
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/coverage -run TestNormalizeTemplate`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/coverage/normalize.go pkg/coverage/normalize_test.go
git commit -m "add input template normalization"
```

### Task 3: Implement hotspot → input translation

**Files:**
- Create: `pkg/coverage/translate.go`
- Test: `pkg/coverage/translate_test.go`

**Step 1: Write the failing test**

```go
func TestTranslateHotspots(t *testing.T) {
  // build small RawGraph with 1-2 controllers + resources
  // build input map JSON for those GVKs
  // detect hotspots, translate first, verify:
  // - objects normalized
  // - pending per controller uses reconciles target
  // - permute/stale tuning set for hotspot type
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/coverage -run TestTranslateHotspots`
Expected: FAIL (function missing)

**Step 3: Write minimal implementation**

```go
func TranslateHotspots(graph *analyze.Graph, hotspots []analyze.HotspotInstance, inputMap InputMap) ([]Input, error) {
  // for each hotspot: collect GVKs, resolve/normalize objects, build pending, set tuning
}
```

Implementation details:
- GVK string format: `core` for empty group, `<group>/<version>/<kind>`.
- Resolve resource NodeID via `graph.Nodes[id].Resource`.
- Expand `inputs`/`outputs` attributes (comma-separated node IDs).
- Warn (log.Printf) if a controller has multiple reconciles targets; choose lexicographic.
- Error if a controller has no reconciles target or if required GVK missing.
- Deterministic name: `hs-<type>-<index>-<kind>` (lowercase kind recommended).
- Tuning:
  - Multi-writer, fan-out, feedback cycle → `PermuteControllers = controllers`
  - Missing trigger → `StaleReads[reader] = []groupKind{resource}`
  - Aggregation join → `StaleReads[controller] = inputs`
  - Feedback cycle → set `MaxDepth` (e.g., 20)

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/coverage -run TestTranslateHotspots`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/coverage/translate.go pkg/coverage/translate_test.go
git commit -m "add hotspot to input translation"
```

### Task 4: Add `cmd/generate` CLI

**Files:**
- Create: `cmd/generate/main.go`
- Create: `cmd/generate/run.go`
- Test: `cmd/generate/generate_test.go`

**Step 1: Write the failing test**

```go
func TestGenerateOutputsInputs(t *testing.T) {
  // temp graph json + input map json
  // runGenerate([...]) writes to temp file
  // assert JSON array length > 0
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./cmd/generate -run TestGenerateOutputsInputs`
Expected: FAIL (command missing)

**Step 3: Write minimal implementation**

```go
// main.go -> flag parsing + runGenerate
// run.go -> read files, parse graph, detect hotspots, filter by type, apply --limit,
// translate to Inputs, error if zero, marshal to JSON, write output file.
```

Flags:
- `--graph` (required)
- `--input-map` (required)
- `--out` (required)
- `--hotspot-type` (optional)
- `--limit` (optional, default 0 = no limit)

**Step 4: Run test to verify it passes**

Run: `go test ./cmd/generate -run TestGenerateOutputsInputs`
Expected: PASS

**Step 5: Commit**

```bash
git add cmd/generate/main.go cmd/generate/run.go cmd/generate/generate_test.go
git commit -m "add generate cli"
```

### Task 5: Run full targeted tests

**Step 1: Run coverage tests**

Run: `go test ./pkg/coverage`
Expected: PASS

**Step 2: Run generate tests**

Run: `go test ./cmd/generate`
Expected: PASS

**Step 3: Commit (if any changes)**

```bash
git add pkg/coverage cmd/generate
git commit -m "fix generate coverage tests"  # only if needed
```

