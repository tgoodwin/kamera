# Crossplane Composition Pipeline Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a new Kamera example that simulates Crossplane’s Composition→CompositionRevision→XR reconciliation flow using the function pipeline without external IO.

**Architecture:** Create `examples/crossplane` as a standalone Go module that imports Crossplane internal controllers (via a module path under `github.com/crossplane/crossplane/v2`). Wire the Composition and Composite controllers into a `tracecheck.ExplorerBuilder`, seed a Composition + XR initial state, and use a stubbed FunctionRunner to return deterministic desired state. Document missing real‑world components in a local README.

**Tech Stack:** Go 1.24, Kamera `tracecheck/explore`, Crossplane v2 (local repo), crossplane-runtime v2, controller-runtime.

**Note on testing:** Per user request, skip example-specific tests and TDD. Rely on Crossplane’s own tests.

---

### Task 1: Create the crossplane example module

**Files:**
- Create: `examples/crossplane/go.mod`
- Create: `examples/crossplane/README.md`

**Step 1: Create directory + go.mod**
Create `examples/crossplane` with a module path under Crossplane so internal imports are allowed:

```go
module github.com/crossplane/crossplane/v2/examples/crossplane

go 1.24.0

require (
    github.com/crossplane/crossplane/v2 v2.0.0
    github.com/crossplane/crossplane-runtime/v2 v2.0.0
    github.com/tgoodwin/kamera v0.0.0
)

replace github.com/tgoodwin/kamera => ../..
replace github.com/crossplane/crossplane/v2 => ~/projects/crossplane
```

**Step 2: Seed README scaffold**
Create `examples/crossplane/README.md` with a short intro and a placeholder “What’s missing vs real Crossplane” section.

**Step 3: Run tidy for the example module**
Run from `examples/crossplane`:

```bash
go mod tidy
```

---

### Task 2: Add Crossplane scheme registration

**Files:**
- Create: `examples/crossplane/scheme.go`

**Step 1: Implement scheme helper**
Add a `newScheme()` function that registers:
- core types (already added by `tracecheck.NewExplorerBuilder`, but keep for clarity)
- Crossplane apiextensions v1 types (Composition, CompositionRevision)
- crossplane-runtime common types if needed for conditions

Skeleton:

```go
func newScheme() *runtime.Scheme {
    scheme := runtime.NewScheme()
    utilruntime.Must(corev1.AddToScheme(scheme))
    utilruntime.Must(v1.AddToScheme(scheme))
    utilruntime.Must(xpv1.AddToScheme(scheme))
    return scheme
}
```

---

### Task 3: Stub FunctionRunner for pipeline composition

**Files:**
- Create: `examples/crossplane/functions_stub.go`

**Step 1: Implement stub FunctionRunner**
Create a small type that implements `composite.FunctionRunner` (from Crossplane internal package). It should:
- Verify the function name (e.g., `"kamera-stub"`), or ignore it for now.
- Return a `RunFunctionResponse` with `Desired` state that:
  - Sets a small XR status field.
  - Produces a composed `ConfigMap` with a fixed name and namespace.
  - Marks the XR ready.

Use a helper like:

```go
func mustStruct(in map[string]any) *structpb.Struct
```

to build protobuf structs.

---

### Task 4: Wire Composition + Composite controllers into Kamera

**Files:**
- Create: `examples/crossplane/scenario.go`

**Step 1: Build ExplorerBuilder**
- Use `tracecheck.NewExplorerBuilder(newScheme())`.
- Register the Composition controller using `composition.NewReconciler` with a `fake.Manager{Client: c}`.
- Register the Composite (XR) controller using `composite.NewReconciler` with:
  - `WithComposer(NewFunctionComposer(c, c, stubRunner))`
  - `WithCompositeSchema(composite.SchemaLegacy)` (or the appropriate schema for your XR)
  - `WithLogger(logging.NewNopLogger())`
  - `WithRecorder(event.NewNopRecorder())`

**Step 2: Define reconciliation bindings**
- Use `.For("apiextensions.crossplane.io/Composition")` for the Composition reconciler.
- Use `.For("example.org/XWidget")` (or your chosen XR kind) for the XR reconciler.
- Keep watchers minimal (document limitations in README).

**Step 3: Build initial state**
Create:
- A `Composition` with `spec.compositeTypeRef` matching your XR GVK and `spec.pipeline` containing one step referencing the stub function.
- An XR (unstructured composite) that sets `spec.compositionRef` to the Composition name.

Add these as top-level objects via `StateEventBuilder` and return the merged state (using `tracecheck.MergeStateNodes`).

---

### Task 5: Add entrypoint + flags

**Files:**
- Create: `examples/crossplane/main.go`

**Step 1: Build runner**
- Parse flags (reuse `explore` flag helpers for config/dump/interactive).
- Load explore config if `-config` is set (same pattern as other examples).
- Build `Runner` and call `Run` with the initial state.

---

### Task 6: Finish README with missing‑pieces list

**Files:**
- Modify: `examples/crossplane/README.md`

**Step 1: Document missing components**
Include bullets for:
- XRD controller (not dynamically creating XR controller)
- Provider/managed resource controllers (no cloud IO)
- Claim syncer
- Connection secret publishing
- Dynamic watch engine
- Real function runtime (uses stubbed FunctionRunner)

**Step 2: Add usage instructions**
Add example commands showing `-interactive=false` and `-output`.

---

### Task 7: Sanity build (no tests)

**Files:** none

**Step 1: Build the example module**
Run from `examples/crossplane`:

```bash
go build ./...
```

(Do not add tests.)
