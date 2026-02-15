# Code-First Input Generation Strategy

## Objective
To automatically generate a corpus of valid, representative Kubernetes resource states (`StateNode`) for fuzzing and exploration. The ultimate output of this pipeline is a persistent `schema-map.json` file (see [Schema](input-map-schema.md)) containing concrete JSON seeds for every managed GVK.

## The Problem
Static dependency graphs identify *where* interactions happen, but they don't describe *valid data*. Fuzzing with random bytes is inefficient. We need "Schema-Aware" instantiation that respects the project's specific validation logic and defaults.

## Output Artifact
Regardless of the strategy used, the process must produce a single `schema-map.json` where GVKs map to lists of instantiated JSON objects.

## Strategy A: Code-First (Preferred)
Many operators (Knative, Karpenter) maintain extensive Go test libraries to construct resources programmatically.

### Pattern 1: Functional Options (Knative)
```go
// pkg/testing/v1
Service("name", "ns", WithConfigSpec(spec), WithAnnotation("k", "v"))
```

### Pattern 2: Struct Overrides (Karpenter)
```go
// pkg/test
NodeClaim(v1.NodeClaim{Spec: v1.NodeClaimSpec{...}})
```

### Implementation
1.  **Discovery:** Scan `pkg/testing`, `pkg/test`, or `test/` for exported functions returning CRD types.
2.  **Synthesis:** Generate a standalone Go program (`kamera_input_gen.go`) in the project root.
    *   This program imports the project's test packages.
    *   It executes the builder functions to create in-memory objects.
    *   It serializes these objects to the `schema-map.json` format.
3.  **Execution:** Run via `go run` to generate the artifact.

## Strategy B: File-Based (Fallback)
Some projects (Kratix, Crossplane) rely heavily on static YAML manifests in their test suites.

### Implementation
1.  **Discovery:** Scan `test/`, `examples/`, `config/samples/` for `.yaml` files.
2.  **Ingestion:** Parse these manifests into generic maps/structs.
3.  **Aggregation:** Organize them by GVK and write to `schema-map.json`.

## Strategy C: Internal Hybrid (Advanced)
For projects like Crossplane where builders exist but are internal/anonymous:
1.  **Extract:** Parse the AST of the test file to extract the helper function logic.
2.  **Replicate:** Synthesize a local version of the helper in our generated `kamera_input_gen.go`.
3.  **Execute:** Proceed as with Strategy A.

## Workflow Selection Logic

The `kamera gen` command will apply the following heuristics:

1.  **Check for Builders:** Search for exported functions returning core CRD types in `pkg/test*`.
    *   If found -> Use **Strategy A**.
2.  **Check for Manifests:** Search for `*.yaml` in `test/`, `examples/`.
    *   If found -> Use **Strategy B**.
3.  **Fallback:** Use basic Schema-based fuzzing (least effective).
