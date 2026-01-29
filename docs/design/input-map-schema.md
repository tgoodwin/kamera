# Input Map Schema

## Objective
To define a persistent, machine-readable format (`input-map.json`) that serves as a **Seed Corpus** for Kubernetes resources. This map provides fully instantiated, valid examples ("templates") for each GVK, ready for direct consumption by the test runner without requiring access to the project's source code or build system.

## Schema Definition

The file is a JSON object mapping GVK keys to a list of resource templates.

```json
{
  "mapping": {
    "serving.knative.dev/v1/Service": [
      {
        "name": "default",
        "object": {
          "apiVersion": "serving.knative.dev/v1",
          "kind": "Service",
          "metadata": {
            "name": "test-service",
            "namespace": "default"
          },
          "spec": {
            "template": {
              "spec": {
                "containers": [{ "image": "busybox" }]
              }
            }
          }
        }
      },
      {
        "name": "with-annotation",
        "object": {
          "apiVersion": "serving.knative.dev/v1",
          "kind": "Service",
          "metadata": {
            "name": "annotated-service",
            "annotations": { "foo": "bar" }
          },
          "spec": { ... }
        }
      }
    ],
    "platform.kratix.io/v1alpha1/Promise": [
      {
        "name": "redis-promise",
        "object": { ... }
      }
    ]
  }
}
```

## Fields

*   **`mapping`** (Object): The root container. Keys are GVK strings in the format `<Group>/<Version>/<Kind>`.
*   **`[value]`** (Array): A list of available templates for that GVK.
    *   **`name`** (String): A descriptive identifier for the template (e.g., "default", "complex-configuration").
    *   **`object`** (Object): The fully expanded JSON representation of the Kubernetes resource. This must be a valid, apply-able manifest.

## Consistency & Validation

The `input-map.json` is tighty coupled to the static `dependency-graph.json`.

1.  **Exact GVK Matching:** The keys in the `mapping` object **MUST** exactly match the `gvk` field of Resource Nodes in the dependency graph.
    *   *Graph Node:* `{"kind": "resource", "gvk": "serving.knative.dev/v1/Service"}`
    *   *Input Map Key:* `"serving.knative.dev/v1/Service"`
    *   *Note:* This includes the version. If the graph specifies `v1`, the map must provide `v1` templates, not `v1beta1`.

2.  **Missing Schema Warning:** During the generation process, the tool must compare the set of GVKs found in the dependency graph against the keys produced in the input map.
    *   **IF** a GVK exists in the graph (as a `watched` or `read` resource) **BUT** has no corresponding entry in `input-map.json`,
    *   **THEN** the tool **MUST** emit a warning to the user:
        > "Warning: No seed templates found for GVK [Group/Version/Kind]. Coverage for this resource will be limited to basic schema fuzzing."

## Generation Workflow

This map is the **Artifact** produced by the Input Generation pipeline:

1.  **Analysis:** The LLM/Tool identifies builders (Code-First) or manifests (File-Based).
2.  **Execution:** The system runs the necessary Go code or parses the YAML files.
3.  **Serialization:** The resulting in-memory objects are serialized to JSON.
4.  **Aggregation:** All objects are collected into this single `input-map.json` file.

## Consumption

The Kamera Test Runner uses this map as a simple lookup table:

> "I need to fuzz a `Service`. Let me look up `serving.knative.dev/v1/Service` in the map. I see 3 templates. I'll pick the 'default' one as my seed."

This decouples the **Testing Logic** (which just needs valid JSON) from the **Project Complexity** (Go modules, internal packages, build flags).
