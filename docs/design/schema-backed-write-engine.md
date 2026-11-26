# Schema-backed API writes

Kamera can opt individual Kubernetes resource versions into schema-backed API
write semantics. For those resources, Server-Side Apply (SSA) is materialized
with Kubernetes' upstream `managedfields` implementation before the controller
client call returns. The committed exploration effect therefore contains the
server result, not the partial apply configuration.

This is the first milestone of [issue #82](https://github.com/tgoodwin/kamera/issues/82)
and replaces the full-object APPLY behavior described by
[issue #78](https://github.com/tgoodwin/kamera/issues/78) for registered GVKs.

## Registering schemas

Schema lookup uses the complete group, version, and kind. A Go `runtime.Scheme`
is not enough because it does not retain Kubernetes merge topology such as
list-map keys or atomic map behavior.

Register CRDs when possible:

```go
builder := tracecheck.NewExplorerBuilder(scheme).
    WithCRD(widgetCRD)
```

`WithCRD` registers a served version's structural schema, resource scope, and
status-subresource availability. This milestone rejects CRDs with multiple
served versions because exact cross-version ownership requires conversion.

Built-in or aggregated resources can be registered from a structural schema:

```go
builder.WithResourceSchema(
    corev1.SchemeGroupVersion.WithKind("ConfigMap"),
    true,  // namespaced
    false, // no status subresource
    configMapSchema,
)
```

A complete OpenAPI v3 document must be paired with authoritative route
metadata; OpenAPI alone does not identify resource scope or status-subresource
availability. `WithOpenAPIV3(document, apiserver.OpenAPIResource{...})` accepts
that explicit metadata, while a document without it is rejected rather than
being guessed. For the simplest setup, use `WithCRD` for CRDs or
`WithResourceSchema(gvk, namespaced, hasStatus, schema)` for built-in and
aggregated resources.

Schema registration errors are returned by `Build` or
`BuildStartStateFromObjects` rather than being deferred into exploration.

## Strict and incremental modes

Registration is incremental by default. A registered GVK uses the new engine;
an unregistered GVK keeps Kamera's existing write behavior. This permits a
harness to migrate one interaction hotspot at a time.

Call `RequireSchemas()` to fail closed for non-apply writes, while APPLY is
always fail-closed: an APPLY to an unregistered GVK is a simulation setup error
in both modes. The error names the reconciler, GVK, resource, and registration
path. Reads, deletes, and (when strict mode is off) legacy non-apply writes do
not require a schema. Initial objects are fixtures, not writes; unregistered
fixtures remain readable and are not given inferred ownership.

Forked builders retain the same immutable schema configuration and strictness.
The mutable ownership state is not kept in the registry; it lives in each
object's `metadata.managedFields`, so it remains branch-local.

## Modeled behavior for registered resources

The first milestone models:

- complete apply bodies from raw apply patches and typed apply configurations;
- field manager, force, dry-run, field-validation, patch type, and subresource
  request information;
- schema-aware SSA merge topology using Kubernetes structured merge;
- managed-field ownership, conflicts, force transfer, shared ownership, and
  omission-based field removal;
- ownership established and updated by CREATE and UPDATE;
- main-resource and status APPLY isolation;
- generic ObjectMeta and owner-reference validation after merging;
- synchronous API errors before the controller follows its success path;
- a frame-local live API view that advances across multiple writes in one
  reconcile, while informer reads may remain stale;
- generation and resource-version responses for materialized writes; and
- commit-only handling in `applyEffects` for already materialized results.

Fixture objects with no managed fields are processed through CREATE ownership
using the deterministic manager `kamera-initial-state` only when their GVK is
registered. Authored manifests therefore get deterministic installer
ownership. Captured/live fixtures must carry real `metadata.managedFields` for
ownership fidelity; the simulator cannot infer those historical owners. An
unregistered fixture is still available to reads and does not require a
schema.

Managed-field timestamps are cleared after upstream field management. They are
observational wall-clock data that would otherwise distinguish identical
exploration branches; managers, operations, API versions, subresources, and
field sets are retained because they affect later writes.

## Current limitations

The schema is currently used for merge topology and managed fields, not as a
complete API-server pipeline. This milestone does not yet provide structural
defaulting, unknown-field pruning, structural/CEL validation, admission
webhooks, conversion webhooks, or complete handwritten validation and
defaulting for built-in resources.

Schema-backed merge patches consume the actual patch body, merge it into the
frame-live object (including null deletion), validate an explicit patch
resourceVersion before server metadata replaces it, and run the merged object
through Kubernetes ownership update semantics. Patch field-manager identity is
preserved, and dry runs do not alter frame bookkeeping. Unsupported strategic,
JSON, and CBOR patch ownership semantics remain outside this milestone; only
JSON/YAML merge patches use this schema-aware path.
Schema-backed and legacy writes share the branch's resource-version sequence.
Only writes that change stored state advance that sequence.

Registries are static for an exploration run. Creating, updating, or deleting a
CRD during a branch does not yet change branch-local schema availability.
Multi-version CRDs and OpenAPI registrations without explicit route metadata
are rejected rather than run with identity conversion. Scale subresources, aggregated API-server-specific
behavior, and versioned built-in schema bundles are also outside this milestone.

The Crossplane example registers ConfigMap explicitly because it is the shared
composed resource involved in issue #78. Other Crossplane resource types remain
on the legacy path until their schemas are registered.
