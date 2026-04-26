# In-Repo KCP Kamera Harness

## Goal

Build a kamera harness inside the KCP repository (`kcp/kamera/`) that directly imports real controller code, eliminating the local-copy approach used in the out-of-repo harness. This avoids drift risk and ensures we always test controller logic at HEAD.

## Context

The existing harness at `kamera/examples/kcp/` lives outside the KCP module. Because KCP's controller packages transitively depend on unpublished staging modules (`kcp-dev/kubernetes/staging/...`), they can't be imported from an external Go module. The existing harness works around this by copying ~500 lines of reconcile logic into local files.

Moving the harness into the KCP repo solves the dependency graph problem entirely. All staging `replace` directives are already in scope. The remaining challenge is visibility: most controllers have unexported `process()` methods, which we solve with `//go:linkname`.

## Architecture

### Directory layout

```
kcp/kamera/
├── go.mod                # module github.com/kcp-dev/kcp/kamera
├── go.sum
├── main.go               # CLI entry point
├── linkname.go           # //go:linkname stubs for unexported process() methods
├── harness.go            # controller wiring: fake clients, informers, indexers
├── scenario.go           # scenario builder, seed objects, watch mappers
├── scenario_test.go      # smoke tests
└── inputs.json           # checked-in seed scenario
```

### Symlink for discoverability

Following the `cluster-api` pattern, symlink from the kamera repo:

```
kamera/examples/kcp -> /Users/tgoodwin/projects/kcp/kamera
```

This keeps the harness visible alongside other examples without duplicating code.

### Module setup

`kamera/go.mod` declares a separate module (same pattern as `kcp/sdk/`):

```go
module github.com/kcp-dev/kcp/kamera

go 1.24.0

require (
    github.com/tgoodwin/kamera v0.0.0-...
    github.com/kcp-dev/kcp v0.30.1-...
    // ... k8s deps
)

replace (
    // Development: local path. For sharing, use a relative path or versioned dep.
    github.com/tgoodwin/kamera => /Users/tgoodwin/projects/kamera
    // inherit KCP's staging replace directives as needed
)
```

### Controllers

Six controllers, all imported from real KCP packages:

| # | Controller | Package | Type | process() signature | Linkname needed? |
|---|-----------|---------|------|-------------------|-----------------|
| 1 | LogicalCluster | `core/logicalcluster` | `*Controller` (exported) | `process(ctx, key string) (bool, error)` | Yes (unexported method on exported type) |
| 2 | APIBinderInitializer | `tenancy/initialization` | `*APIBinder` (exported) | `process(ctx, key string) error` | Yes (unexported method) |
| 3 | DefaultAPIBindingLifecycle | `tenancy/defaultapibindinglifecycle` | `*DefaultAPIBindingController` (exported) | `process(ctx, key string) error` | Yes (unexported method) |
| 4 | ExtraAnnotationSync | `apis/extraannotationsync` | `*controller` (unexported) | `process(ctx, key string) error` | Yes (unexported type + method) |
| 5 | APIExportEndpointSlice | `apis/apiexportendpointslice` | `*controller` (unexported) | `process(ctx, key string) error` | Yes (unexported type + method) |
| 6 | APIExportEndpointSliceURLs | `apis/apiexportendpointsliceurls` | `*controller` (unexported) | `process(ctx, key string) (bool, error)` | Yes (unexported type + method) |

### Linkname approach

For controllers with **exported types** (#1-3), linkname references the type directly:

```go
//go:linkname logicalClusterProcess github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster.(*Controller).process
func logicalClusterProcess(c *corelogicalcluster.Controller, ctx context.Context, key string) (bool, error)
```

For controllers with **unexported types** (#4-6), we use `unsafe.Pointer` since we can't name the type:

```go
//go:linkname extraAnnotationSyncProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/extraannotationsync.(*controller).process
func extraAnnotationSyncProcess(c unsafe.Pointer, ctx context.Context, key string) error
```

For unexported types, the constructors (e.g., `extraannotationsync.NewController()`) return `(*controller, error)`. Even though `controller` is unexported, Go allows holding the return value in a local variable. We extract `unsafe.Pointer` from it for use with linkname:

```go
// NewController returns *controller (unexported type) — we can hold it but not name its type.
// Extract unsafe.Pointer via reflect to pass to the linkname stub.
func ptrFor(v any) unsafe.Pointer {
    return unsafe.Pointer(reflect.ValueOf(v).Pointer())
}

// In harness construction:
extraAnnotationCtrl, err := extraannotationsync.NewController(kcpClient, apiExportInformer, apiBindingInformer)
// extraAnnotationCtrl is *extraannotationsync.controller (unnamed outside the package)
extraAnnotationPtr := ptrFor(extraAnnotationCtrl)

// Later, in ReconcileAtState:
extraAnnotationSyncProcess(extraAnnotationPtr, ctx, key)
```

This works because `unsafe.Pointer` and `*controller` are both pointer-width values with identical calling convention at the ABI level. This is implementation-defined behavior (not a Go spec guarantee), but is stable across all Go versions that support `//go:linkname`.

### Key format translation

KCP controllers use string keys in the format produced by `kcpcache.DeletionHandlingMetaClusterNamespaceKeyFunc`, which encodes cluster-aware keys as `cluster|namespace/name` or `cluster|name` for cluster-scoped resources. Kamera's `Strategy.ReconcileAtState` receives `types.NamespacedName`.

The translation layer (same as the existing harness) uses the `Namespace` field to carry the cluster path:

```go
key := kcpcache.ToClusterAwareKey(name.Namespace, "", name.Name)
```

And watch mappers encode the cluster path into `NamespacedName.Namespace` when enqueuing:

```go
func enqueueTraceObject() tracecheck.WatchMapper {
    return func(obj *unstructured.Unstructured) []reconcile.Request {
        return []reconcile.Request{{NamespacedName: types.NamespacedName{
            Namespace: obj.GetNamespace(), // carries cluster path for KCP objects
            Name:      obj.GetName(),
        }}}
    }
}
```

### Harness wiring

`harness.go` constructs the real controller graph:

1. Create `kcpfakeclient.ClusterClientset` seeded with state objects
2. Create `kcpinformers.SharedInformerFactory`
3. Install real KCP indexers via `pkg/indexers` (indexers must be installed **before** starting informers, since they configure the backing store's index functions)
4. Start informers via `factory.Start(stopCh)`, then `factory.WaitForCacheSync(stopCh)` to ensure all caches are populated before controller construction
5. Construct each controller via its real `New*()` constructor (constructors register event handlers on the already-running informers)
6. Store controller references in a harness struct (exported types directly, unexported types as `unsafe.Pointer`)

### Effect recording via cluster-aware fake clients

KCP controllers write through `kcpclientset.ClusterInterface`, not standard `client.Client`. The fake clientset (`kcpfakeclient.ClusterClientset`) supports `PrependReactor()` which intercepts all Create/Update/Patch/Delete operations. The reactor converts each intercepted action into a kamera `EffectRecorder` call:

```go
reactor := func(action kcptesting.Action) (bool, runtime.Object, error) {
    obj, op := objectAndOpFromAction(action)
    recorder.RecordEffect(ctx, toTraceObject(obj, action.GetCluster()), op, nil)
    return false, nil, nil // don't short-circuit — let the fake client handle storage
}
harness.kcpClient.PrependReactor("*", "*", reactor)
```

This is identical to the existing harness pattern.

### Kamera integration

Each controller is registered with `ExplorerBuilder.WithCustomStrategy()`, providing a `Strategy` that:
- In `PrepareState()`: builds a fresh harness from the current state snapshot
- In `ReconcileAtState()`: installs the effect reactor, translates the key, and calls the linkname'd `process()` for the target controller

This is the same pattern as the existing harness — the only change is that we import real constructors and use linkname instead of local copies.

### What stays the same

The following are ported directly from the existing harness with minimal changes:
- Scenario builder logic (seed objects, watch mappers, state construction)
- Effect recording via fake client reactors
- `restoreTypedObject()` conversion between unstructured and typed objects
- `applyPatch()` for JSON/merge patch handling
- Convergence checking and invariant assertions

### Phasing

**Phase 1**: LogicalCluster + APIBinderInitializer
- Scaffold `kamera/` with `go.mod`, linkname stubs, harness wiring for 2 controllers
- Port scenario builder and seed objects from existing harness
- Smoke test proving convergence

**Phase 2**: Add remaining 4 controllers
- DefaultAPIBindingLifecycle, ExtraAnnotationSync, APIExportEndpointSlice, APIExportEndpointSliceURLs
- Extend harness, scenario builder, and seed objects

## What changes vs. existing harness

- **No local controller copies** — import real packages directly
- **No drift risk** — always testing code at HEAD
- **Linkname stubs** are ~1 line each instead of ~100+ lines of copied reconcile logic
- **Lives in KCP repo** — dependency graph problem disappears entirely
- **Trade-off**: harness is coupled to KCP repo structure

## Non-goals

- Testing admission webhooks or multi-shard behavior
- Replacing KCP's existing integration/e2e tests
- Shipping this harness as a supported KCP component (this is research tooling)

## Risks

1. **Linkname fragility**: if a `process()` method is renamed or its signature changes, we get a linker error. Mitigated by being in-repo (CI catches it immediately).
2. **Go toolchain changes**: `//go:linkname` restrictions have been tightened in recent Go releases. Go 1.24 still allows it with the `unsafe` import.
3. **Separate go.mod maintenance**: need to keep `kamera/go.mod` replace directives in sync with root `go.mod`. Manageable since we only need the kamera replace directive plus whatever KCP staging deps we transitively need.
4. **Build isolation**: since `kamera/` has its own `go.mod`, `go build ./...` from the KCP repo root will not enter this directory. No build tags needed. Standard `go work` or explicit `cd kamera && go test` is required to build/test.
