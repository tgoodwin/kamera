# KCP In-Repo Kamera Harness Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a kamera harness at `kcp/kamera/` that directly imports real KCP controller code via `//go:linkname`, replacing the local-copy approach.

**Architecture:** Separate Go module inside the KCP repo with `replace` directives for kamera (local path) and KCP staging deps (inherited from root go.mod). Controllers are constructed via their real `New*()` constructors; unexported `process()` methods are accessed via `//go:linkname` stubs.

**Tech Stack:** Go 1.24, KCP controller packages, kamera `pkg/tracecheck` + `pkg/explore`, `//go:linkname` + `unsafe`

**Spec:** `docs/superpowers/specs/2026-03-17-kcp-in-repo-harness-design.md`

---

## File Structure

All files created in `/Users/tgoodwin/projects/kcp/kamera/`:

| File | Responsibility |
|------|---------------|
| `go.mod` | Module declaration, replace directives for kamera + KCP staging |
| `linkname.go` | `//go:linkname` stubs for all controller `process()` methods |
| `harness.go` | Fake client setup, informer factory, indexer installation, controller construction, effect reactor |
| `scenario.go` | ExplorerBuilder configuration, seed object builders, watch mappers, scenario conversion |
| `main.go` | CLI entry point (batch + interactive modes) |
| `smoke_test.go` | Convergence test for the default scenario |
| `scenario_test.go` | Unit tests for scenario builder |
| `inputs.json` | Checked-in seed scenario |

Additionally, a symlink at `/Users/tgoodwin/projects/kamera/examples/kcp`.

---

### Task 1: Scaffold go.mod and verify dependency resolution

**Files:**
- Create: `kcp/kamera/go.mod`

This is the riskiest step — if the dependency graph doesn't resolve, nothing else works. We need the KCP staging replace directives plus a local replace for kamera.

- [ ] **Step 1: Create go.mod with module declaration and replace directives**

```go
module github.com/kcp-dev/kcp/kamera

go 1.24.0

require (
	github.com/tgoodwin/kamera v0.0.0
	github.com/kcp-dev/kcp v0.30.1
)

replace (
	github.com/tgoodwin/kamera => /Users/tgoodwin/projects/kamera
	github.com/kcp-dev/kcp => ../
	github.com/kcp-dev/kcp/sdk => ../sdk
)
```

Note: We reference the parent KCP module via `../` and its SDK via `../sdk`, matching how KCP's own root go.mod uses `replace github.com/kcp-dev/kcp/sdk => ./sdk`. The KCP staging k8s replace directives will need to be copied from the root `go.mod` since Go doesn't inherit replace directives across modules.

- [ ] **Step 2: Create a minimal main.go that imports a KCP controller package**

```go
package main

import (
	_ "github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster"
	_ "github.com/tgoodwin/kamera/pkg/tracecheck"
)

func main() {}
```

- [ ] **Step 3: Run go mod tidy and fix any missing replace directives**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go mod tidy
```

This will likely fail initially because KCP's controller packages transitively import k8s staging modules that need replace directives. Copy the full set of `k8s.io/... => github.com/kcp-dev/kubernetes/staging/...` replace directives from `/Users/tgoodwin/projects/kcp/go.mod` into `kamera/go.mod`. Also copy any `kcp-dev/client-go`, `kcp-dev/apimachinery`, etc. replace directives that the root module uses.

Iterate: run `go mod tidy`, read the error, add the missing replace, repeat until it resolves.

**Note:** Check if a `go.work` file exists in the KCP repo root. If it does, the `kamera/` module may need to be added to it, or excluded from it, depending on whether the workspace includes nested modules. Since `kamera/` has its own `go.mod`, Go will exclude it from the parent module's build graph regardless.

- [ ] **Step 4: Verify the minimal main.go compiles**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build .
```

Expected: successful build, producing a `kamera` binary.

- [ ] **Step 5: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/go.mod kamera/go.sum kamera/main.go
git commit -m "kamera: scaffold go.mod with KCP + kamera dependencies"
```

---

### Task 2: Linkname stubs for Phase 1 controllers (LogicalCluster + APIBinder)

**Files:**
- Create: `kcp/kamera/linkname.go`

- [ ] **Step 1: Write linkname.go with stubs for LogicalCluster and APIBinder process methods**

```go
package main

import (
	"context"
	_ "unsafe"

	corelogicalcluster "github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster"
	initialization "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/initialization"
)

//go:linkname logicalClusterProcess github.com/kcp-dev/kcp/pkg/reconciler/core/logicalcluster.(*Controller).process
func logicalClusterProcess(c *corelogicalcluster.Controller, ctx context.Context, key string) (bool, error)

//go:linkname apiBinderProcess github.com/kcp-dev/kcp/pkg/reconciler/tenancy/initialization.(*APIBinder).process
func apiBinderProcess(c *initialization.APIBinder, ctx context.Context, key string) error
```

Both `Controller` and `APIBinder` are exported types, so we can reference them directly — no `unsafe.Pointer` needed for these two.

**Note:** Only the LogicalCluster linkname stub existed in the previous harness. The APIBinder stub is new and untested. If the linker rejects it, verify the exact symbol path against `go tool nm` output on the KCP binary.

- [ ] **Step 2: Verify it compiles**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build .
```

Expected: successful build. If the linker complains about missing symbols, check the exact symbol path matches the package + type + method name.

- [ ] **Step 3: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/linkname.go
git commit -m "kamera: add linkname stubs for LogicalCluster and APIBinder process methods"
```

---

### Task 3: Harness wiring for Phase 1 controllers

**Files:**
- Create: `kcp/kamera/harness.go`

This file sets up fake clients, informers, indexers, and constructs the real controllers. Port from the existing harness at `/Users/tgoodwin/projects/kamera/.worktrees/codex-kcp-example/examples/kcp/upstream_strategy.go`.

- [ ] **Step 1: Write harness.go**

Key structures and functions to implement:

```go
package main

// kcpHarness holds constructed controllers and shared infrastructure
type kcpHarness struct {
    scheme    *runtime.Scheme
    recorder  replay.EffectRecorder
    kcpClient *kcpfakeclient.ClusterClientset

    logicalClusterController *corelogicalcluster.Controller
    apiBinderController      *initialization.APIBinder
}

// kcpStrategy implements tracecheck.Strategy
type kcpStrategy struct {
    controllerID tracecheck.ReconcilerID
    scheme       *runtime.Scheme
    recorder     replay.EffectRecorder
}
```

Port the following functions from the existing `upstream_strategy.go`:
- `newKCPHarness()` — but use real `corelogicalcluster.NewController()` and `initialization.NewAPIBinder()` constructors instead of local copies
- `PrepareState()` / `ReconcileAtState()` — the Strategy interface methods. **Important:** `PrepareState()` must replace `rand.Reader` with a deterministic reader (fixed bytes) for reproducible exploration, and restore it in the cleanup func. Port this from the existing harness (lines 62-64 of `upstream_strategy.go`).
- `splitObjectsForClients()`, `restoreTypedObject()` — object conversion
- `newKCPReactor()`, `objectForAction()`, `lookupTrackedObject()` — effect recording. **Note:** `objectForAction()` must handle `"updatesubresource"` verb (used by KCP fake client for `UpdateStatus()` calls) in addition to create/update/patch/delete.
- `applyPatch()`, `mutateServerSideObject()`, `toRecordedObject()` — helpers
- `ensureAllSynced()`, `harnessFromContext()` — utilities

For controller construction in `newKCPHarness()`, use:
```go
// LogicalCluster — same as existing harness
logicalClusterController, err := corelogicalcluster.NewController(
    func() string { return rootShardExternal },
    h.kcpClient,
    logicalClusterInformer,
)

// APIBinder — directly imported constructor (was local copy before).
// NOTE: The real NewAPIBinder takes 7 params. The existing harness's local
// newKCPAPIBinder wrapper took 9 (extra logicalClusterInformer args) — that
// was an artifact of the local copy's different wiring. Use the real 7-param
// signature here.
apiBinderController, err := initialization.NewAPIBinder(
    h.kcpClient,
    logicalClusterInformer,
    workspaceTypeInformer,   // local
    workspaceTypeInformer,   // global (same informer in single-shard harness)
    apiBindingInformer,
    apiExportInformer,       // local
    apiExportInformer,       // global (same)
)
```

For `ReconcileAtState()`, dispatch by controller ID:
```go
key := kcpcache.ToClusterAwareKey(name.Namespace, "", name.Name)
switch s.controllerID {
case logicalClusterControllerID:
    requeue, err := logicalClusterProcess(harness.logicalClusterController, ctx, key)
    return reconcile.Result{Requeue: requeue}, err
case apiBinderInitializerControllerID:
    return reconcile.Result{}, apiBinderProcess(harness.apiBinderController, ctx, key)
}
```

Install KCP indexers **before** starting informers (indexers configure the backing store's index functions; adding them after start is a race condition). The existing harness had local `installKCP*Indexers` functions. Now use the real `github.com/kcp-dev/kcp/pkg/indexers` package.

**Indexers needed for Phase 1 (LogicalCluster + APIBinder):**
- `indexers.AddIfNotPresentOrDie(workspaceTypeInformer.Informer().GetIndexer(), cache.Indexers{indexers.ByLogicalClusterPathAndName: indexers.IndexByLogicalClusterPathAndName})` — for WorkspaceType path+name lookup
- Same `ByLogicalClusterPathAndName` indexer on APIExport informer
- `indexers.AddIfNotPresentOrDie(apiBindingInformer.Informer().GetIndexer(), cache.Indexers{indexers.APIBindingsByAPIExport: indexers.IndexAPIBindingsByAPIExport})` — for APIBinding→APIExport reverse lookup

Check `pkg/indexers/` for the exact function names and index keys. The existing local harness's `installKCPAPIBinderIndexers()` function shows which informers get which indexers — the real `pkg/indexers` package should export equivalent helpers.

Reference: `/Users/tgoodwin/projects/kcp/pkg/reconciler/tenancy/initialization/apibinder_initializer_controller.go` lines 75-97 shows what the constructor reads from informer indexers.

- [ ] **Step 2: Verify it compiles**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build .
```

- [ ] **Step 3: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/harness.go
git commit -m "kamera: wire harness with real LogicalCluster and APIBinder controllers"
```

---

### Task 4: Scenario builder and seed objects

**Files:**
- Create: `kcp/kamera/scenario.go`
- Create: `kcp/kamera/inputs.json`

Port from `/Users/tgoodwin/projects/kamera/.worktrees/codex-kcp-example/examples/kcp/scenario.go`. This is largely a copy with import path changes.

- [ ] **Step 1: Write scenario.go**

Port these functions from the existing harness (they are independent of which controllers are local vs imported):
- `newKCPExplorerBuilder()` — register Phase 1 controllers only (LogicalCluster + APIBinder)
- `buildInitialKCPState()`, `addSeedObject()`
- All seed object builders: `buildRootLogicalClusterTyped()`, `buildProviderLogicalClusterTyped()`, `buildConsumerLogicalClusterTyped()`, `buildSchedulableShardTyped()`, `buildWorkspaceTypeTyped()`, `buildAPIExportTyped()`, `buildConsumerAPIBindingTyped()`, `buildPartitionTyped()`, `buildAPIExportEndpointSliceTyped()`
- Watch mappers: `enqueueTraceObject()`, `enqueueLogicalClusterForAPIBinding()`
- Helpers: `mustAddToScheme()`, `mustToTraceObject()`, `traceClusterFor()`, `needsClusterRequestKey()`
- Scenario conversion: `scenariosFromInputs()`, `buildStateFromCoverageInput()`, `buildUserActionsFromCoverageInput()`, `applyInputTuning()`
- `defaultInteractiveUserActions()`

For Phase 1, register only 2 controllers in `newKCPExplorerBuilder()`:
```go
builder.WithCustomStrategy(logicalClusterControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(logicalClusterControllerID, scheme, r)
}).For("core.kcp.io/LogicalCluster").
    Watches("core.kcp.io/LogicalCluster", enqueueTraceObject())

builder.WithCustomStrategy(apiBinderInitializerControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(apiBinderInitializerControllerID, scheme, r)
}).For("core.kcp.io/LogicalCluster").
    Watches("core.kcp.io/LogicalCluster", enqueueTraceObject()).
    Watches("apis.kcp.io/APIBinding", enqueueLogicalClusterForAPIBinding)
```

- [ ] **Step 2: Copy inputs.json from existing harness**

```bash
cp /Users/tgoodwin/projects/kamera/.worktrees/codex-kcp-example/examples/kcp/inputs.json /Users/tgoodwin/projects/kcp/kamera/inputs.json
```

- [ ] **Step 3: Verify it compiles**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build .
```

- [ ] **Step 4: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/scenario.go kamera/inputs.json
git commit -m "kamera: add scenario builder and seed objects for Phase 1"
```

---

### Task 5: Main entry point

**Files:**
- Modify: `kcp/kamera/main.go` (replace the minimal stub from Task 1)

- [ ] **Step 1: Write main.go**

Port directly from `/Users/tgoodwin/projects/kamera/.worktrees/codex-kcp-example/examples/kcp/main.go`. The logic is identical — it calls `newKCPExplorerBuilder()`, optionally loads config/inputs, and runs via `explore.Runner` or `explore.ParallelRunner`.

- [ ] **Step 2: Verify it compiles and runs with --help or no args**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build . && ./kamera --help
```

- [ ] **Step 3: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/main.go
git commit -m "kamera: add CLI entry point"
```

---

### Task 6: Smoke test for Phase 1

**Files:**
- Create: `kcp/kamera/smoke_test.go`
- Create: `kcp/kamera/scenario_test.go`

- [ ] **Step 1: Write smoke_test.go**

Adapted from the existing harness's smoke test. For Phase 1 (only 2 controllers), assert:
- At least one converged state exists
- LogicalCluster object present with `status.phase == "Ready"`
- Execution path includes LogicalCluster and APIBinder controllers

```go
func TestKCPPhase1Converges(t *testing.T) {
    builder := newKCPExplorerBuilder()
    builder.WithUserActions(defaultInteractiveUserActions())

    explorer, err := builder.Build("standalone")
    if err != nil {
        t.Fatalf("build explorer: %v", err)
    }

    result := explorer.Explore(context.Background(), buildInitialKCPState(builder))
    if len(result.ConvergedStates) == 0 {
        t.Fatal("expected at least one converged state")
    }

    state := result.ConvergedStates[0]
    objects := explorer.Objects(state)
    assertHasObject(t, objects, "core.kcp.io", "LogicalCluster", "cluster")
    assertObjectFieldEquals(t, objects, "core.kcp.io", "LogicalCluster", "cluster", "status.phase", "Ready")

    path := state.Paths[0]
    assertPathIncludesController(t, path, logicalClusterControllerID)
    assertPathIncludesController(t, path, apiBinderInitializerControllerID)
}
```

Port all assertion helpers from the existing `smoke_test.go`: `assertHasObject`, `assertObjectFieldEquals`, `assertConditionStatus`, `assertObjectAnnotationEquals`, `assertSliceEndpointURL`, `assertPathIncludesController`, `findObject`.

- [ ] **Step 2: Write scenario_test.go**

Port from existing harness. For Phase 1, include:
- `TestScenariosFromInputsRequiresBuilder`
- `TestScenariosFromInputsRequiresInputs`
- `TestDefaultInteractiveUserActionsAreEmpty`

- [ ] **Step 3: Run tests**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go test -v -count=1 -run TestKCPPhase1Converges ./...
```

Expected: PASS. If the test fails, debug by checking:
1. Are informer caches synced before controller construction?
2. Are the right indexers installed?
3. Does the linkname stub match the real `process()` signature?

- [ ] **Step 4: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/smoke_test.go kamera/scenario_test.go
git commit -m "kamera: add smoke and scenario tests for Phase 1"
```

---

### Task 7: Symlink from kamera examples

**Files:**
- Create: symlink at `kamera/examples/kcp`

This replaces the existing harness at `.worktrees/codex-kcp-example/examples/kcp/` (which was the out-of-repo, local-copy approach). The worktree-based harness can be archived once the in-repo harness passes all tests.

- [ ] **Step 1: Create symlink**

```bash
ln -sf /Users/tgoodwin/projects/kcp/kamera /Users/tgoodwin/projects/kamera/examples/kcp
```

- [ ] **Step 2: Verify symlink works**

```bash
ls -la /Users/tgoodwin/projects/kamera/examples/kcp
```

Expected: symlink pointing to `/Users/tgoodwin/projects/kcp/kamera`

- [ ] **Step 3: Commit in kamera repo**

```bash
cd /Users/tgoodwin/projects/kamera && git add examples/kcp
git commit -m "examples: symlink kcp harness to in-repo location"
```

---

### Task 8: Phase 2 — Add remaining 4 controllers

**Files:**
- Modify: `kcp/kamera/linkname.go` (add 4 new stubs)
- Modify: `kcp/kamera/harness.go` (add controller construction + dispatch)
- Modify: `kcp/kamera/scenario.go` (register 4 new controllers in builder)
- Modify: `kcp/kamera/smoke_test.go` (expand convergence assertions)

- [ ] **Step 1: Add linkname stubs for remaining controllers**

Add to `linkname.go`:

```go
import (
    defaultapibinding "github.com/kcp-dev/kcp/pkg/reconciler/tenancy/defaultapibindinglifecycle"
)

//go:linkname defaultAPIBindingProcess github.com/kcp-dev/kcp/pkg/reconciler/tenancy/defaultapibindinglifecycle.(*DefaultAPIBindingController).process
func defaultAPIBindingProcess(c *defaultapibinding.DefaultAPIBindingController, ctx context.Context, key string) error

// These 3 controllers have unexported types — use unsafe.Pointer
//go:linkname extraAnnotationSyncProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/extraannotationsync.(*controller).process
func extraAnnotationSyncProcess(c unsafe.Pointer, ctx context.Context, key string) error

//go:linkname apiExportEndpointSliceProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointslice.(*controller).process
func apiExportEndpointSliceProcess(c unsafe.Pointer, ctx context.Context, key string) error

//go:linkname apiExportEndpointSliceURLsProcess github.com/kcp-dev/kcp/pkg/reconciler/apis/apiexportendpointsliceurls.(*controller).process
func apiExportEndpointSliceURLsProcess(c unsafe.Pointer, ctx context.Context, key string) (bool, error)
```

- [ ] **Step 2: Verify linkname stubs compile**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go build .
```

- [ ] **Step 3: Add controller construction to harness.go**

Extend `kcpHarness` struct:
```go
type kcpHarness struct {
    // ... existing fields ...
    defaultAPIBindingController         *defaultapibinding.DefaultAPIBindingController
    extraAnnotationSyncController       unsafe.Pointer
    apiExportEndpointSliceController    unsafe.Pointer
    apiExportEndpointSliceURLsController unsafe.Pointer
}
```

In `newKCPHarness()`, add construction for each:

```go
// DefaultAPIBindingController (exported type)
defaultAPIBindingCtrl, err := defaultapibinding.NewDefaultAPIBindingController(
    h.kcpClient,
    logicalClusterInformer,
    workspaceTypeInformer, workspaceTypeInformer,
    apiBindingInformer,
    apiExportInformer, apiExportInformer,
)

// ExtraAnnotationSync (unexported type — capture as unsafe.Pointer)
extraAnnotationCtrl, err := extraannotationsync.NewController(
    h.kcpClient, apiExportInformer, apiBindingInformer,
)
h.extraAnnotationSyncController = pointerTo(extraAnnotationCtrl)

// APIExportEndpointSlice (unexported type)
apiExportEndpointSliceCtrl, err := apiexportendpointslice.NewController(
    apiExportEndpointSliceInformer, apiExportInformer,
    partitionInformer, h.kcpClient,
)
h.apiExportEndpointSliceController = pointerTo(apiExportEndpointSliceCtrl)

// APIExportEndpointSliceURLs (unexported type)
apiExportEndpointSliceURLsCtrl, err := apiexportendpointsliceurls.NewController(
    rootShardName,
    apiExportEndpointSliceInformer, apiBindingInformer,
    apiExportEndpointSliceInformer, shardInformer,
    apiExportInformer, h.kcpClient,
)
h.apiExportEndpointSliceURLsController = pointerTo(apiExportEndpointSliceURLsCtrl)
```

Where `pointerTo` safely extracts `unsafe.Pointer` (use the defensive version from the existing harness):
```go
func pointerTo(v any) unsafe.Pointer {
    rv := reflect.ValueOf(v)
    if rv.Kind() != reflect.Pointer || rv.IsNil() {
        return nil
    }
    return unsafe.Pointer(rv.Pointer())
}
```

Extend `ReconcileAtState()` dispatch:
```go
case defaultAPIBindingLifecycleControllerID:
    return reconcile.Result{}, defaultAPIBindingProcess(harness.defaultAPIBindingController, ctx, key)
case apiBindingAnnotationSyncControllerID:
    return reconcile.Result{}, extraAnnotationSyncProcess(harness.extraAnnotationSyncController, ctx, key)
case apiExportEndpointSliceControllerID:
    return reconcile.Result{}, apiExportEndpointSliceProcess(harness.apiExportEndpointSliceController, ctx, key)
case apiExportEndpointSliceURLsControllerID:
    requeue, err := apiExportEndpointSliceURLsProcess(harness.apiExportEndpointSliceURLsController, ctx, key)
    return reconcile.Result{Requeue: requeue}, err
```

Also install the indexers needed by these controllers (port from existing harness's `installKCP*Indexers` functions, or use the real indexer installation if `pkg/indexers` exports what we need).

- [ ] **Step 4: Register remaining controllers in scenario.go**

Add to `newKCPExplorerBuilder()`:

```go
builder.WithCustomStrategy(defaultAPIBindingLifecycleControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(defaultAPIBindingLifecycleControllerID, scheme, r)
}).For("core.kcp.io/LogicalCluster").
    Watches("core.kcp.io/LogicalCluster", enqueueTraceObject()).
    Watches("apis.kcp.io/APIBinding", enqueueLogicalClusterForAPIBinding)

builder.WithCustomStrategy(apiExportEndpointSliceControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(apiExportEndpointSliceControllerID, scheme, r)
}).For("apis.kcp.io/APIExportEndpointSlice").
    Watches("apis.kcp.io/APIExportEndpointSlice", enqueueTraceObject())

builder.WithCustomStrategy(apiExportEndpointSliceURLsControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(apiExportEndpointSliceURLsControllerID, scheme, r)
}).For("apis.kcp.io/APIExportEndpointSlice").
    Watches("apis.kcp.io/APIExportEndpointSlice", enqueueTraceObject()).
    Watches("apis.kcp.io/APIBinding", enqueueEndpointSliceForAPIBinding)

builder.WithCustomStrategy(apiBindingAnnotationSyncControllerID, func(r replay.EffectRecorder) tracecheck.Strategy {
    return newUpstreamKCPStrategy(apiBindingAnnotationSyncControllerID, scheme, r)
}).For("apis.kcp.io/APIBinding").
    Watches("apis.kcp.io/APIBinding", enqueueTraceObject())
```

Add the `enqueueEndpointSliceForAPIBinding` watch mapper (port from existing harness).

- [ ] **Step 5: Expand smoke test to cover all 6 controllers**

Update `smoke_test.go` to match the existing harness's full assertions:

```go
func TestKCPFullScenarioConverges(t *testing.T) {
    builder := newKCPExplorerBuilder()
    builder.WithUserActions(defaultInteractiveUserActions())

    explorer, err := builder.Build("standalone")
    if err != nil {
        t.Fatalf("build explorer: %v", err)
    }

    result := explorer.Explore(context.Background(), buildInitialKCPState(builder))
    if len(result.ConvergedStates) == 0 {
        t.Fatal("expected at least one converged state")
    }

    state := result.ConvergedStates[0]
    objects := explorer.Objects(state)
    bindingName := buildConsumerAPIBindingTyped().Name

    assertHasObject(t, objects, "core.kcp.io", "LogicalCluster", "cluster")
    assertHasObject(t, objects, "apis.kcp.io", "APIBinding", bindingName)
    assertHasObject(t, objects, "apis.kcp.io", "APIExportEndpointSlice", "widgets")

    assertObjectFieldEquals(t, objects, "core.kcp.io", "LogicalCluster", "cluster", "status.phase", "Ready")
    assertConditionStatus(t, objects, "apis.kcp.io", "APIBinding", bindingName, "InitialBindingCompleted", "True")
    assertObjectAnnotationEquals(t, objects, "apis.kcp.io", "APIBinding", bindingName, "extra.apis.kcp.io/visibility", "internal")
    assertSliceEndpointURL(t, objects, "widgets", "https://root.example.invalid/services/apiexport/root:provider/widgets")

    path := state.Paths[0]
    assertPathIncludesController(t, path, logicalClusterControllerID)
    assertPathIncludesController(t, path, apiBinderInitializerControllerID)
    assertPathIncludesController(t, path, defaultAPIBindingLifecycleControllerID)
    assertPathIncludesController(t, path, apiExportEndpointSliceControllerID)
    assertPathIncludesController(t, path, apiExportEndpointSliceURLsControllerID)
    assertPathIncludesController(t, path, apiBindingAnnotationSyncControllerID)
}
```

- [ ] **Step 6: Run full test suite**

```bash
cd /Users/tgoodwin/projects/kcp/kamera && go test -v -count=1 ./...
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
cd /Users/tgoodwin/projects/kcp && git add kamera/
git commit -m "kamera: add remaining 4 controllers (Phase 2 complete)"
```
