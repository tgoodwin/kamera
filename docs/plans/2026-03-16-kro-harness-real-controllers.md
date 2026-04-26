# KRO Harness: Real Controller Integration

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all hand-rolled KRO controller simulations with real KRO controllers wired into kamera's ExplorerBuilder, maximizing the bug surface available for model checking.

**Architecture:** Two small surgical changes to kro (interface extraction + constructor addition) unblock importing both the real `instance.Controller` and `ResourceGraphDefinitionReconciler`. A `dynamic.Interface` adapter bridges kamera's replay `client.Client` to KRO's client interface. The real `graph.Builder` processes the Application RGD using a core-only schema resolver (no API server).

**Tech Stack:** Go, kamera ExplorerBuilder, KRO controller packages, `k8s.io/client-go/dynamic`, `k8s.io/apiserver/pkg/cel/openapi/resolver`

---

## File Structure

### Surgical changes to kro (`~/projects/kro`)

| File | Change | Lines affected |
|------|--------|---------------|
| `pkg/graph/builder.go` | Add `NewBuilderFromResolver(resolver.SchemaResolver, meta.RESTMapper) *Builder` | +5 lines |
| `pkg/controller/resourcegraphdefinition/controller.go` | Extract `DynamicControllerRegistrar` interface; change field + constructor param type | ~6 lines changed |

### New files in kamera (`examples/kro/`)

| File | Responsibility |
|------|---------------|
| `dynamic_adapter.go` | `dynamic.Interface` adapter backed by replay `client.Client` |
| `clientset_adapter.go` | `kroclient.SetInterface` adapter composing dynamic adapter + CRD client + static REST mapper |
| `reconciler_adapters.go` | Signature adapters for Instance + RGD controllers, DynamicControllerRegistrar stub |

### Modified files in kamera

| File | Changes |
|------|---------|
| `scenario.go` | Delete all hand-rolled controllers; rewire `newKROExplorerBuilder` to use real controllers |
| `go.mod` / `go.sum` | Add `github.com/kubernetes-sigs/kro` dependency (local replace) |

### Deleted code (from `scenario.go`)

| Code | Reason |
|------|--------|
| `resourceGraphDefinitionController` struct + `Reconcile` | Replaced by real RGD reconciler |
| `applicationController` struct + `Reconcile` | Replaced by real Instance controller |
| `reconcileDeployment`, `reconcileService`, `reconcileIngress` | Real controller handles these |
| `updateApplicationStatus` | Real controller handles this |
| `buildApplicationDeployment`, `buildApplicationService`, `buildApplicationIngress` | Real controller builds its own resources |
| `applicationSpec`, `applicationSpecFromObject` | Real controller parses spec internally |
| `setResourceGraphDefinitionStatus` | Real RGD controller handles status |
| `updateCRDIfChanged` | Real RGD controller handles CRD lifecycle |
| `buildApplicationCRD` | Real graph.Builder generates CRD from RGD spec |

### Kept code (in `scenario.go`)

| Code | Reason |
|------|--------|
| `newKROExplorerBuilder` | Refactored to wire real controllers |
| `enqueueApplicationFromManagedResource` | Watch mapper still needed |
| `buildQuickstartApplicationRGD` | Fixture for environment state |
| `buildQuickstartApplicationInstance` | Fixture for user actions |
| `scenariosFromInputs`, `buildStateFromCoverageInput`, `buildUserActionsFromCoverageInput` | Input conversion logic unchanged |
| `managedResourceLabels` | May still be needed for test fixtures |
| Helper functions (`getNestedString`, `setNestedField`, etc.) | Used by fixture builders |

---

## Chunk 1: Surgical KRO Changes

### Task 1: Add `NewBuilderFromResolver` to kro's graph package

**Files:**
- Modify: `/Users/tgoodwin/projects/kro/pkg/graph/builder.go`

This adds a constructor that accepts a pre-built `resolver.SchemaResolver` and `meta.RESTMapper`, bypassing the API server requirement. The `Builder` struct only uses these two fields (line 325: `ResolveSchema`, line 330: `RESTMapping`). Pure data injection, zero behavior change.

- [ ] **Step 1: Add the constructor**

After the existing `NewBuilder` function (line 66), add:

```go
// NewBuilderFromResolver creates a Builder with a pre-built schema resolver
// and REST mapper. Useful for testing or environments without an API server.
func NewBuilderFromResolver(schemaResolver resolver.SchemaResolver, restMapper meta.RESTMapper) *Builder {
	return &Builder{
		schemaResolver: schemaResolver,
		restMapper:     restMapper,
	}
}
```

The import for `resolver` is already present: `"k8s.io/apiserver/pkg/cel/openapi/resolver"` (line 30). The import for `meta` is already present: `"k8s.io/apimachinery/pkg/api/meta"` (line 27).

- [ ] **Step 2: Verify kro builds**

```bash
cd /Users/tgoodwin/projects/kro && go build ./...
```

- [ ] **Step 3: Commit in kro repo**

```bash
cd /Users/tgoodwin/projects/kro
git add pkg/graph/builder.go
git commit -m "feat(graph): add NewBuilderFromResolver for test/harness integration

Adds a constructor that accepts pre-built SchemaResolver and RESTMapper,
enabling graph building without an API server connection."
```

---

### Task 2: Extract `DynamicControllerRegistrar` interface

**Files:**
- Modify: `/Users/tgoodwin/projects/kro/pkg/controller/resourcegraphdefinition/controller.go`

The RGD reconciler only calls `Register()` and `Deregister()` on the `DynamicController`. Extracting a 2-method interface allows test harnesses to provide a stub. The real `*dynamiccontroller.DynamicController` already satisfies the interface.

- [ ] **Step 1: Add the interface definition**

Add before the `ResourceGraphDefinitionReconciler` struct (before line 41):

```go
// DynamicControllerRegistrar abstracts the Register/Deregister operations
// on the DynamicController. The real *dynamiccontroller.DynamicController
// satisfies this interface.
type DynamicControllerRegistrar interface {
	Register(ctx context.Context, parent schema.GroupVersionResource, instanceHandler dynamiccontroller.Handler, resourceGVRsToWatch ...schema.GroupVersionResource) error
	Deregister(ctx context.Context, parent schema.GroupVersionResource) error
}
```

- [ ] **Step 2: Change the field type**

Change line 56 from:
```go
dynamicController       *dynamiccontroller.DynamicController
```
to:
```go
dynamicController       DynamicControllerRegistrar
```

- [ ] **Step 3: Change the constructor parameter type**

Change line 63 from:
```go
dynamicController *dynamiccontroller.DynamicController,
```
to:
```go
dynamicController DynamicControllerRegistrar,
```

- [ ] **Step 4: Verify kro builds and tests pass**

```bash
cd /Users/tgoodwin/projects/kro && go build ./... && go test ./pkg/controller/resourcegraphdefinition/...
```

The real `*dynamiccontroller.DynamicController` already has `Register` and `Deregister` methods with matching signatures, so all existing call sites are satisfied.

- [ ] **Step 5: Commit in kro repo**

```bash
cd /Users/tgoodwin/projects/kro
git add pkg/controller/resourcegraphdefinition/controller.go
git commit -m "refactor(rgd): extract DynamicControllerRegistrar interface

The RGD reconciler only uses Register/Deregister on the DynamicController.
Extracting an interface enables test harnesses to provide stubs without
needing the full DynamicController infrastructure."
```

---

## Chunk 2: Kamera Harness Adapters

### Task 3: Add kro dependency to kamera harness

**Files:**
- Modify: `examples/kro/go.mod`

- [ ] **Step 1: Add kro module dependency with local replace**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro
go get github.com/kubernetes-sigs/kro@latest || true
```

Add replace directive to `go.mod`:
```
replace github.com/kubernetes-sigs/kro => /Users/tgoodwin/projects/kro
```

- [ ] **Step 2: Tidy and verify**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro
go mod tidy
go build ./...
```

- [ ] **Step 3: Commit**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro
git add go.mod go.sum
git commit -m "chore(kro): add github.com/kubernetes-sigs/kro dependency"
```

---

### Task 4: Implement dynamic.Interface adapter

**Files:**
- Create: `examples/kro/dynamic_adapter.go`

Wraps kamera's replay `client.Client` to implement `k8s.io/client-go/dynamic.Interface`. Both KRO controllers use `dynamic.Interface` for API operations. The adapter resolves GVR→GVK via RESTMapper and ensures objects have correct TypeMeta.

- [ ] **Step 1: Write dynamic_adapter.go**

```go
package main

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// replayDynamicClient adapts a controller-runtime client.Client to the
// k8s.io/client-go/dynamic.Interface used by KRO's controllers.
type replayDynamicClient struct {
	inner  client.Client
	mapper meta.RESTMapper
}

var _ dynamic.Interface = (*replayDynamicClient)(nil)

func newReplayDynamicClient(c client.Client, mapper meta.RESTMapper) *replayDynamicClient {
	return &replayDynamicClient{inner: c, mapper: mapper}
}

func (d *replayDynamicClient) Resource(resource schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &replayNamespaceableResource{inner: d.inner, mapper: d.mapper, gvr: resource}
}

// replayNamespaceableResource implements dynamic.NamespaceableResourceInterface.
type replayNamespaceableResource struct {
	inner  client.Client
	mapper meta.RESTMapper
	gvr    schema.GroupVersionResource
}

var _ dynamic.NamespaceableResourceInterface = (*replayNamespaceableResource)(nil)

func (r *replayNamespaceableResource) Namespace(ns string) dynamic.ResourceInterface {
	return &replayResourceClient{inner: r.inner, mapper: r.mapper, gvr: r.gvr, namespace: ns}
}

func (r *replayNamespaceableResource) clusterScoped() dynamic.ResourceInterface {
	return &replayResourceClient{inner: r.inner, mapper: r.mapper, gvr: r.gvr}
}

// Cluster-scoped forwarding methods.
func (r *replayNamespaceableResource) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Create(ctx, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Update(ctx, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	return r.clusterScoped().UpdateStatus(ctx, obj, opts)
}
func (r *replayNamespaceableResource) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	return r.clusterScoped().Delete(ctx, name, opts, subresources...)
}
func (r *replayNamespaceableResource) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return r.clusterScoped().DeleteCollection(ctx, opts, listOpts)
}
func (r *replayNamespaceableResource) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Get(ctx, name, opts, subresources...)
}
func (r *replayNamespaceableResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	return r.clusterScoped().List(ctx, opts)
}
func (r *replayNamespaceableResource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return r.clusterScoped().Watch(ctx, opts)
}
func (r *replayNamespaceableResource) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Patch(ctx, name, pt, data, opts, subresources...)
}
func (r *replayNamespaceableResource) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	return r.clusterScoped().Apply(ctx, name, obj, opts, subresources...)
}
func (r *replayNamespaceableResource) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return r.clusterScoped().ApplyStatus(ctx, name, obj, opts)
}

// replayResourceClient implements dynamic.ResourceInterface by delegating to
// the replay client.Client. Resolves GVR→GVK via REST mapper.
type replayResourceClient struct {
	inner     client.Client
	mapper    meta.RESTMapper
	gvr       schema.GroupVersionResource
	namespace string
}

var _ dynamic.ResourceInterface = (*replayResourceClient)(nil)

func (rc *replayResourceClient) gvk() (schema.GroupVersionKind, error) {
	kinds, err := rc.mapper.KindsFor(rc.gvr)
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("resolve GVK for %s: %w", rc.gvr, err)
	}
	if len(kinds) == 0 {
		return schema.GroupVersionKind{}, fmt.Errorf("no GVK found for %s", rc.gvr)
	}
	return kinds[0], nil
}

func (rc *replayResourceClient) ensureTypeMeta(obj *unstructured.Unstructured) error {
	if obj.GetAPIVersion() != "" && obj.GetKind() != "" {
		return nil
	}
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj.SetGroupVersionKind(gvk)
	return nil
}

func (rc *replayResourceClient) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if rc.namespace != "" && obj.GetNamespace() == "" {
		obj.SetNamespace(rc.namespace)
	}
	if err := rc.inner.Create(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if len(subresources) > 0 && subresources[0] == "status" {
		return rc.UpdateStatus(ctx, obj, metav1.UpdateOptions{})
	}
	if err := rc.inner.Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if err := rc.inner.Status().Update(ctx, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(rc.namespace)
	return rc.inner.Delete(ctx, obj)
}

func (rc *replayResourceClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	gvk, err := rc.gvk()
	if err != nil {
		return err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	return rc.inner.DeleteAllOf(ctx, obj, client.InNamespace(rc.namespace))
}

func (rc *replayResourceClient) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	key := client.ObjectKey{Namespace: rc.namespace, Name: name}
	if err := rc.inner.Get(ctx, key, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	listOpts := []client.ListOption{client.InNamespace(rc.namespace)}
	if opts.LabelSelector != "" {
		selector, err := labels.Parse(opts.LabelSelector)
		if err != nil {
			return nil, err
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
	}
	if err := rc.inner.List(ctx, list, listOpts...); err != nil {
		return nil, err
	}
	return list, nil
}

func (rc *replayResourceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	return nil, fmt.Errorf("Watch not supported in replay mode")
}

func (rc *replayResourceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	gvk, err := rc.gvk()
	if err != nil {
		return nil, err
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(rc.namespace)
	patch := client.RawPatch(pt, data)
	if err := rc.inner.Patch(ctx, obj, patch); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if err := rc.ensureTypeMeta(obj); err != nil {
		return nil, err
	}
	if rc.namespace != "" && obj.GetNamespace() == "" {
		obj.SetNamespace(rc.namespace)
	}
	patch := client.Apply
	patchOpts := []client.PatchOption{
		client.ForceOwnership,
		client.FieldOwner(opts.FieldManager),
	}
	if len(subresources) > 0 && subresources[0] == "status" {
		if err := rc.inner.Status().Patch(ctx, obj, patch, patchOpts...); err != nil {
			return nil, err
		}
		return obj, nil
	}
	if err := rc.inner.Patch(ctx, obj, patch, patchOpts...); err != nil {
		return nil, err
	}
	return obj, nil
}

func (rc *replayResourceClient) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return rc.Apply(ctx, name, obj, opts, "status")
}
```

- [ ] **Step 2: Verify compilation**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro && go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add dynamic_adapter.go
git commit -m "feat(kro): add dynamic.Interface adapter for replay client"
```

---

### Task 5: Implement kroclient.SetInterface adapter

**Files:**
- Create: `examples/kro/clientset_adapter.go`

Implements `kroclient.SetInterface` backed by kamera's replay client. Both controllers use `Dynamic()` and `RESTMapper()`. The RGD controller additionally uses `CRD()`.

- [ ] **Step 1: Write clientset_adapter.go**

```go
package main

import (
	"context"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	k8smetadata "k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"

	kroclient "github.com/kubernetes-sigs/kro/pkg/client"
)

// replayClientSet implements kroclient.SetInterface backed by a kamera replay
// client.Client. Provides the client interfaces that KRO controllers need.
type replayClientSet struct {
	dynamicClient *replayDynamicClient
	replayClient  ctrlclient.Client
	restMapper    meta.RESTMapper
}

var _ kroclient.SetInterface = (*replayClientSet)(nil)

func newReplayClientSet(c ctrlclient.Client, mapper meta.RESTMapper) *replayClientSet {
	return &replayClientSet{
		dynamicClient: newReplayDynamicClient(c, mapper),
		replayClient:  c,
		restMapper:    mapper,
	}
}

func (s *replayClientSet) Dynamic() dynamic.Interface     { return s.dynamicClient }
func (s *replayClientSet) RESTMapper() meta.RESTMapper     { return s.restMapper }
func (s *replayClientSet) SetRESTMapper(m meta.RESTMapper) { s.restMapper = m }
func (s *replayClientSet) HTTPClient() *http.Client        { return nil }
func (s *replayClientSet) RESTConfig() *rest.Config        { return nil }
func (s *replayClientSet) Kubernetes() kubernetes.Interface { return nil }
func (s *replayClientSet) Metadata() k8smetadata.Interface { return nil }

func (s *replayClientSet) APIExtensionsV1() apiextensionsv1.ApiextensionsV1Interface {
	return nil
}

func (s *replayClientSet) CRD(cfg kroclient.CRDWrapperConfig) kroclient.CRDInterface {
	return &replayCRDClient{inner: s.replayClient}
}

func (s *replayClientSet) WithImpersonation(user string) (kroclient.SetInterface, error) {
	return s, nil
}

// replayCRDClient implements kroclient.CRDInterface using the replay client.
type replayCRDClient struct {
	inner ctrlclient.Client
}

var _ kroclient.CRDInterface = (*replayCRDClient)(nil)

func (c *replayCRDClient) Ensure(ctx context.Context, crd extv1.CustomResourceDefinition, allowBreakingChanges bool) error {
	existing := &extv1.CustomResourceDefinition{}
	err := c.inner.Get(ctx, ctrlclient.ObjectKey{Name: crd.Name}, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return c.inner.Create(ctx, &crd)
		}
		return err
	}
	existing.Spec = crd.Spec
	existing.Labels = crd.Labels
	return c.inner.Update(ctx, existing)
}

func (c *replayCRDClient) Delete(ctx context.Context, name string) error {
	crd := &extv1.CustomResourceDefinition{}
	crd.SetName(name)
	return ctrlclient.IgnoreNotFound(c.inner.Delete(ctx, crd))
}

func (c *replayCRDClient) Get(ctx context.Context, name string) (*extv1.CustomResourceDefinition, error) {
	crd := &extv1.CustomResourceDefinition{}
	if err := c.inner.Get(ctx, ctrlclient.ObjectKey{Name: name}, crd); err != nil {
		return nil, err
	}
	return crd, nil
}
```

- [ ] **Step 2: Verify compilation**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro && go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add clientset_adapter.go
git commit -m "feat(kro): add kroclient.SetInterface adapter for replay client"
```

---

## Chunk 3: Reconciler Adapters + Controller Wiring

### Task 6: Implement reconciler adapters

**Files:**
- Create: `examples/kro/reconciler_adapters.go`

Three components:
1. **Instance Controller adapter**: Wraps `Reconcile(ctx, req) error` → `(Result, error)`, translating KRO's `requeue` error types.
2. **RGD Controller wrapper**: Uses `reconcile.AsReconciler` to adapt the generic reconciler. Sets `Client` field directly (bypassing `SetupWithManager`). Uses `NewBuilderFromResolver` with core-only schema resolver.
3. **DynamicControllerRegistrar stub**: No-op Register/Deregister since kamera handles watch/enqueue natively.

- [ ] **Step 1: Write reconciler_adapters.go**

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apiextensions-apiserver/pkg/generated/openapi"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubernetes-sigs/kro/pkg/controller/instance"
	"github.com/kubernetes-sigs/kro/pkg/controller/resourcegraphdefinition"
	"github.com/kubernetes-sigs/kro/pkg/dynamiccontroller"
	"github.com/kubernetes-sigs/kro/pkg/graph"
	"github.com/kubernetes-sigs/kro/pkg/metadata"
	"github.com/kubernetes-sigs/kro/pkg/requeue"
	v1alpha1 "github.com/kubernetes-sigs/kro/api/v1alpha1"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
)

// --- Instance Controller ---

// adaptInstanceController wraps KRO's instance.Controller (returns error only)
// into a controller-runtime Reconciler (returns Result, error).
// Translates KRO's requeue error types into Result fields.
func adaptInstanceController(ic *instance.Controller) tracecheck.Reconciler {
	return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
		err := ic.Reconcile(ctx, req)
		if err == nil {
			return reconcile.Result{}, nil
		}

		var reqAfter *requeue.RequeueNeededAfter
		if errors.As(err, &reqAfter) {
			return reconcile.Result{RequeueAfter: reqAfter.Duration()}, nil
		}

		var reqNeeded *requeue.RequeueNeeded
		if errors.As(err, &reqNeeded) {
			return reconcile.Result{Requeue: true}, nil
		}

		var noReq *requeue.NoRequeue
		if errors.As(err, &noReq) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, err
	})
}

// newInstanceController creates a real KRO instance.Controller.
func newInstanceController(
	c ctrlclient.Client,
	log logr.Logger,
	gvr schema.GroupVersionResource,
	rgd *graph.Graph,
) tracecheck.Reconciler {
	mapper := staticRESTMapper()
	clientSet := newReplayClientSet(c, mapper)
	labeler := metadata.NewKROMetaLabeler()

	ic := instance.NewController(
		log,
		instance.ReconcileConfig{
			DefaultRequeueDuration:    3 * time.Second,
			DeletionGraceTimeDuration: 30 * time.Second,
			DeletionPolicy:            "Delete",
		},
		gvr,
		rgd,
		clientSet,
		labeler,
	)
	return adaptInstanceController(ic)
}

// --- RGD Controller ---

// stubDynamicControllerRegistrar is a no-op implementation of the
// DynamicControllerRegistrar interface. Kamera handles watch/enqueue
// natively, so Register/Deregister are not needed.
type stubDynamicControllerRegistrar struct{}

var _ resourcegraphdefinition.DynamicControllerRegistrar = (*stubDynamicControllerRegistrar)(nil)

func (s *stubDynamicControllerRegistrar) Register(
	ctx context.Context,
	parent schema.GroupVersionResource,
	instanceHandler dynamiccontroller.Handler,
	resourceGVRsToWatch ...schema.GroupVersionResource,
) error {
	return nil // kamera handles watches natively
}

func (s *stubDynamicControllerRegistrar) Deregister(
	ctx context.Context,
	parent schema.GroupVersionResource,
) error {
	return nil
}

// newRGDReconciler creates a real KRO ResourceGraphDefinitionReconciler.
// Uses NewBuilderFromResolver with core-only schema resolver (no API server).
func newRGDReconciler(c ctrlclient.Client) tracecheck.Reconciler {
	mapper := staticRESTMapper()
	clientSet := newReplayClientSet(c, mapper)

	// Core-only schema resolver — resolves Deployment, Service, Ingress
	// from compiled-in OpenAPI definitions. No network calls.
	coreResolver := resolver.NewDefinitionsSchemaResolver(
		openapi.GetOpenAPIDefinitions,
		scheme.Scheme,
	)
	graphBuilder := graph.NewBuilderFromResolver(coreResolver, mapper)

	reconciler := resourcegraphdefinition.NewResourceGraphDefinitionReconciler(
		clientSet,
		false, // allowCRDDeletion
		&stubDynamicControllerRegistrar{},
		graphBuilder,
		1, // maxConcurrentReconciles
	)
	// Set embedded client.Client directly (bypasses SetupWithManager)
	reconciler.Client = c

	return reconcile.AsReconciler[*v1alpha1.ResourceGraphDefinition](c, reconciler)
}

// --- Static REST Mapper ---

// staticRESTMapper returns a RESTMapper pre-configured with the GVKs
// that KRO's Application RGD needs.
func staticRESTMapper() meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{
		{Group: "", Version: "v1"},
		{Group: "apps", Version: "v1"},
		{Group: "networking.k8s.io", Version: "v1"},
		{Group: "kro.run", Version: "v1alpha1"},
		{Group: "apiextensions.k8s.io", Version: "v1"},
	})
	mapper.Add(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Service"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "Application"}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "kro.run", Version: "v1alpha1", Kind: "ResourceGraphDefinition"}, meta.RESTScopeRoot)
	mapper.Add(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}, meta.RESTScopeRoot)
	return mapper
}
```

- [ ] **Step 2: Verify compilation**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro && go build ./...
```

- [ ] **Step 3: Commit**

```bash
git add reconciler_adapters.go
git commit -m "feat(kro): add reconciler adapters for Instance + RGD controllers"
```

---

### Task 7: Wire real controllers into ExplorerBuilder

**Files:**
- Modify: `examples/kro/scenario.go`

Delete all hand-rolled controller code. Rewrite `newKROExplorerBuilder` to use the real controllers.

- [ ] **Step 1: Delete hand-rolled controller code from scenario.go**

Delete these structs/functions:
- `resourceGraphDefinitionController` struct and `Reconcile` method
- `applicationController` struct and all methods (`Reconcile`, `reconcileDeployment`, `reconcileService`, `reconcileIngress`, `deleteIngressIfPresent`, `updateApplicationStatus`)
- `setResourceGraphDefinitionStatus`
- `updateCRDIfChanged`
- `applicationSpec` struct and `applicationSpecFromObject`
- `buildApplicationDeployment`, `buildApplicationService`, `buildApplicationIngress`
- `buildApplicationCRD`

- [ ] **Step 2: Rewrite newKROExplorerBuilder**

```go
func newKROExplorerBuilder() *tracecheck.ExplorerBuilder {
	sch := runtime.NewScheme()
	utilruntime.Must(extv1.AddToScheme(sch))
	utilruntime.Must(networkingv1.AddToScheme(sch))
	utilruntime.Must(v1alpha1.AddToScheme(sch))

	builder := tracecheck.NewExplorerBuilder(sch)
	builder.WithMaxDepth(30)

	applicationGVR := schema.GroupVersionResource{
		Group: "kro.run", Version: "v1alpha1", Resource: "applications",
	}

	// Wire real RGD controller — uses NewBuilderFromResolver with core schema
	builder.WithReconciler(resourceGraphDefinitionControllerID, func(c client.Client) tracecheck.Reconciler {
		return newRGDReconciler(c)
	}).For(kroDomainName + "/" + resourceGraphDefinitionKind)

	// Wire real Instance (Application) controller
	builder.WithReconciler(applicationControllerID, func(c client.Client) tracecheck.Reconciler {
		log := ctrl.Log.WithName("application-controller")
		// Build graph from the RGD that will be in state.
		// The Instance controller needs the pre-built graph.
		// We build it here using the same core resolver.
		rgd := buildQuickstartApplicationRGD()
		appGraph := mustBuildGraph(rgd)
		return newInstanceController(c, log, applicationGVR, appGraph)
	}).For(kroDomainName+"/"+applicationKind).
		Watches("apps/Deployment", enqueueApplicationFromManagedResource).
		Watches("Service", enqueueApplicationFromManagedResource).
		Watches("networking.k8s.io/Ingress", enqueueApplicationFromManagedResource)

	builder.WithResourceDep(kroDomainName+"/"+resourceGraphDefinitionKind, resourceGraphDefinitionControllerID)
	builder.WithResourceDep(kroDomainName+"/"+applicationKind, applicationControllerID)

	return builder
}

// mustBuildGraph builds a graph.Graph from an RGD unstructured object.
// Uses the core schema resolver (no API server needed).
func mustBuildGraph(rgdObj *unstructured.Unstructured) *graph.Graph {
	mapper := staticRESTMapper()
	coreResolver := resolver.NewDefinitionsSchemaResolver(
		openapi.GetOpenAPIDefinitions,
		scheme.Scheme,
	)
	graphBuilder := graph.NewBuilderFromResolver(coreResolver, mapper)

	// Convert unstructured to typed RGD
	rgd := &v1alpha1.ResourceGraphDefinition{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(rgdObj.Object, rgd); err != nil {
		panic(fmt.Sprintf("convert RGD: %v", err))
	}

	g, err := graphBuilder.NewResourceGraphDefinition(rgd)
	if err != nil {
		panic(fmt.Sprintf("build graph: %v", err))
	}
	return g
}
```

- [ ] **Step 3: Clean up imports**

Remove unused imports from scenario.go. Add new imports for `v1alpha1`, `graph`, `resolver`, `openapi`, `scheme`.

- [ ] **Step 4: Verify compilation**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro && go build ./...
```

- [ ] **Step 5: Commit**

```bash
git add scenario.go
git commit -m "feat(kro): wire real KRO controllers into kamera harness

Replace all hand-rolled controller simulations with real KRO Instance
Controller and ResourceGraphDefinitionReconciler. Both controllers run
real reconciliation logic through kamera's replay client adapters."
```

---

## Chunk 4: Smoke Test + ANALYSIS.md

### Task 8: Build and verify

**Files:**
- No new files

- [ ] **Step 1: Build the harness**

```bash
cd /Users/tgoodwin/projects/kamera/examples/kro
go build -o kro .
```

- [ ] **Step 2: Run interactive mode**

```bash
./kro --interactive=true
```

Verify: no panics, RGD creates CRD + updates status, Application creates Deployment/Service/Ingress.

- [ ] **Step 3: Run batch scenarios**

```bash
./kro --inputs inputs.json --output /tmp/kro-smoke --interactive=false --timeout 60s
```

- [ ] **Step 4: Check divergence**

```bash
for f in /tmp/kro-smoke/*.jsonl; do
  echo "=== $(basename $f) ==="
  jq -r '.states[0].paths[0][-1].contentsHashAfter' "$f" 2>/dev/null | sort | uniq -c | sort -rn
done
```

- [ ] **Step 5: Debug and fix issues**

Common issues:
- "frame not found" → check controller registration names match resource types
- Panic in Apply → dynamic adapter GVK resolution, check staticRESTMapper
- Graph build failure → core schema resolver missing a type (shouldn't happen for Deployment/Service/Ingress)
- Infinite cycling → real controller unconditional writes (document as finding!)

- [ ] **Step 6: Commit working state**

```bash
git add -A
git commit -m "feat(kro): verified harness with real controllers - smoke test passes"
```

---

### Task 9: Update ANALYSIS.md

**Files:**
- Create/Modify: `examples/kro/.agents/ANALYSIS.md`

- [ ] **Step 1: Document harness architecture**

Cover:
- Both controllers are real (no simulations)
- Surgical kro changes made and why
- Known limitations (e.g., DynamicController stub means microcontroller registration is no-op)
- Remaining simulation gap: kamera's replay client vs real API server semantics

- [ ] **Step 2: Document Phase 0 verification results**

- [ ] **Step 3: Commit**

```bash
git add examples/kro/.agents/ANALYSIS.md
git commit -m "docs(kro): ANALYSIS.md with real-controller harness architecture"
```
