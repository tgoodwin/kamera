# Karpenter Kick-Tires Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a minimal Kamera example that wires Karpenter’s provisioning flow (pending Pod → NodeClaim → Node registration) using the fake CloudProvider.

**Architecture:** Create a new Go module under `examples/karpenter` that registers Karpenter APIs on the scheme, wires a minimal set of controllers, and adds small shims for simulation gaps (name generation, Node registration, singleton provisioner tick). Document assumptions that approximate real Karpenter semantics.

**Tech Stack:** Go 1.24, Kamera tracecheck/explore, Karpenter core repo, controller-runtime fake client (for unit tests).

---

### Task 1: Scaffold the example module and README

**Files:**
- Create: `examples/karpenter/go.mod`
- Create: `examples/karpenter/README.md`
- Create: `examples/karpenter/main.go`
- Create: `examples/karpenter/scheme.go`
- Create: `examples/karpenter/smoke_test.go`

**Step 1: Write the failing test**

`examples/karpenter/smoke_test.go`:
```go
package main

import "testing"

func TestPlaceholder(t *testing.T) {
	t.Fatalf("replace this placeholder with real tests")
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./examples/karpenter -run TestPlaceholder`
Expected: FAIL with "replace this placeholder"

**Step 3: Write minimal implementation**

`examples/karpenter/go.mod`:
```go
module sigs.k8s.io/karpenter/examples/karpenter

go 1.24.0

require (
	github.com/tgoodwin/kamera v0.0.0
	sigs.k8s.io/karpenter v0.0.0
	k8s.io/api v0.33.4
	k8s.io/apimachinery v0.33.4
	sigs.k8s.io/controller-runtime v0.19.0
)

replace github.com/tgoodwin/kamera => ../..
replace sigs.k8s.io/karpenter => /Users/tgoodwin/projects/karpenter
```

`examples/karpenter/scheme.go`:
```go
package main

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"

	_ "sigs.k8s.io/karpenter/pkg/apis/v1"        // register karpenter.sh/v1
	_ "sigs.k8s.io/karpenter/pkg/test/v1alpha1"  // register karpenter.test.sh/v1alpha1
)

func newScheme() *runtime.Scheme {
	// Use the global scheme so Karpenter’s init() registrations are picked up.
	s := scheme.Scheme
	utilruntime.Must(corev1.AddToScheme(s))
	return s
}
```

`examples/karpenter/main.go`:
```go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/tgoodwin/kamera/pkg/explore"
)

func main() {
	flag.Parse()

	builder := newKarpenterExplorerBuilder()
	if cfgPath := explore.ConfigPath(); cfgPath != "" {
		loadedCfg, err := explore.LoadExploreConfigFromFile(cfgPath, builder.Config())
		if err != nil {
			fmt.Fprintf(os.Stderr, "load explore config: %v\n", err)
			os.Exit(1)
		}
		builder.SetConfig(loadedCfg)
	}

	initialState := buildInitialKarpenterState(builder)
	runner, err := explore.NewRunner(builder)
	if err != nil {
		fmt.Fprintf(os.Stderr, "runner setup error: %v\n", err)
		os.Exit(1)
	}
	if err := runner.Run(context.Background(), initialState); err != nil {
		fmt.Fprintf(os.Stderr, "session error: %v\n", err)
		os.Exit(1)
	}
}
```

`examples/karpenter/README.md`:
```md
# Karpenter Kick-the-Tires Example

This example wires a minimal Karpenter provisioning flow into Kamera:
Pending Pod → Provisioner → NodeClaim → Node registration.

## Approximations
- We simulate API server generateName for NodeClaims in the harness.
- We simulate Node registration by creating a Node from NodeClaim status.
- We map Node→NodeClaim via a label set by the simulator (to approximate providerID matching).
- We avoid multi-object list filtering by keeping a single NodePool/NodeClaim/Node.

## Usage

```bash
go run . -interactive=false -output /tmp/kamera-karpenter.jsonl
```

## Next scopes
- **Medium:** add nodepool validation/readiness/registration-health + nodeclaim GC/expiration/disruption.
- **Full:** wire all controllers from `pkg/controllers/controllers.go` (optionally exclude metrics).
```

**Step 4: Run test to verify it passes**

Update `examples/karpenter/smoke_test.go`:
```go
package main

import "testing"

func TestPlaceholder(t *testing.T) {}
```

Run: `go test ./examples/karpenter -run TestPlaceholder`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/karpenter/go.mod examples/karpenter/README.md examples/karpenter/main.go examples/karpenter/scheme.go examples/karpenter/smoke_test.go
git commit -m "add karpenter example scaffold"
```

---

### Task 2: Add the provisioner adapter and deterministic name generation

**Files:**
- Create: `examples/karpenter/provisioner_adapter.go`
- Create: `examples/karpenter/name_generating_client.go`
- Test: `examples/karpenter/provisioner_adapter_test.go`

**Step 1: Write the failing test**

`examples/karpenter/provisioner_adapter_test.go`:
```go
package main

import (
	"context"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestProvisionerAdapterMapsResult(t *testing.T) {
	adapter := provisionerAdapter{
		reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
			return reconciler.Result{RequeueAfter: time.Second * 5}, nil
		},
	}
	res, err := adapter.Reconcile(context.Background(), reconcile.Request{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("expected 5s requeue, got %v", res.RequeueAfter)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./examples/karpenter -run TestProvisionerAdapterMapsResult`
Expected: FAIL (undefined types)

**Step 3: Write minimal implementation**

`examples/karpenter/provisioner_adapter.go`:
```go
package main

import (
	"context"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// provisionerAdapter bridges operatorpkg’s singleton reconciler to controller-runtime.
// NOTE: In real Karpenter, the provisioner is triggered by a singleton source + internal batcher.
// In Kamera we explicitly enqueue it (via PodController + async tick) to preserve semantics.
type provisionerAdapter struct {
	reconcileFunc func(context.Context) (reconciler.Result, error)
}

func (a provisionerAdapter) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	result, err := a.reconcileFunc(ctx)
	return reconcile.Result{
		Requeue:      result.Requeue,
		RequeueAfter: time.Duration(result.RequeueAfter),
	}, err
}
```

`examples/karpenter/name_generating_client.go`:
```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// nameGeneratingClient simulates API server generateName semantics for CREATE.
// This is required because the replay client does not auto-generate names.
type nameGeneratingClient struct {
	client.Client
	counter uint64
}

func (c *nameGeneratingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetName() == "" && obj.GetGenerateName() != "" {
		id := atomic.AddUint64(&c.counter, 1)
		obj.SetName(fmt.Sprintf("%s%05d", obj.GetGenerateName(), id))
	}
	return c.Client.Create(ctx, obj, opts...)
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./examples/karpenter -run TestProvisionerAdapterMapsResult`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/karpenter/provisioner_adapter.go examples/karpenter/name_generating_client.go examples/karpenter/provisioner_adapter_test.go
git commit -m "add provisioner adapter and name generation shim"
```

---

### Task 3: Add NodeRegistrar and watch mapper shims

**Files:**
- Create: `examples/karpenter/node_registrar.go`
- Create: `examples/karpenter/watch_mappers.go`
- Test: `examples/karpenter/node_registrar_test.go`

**Step 1: Write the failing test**

`examples/karpenter/node_registrar_test.go`:
```go
package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestNodeRegistrarCreatesNode(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	nc := &v1.NodeClaim{}
	nc.Name = "nc-1"
	nc.Status.ProviderID = "provider-1"

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nc).Build()
	reg := nodeRegistrar{client: cl}

	_, err := reg.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(nc)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var node corev1.Node
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "provider-1"}, &node); err != nil {
		t.Fatalf("expected node to exist: %v", err)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./examples/karpenter -run TestNodeRegistrarCreatesNode`
Expected: FAIL (undefined types)

**Step 3: Write minimal implementation**

`examples/karpenter/node_registrar.go`:
```go
package main

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// nodeRegistrar simulates kubelet/CCM node registration by creating a Node from a NodeClaim.
// NOTE: In real Karpenter, Nodes are created by kubelet registration, not Karpenter.
// We inject Nodes here so the NodeClaim registration flow can be exercised in simulation.
type nodeRegistrar struct {
	client client.Client
}

func (r nodeRegistrar) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	nc := &v1.NodeClaim{}
	if err := r.client.Get(ctx, req.NamespacedName, nc); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	if nc.Status.ProviderID == "" {
		return reconcile.Result{}, nil
	}

	node := &corev1.Node{}
	node.Name = nc.Status.ProviderID
	node.Spec.ProviderID = nc.Status.ProviderID
	// Simulate startup taint that registration expects to remove.
	node.Spec.Taints = append(node.Spec.Taints, v1.UnregisteredNoExecuteTaint)
	// Label used by watch mapper to relate Node -> NodeClaim (approximation).
	node.Labels = map[string]string{
		"karpenter.sh/nodeclaim-name": nc.Name,
	}

	if err := r.client.Create(ctx, node); err != nil {
		if errors.IsAlreadyExists(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
```

`examples/karpenter/watch_mappers.go`:
```go
package main

import (
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// nodeToNodeClaimMapper approximates Karpenter's providerID-based Node→NodeClaim mapping.
// NOTE: In real Karpenter this mapping is done by listing NodeClaims with a providerID filter.
// The simulator cannot list with field selectors, so we attach a label on Node at creation time.
func nodeToNodeClaimMapper() tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		if obj == nil {
			return nil
		}
		name, ok := obj.GetLabels()["karpenter.sh/nodeclaim-name"]
		if !ok || name == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: name}}}
	}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./examples/karpenter -run TestNodeRegistrarCreatesNode`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/karpenter/node_registrar.go examples/karpenter/watch_mappers.go examples/karpenter/node_registrar_test.go
git commit -m "add node registrar and watch mapper shim"
```

---

### Task 4: Wire the Karpenter controllers and scenario

**Files:**
- Create: `examples/karpenter/builder.go`
- Create: `examples/karpenter/scenario.go`
- Create: `examples/karpenter/recorder.go`
- Modify: `examples/karpenter/smoke_test.go`

**Step 1: Write the failing test**

Update `examples/karpenter/smoke_test.go`:
```go
package main

import "testing"

func TestScenarioBuilds(t *testing.T) {
	if _, err := newScenarioObjects(); err != nil {
		t.Fatalf("expected scenario objects to build: %v", err)
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./examples/karpenter -run TestScenarioBuilds`
Expected: FAIL (undefined function)

**Step 3: Write minimal implementation**

`examples/karpenter/recorder.go`:
```go
package main

import "sigs.k8s.io/karpenter/pkg/events"

// noopRecorder is used to avoid external event sinks during simulation.
type noopRecorder struct{}

func (noopRecorder) Publish(...events.Event) {}

func newNoopRecorder() events.Recorder { return noopRecorder{} }
```

`examples/karpenter/builder.go`:
```go
package main

import (
	"context"
	"sync"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"github.com/tgoodwin/kamera/pkg/simclock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/consistency"
	nodeclaimhydration "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
)

func newKarpenterExplorerBuilder() *tracecheck.ExplorerBuilder {
	b := tracecheck.NewExplorerBuilder(newScheme())
	cp := fake.NewCloudProvider()
	clk := clock.RealClock{}

	var clusterOnce sync.Once
	var cluster *state.Cluster
	getCluster := func(c client.Client) *state.Cluster {
		clusterOnce.Do(func() {
			cluster = state.NewCluster(clk, c, cp)
		})
		return cluster
	}

	var provisionerOnce sync.Once
	var prov *provisioning.Provisioner
	getProvisioner := func(c client.Client) *provisioning.Provisioner {
		provisionerOnce.Do(func() {
			prov = provisioning.NewProvisioner(c, newNoopRecorder(), cp, getCluster(c), clk)
		})
		return prov
	}

	// Provisioner (singleton-style)
	b.WithReconciler("provisioner", func(c client.Client) tracecheck.Reconciler {
		wrapped := &nameGeneratingClient{Client: c}
		prov := getProvisioner(wrapped)
		return provisionerAdapter{
			reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
				return prov.Reconcile(ctx)
			},
		}
	}).For("Pod")

	b.WithReconciler("provisioner.trigger.pod", func(c client.Client) tracecheck.Reconciler {
		wrapped := &nameGeneratingClient{Client: c}
		return provisioning.NewPodController(wrapped, getProvisioner(wrapped), getCluster(wrapped))
	}).For("Pod")

	// State informers
	b.WithReconciler("state.pod", func(c client.Client) tracecheck.Reconciler {
		return informer.NewPodController(c, getCluster(c))
	}).For("Pod")
	b.WithReconciler("state.node", func(c client.Client) tracecheck.Reconciler {
		return informer.NewNodeController(c, getCluster(c))
	}).For("Node")
	b.WithReconciler("state.nodepool", func(c client.Client) tracecheck.Reconciler {
		return informer.NewNodePoolController(c, cp, getCluster(c))
	}).For("NodePool")
	b.WithReconciler("state.nodeclaim", func(c client.Client) tracecheck.Reconciler {
		return informer.NewNodeClaimController(c, cp, getCluster(c))
	}).For("NodeClaim")

	// NodeClaim lifecycle + hydration + consistency
	b.WithReconciler("nodeclaim.hydration", func(c client.Client) tracecheck.Reconciler {
		return nodeclaimhydration.NewController(c, cp)
	}).For("NodeClaim")

	b.WithReconciler("nodeclaim.lifecycle", func(c client.Client) tracecheck.Reconciler {
		return lifecycle.NewController(clk, c, cp, newNoopRecorder(), state.NewNodePoolState())
	}).For("NodeClaim").Watches("Node", nodeToNodeClaimMapper())

	b.WithReconciler("nodeclaim.consistency", func(c client.Client) tracecheck.Reconciler {
		return consistency.NewController(clk, c, cp, newNoopRecorder())
	}).For("NodeClaim").Watches("Node", nodeToNodeClaimMapper())

	// Node hydration
	b.WithReconciler("node.hydration", func(c client.Client) tracecheck.Reconciler {
		return hydration.NewController(c, cp)
	}).For("Node")

	// NodeRegistrar shim
	b.WithReconciler("node.registrar", func(c client.Client) tracecheck.Reconciler {
		return nodeRegistrar{client: c}
	}).For("NodeClaim")

	// Singleton-style deterministic tick for provisioner.
	// NOTE: this approximates controller-runtime singleton.Source() using simclock.
	ticker := simclock.NewTicker(10 * time.Second)
	simclock.RegisterTickerCallback(ticker, func() {
		tracecheck.GetGlobalAsyncEnqueueCollector().Add("provisioner", types.NamespacedName{Name: "singleton"})
	})

	return b
}
```

`examples/karpenter/scenario.go`:
```go
package main

import (
	"github.com/tgoodwin/kamera/pkg/tag"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func newScenarioObjects() ([]client.Object, error) {
	// TestNodeClass (fake cloud provider)
	nc := test.NodeClass(v1alpha1.TestNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	tag.AddSleeveObjectID(nc)

	// NodePool referencing the TestNodeClass
	np := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}})
	tag.AddSleeveObjectID(np)

	// Provisionable Pod (PodScheduled=False, Reason=Unschedulable)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "pause"}}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled,
			Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable,
		}}},
	}
	tag.AddSleeveObjectID(pod)

	return []client.Object{nc, np, pod}, nil
}

func buildInitialKarpenterState(builder *tracecheck.ExplorerBuilder) tracecheck.StateNode {
	stateBuilder := builder.NewStateEventBuilder()
	objs, _ := newScenarioObjects()

	nc := objs[0]
	np := objs[1]
	pod := objs[2]

	// Trigger pod-related controllers at start.
	podState := stateBuilder.AddTopLevelObject(pod, "state.pod", "provisioner.trigger.pod")
	poolState := stateBuilder.AddTopLevelObject(np, "state.nodepool")
	classState := stateBuilder.AddTopLevelObject(nc)

	state := tracecheck.MergeStateNodes(podState, poolState)
	return tracecheck.MergeStateNodes(state, classState)
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./examples/karpenter -run TestScenarioBuilds`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/karpenter/builder.go examples/karpenter/scenario.go examples/karpenter/recorder.go examples/karpenter/smoke_test.go
git commit -m "wire minimal karpenter controllers and scenario"
```

---

### Task 5: Verify deterministic name generation and tick behavior

**Files:**
- Modify: `examples/karpenter/provisioner_adapter_test.go`

**Step 1: Write the failing test**

Add to `examples/karpenter/provisioner_adapter_test.go`:
```go
func TestNameGeneratingClientAssignsName(t *testing.T) {
	// TODO: use fake client to ensure GenerateName becomes Name
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./examples/karpenter -run TestNameGeneratingClientAssignsName`
Expected: FAIL

**Step 3: Write minimal implementation**

Replace the TODO test with:
```go
func TestNameGeneratingClientAssignsName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	obj := &corev1.ConfigMap{}
	obj.GenerateName = "cm-"

	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	wrapped := &nameGeneratingClient{Client: cl}

	if err := wrapped.Create(context.Background(), obj); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if obj.Name == "" {
		t.Fatalf("expected generated name to be set")
	}
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./examples/karpenter -run TestNameGeneratingClientAssignsName`
Expected: PASS

**Step 5: Commit**

```bash
git add examples/karpenter/provisioner_adapter_test.go

git commit -m "test name generation shim"
```

---

### Task 6: Basic end-to-end run + doc update

**Files:**
- Modify: `examples/karpenter/README.md`

**Step 1: Run the example headless**

Run: `go run . -interactive=false -output /tmp/kamera-karpenter.jsonl`
Expected: A dump file is created, no errors printed.

**Step 2: Update README with run notes**

Add a short "Observed flow" section with the expected reconciliation sequence.

**Step 3: Commit**

```bash
git add examples/karpenter/README.md

git commit -m "document karpenter kick-tires flow"
```

---

### Task 7: Final test pass

**Step 1: Run tests**

Run: `go test ./examples/karpenter`
Expected: PASS

**Step 2: Commit (if any edits were made)**

```bash
git status --short
```
