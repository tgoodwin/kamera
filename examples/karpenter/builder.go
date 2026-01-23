package main

import (
	"context"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/node/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/consistency"
	nodeclaimhydration "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"
	"sigs.k8s.io/karpenter/pkg/test"
)

func newKarpenterExplorerBuilder() *tracecheck.ExplorerBuilder {
	b := tracecheck.NewExplorerBuilder(newScheme())
	cp := fake.NewCloudProvider()
	clk := clock.RealClock{}
	opts := test.Options()

	wrapWithOptions := func(inner tracecheck.Reconciler) tracecheck.Reconciler {
		return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			ctx = options.ToContext(ctx, opts)
			return inner.Reconcile(ctx, req)
		})
	}

	var clusterOnce sync.Once
	var cluster *state.Cluster
	getCluster := func(c client.Client) *state.Cluster {
		clusterOnce.Do(func() {
			cluster = state.NewCluster(clk, &nameGeneratingClient{Client: c}, cp)
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

	nodePoolHealth := nodepoolhealth.NewState()

	// Provisioner (singleton-style)
	b.WithReconciler("provisioner", func(c client.Client) tracecheck.Reconciler {
		wrapped := &nameGeneratingClient{Client: c}
		prov := getProvisioner(wrapped)
		adapter := provisionerAdapter{
			reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
				return prov.Reconcile(ctx)
			},
		}
		return wrapWithOptions(adapter)
	}).For("Pod")

	b.WithReconciler("provisioner.trigger.pod", func(c client.Client) tracecheck.Reconciler {
		wrapped := &nameGeneratingClient{Client: c}
		pc := provisioning.NewPodController(wrapped, getProvisioner(wrapped), getCluster(wrapped))
		return wrapWithOptions(reconcile.AsReconciler(wrapped, pc))
	}).For("Pod")

	// State informers
	b.WithReconciler("state.pod", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(informer.NewPodController(c, getCluster(c)))
	}).For("Pod")
	b.WithReconciler("state.node", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(informer.NewNodeController(c, getCluster(c)))
	}).For("Node")
	b.WithReconciler("state.nodepool", func(c client.Client) tracecheck.Reconciler {
		rec := informer.NewNodePoolController(c, cp, getCluster(c))
		return wrapWithOptions(reconcile.AsReconciler(c, rec))
	}).For("NodePool")
	b.WithReconciler("state.nodeclaim", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(informer.NewNodeClaimController(c, cp, getCluster(c)))
	}).For("NodeClaim")

	// NodeClaim lifecycle + hydration + consistency
	b.WithReconciler("nodeclaim.hydration", func(c client.Client) tracecheck.Reconciler {
		rec := nodeclaimhydration.NewController(c, cp)
		return wrapWithOptions(reconcile.AsReconciler(c, rec))
	}).For("NodeClaim")

	b.WithReconciler("nodeclaim.lifecycle", func(c client.Client) tracecheck.Reconciler {
		rec := lifecycle.NewController(clk, c, cp, newNoopRecorder(), nodePoolHealth)
		return wrapWithOptions(reconcile.AsReconciler(c, rec))
	}).For("NodeClaim").Watches("Node", nodeToNodeClaimMapper())

	b.WithReconciler("nodeclaim.consistency", func(c client.Client) tracecheck.Reconciler {
		rec := consistency.NewController(clk, c, cp, newNoopRecorder())
		return wrapWithOptions(reconcile.AsReconciler(c, rec))
	}).For("NodeClaim").Watches("Node", nodeToNodeClaimMapper())

	// Node hydration
	b.WithReconciler("node.hydration", func(c client.Client) tracecheck.Reconciler {
		rec := hydration.NewController(c, cp)
		return wrapWithOptions(reconcile.AsReconciler(c, rec))
	}).For("Node")

	// NodeRegistrar shim
	b.WithReconciler("node.registrar", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(nodeRegistrar{client: c})
	}).For("NodeClaim")

	// Singleton-style deterministic tick for provisioner.
	// NOTE: this approximates controller-runtime singleton.Source() using simclock.
	ticker := simclock.NewTicker(10 * time.Second)
	simclock.RegisterTickerCallback(ticker, func() {
		tracecheck.GetGlobalAsyncEnqueueCollector().Add("provisioner", types.NamespacedName{Name: "singleton"})
	})

	return b
}
