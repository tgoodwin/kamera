package main

import (
	"context"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/tgoodwin/kamera/pkg/simclock"
	"github.com/tgoodwin/kamera/pkg/tracecheck"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/node/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	terminatortypes "sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	nodeclaimhydration "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolhealth"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	nodepoolreadiness "sigs.k8s.io/karpenter/pkg/controllers/nodepool/readiness"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func newKarpenterExplorerBuilder() *tracecheck.ExplorerBuilder {
	b := tracecheck.NewExplorerBuilder(newScheme())
	// Use a 30-second step so that karpenter's 1-minute state-informer
	// RequeueAfter translates to only 2 simulated steps instead of 60.
	// This lets the stable-requeue-after convergence mechanism build its
	// streak (threshold=3) within a reasonable depth budget.
	simclock.Configure(time.Unix(0, 0), 30*time.Second)

	// Zero out retry backoff to avoid real-time sleeps during simulation.
	retry.DefaultBackoff = wait.Backoff{Steps: 1, Duration: 0}

	// Disable generic pod lifecycle simulation for this harness.
	// Karpenter provisioning should reason over unschedulable pods, and Pod lifecycle
	// progression here can consume that signal before Karpenter-specific controllers run.
	b.WithReconciler("PodLifecycleController", func(c client.Client) tracecheck.Reconciler {
		return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, nil
		})
	}).For("disabled/PodLifecycle")

	cp := &deterministicCloudProvider{fake.NewCloudProvider()}
	clk := clock.RealClock{}
	opts := test.Options()
	switcher := newSwitchingClient()
	provisionerClient := &nameGeneratingClient{Client: switcher}

	wrapWithOptions := func(c client.Client, inner tracecheck.Reconciler) tracecheck.Reconciler {
		return reconcile.Func(func(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
			// Ensure shared cluster state sees the correct per-reconciler replay client.
			switcher.Set(c)
			ctx = options.ToContext(ctx, opts)
			return inner.Reconcile(ctx, req)
		})
	}

	var clusterOnce sync.Once
	var cluster *state.Cluster
	getCluster := func(c client.Client) *state.Cluster {
		clusterOnce.Do(func() {
			cluster = state.NewCluster(clk, switcher, cp)
		})
		return cluster
	}

	var provisionerOnce sync.Once
	var prov *provisioning.Provisioner
	getProvisioner := func() *provisioning.Provisioner {
		provisionerOnce.Do(func() {
			prov = provisioning.NewProvisioner(provisionerClient, newNoopRecorder(), cp, getCluster(provisionerClient), clk)
			prov.SetSimulation(true)
		})
		return prov
	}

	// Provisioner (singleton-style)
	b.WithReconciler("provisioner", func(c client.Client) tracecheck.Reconciler {
		prov := getProvisioner()
		adapter := provisionerAdapter{
			reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
				return prov.Reconcile(ctx)
			},
		}
		return wrapWithOptions(c, adapter)
	}).For("Pod")

	b.WithReconciler("provisioner.trigger.pod", func(c client.Client) tracecheck.Reconciler {
		wrapped := &nameGeneratingClient{Client: c}
		pc := provisioning.NewPodController(wrapped, getProvisioner(), getCluster(wrapped))
		return wrapWithOptions(c, reconcile.AsReconciler(wrapped, pc))
	}).For("Pod")

	// State informers
	b.WithReconciler("state.pod", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(c, informer.NewPodController(c, getCluster(c)))
	}).For("Pod")
	b.WithReconciler("state.node", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(c, informer.NewNodeController(c, getCluster(c)))
	}).For("Node")
	b.WithReconciler("state.nodepool", func(c client.Client) tracecheck.Reconciler {
		rec := informer.NewNodePoolController(c, cp, getCluster(c))
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("karpenter.sh/NodePool")
	b.WithReconciler("state.nodeclaim", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(c, informer.NewNodeClaimController(c, cp, getCluster(c)))
	}).For("karpenter.sh/NodeClaim")

	// NodeClaim hydration + launch
	b.WithReconciler("nodeclaim.hydration", func(c client.Client) tracecheck.Reconciler {
		rec := nodeclaimhydration.NewController(c, cp)
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("karpenter.sh/NodeClaim")

	recorder := newNoopRecorder()

	// NodeClaim lifecycle: launch → registration → initialization → liveness.
	// Replaces the previous nodeClaimLauncher shim + nodeRegistrar shim.
	var npHealthOnce sync.Once
	var npHealth *nodepoolhealth.State
	getNpHealth := func() *nodepoolhealth.State {
		npHealthOnce.Do(func() {
			npHealth = nodepoolhealth.NewState()
		})
		return npHealth
	}

	b.WithReconciler("nodeclaim.lifecycle", func(c client.Client) tracecheck.Reconciler {
		rec := lifecycle.NewController(clk, c, cp, recorder, getNpHealth())
		rec.Simulation = true
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("karpenter.sh/NodeClaim")

	// NodePool readiness: propagates NodeClass readiness into NodePool status conditions.
	// The provisioner filters out NodePools where StatusConditions[Ready] is not True,
	// so this controller is the write side of the NodeClass-readiness TOCTOU window.
	b.WithReconciler("nodepool.readiness", func(c client.Client) tracecheck.Reconciler {
		rec := nodepoolreadiness.NewController(c, cp)
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("karpenter.sh/NodePool").
		Watches("karpenter.test.sh/TestNodeClass", nodeClassToNodePoolMapper([]string{"default", "pool-a", "pool-b"}))

	// Node hydration
	b.WithReconciler("node.hydration", func(c client.Client) tracecheck.Reconciler {
		rec := hydration.NewController(c, cp)
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("Node")

	// NodeRegistrar shim: creates a Node object when a NodeClaim has a ProviderID.
	// In real Karpenter, the kubelet registers the Node. The lifecycle controller's
	// registration step then finds the Node by ProviderID (via MatchingFields query).
	b.WithReconciler("node.registrar", func(c client.Client) tracecheck.Reconciler {
		return wrapWithOptions(c, nodeRegistrar{client: c})
	}).For("karpenter.sh/NodeClaim")

	// --- Disruption controllers ---

	// Disruption queue: executes disruption commands (taint, create replacements, delete).
	var queueOnce sync.Once
	var disruptionQueue *disruption.Queue
	getDisruptionQueue := func() *disruption.Queue {
		queueOnce.Do(func() {
			disruptionQueue = disruption.NewQueue(switcher, recorder, getCluster(switcher), clk, getProvisioner())
		})
		return disruptionQueue
	}

	b.WithReconciler("disruption.queue", func(c client.Client) tracecheck.Reconciler {
		q := getDisruptionQueue()
		return wrapWithOptions(c, reconcile.AsReconciler(c, q))
	}).For("karpenter.sh/NodeClaim")

	// Main disruption controller (singleton-style, like provisioner).
	b.WithReconciler("disruption", func(c client.Client) tracecheck.Reconciler {
		ctrl := disruption.NewController(clk, c, getProvisioner(), cp, recorder, getCluster(c), getDisruptionQueue())
		adapter := provisionerAdapter{
			reconcileFunc: func(ctx context.Context) (reconciler.Result, error) {
				return ctrl.Reconcile(ctx)
			},
		}
		return wrapWithOptions(c, adapter)
	}).For("karpenter.sh/NodeClaim")

	// NodeClaim disruption conditions: marks NodeClaims as drifted/consolidatable.
	b.WithReconciler("nodeclaim.disruption", func(c client.Client) tracecheck.Reconciler {
		rec := nodeclaimdisruption.NewController(clk, c, cp)
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("karpenter.sh/NodeClaim")

	// Node termination: finalizes node deletion (drain, detach, terminate).
	b.WithReconciler("node.termination", func(c client.Client) tracecheck.Reconciler {
		eq := terminatortypes.NewQueue(c, recorder)
		term := terminatortypes.NewTerminator(clk, c, eq, recorder)
		rec := termination.NewController(clk, c, cp, term, recorder)
		return wrapWithOptions(c, reconcile.AsReconciler(c, rec))
	}).For("Node")

	// Singleton-style deterministic tick for provisioner.
	// NOTE: this approximates controller-runtime singleton.Source() using simclock.
	// Interval must be a positive multiple of the simclock step (30s).
	ticker := simclock.NewTicker(30 * time.Second)
	simclock.RegisterTickerCallback(ticker, func() {
		tracecheck.GetGlobalAsyncEnqueueCollector().Add("provisioner", types.NamespacedName{Name: "singleton"})
	})

	// Singleton tick for disruption controller.
	disruptionTicker := simclock.NewTicker(30 * time.Second)
	simclock.RegisterTickerCallback(disruptionTicker, func() {
		tracecheck.GetGlobalAsyncEnqueueCollector().Add("disruption", types.NamespacedName{Name: "singleton"})
	})

	// Reset shared in-memory state before each forked trial so that Monte
	// Carlo trials and perturbation phases each start from a clean slate.
	b.OnFork(func() {
		getCluster(nil).Reset()
		cp.Reset()
		provisionerClient.Reset()
		// Clear disruption queue's in-flight command tracking.
		if q := getDisruptionQueue(); q != nil {
			q.Lock()
			for k := range q.ProviderIDToCommand {
				delete(q.ProviderIDToCommand, k)
			}
			q.Unlock()
		}
	})

	// Reset shared in-memory state on fault injection crash. All controllers
	// share the same process in real Kubernetes, so a crash resets everything.
	// Unlike OnFork (which resets to empty), OnCrash resets to a state where
	// controllers will re-list from the API server on their next reconcile.
	b.OnCrash(func() {
		getCluster(nil).Reset()
		cp.Reset()
		provisionerClient.Reset()
		if q := getDisruptionQueue(); q != nil {
			q.Lock()
			for k := range q.ProviderIDToCommand {
				delete(q.ProviderIDToCommand, k)
			}
			q.Unlock()
		}
	})

	return b
}

// nodeClassToNodePoolMapper returns a WatchMapper that enqueues all NodePools
// referencing the changed TestNodeClass. This mirrors the real Karpenter behavior
// where nodepool.readiness watches NodeClass events via NodeClassEventHandler.
//
// Since kamera's watch mappers run outside reconciler context (the switching
// client may not have a valid delegate), we enumerate NodePool names from the
// initial environment state rather than listing from the API at runtime.
func nodeClassToNodePoolMapper(nodePoolNames []string) tracecheck.WatchMapper {
	return func(obj *unstructured.Unstructured) []reconcile.Request {
		reqs := make([]reconcile.Request, 0, len(nodePoolNames))
		for _, name := range nodePoolNames {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: name},
			})
		}
		return reqs
	}
}
