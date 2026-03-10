package main

import (
	"context"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// nodeRegistrar simulates kubelet/CCM node registration by creating a Node from a NodeClaim.
// Because the karpenter lifecycle controller is not used in this harness, the
// registrar creates Nodes that are immediately "ready": labels are synced from
// the NodeClaim, allocatable/capacity are copied, and the Node is marked as
// registered + initialized (no startup taint).
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

	// Copy labels from NodeClaim (instance-type, topology, capacity-type, etc.)
	// and mark the Node as registered and initialized.
	node.Labels = lo.Assign(nc.Labels, map[string]string{
		v1.NodeRegisteredLabelKey:      "true",
		v1.NodeInitializedLabelKey:     "true",
		"karpenter.sh/nodeclaim-name": nc.Name,
	})

	// Copy allocatable/capacity so state informers see the node's resources.
	node.Status.Allocatable = nc.Status.Allocatable
	node.Status.Capacity = nc.Status.Capacity
	node.Status.Conditions = []corev1.NodeCondition{
		{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
	}

	if err := r.client.Create(ctx, node); err != nil {
		if errors.IsAlreadyExists(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}
