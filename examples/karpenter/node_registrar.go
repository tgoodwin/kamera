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
// The real lifecycle controller launches the NodeClaim and waits for this external
// registration event before marking it registered and initialized. The registrar
// creates a ready Node with labels, allocatable/capacity, and no startup taint.
type nodeRegistrar struct {
	client client.Client
}

func (r nodeRegistrar) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return reconcile.Result{}, r.Register(ctx, req)
}

// Register creates the Node corresponding to a launched NodeClaim. In the
// explorer this is called immediately after lifecycle assigns a ProviderID,
// representing the external kubelet/CCM registration event.
func (r nodeRegistrar) Register(ctx context.Context, req reconcile.Request) error {
	nc := &v1.NodeClaim{}
	if err := r.client.Get(ctx, req.NamespacedName, nc); err != nil {
		return client.IgnoreNotFound(err)
	}
	if nc.Status.ProviderID == "" || !nc.DeletionTimestamp.IsZero() {
		return nil
	}

	node := &corev1.Node{}
	node.Name = nc.Status.ProviderID
	node.Spec.ProviderID = nc.Status.ProviderID

	// Copy labels from NodeClaim (instance-type, topology, capacity-type, etc.)
	// and mark the Node as registered and initialized.
	node.Labels = lo.Assign(nc.Labels, map[string]string{
		v1.NodeRegisteredLabelKey:     "true",
		v1.NodeInitializedLabelKey:    "true",
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
			return nil
		}
		return err
	}
	return nil
}
