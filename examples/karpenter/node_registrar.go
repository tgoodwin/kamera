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
