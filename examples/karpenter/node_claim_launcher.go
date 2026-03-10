package main

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
)

// nodeClaimLauncher is a simplified replacement for karpenter's full lifecycle
// controller. It only handles the "launch" stage: calling CloudProvider.Create()
// to assign a ProviderID and populate instance details on the NodeClaim.
//
// Registration, initialization, and liveness are intentionally omitted:
//   - Registration uses MatchingFields queries unsupported by the replay client
//   - Liveness generates RequeueAfter that creates unbounded exploration depth
//   - The nodeRegistrar shim handles Node creation separately
type nodeClaimLauncher struct {
	cloudProvider cloudprovider.CloudProvider
	kubeClient    client.Client
}

func (l *nodeClaimLauncher) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	if nodeClaim.Status.ProviderID != "" {
		return reconcile.Result{}, nil
	}
	stored := nodeClaim.DeepCopy()
	created, err := l.cloudProvider.Create(ctx, nodeClaim)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("launching nodeclaim: %w", err)
	}
	lifecycle.PopulateNodeClaimDetails(nodeClaim, created)
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeLaunched)

	// Persist labels and status (ProviderID, allocatable, etc.) back to the API
	// so that downstream controllers (nodeRegistrar, state informers) see them.
	if err := l.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	return reconcile.Result{}, nil
}
