package kamera

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	netv1alpha1 "knative.dev/networking/pkg/apis/networking/v1alpha1"
)

// IngressStatusStub immediately marks knative ingress resources as ready so that upstream
// Route reconciliation can observe a healthy networking layer.
type IngressStatusStub struct {
	client.Client
}

func (r *IngressStatusStub) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ing := &netv1alpha1.Ingress{}
	if err := r.Get(ctx, req.NamespacedName, ing); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	if !ing.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	host := fmt.Sprintf("%s.%s.example.com", ing.Name, ing.Namespace)
	clusterHost := fmt.Sprintf("%s.%s.svc.cluster.local", ing.Name, ing.Namespace)

	desiredStatus := ing.Status
	desiredStatus.InitializeConditions()
	desiredStatus.MarkNetworkConfigured()
	desiredStatus.MarkLoadBalancerReady(
		[]netv1alpha1.LoadBalancerIngressStatus{{
			Domain:         host,
			DomainInternal: clusterHost,
		}},
		[]netv1alpha1.LoadBalancerIngressStatus{{
			DomainInternal: clusterHost,
		}},
	)
	desiredStatus.ObservedGeneration = ing.Generation

	if equality.Semantic.DeepEqual(ing.Status, desiredStatus) {
		return reconcile.Result{}, nil
	}

	ing.Status = desiredStatus
	if err := r.Status().Update(ctx, ing); err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

// Ensure the stub satisfies the tracecheck client expectations.
var _ reconcile.Reconciler = (*IngressStatusStub)(nil)
